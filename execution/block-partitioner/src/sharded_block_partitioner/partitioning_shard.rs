// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use crate::sharded_block_partitioner::{
    conflict_detector::CrossShardConflictDetector,
    dependency_analyzer::DependencyAnalyzer,
    messages::{
        ControlMsg, CrossShardMsg, CrossShardMsg::DiscardedSenders, DependencyAnalysisMsg,
        DiscardedSendersMsg, PartitionBlockMsg, PartitionedBlockResponse, PartitioningStatus,
    },
};
use aptos_logger::trace;
use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use std::sync::mpsc::{Receiver, Sender};

/// A remote block executor that receives transactions from a channel and executes them in parallel.
/// Currently it runs in the local machine and it will be further extended to run in a remote machine.
pub struct PartitioningShard {
    shard_id: usize,
    control_rx: Receiver<ControlMsg>,
    result_tx: Sender<PartitionedBlockResponse>,
    message_rxs: Vec<Receiver<CrossShardMsg>>,
    messages_txs: Vec<Sender<CrossShardMsg>>,
}

impl PartitioningShard {
    pub fn new(
        shard_id: usize,
        control_rx: Receiver<ControlMsg>,
        result_tx: Sender<PartitionedBlockResponse>,
        message_rxs: Vec<Receiver<CrossShardMsg>>,
        messages_txs: Vec<Sender<CrossShardMsg>>,
    ) -> Self {
        Self {
            shard_id,
            control_rx,
            result_tx,
            message_rxs,
            messages_txs,
        }
    }

    fn broadcast_dependency_analysis(&self, conflict_detector: &CrossShardConflictDetector) {
        let mut now = std::time::Instant::now();
        let num_shards = self.messages_txs.len();
        let dependency_analysis_msg = conflict_detector.get_dependency_analysis_msg();
        for i in 0..num_shards {
            if i != self.shard_id {
                self.messages_txs[i]
                    .send(CrossShardMsg::DependencyAnalysis(
                        dependency_analysis_msg.clone(),
                    ))
                    .unwrap();
            }
        }
        println!("Time taken for dependency analysis: {:?} for shard_id {:?}", now.elapsed(), self.shard_id);
    }

    fn collect_cross_shard_dependency_analysis(&self) -> Vec<DependencyAnalysisMsg> {
        let mut dependency_analysis_msgs = vec![DependencyAnalysisMsg::default(); self.messages_txs.len()];
        for i in 0..self.messages_txs.len() {
            if i == self.shard_id {
                continue;
            }
            let msg = self.message_rxs[i].recv().unwrap();
            match msg {
                CrossShardMsg::DependencyAnalysis(dependency_analysis_msg) => {
                    dependency_analysis_msgs[i] = dependency_analysis_msg;
                }
                _ => panic!("Unexpected message"),
            }
        }
        dependency_analysis_msgs
    }

    fn broadcast_discarded_senders_msg(&self, discarded_sender_msg: &DiscardedSendersMsg) {
        let num_shards = self.messages_txs.len();
        for i in 0..num_shards {
            if i != self.shard_id {
                self.messages_txs[i]
                    .send(CrossShardMsg::DiscardedSenders(
                        discarded_sender_msg.clone(),
                    ))
                    .unwrap();
            }
        }
    }

    fn collect_cross_shard_discarded_senders(&self) -> Vec<DiscardedSendersMsg> {
        let mut discarded_senders_msgs = vec![DiscardedSendersMsg::default(); self.messages_txs.len()];
        for i in 0..self.messages_txs.len() {
            if i == self.shard_id {
                continue;
            }
            let msg = self.message_rxs[i].recv().unwrap();
            match msg {
                CrossShardMsg::DiscardedSenders(discarded_sender_msg) => {
                    discarded_senders_msgs[i] = discarded_sender_msg;
                }
                _ => panic!("Unexpected message"),
            }
        }
        discarded_senders_msgs
    }

    fn partition_block(&self, partition_msg: PartitionBlockMsg) {
        let PartitionBlockMsg {
            transactions,
            index_offset,
        } = partition_msg;
        let mut now = std::time::Instant::now();
        let num_shards = self.messages_txs.len();

        let mut conflict_detector = CrossShardConflictDetector::new(self.shard_id, num_shards, &transactions);
        self.broadcast_dependency_analysis(&conflict_detector);
        let dependency_analysis_msgs = self.collect_cross_shard_dependency_analysis();
        let discarded_sender_msg = conflict_detector
            .discard_conflicting_transactions(&transactions, &dependency_analysis_msgs);
        self.broadcast_discarded_senders_msg(&discarded_sender_msg);
        println!("Time taken for conflict detection: {:?} for shard_id {:?}", now.elapsed(), self.shard_id);
        now = std::time::Instant::now();
        let discarded_senders_msgs = self.collect_cross_shard_discarded_senders();

        let partitioning_status = conflict_detector
            .discard_discarded_sender_transactions(&transactions, &discarded_senders_msgs);
        println!("Time taken for discarding discarded sender: {:?} for shard_id {:?}", now.elapsed(), self.shard_id);

        now = std::time::Instant::now();
        // split the transaction into accepted and discarded statuses
        let mut accepted_txns: Vec<(usize, AnalyzedTransaction)> = Vec::new();
        let mut rejected_txns: Vec<(usize, AnalyzedTransaction)> = Vec::new();
        for (i, txn) in transactions.into_iter().enumerate() {
            if partitioning_status[i] == PartitioningStatus::Accepted {
                accepted_txns.push((index_offset + i, txn));
            } else {
                rejected_txns.push((index_offset + i, txn));
            }
        }
        println!("Time taken for splitting transactions: {:?} for shard_id {:?}", now.elapsed(), self.shard_id);

        // send the result back to the controller
        self.result_tx
            .send(PartitionedBlockResponse::new(accepted_txns, rejected_txns))
            .unwrap();
    }

    pub fn start(&self) {
        loop {
            let command = self.control_rx.recv().unwrap();
            match command {
                ControlMsg::PartitionBlock(msg) => {
                    self.partition_block(msg);
                },
                ControlMsg::Stop => {
                    break;
                },
            }
        }
        trace!("Shard {} is shutting down", self.shard_id);
    }
}
