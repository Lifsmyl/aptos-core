// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::sharded_block_partitioner::{
    dependency_analyzer::RWSetWithTxnIndex,
    messages::{
        AddTxnsWithCrossShardDep, ControlMsg,
        ControlMsg::{AddCrossShardDepReq, FilterCrossShardDepReq},
        CrossShardMsg, FilterTxnsWithCrossShardDep, PartitioningBlockResponse,
    },
    partitioning_shard::PartitioningShard,
};
use aptos_logger::error;
use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use std::{
    collections::HashMap,
    sync::{
        mpsc::{Receiver, Sender},
        Arc,
    },
    thread,
};

mod conflict_detector;
pub mod dependency_analyzer;
mod messages;
mod partitioning_shard;

pub type ShardId = usize;
pub type TxnIndex = usize;

#[derive(Default, Debug)]
pub struct CrossShardDependencies {
    pub depends_on: Vec<TxnIndex>,
}

impl CrossShardDependencies {
    pub fn len(&self) -> usize {
        self.depends_on.len()
    }

    pub fn is_empty(&self) -> bool {
        self.depends_on.is_empty()
    }

    pub fn add_depends_on_txn(&mut self, txn_index: TxnIndex) {
        self.depends_on.push(txn_index);
    }
}

#[derive(Debug)]
/// A contiguous chunk of transactions (along with their dependencies) in a block.
pub struct TransactionChunk {
    pub start_index: TxnIndex,
    pub transactions: Vec<TransactionWithDependencies>,
}

impl TransactionChunk {
    pub fn new(start_index: TxnIndex, transactions: Vec<TransactionWithDependencies>) -> Self {
        Self {
            start_index,
            transactions,
        }
    }

    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }
}

#[derive(Debug)]
pub struct TransactionWithDependencies {
    pub txn: AnalyzedTransaction,
    pub cross_shard_dependencies: CrossShardDependencies,
}

impl TransactionWithDependencies {
    pub fn new(txn: AnalyzedTransaction, cross_shard_dependencies: CrossShardDependencies) -> Self {
        Self {
            txn,
            cross_shard_dependencies,
        }
    }

    #[cfg(test)]
    pub fn txn(&self) -> &AnalyzedTransaction {
        &self.txn
    }

    #[cfg(test)]
    pub fn cross_shard_dependencies(&self) -> &CrossShardDependencies {
        &self.cross_shard_dependencies
    }
}

pub struct ShardedBlockPartitioner {
    num_shards: usize,
    control_txs: Vec<Sender<ControlMsg>>,
    result_rxs: Vec<Receiver<PartitioningBlockResponse>>,
    shard_threads: Vec<thread::JoinHandle<()>>,
}

impl ShardedBlockPartitioner {
    pub fn new(num_shards: usize) -> Self {
        println!(
            "Creating a new sharded block partitioner with {} shards",
            num_shards
        );
        assert!(num_shards > 0, "num_executor_shards must be > 0");
        // create channels for cross shard messages across all shards. This is a full mesh connection.
        // Each shard has a vector of channels for sending messages to other shards and
        // a vector of channels for receiving messages from other shards.
        let mut messages_txs = vec![];
        let mut messages_rxs = vec![];
        for _ in 0..num_shards {
            messages_txs.push(vec![]);
            messages_rxs.push(vec![]);
            for _ in 0..num_shards {
                let (messages_tx, messages_rx) = std::sync::mpsc::channel();
                messages_txs.last_mut().unwrap().push(messages_tx);
                messages_rxs.last_mut().unwrap().push(messages_rx);
            }
        }
        let mut control_txs = vec![];
        let mut result_rxs = vec![];
        let mut shard_join_handles = vec![];
        for (i, message_rxs) in messages_rxs.into_iter().enumerate() {
            let (control_tx, control_rx) = std::sync::mpsc::channel();
            let (result_tx, result_rx) = std::sync::mpsc::channel();
            control_txs.push(control_tx);
            result_rxs.push(result_rx);
            shard_join_handles.push(spawn_partitioning_shard(
                i,
                control_rx,
                result_tx,
                message_rxs,
                messages_txs.iter().map(|txs| txs[i].clone()).collect(),
            ));
        }
        Self {
            num_shards,
            control_txs,
            result_rxs,
            shard_threads: shard_join_handles,
        }
    }

    // reorders the transactions so that transactions from the same sender always go to the same shard.
    // This places transactions from the same sender next to each other, which is not optimal for parallelism.
    // TODO(skedia): Improve this logic to shuffle senders
    pub fn reorder_txns_by_senders(
        &self,
        txns: Vec<AnalyzedTransaction>,
    ) -> Vec<Vec<AnalyzedTransaction>> {
        let approx_txns_per_shard = (txns.len() as f64 / self.num_shards as f64).ceil() as usize;
        let mut sender_to_txns = HashMap::new();
        let mut sender_order = Vec::new(); // Track sender ordering

        for txn in txns {
            let sender = txn.sender().unwrap();
            if !sender_to_txns.contains_key(&sender) {
                sender_order.push(sender); // Add sender to the order vector
            }
            sender_to_txns
                .entry(sender)
                .or_insert_with(Vec::new)
                .push(txn);
        }

        let mut result = Vec::new();
        result.push(Vec::new());

        for sender in sender_order {
            let txns = sender_to_txns.remove(&sender).unwrap();
            let txns_in_shard = result.last().unwrap().len();

            if txns_in_shard < approx_txns_per_shard {
                result.last_mut().unwrap().extend(txns);
            } else {
                result.push(txns);
            }
        }

        // pad the rest of the shard with empty txns
        for _ in result.len()..self.num_shards {
            result.push(Vec::new());
        }

        result
    }

    fn send_partition_msgs(&self, partition_msg: Vec<ControlMsg>) {
        for (i, msg) in partition_msg.into_iter().enumerate() {
            self.control_txs[i].send(msg).unwrap();
        }
    }

    fn collect_partition_block_response(
        &self,
    ) -> (
        Vec<TransactionChunk>,
        Vec<RWSetWithTxnIndex>,
        Vec<Vec<AnalyzedTransaction>>,
    ) {
        let mut frozen_chunks = Vec::new();
        let mut frozen_rw_set_with_index = Vec::new();
        let mut rejected_txns_vec = Vec::new();
        for rx in &self.result_rxs {
            let PartitioningBlockResponse {
                frozen_chunk,
                rw_set_with_index,
                rejected_txns,
            } = rx.recv().unwrap();
            frozen_chunks.push(frozen_chunk);
            frozen_rw_set_with_index.push(rw_set_with_index);
            rejected_txns_vec.push(rejected_txns);
        }
        (frozen_chunks, frozen_rw_set_with_index, rejected_txns_vec)
    }

    pub fn partition(&self, transactions: Vec<AnalyzedTransaction>) -> Vec<TransactionChunk> {
        let total_txns = transactions.len();
        if total_txns == 0 {
            return vec![];
        }

        // First round, we filter all transactions with cross-shard dependencies
        let partitioned_txns = self.reorder_txns_by_senders(transactions);
        let partition_block_msgs = partitioned_txns
            .into_iter()
            .map(|txns| {
                FilterCrossShardDepReq(FilterTxnsWithCrossShardDep::new(
                    txns,
                    Arc::new(vec![]),
                    Arc::new(vec![]),
                ))
            })
            .collect::<Vec<ControlMsg>>();
        self.send_partition_msgs(partition_block_msgs);
        let (frozen_chunks, frozen_rw_set_with_index, rejected_txns_vecs) =
            self.collect_partition_block_response();

        // Second round, we don't filter transactions, we just add cross shard dependencies for
        // remaining transactions.
        let mut index_offset = frozen_chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
        let round_one_frozen_chunks_arc = Arc::new(frozen_chunks);
        let frozen_rw_set_with_index_arc = Arc::new(frozen_rw_set_with_index);

        let partition_block_msgs = rejected_txns_vecs
            .into_iter()
            .map(|rejected_txns| {
                let rejected_txns_len = rejected_txns.len();
                let partitioning_msg = AddCrossShardDepReq(AddTxnsWithCrossShardDep::new(
                    rejected_txns,
                    index_offset,
                    round_one_frozen_chunks_arc.clone(),
                    frozen_rw_set_with_index_arc.clone(),
                ));
                index_offset += rejected_txns_len;
                partitioning_msg
            })
            .collect::<Vec<ControlMsg>>();
        self.send_partition_msgs(partition_block_msgs);
        let (round_two_frozen_chunks, _, _) = self.collect_partition_block_response();

        Arc::try_unwrap(round_one_frozen_chunks_arc)
            .unwrap()
            .into_iter()
            .chain(round_two_frozen_chunks.into_iter())
            .collect::<Vec<TransactionChunk>>()
    }
}

impl Drop for ShardedBlockPartitioner {
    /// Best effort stops all the executor shards and waits for the thread to finish.
    fn drop(&mut self) {
        // send stop command to all executor shards
        for control_tx in self.control_txs.iter() {
            if let Err(e) = control_tx.send(ControlMsg::Stop) {
                error!("Failed to send stop command to executor shard: {:?}", e);
            }
        }

        // wait for all executor shards to stop
        for shard_thread in self.shard_threads.drain(..) {
            shard_thread.join().unwrap_or_else(|e| {
                error!("Failed to join executor shard thread: {:?}", e);
            });
        }
    }
}

fn spawn_partitioning_shard(
    shard_id: ShardId,
    control_rx: Receiver<ControlMsg>,
    result_tx: Sender<PartitioningBlockResponse>,
    message_rxs: Vec<Receiver<CrossShardMsg>>,
    messages_txs: Vec<Sender<CrossShardMsg>>,
) -> thread::JoinHandle<()> {
    // create and start a new executor shard in a separate thread
    thread::Builder::new()
        .name(format!("partitioning-shard-{}", shard_id))
        .spawn(move || {
            let partitioning_shard =
                PartitioningShard::new(shard_id, control_rx, result_tx, message_rxs, messages_txs);
            partitioning_shard.start();
        })
        .unwrap()
}

#[cfg(test)]
mod tests {
    use crate::{
        sharded_block_partitioner::ShardedBlockPartitioner,
        test_utils::{
            create_non_conflicting_p2p_transaction, create_signed_p2p_transaction,
            generate_test_account, TestAccount,
        },
    };

    // fn verify_no_cross_shard_dependency(partitioned_txns: Vec<Vec<TransactionWithDependencies>>) {
    //     for txns in partitioned_txns {
    //         for txn in txns {
    //             assert_eq!(txn.cross_shard_dependencies().len(), 0);
    //         }
    //     }
    // }

    #[test]
    // Test that the partitioner works correctly for a single sender and multiple receivers.
    // In this case the expectation is that only the first shard will contain transactions and all
    // other shards will be empty.
    fn test_single_sender_txns() {
        let mut sender = generate_test_account();
        let mut receivers = Vec::new();
        let num_txns = 10;
        for _ in 0..num_txns {
            receivers.push(generate_test_account());
        }
        let transactions = create_signed_p2p_transaction(
            &mut sender,
            receivers.iter().collect::<Vec<&TestAccount>>(),
        );
        let partitioner = ShardedBlockPartitioner::new(4);
        let partitioned_txns = partitioner.partition(transactions.clone());
        assert_eq!(partitioned_txns.len(), 4);
        // The first shard should contain all the transactions
        assert_eq!(partitioned_txns[0].len(), num_txns);
        // The rest of the shards should be empty
        for txns in partitioned_txns.iter().take(4).skip(1) {
            assert_eq!(txns.len(), 0);
        }
        // Verify that the transactions are in the same order as the original transactions and cross shard
        // dependencies are empty.
        for (i, txn) in partitioned_txns[0].transactions.iter().enumerate() {
            assert_eq!(txn.txn(), &transactions[i]);
            assert_eq!(txn.cross_shard_dependencies().len(), 0);
        }
    }

    #[test]
    // Test that the partitioner works correctly for no conflict transactions. In this case, the
    // expectation is that no transaction is reordered.
    fn test_non_conflicting_txns() {
        let num_txns = 4;
        let num_shards = 2;
        let mut transactions = Vec::new();
        for _ in 0..num_txns {
            transactions.push(create_non_conflicting_p2p_transaction())
        }
        let partitioner = ShardedBlockPartitioner::new(num_shards);
        let partitioned_txns = partitioner.partition(transactions.clone());
        assert_eq!(partitioned_txns.len(), num_shards);
        // Verify that the transactions are in the same order as the original transactions and cross shard
        // dependencies are empty.
        let mut current_index = 0;
        for analyzed_txns in partitioned_txns.into_iter() {
            assert_eq!(analyzed_txns.len(), num_txns / num_shards);
            for txn in analyzed_txns.transactions.iter() {
                assert_eq!(txn.txn(), &transactions[current_index]);
                assert_eq!(txn.cross_shard_dependencies().len(), 0);
                current_index += 1;
            }
        }
    }

    #[test]
    fn test_same_sender_in_one_shard() {
        let num_shards = 3;
        let mut sender = generate_test_account();
        let mut txns_from_sender = Vec::new();
        for _ in 0..5 {
            txns_from_sender.push(
                create_signed_p2p_transaction(&mut sender, vec![&generate_test_account()])
                    .remove(0),
            );
        }
        let mut non_conflicting_transactions = Vec::new();
        for _ in 0..5 {
            non_conflicting_transactions.push(create_non_conflicting_p2p_transaction());
        }

        let mut transactions = Vec::new();
        let mut txn_from_sender_index = 0;
        let mut non_conflicting_txn_index = 0;
        transactions.push(non_conflicting_transactions[non_conflicting_txn_index].clone());
        non_conflicting_txn_index += 1;
        transactions.push(txns_from_sender[txn_from_sender_index].clone());
        txn_from_sender_index += 1;
        transactions.push(txns_from_sender[txn_from_sender_index].clone());
        txn_from_sender_index += 1;
        transactions.push(non_conflicting_transactions[non_conflicting_txn_index].clone());
        non_conflicting_txn_index += 1;
        transactions.push(txns_from_sender[txn_from_sender_index].clone());
        txn_from_sender_index += 1;
        transactions.push(txns_from_sender[txn_from_sender_index].clone());
        txn_from_sender_index += 1;
        transactions.push(non_conflicting_transactions[non_conflicting_txn_index].clone());
        transactions.push(txns_from_sender[txn_from_sender_index].clone());

        let partitioner = ShardedBlockPartitioner::new(num_shards);
        let partitioned_txns = partitioner.partition(transactions.clone());
        assert_eq!(partitioned_txns.len(), num_shards);
        assert_eq!(partitioned_txns[0].len(), 6);
        assert_eq!(partitioned_txns[1].len(), 2);
        assert_eq!(partitioned_txns[2].len(), 0);

        // verify that all transactions from the sender end up in shard 0
        // for (index, txn) in txns_from_sender.iter().enumerate() {
        //     assert_eq!(partitioned_txns[0][index + 1].txn(), txn);
        // }
        // verify_no_cross_shard_dependency(partitioned_txns);
    }

    #[test]
    fn test_cross_shard_dependencies() {
        let num_shards = 3;
        let mut account1 = generate_test_account();
        let mut account2 = generate_test_account();
        let mut account3 = generate_test_account();
        let mut account4 = generate_test_account();

        let transactions = vec![
            // Should go in shard 0
            create_signed_p2p_transaction(&mut account1, vec![&account2]).remove(0),
            create_signed_p2p_transaction(&mut account1, vec![&account3]).remove(0),
            create_signed_p2p_transaction(&mut account2, vec![&account4]).remove(0),
            // Should go in shard 1
            create_signed_p2p_transaction(&mut account3, vec![&account4]).remove(0),
            create_signed_p2p_transaction(&mut account3, vec![&account1]).remove(0),
            create_signed_p2p_transaction(&mut account3, vec![&account2]).remove(0),
            // Should go in shard 2
            create_signed_p2p_transaction(&mut account4, vec![&account1]).remove(0),
            create_signed_p2p_transaction(&mut account4, vec![&account2]).remove(0),
        ];

        let partitioner = ShardedBlockPartitioner::new(num_shards);
        let partitioned_txns = partitioner.partition(transactions);
        assert_eq!(partitioned_txns.len(), num_shards);
    }
}
