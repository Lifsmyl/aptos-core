// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use std::collections::HashSet;

pub type ShardId = usize;
pub type TxnIndex = usize;

#[derive(Default, Debug, Clone)]
pub struct CrossShardDependencies {
    depends_on: HashSet<TxnIndex>,
    // TODO (skedia) add support for this.
    _dependents: HashSet<TxnIndex>,
}

impl CrossShardDependencies {
    pub fn len(&self) -> usize {
        self.depends_on.len()
    }

    pub fn is_empty(&self) -> bool {
        self.depends_on.is_empty()
    }

    pub fn is_depends_on(&self, txn_index: TxnIndex) -> bool {
        self.depends_on.contains(&txn_index)
    }

    pub fn add_depends_on_txn(&mut self, txn_index: TxnIndex) {
        self.depends_on.insert(txn_index);
    }
}

#[derive(Debug, Clone)]
/// A contiguous chunk of transactions (along with their dependencies) in a block.
pub struct TransactionsChunk {
    // This is the index of first transaction relative to the block.
    pub start_index: TxnIndex,
    pub transactions: Vec<TransactionWithDependencies>,
}

impl TransactionsChunk {
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

    pub fn transactions_with_deps(&self) -> &Vec<TransactionWithDependencies> {
        &self.transactions
    }
}

#[derive(Debug, Clone)]
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
