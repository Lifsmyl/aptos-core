// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    counters::{
        GOT_CONNECTION, LATEST_PROCESSED_VERSION, PROCESSOR_ERRORS, PROCESSOR_INVOCATIONS,
        PROCESSOR_SUCCESSES, UNABLE_TO_GET_CONNECTION,
    },
    database::{execute_with_better_error, PgDbPool, PgPoolConnection},
    models::processor_status::ProcessorStatus,
    schema::processor_status,
};
use aptos_protos::transaction::testing1::v1::Transaction as ProtoTransaction;
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, prelude::*};
use std::fmt::Debug;

pub type ProcessingResult = Vec<ProcessorStatus>;

/// The `TransactionProcessor` is used by an instance of a `Tailer` to process transactions
#[async_trait]
pub trait TransactionProcessor: Send + Sync + Debug {
    /// name of the processor, for status logging
    /// This will get stored in the database for each (`TransactionProcessor`, transaction_version) pair
    fn name(&self) -> &'static str;

    /// Process all transactions within a block and processes it. This method will be called from `process_transaction_with_status`
    /// In case a transaction cannot be processed, we will fail the entire block.
    async fn process_transactions(
        &self,
        transactions: Vec<ProtoTransaction>,
        start_version: u64,
        end_version: u64,
    ) -> Result<ProcessingResult, TransactionProcessingError>;

    /// Gets a reference to the connection pool
    /// This is used by the `get_conn()` helper below
    fn connection_pool(&self) -> &PgDbPool;

    //* Below are helper methods that don't need to be implemented *//

    /// Gets the connection.
    /// If it was unable to do so (default timeout: 30s), it will keep retrying until it can.
    fn get_conn(&self) -> PgPoolConnection {
        let pool = self.connection_pool();
        loop {
            match pool.get() {
                Ok(conn) => {
                    GOT_CONNECTION.inc();
                    return conn;
                },
                Err(err) => {
                    UNABLE_TO_GET_CONNECTION.inc();
                    aptos_logger::error!(
                        "Could not get DB connection from pool, will retry in {:?}. Err: {:?}",
                        pool.connection_timeout(),
                        err
                    );
                },
            };
        }
    }

    /// Store last processed version from database. We can assume that all previously processed
    /// versions are successful because any gap would cause the processor to panic
    async fn update_last_processed_version(&self, version: u64) -> anyhow::Result<()> {
        let mut conn = self.get_conn();
        let status = ProcessorStatus {
            processor: self.name().to_string(),
            last_success_version: version as i64,
        };
        execute_with_better_error(
            &mut conn,
            diesel::insert_into(processor_status::table)
                .values(&status)
                .on_conflict(processor_status::processor)
                .do_update()
                .set((
                    processor_status::last_success_version
                        .eq(excluded(processor_status::last_success_version)),
                    processor_status::last_updated.eq(excluded(processor_status::last_updated)),
                )),
            Some(" WHERE processor_status.last_success_version <= EXCLUDED.last_success_version "),
        )?;
        Ok(())
    }
}
