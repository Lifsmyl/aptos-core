// Copyright © Aptos Foundation

use aptos_block_partitioner::{
    sharded_block_partitioner::ShardedBlockPartitioner,
    test_utils::{create_signed_p2p_transaction, generate_test_account, TestAccount},
};
use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use clap::Parser;
use rand::{rngs::OsRng, Rng};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    sync::Mutex,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value = "2000000")]
    pub num_accounts: usize,

    #[clap(long, default_value = "100000")]
    pub block_size: usize,

    #[clap(long, default_value = "10")]
    pub num_blocks: usize,

    #[clap(long, default_value = "12")]
    pub num_shards: usize,
}

fn main() {
    println!("Starting the block partitioning benchmark");
    let args = Args::parse();
    let num_accounts = args.num_accounts;
    println!("Creating {} accounts", num_accounts);
    let accounts: Vec<Mutex<TestAccount>> = (0..num_accounts)
        .into_par_iter()
        .map(|_i| Mutex::new(generate_test_account()))
        .collect();
    println!("Created {} accounts", num_accounts);
    println!("Creating {} transactions", args.block_size);
    let transactions: Vec<AnalyzedTransaction> = (0..args.block_size)
        .into_iter()
        .map(|_| {
            // randomly select a sender and receiver from accounts
            let mut rng = OsRng;
            let sender_index = rng.gen_range(0, num_accounts);
            let receiver_index = rng.gen_range(0, num_accounts);
            let receiver = accounts[receiver_index].lock().unwrap();
            let mut sender = accounts[sender_index].lock().unwrap();
            create_signed_p2p_transaction(&mut sender, vec![&receiver]).remove(0)
        })
        .collect();

    // profile the time taken
    let partitioner = ShardedBlockPartitioner::new(args.num_shards);
    for _ in 0..args.num_blocks {
        let transactions = transactions.clone();
        println!("Starting to partition");
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to retrieve current time")
            .as_millis();
        // print current time
        println!("Current time: {:?}", current_time);
        let now = Instant::now();
        partitioner.partition(transactions);
        let elapsed = now.elapsed();
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to retrieve current time")
            .as_millis();
        println!("partition end time from main {:?}", current_time);
        println!("Time taken to partition: {:?}", elapsed);
    }
}
