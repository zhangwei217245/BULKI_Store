use bulkistore_commons::common::RPCData;
use bulkistore_commons::proto::grpc_bulkistore_client::GrpcBulkistoreClient;
use bulkistore_commons::proto::{RequestMetadata, RpcRequest};
use mpi::topology::SimpleCommunicator;
use mpi::traits::*;
use rand::Rng;
use rmp_serde;
use std::env;
use std::fs;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;

use crate::cltctx::{BenchmarkStats, ClientContext};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize MPI with thread support
    let (universe, thread_level) = mpi::initialize_with_threading(mpi::Threading::Multiple)
        .ok_or("Failed to initialize MPI with threading support")?;
    println!("MPI thread level: {:?}", thread_level);
    
    let world = universe.world();
    
    // Create client context
    let context = ClientContext::new(world)?;
    println!("Client running on MPI process {}", context.get_rank());

    // Run benchmark with 1000 requests
    for i in 1..13 {
        let num_requests = 100;
        let data_size = 2_i32.pow(i as u32) as usize;
        let rpc_data = RPCData {
            func_name: if context.get_rank() % 2 == 0 {
                "times_three"
            } else {
                "times_two"
            }
            .to_string(),
            data: vec![1; data_size],
        };
    
        println!(
            "[Client {}] Sending RPC: func_name='{}', data.len={}",
            context.get_rank(),
            rpc_data.func_name,
            rpc_data.data.len()
        );
    
        let target_rank = context.get_rank() % context.get_server_addresses().len() as i32;
        // Send RPC request
        match context
            .send_rpc(target_rank as usize, rpc_data.clone())
            .await
        {
            Ok(result) => {
                println!(
                    "[Client {}] Received response : func_name='{}', data.len={}",
                    context.get_rank(),
                    result.func_name,
                    result.data.len()
                );
            }
            Err(e) => {
                eprintln!("[Client {}] Request failed: {}", context.get_rank(), e);
            }
        }

        let stats = context
            .benchmark_rpc(target_rank as usize, num_requests, rpc_data.clone())
            .await;
        println!(
            "Benchmark Result ({}) [Client {}]: reqs={} (success={}, fail={}), duration={}ms, lat_avg={:.2}ms, lat_min={}ms, lat_max={}ms, throughput={:.2} req/s",
            rpc_data.data.len(),
            context.get_rank(),
            stats.total_requests,
            stats.successful_requests,
            stats.failed_requests,
            stats.total_duration_ms,
            stats.avg_latency_ms,
            stats.min_latency_ms,
            stats.max_latency_ms,
            (stats.successful_requests as f64 * 1000.0) / stats.total_duration_ms as f64
        );
    }

    // Make sure MPI is properly finalized
    // drop(context);
    // drop(world);
    // drop(universe);
    Ok(())
}
