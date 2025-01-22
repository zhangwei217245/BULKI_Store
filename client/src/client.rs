mod cltctx;
use cltctx::ClientContext;
use commons::rpc::{MessageType, RPCData, RPCMetadata};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut context = ClientContext::new();
    context.initialize().await?;
    println!("Client running on MPI process {}", context.get_rank());

    // Run benchmark with 1000 requests
    for i in 1..13 {
        let num_requests = 100;
        let data_size = 2_i32.pow(i as u32) as usize;

        let handler_name = if context.get_rank() % 2 == 0 {
            "times_three"
        } else {
            "times_two"
        }
        .to_string();

        let data = vec![1; data_size];

        let target_rank = context.get_rank() % context.get_server_count();
        // Send RPC request
        match context
            .send_message(target_rank as usize, handler_name.clone(), data.as_slice())
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

    let num_requests = 1000;
    let data_len = 1000;
    context
        .benchmark_rpc(num_requests, data_len)
        .await?
        .print_stats();
    Ok(())
}
