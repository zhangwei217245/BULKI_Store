mod cltctx;
use std::sync::Arc;

use crate::cltctx::{BenchmarkStats, ClientContext};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "mpi")]
    let universe = {
        let (universe, _) = mpi::initialize_with_threading(mpi::Threading::Multiple).unwrap();
        Some(Arc::new(universe))
    };

    #[cfg(not(feature = "mpi"))]
    let universe = None;

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
