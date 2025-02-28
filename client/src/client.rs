mod cltctx;
use std::sync::Arc;

use cltctx::ClientContext;
// use commons::rpc::{MessageType, RPCData, RPCMetadata};
use log::debug;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "mpi")]
    let universe = {
        let (universe, _) = mpi::initialize_with_threading(mpi::Threading::Multiple).unwrap();
        Some(Arc::new(universe))
    };

    #[cfg(not(feature = "mpi"))]
    let universe = None;

    let mut context = ClientContext::new();
    context.initialize(universe).await?;
    debug!("Client running on MPI process {}", context.get_rank());

    let num_requests = 1000;
    let data_len = 1000;
    context
        .benchmark_rpc(num_requests, data_len)
        .await?
        .print_stats();
    Ok(())
}
