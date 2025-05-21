mod cltctx;
use std::sync::Arc;

use cltctx::ClientContext;
// use commons::rpc::{MessageType, RPCData, RPCMetadata};
use log::debug;
use mpi::traits::Communicator;

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
    let (rank, size) = match &universe {
        Some(universe) => (
            Some(universe.world().rank() as u32),
            Some(universe.world().size() as u32),
        ),
        None => (Some(0), Some(1)),
    };

    context.initialize(universe, rank, size).await?;
    debug!("Client running on MPI process {}", context.get_rank());

    // let num_requests = 1000;
    // let data_len = 1000;
    // context
    //     .benchmark_rpc(num_requests, data_len)
    //     .await?
    //     .print_stats();
    Ok(())
}
