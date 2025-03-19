use std::sync::Arc;

use log::{debug, info};
mod bench;
mod datastore;
mod health;
mod srvctx;
use anyhow::Result;
use env_logger;
use srvctx::{get_rank, get_size, ServerContext};
use std::time::Instant;

fn close_resources() -> Result<()> {
    let timer = Instant::now();
    info!(
        "[R{}/S{}] Closing resources, checkpointing data...",
        get_rank(),
        get_size()
    );
    let checkpoint_result = datastore::dump_memory_store();
    info!(
        "[R{}/S{}] Finished checkpointing in {} seconds",
        get_rank(),
        get_size(),
        timer.elapsed().as_secs()
    );
    return checkpoint_result;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    #[cfg(feature = "mpi")]
    let universe = {
        let (universe, _) = mpi::initialize_with_threading(mpi::Threading::Multiple).unwrap();
        Some(Arc::new(universe))
    };

    #[cfg(not(feature = "mpi"))]
    let universe = None;

    // Create and initialize server context
    let mut server_context = ServerContext::new();

    // Initialize with MPI universe - critical for Perlmutter
    server_context.initialize(universe).await?;
    info!("[R{}/S{}] Server initialized", get_rank(), get_size());

    // Start the RX endpoints before wrapping in Arc
    futures::executor::block_on(async { server_context.start_endpoints().await })?;
    info!("[R{}/S{}] Server endpoints started", get_rank(), get_size());

    info!("[R{}/S{}] Loading Data...", get_rank(), get_size());
    let timer = Instant::now();
    datastore::load_memory_store()?;
    info!(
        "[R{}/S{}] Data loaded in {} seconds",
        get_rank(),
        get_size(),
        timer.elapsed().as_secs()
    );

    // Handle shutdown signals with proper MPI cleanup
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("[R{}/S{}] Received Ctrl+C signal", get_rank(), get_size());
                server_context.shutdown(|| async { close_resources() }).await?;
                debug!("[R{}/S{}] Server shutdown complete", get_rank(), get_size());
            }
            _ = terminate.recv() => {
                info!("[R{}/S{}] Received SIGTERM signal", get_rank(), get_size());
                server_context.shutdown(|| async { close_resources() }).await?;
                debug!("[R{}/S{}] Server shutdown complete", get_rank(), get_size());
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        info!("[R{}/S{}] Received Ctrl+C signal", get_rank(), get_size());
        server_context
            .shutdown(|| async { close_resources() })
            .await?;
        debug!("[R{}/S{}] Server shutdown complete", get_rank(), get_size());
    }

    Ok(())
}
