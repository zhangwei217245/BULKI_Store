use std::sync::Arc;

use log::{debug, info};
mod bench;
mod datastore;
mod health;
mod srvctx;
use anyhow::Result;
use env_logger;
use srvctx::ServerContext;
use std::time::Instant;

fn close_resources() -> Result<()> {
    // TODO: calling resource close function
    let timer = Instant::now();
    println!("Closing resources, checkpointing data...");
    let checkpoint_result = datastore::dump_memory_store();
    println!(
        "Finished checkpointing in {} seconds",
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
    server_context.initialize(universe).await?;

    info!("BulkiStore server {} starting...", server_context.rank);

    info!("Server rank: {}", server_context.rank);
    info!("Server size: {}", server_context.size);
    info!("Loading Data...");
    let timer = Instant::now();
    datastore::load_memory_store()?;
    info!("Data loaded in {} seconds", timer.elapsed().as_secs());

    // Start the RX endpoints (c2s and s2s)
    server_context.start_endpoints().await?;
    info!("Server endpoints started");

    // Wait for either Ctrl+C or SIGTERM
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C signal");
                // Shutdown all endpoints
                server_context.shutdown(|| async { close_resources() }).await?;
                debug!("Server shutdown complete");
            }
            _ = terminate.recv() => {
                info!("Received SIGTERM signal");
                // Shutdown all endpoints
                server_context.shutdown(|| async { close_resources() }).await?;
                debug!("Server shutdown complete");
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        info!("Received Ctrl+C signal");
        // Shutdown all endpoints
        server_context
            .shutdown(|| async { close_resources() })
            .await?;
        debug!("Server shutdown complete");
    }

    Ok(())
}
