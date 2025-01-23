use std::sync::Arc;

use log::{debug, info};
mod bench;
mod datastore;
mod health;
mod srvctx;
use anyhow::Result;
use srvctx::ServerContext;

fn close_resources() -> Result<()> {
    // TODO: calling resource close functions
    println!("Closing resources");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
