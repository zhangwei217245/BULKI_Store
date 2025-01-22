use log::{debug, info};
mod bench;
mod datastore;
mod health;
mod srvctx;
use srvctx::ServerContext;

// ... (rest of the code remains the same)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create and initialize server context
    let mut server_context = ServerContext::new();
    server_context.initialize().await?;

    info!("BulkiStore server {} starting...", server_context.rank);

    // Start the RX endpoints (c2s and s2s)
    server_context.start_endpoints().await?;
    info!("Server endpoints started");

    // Shutdown all endpoints
    server_context.shutdown().await?;
    debug!("Server shutdown complete");

    Ok(())
}
