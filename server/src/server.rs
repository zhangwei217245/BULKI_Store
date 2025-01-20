use std::net::SocketAddr;
use log::{debug, error, info};
use tokio::sync::oneshot;
use rsmpi::*;
use tonic::transport::Server;
use bulkistore_commons::proto::grpc_bulkistore_server::GrpcBulkistoreServer;
use bulkistore_commons::rpc::grpc::RXTXUtils;

mod datastore;
mod srvctx;
use srvctx::srvctx::ServerContext;

// ... (rest of the code remains the same)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let world = None;
    #[cfg(feature = "mpi")]
    {
        let (_universe, threading) = mpi::initialize_with_threading(Threading::Multiple).unwrap();
        let world = Some(_universe.world());
    }

    // Create and initialize server context
    let mut server_context = ServerContext::new();
    server_context.initialize(world).await?;

    info!(
        "BulkiStore server {} starting...",
        server_context.rank
    );

    // Start the RX endpoints (c2s and s2s)
    server_context.start_endpoints().await?;
    info!("Server endpoints started");

    // Shutdown all endpoints
    server_context.shutdown().await?;
    debug!("Server shutdown complete");

    Ok(())
}
