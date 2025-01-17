use bulkistore_commons::proto::grpc_bulkistore_server::GrpcBulkistoreServer;
use std::net::SocketAddr;
use tonic::transport::Server;

use log::{debug, info};
mod bench;
mod datastore;
mod srvctx;
use srvctx::srvctx::ServerContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize server context and get server address
    let (server_context, server_addr) = ServerContext::initialize()?;
    let addr = server_addr.parse::<SocketAddr>()?;

    println!(
        "BulkiStore server {} listening on {}",
        server_context.rank, addr
    );

    Server::builder()
        .add_service(GrpcBulkistoreServer::new(server_context))
        .serve(addr)
        .await?;

    Ok(())
}
