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

    println!("[Rank {}] Found available port: {}", rank, port);

    // Get hostname
    let hostname = hostname::get().expect("Failed to get hostname");

    // Create server info string
    let server_info = format!("{}|{}:{}", rank, hostname.into_string().unwrap(), port);
    println!("[Rank {}] Server info: {}", rank, server_info);

    // Convert string to bytes for MPI communication
    let mut info_bytes = vec![0u8; MAX_SERVER_ADDR_LEN];
    let server_info_bytes = server_info.as_bytes();
    info_bytes[..server_info_bytes.len()].copy_from_slice(server_info_bytes);

    // Create buffer to receive all server information
    let mut all_server_info = vec![0u8; MAX_SERVER_ADDR_LEN * size as usize];

    // All gather the fixed-size buffers
    world.all_gather_into(&info_bytes[..], &mut all_server_info);

    // Process the received data into server addresses
    let mut server_addresses = vec![String::new(); size as usize];
    for i in 0..size as usize {
        let start = i * MAX_SERVER_ADDR_LEN;
        let end = start + MAX_SERVER_ADDR_LEN;
        let slice = &all_server_info[start..end];

        // Find the actual length of the string (until first 0)
        let actual_len = slice
            .iter()
            .position(|&x| x == 0)
            .unwrap_or(MAX_SERVER_ADDR_LEN);

        if let Ok(info) = String::from_utf8(slice[..actual_len].to_vec()) {
            let parts: Vec<&str> = info.splitn(2, '|').collect();
            if parts.len() == 2 {
                server_addresses[i] = parts[1].to_string();
            }
        }
    }

    println!("[Rank {}] Collected server addresses:", rank);
    for (r, addr) in server_addresses.iter().enumerate() {
        println!("[Rank {}]   Rank {} -> {}", rank, r, addr);
    }

    // Rank 0 writes the server information to a file
    if rank == 0 {
        let pdc_tmp_dir = ServerContext::get_pdc_tmp_dir();
        fs::create_dir_all(&pdc_tmp_dir)?;
        let server_list_path = pdc_tmp_dir.join("server_list");
        let mut file = File::create(server_list_path.clone())?;
        for (rank, addr) in server_addresses.iter().enumerate() {
            writeln!(file, "{}|{}", rank, addr)?;
        }
        println!(
            "[Rank 0] Writing server list to: {}",
            server_list_path.display()
        );
    }

    // Create and start the gRPC server
    let addr = format!("0.0.0.0:{}", port).parse::<SocketAddr>()?;
    let mut server_context = ServerContext::new(world)?;
    server_context.server_addresses = server_addresses;

    println!("BulkiStore server {} listening on {}", rank, addr);












// use bulkistore_commons::proto::grpc_bulkistore_server::GrpcBulkistoreServer;
// use std::net::SocketAddr;
// use tonic::transport::Server;

// mod datastore;
// mod srvctx;
// use srvctx::srvctx::ServerContext;

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // Initialize server context and get server address
//     let (server_context, server_addr) = ServerContext::initialize()?;
//     let addr = server_addr.parse::<SocketAddr>()?;

//     println!(
//         "BulkiStore server {} listening on {}",
//         server_context.rank, addr
//     );

    // Create a ready file to signal that this server is up
    let ready_file = std::env::var("SERVER_READY_FILE")
        .unwrap_or_else(|_| format!("/tmp/bulki_server_{}_ready", rank));
    std::fs::write(&ready_file, format!("Server {} ready on {}", rank, addr))
        .expect("Failed to create ready file");

    // Create a signal handler for graceful shutdown
    let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = Server::builder()
        .add_service(GrpcBulkistoreServer::new(server_context))
        .serve(addr)
        .await?;

    Ok(())
}
