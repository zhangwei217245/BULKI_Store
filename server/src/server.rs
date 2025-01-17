use std::{
    env,
    fs::{self, File},
    io::Write,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use bulkistore_commons::common::RPCData;
use bulkistore_commons::proto::grpc_bulkistore_server::{GrpcBulkistore, GrpcBulkistoreServer};
use bulkistore_commons::proto::{ResponseMetadata, RpcRequest, RpcResponse};
use hostname::get as get_hostname;
use mpi::traits::*;
use rmp_serde;
use tonic::{transport::Server, Request, Response, Status};

mod datastore;
use datastore::bulki_store::{BulkiStore, Dispatchable};

const DEFAULT_BASE_PORT: u16 = 50051;
const MAX_PORT_ATTEMPTS: u16 = 1000;
const MAX_SERVER_ADDR_LEN: usize = 256;

pub struct ServerContext {
    rank: i32,
    size: i32,
    #[allow(unused)]
    world: mpi::topology::SimpleCommunicator,
    #[allow(unused)]
    hostname: String,
    server_addresses: Vec<String>,
    store: BulkiStore,
}

impl ServerContext {
    pub fn new(
        world: mpi::topology::SimpleCommunicator,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rank = world.rank();
        let size = world.size();
        let hostname = get_hostname()?.into_string().unwrap();

        Ok(Self {
            rank,
            size,
            world,
            hostname,
            server_addresses: Vec::new(),
            store: BulkiStore::new(),
        })
    }

    fn get_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn find_available_port(base_port: u16) -> Option<u16> {
        (base_port..base_port + MAX_PORT_ATTEMPTS)
            .find(|&port| TcpListener::bind(format!("0.0.0.0:{}", port)).is_ok())
    }

    fn get_pdc_tmp_dir() -> PathBuf {
        env::var("PDC_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".pdc_tmp"))
    }
}

#[tonic::async_trait]
impl GrpcBulkistore for ServerContext {
    async fn process_request(
        &self,
        request: Request<RpcRequest>,
    ) -> Result<Response<RpcResponse>, Status> {
        let request_inner = request.into_inner();

        // Get metadata from request
        let metadata = request_inner.metadata.unwrap_or_default();
        println!(
            "[Server {}] Received request {} from client {} (world size: {})",
            self.rank, metadata.request_id, metadata.client_rank, metadata.client_world_size
        );

        // Try to deserialize the binary data as RPCData
        match rmp_serde::from_slice::<RPCData>(&request_inner.binary_data) {
            Ok(rpc_data) => {
                println!(
                    "[Server {}] Processing RPC call: func_name='{}', data.len={}",
                    self.rank, rpc_data.func_name, rpc_data.data.len()
                );

                // Use BulkiStore's dispatch method
                let result_data = match self.store.dispatch(&rpc_data.func_name, &rpc_data.data) {
                    Some(result) => {
                        // Create result RPCData
                        let result_rpc = RPCData {
                            func_name: format!("{}_result", rpc_data.func_name),
                            data: result,
                        };

                        // Serialize the result
                        rmp_serde::to_vec(&result_rpc).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?
                    }
                    None => {
                        return Ok(Response::new(RpcResponse {
                            status: 1,
                            error_message: format!("Unknown function: {}", rpc_data.func_name),
                            result_data: Vec::new(),
                            metadata: None,
                        }));
                    }
                };

                Ok(Response::new(RpcResponse {
                    status: 0,
                    error_message: String::new(),
                    result_data: result_data,
                    metadata: Some(ResponseMetadata {
                        server_rank: self.rank,
                        server_world_size: self.size,
                        received_at_ms: metadata.timestamp_ms,
                        processed_at_ms: Self::get_timestamp_ms(),
                        request_id: metadata.request_id,
                    }),
                }))
            }
            Err(e) => Ok(Response::new(RpcResponse {
                status: 1,
                error_message: format!("Failed to deserialize request data: {}", e),
                result_data: Vec::new(),
                metadata: None,
            })),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize MPI
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();

    // Find available port
    let port = ServerContext::find_available_port(DEFAULT_BASE_PORT)
        .expect("Could not find available port");

    println!("[Rank {}] Found available port: {}", rank, port);

    // Get hostname
    let hostname = get_hostname().expect("Failed to get hostname");

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

    Server::builder()
        .add_service(GrpcBulkistoreServer::new(server_context))
        .serve(addr)
        .await?;

    Ok(())
}
