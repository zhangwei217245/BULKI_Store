use log::{debug, error, info, warn};
use std::{
    env,
    fs::{self, File},
    io::Write,
    net::TcpListener,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use bulkistore_commons::common::RPCData;
use bulkistore_commons::proto::grpc_bulkistore_server::GrpcBulkistore;
use bulkistore_commons::proto::{ResponseMetadata, RpcRequest, RpcResponse};
use hostname::get as get_hostname;
#[cfg(feature = "mpi")]
use mpi::traits::*;
use tonic::{Request, Response, Status};

use crate::datastore::bulki_store::{BulkiStore, Dispatchable};

pub const DEFAULT_BASE_PORT: u16 = 50051;
pub const MAX_PORT_ATTEMPTS: u16 = 1000;
pub const MAX_SERVER_ADDR_LEN: usize = 512;

pub mod srvctx {
    use super::*;

    pub struct ServerContext {
        pub rank: i32,
        pub size: i32,
        #[cfg_attr(feature = "mpi", allow(unused))]
        pub world: Option<mpi::topology::SimpleCommunicator>,
        #[allow(unused)]
        pub hostname: String,
        pub server_addresses: Vec<String>,
        pub store: BulkiStore,
        pub port: u16,
    }

    impl ServerContext {
        pub fn initialize() -> Result<(Self, String), Box<dyn std::error::Error>> {
            // Initialize MPI-related values (world, rank, size)
            #[cfg(feature = "mpi")]
            let (world, rank, size) = {
                let universe = mpi::initialize().unwrap();
                let world = universe.world();
                (Some(world), world.rank(), world.size())
            };

            #[cfg(not(feature = "mpi"))]
            let (world, rank, size) = (None, 0, 1);

            // Find available port
            let port = Self::find_available_port(DEFAULT_BASE_PORT)
                .expect("Could not find available port");

            // Get hostname
            let hostname = get_hostname()?.into_string().unwrap();

            // Create server info string
            let server_info = format!("{}|{}:{}", rank, hostname, port);
            info!(target:"server", "[Rank {}] Server info: {}", rank, server_info);

            let mut server_addresses = vec![String::new(); size as usize];

            // Broadcast server info to all ranks
            // Convert string to bytes for MPI communication
            let mut info_bytes = vec![0u8; MAX_SERVER_ADDR_LEN];
            let server_info_bytes = server_info.as_bytes();
            info_bytes[..server_info_bytes.len()].copy_from_slice(server_info_bytes);

            // Create buffer to receive all server information
            let mut all_server_info = vec![0u8; MAX_SERVER_ADDR_LEN * size as usize];

            // All gather the fixed-size buffers
            #[cfg(feature = "mpi")]
            {
                world.all_gather_into(&info_bytes[..], &mut all_server_info);
            }
            #[cfg(not(feature = "mpi"))]
            {
                // For single process, just copy the local info into the first slot
                all_server_info[..info_bytes.len()].copy_from_slice(&info_bytes);
            }

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
            debug!("[Rank {}] Collected server addresses:", rank);
            for (r, addr) in server_addresses.iter().enumerate() {
                debug!("[Rank {}]   Rank {} -> {}", rank, r, addr);
            }

            // Rank 0 writes the server information to a file
            if rank == 0 {
                let pdc_tmp_dir = Self::get_pdc_tmp_dir();
                fs::create_dir_all(&pdc_tmp_dir)?;
                let server_list_path = pdc_tmp_dir.join("server_list");
                let mut file = File::create(server_list_path.clone())?;
                for (rank, addr) in server_addresses.iter().enumerate() {
                    writeln!(file, "{}|{}", rank, addr)?;
                }
                debug!(
                    "[Rank 0] Writing server list to: {}",
                    server_list_path.display()
                );
            }

            let server_addr = format!("0.0.0.0:{}", port);

            Ok((
                Self {
                    rank,
                    size,
                    world,
                    hostname,
                    server_addresses,
                    store: BulkiStore::new(),
                    port,
                },
                server_addr,
            ))
        }

        pub fn get_timestamp_ms() -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        }

        pub fn find_available_port(base_port: u16) -> Option<u16> {
            (base_port..base_port + MAX_PORT_ATTEMPTS)
                .find(|&port| TcpListener::bind(format!("0.0.0.0:{}", port)).is_ok())
        }

        pub fn get_pdc_tmp_dir() -> PathBuf {
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
                        "[Server {}] Processing RPC call: func_name='{}', data={:?}",
                        self.rank, rpc_data.func_name, rpc_data.data
                    );

                    // Use BulkiStore's dispatch method
                    let result_data = match self.store.dispatch(&rpc_data.func_name, &rpc_data.data)
                    {
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
}
