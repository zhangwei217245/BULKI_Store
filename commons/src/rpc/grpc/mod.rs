pub mod grpc {
    use crate::rpc::{RxContext, TxContext, RPCMetadata, RPCData, MessageType, RxEndpoint, TxEndpoint, RPCHandler};
    use crate::store::BulkiStore;
    use anyhow::Result;
    use hostname;
    use log::{debug, error, info, warn};
    use mpi::topology::SimpleCommunicator;
    use std::fs::{self, File};
    use std::io::Write;
    use std::env;
    use std::net::SocketAddr;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tonic::{Request, Response};
    use tonic::transport::{Channel, Server};
    use tokio::sync::{broadcast, Mutex};
    use rand::Rng;
    use bincode;

    const MAX_SERVER_ADDR_LEN: usize = 256;
    const DEFAULT_BASE_PORT: u16 = 50051;

    pub struct GRPC_RX {
        pub context: RxContext<String>,
    }
    pub struct GRPC_TX {
        pub context: TxContext<String>,
        // Cache connections using Arc<Mutex> for thread-safety
        connections: Arc<Mutex<HashMap<usize, GrpcBulkistoreClient<Channel>>>>,
    }
    pub struct RXTXUtils;

    impl GRPC_RX {
        fn new(rpc_id: String) -> Self {
            Self {
                context: RxContext::new(rpc_id),
            }
        }
    }

    impl GRPC_TX {
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
        const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(300*3); // 15 minutes
        const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
        const MAX_CONCURRENT_REQUESTS: usize = 1000;

        fn new(rpc_id: String) -> Self {
            Self {
                context: TxContext::new(rpc_id),
                connections: Arc::new(Mutex::new(HashMap::new())),
            }
        }
        async fn check_connection_health(&self, client: &GrpcBulkistoreClient<Channel>) -> bool {
            match client.health_check(Request::new(())).await {
                Ok(_) => true,
                Err(e) => {
                    debug!("Connection health check failed: {}", e);
                    false
                }
            }
        }
        async fn get_or_create_connection(
            &self,
            rx_id: usize,
        ) -> Result<GrpcBulkistoreClient<Channel>> {
            // Try existing connection
            {
                // Lock acquired
                let connections = self.connections.lock().await;
                if let Some(client) = connections.get(&rx_id) {
                    // Check if connection is healthy
                    if self.check_connection_health(client).await {
                        return Ok(client.clone());
                    }
                    // Connection unhealthy, will create new one
                    debug!("Existing connection unhealthy, creating new one");
                }
            } // Lock automatically released here

        
            let server_addresses = self.context.server_addresses
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Server addresses not initialized"))?;
                
            if rx_id >= server_addresses.len() {
                return Err(anyhow::anyhow!("Invalid RX id: {}", rx_id));
            }
        
            let server_addr = &server_addresses[rx_id];
            let endpoint = format!("http://{}", server_addr);
            
            // Use Tonic's advanced channel features
            let channel = Channel::from_shared(endpoint)?
                .connect_timeout(Self::CONNECT_TIMEOUT)
                .tcp_keepalive(Some(Self::KEEPALIVE_INTERVAL))
                .tcp_nodelay(true) // Disable Nagle's algorithm for better latency
                .http2_keep_alive_interval(Self::KEEPALIVE_INTERVAL)
                .http2_keep_alive_timeout(Self::KEEPALIVE_TIMEOUT)
                .concurrency_limit(Self::MAX_CONCURRENT_REQUESTS)
                // Remove rate limit since it's a cached connection
                // Let application layer handle throttling if needed
                .connect()
                .await?;
        
            let client = GrpcBulkistoreClient::new(channel);
            
            // Cache the new connection
            {
                let mut connections = self.connections.lock().await;
                connections.insert(rx_id, client.clone());
            }

            Ok(client)
        }

        // Periodic health check for all connections
        async fn maintain_connections(&self) {
            loop {
                tokio::time::sleep(Duration::from_secs(120)).await; // Check every minute
                
                let mut unhealthy_connections = Vec::new();
                
                // Find unhealthy connections
                {
                    let connections = self.connections.lock().await;
                    for (rx_id, client) in connections.iter() {
                        if !self.check_connection_health(client).await {
                            unhealthy_connections.push(*rx_id);
                        }
                    }
                }
                
                // Remove unhealthy connections
                if !unhealthy_connections.is_empty() {
                    let mut connections = self.connections.lock().await;
                    for rx_id in unhealthy_connections {
                        connections.remove(&rx_id);
                        debug!("Removed unhealthy connection to RX {}", rx_id);
                    }
                }
            }
        }
    }

    impl RXTXUtils {

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

    impl RxEndpoint for GRPC_RX {
        fn initialize(&mut self, world: Option<SimpleCommunicator>) -> Result<()> {
            self.context.world = world;

            #[cfg(not(feature = "mpi"))]
            {
                self.context.rank = 0;
                self.context.size = 1;
            }

            #[cfg(feature = "mpi")]
            {
                if let Some(ref world) = self.world {
                    self.context.rank = world.unwrap().rank();
                    self.context.size = world.unwrap().size();
                }
            }

            // Find available port
            let port = RXTXUtils::find_available_port(DEFAULT_BASE_PORT)
                .ok_or_else(|| anyhow::anyhow!("Could not find available port"));

            // Get hostname
            let hostname = hostname::get()?
                .into_string()
                .map_err(|_| anyhow::anyhow!("Invalid hostname"))?;

            self.context.address = format!("0.0.0.0:{}", hostname, port);

            let mut server_addresses = vec![String::new(); self.context.size as usize];
            server_addresses[self.context.rank] = format!("{}:{}", hostname, port);
            self.context.server_addresses = server_addresses;

            Ok(())
        }
        fn exchange_addresses(&mut self) -> Result<()> {
            // Create server info string
            let server_info = format!(
                "{}|{}",
                self.context.rank, self.context.server_addresses[self.context.rank]
            );
            info!(target:"server", "[Rank {}] Server info: {}", self.context.rank, server_info);

            // Broadcast server info to all ranks
            // Convert string to bytes for MPI communication
            let mut info_bytes = vec![0u8; MAX_SERVER_ADDR_LEN];
            let server_info_bytes = server_info.as_bytes();
            info_bytes[..server_info_bytes.len()].copy_from_slice(server_info_bytes);

            // Create buffer to receive all server information
            let mut all_server_info = vec![0u8; MAX_SERVER_ADDR_LEN * self.size as usize];

            // All gather the fixed-size buffers
            #[cfg(feature = "mpi")]
            {
                if let Some(ref world) = self.world {
                    world.all_gather_into(&info_bytes[..], &mut all_server_info);
                }
            }
            #[cfg(not(feature = "mpi"))]
            {
                // For single process, just copy the local info into the first slot
                all_server_info[..info_bytes.len()].copy_from_slice(&info_bytes);
            }

            for i in 0..self.context.size as usize {
                let start = i * MAX_SERVER_ADDR_LEN;
                let end = start + MAX_SERVER_ADDR_LEN;
                let slice = &all_server_info[start..end];

                let actual_len = slice
                    .iter()
                    .position(|&x| x == 0)
                    .unwrap_or(MAX_SERVER_ADDR_LEN);

                if let Ok(info) = String::from_utf8(slice[..actual_len].to_vec()) {
                    let parts: Vec<&str> = info.splitn(2, '|').collect();
                    if parts.len() == 2 {
                        if let Ok(rank) = parts[0].parse::<usize>() {
                            self.context.server_addresses[rank] = parts[1].to_string();
                        }
                    }
                }
            }

            // Verify no empty addresses
            for rank in 0..self.context.size as usize {
                if self.context.server_addresses[rank].is_empty() {
                    return Err(anyhow::anyhow!("Missing address for rank {}", rank));
                }
            }

            Ok(())
        }
        fn write_addresses(&self) -> Result<()> {
            // Rank 0 writes the server information to a file
            if self.context.rank == 0 {
                let pdc_tmp_dir = RXTXUtils::get_pdc_tmp_dir();
                fs::create_dir_all(&pdc_tmp_dir)?;
                let server_list_path = pdc_tmp_dir.join(format!("rx_{}.txt", self.context.rpc_id));
                let mut file = File::create(server_list_path.clone())?;
                for (rank, addr) in self.context.server_addresses.iter().enumerate() {
                    writeln!(file, "{}|{}", rank, addr)?;
                }
                debug!(
                    "[Rank 0] Writing server list to: {}",
                    server_list_path.display()
                );

                // Create a ready file to signal that this server is up
                let ready_file = pdc_tmp_dir.join(format!("rx_{}_ready.txt", self.context.rpc_id));
                std::fs::write(&ready_file, "ready")
                .expect("Failed to create ready file");
            }
            Ok(())
        }
        async fn listen<F, Fut>(&mut self, shutdown_handler: F) -> Result<()>
        where
            F: FnOnce() -> Fut + Send + 'static,
            Fut: Future<Output = Result<()>> + Send + 'static,
        {
            let addr = format!("0.0.0.0:{}", self.port).parse()?;
            let server = tonic::transport::Server::builder()
                .add_service(/* your gRPC service */)
                .serve_with_shutdown(addr, shutdown_handler());
            
            server.await?;
            Ok(())
        }

        fn respond(&self, msg: &Self::RPCData) -> Result<()>{

        }
        fn close(&self) -> Result<()>{
            
            Ok(())
        }
    }
    impl TxEndpoint for GRPC_TX<String> {

        fn initialize(&mut self, world: Option<SimpleCommunicator>) -> Result<()>{
            self.context.world = world;

            #[cfg(not(feature = "mpi"))]
            {
                self.context.rank = 0;
                self.context.size = 1;
            }

            #[cfg(feature = "mpi")]
            {
                if let Some(ref world) = self.world {
                    self.context.rank = world.unwrap().rank();
                    self.context.size = world.unwrap().size();
                }
            }

            Ok(())
        }
        fn discover_servers(&mut self) -> Result<()> {
            let pdc_tmp_dir = RXTXUtils::get_pdc_tmp_dir();

            // Wait for ready file to be created
            let ready_file = pdc_tmp_dir.join(format!("rx_{}_ready.txt", self.context.rpc_id));
            while !ready_file.exists() {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            // read the ready file and check if the content is "ready"
            let contents = fs::read_to_string(&ready_file).expect("Failed to read ready file");
            if contents != "ready" {
                return Err("Ready file does not contain 'ready'".into());
            }

            let file_path = pdc_tmp_dir.join(format!("rx_{}.txt", self.context.rpc_id));
            // Convert PathBuf to &Path for read_to_string
            let contents = fs::read_to_string(&file_path)?;

            // Filter out empty lines and collect
            let lines: Vec<_> = contents
                .lines()
                .filter(|line| !line.trim().is_empty())
                .collect();

            if lines.is_empty() {
                return Err("No server addresses found in file".into());
            }

            // Create a vector with size matching number of servers
            let mut addresses = vec![String::new(); lines.len()];

            // Parse each line in format "rank|host:port" and place in correct position
            for line in lines {
                if let Some((rank_str, addr)) = line.split_once('|') {
                    if let Ok(rank) = rank_str.parse::<usize>() {
                        addresses[rank] = addr.to_string();
                    }
                }
            }

            self.context.server_addresses = addresses;
            println!(
                "[TX Rank {}] Found {} server(s)",
                self.context.rank,
                self.context.server_addresses.len()
            );
            Ok(())
        }
        async fn send_message(&self, rx_id: usize, handler_name:String, data: Vec<u8>) -> Result<()> {
            // Get cached or create new connection
            let mut client = self.get_or_create_connection(rx_id).await?;

            let metadata = RPCMetadata {
                client_rank: self.context.rank as u32,
                server_rank: rx_id as u32,
                request_id: rand::thread_rng().gen::<u64>(),
                request_issued_time: RXTXUtils::get_timestamp_ms(),
                request_received_time: 0,
                request_processed_time: 0,
                message_type: MessageType::Request,
                handler_name: handler_name,
                error_message: None,
            };

            let rpc_data = RPCData {
                metadata: Some(metadata),
                data,
            };
            
            // Serialize RPCData to binary
            let binary_data = bincode::serialize(&rpc_data)
                .map_err(|e| anyhow::anyhow!("Failed to serialize message: {}", e))?;

            // Create request
            let request = tonic::Request::new(RpcRequest {
                binary_data: binary_data,
            });

            // Send request and wait for response
            match client.process_request(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    // Deserialize the response binary_data to get our RPCData
                    match bincode::deserialize::<RPCData>(&response.binary_data) {
                        Ok(rpc_data) => {
                            if let Some(metadata) = rpc_data.metadata {
                                if metadata.message_type == MessageType::Error {
                                    Err(anyhow::anyhow!(
                                        "Request failed: {}",
                                        metadata.error_message.unwrap_or_default()
                                    ))
                                } else {
                                    debug!("Successfully sent message to RX {}", rx_id);
                                    handler.handle_response(metadata, rpc_data.data).await?;
                                    Ok(())
                                }
                            } else {
                                Err(anyhow::anyhow!("Response missing metadata"))
                            }
                        },
                        Err(e) => Err(anyhow::anyhow!("Failed to deserialize response: {}", e))
                    }
                },
                Err(status) => {
                    // If connection error, remove it from cache
                    if status.code() == tonic::Code::Unavailable {
                        let mut connections = self.connections.lock().await;
                        connections.remove(&rx_id);
                        debug!("Removed failed connection to RX {}", rx_id);
                    }
                    Err(anyhow::anyhow!("RPC failed: {}", status))
                }
            }
        }
        async fn close(&self) -> Result<()>{
            // Clear all cached connections
            let mut connections = self.connections.lock().await;
            connections.clear();
            debug!("Cleared {} cached gRPC TX connections", connections.len());

            debug!("gRPC TX shutdown complete");
            Ok(())
        }

    }

}
