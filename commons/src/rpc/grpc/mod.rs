// Include the generated protobuf code
pub mod bulkistore {
    tonic::include_proto!("bulkistore");
}

use crate::err::RpcErr;
use crate::handler::HandlerDispatcher;
use crate::utils::{FileUtility, NetworkUtility, TimeUtility};
use crate::{
    err::{RPCResult, StatusCode},
    rpc::{MessageType, RPCData, RPCMetadata, RxContext, RxEndpoint, TxContext, TxEndpoint},
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bulkistore::grpc_bulkistore_client::GrpcBulkistoreClient;
use bulkistore::grpc_bulkistore_server::{GrpcBulkistore, GrpcBulkistoreServer};
use bulkistore::RpcMessage;
use dashmap::DashMap;
use hostname;
use log::{debug, info};
#[cfg(feature = "mpi")]
use mpi::topology::SimpleCommunicator;
#[cfg(feature = "mpi")]
use mpi::traits::{Communicator, CommunicatorCollectives};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::marker::Sync;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, Server};
use tonic::Request;

const MAX_SERVER_ADDR_LEN: usize = 256;
const DEFAULT_BASE_PORT: u16 = 50000;

const MAX_DECODING_MESSAGE_SIZE: usize = usize::MAX - 100;
const MAX_ENCODING_MESSAGE_SIZE: usize = usize::MAX - 100;

#[derive(Default, Clone)]
pub struct GrpcRX {
    pub context: RxContext<String>,
    pub port: u16,
    pub hostname: String,
    pub rx_id: u16,
}
#[derive(Default, Clone)]
pub struct GrpcTX {
    pub context: TxContext<String>,
    connections: DashMap<usize, Channel>,
}

impl GrpcRX {
    #[cfg(feature = "mpi")]
    pub fn new(rpc_id: String, world: Option<Arc<SimpleCommunicator>>) -> Self {
        Self {
            context: RxContext::new(rpc_id, world),
            port: 0,
            hostname: String::new(),
            rx_id: 0,
        }
    }
    #[cfg(not(feature = "mpi"))]
    pub fn new(rpc_id: String, world: Option<()>) -> Self {
        Self {
            context: RxContext::new(rpc_id, world),
            port: 0,
            hostname: String::new(),
            rx_id: 0,
        }
    }
}

impl GrpcTX {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(120); // 2 minutes
    #[allow(dead_code)]
    const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
    // const MAX_CONCURRENT_REQUESTS: usize = 1000;

    #[cfg(feature = "mpi")]
    pub fn new(rpc_id: String, world: Option<Arc<SimpleCommunicator>>) -> Self {
        Self {
            context: TxContext::new(rpc_id, world),
            connections: DashMap::new(),
        }
    }
    #[cfg(not(feature = "mpi"))]
    pub fn new(rpc_id: String, world: Option<()>) -> Self {
        Self {
            context: TxContext::new(rpc_id, world),
            connections: DashMap::new(),
        }
    }

    async fn check_connection_health(&self, channel: &Channel) -> bool {
        // Send an empty message just to check if connection is alive
        let metadata = RPCMetadata {
            client_rank: self.context.rank as u32,
            server_rank: 0,
            request_id: rand::rng().random(),
            request_issued_time: TimeUtility::get_timestamp_ms(),
            request_received_time: 0,
            processing_duration_us: None,
            message_type: MessageType::Request,
            handler_name: String::from("health::check"),
            handler_result: None,
        };
        let data = RPCData {
            metadata: Some(metadata),
            data: Some(vec![0; 2]),
        };
        let request = RpcMessage {
            binary_data: rmp_serde::to_vec(&data).unwrap(),
        };
        let mut client = GrpcBulkistoreClient::new(channel.clone());
        match client.process_request(Request::new(request)).await {
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
            if let Some(channel) = self.connections.get(&rx_id) {
                // Check if connection is healthy
                if self.check_connection_health(&channel).await {
                    return Ok(GrpcBulkistoreClient::new(channel.clone()));
                }
                // Connection unhealthy, will create new one
                debug!("Existing connection unhealthy, creating new one");
            }
        } // Lock automatically released here

        let server_addresses = self
            .context
            .server_addresses
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Server addresses not initialized"))?;

        if rx_id >= server_addresses.len() {
            return Err(anyhow::anyhow!("Invalid RX id: {}", rx_id));
        }

        let server_addr = server_addresses[rx_id].as_str();
        let endpoint = format!("http://{}", server_addr);

        debug!("[TX Rank {}] got endpoint {}", self.context.rank, endpoint);

        // Use Tonic's advanced channel features
        let channel = match Channel::builder(endpoint.parse().unwrap())
            .connect_timeout(Self::CONNECT_TIMEOUT)
            .tcp_keepalive(Some(Self::KEEPALIVE_INTERVAL))
            // .tcp_nodelay(true) // Disable Nagle's algorithm for better latency
            .http2_keep_alive_interval(Self::KEEPALIVE_INTERVAL)
            .keep_alive_timeout(Self::KEEPALIVE_TIMEOUT)
            .connect()
            .await
        {
            Ok(channel) => channel,
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to connect to {}: {}", endpoint, e));
            }
        };

        debug!("[TX Rank {}] Connected to RX {}", self.context.rank, rx_id);

        self.connections.insert(rx_id, channel);

        let client = GrpcBulkistoreClient::new(self.connections.get(&rx_id).unwrap().clone())
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE);

        Ok(client)
    }

    // Periodic health check for all connections
    #[allow(dead_code)]
    async fn maintain_connections(&self) {
        loop {
            tokio::time::sleep(Duration::from_secs(120)).await; // Check every two minute

            let mut unhealthy_connections = Vec::new();

            // Find unhealthy connections

            for i in 0..self.connections.len() {
                if let Some(channel) = self.connections.get(&i).as_deref() {
                    if !self.check_connection_health(channel).await {
                        unhealthy_connections.push(i);
                    }
                }
            }

            // Remove unhealthy connections
            if !unhealthy_connections.is_empty() {
                for rx_id in unhealthy_connections {
                    self.connections.remove(&rx_id);
                    debug!("Removed unhealthy connection to RX {}", rx_id);
                }
            }
        }
    }
}

#[async_trait]
impl GrpcBulkistore for GrpcRX {
    async fn process_request(
        &self,
        request: tonic::Request<RpcMessage>,
    ) -> Result<tonic::Response<RpcMessage>, tonic::Status> {
        let request = request.into_inner();

        let start_time = TimeUtility::get_timestamp_ms();
        // Deserialize the binary_data back into RPCData
        let rpc_data = match rmp_serde::from_slice::<RPCData>(&request.binary_data) {
            Ok(data) => data,
            Err(e) => {
                return Err(tonic::Status::internal(format!(
                    "Failed to deserialize request: {}",
                    e
                )))
            }
        };
        debug!(
            "Request deserialization time: {} ms",
            TimeUtility::get_timestamp_ms() - start_time
        );

        // Call respond and await its result
        let response = self
            .respond_request(rpc_data)
            .await
            .map_err(|e| tonic::Status::internal(format!("Failed to process request: {}", e)))?;

        let response_start_time = TimeUtility::get_timestamp_ms();
        // Serialize the response back to binary format
        let binary_response = rmp_serde::to_vec(&response)
            .map_err(|e| tonic::Status::internal(format!("Failed to serialize response: {}", e)))?;

        debug!(
            "Response serialization time: {} ms",
            TimeUtility::get_timestamp_ms() - response_start_time
        );

        // Create the RpcMessage response
        Ok(tonic::Response::new(RpcMessage {
            binary_data: binary_response,
        }))
    }
}

#[async_trait]
impl RxEndpoint for GrpcRX {
    type Address = String;

    fn initialize<F>(&mut self, rx_id: u16, handler_register: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        #[cfg(feature = "mpi")]
        {
            let _ = self.context.initialize_mpi();
        }

        #[cfg(not(feature = "mpi"))]
        {
            self.context.rank = 0;
            self.context.size = 1;
        }
        self.rx_id = rx_id;
        // Register handlers
        self.context.handler = Some(Arc::new(HandlerDispatcher::new()));
        let _ = handler_register(self)?;
        Ok(())
    }

    async fn listen(
        &mut self,
        start_listen: oneshot::Sender<()>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), anyhow::Error> {
        let rx_rank = (self.rx_id % 10 * 10) + self.context.rank as u16;
        let base_port = DEFAULT_BASE_PORT + rx_rank;
        // let max_attempts = 200;

        let hostname = hostname::get()?
            .into_string()
            .map_err(|_| anyhow::anyhow!("Invalid hostname"))?;

        let port = NetworkUtility::find_available_port(base_port, self.context.rank as u16);
        if port.is_none() {
            return Err(anyhow::anyhow!("Failed to find available port"));
        }
        let port = port.unwrap();
        let addr = format!("0.0.0.0:{}", port);

        let Ok(socket_addr) = addr.parse::<SocketAddr>() else {
            return Err(anyhow::anyhow!(
                "Failed to parse address {}:{}",
                hostname,
                port
            ));
        };
        // bind with TCPListener
        let listener = tokio::net::TcpListener::bind(socket_addr).await?;

        self.port = port;
        self.hostname = hostname.clone();
        self.context.address = Some(addr);
        let mut server_addresses = vec![String::new(); self.context.size as usize];
        server_addresses[self.context.rank] = format!("{}:{}", hostname, port);
        self.context.server_addresses = Some(server_addresses);
        debug!(
            "[Rank {}] Server addresses: {:?}",
            self.context.rank, self.context.server_addresses
        );

        let service = Arc::new(self.clone());
        // spawn a new task to start server
        tokio::spawn(async move {
            Server::builder()
                .add_service(
                    GrpcBulkistoreServer::from_arc(service)
                        .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE)
                        .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE)
                        .accept_compressed(CompressionEncoding::Zstd)
                        .send_compressed(CompressionEncoding::Zstd),
                )
                // add as a dev-dependency the crate `tokio-stream` with feature `net` enabled
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async move {
                        shutdown_rx.await.ok();
                    },
                )
                .await
        });

        debug!(
            "[Rank {}] Successfully started server on {}:{}",
            self.context.rank, hostname, port
        );

        let _ = start_listen.send(());
        Ok(())
    }

    fn exchange_addresses(&mut self) -> Result<()> {
        // Create server info string
        let server_info = format!(
            "{}|{}",
            self.context.rank,
            self.context.server_addresses.as_ref().unwrap()[self.context.rank]
        );
        debug!(target:"server", "[Rank {}] Server info: {}", self.context.rank, server_info);

        // Broadcast server info to all ranks
        // Convert string to bytes for MPI communication
        let mut info_bytes = vec![0u8; MAX_SERVER_ADDR_LEN];
        let server_info_bytes = server_info.as_bytes();
        info_bytes[..server_info_bytes.len()].copy_from_slice(server_info_bytes);

        // Create buffer to receive all server information
        let mut all_server_info = vec![0u8; MAX_SERVER_ADDR_LEN * self.context.size as usize];

        let exchange_addresses_time = Instant::now();
        // All gather the fixed-size buffers
        #[cfg(feature = "mpi")]
        {
            if let Some(ref world) = self.context.world {
                world.all_gather_into(&info_bytes[..], &mut all_server_info);
            }
        }
        #[cfg(not(feature = "mpi"))]
        {
            // For single process, just copy the local info into the first slot
            all_server_info[..info_bytes.len()].copy_from_slice(&info_bytes);
        }
        debug!(
            "[Rank {}] all_gather_into time: {} ms",
            self.context.rank,
            exchange_addresses_time.elapsed().as_millis()
        );

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
                        self.context.server_addresses.as_mut().unwrap()[rank] =
                            parts[1].to_string();
                    }
                }
            }
        }

        // Verify no empty addresses
        for rank in 0..self.context.size as usize {
            if self.context.server_addresses.as_ref().unwrap()[rank].is_empty() {
                return Err(anyhow::anyhow!("Missing address for rank {}", rank));
            }
        }

        Ok(())
    }

    fn write_addresses(&self) -> Result<()> {
        // Rank 0 writes the server information to a file
        if self.context.rank == 0 {
            let pdc_tmp_dir = FileUtility::get_pdc_tmp_dir();
            fs::create_dir_all(&pdc_tmp_dir)?;
            let server_list_path = pdc_tmp_dir.join(format!("rx_{}.txt", self.context.rpc_id));
            let mut file = File::create(server_list_path.clone())?;
            for (rank, addr) in self
                .context
                .server_addresses
                .as_ref()
                .unwrap()
                .iter()
                .enumerate()
            {
                writeln!(file, "{}|{}", rank, addr)?;
            }
            debug!(
                "[Rank 0] Writing server list to: {}",
                server_list_path.display()
            );

            // Create a ready file to signal that this server is up
            let ready_file = pdc_tmp_dir.join(format!("rx_{}_ready.txt", self.context.rpc_id));
            std::fs::write(&ready_file, "ready").expect("Failed to create ready file");
        }
        Ok(())
    }

    async fn respond_request(&self, mut msg: RPCData) -> Result<RPCData> {
        use tokio::time::Instant;

        let start_time = Instant::now();
        // update the received time in the metadata
        if let Some(metadata) = &mut msg.metadata {
            metadata.request_received_time = TimeUtility::get_timestamp_us();
        }

        let mut result = self
            .context
            .handler
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Handler not initialized"))?
            .handle(msg)
            .await?;

        // Update the request processing completion time and duration
        if let Some(metadata) = &mut result.metadata {
            // this is required, since the client will check message type.
            metadata.message_type = MessageType::Response;
            // Store the actual processing duration in microseconds
            metadata.processing_duration_us = Some(start_time.elapsed().as_micros() as u64);
        }

        Ok(result)
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl TxEndpoint for GrpcTX {
    type Address = String;

    fn initialize<F>(&mut self, handler_register: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        #[cfg(not(feature = "mpi"))]
        {
            self.context.world = None;
            self.context.rank = 0;
            self.context.size = 1;
        }

        #[cfg(feature = "mpi")]
        {
            if let Some(world) = &self.context.world {
                self.context.rank = world.rank() as usize;
                self.context.size = world.size() as usize;
            } else {
                self.context.rank = 0;
                self.context.size = 1;
            }
        }

        // Register handlers
        self.context.handler = Some(Arc::new(HandlerDispatcher::new()));
        handler_register(self)?;
        Ok(())
    }
    fn discover_servers(&mut self) -> Result<isize> {
        let pdc_tmp_dir = FileUtility::get_pdc_tmp_dir();
        debug!("Getting PDC tmp dir: {}", pdc_tmp_dir.display());

        // Wait for ready file to be created
        let ready_file = pdc_tmp_dir.join(format!("rx_{}_ready.txt", self.context.rpc_id));
        debug!("Waiting for ready file: {}", ready_file.display());
        while !ready_file.exists() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        debug!("Ready file found: {}", ready_file.display());
        // read the ready file and check if the content is "ready"
        let contents = fs::read_to_string(&ready_file).expect("Failed to read ready file");
        if contents != "ready" {
            return Err(anyhow!("Ready file does not contain 'ready'"));
        }

        let file_path = pdc_tmp_dir.join(format!("rx_{}.txt", self.context.rpc_id));
        debug!("Reading server list from: {}", file_path.display());
        // Convert PathBuf to &Path for read_to_string
        let contents = fs::read_to_string(&file_path)?;

        // Filter out empty lines and collect
        let lines: Vec<_> = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();

        if lines.is_empty() {
            return Err(anyhow::anyhow!("No server addresses found in file"));
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

        self.context.server_addresses = Some(addresses);
        info!(
            "[TX Rank {}] Found {} server(s)",
            self.context.rank,
            self.context.server_addresses.as_ref().unwrap().len()
        );
        Ok(self.context.server_addresses.as_ref().unwrap().len() as isize)
    }

    async fn send_message<T, R>(&self, rx_id: usize, handler_name: &str, input: &T) -> RPCResult<R>
    where
        T: Serialize + Sync + 'static,
        R: for<'de> Deserialize<'de>,
    {
        // Get cached or create new connection first
        let mut client = self.get_or_create_connection(rx_id).await.map_err(|e| {
            RpcErr::new(
                StatusCode::Internal,
                format!("Failed to get or create connection: {}", e),
            )
        })?;

        debug!(
            "[TX Rank {}] Sending {} message to RX Rank {}",
            self.context.rank, handler_name, rx_id
        );

        let metadata = RPCMetadata {
            client_rank: self.context.rank as u32,
            server_rank: rx_id as u32,
            request_id: rand::rng().random::<u64>(),
            request_issued_time: TimeUtility::get_timestamp_us(),
            request_received_time: 0,
            processing_duration_us: None,
            message_type: MessageType::Request,
            handler_name: handler_name.to_string(),
            handler_result: None,
        };

        let data = rmp_serde::to_vec(input).map_err(|e| {
            RpcErr::new(
                StatusCode::InvalidRequest,
                format!("Failed to serialize params: {}", e),
            )
        })?;

        let data = RPCData {
            metadata: Some(metadata),
            data: Some(data),
        };

        let bin_request = rmp_serde::to_vec(&data)
            .map_err(|e| RpcErr::new(StatusCode::Internal, format!("{}", e)))?;

        // Create request
        let request = tonic::Request::new(RpcMessage {
            binary_data: bin_request,
        });
        let req_size = request.get_ref().binary_data.len();
        debug!(
            "[TX Rank {}] Sending request of length: {} to RX {} for fn {}",
            self.context.rank, req_size, rx_id, handler_name
        );

        // Send binary request and wait for response
        match client.process_request(request).await {
            Ok(response) => {
                let bin_response = response.into_inner().binary_data;
                match rmp_serde::from_slice::<RPCData>(&bin_response) {
                    Ok(resp_data) => {
                        // Extract message_type without moving the entire metadata
                        let message_type = resp_data.metadata.as_ref().map(|m| m.message_type);
                        // extract rx_id from metadata
                        let rx_id = resp_data
                            .metadata
                            .as_ref()
                            .map(|m| m.server_rank as usize)
                            .unwrap_or(0);
                        // extract handler_result status code
                        let status_code = resp_data
                            .metadata
                            .as_ref()
                            .and_then(|r| r.handler_result.as_ref())
                            .map(|hs| hs.status_code)
                            .unwrap_or(StatusCode::Unknown.into());
                        let handler_message = resp_data
                            .metadata
                            .as_ref()
                            .and_then(|r| r.handler_result.as_ref())
                            .and_then(|hs| hs.message.clone())
                            .unwrap_or("".to_string());

                        debug!(
                            "[TX Rank {}][FN {}] {:?} from RX {}, status code: {}, request handler message: {}, response of length: {}",
                            self.context.rank,
                            handler_name,
                            message_type,
                            rx_id,
                            status_code,
                            handler_message,
                            bin_response.len()
                        );
                        // validate request handler result by status code and message.
                        if status_code != StatusCode::Ok as u8 {
                            return Err(RpcErr::new(status_code.into(), handler_message));
                        }
                        match message_type {
                            Some(MessageType::Response) => {
                                if let Some(handler) = &self.context.handler {
                                    let resp_data =
                                        handler.handle(resp_data).await.map_err(|e| {
                                            RpcErr::new(
                                                StatusCode::Internal,
                                                format!("Handler error: {}", e),
                                            )
                                        })?;
                                    rmp_serde::from_slice::<R>(
                                        resp_data.data.as_ref().unwrap_or(&vec![]),
                                    )
                                    .map_err(|e| {
                                        RpcErr::new(
                                            StatusCode::Internal,
                                            format!(
                                                "Failed to deserialize RPCData response: {}",
                                                e
                                            ),
                                        )
                                    })
                                } else {
                                    debug!(
                                        "No response handler registered, returning raw response"
                                    );
                                    Err(RpcErr::new(
                                        StatusCode::Internal,
                                        "No response handler registered",
                                    ))
                                }
                            }
                            _ => Err(RpcErr::new(
                                StatusCode::Internal,
                                format!("Unexpected message type: {:?}", message_type),
                            )),
                        }
                    }
                    Err(e) => Err(RpcErr::new(
                        StatusCode::Internal,
                        format!(
                            "Failed to deserialize binary response from RpcMessage: {}",
                            e
                        ),
                    )),
                }
            }
            Err(status) => {
                // If connection error, remove it from cache
                if status.code() == tonic::Code::Unavailable {
                    self.connections.remove(&rx_id);
                    debug!("Removed failed connection to RX {}", rx_id);
                }
                Err(RpcErr::new(StatusCode::ConnectionFailure, status.message()))
            }
        }
    }

    async fn close(&self) -> Result<()> {
        // Clear all cached connections
        let conn_count = {
            let count = self.connections.len();
            self.connections.clear();
            count
        };
        debug!("Cleared {} cached gRPC TX connections", conn_count);
        debug!("gRPC TX shutdown complete");
        Ok(())
    }
}
