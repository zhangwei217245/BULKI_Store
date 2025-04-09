use crate::err::{RPCError, RPCResult};
use crate::handler::{HandlerDispatcher, RequestHandlerKind, ResponseHandlerKind};
use crate::rpc::{MessageType, RPCData, RPCMetadata, RxContext, RxEndpoint, TxContext, TxEndpoint};
use anyhow::Result;
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};
use tokio::task;
use tokio::time::sleep;

pub mod factory;

// Message format:
// [4 bytes] Total message length
// [1 byte]  Message type (0=request, 1=response)
// [N bytes] Serialized RPCData

const HEADER_SIZE: usize = 5;
const CONNECTION_TIMEOUT_MS: u64 = 100;
const MAX_RETRIES: usize = 3;
const CONNECTION_POOL_SIZE: usize = 10;

pub type TcpAddress = SocketAddr;

// Connection pool for reusing connections
struct ConnectionPool {
    connections: HashMap<SocketAddr, Vec<TokioTcpStream>>,
}

impl ConnectionPool {
    fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    async fn get_connection(&mut self, addr: SocketAddr) -> Result<TokioTcpStream> {
        if let Some(conns) = self.connections.get_mut(&addr) {
            if let Some(conn) = conns.pop() {
                return Ok(conn);
            }
        }

        // Create new connection if none available
        let stream = TokioTcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    fn return_connection(&mut self, addr: SocketAddr, conn: TokioTcpStream) {
        self.connections
            .entry(addr)
            .or_insert_with(Vec::new)
            .push(conn);

        // Limit pool size
        if let Some(conns) = self.connections.get_mut(&addr) {
            while conns.len() > CONNECTION_POOL_SIZE {
                conns.remove(0);
            }
        }
    }
}

pub struct TcpRxEndpoint {
    context: RxContext<TcpAddress>,
    listener: Option<TokioTcpListener>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl TcpRxEndpoint {
    pub fn new(context: RxContext<TcpAddress>) -> Self {
        Self {
            context,
            listener: None,
            shutdown_tx: None,
        }
    }
}

#[async_trait]
impl RxEndpoint for TcpRxEndpoint {
    type Address = TcpAddress;

    fn initialize<F>(&mut self, rx_id: u16, handler_register: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        // Bind to an available port
        let listener = TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            rx_id as u16,
        ))?;

        let local_addr = listener.local_addr()?;
        self.context.address = Some(local_addr);

        // Convert to tokio listener
        let tokio_listener = TokioTcpListener::from_std(listener)?;
        self.listener = Some(tokio_listener);

        // Register handlers
        handler_register(self)?;

        Ok(())
    }

    async fn listen(
        &mut self,
        start_listen: oneshot::Sender<()>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), anyhow::Error> {
        let listener = self
            .listener
            .take()
            .ok_or_else(|| anyhow::anyhow!("Listener not initialized"))?;
        let handler = self
            .context
            .handler
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Handler not initialized"))?;

        // Signal that we're ready to listen
        let _ = start_listen.send(());

        // Create a channel for shutdown
        let (inner_shutdown_tx, mut inner_shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(oneshot::Sender::from_raw(inner_shutdown_tx.into_raw()));

        // Spawn a task to handle the shutdown signal
        let shutdown_task = tokio::spawn(async move {
            let _ = shutdown_rx.await;
            let _ = inner_shutdown_tx.send(()).await;
        });

        // Accept connections
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _)) => {
                            let handler_clone = handler.clone();
                            tokio::spawn(async move {
                                Self::handle_connection(stream, handler_clone).await;
                            });
                        }
                        Err(e) => {
                            eprintln!("Error accepting connection: {}", e);
                        }
                    }
                }
                _ = inner_shutdown_rx.recv() => {
                    break;
                }
            }
        }

        shutdown_task.abort();
        Ok(())
    }

    fn exchange_addresses(&mut self) -> Result<()> {
        // For TCP, we just need to write our address to a file
        // and read other addresses from files
        self.write_addresses()?;

        // Read addresses of other servers
        let mut addresses = Vec::new();
        for i in 0..self.context.size {
            if i == self.context.rank {
                addresses.push(self.context.address.unwrap());
                continue;
            }

            let filename = format!("rx_{}_rank_{}.txt", self.context.rpc_id, i);
            let mut retries = 0;
            let mut address = None;

            while retries < MAX_RETRIES {
                if let Ok(content) = std::fs::read_to_string(&filename) {
                    if let Ok(addr) = content.trim().parse() {
                        address = Some(addr);
                        break;
                    }
                }
                retries += 1;
                std::thread::sleep(Duration::from_millis(100));
            }

            if let Some(addr) = address {
                addresses.push(addr);
            } else {
                return Err(anyhow::anyhow!("Failed to read address for rank {}", i));
            }
        }

        self.context.server_addresses = Some(addresses);
        Ok(())
    }

    fn write_addresses(&self) -> Result<()> {
        if let Some(addr) = self.context.address {
            let filename = format!("rx_{}_rank_{}.txt", self.context.rpc_id, self.context.rank);
            std::fs::write(filename, format!("{}", addr))?;
        }
        Ok(())
    }

    async fn respond_request(&self, msg: RPCData) -> Result<RPCData> {
        // This is used internally by handle_connection
        unimplemented!("respond_request is implemented internally")
    }

    fn close(&self) -> Result<()> {
        if let Some(shutdown_tx) = &self.shutdown_tx {
            let _ = shutdown_tx.send(());
        }
        Ok(())
    }
}

impl TcpRxEndpoint {
    async fn handle_connection(
        mut stream: TokioTcpStream,
        handler: Arc<HandlerDispatcher<RequestHandlerKind>>,
    ) {
        // Set TCP_NODELAY to reduce latency
        if let Err(e) = stream.set_nodelay(true) {
            eprintln!("Failed to set TCP_NODELAY: {}", e);
        }

        // Read message header
        let mut header = [0u8; HEADER_SIZE];
        if let Err(e) = stream.read_exact(&mut header).await {
            eprintln!("Error reading header: {}", e);
            return;
        }

        // Parse header
        let msg_len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let msg_type = MessageType::from_u8(header[4]).unwrap_or(MessageType::Request);

        // Read message body
        let mut buffer = vec![0u8; msg_len];
        if let Err(e) = stream.read_exact(&mut buffer).await {
            eprintln!("Error reading message body: {}", e);
            return;
        }

        // Deserialize request
        let request: RPCData = match bincode::deserialize(&buffer) {
            Ok(req) => req,
            Err(e) => {
                eprintln!("Error deserializing request: {}", e);
                return;
            }
        };

        // Process request
        let received_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mut metadata = request.metadata.unwrap_or_default();
        metadata.request_received_time = received_time;

        let start_time = Instant::now();
        let mut response = RPCData {
            data: None,
            metadata: Some(metadata.clone()),
        };

        // Dispatch to handler
        let handler_name = metadata.handler_name.clone();
        let result = handler.dispatch(&handler_name, request).await;

        // Update metadata
        let processing_time = start_time.elapsed().as_micros() as u64;
        if let Some(meta) = &mut response.metadata {
            meta.processing_duration_us = Some(processing_time);
            meta.message_type = MessageType::Response;
            meta.handler_result = Some(result.clone());
        }

        // Set response data
        match result.status_code {
            0 => response.data = result.data,
            _ => response.data = None,
        }

        // Serialize and send response
        let serialized = match bincode::serialize(&response) {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error serializing response: {}", e);
                return;
            }
        };

        // Create response header
        let mut response_header = [0u8; HEADER_SIZE];
        response_header[0..4].copy_from_slice(&(serialized.len() as u32).to_be_bytes());
        response_header[4] = MessageType::Response.to_u8();

        // Send response
        if let Err(e) = stream.write_all(&response_header).await {
            eprintln!("Error writing response header: {}", e);
            return;
        }

        if let Err(e) = stream.write_all(&serialized).await {
            eprintln!("Error writing response body: {}", e);
        }
    }
}

pub struct TcpTxEndpoint {
    context: TxContext<TcpAddress>,
    connection_pool: Arc<TokioMutex<ConnectionPool>>,
}

impl TcpTxEndpoint {
    pub fn new(context: TxContext<TcpAddress>) -> Self {
        Self {
            context,
            connection_pool: Arc::new(TokioMutex::new(ConnectionPool::new())),
        }
    }
}

#[async_trait]
impl TxEndpoint for TcpTxEndpoint {
    type Address = TcpAddress;

    fn initialize<F>(&mut self, handler_register: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        handler_register(self)
    }

    fn discover_servers(&mut self) -> Result<isize> {
        let mut addresses = Vec::new();

        for i in 0..self.context.size {
            let filename = format!("rx_{}_rank_{}.txt", self.context.rpc_id, i);
            let mut retries = 0;
            let mut address = None;

            while retries < MAX_RETRIES {
                if let Ok(content) = std::fs::read_to_string(&filename) {
                    if let Ok(addr) = content.trim().parse() {
                        address = Some(addr);
                        break;
                    }
                }
                retries += 1;
                std::thread::sleep(Duration::from_millis(100));
            }

            if let Some(addr) = address {
                addresses.push(addr);
            } else {
                return Err(anyhow::anyhow!("Failed to read address for rank {}", i));
            }
        }

        self.context.server_addresses = Some(addresses);
        Ok(addresses.len() as isize)
    }

    async fn send_message<T, R>(&self, rx_id: usize, handler_name: &str, input: &T) -> RPCResult<R>
    where
        T: Serialize + std::marker::Sync + 'static,
        R: for<'de> Deserialize<'de>,
    {
        let addresses = self
            .context
            .server_addresses
            .as_ref()
            .ok_or_else(|| RPCError::new("Server addresses not discovered"))?;

        if rx_id >= addresses.len() {
            return Err(RPCError::new(&format!("Invalid rx_id: {}", rx_id)));
        }

        let server_addr = addresses[rx_id];

        // Serialize input
        let serialized_input = bincode::serialize(input)
            .map_err(|e| RPCError::new(&format!("Serialization error: {}", e)))?;

        // Create request metadata
        let request_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let metadata = RPCMetadata {
            client_rank: self.context.rank as u32,
            server_rank: rx_id as u32,
            request_id: rand::random::<u64>(),
            request_issued_time: request_time,
            request_received_time: 0,
            processing_duration_us: None,
            message_type: MessageType::Request,
            handler_name: handler_name.to_string(),
            handler_result: None,
        };

        // Create request
        let request = RPCData {
            data: Some(serialized_input),
            metadata: Some(metadata),
        };

        // Serialize the entire request
        let serialized_request = bincode::serialize(&request)
            .map_err(|e| RPCError::new(&format!("Request serialization error: {}", e)))?;

        // Get connection from pool
        let mut pool = self.connection_pool.lock().await;
        let mut stream = pool
            .get_connection(server_addr)
            .await
            .map_err(|e| RPCError::new(&format!("Connection error: {}", e)))?;

        // Create request header
        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&(serialized_request.len() as u32).to_be_bytes());
        header[4] = MessageType::Request.to_u8();

        // Send request
        stream
            .write_all(&header)
            .await
            .map_err(|e| RPCError::new(&format!("Send error: {}", e)))?;

        stream
            .write_all(&serialized_request)
            .await
            .map_err(|e| RPCError::new(&format!("Send error: {}", e)))?;

        // Read response header
        let mut response_header = [0u8; HEADER_SIZE];
        stream
            .read_exact(&mut response_header)
            .await
            .map_err(|e| RPCError::new(&format!("Receive error: {}", e)))?;

        let response_len = u32::from_be_bytes([
            response_header[0],
            response_header[1],
            response_header[2],
            response_header[3],
        ]) as usize;

        // Read response body
        let mut response_buffer = vec![0u8; response_len];
        stream
            .read_exact(&mut response_buffer)
            .await
            .map_err(|e| RPCError::new(&format!("Receive error: {}", e)))?;

        // Return connection to pool
        pool.return_connection(server_addr, stream);

        // Deserialize response
        let response: RPCData = bincode::deserialize(&response_buffer)
            .map_err(|e| RPCError::new(&format!("Response deserialization error: {}", e)))?;

        // Check for handler errors
        if let Some(meta) = &response.metadata {
            if let Some(result) = &meta.handler_result {
                if result.status_code != 0 {
                    return Err(RPCError::new(&format!(
                        "Handler error: {}",
                        result.message.clone().unwrap_or_default()
                    )));
                }
            }
        }

        // Deserialize response data
        if let Some(data) = response.data {
            let result: R = bincode::deserialize(&data)
                .map_err(|e| RPCError::new(&format!("Result deserialization error: {}", e)))?;
            Ok(result)
        } else {
            Err(RPCError::new("Empty response data"))
        }
    }

    async fn close(&self) -> Result<()> {
        // Nothing special to do for TCP connections
        Ok(())
    }
}
