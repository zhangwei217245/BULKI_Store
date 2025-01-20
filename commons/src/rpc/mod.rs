use anyhow::Result;
#[cfg(feature = "mpi")]
use mpi::topology::SimpleCommunicator;
use serde::{Deserialize, Serialize};
use std::{
    default::Default,
    env,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

pub mod grpc; // Expose the grpc submodule

pub enum RPCImpl {
    Grpc,
}

impl std::str::FromStr for RPCImpl {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "grpc" => Ok(RPCImpl::Grpc),
            _ => Err(format!("Unknown RPC implementation: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub enum MessageType {
    #[default]
    Request,
    Response,
    Error,
}

impl MessageType {
    pub fn to_u8(self) -> u8 {
        match self {
            MessageType::Request => 0,
            MessageType::Response => 1,
            MessageType::Error => 2,
        }
    }

    pub fn from_u8(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(MessageType::Request),
            1 => Ok(MessageType::Response),
            2 => Ok(MessageType::Error),
            _ => Err(format!("Invalid MessageType value: {}", value)),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RPCMetadata {
    pub client_rank: u32,
    pub server_rank: u32,
    pub request_id: u64,
    pub request_issued_time: u64,
    pub request_received_time: u64,
    pub request_processed_time: u64,
    pub message_type: MessageType,
    pub handler_name: String,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RPCData {
    pub data: Vec<u8>,
    pub metadata: Option<RPCMetadata>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HandlerResult {
    pub success: bool,
    pub code: u32,
    pub message: String,
}

pub struct RequestHandler;
impl RequestHandler {
    fn handle_request(
        &self,
        request: RPCData,
    ) -> impl std::future::Future<Output = Result<HandlerResult>> + Send + 'static {
        async move {
            Ok(HandlerResult {
                success: true,
                code: 0,
                message: "Success".to_string(),
            })
        }
    }
}

pub struct ResponseHandler;

impl ResponseHandler {
    fn handle_response(
        &self,
        response: RPCData,
    ) -> impl std::future::Future<Output = Result<HandlerResult>> + Send + 'static {
        async move {
            Ok(HandlerResult {
                success: true,
                code: 0,
                message: "Success".to_string(),
            })
        }
    }
}

// #[derive(Clone)]
pub struct TxContext<A> {
    pub rpc_id: String,
    pub rank: usize,
    pub size: usize,
    #[cfg(feature = "mpi")]
    pub world: Option<SimpleCommunicator>,
    #[cfg(not(feature = "mpi"))]
    pub world: Option<()>,
    pub server_addresses: Option<Vec<A>>,
}

impl<A> TxContext<A> {
    #[cfg(feature = "mpi")]
    pub fn new(rpc_id: String, world: Option<SimpleCommunicator>) -> Self {
        Self {
            rpc_id,
            rank: 0,
            size: 0,
            world,
            server_addresses: None,
        }
    }

    #[cfg(not(feature = "mpi"))]
    pub fn new(rpc_id: String, world: Option<()>) -> Self {
        Self {
            rpc_id,
            rank: 0,
            size: 0,
            world,
            server_addresses: None,
        }
    }
}

// #[derive(Clone)]
pub struct RxContext<A> {
    pub rpc_id: String,
    pub rank: usize,
    pub size: usize,
    #[cfg(feature = "mpi")]
    pub world: Option<SimpleCommunicator>,
    #[cfg(not(feature = "mpi"))]
    pub world: Option<()>,
    pub server_addresses: Option<Vec<A>>,
    pub address: Option<A>,
}

impl<A> RxContext<A> {
    #[cfg(feature = "mpi")]
    pub fn new(rpc_id: String, world: Option<SimpleCommunicator>) -> Self {
        Self {
            rpc_id,
            rank: 0,
            size: 0,
            world,
            server_addresses: None,
            address: None,
        }
    }
    #[cfg(not(feature = "mpi"))]
    pub fn new(rpc_id: String, world: Option<()>) -> Self {
        Self {
            rpc_id,
            rank: 0,
            size: 0,
            world,
            server_addresses: None,
            address: None,
        }
    }
}

pub trait RxEndpoint {
    type Address;
    type Handler: RequestHandler;

    fn initialize(&mut self) -> Result<()>;
    fn exchange_addresses(&mut self) -> Result<()>;
    // write the RxEndpoint addresses to a file named with rx_{rpc_id}.txt
    fn write_addresses(&self) -> Result<()>;
    // start to listen for incoming requests asynchronously
    // can be used in two ways:
    // 1. await server.listen(handler) - for blocking behavior
    // 2. tokio::spawn(server.listen(handler)) - for non-blocking behavior
    fn listen<F, Fut>(
        &self,
        shutdown_handler: F,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static;
    // respond to a request using the handler
    fn respond(&self, msg: &RPCData) -> Result<()>;
    // close the endpoint
    fn close(&self) -> Result<()>;
}

pub trait TxEndpoint {
    type Address;
    type Handler: ResponseHandler;

    fn initialize(&mut self) -> Result<()>;
    fn discover_servers(&mut self) -> Result<()>;
    // send a message to a server identified by its index
    fn send_message(
        &self,
        rx_id: usize,
        msg: &RPCData,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static;
    fn close(&self) -> Result<()>;
}

pub struct RXTXUtils;

impl RXTXUtils {
    pub fn get_timestamp_ms() -> u64 {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        since_epoch.as_millis() as u64
    }

    pub fn get_pdc_tmp_dir() -> PathBuf {
        let pdc_tmp_dir = env::var("PDC_TMP_DIR").expect("PDC_TMP_DIR not set");
        PathBuf::from(pdc_tmp_dir)
    }
}
