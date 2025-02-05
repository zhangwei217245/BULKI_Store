use anyhow::Result;
use async_trait::async_trait;
#[cfg(feature = "mpi")]
use mpi::topology::SimpleCommunicator;
use mpi::traits::Communicator;
use serde::{Deserialize, Serialize};
use std::{
    default::Default,
    env,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, Barrier};

use crate::handler::{HandlerDispatcher, HandlerResult, RequestHandlerKind, ResponseHandlerKind};

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
}

impl MessageType {
    pub fn to_u8(self) -> u8 {
        match self {
            MessageType::Request => 0,
            MessageType::Response => 1,
        }
    }

    pub fn from_u8(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(MessageType::Request),
            1 => Ok(MessageType::Response),
            _ => Err(format!("Invalid MessageType value: {}", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum StatusCode {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Default for StatusCode {
    fn default() -> Self {
        StatusCode::Ok
    }
}

impl From<StatusCode> for u8 {
    fn from(code: StatusCode) -> Self {
        code as u8
    }
}

impl TryFrom<u8> for StatusCode {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(StatusCode::Ok),
            1 => Ok(StatusCode::Cancelled),
            2 => Ok(StatusCode::Unknown),
            3 => Ok(StatusCode::InvalidArgument),
            4 => Ok(StatusCode::DeadlineExceeded),
            5 => Ok(StatusCode::NotFound),
            6 => Ok(StatusCode::AlreadyExists),
            7 => Ok(StatusCode::PermissionDenied),
            8 => Ok(StatusCode::ResourceExhausted),
            9 => Ok(StatusCode::FailedPrecondition),
            10 => Ok(StatusCode::Aborted),
            11 => Ok(StatusCode::OutOfRange),
            12 => Ok(StatusCode::Unimplemented),
            13 => Ok(StatusCode::Internal),
            14 => Ok(StatusCode::Unavailable),
            15 => Ok(StatusCode::DataLoss),
            16 => Ok(StatusCode::Unauthenticated),
            _ => Err(anyhow::anyhow!("Invalid status code: {}", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RPCMetadata {
    pub client_rank: u32,
    pub server_rank: u32,
    pub request_id: u64,
    pub request_issued_time: u64,
    pub request_received_time: u64,
    pub processing_duration_us: Option<u64>, // Duration in microseconds
    pub message_type: MessageType,
    pub handler_name: String,
    pub handler_result: Option<HandlerResult>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RPCData {
    pub data: Vec<u8>,
    pub metadata: Option<RPCMetadata>,
}

impl RPCData {
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self {
            data: data.into(),
            metadata: None,
        }
    }
}

#[derive(Default, Clone)]
pub struct TxContext<A> {
    pub rpc_id: String,
    pub rank: usize,
    pub size: usize,
    #[cfg(feature = "mpi")]
    pub world: Option<Arc<SimpleCommunicator>>,
    #[cfg(not(feature = "mpi"))]
    pub world: Option<()>,
    pub server_addresses: Option<Vec<A>>,
    pub handler: Option<Arc<HandlerDispatcher<ResponseHandlerKind>>>,
}

impl<A> TxContext<A> {
    #[cfg(feature = "mpi")]
    pub fn new(rpc_id: String, world: Option<Arc<SimpleCommunicator>>) -> Self {
        Self {
            rpc_id,
            rank: world.as_ref().map(|w| w.rank()).unwrap_or(0) as usize,
            size: world.as_ref().map(|w| w.size()).unwrap_or(1) as usize,
            world,
            server_addresses: None,
            handler: None,
        }
    }

    #[cfg(not(feature = "mpi"))]
    pub fn new(rpc_id: String, world: Option<()>) -> Self {
        Self {
            rpc_id,
            rank: 0,
            size: 1,
            world,
            server_addresses: None,
            handler: None,
        }
    }
}

#[derive(Default, Clone)]
pub struct RxContext<A> {
    pub rpc_id: String,
    pub rank: usize,
    pub size: usize,
    #[cfg(feature = "mpi")]
    pub world: Option<Arc<SimpleCommunicator>>,
    #[cfg(not(feature = "mpi"))]
    pub world: Option<()>,
    pub server_addresses: Option<Vec<A>>,
    pub address: Option<A>,
    pub handler: Option<Arc<HandlerDispatcher<RequestHandlerKind>>>,
}

impl<A> RxContext<A> {
    #[cfg(feature = "mpi")]
    pub fn new(rpc_id: String, world: Option<Arc<SimpleCommunicator>>) -> Self {
        Self {
            rpc_id,
            rank: 0,
            size: 1,
            world,
            server_addresses: None,
            address: None,
            handler: None,
        }
    }

    #[cfg(not(feature = "mpi"))]
    pub fn new(rpc_id: String, world: Option<()>) -> Self {
        Self {
            rpc_id,
            rank: 0,
            size: 1,
            world,
            server_addresses: None,
            address: None,
            handler: None,
        }
    }

    #[cfg(feature = "mpi")]
    pub fn initialize_mpi(&mut self) {
        if let Some(world) = &self.world {
            self.rank = world.rank() as usize;
            self.size = world.size() as usize;
        }
    }
}

#[async_trait]
pub trait RxEndpoint {
    type Address;

    fn initialize<F>(&mut self, rx_id: u16, handler_register: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>;
    // start to listen for incoming requests asynchronously
    // can be used in two ways:
    // 1. await server.listen(handler) - for blocking behavior
    // 2. tokio::spawn(server.listen(handler)) - for non-blocking behavior
    async fn listen(
        &mut self,
        tokio_barrier: Arc<Barrier>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), anyhow::Error>;
    // exchange the RxEndpoint addresses with the server
    fn exchange_addresses(&mut self) -> Result<()>;
    // write the RxEndpoint addresses to a file named with rx_{rpc_id}.txt
    fn write_addresses(&self) -> Result<()>;
    // respond to a request using the handler
    async fn respond_request(&self, msg: RPCData) -> Result<RPCData>;
    // close the endpoint
    fn close(&self) -> Result<()>;
}

#[async_trait]
pub trait TxEndpoint {
    type Address;

    fn initialize<F>(&mut self, handler_register: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>;

    fn discover_servers(&mut self) -> Result<()>;
    // send a message to a server identified by its index
    async fn send_message(&self, rx_id: usize, handler_name: &str, msg: RPCData)
        -> Result<RPCData>;
    async fn process_response(&self, response: RPCData) -> Result<RPCData>;
    async fn close(&self) -> Result<()>;
}
