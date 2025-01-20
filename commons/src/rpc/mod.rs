pub mod rpc {
    use anyhow::Result;
    use mpi::topology::SimpleCommunicator;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::broadcast;
    use uuid::Uuid;

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

    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum MessageType {
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

    pub trait RPCHandler {
        async fn handle_response(&self, metadata: RPCMetadata, data: Vec<u8>) -> Result<()>;
    }

    pub struct TxContext<A> {
        pub rpc_id: String,
        pub rank: usize,
        pub size: usize,
        pub world: Option<SimpleCommunicator>,
        pub server_addresses: Option<Vec<A>>,
    }

    pub struct RxContext<A> {
        pub rpc_id: String,
        pub rank: usize,
        pub size: usize,
        pub world: Option<SimpleCommunicator>,
        pub server_addresses: Option<Vec<A>>,
        pub address: Option<A>,
    }

    impl<A> RxContext<A> {
        pub fn new(rpc_id: String) -> Self {
            Self {
                rpc_id,
                rank: 0,
                size: 0,
                world: None,
                server_addresses: None,
                address: None,
            }
        }
    }

    impl<A> TxContext<A> {
        pub fn new(rpc_id: String) -> Self {
            Self {
                rpc_id: rpc_id,
                rank: 0,
                size: 0,
                world: None,
                server_addresses: None,
            }
        }
    }

    pub trait RxEndpoint {
        type Address;
        type Handler;
        type RPCData;

        // initialize the RxEndpoint with MPI world if available
        fn initialize(&mut self, world: Option<SimpleCommunicator>) -> Result<()>;
        // exchange server addresses so that every RxEndpoint rank knows each other
        fn exchange_addresses(&mut self) -> Result<()>;
        // write the RxEndpoint addresses to a file named with rx_{rpc_id}.txt
        fn write_addresses(&self) -> Result<()>;
        // start to listen for incoming requests asynchronously
        // can be used in two ways:
        // 1. await server.listen(handler) - for blocking behavior
        // 2. tokio::spawn(server.listen(handler)) - for non-blocking behavior
        async fn listen<F, Fut>(&mut self, shutdown_handler: F) -> Result<()>
        where
            F: FnOnce() -> Fut + Send + 'static,
            Fut: std::future::Future<Output = Result<()>> + Send + 'static;
        // respond to a request using the handler
        fn respond(&self, msg: &Self::RPCData) -> Result<()>;
        // close the endpoint
        fn close(&self) -> Result<()>;
    }

    pub trait TxEndpoint {
        type Address;
        type Handler: RPCHandler;
        type RPCData;

        fn initialize(&mut self, world: Option<SimpleCommunicator>) -> Result<()>;
        fn discover_servers(&mut self) -> Result<()>;
        async fn send_message(&self, rx_id: usize, msg: &Self::RPCData) -> Result<()>;
        fn close(&self) -> Result<()>;
    }
}