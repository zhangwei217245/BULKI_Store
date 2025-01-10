use crate::dispatcher::{DispatchResult, Dispatcher, HandlerFn};
// common logic for RPC receiver here
// import msg mod
use crate::msg::{RPCData, RPCMetadata};
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct RpcReceiverConfig {
    pub receiver_rank: i32,
    pub host: String,
    pub port: u16,
}

pub struct RpcReceiver {
    config: RpcReceiverConfig,
    dispatcher: Option<Dispatcher>,
}

impl RpcReceiver {
    pub fn new(config: RpcReceiverConfig) -> Self {
        Self {
            config,
            dispatcher: None,
        }
    }

    pub fn set_dispatcher(&mut self, dispatcher: Dispatcher) {
        self.dispatcher = Some(dispatcher);
    }

    pub async fn register_handler(&self, module: &str, func: &str, handler: HandlerFn) {
        let dispatcher = self.dispatcher.as_ref().unwrap();
        dispatcher.register_handler(module, func, handler).await;
    }

    pub async fn unregister_handler(&self, module: &str, func: &str) {
        let dispatcher = self.dispatcher.as_ref().unwrap();
        dispatcher.unregister_handler(module, func);
    }

    pub async fn dispatch(&self, func_path: &str, payload: Vec<u8>) -> DispatchResult<Vec<u8>> {
        let dispatcher = self.dispatcher.as_ref().unwrap();
        dispatcher.dispatch(func_path, payload).await
    }

    pub fn get_config(&self) -> &RpcReceiverConfig {
        &self.config
    }
}

pub trait RpcReceiverBehavior {
    type Error: Error + Debug;

    /// Start the receiver, listening for incoming connections/requests.
    fn start(&mut self) -> Result<(), Self::Error>;

    /// Stop the receiver gracefully.
    fn stop(&mut self) -> Result<(), Self::Error>;

    /// Low-level hook to accept and decode a request.
    fn receive_request(&self) -> Result<RPCData, Self::Error>;

    /// Process a request with timing, logging, and dispatch.
    fn process_request(&self, request: RPCData) -> Result<RPCData, Self::Error> {
        let start_time = Instant::now();
        // Optionally: log request reception, acquire connection/resource etc.
        let response = self.dispatch_request(request);
        let duration = start_time.elapsed();
        println!("Request processed in {:?}", duration);
        response
    }

    /// Dispatch the request to application-specific logic.
    fn dispatch_request(&self, request: RPCData) -> Result<RPCData, Self::Error>;

    /// Hook to serialize and send a response.
    fn send_response(&self, response: RPCData) -> Result<(), Self::Error>;

    /// Manage connection pool: acquire a connection/resource.
    fn with_connection<F, R>(&self, operation: F) -> R
    where
        F: FnOnce() -> R;
}
