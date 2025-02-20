use std::collections::HashMap;
use std::sync::Mutex;

use crate::rpc::RPCData;
use anyhow::Result;
use inventory;
use serde::{Deserialize, Serialize};
use std::time::Instant;

pub enum HandlerType {
    Request,
    Response,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct HandlerResult {
    pub status_code: u8,
    pub message: Option<String>,
}

pub trait HandlerKind {
    fn handler_type() -> HandlerType;
}

pub struct RequestHandlerKind;
pub struct ResponseHandlerKind;

impl HandlerKind for RequestHandlerKind {
    fn handler_type() -> HandlerType {
        HandlerType::Request
    }
}

impl HandlerKind for ResponseHandlerKind {
    fn handler_type() -> HandlerType {
        HandlerType::Response
    }
}

impl HandlerType {
    fn prefix(&self) -> &'static str {
        match self {
            HandlerType::Request => "req::",
            HandlerType::Response => "resp::",
        }
    }

    fn prefix_name(&self, name: &str) -> String {
        format!("{}{}", self.prefix(), name)
    }
}

pub trait Handler {
    fn handle(&self, data: &mut RPCData) -> HandlerResult;
}

// Implement Handler for standalone functions
impl<F> Handler for F
where
    F: Fn(&mut RPCData) -> HandlerResult,
{
    fn handle(&self, data: &mut RPCData) -> HandlerResult {
        self(data)
    }
}

pub trait RequestHandlerRegistration {
    fn register_handlers(&self, dispatcher: &mut HandlerDispatcher<RequestHandlerKind>);
}

pub trait ResponseHandlerRegistration {
    fn register_handlers(&self, dispatcher: &mut HandlerDispatcher<ResponseHandlerKind>);
}

// Concrete type for request handler registration
pub struct RequestHandlerRegistrationImpl;
impl RequestHandlerRegistration for RequestHandlerRegistrationImpl {
    fn register_handlers(&self, dispatcher: &mut HandlerDispatcher<RequestHandlerKind>) {
        dispatcher.register_all();
    }
}

// Concrete type for response handler registration
pub struct ResponseHandlerRegistrationImpl;
impl ResponseHandlerRegistration for ResponseHandlerRegistrationImpl {
    fn register_handlers(&self, dispatcher: &mut HandlerDispatcher<ResponseHandlerKind>) {
        dispatcher.register_all();
    }
}

inventory::collect!(RequestHandlerRegistrationImpl);
inventory::collect!(ResponseHandlerRegistrationImpl);

pub struct HandlerDispatcher<H: HandlerKind> {
    functions: HashMap<String, Mutex<Box<dyn Handler + Send + Sync>>>,
    _phantom: std::marker::PhantomData<H>,
}

impl<H: HandlerKind> HandlerDispatcher<H> {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn register<T>(&mut self, name: &str, handler: T) -> &mut Self
    where
        T: Handler + Send + Sync + 'static,
    {
        self.functions.insert(
            H::handler_type().prefix_name(name),
            Mutex::new(Box::new(handler)),
        );
        self
    }

    pub fn register_all(&mut self)
    where
        H: 'static,
    {
        if std::any::TypeId::of::<H>() == std::any::TypeId::of::<RequestHandlerKind>() {
            // Safe to transmute since we verified the type
            let this = unsafe {
                std::mem::transmute::<
                    &mut HandlerDispatcher<H>,
                    &mut HandlerDispatcher<RequestHandlerKind>,
                >(self)
            };
            for registration in inventory::iter::<RequestHandlerRegistrationImpl>().into_iter() {
                registration.register_handlers(this);
            }
        } else if std::any::TypeId::of::<H>() == std::any::TypeId::of::<ResponseHandlerKind>() {
            // Safe to transmute since we verified the type
            let this = unsafe {
                std::mem::transmute::<
                    &mut HandlerDispatcher<H>,
                    &mut HandlerDispatcher<ResponseHandlerKind>,
                >(self)
            };
            for registration in inventory::iter::<ResponseHandlerRegistrationImpl>().into_iter() {
                registration.register_handlers(this);
            }
        }
    }

    pub async fn handle(&self, mut message: RPCData) -> Result<RPCData> {
        // Get handler name before borrowing message
        let handler_name = message
            .metadata
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Message metadata is missing"))?
            .handler_name
            .clone();

        let handler_name = H::handler_type().prefix_name(&handler_name);

        let handler = self
            .functions
            .get(&handler_name)
            .ok_or_else(|| anyhow::anyhow!("No handler found for {}", handler_name))?;

        let start_time = Instant::now();

        // Call the handler with mutable reference to message
        let handler_result = handler
            .lock()
            .map_err(|_| anyhow::anyhow!("Handler mutex poisoned"))?
            .handle(&mut message);

        // Update the metadata with handler result and timing information
        if let Some(metadata) = &mut message.metadata {
            metadata.handler_result = Some(handler_result);
            metadata.processing_duration_us = Some(start_time.elapsed().as_micros() as u64);
        }

        Ok(message)
    }
}
