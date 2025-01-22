use std::collections::HashMap;

use crate::rpc::{RPCData, StatusCode};
use crate::utils::TimeUtility;
use anyhow::Result;
use inventory;
use serde::{Deserialize, Serialize};

enum HandlerType {
    Request,
    Response,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct HandlerResult {
    pub status_code: u8,
    pub message: String,
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
    functions: HashMap<String, fn(&Vec<u8>) -> Result<Vec<u8>, HandlerResult>>,
    _phantom: std::marker::PhantomData<H>,
}

impl<H: HandlerKind> HandlerDispatcher<H> {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn register(
        &mut self,
        name: &str,
        handler: fn(&Vec<u8>) -> Result<Vec<u8>, HandlerResult>,
    ) {
        self.functions
            .insert(H::handler_type().prefix_name(name), handler);
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
        // Ensure we have metadata
        let metadata = message
            .metadata
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Message missing metadata"))?;

        // Get the handler name and try to find the function
        let handler_name = H::handler_type().prefix_name(&metadata.handler_name);
        let handler = self
            .functions
            .get(&handler_name)
            .ok_or_else(|| anyhow::anyhow!("Handler not found: {}", handler_name))?;

        // Execute the handler and capture any errors
        let result = match handler(&message.data) {
            Ok(data) => {
                metadata.handler_result = Some(HandlerResult {
                    status_code: StatusCode::Ok as u8,
                    message: "Success".to_string(),
                });
                data
            }
            Err(e) => {
                metadata.handler_result = Some(e.clone());
                return Err(anyhow::anyhow!("Handler error: {:?}", e));
            }
        };
        // Update the response data
        message.data = result;

        Ok(message)
    }
}
