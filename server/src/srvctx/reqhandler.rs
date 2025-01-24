use anyhow::Result;
use commons::handler::{HandlerDispatcher, HandlerResult, RequestHandlerKind};
use commons::rpc::grpc::GrpcRX;
use std::sync::{Arc, Mutex};

/// Register request handlers for the GrpcRX endpoint
pub fn register_handlers(rx: &mut GrpcRX) -> Result<()> {
    if let Some(handler) = &mut rx.context.handler {
        Arc::get_mut(handler)
            .ok_or_else(|| anyhow::anyhow!("Handler dispatcher is shared and cannot be mutated"))?
            .register("bench::times_two", crate::bench::times_two)
            .register("bench::times_three", crate::bench::times_three)
            .register("datastore::times_two", crate::datastore::times_two)
            .register("datastore::times_three", crate::datastore::times_three);
        Ok(())
    } else {
        Err(anyhow::anyhow!("Handler dispatcher not initialized"))
    }
}
