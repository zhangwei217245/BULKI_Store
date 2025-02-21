use anyhow::Result;
use commons::rpc::grpc::GrpcTX;
use std::sync::Arc;

/// Register request handlers for the GrpcTX endpoint for server < - > server communication
pub fn register_handlers(tx: &mut GrpcTX) -> Result<()> {
    if let Some(handler) = &mut tx.context.handler {
        Arc::get_mut(handler)
            .ok_or_else(|| anyhow::anyhow!("Handler dispatcher is shared and cannot be mutated"))?
            .register("health::check", crate::health::check);
        Ok(())
    } else {
        Err(anyhow::anyhow!("Handler dispatcher not initialized"))
    }
}
