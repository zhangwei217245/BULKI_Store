use anyhow::Result;
use commons::rpc::grpc::GrpcTX;
use std::sync::Arc;

use crate::bk_ndarr;
use crate::datastore;

/// Register request handlers for the GrpcTX endpoint
pub fn register_handlers(tx: &mut GrpcTX) -> Result<()> {
    if let Some(handler) = &mut tx.context.handler {
        Arc::get_mut(handler)
            .ok_or_else(|| anyhow::anyhow!("Handler dispatcher is shared and cannot be mutated"))?
            .register("datastore::times_two", bk_ndarr::process_response)
            .register(
                "datastore::create_objects",
                datastore::create_objects_resp_proc,
            );
        Ok(())
    } else {
        Err(anyhow::anyhow!("Handler dispatcher not initialized"))
    }
}
