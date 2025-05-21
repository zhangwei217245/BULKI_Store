use anyhow::Result;
use commons::rpc::grpc::GrpcTX;
use commons::utils::RPCUtility;
use std::sync::Arc;

#[allow(dead_code)]
/// Register request handlers for the GrpcTX endpoint
pub fn register_handlers(tx: &mut GrpcTX) -> Result<()> {
    if let Some(handler) = &mut tx.context.handler {
        Arc::get_mut(handler)
            .ok_or_else(|| anyhow::anyhow!("Handler dispatcher is shared and cannot be mutated"))?
            .register("datastore::times_two", RPCUtility::common_resp_proc)
            .register("datastore::create_objects", RPCUtility::common_resp_proc)
            .register("datastore::get_object_data", RPCUtility::common_resp_proc)
            .register(
                "datastore::get_object_metadata",
                RPCUtility::common_resp_proc,
            )
            .register(
                "datastore::get_multiple_object_metadata",
                RPCUtility::common_resp_proc,
            )
            .register(
                "datastore::get_multiple_object_data",
                RPCUtility::common_resp_proc,
            )
            .register(
                "datastore::load_batch_samples",
                RPCUtility::common_resp_proc,
            )
            .register(
                "datastore::force_checkpointing",
                RPCUtility::common_resp_proc,
            )
            .register(
                "datastore::get_checkpointing_progress",
                RPCUtility::common_resp_proc,
            );
        Ok(())
    } else {
        Err(anyhow::anyhow!("Handler dispatcher not initialized"))
    }
}
