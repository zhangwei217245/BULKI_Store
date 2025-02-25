use anyhow::Result;
use commons::rpc::grpc::GrpcRX;
use std::sync::Arc;

/// Register request handlers for the GrpcRX endpoint
pub fn register_handlers(rx: &mut GrpcRX) -> Result<()> {
    if let Some(handler) = &mut rx.context.handler {
        Arc::get_mut(handler)
            .ok_or_else(|| anyhow::anyhow!("Handler dispatcher is shared and cannot be mutated"))?
            .register("health::check", crate::health::check)
            .register("bench::times_two", crate::bench::times_two)
            .register("bench::times_three", crate::bench::times_three)
            .register("datastore::times_two", crate::datastore::times_two)
            .register("datastore::times_three", crate::datastore::times_three)
            .register(
                "datastore::create_objects",
                crate::datastore::create_objects,
            )
            .register(
                "datastore::get_object_data",
                crate::datastore::get_object_data,
            )
            .register(
                "datastore::get_object_metadata",
                crate::datastore::get_object_metadata,
            )
            .register(
                "datastore::update_metadata",
                crate::datastore::update_metadata,
            )
            .register("datastore::update_array", crate::datastore::update_array)
            .register("datastore::delete_object", crate::datastore::delete_object);
        Ok(())
    } else {
        Err(anyhow::anyhow!("Handler dispatcher not initialized"))
    }
}
