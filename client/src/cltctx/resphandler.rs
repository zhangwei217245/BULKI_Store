use anyhow::Result;
use commons::err::StatusCode;
use commons::handler::HandlerResult;
use commons::rpc::grpc::GrpcTX;
use commons::rpc::RPCData;
use commons::utils::TimeUtility;
use log::{debug, info};
use std::sync::Arc;

#[allow(dead_code)]
pub fn common_resp_proc(response: &mut RPCData) -> HandlerResult {
    debug!(
        "Processing response: data length: {:?}",
        response.data.as_ref().map(|v| v.len()).unwrap_or(0)
    );

    // If metadata is missing, return error
    let result_metadata = match response.metadata.as_mut() {
        Some(metadata) => {
            info!("Request issued time: {:?}, received time: {:?}, processing duration: {:?}, response received time: {:?}", 
            metadata.request_issued_time, metadata.request_received_time, metadata.processing_duration_us, TimeUtility::get_timestamp_ms());
            metadata
        }
        None => {
            return HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some("Response metadata is missing".to_string()),
            }
        }
    };

    // If handler_result is missing, return error
    match &result_metadata.handler_result {
        Some(handler_result) => handler_result.to_owned(),
        None => HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some("Handler result is missing".to_string()),
        },
    }
}

#[allow(dead_code)]
/// Register request handlers for the GrpcTX endpoint
pub fn register_handlers(tx: &mut GrpcTX) -> Result<()> {
    if let Some(handler) = &mut tx.context.handler {
        Arc::get_mut(handler)
            .ok_or_else(|| anyhow::anyhow!("Handler dispatcher is shared and cannot be mutated"))?
            .register("datastore::times_two", common_resp_proc)
            .register("datastore::create_objects", common_resp_proc)
            .register("datastore::get_object_data", common_resp_proc)
            .register("datastore::get_object_metadata", common_resp_proc);
        Ok(())
    } else {
        Err(anyhow::anyhow!("Handler dispatcher not initialized"))
    }
}
