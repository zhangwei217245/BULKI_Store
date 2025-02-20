use commons::{handler::HandlerResult, rpc::RPCData};
use log::debug;

pub fn create_objects_resp_proc(response: &mut RPCData) -> HandlerResult {
    debug!(
        "Processing response: data length: {:?}",
        response.data.as_ref().unwrap().len()
    );

    let result_metadata = response.metadata.as_mut().unwrap();
    let handler_result = result_metadata.handler_result.as_ref().unwrap();
    handler_result.to_owned()
}
