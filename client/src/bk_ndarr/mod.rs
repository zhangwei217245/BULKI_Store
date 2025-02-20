use commons::handler::HandlerResult;
use commons::rpc::{RPCData, StatusCode};
use log::debug;

pub fn process_response(data: &mut RPCData) -> HandlerResult {
    debug!(
        "Processing response data length: {:?}",
        data.data.as_ref().unwrap().len()
    );
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}
