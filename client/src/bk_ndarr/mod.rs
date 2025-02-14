use anyhow::Result;

use commons::handler::HandlerResult;
use commons::rpc::{RPCData, StatusCode};
use log::{debug, error};

pub fn process_response(data: &mut RPCData) -> HandlerResult {
    debug!("Processing response: {:?}", data);
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}
