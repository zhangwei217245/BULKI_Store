use commons::handler::HandlerResult;
use commons::rpc::{RPCData, StatusCode};
use log::debug;

pub fn check(data: &mut RPCData) -> HandlerResult {
    debug!("Health check received: {:?}", data);
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

#[allow(dead_code)]
pub struct HealthCheck {}

impl HealthCheck {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {}
    }
}
