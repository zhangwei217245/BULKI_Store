use std::env;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use log::{debug, info};

use crate::err::StatusCode;
use crate::handler::HandlerResult;
use crate::rpc::RPCData;

pub struct TimeUtility;
pub struct NetworkUtility;
pub struct FileUtility;

pub struct RPCUtility;

impl TimeUtility {
    pub fn get_timestamp_ms() -> u64 {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        since_epoch.as_millis() as u64
    }

    #[allow(dead_code)]
    pub fn get_timestamp_us() -> u64 {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        since_epoch.as_micros() as u64
    }

    #[allow(dead_code)]
    pub fn get_timestamp_ns() -> u64 {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        since_epoch.as_nanos() as u64
    }
}

impl NetworkUtility {
    const MAX_PORT_ATTEMPTS: u16 = 10;

    pub fn find_available_port(base_port: u16, rank: u16) -> Option<u16> {
        let base_port = base_port + rank;
        (base_port..base_port + Self::MAX_PORT_ATTEMPTS)
            .find(|&port| TcpListener::bind(format!("0.0.0.0:{}", port)).is_ok())
    }
}

impl FileUtility {
    pub fn get_pdc_tmp_dir() -> PathBuf {
        env::var_os("PDC_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./.pdc_tmp"))
    }
}

impl RPCUtility {
    #[allow(dead_code)]
    pub fn common_resp_proc(response: &mut RPCData) -> HandlerResult {
        debug!(
            "Processing response: data length: {:?}",
            response.data.as_ref().map(|v| v.len()).unwrap_or(0)
        );

        // If metadata is missing, return error
        let result_metadata = match response.metadata.as_mut() {
            Some(metadata) => {
                info!("Request id: {:?}, handler name: {:?}, issued time: {:?}, received time: {:?}, processing duration: {:?}, response received time: {:?}", 
            metadata.request_id, metadata.handler_name, metadata.request_issued_time, metadata.request_received_time, metadata.processing_duration_us, TimeUtility::get_timestamp_ms());
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
}
