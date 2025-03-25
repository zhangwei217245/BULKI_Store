use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{env, thread};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::{debug, info};
use sysinfo::{get_current_pid, System};

use crate::err::StatusCode;
use crate::handler::HandlerResult;
use crate::rpc::RPCData;

pub struct TimeUtility;
pub struct NetworkUtility;
pub struct FileUtility;
pub struct SystemUtility;
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
                let current_time = TimeUtility::get_timestamp_us();
                let request_transfer = metadata.request_received_time - metadata.request_issued_time;
                info!("Request id: {:?}, handler name: {:?}, request-transfer: {:?}  processing-duration: {:?}  calculated-response-transfer: {:?}  total-time: {:?}", 
                metadata.request_id, metadata.handler_name, 
                request_transfer, // request-transfer
                metadata.processing_duration_us, // processing duration without serde
                current_time - (metadata.request_issued_time + metadata.processing_duration_us.unwrap_or(0) + request_transfer), // calculated response-transfer
                current_time - metadata.request_issued_time); // total time.
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

impl SystemUtility {
    pub fn monitor_memory_usage(running: Arc<AtomicBool>, frequency_per_min: u64, source: String,rank: usize, size: usize) -> JoinHandle<()> {
        return thread::spawn(move || {
            let interval = Duration::from_secs(60/frequency_per_min); // Log every minute by default
            let mut sys = System::new_all();
            while running.load(Ordering::SeqCst) {
                // Refresh system information
                sys.refresh_all();
                // Get the current process ID
                let pid = get_current_pid().expect("Failed to get current PID");
                // Retrieve and print the memory usage in kilobytes
                if let Some(process) = sys.process(pid) {
                    info!(
                        "[{} R{}/S{}] Memory usage: {} MB",
                        source,
                        rank,
                        size,
                        process.memory() / 1024 / 1024
                    );
                } else {
                    info!(
                        "[{} R{}/S{}] Current process not found!",
                        source,
                        rank,
                        size
                    );
                }
                // Sleep for the specified interval
                thread::sleep(interval);
            }
        });
    }
}
