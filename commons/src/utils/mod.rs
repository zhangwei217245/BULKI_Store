use std::env;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TimeUtility;
pub struct NetworkUtility;
pub struct FileUtility;

impl TimeUtility {
    pub fn get_timestamp_ms() -> u64 {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        since_epoch.as_millis() as u64
    }
}

impl NetworkUtility {
    const MAX_PORT_ATTEMPTS: u16 = 10;

    pub fn find_available_port(base_port: u16) -> Option<u16> {
        (base_port..base_port + Self::MAX_PORT_ATTEMPTS)
            .find(|&port| TcpListener::bind(format!("0.0.0.0:{}", port)).is_ok())
    }
}

impl FileUtility {
    pub fn get_pdc_tmp_dir() -> PathBuf {
        env::var_os("PDC_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./pdc_tmp"))
    }
}
