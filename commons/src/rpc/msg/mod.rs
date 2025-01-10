use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RPCMetadata {
    pub client_rank: u32,
    pub server_rank: u32,
    pub request_id: u64,
    pub request_issued_time: u64,
    pub request_received_time: u64,
    pub request_processed_time: u64,
    pub handler_name: String,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RPCData {
    pub data: Vec<u8>,
    pub metadata: Option<RPCMetadata>,
}
