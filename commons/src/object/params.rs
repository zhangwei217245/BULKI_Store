use super::types::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Structure to hold object creation parameters sent by client
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateObjectParams {
    pub obj_id: u128,
    pub parent_id: Option<u128>,
    pub name: String,
    pub initial_metadata: Option<HashMap<String, MetadataValue>>,
    pub array_data: Option<SupportedRustArrayD>,
    pub client_rank: u32, // MPI rank of the client
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetMajorObjectParams {
    pub major_obj_id: u128,
}
