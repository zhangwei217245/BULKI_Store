use super::types::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Structure to hold object creation parameters sent by client
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateObjectParams {
    pub obj_id: u128,
    pub obj_name: String,
    pub parent_id: Option<u128>,
    pub obj_name_key: String,
    pub initial_metadata: Option<HashMap<String, MetadataValue>>,
    pub array_data: Option<SupportedRustArrayD>,
    pub client_rank: u32, // MPI rank of the client
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetObjectSliceParams {
    pub obj_id: u128,
    pub region: Option<Vec<SerializableSliceInfoElem>>,
    pub sub_obj_regions: Option<Vec<(String, Option<Vec<SerializableSliceInfoElem>>)>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetObjectSliceResponse {
    pub obj_id: u128,
    pub array_slice: Option<SupportedRustArrayD>,
    pub sub_obj_slices: Option<Vec<(u128, Option<String>, Option<SupportedRustArrayD>)>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SerializableMetaKeySpec {
    Simple(Vec<String>),
    WithObject(HashMap<String, Vec<String>>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetObjectMetaParams {
    pub obj_id: u128,
    pub meta_keys: Option<Vec<String>>,
    pub sub_meta_keys: Option<SerializableMetaKeySpec>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetObjectMetaResponse {
    pub obj_id: u128,
    pub obj_name: String,
    pub metadata: Option<HashMap<String, MetadataValue>>,
    pub sub_obj_metadata: Option<Vec<(u128, String, HashMap<String, MetadataValue>)>>,
}
