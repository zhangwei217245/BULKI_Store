use anyhow::Result;
use commons::handler::HandlerResult;
use commons::region::SerializableNDArray;
use commons::rpc::{RPCData, StatusCode};
use rayon::prelude::*;

pub fn times_two(data: &mut RPCData) -> HandlerResult {
    match SerializableNDArray::deserialize(&data.data) {
        Ok(array) => {
            let result = array.mapv(|x: f64| x * 2.0);
            data.data = SerializableNDArray::serialize(result).unwrap();
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        Err(_) => HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(String::from("Failed to deserialize array")),
        },
    }
}

pub fn times_three(data: &mut RPCData) -> HandlerResult {
    match SerializableNDArray::deserialize(&data.data) {
        Ok(array) => {
            let result = array.mapv(|x: f64| x * 3.0);
            data.data = SerializableNDArray::serialize(result).unwrap();
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        Err(_) => HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(String::from("Failed to deserialize array")),
        },
    }
}

#[derive(Default)]
pub struct BulkiStore {
    // Add any store-specific fields here
}

impl BulkiStore {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }
}
