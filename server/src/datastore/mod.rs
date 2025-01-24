use anyhow::Result;
use commons::region::SerializableNDArray;
use rayon::prelude::*;

pub fn times_two(data: &Vec<u8>) -> Result<Vec<u8>> {
    match SerializableNDArray::deserialize(data) {
        Ok(array) => {
            let result = array.mapv(|x: f64| x * 2.0);
            SerializableNDArray::serialize(result)
        }
        Err(_) => Err(anyhow::anyhow!("Failed to deserialize array")),
    }
}

pub fn times_three(data: &Vec<u8>) -> Result<Vec<u8>> {
    match SerializableNDArray::deserialize(data) {
        Ok(array) => {
            let result = array.mapv(|x: f64| x * 3.0);
            SerializableNDArray::serialize(result)
        }
        Err(_) => Err(anyhow::anyhow!("Failed to deserialize array")),
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

    pub fn times_two(&self, data: &Vec<u8>) -> Result<Vec<u8>> {
        times_two(data)
    }

    pub fn times_three(&self, data: &Vec<u8>) -> Result<Vec<u8>> {
        times_three(data)
    }
}
