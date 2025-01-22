use commons::req_handler;
use rayon::prelude::*;

#[derive(Default)]
pub struct BulkiStore {
    // Add any store-specific fields here
}

impl BulkiStore {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn times_two(&self, data: &Vec<u8>) -> Vec<u8> {
        data.par_iter().map(|x| x * 2).collect()
    }

    pub fn times_three(&self, data: &Vec<u8>) -> Vec<u8> {
        data.par_iter().map(|x| x * 3).collect()
    }
}
