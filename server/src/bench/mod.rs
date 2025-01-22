use rayon::prelude::*;
use tonic::async_trait;
pub struct Bench {}

impl Bench {
    pub fn new() -> Self {
        Self {}
    }

    pub fn times_two(&self, data: &Vec<u8>) -> Vec<u8> {
        data.par_iter().map(|x| x * 2).collect()
    }

    pub fn times_three(&self, data: &Vec<u8>) -> Vec<u8> {
        data.par_iter().map(|x| x * 3).collect()
    }
}
