use anyhow::Result;
use commons::handler::HandlerResult;
use commons::rpc::{RPCData, StatusCode};
use rayon::prelude::*;

pub fn times_two(data: &mut RPCData) -> HandlerResult {
    println!("times_two");
    // Modify data in place
    data.data = data.data.par_iter().map(|x| x * 2).collect();
    // Return success status
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

pub fn times_three(data: &mut RPCData) -> HandlerResult {
    println!("times_three");
    // Modify data in place
    data.data = data.data.par_iter().map(|x| x * 3).collect();
    // Return success status
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

pub struct Bench {}

impl Bench {
    pub fn new() -> Self {
        Self {}
    }

    pub fn times_two(&self, data: &Vec<u8>) -> Result<Vec<u8>> {
        println!("times_two");
        Ok(data.par_iter().map(|x| x * 2).collect())
    }

    pub fn times_three(&self, data: &Vec<u8>) -> Result<Vec<u8>> {
        println!("times_three");
        Ok(data.par_iter().map(|x| x * 3).collect())
    }
}
