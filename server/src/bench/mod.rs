use anyhow::Result;
use rayon::prelude::*;

pub fn times_two(data: &Vec<u8>) -> Result<Vec<u8>> {
    println!("times_two");
    Ok(data.par_iter().map(|x| x * 2).collect())
}

pub fn times_three(data: &Vec<u8>) -> Result<Vec<u8>> {
    println!("times_three");
    Ok(data.par_iter().map(|x| x * 3).collect())
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
