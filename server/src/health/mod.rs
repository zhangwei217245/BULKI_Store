use anyhow::Result;
pub fn check(data: &Vec<u8>) -> Result<Vec<u8>> {
    Ok(vec![1])
}
pub struct HealthCheck {}

impl HealthCheck {
    pub fn new() -> Self {
        Self {}
    }

    pub fn check(&self, data: &Vec<u8>) -> Result<Vec<u8>> {
        Ok(vec![1]) // Return a simple byte vector indicating healthy status
    }
}
