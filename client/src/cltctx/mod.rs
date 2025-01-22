use anyhow::{anyhow, Result};
use commons::rpc::grpc::GrpcTX;
use commons::rpc::{MessageType, RPCData, RPCMetadata, TxEndpoint};
use log::{debug, info};
#[cfg(feature = "mpi")]
use mpi::topology::SimpleCommunicator;
#[cfg(feature = "mpi")]
use mpi::traits::*;
use rand::Rng;
use std::sync::Arc;

pub struct ClientContext {
    #[cfg(feature = "mpi")]
    world: Option<Arc<SimpleCommunicator>>,
    #[cfg(not(feature = "mpi"))]
    world: Option<()>,
    rank: usize,
    size: usize,
    pub c2s_client: Option<GrpcTX>,
}

#[derive(Debug)]
pub struct BenchmarkStats {
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub total_duration_ms: u128,
    pub min_latency_ms: u128,
    pub max_latency_ms: u128,
    pub avg_latency_ms: f64,
}

impl ClientContext {
    pub fn new() -> Self {
        Self {
            world: None,
            rank: 0,
            size: 1,
            c2s_client: None,
        }
    }

    pub async fn initialize(&mut self) -> Result<()> {
        #[cfg(feature = "mpi")]
        {
            let (_universe, _) = mpi::initialize_with_threading(mpi::Threading::Multiple).unwrap();
            self.world = Some(Arc::new(_universe.world()));
            self.rank = self.world.as_ref().map(|w| w.rank()).unwrap_or(0) as usize;
            self.size = self.world.as_ref().map(|w| w.size()).unwrap_or(1) as usize;
        }
        #[cfg(not(feature = "mpi"))]
        {
            self.world = Some(());
            self.rank = 0;
            self.size = 1;
        }
        // Initialize client-server endpoint
        let mut c2s_client = GrpcTX::new("c2s".to_string(), self.world.clone());
        c2s_client.initialize()?;
        c2s_client.discover_servers()?;
        self.c2s_client = Some(c2s_client);
        Ok(())
    }

    pub fn get_server_count(&self) -> usize {
        self.c2s_client
            .as_ref()
            .and_then(|c| c.context.server_addresses.as_ref())
            .map(|addresses| addresses.len())
            .unwrap_or(0)
    }

    pub fn get_rank(&self) -> usize {
        self.rank
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub async fn send_message(
        &self,
        server_rank: usize,
        handler_name: String,
        data: &[u8],
    ) -> Result<()> {
        if let Some(client) = &self.c2s_client {
            client.send_message(server_rank, handler_name, data).await
        } else {
            Err(anyhow!("C2S client not initialized"))
        }
    }

    pub async fn benchmark_rpc(
        &self,
        server_rank: usize,
        num_requests: usize,
        handler_name: String,
        data: Vec<u8>,
    ) -> BenchmarkStats {
        let mut stats = BenchmarkStats {
            total_requests: num_requests,
            successful_requests: 0,
            failed_requests: 0,
            total_duration_ms: 0,
            min_latency_ms: u128::MAX,
            max_latency_ms: 0,
            avg_latency_ms: 0.0,
        };

        let start_time = std::time::SystemTime::now();

        for _ in 0..num_requests {
            let request_start = std::time::SystemTime::now();
            match self
                .send_message(server_rank, handler_name.clone(), &data)
                .await
            {
                Ok(_) => {
                    stats.successful_requests += 1;
                    if let Ok(duration) = request_start.elapsed() {
                        let latency = duration.as_millis();
                        stats.min_latency_ms = stats.min_latency_ms.min(latency);
                        stats.max_latency_ms = stats.max_latency_ms.max(latency);
                        stats.total_duration_ms += latency;
                    }
                }
                Err(_) => {
                    stats.failed_requests += 1;
                }
            }
        }

        if stats.successful_requests > 0 {
            stats.avg_latency_ms =
                stats.total_duration_ms as f64 / stats.successful_requests as f64;
        }

        if let Ok(total_duration) = start_time.elapsed() {
            stats.total_duration_ms = total_duration.as_millis();
        }

        stats
    }
}

impl BenchmarkStats {
    pub fn print_stats(&self) {
        let tps = if self.total_duration_ms > 0 {
            (self.successful_requests as f64 * 1000.0) / self.total_duration_ms as f64
        } else {
            0.0
        };
        println!(
            "total={} success={} failed={} duration={} min={} max={} avg={} tps={:.2}",
            self.total_requests,
            self.successful_requests,
            self.failed_requests,
            self.total_duration_ms,
            self.min_latency_ms,
            self.max_latency_ms,
            self.avg_latency_ms,
            tps
        );
    }
}
