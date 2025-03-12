pub mod resphandler;
use anyhow::Result;
use commons::err::{RPCResult, RpcErr, StatusCode};
use commons::rpc::grpc::GrpcTX;
use commons::rpc::TxEndpoint;
use lazy_static::lazy_static;
use log::{debug, info};
use mpi::environment::Universe;
#[cfg(feature = "mpi")]
use mpi::topology::SimpleCommunicator;
#[cfg(feature = "mpi")]
use mpi::traits::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

lazy_static! {
    static ref CLIENT_RANK: AtomicU32 = AtomicU32::new(0);
    static ref CLIENT_COUNT: AtomicU32 = AtomicU32::new(1);
    static ref SERVER_COUNT: AtomicU32 = AtomicU32::new(0);
}

#[allow(dead_code)]
pub fn get_client_rank() -> u32 {
    CLIENT_RANK.load(Ordering::SeqCst)
}
#[allow(dead_code)]
pub fn get_client_count() -> u32 {
    CLIENT_COUNT.load(Ordering::SeqCst)
}
#[allow(dead_code)]
pub fn get_server_count() -> u32 {
    SERVER_COUNT.load(Ordering::SeqCst)
}

pub struct ClientContext {
    #[cfg(feature = "mpi")]
    pub universe: Option<Arc<Universe>>,
    #[cfg(feature = "mpi")]
    pub world: Option<Arc<SimpleCommunicator>>,
    #[cfg(not(feature = "mpi"))]
    pub world: Option<()>,
    rank: usize,
    size: usize,
    pub c2s_client: Option<GrpcTX>,
}
#[allow(dead_code)]
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
            universe: None,
            world: None,
            rank: 0,
            size: 1,
            c2s_client: None,
        }
    }

    pub async fn initialize(&mut self, universe: Option<Arc<Universe>>) -> Result<()> {
        #[cfg(feature = "mpi")]
        {
            if let Some(universe) = universe {
                // Store the universe first
                self.universe = Some(universe.clone());
                // Then get the world communicator
                let world = self.universe.as_ref().unwrap().world();
                self.world = Some(Arc::new(world));
                // Initialize MPI-related fields
                if let Some(world) = &self.world {
                    self.rank = world.rank() as usize;
                    self.size = world.size() as usize;
                }
            } else {
                self.rank = 0;
                self.size = 1;
            }
            CLIENT_RANK.store(self.rank as u32, Ordering::SeqCst);
            CLIENT_COUNT.store(self.size as u32, Ordering::SeqCst);
            info!(
                "[MPI Enabled]Client rank: {}, Client count: {}",
                self.rank, self.size
            );
        }
        #[cfg(not(feature = "mpi"))]
        {
            self.world = Some(());
            self.rank = 0;
            self.size = 1;
            CLIENT_RANK.store(self.rank as u32, Ordering::SeqCst);
            CLIENT_COUNT.store(self.size as u32, Ordering::SeqCst);
            info!(
                "[MPI Disabled]Client rank: {}, Client count: {}",
                self.rank, self.size
            );
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn ensure_client_initialized(&mut self) -> Result<()> {
        if self.c2s_client.is_none() {
            // Initialize client-server endpoint
            let mut c2s_client = GrpcTX::new("c2s".to_string(), self.world.clone());
            info!("Initializing client-server endpoint");
            c2s_client.initialize(resphandler::register_handlers)?;
            info!("Client-server endpoint initialized");
            let server_count = c2s_client.discover_servers()?;
            SERVER_COUNT.store(server_count as u32, Ordering::SeqCst);
            info!(
                "{:?} Servers discovered",
                SERVER_COUNT.load(Ordering::SeqCst)
            );
            self.c2s_client = Some(c2s_client);
        }
        Ok(())
    }
    #[allow(dead_code)]
    pub fn initialize_blocking(&mut self, universe: Option<Arc<Universe>>) -> Result<()> {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(self.initialize(universe))
    }

    #[allow(dead_code)]
    pub fn get_server_count(&self) -> usize {
        self.c2s_client
            .as_ref()
            .and_then(|c| c.context.server_addresses.as_ref())
            .map(|addresses| addresses.len())
            .unwrap_or(0)
    }
    #[allow(dead_code)]
    pub fn get_rank(&self) -> usize {
        self.rank
    }
    #[allow(dead_code)]
    pub fn get_size(&self) -> usize {
        self.size
    }

    pub async fn send_message<T, R>(
        &self,
        server_rank: usize,
        handler_name: &str,
        data: &T,
    ) -> RPCResult<R>
    where
        T: Serialize + Sync + 'static,
        R: for<'de> Deserialize<'de>,
    {
        if let Some(client) = &self.c2s_client {
            debug!("Sending message to server rank {}", server_rank);
            client.send_message(server_rank, handler_name, data).await
        } else {
            Err(RpcErr::new(
                StatusCode::Unknown,
                "C2S client not initialized",
            ))
        }
    }
    #[allow(dead_code)]
    pub async fn benchmark_rpc(
        &self,
        num_requests: usize,
        data_len: usize,
    ) -> Result<BenchmarkStats> {
        let mut stats = BenchmarkStats {
            total_requests: num_requests,
            successful_requests: 0,
            failed_requests: 0,
            total_duration_ms: 0,
            min_latency_ms: u128::MAX,
            max_latency_ms: 0,
            avg_latency_ms: 0.0,
        };

        // prepare data
        let data = vec![1; data_len];

        // calculate the number of requests each client rank should send
        let size = self.get_size();
        let rank = self.get_rank();
        let base_requests = num_requests / size;
        let remainder = num_requests % size;
        let num_requests = if rank < remainder {
            base_requests + 1
        } else {
            base_requests
        };

        debug!(
            "Rank {} will send {} requests (base={}, remainder={})",
            rank, num_requests, base_requests, remainder
        );

        let start_time = std::time::SystemTime::now();

        for i in 0..base_requests {
            let request_start = std::time::SystemTime::now();
            let server_rank = i % self.get_server_count();
            match self
                .send_message::<Vec<u8>, Vec<u8>>(server_rank, "health::HealthCheck::check", &data)
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
        // we should call a MPI barrier if mpi is enabled here
        #[cfg(feature = "mpi")]
        {
            if let Some(world) = &self.world {
                world.barrier();
            }
        }

        if let Ok(total_duration) = start_time.elapsed() {
            stats.total_duration_ms = total_duration.as_millis();
        }

        Ok(stats)
    }
}

impl BenchmarkStats {
    #[allow(dead_code)]
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
