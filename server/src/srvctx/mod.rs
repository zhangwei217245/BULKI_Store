use anyhow::Result;
use commons::rpc::grpc::{GrpcRX, GrpcTX};
use commons::rpc::{RxEndpoint, TxEndpoint};
use commons::utils::FileUtility;
use log::{error, info, warn};
#[cfg(feature = "mpi")]
use mpi::{
    environment::Universe,
    topology::{Communicator, SimpleCommunicator},
    traits::CommunicatorCollectives,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, Barrier, Mutex as TokioMutex};
mod reqhandler;
mod resphandler;

pub struct ServerContext {
    pub rank: usize,
    pub size: usize,
    #[cfg(feature = "mpi")]
    pub universe: Option<Arc<Universe>>,
    #[cfg(feature = "mpi")]
    pub world: Option<Arc<SimpleCommunicator>>,
    #[cfg(not(feature = "mpi"))]
    pub world: Option<()>,
    // RxEndpoint for client-server communication
    pub c2s_endpoint: Option<GrpcRX>,
    // RxEndpoint for server-server communication
    pub s2s_endpoint: Option<GrpcRX>,
    pub s2s_client: Option<GrpcTX>,
    endpoints: HashMap<String, Arc<TokioMutex<GrpcRX>>>,
    endpoint_shutdowns: HashMap<String, oneshot::Sender<()>>,
}

#[derive(Debug)]
pub enum PersistenceEvent {
    SaveData { endpoint_id: String, data: Vec<u8> },
    Shutdown,
}

impl ServerContext {
    pub fn new() -> Self {
        Self {
            rank: 0,
            size: 1,
            world: None,
            universe: None,
            c2s_endpoint: None,
            s2s_endpoint: None,
            s2s_client: None,
            endpoints: HashMap::new(),
            endpoint_shutdowns: HashMap::new(),
        }
    }

    pub async fn register_endpoint(
        &mut self,
        id: &str,
        mut endpoint: GrpcRX,
        barrier: Arc<Barrier>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = endpoint.listen(barrier.clone(), rx).await?;
        let ready_file = FileUtility::get_pdc_tmp_dir().join(format!("rx_{}_ready.txt", id));
        // write 'ready' to the ready file
        std::fs::write(&ready_file, "ready").expect("Failed to create ready file");
        let endpoint = Arc::new(TokioMutex::new(endpoint));
        self.endpoints.insert(id.to_string(), endpoint);
        self.endpoint_shutdowns.insert(id.to_string(), tx);
        Ok(())
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
        }

        #[cfg(not(feature = "mpi"))]
        {
            self.rank = 0;
            self.size = 1;
        }

        // Initialize client-server endpoint
        let mut c2s = GrpcRX::new("c2s".to_string(), self.world.clone());
        c2s.initialize(0u16, reqhandler::register_handlers)?;
        self.c2s_endpoint = Some(c2s);

        // Initialize server-server endpoint
        let mut s2s = GrpcRX::new("s2s".to_string(), self.world.clone());
        s2s.initialize(1u16, reqhandler::register_handlers)?;
        self.s2s_endpoint = Some(s2s);

        Ok(())
    }

    pub async fn start_endpoints(&mut self) -> Result<()> {
        let barrier = Arc::new(tokio::sync::Barrier::new(3)); // 1 for main + 2 for endpoints

        // Register c2s endpoint
        if let Some(c2s) = self.c2s_endpoint.take() {
            let barrier_clone = barrier.clone();
            self.register_endpoint("c2s", c2s, barrier_clone).await?;
        }

        // Register s2s endpoint
        if let Some(s2s) = self.s2s_endpoint.take() {
            let barrier_clone = barrier.clone();
            self.register_endpoint("s2s", s2s, barrier_clone).await?;
        }

        // Wait for both endpoints to be ready
        barrier.wait().await;

        // apply MPI barrier
        #[cfg(feature = "mpi")]
        {
            if let Some(world) = &self.world {
                world.barrier();
            }
        }

        // Exchange and write addresses for registered endpoints
        if let Some(c2s) = self.endpoints.get("c2s") {
            c2s.lock().await.exchange_addresses()?;
            c2s.lock().await.write_addresses()?;
        }

        if let Some(s2s) = self.endpoints.get("s2s") {
            s2s.lock().await.exchange_addresses()?;
            s2s.lock().await.write_addresses()?;
        }

        // Initialize s2s client
        let mut s2s_client = GrpcTX::new("s2s".to_string(), self.world.clone());
        s2s_client.initialize(resphandler::register_handlers)?;
        s2s_client.discover_servers()?;
        self.s2s_client = Some(s2s_client);

        Ok(())
    }

    pub async fn shutdown<F, Fut>(&mut self, pre_shutdown: F) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        // Execute pre-shutdown tasks
        pre_shutdown().await?;

        // Send shutdown signals to all endpoints
        for (id, tx) in self.endpoint_shutdowns.drain() {
            if let Err(_) = tx.send(()) {
                warn!("Failed to send shutdown signal to endpoint: {}", id);
            }
            let ready_file = FileUtility::get_pdc_tmp_dir().join(format!("rx_{}_ready.txt", id));
            // delete the ready file
            if let Err(e) = std::fs::remove_file(&ready_file) {
                warn!("Failed to remove ready file for endpoint {}: {}", id, e);
            }
        }

        // Close all endpoints
        for (id, endpoint) in &self.endpoints {
            // Handle any final persistence here if needed
            if let Err(e) = endpoint.lock().await.close() {
                warn!("Failed to close endpoint {}: {}", id, e);
            }
        }

        // Clear endpoints
        self.endpoints.clear();

        Ok(())
    }
}
