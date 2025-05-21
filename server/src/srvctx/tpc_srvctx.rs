use anyhow::Result;
use commons::err::RPCResult;
use commons::rpc::tcp::{TcpRxEndpoint, TcpTxEndpoint};
use commons::rpc::{RPCImpl, RxContext, RxEndpoint, TxContext, TxEndpoint};
use commons::utils::FileUtility;
use lazy_static::lazy_static;
use log::{debug, info, warn};
#[cfg(feature = "mpi")]
use mpi::{
    environment::Universe,
    topology::{Communicator, SimpleCommunicator},
    traits::CommunicatorCollectives,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{oneshot, Mutex as TokioMutex};
// mod reqhandler;
// mod resphandler;

lazy_static! {
    static ref PROCESS_RANK: AtomicU32 = AtomicU32::new(0);
    static ref PROCESS_SIZE: AtomicU32 = AtomicU32::new(1);
    // Global S2S client that can be accessed from any thread
    // Initialize s2s client
    static ref GLOBAL_S2S_CLIENT: RwLock<Option<Box<dyn TxEndpoint<Address = SocketAddr>>>> = {
        let mut s2s_context = TxContext::<SocketAddr>::new("s2s".to_string(), None);
        let mut s2s_client = TcpTxEndpoint::new(s2s_context);
        let _ = s2s_client.initialize(resphandler::register_handlers);
        let _ = s2s_client.discover_servers();
        RwLock::new(Some(Box::new(s2s_client)))
    };
    static ref RUNTIME: RwLock<Option<tokio::runtime::Runtime>> = {
        match tokio::runtime::Runtime::new() {
            Ok(rt) => RwLock::new(Some(rt)),
            Err(e) => {
                eprintln!("Failed to create Tokio runtime: {}", e);
                RwLock::new(None)
            }
        }
    };
}

#[allow(dead_code)]
pub fn get_rank() -> u32 {
    PROCESS_RANK.load(Ordering::SeqCst)
}

#[allow(dead_code)]
pub fn get_size() -> u32 {
    PROCESS_SIZE.load(Ordering::SeqCst)
}

#[allow(dead_code)]
pub async fn server_rpc_call<T, R>(srv_id: u32, method_name: &str, input: &T) -> RPCResult<R>
where
    T: Serialize + std::marker::Sync + Send + Clone + 'static,
    R: for<'de> Deserialize<'de> + Send + 'static,
{
    // Get global client
    match GLOBAL_S2S_CLIENT.read() {
        Ok(guard) => {
            if let Some(client) = guard.as_ref() {
                // Clone input for the new thread
                let input_clone = input.clone();
                let method_name_clone = method_name.to_string();

                client
                    .send_message::<T, R>(srv_id as usize, &method_name_clone, &input_clone)
                    .await
            } else {
                return Err(commons::err::RpcErr::new(
                    commons::err::StatusCode::Internal,
                    "S2S client not initialized",
                ));
            }
        }
        Err(_) => {
            return Err(commons::err::RpcErr::new(
                commons::err::StatusCode::Internal,
                "Failed to acquire read lock on S2S client",
            ));
        }
    }
}

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
    pub c2s_endpoint: Option<Box<dyn RxEndpoint<Address = SocketAddr>>>,
    // RxEndpoint for server-server communication
    pub s2s_endpoint: Option<Box<dyn RxEndpoint<Address = SocketAddr>>>,
    endpoints: HashMap<String, Arc<TokioMutex<Box<dyn RxEndpoint<Address = SocketAddr>>>>>,
    endpoint_shutdowns: HashMap<String, oneshot::Sender<()>>,
}

#[allow(dead_code)]
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
            endpoints: HashMap::new(),
            endpoint_shutdowns: HashMap::new(),
        }
    }

    pub async fn register_endpoint(
        &mut self,
        id: &str,
        start_listen_tx: oneshot::Sender<()>,
        mut endpoint: Box<dyn RxEndpoint<Address = SocketAddr>>,
    ) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let _ = endpoint.listen(start_listen_tx, shutdown_rx).await?;
        debug!("Endpoint {} is listening now", id);
        let endpoint = Arc::new(TokioMutex::new(endpoint));
        self.endpoints.insert(id.to_string(), endpoint);
        self.endpoint_shutdowns.insert(id.to_string(), shutdown_tx);
        Ok(())
    }

    pub async fn initialize(&mut self, universe: Option<Arc<Universe>>) -> Result<()> {
        debug!("ServerContext::initialize");
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
            PROCESS_RANK.store(self.rank as u32, Ordering::SeqCst);
            PROCESS_SIZE.store(self.size as u32, Ordering::SeqCst);
        }

        #[cfg(not(feature = "mpi"))]
        {
            self.rank = 0;
            self.size = 1;
            // set env var for MPI_SIZE and MPI_RANK
            PROCESS_RANK.store(0u32, Ordering::SeqCst);
            PROCESS_SIZE.store(1u32, Ordering::SeqCst);
        }

        // Initialize client-server endpoint
        debug!("Initializing client-server endpoint");
        let mut c2s_context = RxContext::<SocketAddr>::new("c2s".to_string(), self.world.clone());
        let mut c2s = TcpRxEndpoint::new(c2s_context);
        c2s.initialize(0u16, reqhandler::register_handlers)?;
        self.c2s_endpoint = Some(Box::new(c2s));

        // Initialize server-server endpoint
        debug!("Initializing server-server endpoint");
        let mut s2s_context = RxContext::<SocketAddr>::new("s2s".to_string(), self.world.clone());
        let mut s2s = TcpRxEndpoint::new(s2s_context);
        s2s.initialize(1u16, reqhandler::register_handlers)?;
        self.s2s_endpoint = Some(Box::new(s2s));

        // Initialize the datastore
        debug!("Initializing datastore...");
        crate::datastore::init_datastore();

        debug!(
            "DataStore initialized! Server running on MPI process {}",
            self.rank
        );
        Ok(())
    }

    pub async fn start_endpoints<F>(&mut self, callback: Option<F>) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        let endpoint_list = ["c2s", "s2s"];
        for &id in endpoint_list.iter() {
            let (start_listen_tx, start_listen_rx) = oneshot::channel();
            match id {
                "c2s" => {
                    if let Some(c2s) = self.c2s_endpoint.take() {
                        self.register_endpoint("c2s", start_listen_tx, c2s).await?;
                    }
                    let _ = start_listen_rx.await.ok().unwrap();
                }
                "s2s" => {
                    if let Some(s2s) = self.s2s_endpoint.take() {
                        self.register_endpoint("s2s", start_listen_tx, s2s).await?;
                    }
                    let _ = start_listen_rx.await.ok().unwrap();
                }
                _ => {
                    return Err(anyhow::anyhow!("Invalid endpoint id"));
                }
            }
        }
        // Send start listen signal
        debug!("c2s and s2s endpoints registered");

        // apply MPI barrier
        #[cfg(feature = "mpi")]
        {
            if let Some(world) = &self.world {
                world.barrier();
            }
        }

        // Exchange addresses for registered endpoints
        if let Some(c2s) = self.endpoints.get("c2s") {
            c2s.lock().await.exchange_addresses()?;
        }

        if let Some(s2s) = self.endpoints.get("s2s") {
            s2s.lock().await.exchange_addresses()?;
        }

        // Execute the callback function so we can get the server ready.
        if let Some(callback) = callback {
            callback()?;
        }

        // Write addresses for registered endpoints
        if let Some(c2s) = self.endpoints.get("c2s") {
            c2s.lock().await.write_addresses()?;
        }

        if let Some(s2s) = self.endpoints.get("s2s") {
            s2s.lock().await.write_addresses()?;
        }

        // test if s2s ready file is there, if yes, let's move on, otherwise, just wait for that file to be created
        let ready_file = FileUtility::get_pdc_tmp_dir().join(format!("rx_s2s_ready.txt"));

        info!("Waiting for s2s ready file...");
        let timeout = tokio::time::Duration::from_secs(10);
        let start = tokio::time::Instant::now();
        while !ready_file.exists() {
            if start.elapsed() >= timeout {
                anyhow::bail!("Timeout waiting for s2s ready file");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        debug!("s2s ready file found: {}", ready_file.display());

        // Check if the global s2s client is poisoned
        let s2s_is_poisoned = GLOBAL_S2S_CLIENT.is_poisoned();
        debug!("s2s client poisoned: {}", s2s_is_poisoned);

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

            if self.rank == 0 {
                let ready_file =
                    FileUtility::get_pdc_tmp_dir().join(format!("rx_{}_ready.txt", id));
                // delete the ready file
                if let Err(e) = tokio::fs::remove_file(&ready_file).await {
                    warn!("Failed to remove ready file for endpoint {}: {}", id, e);
                }

                let server_list_path =
                    FileUtility::get_pdc_tmp_dir().join(format!("rx_{}.txt", id));
                // delete the server list file
                if let Err(e) = tokio::fs::remove_file(&server_list_path).await {
                    warn!(
                        "Failed to remove server list file for endpoint {}: {}",
                        id, e
                    );
                }
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

        // Clean up global resources
        clear_global_resources();

        Ok(())
    }
}

/// Clear global resources
///
/// This ensures proper cleanup of resources across platforms.
/// Particularly important for proper MPI finalization on Perlmutter.
pub fn clear_global_resources() {
    // Clear global S2S client
    if GLOBAL_S2S_CLIENT.is_poisoned() {
        return;
    }
    if let Ok(mut global_client) = GLOBAL_S2S_CLIENT.write() {
        *global_client = None;
    }
}
