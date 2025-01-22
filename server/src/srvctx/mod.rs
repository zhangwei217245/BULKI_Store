use anyhow::Result;
use commons::rpc::grpc::{GrpcRX, GrpcTX};
use commons::rpc::{RXTXUtils, RxEndpoint, TxEndpoint};
use log::error;
#[cfg(feature = "mpi")]
use mpi::topology::{Communicator, SimpleCommunicator};
#[cfg(feature = "mpi")]
use mpi::Threading;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::oneshot;

pub struct ServerContext {
    pub rank: usize,
    pub size: usize,
    #[cfg(feature = "mpi")]
    pub world: Option<Arc<SimpleCommunicator>>,
    #[cfg(not(feature = "mpi"))]
    pub world: Option<()>,
    // RxEndpoint for client-server communication
    pub c2s_endpoint: Option<GrpcRX>,
    // RxEndpoint for server-server communication
    pub s2s_endpoint: Option<GrpcRX>,
    pub s2s_client: Option<GrpcTX>,
    // Shutdown senders for each endpoint
    c2s_shutdown: Option<oneshot::Sender<()>>,
    s2s_shutdown: Option<oneshot::Sender<()>>,
}

impl ServerContext {
    pub fn new() -> Self {
        Self {
            rank: 0,
            size: 1,
            world: None,
            c2s_endpoint: None,
            s2s_endpoint: None,
            s2s_client: None,
            c2s_shutdown: None,
            s2s_shutdown: None,
        }
    }

    async fn handle_shutdown(rx: oneshot::Receiver<()>, ready_file: PathBuf) {
        // Wait for either SIGTERM or SIGINT
        let ctrl_c = async {
            signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to listen for SIGTERM")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        let rx = async {
            rx.await.ok();
        };

        // Wait for any signal
        tokio::select! {
            _ = ctrl_c => println!("Received Ctrl+C signal"),
            _ = terminate => println!("Received SIGTERM signal"),
            _ = rx => println!("Received internal shutdown signal"),
        }
        println!("Starting graceful shutdown...");

        // Clean up ready file on shutdown
        if let Err(e) = std::fs::remove_file(&ready_file) {
            eprintln!("Failed to remove ready file: {}", e);
        }
    }

    pub async fn initialize(&mut self) -> Result<()> {
        // Initialize rank and size based on MPI world
        #[cfg(feature = "mpi")]
        {
            let (_universe, _) = mpi::initialize_with_threading(Threading::Multiple).unwrap();
            self.world = Some(Arc::new(_universe.world()));
            self.rank = self.world.as_ref().map(|w| w.rank()).unwrap_or(0) as usize;
            self.size = self.world.as_ref().map(|w| w.size()).unwrap_or(1) as usize;
        }
        // Initialize client-server endpoint
        #[cfg(not(feature = "mpi"))]
        {
            self.world = Some(());
            self.rank = 0;
            self.size = 1;
        }
        // Initialize client-server endpoint
        let mut c2s = GrpcRX::new("c2s".to_string(), self.world.clone());
        c2s.initialize()?;
        c2s.exchange_addresses()?;
        c2s.write_addresses()?;
        self.c2s_endpoint = Some(c2s);

        // Initialize server-server endpoint
        let mut s2s = GrpcRX::new("s2s".to_string(), self.world.clone());
        s2s.initialize()?;
        s2s.exchange_addresses()?;
        s2s.write_addresses()?;
        self.s2s_endpoint = Some(s2s);

        // Initialize s2s client
        let mut s2s_client = GrpcTX::new("s2s".to_string(), self.world.clone());
        s2s_client.initialize()?;
        s2s_client.discover_servers()?;
        self.s2s_client = Some(s2s_client);

        Ok(())
    }

    pub async fn start_endpoints(&mut self) -> Result<()> {
        // Start c2s endpoint
        if let Some(c2s) = self.c2s_endpoint.take() {
            let (tx1, rx1) = oneshot::channel();
            self.c2s_shutdown = Some(tx1);
            let ready_file = RXTXUtils::get_pdc_tmp_dir().join("rx_c2s_ready.txt");
            tokio::spawn(async move {
                if let Err(e) = c2s
                    .listen(async move {
                        Self::handle_shutdown(rx1, ready_file).await;
                    })
                    .await
                {
                    error!("C2S endpoint error: {}", e);
                }
            });
        }

        // Start s2s endpoint
        if let Some(s2s) = self.s2s_endpoint.take() {
            let (tx2, rx2) = oneshot::channel();
            self.s2s_shutdown = Some(tx2);
            let ready_file = RXTXUtils::get_pdc_tmp_dir().join("rx_s2s_ready.txt");
            tokio::spawn(async move {
                if let Err(e) = s2s
                    .listen(async move {
                        Self::handle_shutdown(rx2, ready_file).await;
                    })
                    .await
                {
                    error!("S2S endpoint error: {}", e);
                }
            });
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        // Send shutdown signals
        if let Some(tx) = self.c2s_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.s2s_shutdown.take() {
            let _ = tx.send(());
        }

        // Close endpoints
        if let Some(c2s) = &self.c2s_endpoint {
            c2s.close()?;
        }
        if let Some(s2s) = &self.s2s_endpoint {
            s2s.close()?;
        }
        Ok(())
    }
}
