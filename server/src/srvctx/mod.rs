use log::{debug, error, info, warn};
use anyhow::Result;
use bulkistore_commons::rpc::{RxEndpoint, RPCImpl};
use bulkistore_commons::rpc::grpc::{GRPC_RX, RXTXUtils};
use mpi::topology::SimpleCommunicator;
use tokio::sync::{broadcast, oneshot};
use tokio::signal;
use std::str::FromStr;
use std::path::PathBuf;
use hostname;

pub mod srvctx {
    use super::*;

    #[derive(Clone)]
    pub struct ServerContext {
        pub rank: usize,
        pub size: usize,
        pub world: Option<SimpleCommunicator>,
        // RxEndpoint for client-server communication
        pub c2s_endpoint: Option<GRPC_RX>,
        // RxEndpoint for server-server communication
        pub s2s_endpoint: Option<GRPC_RX>,
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

        pub async fn initialize(&mut self, world: Option<SimpleCommunicator>) -> Result<()> {
            self.world = world;

            // Initialize rank and size based on MPI world
            #[cfg(feature = "mpi")]
            if let Some(ref world) = self.world {
                self.rank = world.unwrap().rank();
                self.size = world.unwrap().size();
            }

            // Initialize client-server endpoint
            let mut c2s = GRPC_RX::new("c2s".to_string());
            c2s.initialize(self.world.clone())?;
            c2s.exchange_addresses()?;
            c2s.write_addresses()?;
            self.c2s_endpoint = Some(c2s);

            // Initialize server-server endpoint
            let mut s2s = GRPC_RX::new("s2s".to_string());
            s2s.initialize(self.world.clone())?;
            s2s.exchange_addresses()?;
            s2s.write_addresses()?;
            self.s2s_endpoint = Some(s2s);

            Ok(())
        }

        pub async fn start_endpoints(&mut self) -> Result<()> {
            // Start c2s endpoint
            if let Some(c2s) = &mut self.c2s_endpoint {
                let (tx1, rx1) = oneshot::channel();
                self.c2s_shutdown = Some(tx1);
                let ready_file = RXTXUtils::get_pdc_tmp_dir().join("rx_c2s_ready.txt");
                tokio::spawn(async move {
                    let _ = c2s.listen(async move {
                        Self::handle_shutdown(rx1, ready_file).await;
                        Ok(())
                    }).await;
                });
            }

            // Start s2s endpoint
            if let Some(s2s) = &mut self.s2s_endpoint {
                let (tx2, rx2) = oneshot::channel();
                self.s2s_shutdown = Some(tx2);
                let ready_file = RXTXUtils::get_pdc_tmp_dir().join("rx_s2s_ready.txt");
                tokio::spawn(async move {
                    let _ = s2s.listen(async move {
                        Self::handle_shutdown(rx2, ready_file).await;
                        Ok(())
                    }).await;
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
}
