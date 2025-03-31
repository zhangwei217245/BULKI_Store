use commons::utils::SystemUtility;
use log::{debug, info};
use std::sync::Arc;
mod bench;
mod datastore;
mod health;
mod srvctx;
use anyhow::Result;
use env_logger;
use lazy_static::lazy_static;
use srvctx::{get_rank, get_size, ServerContext};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

lazy_static! {
    static ref MEMORY_MONITOR_RUNNING: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
}

fn prepare_resources() -> Result<()> {
    debug!("[R{}/S{}] Loading Data...", get_rank(), get_size());
    let timer = Instant::now();
    let num_obj_loaded = datastore::load_memory_store()?;
    info!(
        "[R{}/S{}] {} objects loaded in {} seconds",
        get_rank(),
        get_size(),
        num_obj_loaded,
        timer.elapsed().as_secs()
    );
    Ok(())
}

fn close_resources() -> Result<()> {
    // let timer = Instant::now();
    // info!(
    //     "[R{}/S{}] Closing resources, checkpointing data...",
    //     get_rank(),
    //     get_size()
    // );
    // datastore::dump_memory_store()?;
    // info!(
    //     "[R{}/S{}] Finished checkpointing in {} seconds",
    //     get_rank(),
    //     get_size(),
    //     timer.elapsed().as_secs()
    // );

    MEMORY_MONITOR_RUNNING.store(false, Ordering::SeqCst);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    #[cfg(feature = "mpi")]
    let universe = {
        let (universe, _) = mpi::initialize_with_threading(mpi::Threading::Multiple).unwrap();
        Some(Arc::new(universe))
    };

    #[cfg(not(feature = "mpi"))]
    let universe = None;
    // Create and initialize server context
    let mut server_context = ServerContext::new();

    // Initialize with MPI universe - critical for Perlmutter
    server_context.initialize(universe).await?;
    info!("[R{}/S{}] Server initialized", get_rank(), get_size());

    // Start memory monitoring thread
    let _memory_monitor_thread = SystemUtility::monitor_memory_usage(
        MEMORY_MONITOR_RUNNING.clone(),
        2,
        "Server".to_string(),
        get_rank() as usize,
        get_size() as usize,
    );

    // Start the RX endpoints before wrapping in Arc
    futures::executor::block_on(async {
        server_context
            .start_endpoints(Some(prepare_resources))
            .await
    })?;
    info!("[R{}/S{}] Server endpoints started", get_rank(), get_size());

    // Handle shutdown signals with proper MPI cleanup
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("[R{}/S{}] Received Ctrl+C signal", get_rank(), get_size());
                // Signal memory monitor thread to stop
                server_context.shutdown(|| async { close_resources() }).await?;
                debug!("[R{}/S{}] Server shutdown complete", get_rank(), get_size());
            }
            _ = terminate.recv() => {
                info!("[R{}/S{}] Received SIGTERM signal", get_rank(), get_size());
                // Signal memory monitor thread to stop
                server_context.shutdown(|| async { close_resources() }).await?;
                debug!("[R{}/S{}] Server shutdown complete", get_rank(), get_size());
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        info!("[R{}/S{}] Received Ctrl+C signal", get_rank(), get_size());
        server_context
            .shutdown(|| async { close_resources() })
            .await?;
        debug!("[R{}/S{}] Server shutdown complete", get_rank(), get_size());
    }

    // // Wait for memory monitor thread to finish
    // if let Err(e) = memory_monitor_thread.join() {
    //     debug!(
    //         "[R{}/S{}] Error joining memory monitor thread: {:?}",
    //         get_rank(),
    //         get_size(),
    //         e
    //     );
    // }

    Ok(())
}
