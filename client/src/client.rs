mod cltctx;
use cltctx::ClientContext;
use commons::rpc::{MessageType, RPCData, RPCMetadata};
use log::debug;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut context = ClientContext::new();
    context.initialize().await?;
    debug!("Client running on MPI process {}", context.get_rank());

    let num_requests = 1000;
    let data_len = 1000;
    context.benchmark_rpc(num_requests, data_len);
    Ok(())
}
