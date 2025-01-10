use bulkistore_commons::common::RPCData;
use bulkistore_commons::proto::grpc_bulkistore_client::GrpcBulkistoreClient;
use bulkistore_commons::proto::{RequestMetadata, RpcRequest};
use mpi::topology::SimpleCommunicator;
use mpi::traits::*;
use rand::Rng;
use rmp_serde;
use std::env;
use std::fs;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;

pub struct ClientContext {
    world: SimpleCommunicator,
    rank: i32,
    size: i32,
    server_addresses: Vec<String>,
    client_addresses: Vec<String>,
}

impl ClientContext {
    pub fn new(world: SimpleCommunicator) -> Result<Self, Box<dyn std::error::Error>> {
        let rank = world.rank();
        let size = world.size();

        // Initialize with empty addresses
        let mut context = ClientContext {
            world,
            rank,
            size,
            server_addresses: Vec::new(),
            client_addresses: vec![String::new(); size as usize],
        };

        // Write client addresses and read server addresses
        context.write_client_addresses()?;
        context.read_server_addresses()?;

        Ok(context)
    }

    fn get_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn get_pdc_tmp_dir() -> String {
        env::var("PDC_TMP").unwrap_or_else(|_| ".pdc_tmp".to_string())
    }

    /// Read server addresses from the PDC_TMP directory
    fn read_server_addresses(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let pdc_tmp = Self::get_pdc_tmp_dir();
        let file_path = format!("{}/server_list", pdc_tmp);
        let contents = fs::read_to_string(file_path)?;

        // Filter out empty lines and collect
        let lines: Vec<_> = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();

        if lines.is_empty() {
            return Err("No server addresses found in file".into());
        }

        // Create a vector with size matching number of servers
        let mut addresses = vec![String::new(); lines.len()];

        // Parse each line in format "rank|host:port" and place in correct position
        for line in lines {
            if let Some((rank_str, addr)) = line.split_once('|') {
                if let Ok(rank) = rank_str.parse::<usize>() {
                    addresses[rank] = addr.to_string();
                }
            }
        }

        self.server_addresses = addresses;
        println!(
            "[Client {}] Found {} server(s)",
            self.rank,
            self.server_addresses.len()
        );
        Ok(())
    }

    /// Write client address to PDC_TMP directory
    fn write_client_addresses(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let client_info = format!("client_{}", self.rank);
        let max_size = 256;
        let mut send_buffer = vec![0u8; max_size];
        send_buffer[..client_info.len()].copy_from_slice(client_info.as_bytes());

        // Allocate receive buffer for all ranks since all_gather_into distributes to all processes
        let mut recv_buffer = vec![0u8; max_size * self.size as usize];

        // Gather all client information to all ranks
        self.world
            .all_gather_into(&send_buffer[..], &mut recv_buffer);

        // Process and store client addresses
        for (i, chunk) in recv_buffer.chunks(max_size).enumerate() {
            let len = chunk.iter().position(|&x| x == 0).unwrap_or(chunk.len());
            let client_str = String::from_utf8_lossy(&chunk[..len]);
            self.client_addresses[i] = client_str.to_string();
        }

        // Only rank 0 writes the file
        if self.rank == 0 {
            let pdc_tmp = Self::get_pdc_tmp_dir();
            fs::create_dir_all(&pdc_tmp)?;
            let file_path = format!("{}/client_list", pdc_tmp);
            let mut file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(file_path)?;

            // Write each client's information
            for (i, addr) in self.client_addresses.iter().enumerate() {
                writeln!(file, "{}:{}", i, addr)?;
            }
            println!(
                "[Client 0] Wrote client addresses to {}/client_addresses.txt",
                pdc_tmp
            );
        }

        Ok(())
    }

    pub async fn connect_to_server(
        &self,
        server_rank: usize,
    ) -> Result<GrpcBulkistoreClient<Channel>, Box<dyn std::error::Error>> {
        if server_rank >= self.server_addresses.len() {
            return Err(format!("Invalid server rank: {}", server_rank).into());
        }

        let server_addr = &self.server_addresses[server_rank];
        let endpoint = format!("http://{}", server_addr);
        let channel = Channel::from_shared(endpoint.clone())?.connect().await?;

        Ok(GrpcBulkistoreClient::new(channel))
    }

    pub async fn send_request(
        &self,
        server_rank: usize,
        _operation: &str,
        binary_data: Vec<u8>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut client = self.connect_to_server(server_rank).await?;

        // Generate request ID
        let request_id = rand::thread_rng().gen::<u64>();

        // Create request metadata
        let metadata = RequestMetadata {
            client_rank: self.rank,
            client_world_size: self.size,
            timestamp_ms: Self::get_timestamp_ms(),
            request_id,
        };

        // Create request
        let request = tonic::Request::new(RpcRequest {
            metadata: Some(metadata),
            binary_data,
        });

        // Send request and wait for response
        let response = client.process_request(request).await?;
        let response = response.into_inner();

        // Check status
        if response.status != 0 {
            return Err(format!(
                "Request failed with status {}: {}",
                response.status, response.error_message
            )
            .into());
        }

        // Log response metadata if present
        if let Some(metadata) = response.metadata {
            println!(
                "[Client {}] Response for request {}: processed by server {} in {}ms",
                self.rank,
                metadata.request_id,
                metadata.server_rank,
                metadata.processed_at_ms - metadata.received_at_ms
            );
        }

        Ok(response.result_data)
    }

    pub async fn send_rpc(
        &self,
        server_rank: usize,
        rpc_data: RPCData,
    ) -> Result<RPCData, Box<dyn std::error::Error>> {
        // Serialize the RPCData
        let binary_data = rmp_serde::to_vec(&rpc_data)?;

        // Send the request
        let result_data = self
            .send_request(server_rank, &rpc_data.func_name, binary_data)
            .await?;

        // Deserialize the response
        let result = rmp_serde::from_slice(&result_data)?;
        Ok(result)
    }

    pub fn get_rank(&self) -> i32 {
        self.rank
    }

    pub fn get_size(&self) -> i32 {
        self.size
    }

    pub fn get_server_addresses(&self) -> &[String] {
        &self.server_addresses
    }

    pub fn get_client_addresses(&self) -> &[String] {
        &self.client_addresses
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize MPI
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    // Create client context
    let context = ClientContext::new(world)?;
    println!("Client running on MPI process {}", context.get_rank());

    // Create test RPCData
    let rpc_data = RPCData {
        func_name: if context.get_rank() % 2 == 0 {
            "times_three"
        } else {
            "times_two"
        }
        .to_string(),
        data: vec![1, 2, 3],
    };

    println!(
        "[Client {}] Sending RPC: func_name='{}', data={:?}",
        context.get_rank(),
        rpc_data.func_name,
        rpc_data.data
    );

    // Send RPC request
    match context.send_rpc(0, rpc_data).await {
        Ok(result) => {
            println!(
                "[Client {}] Received response: func_name='{}', data={:?}",
                context.get_rank(),
                result.func_name,
                result.data
            );
        }
        Err(e) => {
            eprintln!("[Client {}] Request failed: {}", context.get_rank(), e);
        }
    }

    Ok(())
}
