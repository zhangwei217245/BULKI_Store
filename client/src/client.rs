use bulkistore_commons::proto::grpc_bulkistore_client::GrpcBulkistoreClient;
use bulkistore_commons::proto::{RequestMetadata, RpcRequest};
use mpi::topology::SimpleCommunicator;
use mpi::traits::*;
use std::env;
use std::fs;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;
use tonic::Request;
use uuid::Uuid;

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
        let file_path = format!("{}/server_addresses.txt", pdc_tmp);
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
            let file_path = format!("{}/client_addresses.txt", pdc_tmp);
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
        operation: &str,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut client = self.connect_to_server(server_rank).await?;

        let request = Request::new(RpcRequest {
            metadata: Some(RequestMetadata {
                client_rank: self.rank,
                client_world_size: self.size,
                timestamp_ms: Self::get_timestamp_ms(),
                request_id: Uuid::new_v4().to_string(),
                operation_type: operation.to_string(),
            }),
            binary_data: data,
        });

        let response = client.process_request(request).await?;
        let response = response.into_inner();

        if response.status != 0 {
            return Err(format!("Server error: {}", response.error_message).into());
        }

        Ok(response.result_data)
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

    // Send a test request to server 0
    let test_data = vec![1, 2, 3];
    match context.send_request(0, "test_operation", test_data).await {
        Ok(result) => {
            println!("Request completed successfully");
            println!("Received result data: {:?}", result);
        }
        Err(e) => {
            eprintln!("Request failed: {}", e);
        }
    }

    Ok(())
}
