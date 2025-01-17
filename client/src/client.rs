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

#[derive(Debug)]
pub struct BenchmarkStats {
    total_requests: usize,
    successful_requests: usize,
    failed_requests: usize,
    total_duration_ms: u128,
    min_latency_ms: u128,
    max_latency_ms: u128,
    avg_latency_ms: f64,
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

    pub async fn benchmark_rpc(
        &self,
        server_rank: usize,
        num_requests: usize,
        rpc_data: RPCData,
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

        let mut total_latency = 0u128;
        let benchmark_start = SystemTime::now();

        for i in 0..num_requests {
            let request_start = SystemTime::now();
            match self.send_rpc(server_rank, rpc_data.clone()).await {
                Ok(_result) => {
                    let latency = request_start.elapsed().unwrap().as_millis();
                    stats.successful_requests += 1;
                    total_latency += latency;
                    stats.min_latency_ms = stats.min_latency_ms.min(latency);
                    stats.max_latency_ms = stats.max_latency_ms.max(latency);

                    if (i + 1) % 100 == 0 {
                        println!(
                            "[Client {}] Completed {} requests, current latency: {}ms",
                            self.get_rank(),
                            i + 1,
                            latency
                        );
                    }
                }
                Err(e) => {
                    stats.failed_requests += 1;
                    eprintln!(
                        "[Client {}] Request {} failed: {}",
                        self.get_rank(),
                        i + 1,
                        e
                    );
                }
            }
        }

        stats.total_duration_ms = benchmark_start.elapsed().unwrap().as_millis();
        stats.avg_latency_ms = total_latency as f64 / stats.successful_requests as f64;

        println!("\nBenchmark Results for Client {}:", self.get_rank());
        println!("Total Requests: {}", stats.total_requests);
        println!("Successful Requests: {}", stats.successful_requests);
        println!("Failed Requests: {}", stats.failed_requests);
        println!("Total Duration: {}ms", stats.total_duration_ms);
        println!("Average Latency: {:.2}ms", stats.avg_latency_ms);
        println!("Min Latency: {}ms", stats.min_latency_ms);
        println!("Max Latency: {}ms", stats.max_latency_ms);
        println!(
            "Requests/second: {:.2}",
            (stats.successful_requests as f64 * 1000.0) / stats.total_duration_ms as f64
        );

        stats
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // read command line arguments
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        println!("Usage: client num_req data_size");
        return Ok(());
    }

    let num_requests = args[1].parse::<usize>().unwrap();
    let data_size = args[2].parse::<usize>().unwrap();

    // Initialize MPI
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    // Create client context
    let context = ClientContext::new(world)?;
    println!("Client running on MPI process {}", context.get_rank());

    let rpc_data = RPCData {
        func_name: if context.get_rank() % 2 == 0 {
            "times_three"
        } else {
            "times_two"
        }
        .to_string(),
        data: vec![1; data_size],
    };

    println!(
        "[Client {}] Sending RPC: func_name='{}', data.len={}",
        context.get_rank(),
        rpc_data.func_name,
        rpc_data.data.len()
    );

    let target_rank = context.get_rank() % context.get_server_addresses().len() as i32;
    // Send RPC request
    match context
        .send_rpc(target_rank as usize, rpc_data.clone())
        .await
    {
        Ok(result) => {
            println!(
                "[Client {}] Received response : func_name='{}', data.len={}",
                context.get_rank(),
                result.func_name,
                result.data.len()
            );
        }
        Err(e) => {
            eprintln!("[Client {}] Request failed: {}", context.get_rank(), e);
        }
    }

    // Run benchmark with 1000 requests
    context
        .benchmark_rpc(target_rank as usize, num_requests, rpc_data.clone())
        .await;

    Ok(())
}
