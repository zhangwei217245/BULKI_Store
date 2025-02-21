fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=src/rpc/grpc/proto/bulkistore.proto");
    
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["src/rpc/grpc/proto/bulkistore.proto"], &["src/rpc/grpc/proto"])?;
    Ok(())
}
