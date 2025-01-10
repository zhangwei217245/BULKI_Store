fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("src/rpc/grpc/proto/grpc_bulkistore.proto")?;
    Ok(())
}
