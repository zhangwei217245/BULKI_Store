// Include the generated protobuf code
pub mod proto {
    tonic::include_proto!("bulkistore"); // assuming your proto package is named "bulkistore"
}

pub mod dispatch;
pub mod rpc;
