pub mod common {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug)]
    pub struct RPCData {
        pub func_name: String,
        pub data: Vec<u8>,
    }
}

// Include the generated protobuf code
pub mod proto {
    tonic::include_proto!("bulkistore"); // assuming your proto package is named "bulkistore"
}
