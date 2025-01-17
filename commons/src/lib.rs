pub mod dispatch;
pub mod common {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Clone, Deserialize, Debug)]
    pub struct RPCData {
        pub func_name: String,
        pub data: Vec<u8>,
    }
}

// Include the generated protobuf code

pub mod handler;
pub mod object;
pub mod region;
pub mod rpc;
pub mod utils;

// Re-export the proc macro
pub use commons_macros::req_handler;
pub use commons_macros::resp_handler;
