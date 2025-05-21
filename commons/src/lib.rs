// Include the generated protobuf code
pub mod err;
pub mod handler;
pub mod job;
pub mod object;
pub mod region;
pub mod rpc;
pub mod utils;

// Re-export the proc macro
pub use commons_macros::req_handler;
pub use commons_macros::resp_handler;
