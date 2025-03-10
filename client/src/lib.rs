pub mod cltctx;

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn version_py() -> String {
    VERSION.to_string()
}
