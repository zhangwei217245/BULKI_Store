// mod cltctx;
// use anyhow;

// use ndarray::ArrayD;
// use pyo3::prelude::*;
// use tokio::runtime::Runtime;

// // Wrapper type for our error conversions
// struct BulkiError(anyhow::Error);

// impl From<anyhow::Error> for BulkiError {
//     fn from(err: anyhow::Error) -> Self {
//         BulkiError(err)
//     }
// }

// impl From<BulkiError> for PyErr {
//     fn from(err: BulkiError) -> PyErr {
//         PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.0.to_string())
//     }
// }

// use cltctx::ClientContext;

// #[pyclass]
// struct BulkiStoreClient {
//     context: Option<ClientContext>,
// }

// #[pymethods]
// impl BulkiStoreClient {
//     #[new]
//     fn new() -> Self {
//         BulkiStoreClient { context: None }
//     }

//     fn initialize(&mut self) -> PyResult<()> {
//         let mut context = ClientContext::new();
//         let rt = Runtime::new().unwrap();
//         rt.block_on(context.initialize())
//             .map_err(BulkiError::from)
//             .map_err(PyErr::from)?;
//         self.context = Some(context);
//         Ok(())
//     }

//     fn run_benchmark(&self) -> PyResult<()> {
//         let context = self
//             .context
//             .as_ref()
//             .ok_or(anyhow::anyhow!("Client not initialized"))
//             .map_err(BulkiError::from)
//             .map_err(PyErr::from)?;

//         // Example parameters - you may want to make these configurable
//         let num_requests = 1000;
//         let data_len = 1000; // Size of the test data in bytes

//         let stats = context.benchmark_rpc(num_requests, data_len);
//         let stats = Runtime::new()?.block_on(stats);
//         stats.print_stats();
//         Ok(())
//     }
// }

// #[pymodule]
// fn bulkistore_client(_py: Python, m: &PyModule) -> PyResult<()> {
//     m.add_class::<BulkiStoreClient>()?;
//     Ok(())
// }

use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
#[pyo3(name = "bkstore_client")]
fn pyo3_example(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
