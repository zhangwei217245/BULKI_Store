mod cltctx;
use anyhow;
use ndarray::ArrayD;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::runtime::Runtime;

// Wrapper type for our error conversions
struct BulkiError(anyhow::Error);

impl From<anyhow::Error> for BulkiError {
    fn from(err: anyhow::Error) -> Self {
        BulkiError(err)
    }
}

impl From<BulkiError> for PyErr {
    fn from(err: BulkiError) -> PyErr {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.0.to_string())
    }
}

use cltctx::ClientContext;

#[pyclass]
struct BulkiStoreClient {
    context: Option<ClientContext>,
}

#[pymethods]
impl BulkiStoreClient {
    #[new]
    fn new() -> Self {
        BulkiStoreClient { context: None }
    }

    fn initialize(&mut self) -> PyResult<()> {
        let mut context = ClientContext::new();
        let rt = Runtime::new().unwrap();
        rt.block_on(context.initialize())?;
        self.context = Some(context);
        Ok(())
    }

    fn run_benchmark(&self) -> PyResult<()> {
        let context = self
            .context
            .as_ref()
            .ok_or(anyhow::anyhow!("Client not initialized"))?;

        // Example parameters - you may want to make these configurable
        let server_rank = 0;
        let num_requests = 1000;
        let handler_name = "test_handler".to_string();
        let data = vec![1, 2, 3, 4]; // Example data

        let stats = context.benchmark_rpc(server_rank, num_requests, handler_name, data);
        let stats = Runtime::new()?.block_on(stats);
        stats.print_stats();
        Ok(())
    }
}

#[pymodule]
fn bulkistore_client(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<BulkiStoreClient>()?;
    Ok(())
}
