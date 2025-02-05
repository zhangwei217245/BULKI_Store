// mod cltctx;
// mod bk_ndarr;
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
//         context.initialize_blocking().expect("Failed to initialize client context");
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

mod bk_ndarr;
mod cltctx;
use crate::bk_ndarr::*;
use std::cell::RefCell;
use std::ops::Add;
use std::sync::Arc;

use bincode;
use cltctx::ClientContext;
use commons::region::SerializableNDArray;
use commons::rpc::RPCData;
use log::{debug, error, warn};
use numpy::ndarray::{Array1, ArrayD, ArrayView1, ArrayViewD, ArrayViewMutD, Zip};
use numpy::PyArray;
use numpy::{
    datetime::{units, Timedelta},
    Complex64, Element, IntoPyArray, PyArray1, PyArrayDyn, PyArrayMethods, PyReadonlyArray1,
    PyReadonlyArrayDyn, PyReadwriteArray1, PyReadwriteArrayDyn,
};
use pyo3::exceptions::PyIndexError;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    pymodule,
    types::{PyAnyMethods, PyDict, PyDictMethods, PyModule},
    Bound, FromPyObject, PyAny, PyObject, PyResult, Python,
};
use pyo3::{IntoPy, IntoPyObject, IntoPyObjectExt, PyErr};

thread_local! {
    static RUNTIME: RefCell<Option<tokio::runtime::Runtime>> = RefCell::new(None);
    static CONTEXT: RefCell<Option<ClientContext>> = RefCell::new(None);
}

#[pymodule]
#[pyo3(name = "bkstore_client")]
fn rust_ext<'py>(m: &Bound<'py, PyModule>) -> PyResult<()> {
    // Default to non-MPI mode for safety in interactive sessions
    let universe = None;

    let mut context = ClientContext::new();

    // Get or create the runtime from thread-local storage
    RUNTIME.with(|rt_cell| {
        let mut rt = rt_cell.borrow_mut();
        if rt.is_none() {
            *rt = Some(tokio::runtime::Runtime::new().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create Tokio runtime: {}",
                    e
                ))
            })?);
        }

        // Use the runtime to run the async initialization
        rt.as_ref()
            .unwrap()
            .block_on(context.initialize(universe))
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to initialize client context: {}",
                    e
                ))
            })?;

        Ok::<_, PyErr>(())
    })?;

    debug!("Client running in non-MPI mode");

    // Store the context in thread-local storage
    CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = Some(context);
    });

    #[pyfn(m)]
    #[pyo3(name = "init")]
    fn init_py(py: Python<'_>) -> PyResult<()> {
        // First check if MPI should be initialized
        let universe = {
            #[cfg(feature = "mpi")]
            {
                // Try to import mpi4py
                if let Ok(_mpi4py) = py.import("mpi4py.MPI") {
                    // MPI is available, initialize it
                    match mpi::initialize_with_threading(mpi::Threading::Multiple) {
                        Some((universe, _)) => Some(Arc::new(universe)),
                        None => {
                            debug!("MPI initialization failed");
                            None
                        }
                    }
                } else {
                    debug!("mpi4py not found, running without MPI");
                    None
                }
            }
            #[cfg(not(feature = "mpi"))]
            {
                None
            }
        };

        // Initialize or reinitialize context with the determined MPI state
        let mut context = ClientContext::new();

        RUNTIME.with(|rt_cell| {
            let mut rt = rt_cell.borrow_mut();
            if rt.is_none() {
                *rt = Some(tokio::runtime::Runtime::new().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to create Tokio runtime: {}",
                        e
                    ))
                })?);
            }

            // Initialize context with MPI if available
            rt.as_ref()
                .unwrap()
                .block_on(context.initialize(universe))
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to initialize context: {}",
                        e
                    ))
                })?;

            // Initialize network client
            rt.as_ref()
                .unwrap()
                .block_on(context.ensure_client_initialized())
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to initialize client: {}",
                        e
                    ))
                })?;

            Ok::<_, PyErr>(())
        })?;

        // Store the context
        CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = Some(context);
        });

        Ok(())
    }

    #[pyfn(m)]
    #[pyo3(name = "times_two")]
    fn times_two<'py>(py: Python<'py>, x: PyReadonlyArrayDyn<'py, f64>) -> PyResult<PyObject> {
        // Convert numpy array to rust ndarray
        let x_array: ndarray::ArrayBase<ndarray::OwnedRepr<f64>, ndarray::Dim<ndarray::IxDynImpl>> =
            x.as_array().to_owned();

        // Serialize the ndarray using messagepack
        let serialized = SerializableNDArray::serialize(x_array)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("Serialization error: {}", e)))?;

        // Send message to server 0 and get response
        let response = CONTEXT
            .with(|ctx| {
                let ctx = ctx.borrow();
                let ctx_ref = ctx.as_ref().expect("Context not initialized");
                RUNTIME.with(|rt_cell| {
                    let rt = rt_cell.borrow();
                    let rt_ref = rt.as_ref().expect("Runtime not initialized");
                    rt_ref.block_on(ctx_ref.send_message(
                        0,
                        "datastore::times_two",
                        RPCData {
                            metadata: None,
                            data: serialized,
                        },
                    ))
                })
            })
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("RPC error: {}", e)))?;

        // Get binary data from response
        let response_data = match response {
            data => data.data,
            _ => return Err(PyErr::new::<PyValueError, _>("Unexpected response type")),
        };

        // Deserialize back into rust ndarray
        let result: ArrayD<f64> = SerializableNDArray::deserialize(&response_data)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("Deserialization error: {}", e)))?;

        // Convert back to numpy array
        let py_array = PyArray::from_array(py, &result);
        Ok(py_array.into_py(py))
    }

    // example using generic PyObject
    fn head(py: Python<'_>, x: ArrayViewD<'_, PyObject>) -> PyResult<PyObject> {
        x.get(0)
            .map(|obj| obj.clone_ref(py))
            .ok_or_else(|| PyIndexError::new_err("array index out of range"))
    }

    // example using immutable borrows producing a new array
    fn axpy(a: f64, x: ArrayViewD<'_, f64>, y: ArrayViewD<'_, f64>) -> ArrayD<f64> {
        a * &x + &y
    }

    // example using a mutable borrow to modify an array in-place
    fn mult(a: f64, mut x: ArrayViewMutD<'_, f64>) {
        x *= a;
    }

    // example using complex numbers
    fn conj(x: ArrayViewD<'_, Complex64>) -> ArrayD<Complex64> {
        x.map(|c| c.conj())
    }

    // example using generics
    fn generic_add<T: Copy + Add<Output = T>>(
        x: ArrayView1<'_, T>,
        y: ArrayView1<'_, T>,
    ) -> Array1<T> {
        &x + &y
    }

    // wrapper of `head`
    #[pyfn(m)]
    #[pyo3(name = "head")]
    fn head_py<'py>(x: PyReadonlyArrayDyn<'py, PyObject>) -> PyResult<PyObject> {
        head(x.py(), x.as_array())
    }

    // wrapper of `axpy`
    #[pyfn(m)]
    #[pyo3(name = "axpy")]
    fn axpy_py<'py>(
        py: Python<'py>,
        a: f64,
        x: PyReadonlyArrayDyn<'py, f64>,
        y: PyReadonlyArrayDyn<'py, f64>,
    ) -> Bound<'py, PyArrayDyn<f64>> {
        let x = x.as_array();
        let y = y.as_array();
        let z = axpy(a, x, y);
        z.into_pyarray(py)
    }

    // wrapper of `mult`
    #[pyfn(m)]
    #[pyo3(name = "mult")]
    fn mult_py<'py>(a: f64, mut x: PyReadwriteArrayDyn<'py, f64>) {
        let x = x.as_array_mut();
        mult(a, x);
    }

    // wrapper of `conj`
    #[pyfn(m)]
    #[pyo3(name = "conj")]
    fn conj_py<'py>(
        py: Python<'py>,
        x: PyReadonlyArrayDyn<'py, Complex64>,
    ) -> Bound<'py, PyArrayDyn<Complex64>> {
        conj(x.as_array()).into_pyarray(py)
    }

    // example of how to extract an array from a dictionary
    #[pyfn(m)]
    fn extract(d: &Bound<'_, PyDict>) -> f64 {
        let x = d
            .get_item("x")
            .unwrap()
            .unwrap()
            .downcast_into::<PyArray1<f64>>()
            .unwrap();

        x.readonly().as_array().sum()
    }

    // example using timedelta64 array
    #[pyfn(m)]
    fn add_minutes_to_seconds<'py>(
        mut x: PyReadwriteArray1<'py, Timedelta<units::Seconds>>,
        y: PyReadonlyArray1<'py, Timedelta<units::Minutes>>,
    ) {
        #[allow(deprecated)]
        Zip::from(x.as_array_mut())
            .and(y.as_array())
            .for_each(|x, y| *x = (i64::from(*x) + 60 * i64::from(*y)).into());
    }

    // This crate follows a strongly-typed approach to wrapping NumPy arrays
    // while Python API are often expected to work with multiple element types.
    //
    // That kind of limited polymorphis can be recovered by accepting an enumerated type
    // covering the supported element types and dispatching into a generic implementation.
    #[derive(FromPyObject)]
    enum SupportedArray<'py> {
        F64(Bound<'py, PyArray1<f64>>),
        I64(Bound<'py, PyArray1<i64>>),
    }

    #[pyfn(m)]
    fn polymorphic_add<'py>(
        x: SupportedArray<'py>,
        y: SupportedArray<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        match (x, y) {
            (SupportedArray::F64(x), SupportedArray::F64(y)) => Ok(generic_add(
                x.readonly().as_array(),
                y.readonly().as_array(),
            )
            .into_pyarray(x.py())
            .into_any()),
            (SupportedArray::I64(x), SupportedArray::I64(y)) => Ok(generic_add(
                x.readonly().as_array(),
                y.readonly().as_array(),
            )
            .into_pyarray(x.py())
            .into_any()),
            (SupportedArray::F64(x), SupportedArray::I64(y))
            | (SupportedArray::I64(y), SupportedArray::F64(x)) => {
                let y = y.cast::<f64>(false)?;

                Ok(
                    generic_add(x.readonly().as_array(), y.readonly().as_array())
                        .into_pyarray(x.py())
                        .into_any(),
                )
            }
        }
    }

    // async fn create_grpc_client(
    //     address: &str,
    // ) -> Result<GrpcBulkistoreClient<tonic::transport::Channel>, Box<dyn std::error::Error>> {
    //     // First, validate the address
    //     match address.to_socket_addrs() {
    //         Ok(mut addr_iter) => {
    //             if addr_iter.next().is_none() {
    //                 error!("Invalid socket address: {}", address);
    //                 return Err("Invalid socket address".into());
    //             }
    //         }
    //         Err(e) => {
    //             error!("Failed to resolve address {}: {}", address, e);
    //             return Err(e.into());
    //         }
    //     }

    //     // Attempt connection with more detailed error logging
    //     match GrpcBulkistoreClient::connect(address).await {
    //         Ok(client) => Ok(client),
    //         Err(e) => {
    //             error!("Transport error connecting to {}: {}", address, e);
    //             warn!("Possible causes:");
    //             warn!("1. Server not running");
    //             warn!("2. Incorrect address or port");
    //             warn!("3. Network connectivity issues");
    //             warn!("4. Firewall blocking connection");
    //             Err(e.into())
    //         }
    //     }
    // }

    Ok(())
}

// /// A Python module implemented in Rust. The name of this function must match
// /// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
// /// import the module. https://pyo3.rs/v0.20.0/module
// #[pymodule]
// #[pyo3(name = "bkstore_client")]
// fn pyo3_example(m: &Bound<'_, PyModule>) -> PyResult<()> {
//     m.add_function(wrap_pyfunction!(sum_as_string, m)?);
//     Ok(())
// }
