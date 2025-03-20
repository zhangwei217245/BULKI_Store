pub mod converter;
mod proc;
use client::cltctx::{get_client_count, get_client_rank, get_server_count, ClientContext};

use commons::err::RPCResult;
use commons::object::objid::GlobalObjectIdExt;
use commons::object::params::{
    CreateObjectParams, GetObjectMetaParams, GetObjectMetaResponse, GetObjectSliceParams,
    GetObjectSliceResponse,
};
use commons::object::types::{ObjectIdentifier, SupportedRustArrayD};
use converter::{IntoBoundPyAny, MetaKeySpec, SupportedNumpyArray};

use log::{debug, info};
use numpy::{
    ndarray::{ArrayD, ArrayViewD, ArrayViewMutD, Axis},
    Complex64,
};
use pyo3::types::PyInt;
use pyo3::{
    exceptions::PyValueError,
    types::{PyDict, PySlice},
    Bound, PyErr, PyObject, PyResult, Python,
};
use pyo3::{IntoPyObjectExt, Py, PyAny};
use serde::{Deserialize, Serialize};
use std::time::Instant;
// #[cfg(feature = "mpi")]
// use std::sync::Arc;
use std::{cell::RefCell, ops::Add};

thread_local! {
    static RUNTIME: RefCell<Option<tokio::runtime::Runtime>> = RefCell::new(None);
    static CONTEXT: RefCell<Option<ClientContext>> = RefCell::new(None);
    // request counter:
    static REQUEST_COUNTER: RefCell<u32> = RefCell::new(0);
}

pub fn init_py<'py>(_py: Python<'py>, rank: Option<u32>, size: Option<u32>) -> PyResult<()> {
    // First check if MPI should be initialized
    let universe = {
        #[cfg(feature = "mpi")]
        {
            // use mpi::environment::Universe;
            // use mpi::traits::*;
            use pyo3::types::PyAnyMethods;
            use std::sync::Arc;
            // Try to check if mpi4py is imported.
            match _py.import("mpi4py.MPI") {
                Ok(mpi4py) => {
                    info!("mpi4py found, checking MPI initialization status...");
                    // CRITICAL: Check if MPI is already initialized by mpi4py
                    let is_initialized = mpi4py
                        .getattr("Is_initialized")
                        .and_then(|f| f.call0())
                        .and_then(|r| r.extract::<bool>())
                        .unwrap_or(false);

                    match is_initialized {
                        true => {
                            info!("MPI already initialized by mpi4py, try to initialize from Rust, if not successful, will treat as single process");
                            match mpi::initialize() {
                                Some(universe) => Some(Arc::new(universe)),
                                None => {
                                    use log::error;
                                    error!("Failed to initialize MPI from Rust, will treat as single process");
                                    None
                                }
                            }
                        }
                        false => {
                            info!("MPI not initialized by mpi4py, try to initialize from Rust, if not successful, will treat as single process");
                            match mpi::initialize_with_threading(mpi::Threading::Multiple) {
                                Some((universe, _)) => Some(Arc::new(universe)),
                                None => {
                                    use log::error;
                                    error!("Failed to initialize MPI from Rust, will treat as single process");
                                    None
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    info!("mpi4py not found, try to initialize MPI from Rust");
                    match mpi::initialize_with_threading(mpi::Threading::Multiple) {
                        Some((universe, _)) => Some(Arc::new(universe)),
                        None => {
                            use log::error;
                            error!("Failed to initialize MPI from Rust");
                            None
                        }
                    }
                }
            }
        }
        #[cfg(not(feature = "mpi"))]
        {
            None
        }
    };

    if universe.is_some() {
        info!("Running pyclient with MPI");
    } else {
        info!("Running pyclient without MPI");
    }

    // Initialize context using our new datastore function
    let mut context = ClientContext::new();

    // Initialize context with proper runtime management
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
            .block_on(context.initialize(universe, rank, size))
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

fn rpc_call<T, R>(srv_id: u32, method_name: &str, input: &T) -> RPCResult<R>
where
    T: Serialize + std::marker::Sync + 'static,
    R: for<'de> Deserialize<'de>,
{
    let response = CONTEXT.with(|ctx| {
        let ctx = ctx.borrow();
        let ctx_ref = ctx.as_ref().expect("Context not initialized");
        RUNTIME.with(|rt_cell| {
            let rt = rt_cell.borrow();
            let rt_ref = rt.as_ref().expect("Runtime not initialized");
            rt_ref.block_on(ctx_ref.send_message::<T, R>(srv_id as usize, method_name, input))
        })
    });
    // Increment request counter
    REQUEST_COUNTER.with(|counter| {
        let mut counter = counter.borrow_mut();
        *counter += 1;
    });
    response
}

pub fn create_object_impl<'py>(
    py: Python<'py>,
    obj_name_key: String,
    parent_id: Option<u128>,
    metadata: Option<Bound<'py, PyDict>>,
    data: Option<SupportedNumpyArray<'py>>,
    array_meta_list: Option<Vec<Option<Bound<'py, PyDict>>>>,
    array_data_list: Option<Vec<Option<SupportedNumpyArray<'py>>>>,
) -> PyResult<Vec<Py<PyInt>>> {
    let timer = Instant::now();
    let create_obj_params: Vec<commons::object::params::CreateObjectParams> =
        proc::create_objects_req_proc(
            obj_name_key,
            parent_id,
            metadata,
            data,
            array_meta_list,
            array_data_list,
        )
        .map_err(|e| {
            PyErr::new::<PyValueError, _>(format!("Failed to create object parameters: {}", e))
        })?;
    let main_obj_id = create_obj_params[0].obj_id;
    // Get binary data from response (assuming response.data contains the binary payload)
    let result = rpc_call::<Vec<CreateObjectParams>, Vec<u128>>(
        main_obj_id.vnode_id() % get_server_count(),
        "datastore::create_objects",
        &create_obj_params,
    )
    .map_err(|e| PyErr::new::<PyValueError, _>(format!("Failed to create objects: {}", e)))?;
    debug!("create_objects: result vector length: {:?}", result.len());
    debug!("create_objects: result vector: {:?}", result);
    info!(
        "[R{}/S{}] create_objects: result vector: {:?} in {:?}ms",
        get_client_rank(),
        get_client_count(),
        result,
        timer.elapsed().as_millis()
    );
    converter::convert_vec_u128_to_py_long(py, result)
}

pub fn get_object_metadata_impl<'py>(
    py: Python<'py>,
    obj_id: ObjectIdentifier,
    meta_keys: Option<Vec<String>>,
    sub_meta_keys: Option<MetaKeySpec>,
) -> PyResult<Py<PyDict>> {
    let timer = Instant::now();
    let vnode_id = obj_id.vnode_id();
    let get_object_metadata_params =
        proc::get_object_metadata_req_proc(obj_id, meta_keys, sub_meta_keys);
    match get_object_metadata_params {
        Err(_) => Err(PyErr::new::<PyValueError, _>(
            "Failed to create get_object_metadata_params",
        )),
        Ok(params) => {
            let result = rpc_call::<GetObjectMetaParams, GetObjectMetaResponse>(
                vnode_id % get_server_count(),
                "datastore::get_object_metadata",
                &params,
            )
            .map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Failed to get object metadata: {}", e))
            })?;
            debug!("get_object_metadata: result: {:?}", result);
            info!(
                "[R{}/S{}] get_object_metadata: result: {:?}, {:?} in {:?}ms",
                get_client_rank(),
                get_client_count(),
                result.obj_id,
                result.obj_name,
                timer.elapsed().as_millis()
            );
            converter::convert_get_object_meta_response_to_pydict(py, result)
        }
    }
}

pub fn get_object_data_impl<'py>(
    py: Python<'py>,
    obj_id: ObjectIdentifier,
    region: Option<Vec<Bound<'py, PySlice>>>,
    sub_obj_regions: Option<Vec<(String, Vec<Bound<'py, PySlice>>)>>,
) -> PyResult<Py<PyDict>> {
    let timer = Instant::now();
    let vnode_id = obj_id.vnode_id();
    let get_object_data_params = proc::get_object_slice_req_proc(obj_id, region, sub_obj_regions);
    match get_object_data_params {
        Err(_) => Err(PyErr::new::<PyValueError, _>(
            "Failed to create object parameters",
        )),
        Ok(params) => {
            let result = rpc_call::<GetObjectSliceParams, GetObjectSliceResponse>(
                vnode_id % get_server_count(),
                "datastore::get_object_data",
                &params,
            )
            .map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Failed to get object data: {}", e))
            })?;
            // debug!("get_object_data: result vector: {:?}", result);
            info!(
                "[R{}/S{}] get_object_data: result: {:?}, {:?} in {:?}ms",
                get_client_rank(),
                get_client_count(),
                result.obj_id,
                result.obj_name,
                timer.elapsed().as_millis()
            );
            converter::convert_get_object_slice_response_to_pydict(py, result)
        }
    }
}

pub fn force_checkpointing_impl<'py>(py: Python<'py>) -> PyResult<Py<PyAny>> {
    let args = ("client".to_string(), 0);
    let mut results = Vec::new();
    for i in 0..get_server_count() {
        let result = rpc_call::<(String, u32), u32>(i, "datastore::force_checkpointing", &args)
            .map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Failed to force checkpointing: {}", e))
            })?;
        results.push(result);
    }

    Ok(results
        .into_bound_py_any(py)
        .and_then(|rst| Ok(rst.unbind()))?)
}

pub fn times_two_impl<'py>(py: Python<'py>, x: SupportedNumpyArray<'py>) -> PyResult<PyObject> {
    let input = x.into_array_type();

    let srv_id = REQUEST_COUNTER.with(|counter| (*counter.borrow() as u32) % get_server_count());

    let result = rpc_call::<SupportedRustArrayD, SupportedRustArrayD>(
        srv_id,
        "datastore::times_two",
        &input,
    )
    .map_err(|e| PyErr::new::<PyValueError, _>(format!("RPC error: {}", e)))?;

    Ok(result
        .into_bound_py_any(py)
        .and_then(|rst| Ok(rst.unbind()))
        .map_err(|e| {
            PyErr::new::<PyValueError, _>(format!(
                "Failed to convert SupportedRustArrayD to PyAny: {}",
                e
            ))
        })?)
}

// example using generic T
pub fn head<T: Copy + Add<Output = T>>(_py: Python<'_>, x: ArrayViewD<'_, T>) -> ArrayD<T> {
    // Ensure the array has at least one dimension
    assert!(x.ndim() > 0, "Input array must have at least one dimension");

    // Get the first element along the first dimension.
    let first = x.index_axis(Axis(0), 0).to_owned();

    // Convert to a dynamic array (ArrayD<T>) and return.
    first.into_dyn()
}

// example using immutable borrows producing a new array
pub fn axpy(a: f64, x: ArrayViewD<'_, f64>, y: ArrayViewD<'_, f64>) -> ArrayD<f64> {
    a * &x + &y
}

// example using a mutable borrow to modify an array in-place
pub fn mult(a: f64, mut x: ArrayViewMutD<'_, f64>) {
    x *= a;
}

// example using complex numbers
pub fn conj(x: ArrayViewD<'_, Complex64>) -> ArrayD<Complex64> {
    x.map(|c| c.conj())
}

// example using generics
pub fn generic_add<T: Copy + Add<Output = T>>(
    x: ArrayViewD<'_, T>,
    y: ArrayViewD<'_, T>,
) -> ArrayD<T> {
    &x + &y
}

pub fn array_slicing<'py, T: Copy>(
    x: ArrayViewD<'_, T>,
    indices: Vec<Bound<'py, PySlice>>,
) -> PyResult<ArrayD<T>> {
    // Convert all Python slices into ndarray slice elements
    let slice_spec = converter::convert_pyslice_vec_to_rust_slice_vec(x.ndim(), Some(indices))?;
    // Apply slicing and return an owned copy
    Ok(x.slice(&slice_spec[..]).to_owned())
}
