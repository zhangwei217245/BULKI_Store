pub mod converter;
mod proc;
use client::cltctx::{get_client_count, get_client_rank, get_server_count, ClientContext};

use commons::err::RPCResult;
use commons::job::JobProgress;
use commons::object::objid::GlobalObjectIdExt;
use commons::object::params::{
    CreateObjectParams, GetObjectMetaParams, GetObjectMetaResponse, GetObjectSliceParams,
    GetObjectSliceResponse, GetSampleRequest, GetSampleResponse,
};
use commons::object::types::{ObjectIdentifier, SupportedRustArrayD};
use commons::utils::SystemUtility;
use converter::{IntoBoundPyAny, MetaKeySpec, SupportedNumpyArray};

use crossbeam::queue::SegQueue;
use lazy_static::lazy_static;
use log::{debug, info, warn};
use numpy::{
    ndarray::{ArrayD, ArrayViewD, ArrayViewMutD, Axis},
    Complex64,
};
use once_cell::sync::OnceCell;
use pyo3::types::{PyDictMethods, PyInt};
use pyo3::{
    exceptions::PyValueError,
    types::{PyDict, PySlice},
    Bound, PyErr, PyObject, PyResult, Python,
};
use pyo3::{IntoPyObjectExt, Py, PyAny};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env::VarError;
use std::ffi::CStr;
use std::ops::Add;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

// Process-wide static variables with thread-safe access
// These are initialized once per process and shared across threads
static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
// static BLOCKING_CONTEXT: OnceCell<std::sync::Mutex<ClientContext>> = OnceCell::new();
static ASYNC_CONTEXT: OnceCell<tokio::sync::Mutex<ClientContext>> = OnceCell::new();
// Atomic counter for thread-safe incrementing across threads
static REQUEST_COUNTER: AtomicU32 = AtomicU32::new(0);
// Flag to control memory monitoring thread

lazy_static! {
    pub static ref GLOBAL_DATA_QUEUE: Arc<SegQueue<Py<PyAny>>> = Arc::new(SegQueue::new());
    pub static ref GLOBAL_INDEX_QUEUE: Arc<SegQueue<(usize, Vec<(usize, usize)>)>> = Arc::new(SegQueue::new());
    static ref PREFETCH_THREAD_POOL: rayon::ThreadPool = {
        // Use number of available cores or a reasonable fixed size
        let num_threads = std::cmp::min(num_cpus::get(), 8);
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .expect("Failed to build Rayon thread pool")
    };
    pub static ref NEXT_BATCH_INDEX: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref LOG_EXEC_COUNTER: AtomicU32 = AtomicU32::new(0);
}

pub fn init_py<'py>(
    _py: Python<'py>,
    rank: Option<u32>,
    size: Option<u32>,
    batch_size: Option<usize>,
) -> PyResult<()> {
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
                                    warn!("Failed to initialize MPI from Rust, will treat as single process");
                                    None
                                }
                            }
                        }
                        false => {
                            info!("MPI not initialized by mpi4py, try to initialize from Rust, if not successful, will treat as single process");
                            match mpi::initialize_with_threading(mpi::Threading::Multiple) {
                                Some((universe, _)) => Some(Arc::new(universe)),
                                None => {
                                    warn!("Failed to initialize MPI from Rust, will treat as single process");
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
                            warn!("Failed to initialize MPI from Rust");
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

    // Initialize the runtime once per process
    if RUNTIME.get().is_none() {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create Tokio runtime: {}",
                e
            ))
        })?;

        // This will fail if another thread already initialized the runtime, which is fine
        let _ = RUNTIME.set(rt);
    }

    // Initialize the context once per process
    if ASYNC_CONTEXT.get().is_none() {
        // Create a new context
        let mut context = ClientContext::new();
        // Get a reference to the runtime
        let rt = RUNTIME.get().expect("Runtime not initialized");
        // Initialize context with MPI if available
        rt.block_on(context.initialize(universe, rank, size, batch_size))
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to initialize context: {}",
                    e
                ))
            })?;
        // Initialize network client
        rt.block_on(context.ensure_client_initialized())
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to initialize client: {}",
                    e
                ))
            })?;
        let _ = ASYNC_CONTEXT.set(tokio::sync::Mutex::new(context));
    } else {
        // Context already exists, but we might need to update it with new parameters
        let rt = RUNTIME.get().expect("Runtime not initialized");
        rt.block_on(async {
            let mut context_guard = ASYNC_CONTEXT
                .get()
                .expect("Context not initialized")
                .lock()
                .await;
            // Reinitialize with new parameters if provided
            if rank.is_some() || size.is_some() || batch_size.is_some() {
                let _ = context_guard
                    .initialize(universe, rank, size, batch_size)
                    .await
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to reinitialize context: {}",
                            e
                        ))
                    });
                let _ = context_guard
                    .ensure_client_initialized()
                    .await
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to ensure client initialized: {}",
                            e
                        ))
                    });
            }
        });
    }
    Ok(())
}

/// Shuts down the client and cleans up resources
pub fn close_py<'py>(_py: Python<'py>) -> PyResult<()> {
    // Add any other cleanup tasks here
    info!(
        "[R{}/S{}] Client shutdown complete",
        get_client_rank(),
        get_client_count()
    );
    Ok(())
}

fn rpc_call<T, R>(srv_id: u32, method_name: &str, input: &T) -> RPCResult<R>
where
    T: Serialize + std::marker::Sync + 'static,
    R: for<'de> Deserialize<'de>,
{
    // Get references to our static variables
    let rt = RUNTIME.get().expect("Runtime not initialized");
    // Make the RPC call
    let response = rt.block_on(async {
        ASYNC_CONTEXT
            .get()
            .expect("Context not initialized")
            .lock()
            .await
            .send_message::<T, R>(srv_id as usize, method_name, input)
            .await
    });
    // Increment request counter atomically
    REQUEST_COUNTER.fetch_add(1, Ordering::SeqCst);

    response
}
#[allow(dead_code)]
async fn async_rpc_call<T, R>(srv_id: u32, method_name: &str, input: &T) -> RPCResult<R>
where
    T: Serialize + std::marker::Sync + 'static,
    R: for<'de> Deserialize<'de>,
{
    // Get references to our static variables
    let context_mutex = ASYNC_CONTEXT.get().expect("Context not initialized");
    // Acquire the lock to access the context
    let context = context_mutex.lock().await;
    // Make the RPC call
    let response = context
        .send_message::<T, R>(srv_id as usize, method_name, input)
        .await;
    // Increment request counter atomically
    REQUEST_COUNTER.fetch_add(1, Ordering::SeqCst);

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
    let create_obj_params: Vec<commons::object::params::CreateObjectParams> = {
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
        })?
    };
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
    if LOG_EXEC_COUNTER.fetch_add(1, Ordering::Relaxed) % 100 == 0 {
        info!(
            "[R{}/S{}] create_objects: result vector: {:?} in {:?} ms, memory: {:?} MB",
            get_client_rank(),
            get_client_count(),
            result,
        timer.elapsed().as_millis(),
        SystemUtility::get_current_memory_usage_mb()
    );
    drop(create_obj_params);
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
            if LOG_EXEC_COUNTER.fetch_add(1, Ordering::Relaxed) % 100 == 0 {
                info!(
                    "[R{}/S{}] get_object_metadata: result: {:?}, {:?} in {:?} ms, memory: {:?} MB",
                    get_client_rank(),
                    get_client_count(),
                    result.obj_id,
                result.obj_name,
                timer.elapsed().as_millis(),
                SystemUtility::get_current_memory_usage_mb()
            );
            converter::convert_get_object_meta_response_to_pydict(py, result)
                .map(|dict| dict.into())
        }
    }
}

pub fn get_multiple_object_metadata_impl<'py>(
    py: Python<'py>,
    obj_ids: Vec<ObjectIdentifier>,
    arr_meta_keys: Option<Vec<Vec<String>>>,
    arr_sub_meta_keys: Option<Vec<MetaKeySpec>>,
) -> PyResult<Py<PyDict>> {
    let timer = Instant::now();
    // group obj_ids by vnode_id
    let mut grouped_request = HashMap::new();
    for (i, obj_id) in obj_ids.iter().enumerate() {
        let meta_keys = arr_meta_keys.as_ref().map(|v| v[i].clone());
        let sub_meta_keys = arr_sub_meta_keys.as_ref().map(|v| v[i].clone());
        let get_object_metadata_params =
            proc::get_object_metadata_req_proc(obj_id.clone(), meta_keys, sub_meta_keys).unwrap();
        grouped_request
            .entry(obj_id.vnode_id())
            .or_insert_with(Vec::new)
            .push(get_object_metadata_params);
    }

    // get metadata for each vnode_id
    let results = PyDict::new(py);
    for (vnode_id, obj_meta_params) in grouped_request {
        let result = rpc_call::<Vec<GetObjectMetaParams>, Vec<Option<GetObjectMetaResponse>>>(
            vnode_id % get_server_count(),
            "datastore::get_multiple_object_metadata",
            &obj_meta_params,
        )
        .map_err(|e| {
            PyErr::new::<PyValueError, _>(format!("Failed to get multiple object metadata: {}", e))
        })?;
        for response in result {
            let obj_id = response.as_ref().unwrap().obj_id;
            let obj_name = response.as_ref().unwrap().obj_name.clone();
            let result_dict =
                converter::convert_get_object_meta_response_to_pydict(py, response.unwrap())?;
            match &obj_ids[0] {
                ObjectIdentifier::U128(_id) => {
                    results.set_item(obj_id, result_dict)?;
                }
                ObjectIdentifier::Name(_name) => {
                    results.set_item(obj_name, result_dict)?;
                }
            };
        }
    }
    if LOG_EXEC_COUNTER.fetch_add(1, Ordering::Relaxed) % 100 == 0 {
        info!(
            "[R{}/S{}] get_multiple_object_metadata complete, {:?} in {:?} ms, memory usage: {} MB",
            get_client_rank(),
            get_client_count(),
            obj_ids.len(),
            timer.elapsed().as_millis(),
            SystemUtility::get_current_memory_usage_mb()
        );
    }
    Ok(results.into())
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
    let sim_data = false;
    match get_object_data_params {
        Err(_) => Err(PyErr::new::<PyValueError, _>(
            "Failed to create object parameters",
        )),
        Ok(params) => {
            let result = match sim_data {
                // generate random data based on given region/sub-region
                true => proc::gen_sim_data(params).map_err(|e| {
                    PyErr::new::<PyValueError, _>(format!("Failed to get object data: {}", e))
                }),
                false => rpc_call::<GetObjectSliceParams, GetObjectSliceResponse>(
                    vnode_id % get_server_count(),
                    "datastore::get_object_data",
                    &params,
                )
                .map_err(|e| {
                    PyErr::new::<PyValueError, _>(format!("Failed to get object data: {}", e))
                }),
            }?;

            if LOG_EXEC_COUNTER.fetch_add(1, Ordering::Relaxed) % 100 == 0 {
                info!(
                    "[R{}/S{}] get_object_data: result: {:?}, {:?} in {:?}ms, memory: {:?} MB",
                    get_client_rank(),
                    get_client_count(),
                    result.obj_id,
                    result.obj_name,
                    timer.elapsed().as_millis(),
                    SystemUtility::get_current_memory_usage_mb()
                );
            }
            converter::convert_get_object_slice_response_to_pydict(py, result)
                .map(|dict| dict.into())
        }
    }
}

pub fn get_multiple_object_data_impl<'py>(
    py: Python<'py>,
    obj_ids: Vec<ObjectIdentifier>,
    arr_regions: Option<Vec<Vec<Bound<'py, PySlice>>>>,
    arr_sub_obj_regions: Option<Vec<Vec<(String, Vec<Bound<'py, PySlice>>)>>>,
) -> PyResult<Py<PyDict>> {
    let timer = Instant::now();
    let mut grouped_request = HashMap::new();
    for (i, obj_id) in obj_ids.iter().enumerate() {
        let regions = arr_regions.as_ref().map(|v| v[i].clone());
        let sub_obj_regions = arr_sub_obj_regions.as_ref().map(|v| v[i].clone());
        let get_object_data_params =
            proc::get_object_slice_req_proc(obj_id.clone(), regions, sub_obj_regions).unwrap();
        grouped_request
            .entry(obj_id.vnode_id())
            .or_insert_with(Vec::new)
            .push(get_object_data_params);
    }

    // get metadata for each vnode_id
    let results = PyDict::new(py);
    for (vnode_id, obj_data_params) in grouped_request {
        let result = rpc_call::<Vec<GetObjectSliceParams>, Vec<GetObjectSliceResponse>>(
            vnode_id % get_server_count(),
            "datastore::get_multiple_object_data",
            &obj_data_params,
        )
        .map_err(|e| {
            PyErr::new::<PyValueError, _>(format!("Failed to get multiple object metadata: {}", e))
        })?;
        for response in result {
            let obj_id = response.obj_id;
            let obj_name = response.obj_name.clone();
            let result_dict = converter::convert_get_object_slice_response_to_pydict(py, response)?;
            match &obj_ids[0] {
                ObjectIdentifier::U128(_id) => {
                    results.set_item(obj_id, result_dict)?;
                }
                ObjectIdentifier::Name(_name) => {
                    results.set_item(obj_name, result_dict)?;
                }
            };
        }
    }
    if LOG_EXEC_COUNTER.fetch_add(1, Ordering::Relaxed) % 100 == 0 {
        info!(
            "[R{}/S{}] get_multiple_object_data complete, {:?} in {:?} ms, memory usage: {} MB",
            get_client_rank(),
            get_client_count(),
            obj_ids.len(),
            timer.elapsed().as_millis(),
            SystemUtility::get_current_memory_usage_mb()
        );
    }
    Ok(results.into())
}

pub fn pop_queue_data_impl<'py>(_py: Python<'py>) -> PyResult<Py<PyAny>> {
    let data = GLOBAL_DATA_QUEUE.pop();
    debug!(
        "[R{}/S{}] queue length after pop_queue_data: {:?}, memory usage: {} MB",
        get_client_rank(),
        get_client_count(),
        GLOBAL_DATA_QUEUE.len(),
        SystemUtility::get_current_memory_usage_mb()
    );
    data.ok_or(PyErr::new::<PyValueError, _>("Queue is empty"))
}

pub fn check_queue_length_impl<'py>(py: Python<'py>) -> PyResult<Py<PyAny>> {
    debug!(
        "[R{}/S{}] current queue length: {:?}, memory usage: {} MB",
        get_client_rank(),
        get_client_count(),
        GLOBAL_DATA_QUEUE.len(),
        SystemUtility::get_current_memory_usage_mb()
    );
    GLOBAL_DATA_QUEUE.len().into_py_any(py)
}

pub fn fetch_samples_impl<'py>(
    py: Python<'py>,
    label: String,
    sample_ids: Vec<usize>,
    part_size: usize,
    sample_var_keys: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let timer = Instant::now();
    // Clone the data we need to move into the background task
    let sample_ids_clone = sample_ids.clone();

    // Calculate partition mappings (objectId, local_sample_id, global_sample_id)
    let mut grouped_request: HashMap<u32, Vec<GetSampleRequest>> = HashMap::new();
    sample_ids_clone
        .iter()
        .enumerate()
        .for_each(|(original_idx, &global_sample_id)| {
            let part_id = global_sample_id / part_size;
            let local_smp_id = global_sample_id % part_size;
            let obj_identifier = ObjectIdentifier::Name(format!("{}/part_{}", label, part_id));
            let vnode_id = obj_identifier.vnode_id();
            let request = GetSampleRequest {
                original_idx,
                sample_id: global_sample_id,
                obj_id: obj_identifier,
                local_sample_id: local_smp_id,
                sample_var_keys: sample_var_keys.clone(),
            };
            grouped_request
                .entry(vnode_id)
                .and_modify(|v: &mut Vec<GetSampleRequest>| v.push(request.clone()))
                .or_insert_with(|| vec![request.clone()]);
        });

    let results = grouped_request
        .par_iter()
        .map(|(&vnode_id, requests)| {
            match rpc_call::<Vec<GetSampleRequest>, HashMap<usize, GetSampleResponse>>(
                vnode_id % get_server_count(),
                "datastore::load_batch_samples",
                requests,
            ) {
                Ok(result) => result,
                Err(e) => {
                    eprintln!(
                        "Error in background prefetch task for server {}: {}",
                        vnode_id % get_server_count(),
                        e
                    );
                    HashMap::new()
                }
            }
        })
        .reduce(
            || HashMap::new(),
            |mut acc, x| {
                acc.extend(x);
                acc
            },
        );

    // Convert results to Python dictionary
    let dict = PyDict::new(py);
    for (sample_id, response) in results {
        dict.set_item(
            sample_id,
            converter::convert_sample_response_to_pydict(py, Some(&response))?,
        )?;
    }
    let elapsed_time = timer.elapsed().as_millis();
    if LOG_EXEC_COUNTER.fetch_add(1, Ordering::Relaxed) % 100 == 0 {
        info!(
            "[R{}/S{}] fetch_samples: {:?} in {:?} ms, memory: {:?} MB",
            get_client_rank(),
            get_client_count(),
            sample_ids.len(),
            elapsed_time,
            SystemUtility::get_current_memory_usage_mb()
        );
    }
    Ok(dict.into())
}

pub fn prefetch_samples_normal_impl<'py>(
    py: Python<'py>,
    label: String,
    sample_ids: Vec<usize>,
    part_size: usize,
    sample_var_keys: Vec<String>,
) -> PyResult<Py<PyAny>> {
    // Clone the data we need to move into the background task
    let sample_ids_clone = sample_ids.clone();
    let label_clone = label.clone();
    let sample_var_keys_clone = sample_var_keys.clone();
    // Calculate partition mappings (objectId, local_sample_id, global_sample_id)
    let mut grouped_request: HashMap<u32, Vec<GetSampleRequest>> = HashMap::new();
    sample_ids_clone
        .iter()
        .enumerate()
        .for_each(|(original_idx, &global_sample_id)| {
            let part_id = global_sample_id / part_size;
            let local_smp_id = global_sample_id % part_size;
            let obj_identifier =
                ObjectIdentifier::Name(format!("{}/part_{}", label_clone, part_id));
            let vnode_id = obj_identifier.vnode_id();
            let request = GetSampleRequest {
                original_idx,
                sample_id: global_sample_id,
                obj_id: obj_identifier,
                local_sample_id: local_smp_id,
                sample_var_keys: sample_var_keys_clone.clone(),
            };
            grouped_request
                .entry(vnode_id)
                .and_modify(|v: &mut Vec<GetSampleRequest>| v.push(request.clone()))
                .or_insert_with(|| vec![request.clone()]);
        });

    // Replace the current implementation with this:
    for (vnode_id, requests) in grouped_request {
        let queue_arc = Arc::clone(&GLOBAL_DATA_QUEUE);
        let requests_clone = requests.clone();
        let server_id = vnode_id % get_server_count();

        PREFETCH_THREAD_POOL.spawn(move || {
            // Process requests in this background thread
            let result = match rpc_call::<Vec<GetSampleRequest>, HashMap<usize, GetSampleResponse>>(
                server_id,
                "datastore::load_batch_samples",
                &requests_clone,
            ) {
                Ok(result) => result,
                Err(e) => {
                    error!(
                        "Error in background prefetch task for server {}: {}",
                        server_id, e
                    );
                    HashMap::new()
                }
            };

            // Push results directly to the global queue
            Python::with_gil(|py| {
                let found_count = result
                    .iter()
                    .map(|(_global_sample_id, data)| {
                        match converter::convert_sample_response_to_pydict(
                            py,
                            Some(data),
                        ) {
                            Ok(dict) => {
                                if let Ok(pyobj) = dict.into_py_any(py) {
                                    queue_arc.push(pyobj);
                                    1
                                } else {
                                    0
                                }
                            }
                            Err(_) => 0,
                        }
                    })
                    .sum::<usize>();

                if LOG_EXEC_COUNTER.fetch_add(1, Ordering::Relaxed) % 100 == 0 {
                    info!(
                        "[R{}/S{}] Background thread for server {} completed: {}/{} samples loaded, memory: {:?} MB",
                        get_client_rank(),
                        get_client_count(),
                        server_id,
                        found_count,
                        requests_clone.len(),
                        SystemUtility::get_current_memory_usage_mb()
                    );
                }
            });
        });
    }
    // Return the number of samples requested (not the actual queue length yet)
    sample_ids.len().into_py_any(py)
}

pub fn prefetch_samples_into_queue_impl<'py>(
    py: Python<'py>,
    label: String,
    sample_ids: Vec<usize>,
    part_size: usize,
    sample_var_keys: Vec<String>,
    batch_size: Option<usize>,
    prefetch_factor: Option<usize>,
) -> PyResult<Py<PyAny>> {
    // Clone the data we need to move into the background task
    let sample_ids_clone = sample_ids.clone();
    let batch_size = batch_size.unwrap_or(128);
    let prefetch_factor = prefetch_factor.unwrap_or(64);
    let num_threads = std::env::var("BULKI_PREFETCH_THREAD_NUM")
        .and_then(|s| s.parse::<usize>().map_err(|_| VarError::NotPresent))
        .unwrap_or(8);
    let chunk_size = batch_size * prefetch_factor / num_threads;
    // slice the array into bigger batches
    sample_ids_clone
        .iter()
        .enumerate()
        .map(|(i, &x)| (i, x))
        .collect::<Vec<(usize, usize)>>()
        .chunks(chunk_size)
        .enumerate()
        .map(|(i, batch)| (i, batch.to_vec()))
        .collect::<Vec<(usize, Vec<(usize, usize)>)>>()
        .iter()
        .for_each(|x| {
            GLOBAL_INDEX_QUEUE.push(x.clone());
        });
    for thread_id in 0..num_threads {
        let label_clone = label.clone();
        let sample_var_keys_clone = sample_var_keys.clone();

        PREFETCH_THREAD_POOL.spawn(move || {
            let thread_id_clone = thread_id.clone();
            loop {
                let ordered_index_group = match GLOBAL_INDEX_QUEUE.pop() {
                    Some(batch) => batch,
                    None => {
                        // If queue is empty, sleep a bit and try again
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        continue;
                    }
                };
                let (group_idx, index_group) = ordered_index_group;
                // Calculate partition mappings (objectId, local_sample_id, global_sample_id)
                let grouped_requests = index_group
                    .iter()
                    .map(|(original_idx, global_sample_id)| {
                        let part_id = global_sample_id / part_size;
                        let local_smp_id = global_sample_id % part_size;
                        let obj_identifier = ObjectIdentifier::Name(format!(
                            "{}/part_{}",
                            label_clone.clone(),
                            part_id
                        ));
                        let lookup_id = obj_identifier.vnode_id() % get_server_count();
                        (lookup_id, GetSampleRequest {
                            original_idx: *original_idx,
                            sample_id: *global_sample_id,
                            obj_id: obj_identifier,
                            local_sample_id: local_smp_id,
                            sample_var_keys: sample_var_keys_clone.clone(),
                        })
                    }).fold(HashMap::new(), |mut acc, (lookup_id, request)| {
                        acc.entry(lookup_id).or_insert_with(Vec::new).push(request);
                        acc
                    });
                let results = grouped_requests
                    .par_iter()
                    .map(|(&lookup_id, requests)| {
                        match rpc_call::<Vec<GetSampleRequest>, HashMap<usize, GetSampleResponse>>(
                            lookup_id,
                            "datastore::load_batch_samples",
                            requests,
                        ) {
                            Ok(result) => result,
                            Err(e) => {
                                eprintln!(
                                    "Error in background prefetch task for server {}: {}",
                                    lookup_id, e
                                );
                                HashMap::new()
                            }
                        }
                    })
                    .reduce(
                        || HashMap::new(),
                        |mut acc, x| {
                            acc.extend(x);
                            acc
                        },
                    );
                // Wait until it's this batch's turn to be processed
                loop {
                    let current_index = NEXT_BATCH_INDEX.load(Ordering::SeqCst);
                    if current_index == group_idx {
                        // It's our turn to process
                        break;
                    }
                    // Sleep briefly to avoid busy waiting
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                let mut found_count = 0;
                Python::with_gil(|py| {
                    let mut ordered_results = Vec::with_capacity(index_group.len());
                    for (original_idx, global_sample_id) in index_group.iter() {
                        if let Some(response) = results.get(global_sample_id) {
                            if let Ok(pyobj) = converter::convert_sample_response_to_pydict(py, Some(response)) {
                                ordered_results.push((*original_idx, pyobj.into_any().into()));
                            }
                        }
                    }
                    // Sort by original_idx before pushing to the queue
                    ordered_results.sort_by_key(|(idx, _)| *idx);
                    found_count += ordered_results.len();
                    for (_, pyobj) in ordered_results {
                        GLOBAL_DATA_QUEUE.push(pyobj);
                    }
                });
                if LOG_EXEC_COUNTER.fetch_add(1, Ordering::Relaxed) % 100 == 0 {
                    info!(
                        "[R{}/S{}] Background thread {} completed batch {}: {}/{} samples loaded, memory: {:?} MB",
                        get_client_rank(),
                        get_client_count(),
                        thread_id_clone,
                        group_idx,
                        found_count,
                        index_group.len(),
                        SystemUtility::get_current_memory_usage_mb()
                    );
                }
                NEXT_BATCH_INDEX.fetch_add(1, Ordering::SeqCst);
                // Explicit memory management to prevent leaks
                Python::with_gil(|py| {
                    let code = CStr::from_bytes_with_nul(b"import gc; gc.collect()\0").unwrap();
                    py.run(code, None, None).ok();
                });
            }
        });
    }
    // Return the number of samples requested (not the actual queue length yet)
    sample_ids.len().into_py_any(py)
}

pub fn force_checkpointing_impl<'py>(py: Python<'py>) -> PyResult<Py<PyAny>> {
    let uuid = Uuid::now_v7().to_string();
    let args = ("client".to_string(), uuid.clone());
    let mut results = Vec::new();
    for i in 0..get_server_count() {
        let result =
            rpc_call::<(String, String), usize>(i, "datastore::force_checkpointing", &args)
                .map_err(|e| {
                    PyErr::new::<PyValueError, _>(format!("Failed to force checkpointing: {}", e))
                })?;
        results.push(result);
    }
    let rst_dict = PyDict::new(py);
    rst_dict.set_item("job_id", uuid)?;
    rst_dict.set_item("submitted", results.iter().sum::<usize>())?;
    rst_dict.into_py_any(py).into()
}

/// Get the progress of a checkpointing job
pub fn get_job_progress_impl<'py>(py: Python<'py>, job_id: String) -> PyResult<Py<PyAny>> {
    let final_result = PyDict::new(py);
    for i in 0..get_server_count() {
        let server_id = 0;
        let srv_response = rpc_call::<String, Option<JobProgress>>(
            server_id,
            "datastore::get_checkpointing_progress",
            &job_id,
        )
        .map_err(|e| PyErr::new::<PyValueError, _>(format!("Failed to get job progress: {}", e)))?;

        match srv_response {
            Some(mut job_progress) => {
                job_progress.sync(true);
                let progress_dict = PyDict::new(py);
                progress_dict.set_item("job_id", &job_id)?;
                progress_dict.set_item("status", job_progress.status_str())?;
                progress_dict.set_item("progress", job_progress.get_progress())?;
                final_result.set_item(format!("srv_{}", i), progress_dict)?;
            }
            None => {
                let progress_dict = PyDict::new(py);
                progress_dict.set_item("job_id", &job_id)?;
                progress_dict.set_item("status", "not_found")?;
                progress_dict.set_item("progress", 0.0)?;
                progress_dict.set_item("message", "Job not found")?;
                final_result.set_item(format!("srv_{}", i), progress_dict)?;
            }
        };
    }
    Ok(final_result.into_any().into())
}

pub fn is_job_completed_impl<'py>(py: Python<'py>, job_id: String) -> PyResult<Py<PyAny>> {
    let server_id_list = (0..get_server_count()).collect::<Vec<u32>>();
    let completed_count: u32 = server_id_list
        .par_iter()
        .map(|server_id| {
            let srv_response = rpc_call::<String, Option<JobProgress>>(
                server_id.clone(),
                "datastore::get_checkpointing_progress",
                &job_id,
            )
            .map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Failed to get job progress: {}", e))
            })
            .unwrap_or(None);
            match srv_response {
                Some(mut job_progress) => {
                    job_progress.sync(true);
                    if job_progress.status_str() == "completed" {
                        1
                    } else {
                        0
                    }
                }
                None => 0,
            }
        })
        .sum();
    (completed_count == get_server_count()).into_py_any(py)
}

pub fn times_two_impl<'py>(py: Python<'py>, x: SupportedNumpyArray<'py>) -> PyResult<PyObject> {
    let input = x.into_array_type();

    let srv_id = REQUEST_COUNTER.load(Ordering::SeqCst) % get_server_count();

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
