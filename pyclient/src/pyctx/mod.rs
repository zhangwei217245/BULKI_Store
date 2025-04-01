pub mod converter;
mod proc;
use client::cltctx::{get_client_count, get_client_rank, get_server_count, ClientContext};

use commons::err::RPCResult;
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
use std::ops::Add;
use std::sync::atomic::{AtomicU32, Ordering};
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
        let result = rpc_call::<Vec<GetObjectMetaParams>, Vec<GetObjectMetaResponse>>(
            vnode_id % get_server_count(),
            "datastore::get_multiple_object_metadata",
            &obj_meta_params,
        )
        .map_err(|e| {
            PyErr::new::<PyValueError, _>(format!("Failed to get multiple object metadata: {}", e))
        })?;
        for response in result {
            let obj_id = response.obj_id;
            let obj_name = response.obj_name.clone();
            let result_dict = converter::convert_get_object_meta_response_to_pydict(py, response)?;
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
    info!(
        "[R{}/S{}] get_multiple_object_metadata complete, {:?} in {:?} ms, memory usage: {} MB",
        get_client_rank(),
        get_client_count(),
        obj_ids.len(),
        timer.elapsed().as_millis(),
        SystemUtility::get_current_memory_usage_mb()
    );
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

            info!(
                "[R{}/S{}] get_object_data: result: {:?}, {:?} in {:?}ms, memory: {:?} MB",
                get_client_rank(),
                get_client_count(),
                result.obj_id,
                result.obj_name,
                timer.elapsed().as_millis(),
                SystemUtility::get_current_memory_usage_mb()
            );
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
    info!(
        "[R{}/S{}] get_multiple_object_data complete, {:?} in {:?} ms, memory usage: {} MB",
        get_client_rank(),
        get_client_count(),
        obj_ids.len(),
        timer.elapsed().as_millis(),
        SystemUtility::get_current_memory_usage_mb()
    );
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
    info!(
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
    sample_ids_clone.iter().for_each(|&global_sample_id| {
        let part_id = global_sample_id / part_size;
        let local_smp_id = global_sample_id % part_size;
        let obj_identifier = ObjectIdentifier::Name(format!("{}/part_{}", label, part_id));
        let vnode_id = obj_identifier.vnode_id();
        let request = GetSampleRequest {
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
        .map(|(vnode_id, requests)| {
            match rpc_call::<Vec<GetSampleRequest>, HashMap<usize, GetSampleResponse>>(
                vnode_id % get_server_count(),
                "datastore::load_batch_samples",
                requests,
            ) {
                Ok(result) => result,
                Err(e) => {
                    eprintln!(
                        "Error in background prefetch task for vnode {}: {}",
                        vnode_id, e
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
            converter::convert_sample_response_to_pydict(py, Some(response))?,
        )?;
    }
    let elapsed_time = timer.elapsed().as_millis();
    info!(
        "[R{}/S{}] fetch_samples: {:?} in {:?} ms, memory: {:?} MB",
        get_client_rank(),
        get_client_count(),
        sample_ids.len(),
        elapsed_time,
        SystemUtility::get_current_memory_usage_mb()
    );
    Ok(dict.into())
}

pub fn prefetch_samples_into_queue_impl<'py>(
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
    sample_ids_clone.iter().for_each(|&global_sample_id| {
        let part_id = global_sample_id / part_size;
        let local_smp_id = global_sample_id % part_size;
        let obj_identifier = ObjectIdentifier::Name(format!("{}/part_{}", label_clone, part_id));
        let vnode_id = obj_identifier.vnode_id();
        let request = GetSampleRequest {
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

    let results = grouped_request
        .par_iter()
        .map(|(vnode_id, requests)| {
            match rpc_call::<Vec<GetSampleRequest>, HashMap<usize, GetSampleResponse>>(
                vnode_id % get_server_count(),
                "datastore::load_batch_samples",
                requests,
            ) {
                Ok(result) => result,
                Err(e) => {
                    eprintln!(
                        "Error in background prefetch task for vnode {}: {}",
                        vnode_id, e
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

    // Push results to the global queue
    let found_count = sample_ids_clone
        .iter()
        .map(|&global_sample_id| {
            if let Some(data) = results.get(&global_sample_id) {
                let pydict =
                    converter::convert_sample_response_to_pydict(py, Some(data.to_owned()))
                        .map(|d| d.into_py_any(py).unwrap())
                        .unwrap();
                GLOBAL_DATA_QUEUE.push(pydict);
                1
            } else {
                0
            }
        })
        .sum::<usize>();

    info!(
        "Background prefetch completed: {}/{} samples loaded, memory: {:?} MB",
        found_count,
        sample_ids_clone.len(),
        SystemUtility::get_current_memory_usage_mb()
    );
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
    results.iter().sum::<usize>().into_py_any(py)
}

/// Get the progress of a checkpointing job
pub fn get_job_progress_impl<'py>(py: Python<'py>, job_id: String) -> PyResult<Py<PyAny>> {
    let final_result = PyDict::new(py);
    for i in 0..get_server_count() {
        let server_id = 0;
        let srv_response = rpc_call::<String, Option<(String, f32, String)>>(
            server_id,
            "datastore::get_job_progress",
            &job_id,
        )
        .map_err(|e| PyErr::new::<PyValueError, _>(format!("Failed to get job progress: {}", e)))?;

        match srv_response {
            Some((status, progress, message)) => {
                let progress_dict = PyDict::new(py);
                progress_dict.set_item("job_id", &job_id)?;
                progress_dict.set_item("status", status)?;
                progress_dict.set_item("progress", progress)?;
                progress_dict.set_item("message", message)?;
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
            let srv_response = rpc_call::<String, Option<(String, f32, String)>>(
                server_id.clone(),
                "datastore::get_job_progress",
                &job_id,
            )
            .map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Failed to get job progress: {}", e))
            })
            .unwrap_or(None);
            match srv_response {
                Some((status, _progress, _message)) => {
                    if status == "completed" {
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
