pub mod converter;
use crate::cltctx::{get_server_count, ClientContext};
use anyhow::Result;
use commons::rpc::RPCData;
use commons::{object::objid::GlobalObjectIdExt, region::SerializableNDArray};
use converter::SupportedNumpyArray;
use log::debug;
use numpy::{
    ndarray::{ArrayD, ArrayViewD, ArrayViewMutD, Axis, SliceInfoElem},
    Complex64, Element, PyArray, PyReadonlyArrayDyn,
};
use pyo3::types::PyInt;
use pyo3::Py;
use pyo3::{
    exceptions::PyValueError,
    types::{PyAnyMethods, PyDict, PySlice},
    Bound, PyErr, PyObject, PyResult, Python,
};
use std::{cell::RefCell, ops::Add, sync::Arc};

thread_local! {
    static RUNTIME: RefCell<Option<tokio::runtime::Runtime>> = RefCell::new(None);
    static CONTEXT: RefCell<Option<ClientContext>> = RefCell::new(None);
    // request counter:
    static REQUEST_COUNTER: RefCell<u32> = RefCell::new(0);
}

pub fn init_py(py: Python<'_>) -> PyResult<()> {
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

fn rpc_call(srv_id: u32, method_name: &str, data: Option<Vec<u8>>) -> Result<Vec<u8>> {
    debug!(
        "[RPC_CALL][TX Rank {}] Sending Data of length {} to server {}",
        CONTEXT.with(|ctx| ctx.borrow().as_ref().unwrap().get_rank()),
        data.as_ref().map(|d| d.len()).unwrap_or(0),
        srv_id
    );
    let response = CONTEXT
        .with(|ctx| {
            let ctx = ctx.borrow();
            let ctx_ref = ctx.as_ref().expect("Context not initialized");
            RUNTIME.with(|rt_cell| {
                let rt = rt_cell.borrow();
                let rt_ref = rt.as_ref().expect("Runtime not initialized");
                rt_ref.block_on(ctx_ref.send_message(
                    srv_id as usize,
                    method_name,
                    RPCData {
                        metadata: None,
                        data,
                    },
                ))
            })
        })
        .map_err(|e| PyErr::new::<PyValueError, _>(format!("RPC error: {}", e)))?;
    debug!(
        "[RPC_CALL][TX Rank {}] Received response from server {}",
        CONTEXT.with(|ctx| ctx.borrow().as_ref().unwrap().get_rank()),
        srv_id
    );
    // Increment request counter
    REQUEST_COUNTER.with(|counter| {
        let mut counter = counter.borrow_mut();
        *counter += 1;
    });
    // Get binary data from response (assuming response.data contains the binary payload)
    Ok(response.data.unwrap())
}

pub fn create_object_impl<'py>(
    py: Python<'py>,
    obj_name_key: String,
    parent_id: Option<u128>,
    metadata: Option<Bound<'py, PyDict>>,
    array_meta_list: Option<Vec<Option<Bound<'py, PyDict>>>>,
    array_data_list: Option<Vec<Option<SupportedNumpyArray<'py>>>>,
) -> PyResult<Vec<Py<PyInt>>> {
    let create_obj_params = crate::datastore::create_objects_req_proc(
        obj_name_key,
        parent_id,
        metadata,
        array_meta_list,
        array_data_list,
    );
    match create_obj_params {
        None => Err(PyErr::new::<PyValueError, _>(
            "Failed to create object parameters",
        )),
        Some(params) => {
            let main_obj_id = params[0].obj_id;
            // Serialize the parameters using MessagePack
            let serialized_params = rmp_serde::to_vec(&params).map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Failed to serialize params: {}", e))
            })?;
            debug!(
                "create_objects: params data length: {:?}",
                serialized_params.len()
            );
            // Get binary data from response (assuming response.data contains the binary payload)
            let response_data = rpc_call(
                main_obj_id.vnode_id() % get_server_count(),
                "datastore::create_objects",
                Some(serialized_params),
            );
            debug!(
                "create_objects: response data length: {:?}",
                response_data.as_ref().unwrap().len()
            );
            let result: Vec<u128> =
                rmp_serde::from_slice(&response_data.unwrap()).map_err(|e| {
                    PyErr::new::<PyValueError, _>(format!("Deserialization error: {}", e))
                })?;
            debug!("create_objects: result vector length: {:?}", result.len());
            debug!("create_objects: result vector: {:?}", result);
            converter::convert_vec_u128_to_py_long(py, result)
        }
    }
}

pub fn times_two_impl<'py, T>(py: Python<'py>, x: PyReadonlyArrayDyn<'py, T>) -> PyResult<PyObject>
where
    T: Copy + serde::Serialize + for<'de> serde::Deserialize<'de> + Element + std::fmt::Debug,
{
    // Convert numpy array to a rust ndarray (owned copy)
    let x_array = x.as_array().to_owned();

    // Serialize the ndarray (using MessagePack, for example)
    let serialized = SerializableNDArray::serialize(x_array)
        .map_err(|e| PyErr::new::<PyValueError, _>(format!("Serialization error: {}", e)))?;

    let srv_id = REQUEST_COUNTER.with(|counter| (*counter.borrow() as u32) % get_server_count());

    let response_data = rpc_call(srv_id, "datastore::times_two", Some(serialized));

    match response_data {
        Ok(data) => {
            let result: ArrayD<T> = SerializableNDArray::deserialize(&data).map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Deserialization error: {}", e))
            })?;
            let py_array = PyArray::from_array(py, &result);
            Ok(py_array.into_any().into())
        }
        Err(e) => Err(PyErr::new::<PyValueError, _>(format!("RPC error: {}", e))),
    }
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

/// Convert a Python slice (wrapped in Bound) into an ndarray SliceInfoElem.
pub fn py_slice_to_ndarray_slice<'py>(slice: Bound<'py, PySlice>) -> PyResult<SliceInfoElem> {
    let py_slice = slice.as_ref();
    let start: Option<isize> = py_slice.getattr("start")?.extract()?;
    let stop: Option<isize> = py_slice.getattr("stop")?.extract()?;
    let step: Option<isize> = py_slice.getattr("step")?.extract()?;
    Ok(SliceInfoElem::Slice {
        start: start.unwrap_or(0),
        end: stop,
        step: step.unwrap_or(1),
    })
}

pub fn array_slicing<'py, T: Copy>(
    x: ArrayViewD<'_, T>,
    indices: Vec<Bound<'py, PySlice>>,
) -> PyResult<ArrayD<T>> {
    // Convert all Python slices into ndarray slice elements
    let mut slice_spec: Vec<SliceInfoElem> = Vec::with_capacity(x.ndim());

    // Convert provided slices
    for index in indices.iter() {
        slice_spec.push(py_slice_to_ndarray_slice(index.to_owned())?);
    }

    // If fewer slices provided than dimensions, fill rest with full slices
    while slice_spec.len() < x.ndim() {
        slice_spec.push(SliceInfoElem::Slice {
            start: 0,
            end: None,
            step: 1,
        });
    }

    // Apply slicing and return an owned copy
    Ok(x.slice(&slice_spec[..]).to_owned())
}
