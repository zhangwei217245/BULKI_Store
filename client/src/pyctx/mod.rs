use std::{cell::RefCell, ops::Add, sync::Arc};

use log::debug;
use serde::{Deserialize, Serialize};

use crate::cltctx::ClientContext;
use commons::region::SerializableNDArray;
use commons::rpc::RPCData;
use numpy::{
    datetime::{units, Timedelta},
    ndarray::{
        Array1, ArrayD, ArrayView1, ArrayViewD, ArrayViewMutD, Axis, Shape, SliceInfoElem, Zip,
    },
    Complex64, Element, IntoPyArray, PyArray, PyArray1, PyArrayDescr, PyArrayDyn, PyArrayMethods,
    PyReadonlyArray1, PyReadonlyArrayDyn, PyReadwriteArray1, PyReadwriteArrayDyn,
};
use pyo3::{exceptions::PyIndexError, types::PySlice, Py};
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
    // request counter:

}

#[derive(FromPyObject)]
pub enum SupportedArray<'py> {
    I8(Bound<'py, PyArrayDyn<i8>>),
    I16(Bound<'py, PyArrayDyn<i16>>),
    I32(Bound<'py, PyArrayDyn<i32>>),
    I64(Bound<'py, PyArrayDyn<i64>>),
    U8(Bound<'py, PyArrayDyn<u8>>),
    U16(Bound<'py, PyArrayDyn<u16>>),
    U32(Bound<'py, PyArrayDyn<u32>>),
    U64(Bound<'py, PyArrayDyn<u64>>),
    F32(Bound<'py, PyArrayDyn<f32>>),
    F64(Bound<'py, PyArrayDyn<f64>>),
}

// Example helper implementations.
impl<'py> SupportedArray<'py> {
    pub fn is_f64(&self) -> bool {
        matches!(self, SupportedArray::F64(_))
    }

    /// Attempt to cast the array to F64.
    /// For arrays already of type F64, it returns self.
    /// For certain types (like I64), it performs a cast.
    pub fn cast_to_f64(self) -> PyResult<Bound<'py, PyArrayDyn<f64>>> {
        match self {
            SupportedArray::F64(arr) => Ok(arr),
            SupportedArray::F32(arr) => {
                // Assume Bound has a method `cast` that returns a new Bound of the target type.
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedArray::I64(arr) => {
                // Assume Bound has a method `cast` that returns a new Bound of the target type.
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            // You can add additional conversions here:
            SupportedArray::I32(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedArray::I16(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedArray::I8(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedArray::U64(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedArray::U32(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedArray::U16(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedArray::U8(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
        }
    }
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

pub fn times_two_impl<'py, T>(py: Python<'py>, x: PyReadonlyArrayDyn<'py, T>) -> PyResult<PyObject>
where
    T: Copy + serde::Serialize + for<'de> serde::Deserialize<'de> + Element + std::fmt::Debug,
{
    // Convert numpy array to a rust ndarray (owned copy)
    let x_array = x.as_array().to_owned();

    // Serialize the ndarray (using MessagePack, for example)
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

    // Get binary data from response (assuming response.data contains the binary payload)
    let response_data = response.data;

    // Deserialize back into a rust ndarray
    let result: ArrayD<T> = SerializableNDArray::deserialize(&response_data)
        .map_err(|e| PyErr::new::<PyValueError, _>(format!("Deserialization error: {}", e)))?;

    // Convert the resulting ndarray back to a NumPy array
    let py_array = PyArray::from_array(py, &result);
    Ok(py_array.into_py(py))
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
