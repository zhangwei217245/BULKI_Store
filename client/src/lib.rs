mod bk_ndarr;
mod cltctx;
mod datastore;
use crate::bk_ndarr::*;
use std::cell::RefCell;
use std::ops::Add;
use std::sync::Arc;

use cltctx::ClientContext;
use commons::region::SerializableNDArray;
use commons::rpc::RPCData;
use log::{debug, error, warn};
use ndarray::{Axis, SliceInfoElem};
use numpy::{
    datetime::{units, Timedelta},
    ndarray::{Array1, ArrayD, ArrayView1, ArrayViewD, ArrayViewMutD, Shape, Zip},
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

#[pymodule]
#[pyo3(name = "bkstore_client")]
fn rust_ext<'py>(m: &Bound<'py, PyModule>) -> PyResult<()> {
    // Module initialization - just register the init function
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

    #[pyfn(m)]
    #[pyo3(name = "times_two")]
    fn times_two<'py>(py: Python<'py>, x: PyReadonlyArrayDyn<'py, f64>) -> PyResult<PyObject> {
        // Convert numpy array to rust ndarray
        let x_array = x.as_array().to_owned();

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
        };

        // Deserialize back into rust ndarray
        let result: ArrayD<f64> = SerializableNDArray::deserialize(&response_data)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("Deserialization error: {}", e)))?;

        // Convert back to numpy array
        let py_array = PyArray::from_array(py, &result);
        Ok(py_array.into_any().unbind())
    }

    // example using generic T
    fn head<T: Copy + Add<Output = T>>(_py: Python<'_>, x: ArrayViewD<'_, T>) -> ArrayD<T> {
        // Ensure the array has at least one dimension
        assert!(x.ndim() > 0, "Input array must have at least one dimension");

        // Get the first element along the first dimension.
        let first = x.index_axis(Axis(0), 0).to_owned();

        // Convert to a dynamic array (ArrayD<T>) and return.
        first.into_dyn()
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
        x: ArrayViewD<'_, T>,
        y: ArrayViewD<'_, T>,
    ) -> ArrayD<T> {
        &x + &y
    }

    // This crate follows a strongly-typed approach to wrapping NumPy arrays
    // while Python API are often expected to work with multiple element types.
    //
    // That kind of limited polymorphis can be recovered by accepting an enumerated type
    // covering the supported element types and dispatching into a generic implementation.
    #[derive(FromPyObject)]
    enum SupportedArray<'py> {
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

    fn py_slice_to_ndarray_slice(py_slice: Bound<'_, PySlice>) -> PyResult<SliceInfoElem> {
        // Extract the start, stop, and step attributes.
        let start: Option<isize> = py_slice.getattr("start")?.extract()?;
        let stop: Option<isize> = py_slice.getattr("stop")?.extract()?;
        let step: Option<isize> = py_slice.getattr("step")?.extract()?;

        println!("start: {:?}, stop: {:?}, step: {:?}", start, stop, step);

        let start = start.unwrap_or(0);
        let step = step.unwrap_or(1);

        if step == 0 {
            return Err(PyValueError::new_err("slice step cannot be zero"));
        }

        Ok(SliceInfoElem::Slice {
            start,
            end: stop,
            step,
        })
    }

    #[pyfn(m)]
    #[pyo3(name = "translate_slice")]
    fn translate_slice<'py>(py: Python<'py>, py_slice: Bound<'_, PySlice>) -> PyResult<()> {
        let _ = py_slice_to_ndarray_slice(py_slice);
        Ok(())
    }
    // wrapper of `head`
    #[pyfn(m)]
    #[pyo3(name = "head")]
    fn head_py<'py>(py: Python<'py>, x: SupportedArray<'py>) -> PyResult<PyObject> {
        println!("head_py started");

        match x {
            SupportedArray::I8(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::I16(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::I32(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::I64(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::U8(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::U16(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::U32(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::U64(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::F32(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::F64(x) => Ok(head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
        }
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

    #[pyfn(m)]
    fn polymorphic_add<'py>(
        x: SupportedArray<'py>,
        y: SupportedArray<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Example rule: if either array is F64, convert both to F64.
        // Otherwise, they must be the same type.
        if x.is_f64() || y.is_f64() {
            // Convert both to F64 and then add.
            let x_f64 = x.cast_to_f64()?;
            let y_f64 = y.cast_to_f64()?;
            Ok(
                generic_add(x_f64.readonly().as_array(), y_f64.readonly().as_array())
                    .into_pyarray(x_f64.py())
                    .into_any(),
            )
        } else {
            // Otherwise, they should be the same type.
            match (x, y) {
                (SupportedArray::I64(x), SupportedArray::I64(y)) => Ok(generic_add(
                    x.readonly().as_array(),
                    y.readonly().as_array(),
                )
                .into_pyarray(x.py())
                .into_any()),
                // Add more cases for other same-type operations.
                _ => Err(PyValueError::new_err(
                    "Unsupported combination of array types",
                )),
            }
        }
    }

    Ok(())
}
