mod bk_ndarr;
mod cltctx;
mod datastore;
mod pyctx;
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
use pyctx::SupportedArray;
use pyo3::{exceptions::PyIndexError, types::PySlice, Py};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    pymodule,
    types::{PyAnyMethods, PyDict, PyDictMethods, PyModule},
    Bound, FromPyObject, PyAny, PyObject, PyResult, Python,
};
use pyo3::{IntoPy, IntoPyObject, IntoPyObjectExt, PyErr};

#[pymodule]
#[pyo3(name = "bkstore_client")]
fn rust_ext<'py>(m: &Bound<'py, PyModule>) -> PyResult<()> {
    // Module initialization - just register the init function
    #[pyfn(m)]
    #[pyo3(name = "init")]
    fn init_py(py: Python<'_>) -> PyResult<()> {
        pyctx::init_py(py)
    }

    #[pyfn(m)]
    #[pyo3(name = "times_two")]
    fn times_two<'py>(
        py: Python<'py>,
        x: PyObject,
        dtype: Bound<'py, PyArrayDescr>,
    ) -> PyResult<PyObject> {
        // Get the name of the dtype (for example, "float64", "int64", etc.)
        let dtype_name: String = dtype.getattr("name")?.extract()?;

        // Dispatch based on the dtype name.
        match dtype_name.as_str() {
            "float64" => {
                let arr: PyReadonlyArrayDyn<f64> = x.extract(py)?;
                pyctx::times_two_impl(py, arr)
            }
            "float32" => {
                let arr: PyReadonlyArrayDyn<f32> = x.extract(py)?;
                pyctx::times_two_impl(py, arr)
            }
            "int64" => {
                let arr: PyReadonlyArrayDyn<i64> = x.extract(py)?;
                pyctx::times_two_impl(py, arr)
            }
            "int32" => {
                let arr: PyReadonlyArrayDyn<i32> = x.extract(py)?;
                pyctx::times_two_impl(py, arr)
            }
            // Add more cases as needed...
            _ => Err(PyValueError::new_err("Unsupported dtype")),
        }
    }

    #[pyfn(m)]
    #[pyo3(name = "array_slicing")]
    fn array_slicing_py<'py>(
        x: SupportedArray<'py>,
        indices: Vec<Bound<'py, PySlice>>,
    ) -> PyResult<PyObject> {
        // Convert Python objects to IndexType
        match x {
            SupportedArray::I8(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
            SupportedArray::I16(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
            SupportedArray::I32(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
            SupportedArray::I64(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
            SupportedArray::U8(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
            SupportedArray::U16(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
            SupportedArray::U32(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
            SupportedArray::U64(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
            SupportedArray::F32(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
            SupportedArray::F64(x) => Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                .into_pyarray(x.py())
                .into_any()
                .into()),
        }
    }

    // wrapper of `head`
    #[pyfn(m)]
    #[pyo3(name = "head")]
    fn head_py<'py>(py: Python<'py>, x: SupportedArray<'py>) -> PyResult<PyObject> {
        println!("head_py started");

        match x {
            SupportedArray::I8(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::I16(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::I32(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::I64(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::U8(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::U16(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::U32(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::U64(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::F32(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedArray::F64(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
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
        let z = pyctx::axpy(a, x, y);
        z.into_pyarray(py)
    }

    // wrapper of `mult`
    #[pyfn(m)]
    #[pyo3(name = "mult")]
    fn mult_py<'py>(a: f64, mut x: PyReadwriteArrayDyn<'py, f64>) {
        let x = x.as_array_mut();
        pyctx::mult(a, x);
    }

    // wrapper of `conj`
    #[pyfn(m)]
    #[pyo3(name = "conj")]
    fn conj_py<'py>(
        py: Python<'py>,
        x: PyReadonlyArrayDyn<'py, Complex64>,
    ) -> Bound<'py, PyArrayDyn<Complex64>> {
        pyctx::conj(x.as_array()).into_pyarray(py)
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
    #[pyo3(name = "polymorphic_add")]
    fn polymorphic_add_py<'py>(
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
                pyctx::generic_add(x_f64.readonly().as_array(), y_f64.readonly().as_array())
                    .into_pyarray(x_f64.py())
                    .into_any(),
            )
        } else {
            // Otherwise, they should be the same type.
            match (x, y) {
                (SupportedArray::I64(x), SupportedArray::I64(y)) => Ok(pyctx::generic_add(
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
