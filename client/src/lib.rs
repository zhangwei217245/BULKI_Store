mod bk_ndarr;
mod cltctx;
mod datastore;
mod pyctx;

use env_logger;
use numpy::{
    datetime::{units, Timedelta},
    ndarray::Zip,
    Complex64, IntoPyArray, PyArray1, PyArrayDyn, PyArrayMethods, PyReadonlyArray1,
    PyReadonlyArrayDyn, PyReadwriteArray1, PyReadwriteArrayDyn,
};
use pyctx::converter::{MetaKeySpec, PyObjectIdentifier, SupportedNumpyArray};
use pyo3::{
    exceptions::PyValueError,
    pymodule,
    types::{PyAnyMethods, PyDict, PyDictMethods, PyInt, PyModule},
    Bound, Py, PyAny, PyObject, PyResult, Python,
};
use pyo3::{pyclass, types::PySlice};

#[pymodule]
#[pyo3(name = "bkstore_client")]
fn rust_ext<'py>(m: &Bound<'py, PyModule>) -> PyResult<()> {
    env_logger::init();

    #[pyclass]
    struct BKCObject {
        #[pyo3(get)]
        id: u128,
        #[pyo3(get)]
        name: String,
        #[pyo3(get)]
        metadata: Option<Py<PyDict>>,
        #[pyo3(get)]
        array_data: Option<Py<PyAny>>,
        #[pyo3(get)]
        parent_id: Option<u128>,
        #[pyo3(get)]
        children: Vec<u128>,
    }

    // Module initialization - just register the init function
    #[pyfn(m)]
    #[pyo3(name = "init")]
    fn init_py(py: Python<'_>) -> PyResult<()> {
        pyctx::init_py(py)
    }

    /// Creates one or more objects with the given parameters.
    ///
    /// # Arguments
    ///
    /// * `name` - A name for the new object.
    /// * `parent_id` - An optional identifier for the parent object.
    /// * `metadata` - Optional metadata as a list of dictionaries.
    /// * `array_data` - Optional list of supported NumPy arrays.
    ///
    /// # Returns
    ///
    /// A vector of Python objects representing the created objects.
    ///
    /// # Examples
    ///
    /// ```python
    /// import bkstore_client as bkc
    /// objs = bkc.create_objects("example", parent_id=42)
    /// ```
    #[pyfn(m)]
    #[pyo3(name = "create_objects")]
    #[pyo3(signature = (obj_name_key, parent_id=None, metadata=None, data=None, array_meta_list=None, array_data_list=None))]
    fn create_objects<'py>(
        py: Python<'py>,
        obj_name_key: String,
        parent_id: Option<u128>,
        metadata: Option<Bound<'py, PyDict>>,
        data: Option<SupportedNumpyArray<'py>>,
        array_meta_list: Option<Vec<Option<Bound<'py, PyDict>>>>,
        array_data_list: Option<Vec<Option<SupportedNumpyArray<'py>>>>,
    ) -> PyResult<Vec<Py<PyInt>>> {
        pyctx::create_object_impl(
            py,
            obj_name_key,
            parent_id,
            metadata,
            data,
            array_meta_list,
            array_data_list,
        )
    }

    /// Gets the metadata of an object with the given identifier.
    /// The identifier can be either an object ID of u128 or a name of str.
    #[pyfn(m)]
    #[pyo3(name = "get_object_metadata")]
    #[pyo3(signature = (obj_id, meta_keys=None, sub_meta_keys=None))]
    fn get_object_metadata<'py>(
        py: Python<'py>,
        obj_id: PyObjectIdentifier,
        meta_keys: Option<Vec<String>>,
        sub_meta_keys: Option<MetaKeySpec>,
    ) -> PyResult<Py<PyDict>> {
        pyctx::get_object_metadata_impl(py, obj_id.into(), meta_keys, sub_meta_keys)
    }

    /// Gets the data of an object with the given identifier.
    /// The identifier can be either an object ID of u128 or a name of str.
    #[pyfn(m)]
    #[pyo3(name = "get_object_data")]
    #[pyo3(signature = (obj_id, region=None, sub_obj_regions=None))]
    fn get_object_data<'py>(
        py: Python<'py>,
        obj_id: PyObjectIdentifier,
        region: Option<Vec<Bound<'py, PySlice>>>,
        sub_obj_regions: Option<Vec<(String, Vec<Bound<'py, PySlice>>)>>,
    ) -> PyResult<Py<PyDict>> {
        pyctx::get_object_data_impl(py, obj_id.into(), region, sub_obj_regions)
    }

    ///////////////////////////////////////////////////////////////////////////
    //     Ok(())
    // }
    //
    // #[pymodule]
    // #[pyo3(name = "bkstore_client_demo")]
    // fn rust_demo_ext<'py>(m: &Bound<'py, PyModule>) -> PyResult<()> {
    //     env_logger::init();
    //     // Module initialization - just register the init function
    //     #[pyfn(m)]
    //     #[pyo3(name = "init")]
    //     fn init_py(py: Python<'_>) -> PyResult<()> {
    //         pyctx::init_py(py)
    //     }
    ///////////////////////////////////////////////////////////////////////////

    #[pyfn(m)]
    #[pyo3(name = "times_two")]
    fn times_two<'py>(py: Python<'py>, x: SupportedNumpyArray<'py>) -> PyResult<PyObject> {
        // Get the name of the dtype (for example, "float64", "int64", etc.)
        pyctx::times_two_impl(py, x)
    }

    #[pyfn(m)]
    #[pyo3(name = "array_slicing")]
    fn array_slicing_py<'py>(
        x: SupportedNumpyArray<'py>,
        indices: Vec<Bound<'py, PySlice>>,
    ) -> PyResult<PyObject> {
        // Convert Python objects to IndexType
        match x {
            SupportedNumpyArray::I8(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
            SupportedNumpyArray::I16(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
            SupportedNumpyArray::I32(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
            SupportedNumpyArray::I64(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
            SupportedNumpyArray::U8(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
            SupportedNumpyArray::U16(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
            SupportedNumpyArray::U32(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
            SupportedNumpyArray::U64(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
            SupportedNumpyArray::F32(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
            SupportedNumpyArray::F64(x) => {
                Ok(pyctx::array_slicing(x.readonly().as_array(), indices)?
                    .into_pyarray(x.py())
                    .into_any()
                    .into())
            }
        }
    }

    // wrapper of `head`
    #[pyfn(m)]
    #[pyo3(name = "head")]
    fn head_py<'py>(py: Python<'py>, x: SupportedNumpyArray<'py>) -> PyResult<PyObject> {
        println!("head_py started");

        match x {
            SupportedNumpyArray::I8(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedNumpyArray::I16(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedNumpyArray::I32(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedNumpyArray::I64(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedNumpyArray::U8(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedNumpyArray::U16(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedNumpyArray::U32(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedNumpyArray::U64(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedNumpyArray::F32(x) => Ok(pyctx::head(py, x.readonly().as_array())
                .into_pyarray(py)
                .into_any()
                .into()),
            SupportedNumpyArray::F64(x) => Ok(pyctx::head(py, x.readonly().as_array())
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
        x: SupportedNumpyArray<'py>,
        y: SupportedNumpyArray<'py>,
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
                (SupportedNumpyArray::I64(x), SupportedNumpyArray::I64(y)) => Ok(
                    pyctx::generic_add(x.readonly().as_array(), y.readonly().as_array())
                        .into_pyarray(x.py())
                        .into_any(),
                ),
                // Add more cases for other same-type operations.
                _ => Err(PyValueError::new_err(
                    "Unsupported combination of array types",
                )),
            }
        }
    }

    Ok(())
}
