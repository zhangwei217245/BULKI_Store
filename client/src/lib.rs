mod bk_ndarr;
mod cltctx;
mod datastore;
mod pyctx;

use env_logger;
use numpy::{
    datetime::{units, Timedelta},
    ndarray::Zip,
    Complex64, IntoPyArray, PyArray1, PyArrayDescr, PyArrayDyn, PyArrayMethods, PyReadonlyArray1,
    PyReadonlyArrayDyn, PyReadwriteArray1, PyReadwriteArrayDyn,
};
use pyctx::converter::SupportedNumpyArray;
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

    // #[pymethods]
    // impl BKCObject {
    //     #[new]
    //     fn new(
    //         py: Python,
    //         name: String,
    //         parent_id: Option<u128>,
    //         metadata: Option<Py<PyDict>>,
    //         array_data: Option<SupportedNumpyArray>,
    //     ) -> PyResult<Self> {
    //         let obj_ids = create_objects(
    //             py,
    //             name.clone(),
    //             parent_id,
    //             Some(vec![Some(metadata.clone())]),
    //             Some(vec![Some(array_data.clone())]),
    //         )?;

    //         Ok(BKCObject {
    //             id: obj_ids[0].extract(py)?,
    //             name,
    //             metadata: metadata.map(|m| m.into()),
    //             array_data: array_data.map(|a| a.into()),
    //             parent_id,
    //             children: obj_ids[1..]
    //                 .iter()
    //                 .map(|id| id.extract(py).unwrap())
    //                 .collect(),
    //         })
    //     }

    //     fn __repr__(&self) -> String {
    //         format!("BKCObject(id={}, name={})", self.id, self.name)
    //     }
    // }

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
    #[pyo3(signature = (name, parent_id=None, metadata=None, array_meta_list=None, array_data_list=None, sub_object_key=None))]
    fn create_objects<'py>(
        py: Python<'py>,
        name: String,
        parent_id: Option<u128>,
        metadata: Option<Bound<'py, PyDict>>,
        array_meta_list: Option<Vec<Option<Bound<'py, PyDict>>>>,
        array_data_list: Option<Vec<Option<SupportedNumpyArray<'py>>>>,
        sub_object_key: Option<String>,
    ) -> PyResult<Vec<Py<PyInt>>> {
        pyctx::create_object_impl(
            py,
            name,
            parent_id,
            metadata,
            array_meta_list,
            array_data_list,
            sub_object_key,
        )
    }

    /// load an object and possibly its subobjects
    /// by default, we do not load subobjects
    /// but if load_subobjects is set to True, we load subobjects that are filtered by
    /// subobj_meta_filter and subobj_array_data_filter
    ///
    /// Params:
    /// *required_metadata* is a list of metadata keys, and the loaded major object will
    /// contain only the subset of metadata specified by metadata_filter
    /// *required_slice* is a list of PySlices, and the loaded major object will
    /// contain only the subset of array data specified by array_data_filter
    /// *load_subobjects* determines whether to load subobjects or not
    /// *subobj_meta_filter and subobj_array_data_filter are used to filter the subobjects
    /// if load_subobjects is set to True
    // #[pyfn(m)]
    // #[pyo3(name = "load_major_object")]
    // #[pyo3(signature = (major_obj_id, required_metadata=None, required_slice=None, subobj_meta_filter=None, subobj_array_data_filter=None))]
    // fn load_objects<'py>(
    //     py: Python<'py>,
    //     major_obj_id: u128,
    //     metadata_filter: Option<Vec<Option<Bound<'py, PyDict>>>>,
    //     array_data_filter: Option<Vec<Option<SupportedNumpyArray<'py>>>>,
    // ) -> PyResult<Vec<PyObject>> {
    //     pyctx::load_objects_impl(py, obj_ids, metadata_filter, array_data_filter)
    // }

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
