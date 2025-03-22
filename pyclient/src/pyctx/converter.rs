use commons::object::{
    params::{GetObjectMetaResponse, GetObjectSliceResponse},
    types::{MetadataValue, ObjectIdentifier, SerializableSliceInfoElem, SupportedRustArrayD},
};

// use log::info;
use ndarray::SliceInfoElem;
use numpy::{IntoPyArray, PyArrayDyn, PyArrayMethods, ToPyArray};
use pyo3::{
    exceptions::PyValueError,
    types::{PyAnyMethods, PyDict, PyDictMethods, PyInt, PyList, PyListMethods, PySlice},
    Bound, FromPyObject, IntoPyObjectExt, Py, PyAny, PyErr, PyResult, Python,
};
use std::cell::RefCell;
use std::collections::HashMap;

#[derive(FromPyObject)]
pub enum SupportedNumpyArray<'py> {
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
impl<'py> SupportedNumpyArray<'py> {
    // pub fn is_f64(&self) -> bool {
    //     matches!(self, SupportedNumpyArray::F64(_))
    // }

    /// Attempt to cast the array to F64.
    /// For arrays already of type F64, it returns self.
    /// For certain types (like I64), it performs a cast.
    pub fn cast_to_f64(self) -> PyResult<Bound<'py, PyArrayDyn<f64>>> {
        match self {
            SupportedNumpyArray::F64(arr) => Ok(arr),
            SupportedNumpyArray::F32(arr) => {
                // Assume Bound has a method `cast` that returns a new Bound of the target type.
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedNumpyArray::I64(arr) => {
                // Assume Bound has a method `cast` that returns a new Bound of the target type.
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            // You can add additional conversions here:
            SupportedNumpyArray::I32(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedNumpyArray::I16(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedNumpyArray::I8(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedNumpyArray::U64(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedNumpyArray::U32(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedNumpyArray::U16(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
            SupportedNumpyArray::U8(arr) => {
                let casted = arr.cast::<f64>(false)?;
                Ok(casted)
            }
        }
    }

    pub fn into_array_type(self) -> SupportedRustArrayD {
        match self {
            SupportedNumpyArray::I8(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::Int8(arr)
            }
            SupportedNumpyArray::I16(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::Int16(arr)
            }
            SupportedNumpyArray::I32(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::Int32(arr)
            }
            SupportedNumpyArray::I64(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::Int64(arr)
            }
            SupportedNumpyArray::U8(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::UInt8(arr)
            }
            SupportedNumpyArray::U16(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::UInt16(arr)
            }
            SupportedNumpyArray::U32(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::UInt32(arr)
            }
            SupportedNumpyArray::U64(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::UInt64(arr)
            }
            SupportedNumpyArray::F32(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::Float32(arr)
            }
            SupportedNumpyArray::F64(bound) => {
                // Create a readview first to access the array data
                let readview = bound.readonly();
                // Get the array from the read view
                let arr = readview.as_array().to_owned();
                // Explicitly drop the bound reference to release Python memory earlier
                drop(bound);
                SupportedRustArrayD::Float64(arr)
            }
        }
    }
}

pub trait IntoBoundPyAny {
    fn into_bound_py_any<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}

impl IntoBoundPyAny for SupportedRustArrayD {
    fn into_bound_py_any<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            SupportedRustArrayD::Int8(a) => Ok(a.to_pyarray(py).into_any()),
            SupportedRustArrayD::Int16(a) => Ok(a.to_pyarray(py).into_any()),
            SupportedRustArrayD::Int32(a) => Ok(a.to_pyarray(py).into_any()),
            SupportedRustArrayD::Int64(a) => Ok(a.to_pyarray(py).into_any()),
            SupportedRustArrayD::UInt8(a) => Ok(a.to_pyarray(py).into_any()),
            SupportedRustArrayD::UInt16(a) => Ok(a.to_pyarray(py).into_any()),
            SupportedRustArrayD::UInt32(a) => Ok(a.to_pyarray(py).into_any()),
            SupportedRustArrayD::UInt64(a) => Ok(a.to_pyarray(py).into_any()),
            SupportedRustArrayD::Float32(a) => Ok(a.to_pyarray(py).into_any()),
            SupportedRustArrayD::Float64(a) => Ok(a.to_pyarray(py).into_any()),
            _ => Err(PyErr::new::<PyValueError, _>("Unsupported array type")),
        }
    }
}

pub fn convert_metadata_value_to_pyany<'py>(
    py: Python<'py>,
    value: MetadataValue,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        MetadataValue::Int(v) => v.into_bound_py_any(py),
        MetadataValue::UInt(v) => v.into_bound_py_any(py),
        MetadataValue::Float(v) => v.into_bound_py_any(py),
        MetadataValue::String(v) => v.into_bound_py_any(py),
        MetadataValue::IntList(v) => v.into_bound_py_any(py),
        MetadataValue::UIntList(v) => v.into_bound_py_any(py),
        MetadataValue::FloatList(v) => v.into_bound_py_any(py),
        MetadataValue::StringList(v) => v.into_bound_py_any(py),
        MetadataValue::RangeTuple(v) => v.into_bound_py_any(py),
        MetadataValue::RangeList(v) => v.into_bound_py_any(py),
    }
}

pub fn convert_pyany_to_metadata_value<'py>(value: Bound<'py, PyAny>) -> PyResult<MetadataValue> {
    match value.extract::<i64>() {
        Ok(v) => return Ok(MetadataValue::Int(v)),
        Err(_) => {}
    }
    match value.extract::<u64>() {
        Ok(v) => return Ok(MetadataValue::UInt(v)),
        Err(_) => {}
    }
    match value.extract::<f64>() {
        Ok(v) => return Ok(MetadataValue::Float(v)),
        Err(_) => {}
    }
    match value.extract::<String>() {
        Ok(v) => return Ok(MetadataValue::String(v)),
        Err(_) => {}
    }
    match value.extract::<Vec<i64>>() {
        Ok(v) => return Ok(MetadataValue::IntList(v)),
        Err(_) => {}
    }
    match value.extract::<Vec<u64>>() {
        Ok(v) => return Ok(MetadataValue::UIntList(v)),
        Err(_) => {}
    }
    match value.extract::<Vec<f64>>() {
        Ok(v) => return Ok(MetadataValue::FloatList(v)),
        Err(_) => {}
    }
    match value.extract::<Vec<String>>() {
        Ok(v) => return Ok(MetadataValue::StringList(v)),
        Err(_) => {}
    }
    match value.extract::<Vec<(usize, usize)>>() {
        Ok(v) => return Ok(MetadataValue::RangeList(v)),
        Err(_) => {}
    }
    match value.extract::<(usize, usize)>() {
        Ok(v) => return Ok(MetadataValue::RangeTuple(v)),
        Err(_) => {}
    }
    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
        "Unsupported metadata value type",
    ))
}

/// Convert an optional PyDict wrapped in a Bound into an Option<HashMap<String, MetadataValue>>
pub fn convert_metadata<'py>(
    metadata: Option<Vec<Option<Bound<'py, PyDict>>>>,
) -> PyResult<Option<Vec<Option<HashMap<String, MetadataValue>>>>> {
    match metadata {
        None => Ok(None),
        Some(bound) => {
            let mut res = Vec::with_capacity(bound.len());
            for item in bound {
                match item {
                    None => res.push(None),
                    Some(dict) => {
                        let map = RefCell::new(HashMap::new());
                        dict.locked_for_each(|key, value| {
                            let mut map_ref = map.borrow_mut();
                            map_ref
                                .insert(key.to_string(), convert_pyany_to_metadata_value(value)?);
                            Ok(())
                        })?;
                        res.push(Some(map.into_inner()));
                    }
                }
            }
            Ok(Some(res))
        }
    }
}

pub fn convert_vec_u128_to_py_long(py: Python, vec: Vec<u128>) -> PyResult<Vec<Py<PyInt>>> {
    vec.into_iter()
        .map(|num| -> PyResult<_> {
            let obj = num.into_py_any(py).map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Failed to convert {} to PyAny: {}", num, e))
            })?;
            obj.downcast_bound::<PyInt>(py)
                .map_err(|e| {
                    PyErr::new::<PyValueError, _>(format!(
                        "Failed to downcast {} to PyInt: {}",
                        num, e
                    ))
                })
                .map(|pyint| pyint.as_unbound().clone_ref(py))
        })
        .collect()
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

pub fn py_slice_to_ndarray_serde_slice<'py>(
    slice: Bound<'py, PySlice>,
) -> PyResult<SerializableSliceInfoElem> {
    let py_slice = slice.as_ref();
    let start: Option<isize> = py_slice.getattr("start")?.extract()?;
    let stop: Option<isize> = py_slice.getattr("stop")?.extract()?;
    let step: Option<isize> = py_slice.getattr("step")?.extract()?;
    Ok(SerializableSliceInfoElem::Slice {
        start: start.unwrap_or(0),
        end: stop,
        step: step.unwrap_or(1),
    })
}

pub fn convert_pyslice_vec_to_rust_serde_slice_vec<'py>(
    ndim: usize,
    indices: Option<Vec<Bound<'py, PySlice>>>,
) -> PyResult<Vec<SerializableSliceInfoElem>> {
    let mut slice_spec: Vec<SerializableSliceInfoElem> = Vec::with_capacity(ndim);

    // Convert provided slices
    if let Some(indices) = indices {
        for index in indices.iter() {
            slice_spec.push(py_slice_to_ndarray_serde_slice(index.to_owned())?);
        }
    }

    // If fewer slices provided than dimensions, fill rest with full slices
    while slice_spec.len() < ndim {
        slice_spec.push(SerializableSliceInfoElem::Slice {
            start: 0,
            end: None,
            step: 1,
        });
    }

    Ok(slice_spec)
}

pub fn convert_pyslice_vec_to_rust_slice_vec<'py>(
    ndim: usize,
    indices: Option<Vec<Bound<'py, PySlice>>>,
) -> PyResult<Vec<SliceInfoElem>> {
    let mut slice_spec: Vec<SliceInfoElem> = Vec::with_capacity(ndim);

    // Convert provided slices
    if let Some(indices) = indices {
        for index in indices.iter() {
            slice_spec.push(py_slice_to_ndarray_slice(index.to_owned())?);
        }
    }

    // If fewer slices provided than dimensions, fill rest with full slices
    while slice_spec.len() < ndim {
        slice_spec.push(SliceInfoElem::Slice {
            start: 0,
            end: None,
            step: 1,
        });
    }

    Ok(slice_spec)
}

pub fn convert_get_object_slice_response_to_pydict<'py>(
    py: Python<'py>,
    response: GetObjectSliceResponse,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item(
        "array_slice",
        response
            .array_slice
            .map(|x| match x {
                SupportedRustArrayD::Int8(a) => Ok(a.into_pyarray(py).into_any()),
                SupportedRustArrayD::Int16(a) => Ok(a.into_pyarray(py).into_any()),
                SupportedRustArrayD::Int32(a) => Ok(a.into_pyarray(py).into_any()),
                SupportedRustArrayD::Int64(a) => Ok(a.into_pyarray(py).into_any()),
                SupportedRustArrayD::UInt8(a) => Ok(a.into_pyarray(py).into_any()),
                SupportedRustArrayD::UInt16(a) => Ok(a.into_pyarray(py).into_any()),
                SupportedRustArrayD::UInt32(a) => Ok(a.into_pyarray(py).into_any()),
                SupportedRustArrayD::UInt64(a) => Ok(a.into_pyarray(py).into_any()),
                SupportedRustArrayD::Float32(a) => Ok(a.into_pyarray(py).into_any()),
                SupportedRustArrayD::Float64(a) => Ok(a.into_pyarray(py).into_any()),
                _ => Err(PyErr::new::<PyValueError, _>("Unsupported array type")),
            })
            .transpose()?,
    )?;

    // Convert sub-object slices
    let sub_slices = match response.sub_obj_slices {
        Some(slices) => {
            let mut temp_array = Vec::with_capacity(slices.len());
            for (id, name, array) in slices {
                let sub_dict = PyDict::new(py);
                sub_dict.set_item("id", id)?;
                sub_dict.set_item("name", name)?;

                // Convert array to Python object if present
                let py_array = array
                    .map(|x| match x {
                        SupportedRustArrayD::Int8(a) => Ok(a.into_pyarray(py).into_any()),
                        SupportedRustArrayD::Int16(a) => Ok(a.into_pyarray(py).into_any()),
                        SupportedRustArrayD::Int32(a) => Ok(a.into_pyarray(py).into_any()),
                        SupportedRustArrayD::Int64(a) => Ok(a.into_pyarray(py).into_any()),
                        SupportedRustArrayD::UInt8(a) => Ok(a.into_pyarray(py).into_any()),
                        SupportedRustArrayD::UInt16(a) => Ok(a.into_pyarray(py).into_any()),
                        SupportedRustArrayD::UInt32(a) => Ok(a.into_pyarray(py).into_any()),
                        SupportedRustArrayD::UInt64(a) => Ok(a.into_pyarray(py).into_any()),
                        SupportedRustArrayD::Float32(a) => Ok(a.into_pyarray(py).into_any()),
                        SupportedRustArrayD::Float64(a) => Ok(a.into_pyarray(py).into_any()),
                        _ => Err(PyErr::new::<PyValueError, _>("Unsupported array type")),
                    })
                    .transpose()?;

                sub_dict.set_item("array", py_array)?;
                temp_array.push(sub_dict);
            }
            Some(PyList::new(py, temp_array)?)
        }
        None => None,
    };
    dict.set_item("sub_obj_slices", sub_slices)?;
    Ok(dict)
}

pub fn convert_get_object_meta_response_to_pydict<'py>(
    py: Python<'py>,
    response: GetObjectMetaResponse,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);

    // Convert obj_id to Python int
    let obj_id = response.obj_id;
    dict.set_item("obj_id", obj_id)?;

    // Convert metadata if present
    if let Some(metadata) = response.metadata {
        let meta_dict = PyDict::new(py);
        for (key, value) in metadata {
            meta_dict.set_item(key, convert_metadata_value_to_pyany(py, value)?)?;
        }
        dict.set_item("metadata", meta_dict)?;
    } else {
        dict.set_item("metadata", py.None())?;
    }
    // Convert sub_obj_metadata if present
    if let Some(sub_metadata) = response.sub_obj_metadata {
        let sub_meta_list = PyList::empty(py);
        for (sub_obj_id, sub_obj_name, sub_meta) in sub_metadata {
            let sub_dict = PyDict::new(py);
            sub_dict.set_item("obj_id", sub_obj_id)?;
            sub_dict.set_item("name", sub_obj_name)?;

            let meta_dict = PyDict::new(py);
            for (key, value) in sub_meta {
                meta_dict.set_item(key, convert_metadata_value_to_pyany(py, value)?)?;
            }
            sub_dict.set_item("metadata", meta_dict)?;
            sub_meta_list.append(sub_dict)?;
        }
        dict.set_item("sub_obj_metadata", sub_meta_list)?;
    } else {
        dict.set_item("sub_obj_metadata", py.None())?;
    }
    Ok(dict)
}

#[derive(Debug, Clone)]
pub enum MetaKeySpec {
    Simple(Vec<String>),
    WithObject(HashMap<String, Vec<String>>),
}

impl From<Vec<String>> for MetaKeySpec {
    fn from(keys: Vec<String>) -> Self {
        MetaKeySpec::Simple(keys)
    }
}

impl<'py> FromPyObject<'py> for MetaKeySpec {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(keys) = <Vec<String>>::extract_bound(ob) {
            return Ok(MetaKeySpec::Simple(keys));
        }

        if let Ok(dict) = ob.downcast::<PyDict>() {
            let mut meta_map = HashMap::new();
            for (key, value) in dict.iter() {
                let obj_name = key.extract::<String>()?;
                let meta_keys = value.extract::<Vec<String>>()?;
                meta_map.insert(obj_name, meta_keys);
            }
            return Ok(MetaKeySpec::WithObject(meta_map));
        }

        Err(PyValueError::new_err(
            "Expected either a list of strings or a tuple (str, list[str])",
        ))
    }
}

#[derive(Debug, Clone)]
pub enum PyObjectIdentifier {
    U128(u128),
    Name(String),
}

impl<'py> FromPyObject<'py> for PyObjectIdentifier {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        // Try to extract as u128 first
        if let Ok(id) = ob.extract::<u128>() {
            return Ok(PyObjectIdentifier::U128(id));
        }

        if let Ok(s) = ob.extract::<String>() {
            return Ok(PyObjectIdentifier::Name(s));
        }

        Err(PyValueError::new_err(
            "Expected either a u128 number or a str",
        ))
    }
}

impl From<PyObjectIdentifier> for ObjectIdentifier {
    fn from(value: PyObjectIdentifier) -> Self {
        match value {
            PyObjectIdentifier::U128(u) => ObjectIdentifier::U128(u),
            PyObjectIdentifier::Name(s) => ObjectIdentifier::Name(s),
        }
    }
}
