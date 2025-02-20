use commons::object::types::{MetadataValue, SupportedRustArrayD};
use numpy::{PyArrayDyn, PyArrayMethods};
use pyo3::{
    types::{PyAnyMethods, PyDict, PyDictMethods},
    Bound, FromPyObject, PyAny, PyErr, PyResult, Python,
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
    pub fn is_f64(&self) -> bool {
        matches!(self, SupportedNumpyArray::F64(_))
    }

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
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::Int8(arr)
            }
            SupportedNumpyArray::I16(bound) => {
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::Int16(arr)
            }
            SupportedNumpyArray::I32(bound) => {
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::Int32(arr)
            }
            SupportedNumpyArray::I64(bound) => {
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::Int64(arr)
            }
            SupportedNumpyArray::U8(bound) => {
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::UInt8(arr)
            }
            SupportedNumpyArray::U16(bound) => {
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::UInt16(arr)
            }
            SupportedNumpyArray::U32(bound) => {
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::UInt32(arr)
            }
            SupportedNumpyArray::U64(bound) => {
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::UInt64(arr)
            }
            SupportedNumpyArray::F32(bound) => {
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::Float32(arr)
            }
            SupportedNumpyArray::F64(bound) => {
                let arr = bound.readonly().as_array().to_owned();
                SupportedRustArrayD::Float64(arr)
            }
        }
    }
}

pub fn convert_pyany_to_metadata_value<'py>(
    _py: Python<'py>,
    value: Bound<'py, PyAny>,
) -> PyResult<MetadataValue> {
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
            let mut res = Vec::new();
            for item in bound {
                match item {
                    None => res.push(None),
                    Some(dict) => {
                        let map = RefCell::new(HashMap::new());
                        dict.locked_for_each(|key, value| {
                            let mut map_ref = map.borrow_mut();
                            map_ref.insert(
                                key.to_string(),
                                convert_pyany_to_metadata_value(dict.py(), value)?,
                            );
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
