use super::objid::{GlobalObjectId, GlobalObjectIdExt};
use ndarray::{ArrayD, IxDyn, SliceInfo, SliceInfoElem};
use serde::{Deserialize, Serialize};

/// Represents various types of metadata values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataValue {
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    IntList(Vec<i64>),
    UIntList(Vec<u64>),
    FloatList(Vec<f64>),
    StringList(Vec<String>),
    /// For example, container-level "ranges": a list of (start, end) tuples.
    RangeList(Vec<(usize, usize)>),
}

impl MetadataValue {
    pub fn to_string(&self) -> String {
        match self {
            MetadataValue::String(s) => s.to_string(),
            MetadataValue::Int(i) => i.to_string(),
            MetadataValue::UInt(i) => i.to_string(),
            MetadataValue::Float(f) => f.to_string(),
            MetadataValue::StringList(s) => s.join(",").to_string(),
            MetadataValue::IntList(i) => i
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(","),
            MetadataValue::UIntList(i) => i
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(","),
            MetadataValue::FloatList(f) => f
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(","),
            MetadataValue::RangeList(r) => r
                .iter()
                .map(|(s, e)| format!("{}-{}", s, e))
                .collect::<Vec<_>>()
                .join(","),
        }
    }
}

/// Represents the different types of arrays that can be stored
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SupportedRustArrayD {
    // Floating point types
    Float32(ArrayD<f32>),
    Float64(ArrayD<f64>),
    // Signed integer types
    Int8(ArrayD<i8>),
    Int16(ArrayD<i16>),
    Int32(ArrayD<i32>),
    Int64(ArrayD<i64>),
    Int128(ArrayD<i128>),
    // Unsigned integer types
    UInt8(ArrayD<u8>),
    UInt16(ArrayD<u16>),
    UInt32(ArrayD<u32>),
    UInt64(ArrayD<u64>),
    UInt128(ArrayD<u128>),
}

impl SupportedRustArrayD {
    /// Get a slice of the array, returning the same type
    pub fn slice(&self, region: &[SliceInfoElem]) -> SupportedRustArrayD {
        // Create SliceInfo
        let info = SliceInfo::<_, IxDyn, IxDyn>::try_from(region).expect("Invalid slice pattern");

        match self {
            // Floating point types
            SupportedRustArrayD::Float32(arr) => {
                SupportedRustArrayD::Float32(arr.slice(&info).to_owned())
            }
            SupportedRustArrayD::Float64(arr) => {
                SupportedRustArrayD::Float64(arr.slice(&info).to_owned())
            }
            // Signed integer types
            SupportedRustArrayD::Int8(arr) => {
                SupportedRustArrayD::Int8(arr.slice(&info).to_owned())
            }
            SupportedRustArrayD::Int16(arr) => {
                SupportedRustArrayD::Int16(arr.slice(&info).to_owned())
            }
            SupportedRustArrayD::Int32(arr) => {
                SupportedRustArrayD::Int32(arr.slice(&info).to_owned())
            }
            SupportedRustArrayD::Int64(arr) => {
                SupportedRustArrayD::Int64(arr.slice(&info).to_owned())
            }
            SupportedRustArrayD::Int128(arr) => {
                SupportedRustArrayD::Int128(arr.slice(&info).to_owned())
            }
            // Unsigned integer types
            SupportedRustArrayD::UInt8(arr) => {
                SupportedRustArrayD::UInt8(arr.slice(&info).to_owned())
            }
            SupportedRustArrayD::UInt16(arr) => {
                SupportedRustArrayD::UInt16(arr.slice(&info).to_owned())
            }
            SupportedRustArrayD::UInt32(arr) => {
                SupportedRustArrayD::UInt32(arr.slice(&info).to_owned())
            }
            SupportedRustArrayD::UInt64(arr) => {
                SupportedRustArrayD::UInt64(arr.slice(&info).to_owned())
            }
            SupportedRustArrayD::UInt128(arr) => {
                SupportedRustArrayD::UInt128(arr.slice(&info).to_owned())
            }
        }
    }

    /// Get the type name as a string
    pub fn type_name(&self) -> &'static str {
        match self {
            // Floating point types
            SupportedRustArrayD::Float32(_) => "f32",
            SupportedRustArrayD::Float64(_) => "f64",
            // Signed integer types
            SupportedRustArrayD::Int8(_) => "i8",
            SupportedRustArrayD::Int16(_) => "i16",
            SupportedRustArrayD::Int32(_) => "i32",
            SupportedRustArrayD::Int64(_) => "i64",
            SupportedRustArrayD::Int128(_) => "i128",
            // Unsigned integer types
            SupportedRustArrayD::UInt8(_) => "u8",
            SupportedRustArrayD::UInt16(_) => "u16",
            SupportedRustArrayD::UInt32(_) => "u32",
            SupportedRustArrayD::UInt64(_) => "u64",
            SupportedRustArrayD::UInt128(_) => "u128",
        }
    }
}

// Floating point implementations
impl From<ArrayD<f32>> for SupportedRustArrayD {
    fn from(array: ArrayD<f32>) -> Self {
        SupportedRustArrayD::Float32(array)
    }
}

impl From<ArrayD<f64>> for SupportedRustArrayD {
    fn from(array: ArrayD<f64>) -> Self {
        SupportedRustArrayD::Float64(array)
    }
}

// Signed integer implementations
impl From<ArrayD<i8>> for SupportedRustArrayD {
    fn from(array: ArrayD<i8>) -> Self {
        SupportedRustArrayD::Int8(array)
    }
}

impl From<ArrayD<i16>> for SupportedRustArrayD {
    fn from(array: ArrayD<i16>) -> Self {
        SupportedRustArrayD::Int16(array)
    }
}

impl From<ArrayD<i32>> for SupportedRustArrayD {
    fn from(array: ArrayD<i32>) -> Self {
        SupportedRustArrayD::Int32(array)
    }
}

impl From<ArrayD<i64>> for SupportedRustArrayD {
    fn from(array: ArrayD<i64>) -> Self {
        SupportedRustArrayD::Int64(array)
    }
}

impl From<ArrayD<i128>> for SupportedRustArrayD {
    fn from(array: ArrayD<i128>) -> Self {
        SupportedRustArrayD::Int128(array)
    }
}

// Unsigned integer implementations
impl From<ArrayD<u8>> for SupportedRustArrayD {
    fn from(array: ArrayD<u8>) -> Self {
        SupportedRustArrayD::UInt8(array)
    }
}

impl From<ArrayD<u16>> for SupportedRustArrayD {
    fn from(array: ArrayD<u16>) -> Self {
        SupportedRustArrayD::UInt16(array)
    }
}

impl From<ArrayD<u32>> for SupportedRustArrayD {
    fn from(array: ArrayD<u32>) -> Self {
        SupportedRustArrayD::UInt32(array)
    }
}

impl From<ArrayD<u64>> for SupportedRustArrayD {
    fn from(array: ArrayD<u64>) -> Self {
        SupportedRustArrayD::UInt64(array)
    }
}

impl From<ArrayD<u128>> for SupportedRustArrayD {
    fn from(array: ArrayD<u128>) -> Self {
        SupportedRustArrayD::UInt128(array)
    }
}

/// Helper trait to check if a type can be converted to ArrayType
pub trait IntoRustArrayD {
    fn into_rust_array_d(self) -> SupportedRustArrayD;
}

impl<T> IntoRustArrayD for ArrayD<T>
where
    T: 'static,
    ArrayD<T>: Into<SupportedRustArrayD>,
{
    fn into_rust_array_d(self) -> SupportedRustArrayD {
        self.into()
    }
}

/// A serializable version of ndarray::SliceInfoElem
#[derive(Debug, Serialize, Deserialize)]
pub enum SerializableSliceInfoElem {
    Index(isize),
    Slice {
        start: isize,
        end: Option<isize>,
        step: isize,
    },
    NewAxis,
}

impl From<SliceInfoElem> for SerializableSliceInfoElem {
    fn from(elem: SliceInfoElem) -> Self {
        match elem {
            SliceInfoElem::Index(i) => SerializableSliceInfoElem::Index(i),
            SliceInfoElem::Slice { start, end, step } => {
                SerializableSliceInfoElem::Slice { start, end, step }
            }
            SliceInfoElem::NewAxis => SerializableSliceInfoElem::NewAxis,
        }
    }
}

impl From<SerializableSliceInfoElem> for SliceInfoElem {
    fn from(elem: SerializableSliceInfoElem) -> Self {
        match elem {
            SerializableSliceInfoElem::Index(i) => SliceInfoElem::Index(i),
            SerializableSliceInfoElem::Slice { start, end, step } => {
                SliceInfoElem::Slice { start, end, step }
            }
            SerializableSliceInfoElem::NewAxis => SliceInfoElem::NewAxis,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum ObjectIdentifier {
    U128(u128),
    Name(String),
}

impl ObjectIdentifier {
    pub fn vnode_id(&self) -> u32 {
        match self {
            ObjectIdentifier::U128(u) => u.vnode_id(),
            ObjectIdentifier::Name(name) => GlobalObjectId::get_name_hash(name.as_str()),
        }
    }
}
