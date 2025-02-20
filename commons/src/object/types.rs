use ndarray::{ArrayD, IxDyn, Slice, SliceInfo, SliceInfoElem};
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
    pub fn slice(&self, region: &[Slice]) -> SupportedRustArrayD {
        // Convert slices to SliceInfoElem
        let slice_info_elems: Vec<SliceInfoElem> =
            region.iter().map(|s| SliceInfoElem::from(*s)).collect();

        // Create SliceInfo
        let info = SliceInfo::<_, IxDyn, IxDyn>::try_from(slice_info_elems)
            .expect("Invalid slice pattern");

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
pub trait IntoArrayType {
    fn into_array_type(self) -> SupportedRustArrayD;
}

impl<T> IntoArrayType for ArrayD<T>
where
    T: 'static,
    ArrayD<T>: Into<SupportedRustArrayD>,
{
    fn into_array_type(self) -> SupportedRustArrayD {
        self.into()
    }
}
