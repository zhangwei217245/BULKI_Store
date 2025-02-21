use anyhow::Result;
use ndarray::{Array, ArrayD, IxDyn, OwnedRepr};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Serialize, Deserialize)]
pub struct SerializableNDArray<T> {
    shape: Vec<usize>,
    data: Vec<T>,
}

impl<T> SerializableNDArray<T>
where
    T: Serialize + for<'d> Deserialize<'d> + Clone + Debug + Send + Sync,
{
    pub fn serialize(data: ndarray::ArrayBase<OwnedRepr<T>, IxDyn>) -> Result<Vec<u8>> {
        let serialized = rmp_serde::to_vec(&SerializableNDArray {
            shape: data.shape().to_vec(),
            data: data.iter().cloned().collect(),
        })?;
        Ok(serialized)
    }

    pub fn deserialize(data: &Vec<u8>) -> Result<ArrayD<T>> {
        let array: SerializableNDArray<T> = rmp_serde::from_slice(data)?;
        Ok(Array::from_shape_vec(IxDyn(&array.shape), array.data)?)
    }
}

/// A serializable representation of ndarray::Slice
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableSlice {
    pub start: Option<isize>,
    pub end: Option<isize>,
    pub step: isize,
}

impl From<ndarray::Slice> for SerializableSlice {
    fn from(slice: ndarray::Slice) -> Self {
        SerializableSlice {
            start: Some(slice.start),
            end: slice.end,
            step: slice.step,
        }
    }
}

impl From<SerializableSlice> for ndarray::Slice {
    fn from(slice: SerializableSlice) -> Self {
        match (slice.start, slice.end) {
            (Some(start), Some(end)) => ndarray::Slice::new(start, Some(end), slice.step),
            (Some(start), None) => ndarray::Slice::new(start, None, slice.step),
            (None, Some(end)) => ndarray::Slice::new(0, Some(end), slice.step),
            (None, None) => ndarray::Slice::new(0, None, slice.step),
        }
    }
}
