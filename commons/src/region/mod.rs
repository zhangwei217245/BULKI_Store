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
