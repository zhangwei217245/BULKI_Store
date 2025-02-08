// use numpy::ndarray::{ArrayView, Dimension};
// use numpy::{Element, IntoPyArray, PyArray, PyArrayMethods, PyReadonlyArray};
// use pyo3::{PyResult, Python};

use anyhow::Result;

use commons::handler::HandlerResult;
use commons::rpc::{RPCData, StatusCode};
use log::{debug, error};

pub fn process_response(data: &mut RPCData) -> HandlerResult {
    debug!("Processing response: {:?}", data);
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

// /// Convert a Python numpy array to a Rust ndarray view
// pub fn to_rust_view<'py, T, D>(array: &'py PyArray<T, D>) -> ArrayView<'py, T, D>
// where
//     T: Element,
//     D: Dimension,
// {
//     array.readonly();
// }

// // /// Convert a Rust array to a Python numpy array (makes a copy)
// // pub fn to_numpy<'py, T, D, A>(py: Python<'py>, array: A) -> PyResult<&'py PyArray<T, D>>
// // where
// //     T: Element,
// //     D: Dimension,
// //     A: IntoPyArray<Item = T, Dim = D>,
// // {
// //     Ok(array.into_pyarray(py))
// // }

// // /// Convert a Rust array to a Python numpy array by taking ownership
// // pub fn into_numpy<'py, T, D, A>(py: Python<'py>, array: A) -> PyResult<&'py PyArray<T, D>>
// // where
// //     T: Element,
// //     D: Dimension,
// //     A: IntoPyArray<Item = T, Dim = D>,
// // {
// //     Ok(array.into_pyarray(py))
// // }

// // /// Convert any supported numpy array to rust ndarray view
// // pub fn numpy_to_rust_ndview<'py, T, D>(array: &'py PyArray<T, D>) -> ArrayView<'py, T, D>
// // where
// //     T: Element,
// //     D: Dimension,
// // {
// //     to_rust_view(array)
// // }

// // /// Convert rust ndarray view back to numpy array
// // pub fn rust_ndview_to_numpy<'py, T, D, A>(
// //     py: Python<'py>,
// //     array: ArrayView<'py, T, D>,
// // ) -> PyResult<&'py PyArray<T, D>>
// // where
// //     T: Element,
// //     D: Dimension,
// //     ArrayView<'py, T, D>: numpy::ToPyArray<Item = T, Dim = D>,
// // {
// //     to_numpy(py, array)
// // }
