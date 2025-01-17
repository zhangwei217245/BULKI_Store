use anyhow::Result;
use commons::handler::HandlerResult;
use commons::object::params::{GetObjectSliceParams, GetObjectSliceResponse};
use commons::object::types::SerializableSliceInfoElem;
use commons::object::{
    params::CreateObjectParams,
    types::{MetadataValue, SupportedRustArrayD},
    DataObject, DataStore,
};
use commons::region::{SerializableNDArray, SerializableSlice};
use commons::rpc::{RPCData, StatusCode};
use lazy_static::lazy_static;
use log::debug;
use ndarray::{Slice, SliceInfoElem};
use rmp_serde;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, RwLock};

    use bulkistore_commons::dispatch::Dispatchable;
    #[derive(Default)]
    pub struct BulkiStore {
        // Add any store-specific fields here
    }

    impl BulkiStore {
        pub fn new() -> Self {
            Self::default()
        }

    let mut obj_ids: Vec<u128> = Vec::with_capacity(params.len());
    for param in params {
        match create_object_internal(param) {
            Ok(Some(obj_id)) => obj_ids.push(obj_id),
            Ok(None) => {
                return HandlerResult {
                    status_code: StatusCode::Internal as u8,
                    message: Some("Failed to create object: returned None".to_string()),
                }
            }
            Err(e) => {
                return HandlerResult {
                    status_code: StatusCode::Internal as u8,
                    message: Some(format!("Failed to create object: {}", e)),
                }
            }
        }
    }

    debug!("create_objects: obj_ids length: {:?}", obj_ids.len());
    debug!("create_objects: obj_ids: {:?}", obj_ids);
    // Return the id of the object to the client
    data.data = Some(
        rmp_serde::to_vec(&obj_ids)
            .map_err(|e| HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to serialize object: {}", e)),
            })
            .unwrap(),
    );

    debug!("create_objects: {:?}", data.data.as_ref().unwrap().len());

    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

/// Get a DataObject by its ID.
pub fn get_object_data(data: &mut RPCData) -> HandlerResult {
    // Deserialize the ID from the incoming data.
    let params: GetObjectSliceParams = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize id: {}", e)),
        })
        .unwrap();

    // Acquire a read lock on the DataStore.
    let store = GLOBAL_STORE.read().unwrap();
    match store.get(params.obj_id) {
        Some(obj) => {
            // Convert SerializableSliceInfoElem to SliceInfoElem
            let array_slice = obj.get_array_slice(params.region.map(|slices| {
                slices
                    .into_iter()
                    .map(SliceInfoElem::from)
                    .collect::<Vec<_>>()
            }));
            let sub_obj_regions: Option<Vec<(u128, Option<Vec<SliceInfoElem>>)>> =
                params.sub_obj_regions.map(|sub_regions| {
                    sub_regions
                        .into_iter()
                        .filter_map(|(name, slices)| {
                            obj.get_child_id_by_name(&name).map(|id| {
                                (
                                    id,
                                    match slices {
                                        Some(slices) => Some(
                                            slices
                                                .into_iter()
                                                .map(|x| SliceInfoElem::from(x))
                                                .collect::<Vec<_>>(),
                                        ),
                                        None => None,
                                    },
                                )
                            })
                        })
                        .collect()
                });
            let sub_obj_slices = match sub_obj_regions {
                Some(regions) => store.get_regions_by_obj_ids(regions),
                None => vec![],
            };
            let response = GetObjectSliceResponse {
                obj_id: params.obj_id,
                array_slice,
                sub_obj_slices: Some(sub_obj_slices),
            };

            debug!("get_object_data response: {:?}", response);

            data.data = Some(
                rmp_serde::to_vec(&response)
                    .map_err(|e| HandlerResult {
                        status_code: StatusCode::Internal as u8,
                        message: Some(format!("Failed to serialize response: {}", e)),
                    })
                    .unwrap(),
            );

            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        None => HandlerResult {
            status_code: StatusCode::NotFound as u8,
            message: Some(format!("Object {} not found", params.obj_id)),
        },
    }
}

/// Update metadata of a DataObject.
pub fn update_metadata(data: &mut RPCData) -> HandlerResult {
    // Deserialize (id, key, value) from the incoming data.
    let (id, key, value): (u128, String, MetadataValue) =
        rmp_serde::from_slice(&data.data.as_ref().unwrap())
            .map_err(|e| HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to deserialize input: {}", e)),
            })
            .unwrap();

    // Acquire a write lock.
    let store = GLOBAL_STORE.write().unwrap();
    match store.get(id) {
        Some(mut obj) => {
            // Update the metadata and reinsert the object.
            obj.set_metadata(key, value);
            store.insert(obj.clone());
            data.data = Some(
                rmp_serde::to_vec(&obj)
                    .map_err(|e| HandlerResult {
                        status_code: StatusCode::Internal as u8,
                        message: Some(format!("Failed to serialize object: {}", e)),
                    })
                    .unwrap(),
            );
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        None => HandlerResult {
            status_code: StatusCode::NotFound as u8,
            message: Some(format!("Object with id {} not found", id)),
        },
    }
}

/// Update the NDArray of a DataObject.
pub fn update_array(data: &mut RPCData) -> HandlerResult {
    // Deserialize (id, array) from the incoming data.
    let (id, array): (u128, SupportedRustArrayD) =
        rmp_serde::from_slice(&data.data.as_ref().unwrap())
            .map_err(|e| HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to deserialize input: {}", e)),
            })
            .unwrap();

    let store = GLOBAL_STORE.write().unwrap();
    match store.get(id) {
        Some(mut obj) => {
            // Update the NDArray and reinsert the object.
            obj.attach_array(array);
            store.insert(obj.clone());
            data.data = Some(
                rmp_serde::to_vec(&obj)
                    .map_err(|e| HandlerResult {
                        status_code: StatusCode::Internal as u8,
                        message: Some(format!("Failed to serialize object: {}", e)),
                    })
                    .unwrap(),
            );
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        None => HandlerResult {
            status_code: StatusCode::NotFound as u8,
            message: Some(format!("Object with id {} not found", id)),
        },
    }
}

/// Delete a DataObject.
pub fn delete_object(data: &mut RPCData) -> HandlerResult {
    // Deserialize the id.
    let id: u128 = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize id: {}", e)),
        })
        .unwrap();

    let store = GLOBAL_STORE.write().unwrap();
    match store.remove(id) {
        Some(obj) => {
            // Serialize the removed object back into RPCData.
            data.data = Some(
                rmp_serde::to_vec(&obj)
                    .map_err(|e| HandlerResult {
                        status_code: StatusCode::Internal as u8,
                        message: Some(format!("Failed to serialize object: {}", e)),
                    })
                    .unwrap(),
            );
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        None => HandlerResult {
            status_code: StatusCode::NotFound as u8,
            message: Some(format!("Object with id {} not found", id)),
        },
    }
}

#[allow(dead_code)]
/// Get metadata for a DataObject by its ID and metadata keys.
pub fn get_metadata(data: &mut RPCData) -> HandlerResult {
    // Deserialize the ID and keys from the incoming data.
    let (id, keys): (u128, Vec<String>) = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize input: {}", e)),
        })
        .unwrap();

    // Convert String keys to &str
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

    // Acquire a read lock on the DataStore.
    let store = GLOBAL_STORE.read().unwrap();
    match store.get_metadata(id, key_refs) {
        Some(metadata) => {
            // Serialize the metadata back into RPCData.
            data.data = Some(
                rmp_serde::to_vec(&metadata)
                    .map_err(|e| HandlerResult {
                        status_code: StatusCode::Internal as u8,
                        message: Some(format!("Failed to serialize metadata: {}", e)),
                    })
                    .unwrap(),
            );
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        None => HandlerResult {
            status_code: StatusCode::NotFound as u8,
            message: Some(format!("Metadata not found for object {}", id)),
        },
    }
}

#[allow(dead_code)]
/// Get a slice of an NDArray from a DataObject.
pub fn get_object_slice(data: &mut RPCData) -> HandlerResult {
    // Deserialize the ID and slice pattern from the incoming data.
    let (id, region): (u128, Vec<SerializableSliceInfoElem>) =
        rmp_serde::from_slice(&data.data.as_ref().unwrap())
            .map_err(|e| HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to deserialize input: {}", e)),
            })
            .unwrap();

    // Convert SerializableSlice to ndarray::Slice
    let region: Vec<SliceInfoElem> = region.into_iter().map(|s| s.into()).collect();

    // Acquire a read lock on the DataStore.
    let store = GLOBAL_STORE.read().unwrap();
    match store.get_object_slice(id, Some(region)) {
        Some(array) => {
            // Serialize the array back into RPCData.
            data.data = Some(
                rmp_serde::to_vec(&array)
                    .map_err(|e| HandlerResult {
                        status_code: StatusCode::Internal as u8,
                        message: Some(format!("Failed to serialize array slice: {}", e)),
                    })
                    .unwrap(),
            );
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        None => HandlerResult {
            status_code: StatusCode::NotFound as u8,
            message: Some(format!("Array slice not found for object {}", id)),
        },
    }
}

#[allow(dead_code)]
/// Get multiple array slices from multiple DataObjects.
pub fn get_regions_by_obj_ids(data: &mut RPCData) -> HandlerResult {
    // Deserialize the vector of (id, region) pairs from the incoming data.
    let obj_regions: Vec<(u128, Option<Vec<SerializableSliceInfoElem>>)> =
        rmp_serde::from_slice(&data.data.as_ref().unwrap())
            .map_err(|e| HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to deserialize input: {}", e)),
            })
            .unwrap();

    // Convert SerializableSlice to ndarray::Slice
    let obj_regions = obj_regions
        .into_iter()
        .map(|(id, region)| {
            (
                id,
                match region {
                    Some(r) => Some(r.into_iter().map(|s| s.into()).collect()),
                    None => None,
                },
            )
        })
        .collect();

    // Acquire a read lock on the DataStore.
    let store = GLOBAL_STORE.read().unwrap();
    let arrays = store.get_regions_by_obj_ids(obj_regions);

    // Serialize the results back into RPCData.
    data.data = Some(
        rmp_serde::to_vec(&arrays)
            .map_err(|e| HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to serialize array slices: {}", e)),
            })
            .unwrap(),
    );

    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

// Async versions of the benchmark functions
pub fn times_two(data: &mut RPCData) -> HandlerResult {
    match SerializableNDArray::deserialize(&data.data.as_ref().unwrap()) {
        Ok(array) => {
            debug!("Received array: {:?}", array);
            let result = array.mapv(|x: f64| x * 2.0);
            debug!("Result array: {:?}", result);
            data.data = Some(SerializableNDArray::serialize(result).unwrap());
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        Err(_) => HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(String::from("Failed to deserialize array")),
        },
    }
}

pub fn times_three(data: &mut RPCData) -> HandlerResult {
    match SerializableNDArray::deserialize(&data.data.as_ref().unwrap()) {
        Ok(array) => {
            let result = array.mapv(|x: f64| x * 3.0);
            data.data = Some(SerializableNDArray::serialize(result).unwrap());
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        Err(_) => HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(String::from("Failed to deserialize array")),
        },
    }
}
