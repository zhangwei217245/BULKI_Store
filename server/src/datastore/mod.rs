use anyhow::Result;
use commons::handler::HandlerResult;
use commons::object::{
    params::CreateObjectParams,
    types::{MetadataValue, SupportedRustArrayD},
    DataObject, DataStore,
};
use commons::region::{SerializableNDArray, SerializableSlice};
use commons::rpc::{RPCData, StatusCode};
use lazy_static::lazy_static;
use log::debug;
use ndarray::Slice;
use rmp_serde;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, RwLock};

// Global DataStore instance using the standard RwLock.
lazy_static! {
    pub static ref GLOBAL_STORE: Arc<RwLock<DataStore>> = Arc::new(RwLock::new(DataStore::new()));
    static ref SEQUENCE_COUNTER: AtomicU32 = AtomicU32::new(0);
    static ref LAST_TIMESTAMP: AtomicU64 = AtomicU64::new(0);
}

/// Initialize the global DataStore.
/// Synchronous version: no async/await.
pub fn init_datastore() -> HandlerResult {
    // Reset or initialize the store if needed.
    // Acquire a write lock and replace the store.
    *GLOBAL_STORE.write().unwrap() = DataStore::new();
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

fn create_object_internal(param: CreateObjectParams) -> Result<Option<u128>> {
    let obj = DataObject::new(param);
    let obj_id = obj.id;
    // Insert into store
    GLOBAL_STORE.write().unwrap().insert(obj);
    Ok(Some(obj_id))
}

/// Create a new DataObject with server-generated ID
pub fn create_objects(data: &mut RPCData) -> HandlerResult {
    debug!("Received request to create objects");
    // Deserialize the creation parameters
    let params: Vec<CreateObjectParams> = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize params: {}", e)),
        })
        .unwrap();

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
pub fn get_object(data: &mut RPCData) -> HandlerResult {
    // Deserialize the ID from the incoming data.
    let id: u128 = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize id: {}", e)),
        })
        .unwrap();

    // Acquire a read lock on the DataStore.
    let store = GLOBAL_STORE.read().unwrap();
    match store.get(id) {
        Some(obj) => {
            // Serialize the object back into RPCData.
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
    let (id, region): (u128, Vec<SerializableSlice>) =
        rmp_serde::from_slice(&data.data.as_ref().unwrap())
            .map_err(|e| HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to deserialize input: {}", e)),
            })
            .unwrap();

    // Convert SerializableSlice to ndarray::Slice
    let region: Vec<Slice> = region.into_iter().map(|s| s.into()).collect();

    // Acquire a read lock on the DataStore.
    let store = GLOBAL_STORE.read().unwrap();
    match store.get_object_slice(id, &region) {
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
    let obj_regions: Vec<(u128, Vec<SerializableSlice>)> =
        rmp_serde::from_slice(&data.data.as_ref().unwrap())
            .map_err(|e| HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to deserialize input: {}", e)),
            })
            .unwrap();

    // Convert SerializableSlice to ndarray::Slice
    let obj_regions: Vec<(u128, Vec<Slice>)> = obj_regions
        .into_iter()
        .map(|(id, region)| (id, region.into_iter().map(|s| s.into()).collect()))
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
