use crate::srvctx::{get_rank, get_size};
use commons::handler::HandlerResult;
use commons::object::{ArrayType, CreateObjectParams, DataObject, DataStore, MetadataValue};
use commons::region::{SerializableNDArray, SerializableSlice};
use commons::rpc::{RPCData, StatusCode};
use lazy_static::lazy_static;
use ndarray::{ArrayD, Slice};
use rmp_serde;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

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

fn get_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn generate_obj_id(client_rank: u32) -> u128 {
    // Get current timestamp and ensure it's monotonically increasing
    let mut timestamp = get_timestamp_ms();
    let last = LAST_TIMESTAMP.load(Ordering::SeqCst);
    if timestamp <= last {
        timestamp = last + 1;
    }
    LAST_TIMESTAMP.store(timestamp, Ordering::SeqCst);

    // Get sequence number
    let seq = SEQUENCE_COUNTER.fetch_add(1, Ordering::SeqCst);

    // Generate UUID components
    let server_rank = get_rank();

    // Construct the 128-bit ID with following layout:
    // [timestamp(64)][server_rank(16)][client_rank(16)][sequence(32)]
    // This gives us:
    // - 64 bits: timestamp (milliseconds, gives us 584,942 years from epoch)
    // - 16 bits: server rank (65,536 servers)
    // - 16 bits: client rank (65,536 clients)
    // - 32 bits: sequence number (4 billion sequences per millisecond)
    let id = ((timestamp as u128) << 64) |                // Timestamp in top 64 bits
             ((server_rank as u128 & 0xFFFF) << 48) |    // Server rank in next 16 bits
             ((client_rank as u128 & 0xFFFF) << 32) |    // Client rank in next 16 bits
             (seq as u128 & 0xFFFFFFFF); // Sequence in bottom 32 bits

    id
}

/// Create a new DataObject with server-generated ID
pub fn create_object(data: &mut RPCData) -> HandlerResult {
    // Deserialize the creation parameters
    let params: CreateObjectParams = rmp_serde::from_slice(&data.data)
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize params: {}", e)),
        })
        .unwrap();
    let client_rank = params.client_rank;
    let mut obj = DataObject::new(params);
    obj.id = generate_obj_id(client_rank);

    // Insert into store
    GLOBAL_STORE.write().unwrap().insert(obj.clone());

    // Return the id of the object to the client
    data.data = rmp_serde::to_vec(&obj.id)
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to serialize object: {}", e)),
        })
        .unwrap();

    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

/// Get a DataObject by its ID.
pub fn get_object(data: &mut RPCData) -> HandlerResult {
    // Deserialize the ID from the incoming data.
    let id: u128 = rmp_serde::from_slice(&data.data)
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
            data.data = rmp_serde::to_vec(&obj)
                .map_err(|e| HandlerResult {
                    status_code: StatusCode::Internal as u8,
                    message: Some(format!("Failed to serialize object: {}", e)),
                })
                .unwrap();
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
    let (id, key, value): (u128, String, MetadataValue) = rmp_serde::from_slice(&data.data)
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize input: {}", e)),
        })
        .unwrap();

    // Acquire a write lock.
    let mut store = GLOBAL_STORE.write().unwrap();
    match store.get(id) {
        Some(mut obj) => {
            // Update the metadata and reinsert the object.
            obj.set_metadata(key, value);
            store.insert(obj.clone());
            data.data = rmp_serde::to_vec(&obj)
                .map_err(|e| HandlerResult {
                    status_code: StatusCode::Internal as u8,
                    message: Some(format!("Failed to serialize object: {}", e)),
                })
                .unwrap();
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
    let (id, array): (u128, ArrayType) = rmp_serde::from_slice(&data.data)
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize input: {}", e)),
        })
        .unwrap();

    let mut store = GLOBAL_STORE.write().unwrap();
    match store.get(id) {
        Some(mut obj) => {
            // Update the NDArray and reinsert the object.
            obj.attach_array(array);
            store.insert(obj.clone());
            data.data = rmp_serde::to_vec(&obj)
                .map_err(|e| HandlerResult {
                    status_code: StatusCode::Internal as u8,
                    message: Some(format!("Failed to serialize object: {}", e)),
                })
                .unwrap();
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
    let id: u128 = rmp_serde::from_slice(&data.data)
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize id: {}", e)),
        })
        .unwrap();

    let mut store = GLOBAL_STORE.write().unwrap();
    match store.remove(id) {
        Some(obj) => {
            // Serialize the removed object back into RPCData.
            data.data = rmp_serde::to_vec(&obj)
                .map_err(|e| HandlerResult {
                    status_code: StatusCode::Internal as u8,
                    message: Some(format!("Failed to serialize object: {}", e)),
                })
                .unwrap();
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

/// Get metadata for a DataObject by its ID and metadata keys.
pub fn get_metadata(data: &mut RPCData) -> HandlerResult {
    // Deserialize the ID and keys from the incoming data.
    let (id, keys): (u128, Vec<String>) = rmp_serde::from_slice(&data.data)
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
            data.data = rmp_serde::to_vec(&metadata)
                .map_err(|e| HandlerResult {
                    status_code: StatusCode::Internal as u8,
                    message: Some(format!("Failed to serialize metadata: {}", e)),
                })
                .unwrap();
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

/// Get a slice of an NDArray from a DataObject.
pub fn get_object_slice(data: &mut RPCData) -> HandlerResult {
    // Deserialize the ID and slice pattern from the incoming data.
    let (id, region): (u128, Vec<SerializableSlice>) = rmp_serde::from_slice(&data.data)
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
            data.data = rmp_serde::to_vec(&array)
                .map_err(|e| HandlerResult {
                    status_code: StatusCode::Internal as u8,
                    message: Some(format!("Failed to serialize array slice: {}", e)),
                })
                .unwrap();
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

/// Get multiple array slices from multiple DataObjects.
pub fn get_regions_by_obj_ids(data: &mut RPCData) -> HandlerResult {
    // Deserialize the vector of (id, region) pairs from the incoming data.
    let obj_regions: Vec<(u128, Vec<SerializableSlice>)> = rmp_serde::from_slice(&data.data)
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
    data.data = rmp_serde::to_vec(&arrays)
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to serialize array slices: {}", e)),
        })
        .unwrap();

    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

// Async versions of the benchmark functions
pub fn times_two(data: &mut RPCData) -> HandlerResult {
    match SerializableNDArray::deserialize(&data.data) {
        Ok(array) => {
            let result = array.mapv(|x: f64| x * 2.0);
            data.data = SerializableNDArray::serialize(result).unwrap();
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
    match SerializableNDArray::deserialize(&data.data) {
        Ok(array) => {
            let result = array.mapv(|x: f64| x * 3.0);
            data.data = SerializableNDArray::serialize(result).unwrap();
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
