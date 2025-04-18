#[allow(unused_imports)]
use crate::srvctx::{get_rank, get_size, server_rpc_call};
use anyhow::Result;
use commons::err::StatusCode;
use commons::handler::HandlerResult;
use commons::job::JobProgress;
use commons::object::params::{
    GetObjectMetaParams, GetObjectMetaResponse, GetObjectSliceParams, GetObjectSliceResponse,
    GetSampleRequest, GetSampleResponse, SerializableMetaKeySpec,
};
use commons::object::types::{ObjectIdentifier, SerializableSliceInfoElem};
use commons::object::{
    params::CreateObjectParams,
    types::{MetadataValue, SupportedRustArrayD},
    DataObject, DataStore,
};
use commons::region::SerializableNDArray;
use commons::rpc::RPCData;
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
use ndarray::SliceInfoElem;
use rayon::prelude::*;
use rmp_serde;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, RwLock, RwLockReadGuard};

// Global DataStore instance using the standard RwLock.
lazy_static! {
    pub static ref GLOBAL_STORE: Arc<RwLock<DataStore>> = Arc::new(RwLock::new(DataStore::new()));
    static ref SEQUENCE_COUNTER: AtomicU32 = AtomicU32::new(0);
    static ref LAST_TIMESTAMP: AtomicU64 = AtomicU64::new(0);
    pub static ref JOB_PROGRESS: Arc<RwLock<HashMap<String, JobProgress>>> =
        Arc::new(RwLock::new(HashMap::new()));
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

fn create_object_internal(param: CreateObjectParams) -> Result<u128> {
    let obj = DataObject::new(param);
    // Insert into store
    GLOBAL_STORE.write().unwrap().insert(obj)
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
    let mut message = None;

    // insert main object first
    match create_object_internal(params[0].clone()) {
        Ok(obj_id) => obj_ids.push(obj_id),
        Err(e) => {
            message = Some(format!("Failed to create object: {:?}", e));
            debug!("Failed to create object: {:?}", e);
            return HandlerResult {
                status_code: StatusCode::Internal as u8,
                message,
            };
        }
    }
    debug!("Created main object with id: {:?}", obj_ids[0]);

    if params.len() > 1 {
        // using rayon to parallelize the creation of remaining objects (sub-objects)
        let sub_obj_ids: Vec<Result<u128>> = params[1..]
            .par_iter()
            .map(|p| create_object_internal(p.clone()))
            .collect();

        for id in sub_obj_ids {
            match id {
                Ok(obj_id) => obj_ids.push(obj_id),
                Err(e) => {
                    debug!("Failed to create object: {:?}", e);
                    message = Some(format!("Failed to create object: {:?}", e));
                }
            }
        }
    }
    debug!("create_objects: obj_ids length: {:?}", obj_ids.len());
    debug!(
        "[RX Rank {:?}] create_objects: main_obj_id: {:?}, main_obj_name: {:?}",
        crate::srvctx::get_rank(),
        &obj_ids[0],
        &params[0].obj_name
    );
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
        message: message,
    }
}

fn _get_single_object_data_with_store_guard(
    params: GetObjectSliceParams,
    store: &RwLockReadGuard<DataStore>,
) -> Result<GetObjectSliceResponse> {
    let obj_id = match params.obj_id {
        ObjectIdentifier::U128(id) => Ok(id),
        ObjectIdentifier::Name(name) => match store.get_obj_id_by_name(&name.as_str()) {
            Some(id) => Ok(id),
            None => Err(anyhow::anyhow!("Object not found")),
        },
    }?;
    match store.get(obj_id) {
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
                        .into_par_iter()
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
                obj_id,
                obj_name: obj.name.clone(),
                array_slice,
                sub_obj_slices: Some(sub_obj_slices),
            };
            Ok(response)
        }
        None => Err(anyhow::anyhow!("Object not found")),
    }
}

fn get_single_object_data(params: GetObjectSliceParams) -> Result<GetObjectSliceResponse> {
    let store = GLOBAL_STORE.read().unwrap();
    _get_single_object_data_with_store_guard(params, &store)
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

    info!(
        "[RX Rank {:?}] get_object_data: from client {:?}",
        crate::srvctx::get_rank(),
        data.metadata.as_ref().unwrap().client_rank
    );

    match get_single_object_data(params) {
        Ok(response) => {
            data.data = Some(rmp_serde::to_vec(&response).unwrap());
            debug!(
                "[RX Rank {:?}] get_object_data response: obj_id: {:?}, obj_name: {:?}",
                crate::srvctx::get_rank(),
                response.obj_id,
                response.obj_name
            );
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        Err(e) => HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(e.to_string()),
        },
    }
}

pub fn get_multiple_object_data(data: &mut RPCData) -> HandlerResult {
    let params: Vec<GetObjectSliceParams> = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize id: {}", e)),
        })
        .unwrap();

    let param_len = params.len();
    let mut results = Vec::with_capacity(param_len);
    for param in params {
        if let Ok(result) = get_single_object_data(param) {
            results.push(result);
        }
    }
    debug!(
        "[RX Rank {:?}] get_multiple_object_data: req length: {} , resp length: {} , {} failure ignored.",
        crate::srvctx::get_rank(),
        param_len,
        results.len(),
        param_len - results.len()
    );
    data.data = Some(
        rmp_serde::to_vec(&results)
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

fn _get_single_object_metadata_with_store_guard(
    params: GetObjectMetaParams,
    store: &RwLockReadGuard<'_, DataStore>,
) -> Result<GetObjectMetaResponse> {
    let obj_id_u128 = match params.obj_id {
        ObjectIdentifier::U128(id) => id,
        ObjectIdentifier::Name(name) => match store.get_obj_id_by_name(&name.as_str()) {
            Some(id) => id,
            None => {
                return Err(anyhow::anyhow!(format!("Object {} not found", &name)));
            }
        },
    };

    let key_refs = params
        .meta_keys
        .as_ref()
        .map(|ks| ks.iter().map(|k| k.as_str()).collect())
        .unwrap_or_default();

    let obj_metadata: Option<(String, HashMap<String, MetadataValue>)> =
        store.get_obj_metadata(obj_id_u128, key_refs);

    let (obj_name, metadata) = match obj_metadata {
        Some((obj_name, metadata)) => (obj_name, metadata),
        None => {
            return Err(anyhow::anyhow!(format!(
                "Object {} not found",
                &obj_id_u128
            )));
        }
    };

    let sub_metadata_result: Option<Vec<(u128, String, HashMap<String, MetadataValue>)>> =
        match params.sub_meta_keys {
            Some(SerializableMetaKeySpec::Simple(keys)) => {
                // loading the same set of attributes for all related sub-objects
                let sub_obj_ids = store.get_obj_children(obj_id_u128).unwrap_or(vec![]);
                let meta_filter: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                Some(
                    sub_obj_ids
                        .iter()
                        .map(|(id, obj_name)| {
                            (
                                id.to_owned(),
                                obj_name.clone(),
                                store
                                    .get_obj_metadata(id.to_owned(), meta_filter.clone())
                                    .and_then(|(_, ometa)| Some(ometa))
                                    .unwrap_or(HashMap::new()),
                            )
                        })
                        .collect(),
                )
            }
            Some(SerializableMetaKeySpec::WithObject(map)) => {
                // loading different sets of attributes for each specified sub-object
                let result = store.get_sub_obj_metadata_by_names(obj_id_u128, Some(&map));
                match result {
                    Some(result) => Some(
                        result
                            .into_iter()
                            .filter_map(|(id, name, metadata)| Some((id, name, metadata)))
                            .collect(),
                    ),
                    None => {
                        let sub_obj_ids = store.get_obj_children(obj_id_u128).unwrap_or(vec![]);
                        Some(
                            sub_obj_ids
                                .iter()
                                .filter(|(_, obj_name)| map.contains_key(obj_name))
                                .map(|(id, obj_name)| {
                                    (
                                        id.to_owned(),
                                        obj_name.to_owned(),
                                        store
                                            .get_named_obj_metadata(
                                                obj_name,
                                                map.get(obj_name)
                                                    .unwrap_or(&vec![])
                                                    .iter()
                                                    .map(|s| s.as_str())
                                                    .collect(),
                                            )
                                            .unwrap_or(HashMap::new()),
                                    )
                                })
                                .collect(),
                        )
                    }
                }
            }
            None => None,
        };

    let result = GetObjectMetaResponse {
        obj_id: obj_id_u128,
        obj_name: obj_name.clone(),
        metadata: Some(metadata),
        sub_obj_metadata: sub_metadata_result,
    };
    Ok(result)
}

fn get_single_object_metadata(params: GetObjectMetaParams) -> Result<GetObjectMetaResponse> {
    let store = GLOBAL_STORE.read().unwrap();
    _get_single_object_metadata_with_store_guard(params, &store)
}

/// Get metadata for a DataObject by its ID and metadata keys.
pub fn get_object_metadata(data: &mut RPCData) -> HandlerResult {
    // Deserialize the ID from the incoming data.
    let params: GetObjectMetaParams = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize id: {}", e)),
        })
        .unwrap();

    let result = match get_single_object_metadata(params) {
        Ok(result) => result,
        Err(e) => {
            return HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to get object metadata: {}", e)),
            }
        }
    };

    debug!(
        "[RX Rank {:?}] get_object_metadata: obj_id: {:?}, obj_name: {:?}",
        crate::srvctx::get_rank(),
        result.obj_id,
        result.obj_name
    );
    data.data = Some(
        rmp_serde::to_vec(&result)
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

fn _get_multiple_object_metadata_with_store_guard(
    params: Vec<GetObjectMetaParams>,
    store: &RwLockReadGuard<'_, DataStore>,
) -> Result<Vec<Option<GetObjectMetaResponse>>> {
    let param_len = params.len();
    let mut results = Vec::with_capacity(param_len);
    for param in params {
        if let Ok(result) = _get_single_object_metadata_with_store_guard(param, store) {
            results.push(Some(result));
        } else {
            results.push(None);
        }
    }
    debug!(
        "[RX Rank {:?}] _get_multiple_object_metadata: req length: {} , resp length: {} , {} failure ignored.",
        crate::srvctx::get_rank(),
        param_len,
        results.len(),
        param_len - results.len()
    );
    Ok(results)
}

pub fn get_multiple_object_metadata(data: &mut RPCData) -> HandlerResult {
    let params: Vec<GetObjectMetaParams> = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize id: {}", e)),
        })
        .unwrap();

    match _get_multiple_object_metadata_with_store_guard(params, &GLOBAL_STORE.read().unwrap()) {
        Ok(results) => {
            data.data = Some(
                rmp_serde::to_vec(&results)
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
        Err(e) => {
            return HandlerResult {
                status_code: StatusCode::Internal as u8,
                message: Some(format!("Failed to get multiple object metadata: {}", e)),
            }
        }
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
            match store.insert(obj.clone()) {
                Ok(_) => (),
                Err(e) => {
                    return HandlerResult {
                        status_code: StatusCode::Internal as u8,
                        message: Some(format!("Failed to update metadata: {}", e)),
                    }
                }
            };
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

pub fn load_batch_samples(data: &mut RPCData) -> HandlerResult {
    // input : Vec<GetSampleRequest>,
    // output: HashMap<usize, GetSampleResponse>
    let params: Vec<GetSampleRequest> = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize input: {}", e)),
        })
        .unwrap();

    let store = GLOBAL_STORE.read().unwrap();

    let result = params
        .par_iter()
        .filter_map(|param| {
            // First get metadata to determine slice regions
            let metadata_request = GetObjectMetaParams {
                obj_id: param.obj_id.clone(),
                meta_keys: None,
                sub_meta_keys: Some(SerializableMetaKeySpec::Simple(
                    param.sample_var_keys.clone(),
                )),
            };

            if let Ok(meta_response) =
                _get_single_object_metadata_with_store_guard(metadata_request, &store)
            {
                let mut variable_data: HashMap<String, SupportedRustArrayD> = HashMap::new();
                // Process each sub-object metadata
                if let Some(sub_obj_metadata) = meta_response.sub_obj_metadata {
                    // level 1
                    let sub_obj_regions = sub_obj_metadata
                        .iter()
                        .map(|(_oid, vname, meta)| {
                            // Extract metadata values similar to Python code
                            if let (
                                Some(MetadataValue::Int(vdim)),
                                Some(MetadataValue::IntList(vshape)),
                                Some(MetadataValue::IntList(vcount)),
                                Some(MetadataValue::IntList(voffset)),
                            ) = (
                                meta.get("vdim"),
                                meta.get("vshape"),
                                meta.get("vcount"),
                                meta.get("voffset"),
                            ) {
                                // Calculate slice region
                                let mdim = *vdim as usize;
                                let local_smp_id = param.local_sample_id;

                                // Create slice info elements
                                let mut slice_info = Vec::new();
                                for dim_idx in 0..vshape.len() {
                                    if dim_idx == mdim {
                                        // For the dimension with variable data, calculate start and count
                                        let start = (voffset[local_smp_id] - voffset[0]) as isize;
                                        let count = vcount[local_smp_id] as isize;
                                        slice_info.push(SerializableSliceInfoElem::Slice {
                                            start: start,
                                            end: Some(start + count),
                                            step: 1,
                                        });
                                    } else {
                                        // For other dimensions, take the full range
                                        slice_info.push(SerializableSliceInfoElem::Slice {
                                            start: 0,
                                            end: None,
                                            step: 1,
                                        });
                                    }
                                }
                                // Add to regions
                                (vname.clone(), Some(slice_info))
                            } else {
                                (vname.clone(), None)
                            }
                        })
                        .collect::<Vec<(String, Option<Vec<SerializableSliceInfoElem>>)>>();
                    // level 1 end

                    // level 2
                    // Now fetch the actual array data using the slice regions
                    if !sub_obj_regions.is_empty() {
                        let slice_request = GetObjectSliceParams {
                            obj_id: param.obj_id.clone(),
                            region: None, // We're only interested in sub-objects
                            sub_obj_regions: Some(sub_obj_regions),
                        };

                        if let Ok(slice_response) =
                            _get_single_object_data_with_store_guard(slice_request, &store)
                        {
                            // Process the slice response
                            if let Some(sub_obj_slices) = slice_response.sub_obj_slices {
                                for (_, name, array_opt) in sub_obj_slices {
                                    if let Some(array) = array_opt {
                                        variable_data.insert(
                                            name.split('/').last().unwrap().to_string(),
                                            array,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    // level 2 end
                    Some((
                        param.sample_id,
                        GetSampleResponse {
                            original_idx: param.original_idx,
                            sample_id: param.sample_id,
                            variable_data,
                        },
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashMap<usize, GetSampleResponse>>();

    info!(
        "[RX Rank {:?}] load_batch_samples: {}/{} samples loaded",
        crate::srvctx::get_rank(),
        result.len(),
        params.len()
    );

    data.data = Some(
        rmp_serde::to_vec(&result)
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
            match store.insert(obj.clone()) {
                Ok(_) => (),
                Err(e) => {
                    return HandlerResult {
                        status_code: StatusCode::Internal as u8,
                        message: Some(format!("Failed to update array: {}", e)),
                    }
                }
            };
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

pub fn force_checkpointing(data: &mut RPCData) -> HandlerResult {
    let request: (String, String) = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize input: {}", e)),
        })
        .unwrap();
    let mut job_submitted = 0usize;
    info!("force_checkpointing request: {:?}", request);
    if request.0 == "client" {
        // Asynchronously checkpoint the data
        let rank = get_rank();
        let size = get_size();

        // Create a job ID for this checkpointing operation
        let job_id = request.1;

        // Get a handle to the current tokio runtime
        let rt = tokio::runtime::Handle::current();

        // Get total object count for progress tracking
        let total_objects = {
            let store = GLOBAL_STORE.read().unwrap();
            store.objects.len()
        };

        // Initialize job progress
        {
            let mut job_map = JOB_PROGRESS.write().unwrap();
            job_map.insert(
                job_id.clone(),
                JobProgress::new(job_id.clone(), total_objects),
            );
        }

        // Clone the job_id for the new thread
        let job_id_clone = job_id.clone();

        // Spawn a new task to perform the checkpointing
        std::thread::spawn(move || {
            // Enter the tokio runtime context
            let _guard = rt.enter();

            info!(
                "[R{}/S{}] Starting asynchronous checkpointing job: {}",
                rank, size, job_id_clone
            );

            // Mark job as running
            {
                let mut job_map = JOB_PROGRESS.write().unwrap();
                if let Some(job) = job_map.get_mut(&job_id_clone) {
                    job.mark_running();
                }
            }

            let timer = std::time::Instant::now();

            match dump_memory_store_with_progress(job_id_clone.clone()) {
                Ok(saved_count) => {
                    // Mark job as completed
                    {
                        let mut job_map = JOB_PROGRESS.write().unwrap();
                        if let Some(job) = job_map.get_mut(&job_id_clone) {
                            job.mark_completed();
                        }
                    }

                    info!(
                        "[R{}/S{}] Completed asynchronous checkpointing job: {}, saved {} objects in {:.2?}",
                        rank, size, job_id_clone, saved_count, timer.elapsed()
                    );
                }
                Err(e) => {
                    // Mark job as failed
                    {
                        let mut job_map = JOB_PROGRESS.write().unwrap();
                        if let Some(job) = job_map.get_mut(&job_id_clone) {
                            job.mark_failed(&e.to_string());
                        }
                    }

                    error!(
                        "[R{}/S{}] Failed asynchronous checkpointing job: {}, error: {}",
                        rank, size, job_id_clone, e
                    );
                }
            }
        });

        info!(
            "[R{}/S{}] Initiated asynchronous checkpointing job: {}",
            get_rank(),
            get_size(),
            job_id.clone()
        );
        job_submitted += 1;
    }
    // Store the job ID in the response
    data.data = Some(rmp_serde::to_vec(&job_submitted).unwrap());
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

/// Get the progress of a job by its ID
pub fn get_checkpointing_progress(data: &mut RPCData) -> HandlerResult {
    // Deserialize the job ID from the request
    let job_id: String = rmp_serde::from_slice(&data.data.as_ref().unwrap())
        .map_err(|e| HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!("Failed to deserialize job ID: {}", e)),
        })
        .unwrap();

    // Look up the job progress
    let job_progress = {
        let job_map = JOB_PROGRESS.read().unwrap();
        job_map.get(&job_id).cloned()
    };

    data.data = job_progress.map(|mut progress| progress.to_vec().unwrap());

    // Return the job progress or an error if not found
    match data.data.as_ref() {
        Some(_) => HandlerResult {
            status_code: StatusCode::Ok as u8,
            message: None,
        },
        None => HandlerResult {
            status_code: StatusCode::NotFound as u8,
            message: Some(format!("Job with ID {} not found", job_id)),
        },
    }
}

/// Dump memory store with progress callback
pub fn dump_memory_store_with_progress(job_id: String) -> Result<usize> {
    let mut job_map = JOB_PROGRESS.write().unwrap();
    if let Some(job) = job_map.get_mut(&job_id) {
        job.mark_running();
    }

    let store = GLOBAL_STORE.write().unwrap();
    let rank = get_rank();
    let size = get_size();

    // read env var "PDC_DATA_LOC" and use it as the path, the default value should be "./.bulkistore_data"
    let data_dir = std::env::var("PDC_DATA_LOC").unwrap_or("./.bulkistore_data".to_string());
    // Create directory if it doesn't exist
    std::fs::create_dir_all(&data_dir)?;

    let total_objects = store.objects.len();
    if total_objects == 0 {
        info!("[R{}/S{}] No objects to save", rank, size);
        return Ok(0);
    }

    // Create a single file with timestamp to avoid conflicts
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let filename = format!("{}/objects_{}_{}.oset", data_dir, timestamp, rank);

    info!(
        "[R{}/S{}] Saving {} objects to {}",
        rank, size, total_objects, filename
    );

    let file = std::fs::File::create(&filename)?;
    let mut writer = std::io::BufWriter::new(file);

    // Write file header with metadata
    let header = commons::object::DSCheckpointHeader {
        version: 1,
        timestamp,
        rank,
        size,
        object_count: total_objects as u64,
    };

    rmp_serde::encode::write(&mut writer, &header)?;

    let progress_interval = std::cmp::max(1, total_objects / 100); // Report progress every 1%

    // Simply save each (id, object) pair
    store.objects.iter().enumerate().for_each(|(i, obj)| {
        if let Some(job) = job_map.get_mut(&job_id) {
            job.increment_processed();
        }
        // Serialize the ID and object
        let id = obj.id;
        let result: Result<(), anyhow::Error> = (|| {
            rmp_serde::encode::write(&mut writer, &id)?;
            rmp_serde::encode::write(&mut writer, &obj)?;
            Ok(())
        })();

        if let Err(e) = result {
            warn!("[R{}/S{}] Failed to save object {}: {}", rank, size, id, e);
        } else {
            if let Some(job) = job_map.get_mut(&job_id) {
                job.increment_completed();
            }

            if i % progress_interval == 0 || i == total_objects - 1 {
                let progress = (i as f64 + 1.0) * 100.0 / total_objects as f64;
                if let Some(job) = job_map.get_mut(&job_id) {
                    info!(
                        "[R{}/S{}] Saved {}/{} objects ({:.1}%)",
                        rank,
                        size,
                        job.completed(),
                        total_objects,
                        progress
                    );
                }
            }
        }
    });

    // Flush the writer to ensure all data is written
    writer.flush()?;

    let mut completed_steps = 0;
    if let Some(job) = job_map.get_mut(&job_id) {
        completed_steps = job.completed();
        info!(
            "[R{}/S{}] Successfully saved {}/{} objects to {}",
            rank, size, completed_steps, total_objects, filename
        );
    }
    Ok(completed_steps)
}

pub fn load_memory_store() -> Result<usize> {
    let store = GLOBAL_STORE.write().unwrap();
    store.load_memorystore_from_file(get_rank(), get_size())
}

// Async versions of the benchmark functions
pub fn times_two(data: &mut RPCData) -> HandlerResult {
    match rmp_serde::from_slice::<SupportedRustArrayD>(&data.data.as_ref().unwrap()) {
        Ok(array) => {
            debug!("Received array: {:?}", array);
            let result = match array {
                SupportedRustArrayD::Int8(arr) => SupportedRustArrayD::Int8(arr.mapv(|x| x * 2)),
                SupportedRustArrayD::Int16(arr) => SupportedRustArrayD::Int16(arr.mapv(|x| x * 2)),
                SupportedRustArrayD::Int32(arr) => SupportedRustArrayD::Int32(arr.mapv(|x| x * 2)),
                SupportedRustArrayD::Int64(arr) => SupportedRustArrayD::Int64(arr.mapv(|x| x * 2)),
                SupportedRustArrayD::UInt8(arr) => SupportedRustArrayD::UInt8(arr.mapv(|x| x * 2)),
                SupportedRustArrayD::UInt16(arr) => {
                    SupportedRustArrayD::UInt16(arr.mapv(|x| x * 2))
                }
                SupportedRustArrayD::UInt32(arr) => {
                    SupportedRustArrayD::UInt32(arr.mapv(|x| x * 2))
                }
                SupportedRustArrayD::UInt64(arr) => {
                    SupportedRustArrayD::UInt64(arr.mapv(|x| x * 2))
                }
                SupportedRustArrayD::Float32(arr) => {
                    SupportedRustArrayD::Float32(arr.mapv(|x| x * 2.0))
                }
                SupportedRustArrayD::Float64(arr) => {
                    SupportedRustArrayD::Float64(arr.mapv(|x| x * 2.0))
                }
                SupportedRustArrayD::UInt128(arr) => {
                    SupportedRustArrayD::UInt128(arr.mapv(|x| x * 2))
                }
                SupportedRustArrayD::Int128(arr) => {
                    SupportedRustArrayD::Int128(arr.mapv(|x| x * 2))
                }
            };
            debug!("Result array: {:?}", result);
            data.data = Some(rmp_serde::to_vec(&result).unwrap());
            HandlerResult {
                status_code: StatusCode::Ok as u8,
                message: None,
            }
        }
        Err(_) => HandlerResult {
            status_code: StatusCode::Internal as u8,
            message: Some(format!(
                "Failed to deserialize array. Origin data length: {}",
                data.data.as_ref().unwrap().len()
            )),
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
