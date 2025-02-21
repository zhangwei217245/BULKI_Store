use crate::cltctx::get_client_rank;
use commons::{
    handler::HandlerResult,
    object::{objid::GlobalObjectIdExt, params::CreateObjectParams},
    rpc::RPCData,
};
use log::debug;
use pyo3::{types::PyDict, Bound};
use rand::distr::Alphanumeric;
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::pyctx::converter::SupportedNumpyArray;

fn generate_random_string() -> String {
    let pid = std::process::id();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();

    let random_suffix: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8) // Shorter random part since we're adding pid and timestamp
        .map(char::from)
        .collect();

    format!("{:x}_{:x}_{}", pid, timestamp & 0xFFFFFF, random_suffix)
}

/// Notes:
/// if you specify parent_id, all the objects including sub-objects will be created under the parent object,
/// and they will be co-located on the same virtual node.
/// Otherwise, each major object will be created on a different virtual node,
/// and all the sub-objects will be co-located on the same virtual node.
/// But one thing is certain: regardless of the existence of parent_id, the sub-objects and major object will be colocated.
pub fn create_objects_req_proc<'py>(
    obj_name_key: String,
    parent_id: Option<u128>,
    metadata: Option<Bound<'py, PyDict>>,
    array_meta_list: Option<Vec<Option<Bound<'py, PyDict>>>>,
    array_data_list: Option<Vec<Option<SupportedNumpyArray<'py>>>>,
) -> Option<Vec<CreateObjectParams>> {
    // Convert single metadata dict
    let major_metadata = crate::pyctx::converter::convert_metadata(Some(vec![metadata]))
        .unwrap()
        .and_then(|mut vec| vec.pop())
        .flatten();

    let sub_obj_meta_list = crate::pyctx::converter::convert_metadata(array_meta_list).unwrap();

    // Get the name from metadata or generate a random one
    let obj_name = major_metadata
        .as_ref()
        .and_then(|m| m.get(&obj_name_key))
        .map(|v| v.to_string())
        .unwrap_or_else(|| format!("obj_{}", generate_random_string()));

    let main_obj_id = commons::object::objid::GlobalObjectId::with_vnode_id(
        &obj_name,
        parent_id.map(|id| id.vnode_id()),
    )
    .to_u128();

    let create_obj_params: Option<Vec<CreateObjectParams>> = match array_data_list {
        // no array data, this must be a container object
        None => Some(vec![CreateObjectParams {
            obj_id: main_obj_id,
            obj_name: obj_name,
            obj_name_key: obj_name_key.clone(),
            parent_id: parent_id,
            initial_metadata: major_metadata,
            array_data: None,
            client_rank: get_client_rank(),
        }]),
        Some(array_vec) => {
            let vec_len = array_vec.len();
            let mut params = Vec::with_capacity(vec_len + 1);

            // Step 1: create the major object first
            let main_object = CreateObjectParams {
                obj_id: main_obj_id,
                obj_name: obj_name.clone(),
                obj_name_key: obj_name_key.clone(),
                parent_id: parent_id,
                initial_metadata: major_metadata,
                array_data: None,
                client_rank: get_client_rank(),
            };
            params.push(main_object);

            // Step 2: create the sub-objects
            for (i, array) in array_vec.into_iter().enumerate() {
                let metadata = match sub_obj_meta_list.as_ref() {
                    Some(map_list) => map_list[i].to_owned(),
                    None => None,
                };

                let sub_obj_name = match metadata.as_ref() {
                    Some(map) => map
                        .get(&obj_name_key)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| format!("{}/{}", obj_name, i)),
                    None => format!("{}/{}", obj_name, i),
                };

                let obj_id = commons::object::objid::GlobalObjectId::with_vnode_id(
                    &sub_obj_name,
                    Some(parent_id.unwrap_or_else(|| main_obj_id).vnode_id()),
                )
                .to_u128();

                params.push(CreateObjectParams {
                    obj_id,
                    obj_name: sub_obj_name,
                    obj_name_key: obj_name_key.clone(),
                    parent_id: Some(parent_id.unwrap_or_else(|| main_obj_id)),
                    initial_metadata: metadata,
                    array_data: match array {
                        Some(array) => Some(array.into_array_type()),
                        None => None,
                    },
                    client_rank: get_client_rank(),
                });
            }
            Some(params)
        }
    };
    create_obj_params
}

pub fn create_objects_resp_proc(response: &mut RPCData) -> HandlerResult {
    debug!(
        "Processing response: data length: {:?}",
        response.data.as_ref().unwrap().len()
    );

    let result_metadata = response.metadata.as_mut().unwrap();
    let handler_result = result_metadata.handler_result.as_ref().unwrap();
    handler_result.to_owned()
}
