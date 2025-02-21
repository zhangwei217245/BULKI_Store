use crate::cltctx::get_client_rank;
use commons::{
    handler::HandlerResult,
    object::{objid::GlobalObjectIdExt, params::CreateObjectParams},
    rpc::RPCData,
};
use log::debug;
use pyo3::{types::PyDict, Bound};

use crate::pyctx::converter::SupportedNumpyArray;

pub fn create_objects_req_proc<'py>(
    name: String,
    parent_id: Option<u128>,
    metadata: Option<Bound<'py, PyDict>>,
    array_meta_list: Option<Vec<Option<Bound<'py, PyDict>>>>,
    array_data_list: Option<Vec<Option<SupportedNumpyArray<'py>>>>,
    sub_object_key: Option<String>,
) -> Option<Vec<CreateObjectParams>> {
    let major_metadata = crate::pyctx::converter::convert_metadata(Some(vec![metadata])).unwrap();
    let sub_obj_meta_list = crate::pyctx::converter::convert_metadata(array_meta_list).unwrap();
    // let mut create_obj_params: Option<Vec<CreateObjectParams>> = None;
    // the way we acquire the object id here is to guarantee the colocation of the sub-objects.
    // if you need to fully distribute all the objects, you should leave parent_id as None
    let main_obj_id = commons::object::objid::GlobalObjectId::with_vnode_id(
        &name,
        parent_id.map(|id| id.vnode_id()),
    )
    .to_u128();
    let create_obj_params: Option<Vec<CreateObjectParams>> = match array_data_list {
        // no array data, this must be a container object
        None => Some(vec![CreateObjectParams {
            obj_id: main_obj_id,
            name,
            parent_id,
            initial_metadata: match major_metadata.as_ref() {
                Some(map_list) => map_list[0].to_owned(),
                None => None,
            },
            array_data: None,
            client_rank: get_client_rank(),
        }]),
        Some(array_vec) => {
            let vec_len = array_vec.len();
            let mut params = Vec::with_capacity(vec_len + 1);
            // with the above, the object id is guaranteed to be colocated with the parent object on the same virtual node.
            // and even for the sub-objects below, since we take the vnode_id from master object, they will be colocated as well.
            // this will ensure that : regardless of the existence of parent_id, the sub-objects and major object will be colocated.

            // Step 1: create the major object first
            let major_object = CreateObjectParams {
                obj_id: main_obj_id,
                name: name.clone(),
                parent_id,
                initial_metadata: match major_metadata.as_ref() {
                    Some(map_list) => map_list[0].to_owned(),
                    None => None,
                },
                array_data: None,
                client_rank: get_client_rank(),
            };
            params.push(major_object);

            // Step 2: create the sub-objects
            for (i, array) in array_vec.into_iter().enumerate() {
                let metadata = match sub_obj_meta_list.as_ref() {
                    Some(map_list) => map_list[i].to_owned(),
                    None => None,
                };

                let sub_obj_name = match sub_object_key {
                    Some(ref sub_key) => match sub_key.as_str() {
                        _ => match metadata.as_ref() {
                            Some(map) => map[sub_key].to_string(),
                            None => format!("{}/{}", name, i),
                        },
                    },
                    None => format!("{}/{}", name, i),
                };

                let obj_id = commons::object::objid::GlobalObjectId::with_vnode_id(
                    &sub_obj_name,
                    Some(main_obj_id.vnode_id()),
                )
                .to_u128();

                params.push(CreateObjectParams {
                    obj_id,
                    name: sub_obj_name,
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
