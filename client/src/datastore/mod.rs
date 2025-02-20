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
    metadata: Option<Vec<Option<Bound<'py, PyDict>>>>,
    array_data: Option<Vec<Option<SupportedNumpyArray<'py>>>>,
) -> Option<Vec<CreateObjectParams>> {
    let metadata_map = crate::pyctx::converter::convert_metadata(metadata).unwrap();
    // let mut create_obj_params: Option<Vec<CreateObjectParams>> = None;
    // the way we acquire the object id here is to guarantee the colocation of the sub-objects.
    // if you need to fully distribute all the objects, you should leave parent_id as None
    let main_obj_id = commons::object::objid::GlobalObjectId::with_vnode_id(
        &name,
        parent_id.map(|id| id.vnode_id()),
    )
    .to_u128();
    let create_obj_params: Option<Vec<CreateObjectParams>> = match array_data {
        // no array data, this must be a container object
        None => Some(vec![CreateObjectParams {
            obj_id: main_obj_id,
            name,
            parent_id,
            initial_metadata: match metadata_map.as_ref() {
                Some(map_list) => map_list[0].to_owned(),
                None => None,
            },
            array_data: None,
            client_rank: get_client_rank(),
        }]),
        Some(array_vec) => {
            let mut params = Vec::with_capacity(array_vec.len());
            let mut obj_id = main_obj_id;
            // with the above, the object id is guaranteed to be colocated with the parent object on the same virtual node.
            // and even for the sub-objects below, since we take the vnode_id from master object, they will be colocated as well.
            // this will ensure that : regardless of the existence of parent_id, the sub-objects and major object will be colocated.
            let vec_len = array_vec.len();
            for (i, array) in array_vec.into_iter().enumerate() {
                params.push(CreateObjectParams {
                    obj_id,
                    name: format!("{}-{}", name, i),
                    parent_id: Some(parent_id.unwrap_or_else(|| main_obj_id)),
                    initial_metadata: match metadata_map.as_ref() {
                        Some(map_list) => map_list[i].to_owned(),
                        None => None,
                    },
                    array_data: match array {
                        Some(array) => Some(array.into_array_type()),
                        None => None,
                    },
                    client_rank: get_client_rank(),
                });
                if i == vec_len - 1 {
                    break;
                }
                obj_id = commons::object::objid::GlobalObjectId::with_vnode_id(
                    &name,
                    Some(main_obj_id.vnode_id()),
                )
                .to_u128();
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
