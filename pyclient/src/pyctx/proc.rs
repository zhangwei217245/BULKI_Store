use crate::pyctx::converter;
use anyhow::Result;
use client::cltctx::get_client_rank;
use commons::object::types::SupportedRustArrayD;
use commons::object::{
    objid::GlobalObjectIdExt,
    params::{
        CreateObjectParams, GetObjectMetaParams, GetObjectSliceParams, GetObjectSliceResponse,
        SerializableMetaKeySpec,
    },
    types::{ObjectIdentifier, SerializableSliceInfoElem},
};
use log::debug;
use pyo3::{
    types::{PyDict, PySlice},
    Bound,
};
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use std::time::{SystemTime, UNIX_EPOCH};
// use rayon::prelude::*;

use crate::pyctx::converter::SupportedNumpyArray;

fn generate_random_string() -> String {
    let pid = std::process::id();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time should never be before UNIX_EPOCH")
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
    data: Option<SupportedNumpyArray<'py>>,
    sub_obj_meta_list: Option<Vec<Option<Bound<'py, PyDict>>>>,
    sub_obj_data_list: Option<Vec<Option<SupportedNumpyArray<'py>>>>,
) -> Result<Vec<CreateObjectParams>> {
    // Convert single metadata dict
    let major_metadata = {
        let converted = converter::convert_metadata(Some(vec![metadata]))?
            .and_then(|mut vec| vec.pop())
            .flatten();
        converted
    };

    // Get the name from metadata or generate a random one
    let main_obj_name = major_metadata
        .as_ref()
        .and_then(|x| x.get(obj_name_key.as_str()).map(|v| v.to_string()))
        .unwrap_or(format!("obj_{}", generate_random_string()));

    let main_obj_id = commons::object::objid::GlobalObjectId::with_vnode_id(
        main_obj_name.as_str(),
        parent_id.map(|id| id.vnode_id()),
    )
    .to_u128();

    let major_data = match data {
        Some(array) => {
            let converted = array.into_array_type();
            Some(converted)
        }
        None => None,
    };

    let sub_obj_meta_list = {
        let converted = { converter::convert_metadata(sub_obj_meta_list)? };
        converted
    };

    let create_obj_params: Result<Vec<CreateObjectParams>> = match sub_obj_data_list {
        // no array data, this must be a container object
        None => Ok(vec![CreateObjectParams {
            obj_id: main_obj_id,
            obj_name: main_obj_name.clone(),
            obj_name_key: obj_name_key.clone(),
            parent_id: parent_id,
            initial_metadata: major_metadata,
            array_data: major_data,
            client_rank: get_client_rank(),
        }]),
        Some(array_vec) => {
            let vec_len = array_vec.len();
            let mut params = Vec::with_capacity(vec_len + 1);

            // Step 1: create the major object first
            let main_object = CreateObjectParams {
                obj_id: main_obj_id,
                obj_name: main_obj_name.clone(),
                obj_name_key: obj_name_key.clone(),
                parent_id: parent_id,
                initial_metadata: major_metadata,
                array_data: major_data,
                client_rank: get_client_rank(),
            };
            params.push(main_object);

            // Step 2: create the sub-objects
            for (i, array) in array_vec.into_iter().enumerate() {
                let sub_metadata = match sub_obj_meta_list.as_ref() {
                    Some(map_list) => map_list[i].to_owned(),
                    None => None,
                };

                let sub_obj_name = sub_metadata
                    .as_ref()
                    .and_then(|x| x.get(obj_name_key.as_str()).map(|v| v.to_string()))
                    .unwrap_or(format!("{}/{}", main_obj_name, i));

                let obj_id = commons::object::objid::GlobalObjectId::with_vnode_id(
                    sub_obj_name.as_str(),
                    Some(parent_id.unwrap_or_else(|| main_obj_id).vnode_id()),
                )
                .to_u128();

                params.push(CreateObjectParams {
                    obj_id,
                    obj_name: sub_obj_name,
                    obj_name_key: obj_name_key.clone(),
                    parent_id: Some(parent_id.unwrap_or_else(|| main_obj_id)),
                    initial_metadata: sub_metadata,
                    array_data: match array {
                        Some(array) => Some(array.into_array_type()),
                        None => None,
                    },
                    client_rank: get_client_rank(),
                });
            }
            Ok(params)
        }
    };
    create_obj_params
}

pub fn get_object_slice_req_proc<'py>(
    obj_id: ObjectIdentifier,
    region: Option<Vec<Bound<'py, PySlice>>>,
    sub_obj_regions: Option<Vec<(String, Vec<Bound<'py, PySlice>>)>>,
) -> Result<GetObjectSliceParams> {
    debug!(
        "get_object_slice_req_proc: obj_id: {:?}, region: {:?}, sub_obj_regions: {:?}",
        obj_id, region, sub_obj_regions
    );

    // Convert main region
    let region_slices = match region {
        Some(r) => Some(converter::convert_pyslice_vec_to_rust_slice_vec(
            r.len(),
            Some(r),
        )?),
        None => None,
    };

    // Convert to serializable form
    let serializable_region = region_slices.map(|slices| {
        slices
            .into_iter()
            .map(SerializableSliceInfoElem::from)
            .collect()
    });

    // Convert sub-object regions
    let serializable_sub_regions = sub_obj_regions
        .map(|sub_regions| {
            sub_regions
                .into_iter()
                .map(|(name, slices)| {
                    let slice_elems = converter::convert_pyslice_vec_to_rust_serde_slice_vec(
                        slices.len(),
                        Some(slices),
                    );
                    Ok((
                        name,
                        match slice_elems {
                            Ok(slice_elems) => Some(slice_elems),
                            Err(_) => None,
                        },
                    ))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()
        .map_err(|e| anyhow::anyhow!("Failed to process sub-object regions: {}", e))?;

    let get_object_data_params = GetObjectSliceParams {
        obj_id,
        region: serializable_region,
        sub_obj_regions: serializable_sub_regions,
    };

    Ok(get_object_data_params)
}

pub fn get_object_metadata_req_proc<'py>(
    obj_id: ObjectIdentifier,
    meta_keys: Option<Vec<String>>,
    sub_meta_keys: Option<converter::MetaKeySpec>,
) -> Result<GetObjectMetaParams> {
    debug!(
        "get_object_metadata_req_proc: obj_id: {:?}, meta_keys: {:?}, sub_meta_keys: {:?}",
        obj_id, meta_keys, sub_meta_keys
    );
    Ok(GetObjectMetaParams {
        obj_id,
        meta_keys,
        sub_meta_keys: match sub_meta_keys {
            Some(converter::MetaKeySpec::Simple(keys)) => {
                Some(SerializableMetaKeySpec::Simple(keys))
            }
            Some(converter::MetaKeySpec::WithObject(map)) => {
                Some(SerializableMetaKeySpec::WithObject(map))
            }
            None => None,
        },
    })
}

pub fn gen_sim_data(
    get_object_slice_params: GetObjectSliceParams,
) -> Result<GetObjectSliceResponse> {
    // Use the current timestamp as a seed for deterministic but varying data
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut rng = StdRng::seed_from_u64(seed);

    // Calculate shape from region if provided
    let shape = if let Some(region) = &get_object_slice_params.region {
        calculate_shape_from_region(region)
    } else {
        // Default shape if no region is provided
        vec![2, 2]
    };

    // Generate random array data
    let array_data = generate_random_array(&shape, &mut rng);

    // Generate sub-object data if requested
    let sub_obj_slices = if let Some(sub_regions) = &get_object_slice_params.sub_obj_regions {
        let mut sub_slices = Vec::new();

        for (sub_name, sub_region) in sub_regions {
            // Generate a unique sub-object ID
            let sub_obj_id = rng.random::<u128>();

            // Calculate shape for sub-object
            let sub_shape = if let Some(region) = sub_region {
                calculate_shape_from_region(region)
            } else {
                // Default shape for sub-objects
                vec![2, 2]
            };

            // Generate random data for sub-object
            let sub_data = generate_random_array(&sub_shape, &mut rng);

            sub_slices.push((sub_obj_id, sub_name.clone(), Some(sub_data)));
        }

        Some(sub_slices)
    } else {
        None
    };

    // Create response
    Ok(GetObjectSliceResponse {
        obj_id: get_object_slice_params.obj_id.vnode_id() as u128,
        obj_name: format!("sim_obj_{}", get_object_slice_params.obj_id.vnode_id()),
        array_slice: Some(array_data),
        sub_obj_slices,
    })
}

// Helper function to calculate shape from region slice information
fn calculate_shape_from_region(region: &[SerializableSliceInfoElem]) -> Vec<usize> {
    let mut shape = Vec::new();

    for elem in region {
        match elem {
            SerializableSliceInfoElem::Slice { start, end, step } => {
                let size = if *step > 0 {
                    (end.unwrap_or(start + 1) - start + step - 1) / step
                } else if *step < 0 {
                    (start - end.unwrap_or(start + 1) + (-step) - 1) / (-step)
                } else {
                    // Step is 0, which is invalid
                    1
                };
                shape.push(size.max(0) as usize);
            }
            SerializableSliceInfoElem::Index(_) => {
                // Index doesn't contribute to shape
            }
            SerializableSliceInfoElem::NewAxis => {
                shape.push(1);
            }
        }
    }

    // Ensure we have at least a 2D array
    if shape.is_empty() {
        shape.push(1);
        shape.push(1);
    } else if shape.len() == 1 {
        shape.push(1);
    }

    shape
}

// Helper function to generate random array data
fn generate_random_array(shape: &[usize], rng: &mut impl rand::Rng) -> SupportedRustArrayD {
    use commons::object::types::SupportedRustArrayD;
    use ndarray::{ArrayD, IxDyn};

    // Calculate total size
    let size: usize = shape.iter().product();

    // Create a dynamic dimension
    let dim = IxDyn(&shape);

    // Generate random f64 data
    let mut data = Vec::with_capacity(size);
    for _ in 0..size {
        data.push(rng.random::<f64>());
    }

    // Create array and return
    let array = ArrayD::from_shape_vec(dim, data).unwrap();
    SupportedRustArrayD::Float64(array)
}
