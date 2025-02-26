pub mod objid;
pub mod params;
pub mod types;
use dashmap::DashMap;
use ndarray::SliceInfoElem;

use anyhow::Result;
use params::CreateObjectParams;
use rmp_serde::encode;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::path::Path;
use types::{MetadataValue, SupportedRustArrayD};

/// A DataObject that can own an NDArray of various numeric types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataObject {
    /// Unique identifier (UUIDv7 as u128).
    pub id: u128,
    /// parent id
    pub parent_id: Option<u128>,
    /// A human-readable name.
    pub name: String,
    /// The key of the object name in the metadata dictionary.
    pub obj_name_key: String,
    /// Optional attached NDArray of any supported type.
    pub array: Option<SupportedRustArrayD>,
    /// Arbitrary metadata attributes.
    pub metadata: HashMap<String, MetadataValue>,
    /// Nested child DataObjects.
    pub children: Option<HashSet<(u128, String)>>,
    /// The name to ID index for all children.
    pub child_name_idx: Option<HashMap<String, u128>>,
}

impl DataObject {
    /// Create a new DataObject with the given parameters.
    /// NOTE: The id field will be set **by the server** during object creation.
    pub fn new(params: CreateObjectParams) -> Self {
        DataObject {
            // Use a temporary ID that will be replaced by the server
            id: params.obj_id,
            parent_id: params.parent_id,
            obj_name_key: params.obj_name_key,
            name: params.obj_name,
            array: params.array_data,
            metadata: params.initial_metadata.unwrap_or_default(),
            children: Some(HashSet::new()),
            child_name_idx: Some(HashMap::new()),
        }
    }

    /// Attach an NDArray to this DataObject.
    pub fn attach_array(&mut self, array: SupportedRustArrayD) {
        self.array = Some(array);
    }

    /// Add a child DataObject.
    pub fn add_child(&mut self, obj_name: String, child_id: u128) {
        self.children
            .as_mut()
            .unwrap()
            .insert((child_id, obj_name.clone()));
        self.child_name_idx
            .as_mut()
            .unwrap()
            .insert(obj_name, child_id);
    }

    /// add a group of children.
    pub fn add_children(&mut self, obj_name_child_ids: Vec<(String, u128)>) {
        for (obj_name, child_id) in obj_name_child_ids {
            self.add_child(obj_name, child_id);
        }
    }

    pub fn get_child_id_by_name(&self, obj_name: &str) -> Option<u128> {
        self.child_name_idx.as_ref().unwrap().get(obj_name).cloned()
    }

    pub fn get_child_ids_by_names(&self, obj_names: Vec<&str>) -> Vec<u128> {
        obj_names
            .iter()
            .map(|name| self.child_name_idx.as_ref().unwrap()[*name])
            .collect()
    }

    /// get all child ids.
    pub fn get_children_ids(&self) -> Vec<u128> {
        self.children
            .as_ref()
            .unwrap()
            .iter()
            .map(|(id, _)| id.to_owned())
            .collect()
    }

    // remove a child
    pub fn remove_child(&mut self, child_id: u128) {
        self.children
            .as_mut()
            .unwrap()
            .retain(|(id, _)| *id != child_id);
        self.child_name_idx
            .as_mut()
            .unwrap()
            .retain(|_, id| *id != child_id);
    }

    /// Get a slice of the attached NDArray.
    pub fn get_array_slice(
        &self,
        region: Option<Vec<SliceInfoElem>>,
    ) -> Option<SupportedRustArrayD> {
        match (self.array.as_ref(), region) {
            (Some(arr), Some(region)) => Some(arr.slice(&region)),
            (Some(arr), None) => Some(arr.clone()),
            _ => None,
        }
    }

    /// Add or update a metadata attribute.
    pub fn set_metadata(&mut self, key: impl Into<String>, value: MetadataValue) {
        self.metadata.insert(key.into(), value);
    }

    /// Add or update multiple metadata attributes into existing map.
    pub fn set_metadata_map(&mut self, metadata_col: HashMap<String, MetadataValue>) {
        for (key, value) in metadata_col {
            self.metadata.insert(key, value);
        }
    }

    /// get a group of metadata by keys.
    pub fn get_metadata_map(&self, keys: Vec<&str>) -> Option<HashMap<String, MetadataValue>> {
        let mut map = HashMap::new();
        for key in keys {
            if let Some(value) = self.metadata.get(key) {
                map.insert(key.to_string(), value.clone());
            }
        }
        Some(map)
    }

    /// retrieve metadata attribute by key.
    pub fn get_metadata(&self, key: &str) -> Option<MetadataValue> {
        self.metadata.get(key).cloned()
    }

    pub fn save_to_file(&self, path: &str) -> std::io::Result<()> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&path)?;
        // file name format should be {data_dir}/{id}.obj
        let filename = format!("{}/{}.obj", path, self.id);
        let mut file = File::create(filename)?;
        encode::write(&mut file, &self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn load_from_file(filename: &str) -> Result<DataObject, anyhow::Error> {
        let file = File::open(&filename)?;
        match rmp_serde::decode::from_read::<_, DataObject>(file) {
            Ok(obj) => Ok(obj),
            Err(e) => {
                eprintln!("Error reading {:?}: {}", &filename, e);
                return Err(anyhow::Error::msg(e));
            }
        }
    }
}

/// A concurrent DataStore that indexes DataObjects by their u128 IDs using DashMap.
pub struct DataStore {
    pub objects: DashMap<u128, DataObject>,
    pub name_obj_idx: DashMap<String, u128>,
}
impl DataStore {
    /// Create a new, empty DataStore.
    pub fn new() -> Self {
        DataStore {
            objects: DashMap::new(),
            name_obj_idx: DashMap::new(),
        }
    }

    /// Insert or update a DataObject in the store.
    pub fn insert(&self, obj: DataObject) -> Result<u128> {
        let obj_id = obj.id;
        let parent_obj_id = obj.parent_id;
        let obj_name = obj.name.clone();
        // validate if obj_name exists
        if self.name_obj_idx.contains_key(&obj_name) {
            return Err(anyhow::Error::msg("Object name already exists"));
        }
        // save object
        self.objects.insert(obj_id, obj);
        // add name to obj index
        self.name_obj_idx.insert(obj_name.clone(), obj_id);
        // add to parent child index of parent object
        if let Some(parent_id) = parent_obj_id {
            if parent_id == obj_id {
                return Err(anyhow::Error::msg(
                    "Parent and child objects cannot have the same ID",
                ));
            }
            if let Some(mut parent_obj) = self.objects.get_mut(&parent_id) {
                parent_obj.add_child(obj_name.clone(), obj_id);
                Ok(obj_id)
            } else {
                Err(anyhow::Error::msg("Parent object not found"))
            }
        } else {
            Ok(obj_id)
        }
    }

    /// Retrieve a DataObject by its u128 ID.
    pub fn get(&self, id: u128) -> Option<DataObject> {
        self.objects.get(&id).map(|entry| entry.clone())
    }

    pub fn get_named_obj_metadata(
        &self,
        obj_name: &str,
        keys: Vec<&str>,
    ) -> Option<HashMap<String, MetadataValue>> {
        self.name_obj_idx.get(obj_name).and_then(|reference| {
            self.get(reference.value().to_owned())
                .map(|obj| obj.get_metadata_map(keys))
        })?
    }

    /// attach or update metadata of a DataObject by id
    pub fn set_metadata(&mut self, id: u128, metadata: Vec<(String, MetadataValue)>) {
        for (key, value) in metadata {
            self.objects.get_mut(&id).unwrap().set_metadata(key, value);
        }
    }

    /// get the children of an object
    pub fn get_obj_children(&self, id: u128) -> Option<Vec<(u128, String)>> {
        // TODO: to check the case when the specified object does not exist
        Some(
            self.objects
                .get(&id)?
                .children
                .as_ref()?
                .iter()
                .map(|(id, name)| (id.to_owned(), name.to_owned()))
                .collect(),
        )
    }

    /// get a group of metadata by keys.
    pub fn get_obj_metadata(
        &self,
        id: u128,
        keys: Vec<&str>,
    ) -> Option<(String, HashMap<String, MetadataValue>)> {
        self.objects.get(&id).map(|obj| {
            (
                obj.name.clone(),
                obj.get_metadata_map(keys).unwrap_or(HashMap::new()),
            )
        })
    }

    /// Retrieve a slice from the NDArray attached to a DataObject identified by its u128 ID.
    pub fn get_object_slice(
        &self,
        id: u128,
        region: Option<Vec<SliceInfoElem>>,
    ) -> Option<SupportedRustArrayD> {
        self.get(id)?.get_array_slice(region)
    }

    /// Retrieve slices from multiple arrays, each with its own slice pattern
    ///
    /// # Arguments
    /// * `obj_regions` - Vector of tuples containing (object_id, slice_pattern)
    ///
    /// # Returns
    /// * Vector of array slices, in the same order as the input
    pub fn get_regions_by_obj_ids(
        &self,
        obj_regions: Vec<(u128, Option<Vec<SliceInfoElem>>)>,
    ) -> Vec<(u128, Option<String>, Option<SupportedRustArrayD>)> {
        obj_regions
            .into_iter()
            .map(|(id, region)| match self.get(id) {
                Some(obj) => (id, Some(obj.name), self.get_object_slice(id, region)),
                None => (id, None, None),
            })
            .collect()
    }

    /// Remove a DataObject from the store by its u128 ID.
    pub fn remove(&self, id: u128) -> Option<DataObject> {
        self.objects.remove(&id).map(|(_k, v)| v)
    }

    /// Dump all DataObjects to a file in MessagePack format.
    pub fn dump_memorystore_to_file(&self) -> Result<(), anyhow::Error> {
        // read env var "PDC_DATA_DIR" and use it as the path, the default value should be "./data"
        let data_dir = std::env::var("PDC_DATA_DIR").unwrap_or("./data".to_string());
        // scan data_dir and load every file with .obj extension
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&data_dir)?;
        // file name format should be {data_dir}/objects_id.obj
        let objs: Vec<DataObject> = self.objects.iter().map(|entry| entry.clone()).collect();
        for obj in objs {
            obj.save_to_file(&data_dir)?;
        }
        // write name_obj_idx to file
        let filename = format!("{}/name_obj.idx", data_dir);
        let mut file = File::create(filename)?;
        let name_idx_map: HashMap<String, u128> = self
            .name_obj_idx
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        encode::write(&mut file, &name_idx_map)?;
        Ok(())
    }

    /// Load DataObjects from a MessagePack file and populate the DataStore.
    pub fn load_memorystore_from_file(&self) -> Result<(), anyhow::Error> {
        // read env var "PDC_DATA_DIR" and use it as the path, the default value should be "./data"
        let data_dir = std::env::var("PDC_DATA_DIR").unwrap_or("./data".to_string());

        // test if data_dir exists
        if !Path::new(&data_dir).exists() {
            return Err(anyhow::Error::msg(format!(
                "Data directory {} does not exist",
                data_dir
            )));
        }

        // Walk through the directory and find all .obj files
        for entry in std::fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();

            // check if it is idx file and we can load the name_obj.idx file
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("idx") {
                // Open and read the file
                let name_idx_map: HashMap<String, u128> =
                    rmp_serde::decode::from_read(File::open(path.to_str().unwrap())?)?;
                for (name, id) in name_idx_map {
                    self.name_obj_idx.insert(name, id);
                }
                continue;
            }

            // Check if it's a file and has .obj extension
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("obj") {
                // Open and read the file
                let obj = DataObject::load_from_file(path.to_str().unwrap())?;
                let obj_id = obj.id;
                let obj_name = obj.name.clone();
                // insert into objects and name_obj_idx
                self.objects.insert(obj_id, obj);
                self.name_obj_idx.insert(obj_name, obj_id);
            }
        }
        Ok(())
    }
}
