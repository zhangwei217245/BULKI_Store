pub mod objid;
pub mod params;
pub mod types;
use dashmap::DashMap;
use log::{debug, info, warn};
use ndarray::SliceInfoElem;

use anyhow::Result;
use objid::GlobalObjectIdExt;
use params::CreateObjectParams;
use rayon::prelude::*;
use rmp_serde::encode;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
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
            metadata: params.initial_metadata.unwrap_or(HashMap::new()),
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
            (Some(arr), Some(region)) => Some(arr.slice_into_array_d(&region)),
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

    pub fn save_to_file(&self, dir_path: &str) -> std::io::Result<()> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(dir_path)?;
        let mut file = File::create(&format!("{}/{}.obj", dir_path, self.id))?;
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

/// Header structure for checkpoint files
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DSCheckpointHeader {
    pub version: u32,
    pub timestamp: u64,
    pub rank: u32,
    pub size: u32,
    pub object_count: u64,
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
        let look_up_result = self
            .name_obj_idx
            .get(&obj_name)
            .map(|id| id.value().to_owned());
        if look_up_result.is_some() {
            debug!(
                "Object name already exists: {} , returning the corresponding ID",
                obj_name
            );
            return look_up_result.ok_or_else(|| anyhow::Error::msg("Failed to get object ID"));
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
    pub fn get_obj_owned(&self, id: u128) -> Option<DataObject> {
        self.objects.get(&id).map(|entry| entry.clone())
    }

    pub fn get_obj_ref<'a>(&'a self, id: &u128) -> Option<impl Deref<Target = DataObject> + 'a> {
        self.objects.get(id)
    }

    pub fn get_obj_ref_mut<'a>(
        &'a self,
        id: &u128,
    ) -> Option<impl DerefMut<Target = DataObject> + 'a> {
        self.objects.get_mut(id)
    }

    /// Retrieve a DataObject by its name.
    pub fn get_obj_id_by_name(&self, name: &str) -> Option<u128> {
        self.name_obj_idx
            .get(name)
            .map(|reference| reference.value().to_owned())
    }

    pub fn get_named_obj_metadata(
        &self,
        obj_name: &str,
        keys: Vec<&str>,
    ) -> Option<HashMap<String, MetadataValue>> {
        self.name_obj_idx.get(obj_name).and_then(|reference| {
            self.objects
                .get(reference.value())
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

    /// get the obj_children by names
    pub fn get_sub_obj_metadata_by_names(
        &self,
        main_obj_id: u128,
        sub_obj_meta_keys: Option<&HashMap<String, Vec<String>>>,
    ) -> Option<Vec<(u128, String, HashMap<String, MetadataValue>)>> {
        match sub_obj_meta_keys {
            Some(obj_meta_keys) => {
                let children_name_index = &self.objects.get(&main_obj_id)?.child_name_idx;
                match children_name_index {
                    Some(index) => {
                        let mut result = Vec::new();
                        for (name, keys) in obj_meta_keys {
                            if let Some(id) = index.get(name) {
                                result.push((
                                    id.to_owned(),
                                    name.to_owned(),
                                    self.get_obj_metadata(
                                        id.to_owned(),
                                        keys.iter().map(|k| k.as_str()).collect(),
                                    )?
                                    .1,
                                ));
                            }
                        }
                        Some(result)
                    }
                    None => None,
                }
            }
            None => None,
        }
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
        self.get_obj_ref(&id)
            .and_then(|obj| obj.get_array_slice(region))
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
    ) -> Vec<(u128, String, Option<SupportedRustArrayD>)> {
        let mut results: Vec<_> = obj_regions
            .into_par_iter()
            .enumerate()
            .map(|(idx, (id, region))| {
                let result = match self.get_obj_ref(&id) {
                    Some(obj) => (id, obj.name.clone(), obj.get_array_slice(region)),
                    None => (id, "".to_string(), None),
                };
                (idx, result)
            })
            .collect();

        // Sort by the original index to restore input order
        results.sort_by_key(|(idx, _)| *idx);

        // Remove the index from the results
        results.into_iter().map(|(_, result)| result).collect()
    }

    /// Remove a DataObject from the store by its u128 ID.
    pub fn remove(&self, id: u128) -> Option<DataObject> {
        self.objects.remove(&id).map(|(_k, v)| v)
    }

    /// Dump all DataObjects to a single file in MessagePack format.
    /// Uses the object ID's built-in vnode_id for distribution when loading.
    pub fn dump_memorystore_to_file(&self, _rank: u32, _size: u32) -> Result<usize, anyhow::Error> {
        info!("[R{}/S{}] Dumping memory store to file", _rank, _size);
        // read env var "PDC_DATA_LOC" and use it as the path, the default value should be "./.bulkistore_data"
        let data_dir = std::env::var("PDC_DATA_LOC").unwrap_or("./.bulkistore_data".to_string());
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&data_dir)?;

        let total_objects = self.objects.len();
        if total_objects == 0 {
            info!("[R{}/S{}] No objects to save", _rank, _size);
            return Ok(0);
        }

        // Create a single file with timestamp to avoid conflicts
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let filename = format!("{}/objects_{}_{}.oset", data_dir, timestamp, _rank);

        info!(
            "[R{}/S{}] Saving {} objects to {}",
            _rank, _size, total_objects, filename
        );

        let file = std::fs::File::create(&filename)?;
        let mut writer = std::io::BufWriter::new(file);

        // Write file header with metadata
        let header = DSCheckpointHeader {
            version: 1,
            timestamp,
            rank: _rank,
            size: _size,
            object_count: total_objects as u64,
        };

        rmp_serde::encode::write(&mut writer, &header)?;

        let mut saved_count = 0;
        let progress_interval = std::cmp::max(1, total_objects / 100); // Report progress every 1%

        // Simply save each (id, object) pair
        self.objects.iter().enumerate().for_each(|(i, obj)| {
            // Serialize the ID and object
            let id = obj.id;
            let result: Result<(), anyhow::Error> = (|| {
                rmp_serde::encode::write(&mut writer, &id)?;
                rmp_serde::encode::write(&mut writer, &obj)?;
                Ok(())
            })();

            if let Err(e) = result {
                warn!(
                    "[R{}/S{}] Failed to save object {}: {}",
                    _rank, _size, id, e
                );
            } else {
                saved_count += 1;
                if i % progress_interval == 0 || i == total_objects - 1 {
                    let progress = (i as f64 + 1.0) * 100.0 / total_objects as f64;
                    info!(
                        "[R{}/S{}] Saved {}/{} objects ({:.1}%)",
                        _rank,
                        _size,
                        i + 1,
                        total_objects,
                        progress
                    );
                }
            }
        });

        // Flush the writer to ensure all data is written
        writer.flush()?;

        info!(
            "[R{}/S{}] Successfully saved {}/{} objects to {}",
            _rank, _size, saved_count, total_objects, filename
        );

        Ok(saved_count)
    }

    /// Load DataObjects from MessagePack files and populate the DataStore.
    /// Uses the object ID's built-in vnode_id for determining which objects to load.
    pub fn load_memorystore_from_file(
        &self,
        server_rank: u32,
        server_count: u32,
    ) -> Result<usize, anyhow::Error> {
        // read env var "PDC_DATA_LOC" and use it as the path, the default value should be "./.bulkistore_data"
        let data_dir = std::env::var("PDC_DATA_LOC").unwrap_or("./.bulkistore_data".to_string());

        // check if data_dir exists
        if !std::path::Path::new(&data_dir).exists() {
            // Create directory if it doesn't exist
            match std::fs::create_dir_all(&data_dir) {
                Ok(_) => info!(
                    "[R{}/S{}] Created data directory for PDC_DATA_LOC: {}",
                    server_rank, server_count, data_dir
                ),
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to create data directory for PDC_DATA_LOC: {}",
                        e
                    ))
                }
            }
        } else {
            info!(
                "[R{}/S{}] Data directory already exists for PDC_DATA_LOC: {}",
                server_rank, server_count, data_dir
            );
        }

        // Find all .bulki files in the directory
        let mut checkpoint_files: Vec<std::path::PathBuf> = Vec::new();
        for entry in std::fs::read_dir(&data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("oset") {
                checkpoint_files.push(path);
            }
        }

        if checkpoint_files.is_empty() {
            info!(
                "[R{}/S{}] No checkpoint files found in {}",
                server_rank, server_count, data_dir
            );
            return Ok(0);
        } else {
            info!(
                "[R{}/S{}] Found {} checkpoint files in {}",
                server_rank,
                server_count,
                checkpoint_files.len(),
                data_dir
            );
        }

        // Sort files by timestamp (newest first) to load the most recent checkpoint
        checkpoint_files.sort_by(|a, b| {
            let a_name = a.file_name().unwrap_or_default().to_string_lossy();
            let b_name = b.file_name().unwrap_or_default().to_string_lossy();
            b_name.cmp(&a_name) // Reverse order for newest first
        });

        info!(
            "[R{}/S{}] Found {} checkpoint files, loading from newest",
            server_rank,
            server_count,
            checkpoint_files.len()
        );

        let total_loaded: usize = (0..checkpoint_files.len())
            .into_par_iter()
            .map(|i| {
                let file_path =
                    &checkpoint_files[(i + server_rank as usize) % checkpoint_files.len()];
                // Process file_path
                self.process_checkpoint_file(file_path, server_rank, server_count)
            })
            .sum();

        info!(
            "[R{}/S{}] Total objects loaded: {}",
            server_rank, server_count, total_loaded
        );

        Ok(total_loaded)
    }

    fn process_checkpoint_file(
        &self,
        file_path: &PathBuf,
        server_rank: u32,
        server_count: u32,
    ) -> usize {
        let file_name = file_path.file_name().unwrap_or_default().to_string_lossy();
        let file = std::fs::File::open(&file_path).unwrap();
        let mut reader = std::io::BufReader::with_capacity(100 * 1024 * 1024, file);

        // Read and validate the header
        let header: Result<DSCheckpointHeader, _> = rmp_serde::decode::from_read(&mut reader);

        match header {
            Ok(header) => {
                info!(
                            "[R{}/S{}] Processing checkpoint file: {}, version: {}, from rank {}/{}, with {} objects",
                            server_rank, server_count, file_name, header.version, header.rank, header.size, header.object_count
                        );

                if header.object_count == 0 {
                    return 0;
                }

                // Track progress
                let mut objects_processed = 0;
                let mut objects_loaded = 0;
                let progress_interval = std::cmp::max(1, header.object_count as usize / 100);

                // Read and process each object
                loop {
                    // Read object ID
                    let id_result: Result<u128, _> = rmp_serde::decode::from_read(&mut reader);

                    match id_result {
                        Ok(id) => {
                            // Read the corresponding object
                            let obj_result: Result<DataObject, _> =
                                rmp_serde::decode::from_read(&mut reader);

                            match obj_result {
                                Ok(obj) => {
                                    objects_processed += 1;

                                    // Use the vnode_id from the object ID to determine if this server should load it
                                    let vnode_id = id.vnode_id();
                                    let should_load = vnode_id % server_count == server_rank;

                                    if should_load {
                                        // Insert the object into our store
                                        let obj_name = obj.name.clone();

                                        self.objects.insert(id, obj);
                                        if !obj_name.is_empty() {
                                            self.name_obj_idx.insert(obj_name, id);
                                        }

                                        objects_loaded += 1;
                                    }

                                    // Report progress periodically
                                    if objects_processed % (progress_interval * 5) == 0
                                        || objects_processed == header.object_count as usize
                                    {
                                        let progress = (objects_processed as f64 * 100.0)
                                            / header.object_count as f64;
                                        info!(
                                                    "[R{}/S{}] Processed {}/{} objects ({:.1}%), loaded {} objects",
                                                    server_rank, server_count, objects_processed, header.object_count, progress, objects_loaded
                                                );
                                    }

                                    // Check if we've processed all objects in this file
                                    if objects_processed >= header.object_count as usize {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "[R{}/S{}] Error reading object from {}: {}",
                                        server_rank, server_count, file_name, e
                                    );
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            if objects_processed >= header.object_count as usize {
                                // We've read all objects, this is expected EOF
                                break;
                            } else {
                                // Unexpected error
                                warn!(
                                    "[R{}/S{}] Error reading object ID from {}: {}",
                                    server_rank, server_count, file_name, e
                                );
                                break;
                            }
                        }
                    }
                }

                info!(
                    "[R{}/S{}] Completed loading from {}: processed {} objects, loaded {} objects",
                    server_rank, server_count, file_name, objects_processed, objects_loaded
                );

                objects_loaded as usize
            }
            Err(e) => {
                warn!(
                    "[R{}/S{}] Failed to read header from {}: {}",
                    server_rank, server_count, file_name, e
                );
                0
            }
        }
    }
}
