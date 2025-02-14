use dashmap::DashMap;
use ndarray::{ArrayD, IxDyn, Slice, SliceInfo, SliceInfoElem};
use num_traits::Float;
use rmp_serde::{decode, encode};
use serde::{Deserialize, Serialize};
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::path::Path;
use uuid::Uuid; // Make sure your Cargo.toml enables the v7 feature, e.g.:
                // uuid = { version = "1.2", features = ["v7"] }

/// Represents various types of metadata values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataValue {
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    IntList(Vec<i64>),
    UIntList(Vec<u64>),
    FloatList(Vec<f64>),
    StringList(Vec<String>),
    /// For example, container-level "ranges": a list of (start, end) tuples.
    RangeList(Vec<(usize, usize)>),
}

/// Represents the different types of arrays that can be stored
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArrayType {
    // Floating point types
    Float32(ArrayD<f32>),
    Float64(ArrayD<f64>),
    // Signed integer types
    Int8(ArrayD<i8>),
    Int16(ArrayD<i16>),
    Int32(ArrayD<i32>),
    Int64(ArrayD<i64>),
    Int128(ArrayD<i128>),
    // Unsigned integer types
    UInt8(ArrayD<u8>),
    UInt16(ArrayD<u16>),
    UInt32(ArrayD<u32>),
    UInt64(ArrayD<u64>),
    UInt128(ArrayD<u128>),
}

impl ArrayType {
    /// Get a slice of the array, returning the same type
    pub fn slice(&self, region: &[Slice]) -> ArrayType {
        // Convert slices to SliceInfoElem
        let slice_info_elems: Vec<SliceInfoElem> =
            region.iter().map(|s| SliceInfoElem::from(*s)).collect();

        // Create SliceInfo
        let info = SliceInfo::<_, IxDyn, IxDyn>::try_from(slice_info_elems)
            .expect("Invalid slice pattern");

        match self {
            // Floating point types
            ArrayType::Float32(arr) => ArrayType::Float32(arr.slice(&info).to_owned()),
            ArrayType::Float64(arr) => ArrayType::Float64(arr.slice(&info).to_owned()),
            // Signed integer types
            ArrayType::Int8(arr) => ArrayType::Int8(arr.slice(&info).to_owned()),
            ArrayType::Int16(arr) => ArrayType::Int16(arr.slice(&info).to_owned()),
            ArrayType::Int32(arr) => ArrayType::Int32(arr.slice(&info).to_owned()),
            ArrayType::Int64(arr) => ArrayType::Int64(arr.slice(&info).to_owned()),
            ArrayType::Int128(arr) => ArrayType::Int128(arr.slice(&info).to_owned()),
            // Unsigned integer types
            ArrayType::UInt8(arr) => ArrayType::UInt8(arr.slice(&info).to_owned()),
            ArrayType::UInt16(arr) => ArrayType::UInt16(arr.slice(&info).to_owned()),
            ArrayType::UInt32(arr) => ArrayType::UInt32(arr.slice(&info).to_owned()),
            ArrayType::UInt64(arr) => ArrayType::UInt64(arr.slice(&info).to_owned()),
            ArrayType::UInt128(arr) => ArrayType::UInt128(arr.slice(&info).to_owned()),
        }
    }

    /// Get the type name as a string
    pub fn type_name(&self) -> &'static str {
        match self {
            // Floating point types
            ArrayType::Float32(_) => "f32",
            ArrayType::Float64(_) => "f64",
            // Signed integer types
            ArrayType::Int8(_) => "i8",
            ArrayType::Int16(_) => "i16",
            ArrayType::Int32(_) => "i32",
            ArrayType::Int64(_) => "i64",
            ArrayType::Int128(_) => "i128",
            // Unsigned integer types
            ArrayType::UInt8(_) => "u8",
            ArrayType::UInt16(_) => "u16",
            ArrayType::UInt32(_) => "u32",
            ArrayType::UInt64(_) => "u64",
            ArrayType::UInt128(_) => "u128",
        }
    }
}

// Floating point implementations
impl From<ArrayD<f32>> for ArrayType {
    fn from(array: ArrayD<f32>) -> Self {
        ArrayType::Float32(array)
    }
}

impl From<ArrayD<f64>> for ArrayType {
    fn from(array: ArrayD<f64>) -> Self {
        ArrayType::Float64(array)
    }
}

// Signed integer implementations
impl From<ArrayD<i8>> for ArrayType {
    fn from(array: ArrayD<i8>) -> Self {
        ArrayType::Int8(array)
    }
}

impl From<ArrayD<i16>> for ArrayType {
    fn from(array: ArrayD<i16>) -> Self {
        ArrayType::Int16(array)
    }
}

impl From<ArrayD<i32>> for ArrayType {
    fn from(array: ArrayD<i32>) -> Self {
        ArrayType::Int32(array)
    }
}

impl From<ArrayD<i64>> for ArrayType {
    fn from(array: ArrayD<i64>) -> Self {
        ArrayType::Int64(array)
    }
}

impl From<ArrayD<i128>> for ArrayType {
    fn from(array: ArrayD<i128>) -> Self {
        ArrayType::Int128(array)
    }
}

// Unsigned integer implementations
impl From<ArrayD<u8>> for ArrayType {
    fn from(array: ArrayD<u8>) -> Self {
        ArrayType::UInt8(array)
    }
}

impl From<ArrayD<u16>> for ArrayType {
    fn from(array: ArrayD<u16>) -> Self {
        ArrayType::UInt16(array)
    }
}

impl From<ArrayD<u32>> for ArrayType {
    fn from(array: ArrayD<u32>) -> Self {
        ArrayType::UInt32(array)
    }
}

impl From<ArrayD<u64>> for ArrayType {
    fn from(array: ArrayD<u64>) -> Self {
        ArrayType::UInt64(array)
    }
}

impl From<ArrayD<u128>> for ArrayType {
    fn from(array: ArrayD<u128>) -> Self {
        ArrayType::UInt128(array)
    }
}

/// Helper trait to check if a type can be converted to ArrayType
pub trait IntoArrayType {
    fn into_array_type(self) -> ArrayType;
}

impl<T> IntoArrayType for ArrayD<T>
where
    T: 'static,
    ArrayD<T>: Into<ArrayType>,
{
    fn into_array_type(self) -> ArrayType {
        self.into()
    }
}

/// Structure to hold object creation parameters sent by client
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateObjectParams {
    pub name: String,
    pub initial_metadata: Option<HashMap<String, MetadataValue>>,
    pub array_data: Option<ArrayType>,
    pub client_rank: u32, // MPI rank of the client
}

/// A DataObject that can own an NDArray of various numeric types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataObject {
    /// Unique identifier (UUIDv7 as u128).
    pub id: u128,
    /// A human-readable name.
    pub name: String,
    /// Optional attached NDArray of any supported type.
    pub array: Option<ArrayType>,
    /// Arbitrary metadata attributes.
    pub metadata: HashMap<String, MetadataValue>,
    /// Nested child DataObjects.
    pub children: Option<HashSet<u128>>,
}

impl DataObject {
    /// Create a new DataObject with the given parameters.
    /// Note: The id field will be set by the server during object creation.
    pub fn new(params: CreateObjectParams) -> Self {
        DataObject {
            // Use a temporary ID that will be replaced by the server
            id: 0,
            name: params.name,
            array: params.array_data,
            metadata: params.initial_metadata.unwrap_or_default(),
            children: Some(HashSet::new()),
        }
    }

    /// Attach an NDArray to this DataObject.
    pub fn attach_array(&mut self, array: ArrayType) {
        self.array = Some(array);
    }

    /// Add a child DataObject.
    pub fn add_child(&mut self, child_id: u128) {
        self.children.as_mut().unwrap().insert(child_id);
    }

    /// add a group of children.
    pub fn add_children(&mut self, child_ids: Vec<u128>) {
        for child_id in child_ids {
            self.add_child(child_id);
        }
    }

    /// get all child ids.
    pub fn get_children_ids(&self) -> Vec<u128> {
        self.children.as_ref().unwrap().iter().cloned().collect()
    }

    // remove a child
    pub fn remove_child(&mut self, child_id: u128) {
        self.children.as_mut().unwrap().remove(&child_id);
    }

    /// Get a slice of the attached NDArray.
    pub fn get_array_slice(&self, region: &[Slice]) -> Option<ArrayType> {
        self.array.as_ref().map(|arr| arr.slice(region))
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

    pub fn save_to_file(&self, path: &str) -> io::Result<()> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&path)?;
        // file name format should be {data_dir}/{id}.obj
        let filename = format!("{}/{}.obj", path, self.id);
        let mut file = File::create(filename)?;
        encode::write(&mut file, &self).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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
    pub fn insert(&self, obj: DataObject) {
        let obj_id = obj.id;
        let obj_name = obj.name.clone();
        self.objects.insert(obj_id, obj);
        self.name_obj_idx.insert(obj_name, obj_id);
    }

    /// Retrieve a DataObject by its u128 ID.
    pub fn get(&self, id: u128) -> Option<DataObject> {
        self.objects.get(&id).map(|entry| entry.clone())
    }

    /// attach or update metadata of a DataObject by id
    pub fn set_metadata(&mut self, id: u128, metadata: Vec<(String, MetadataValue)>) {
        for (key, value) in metadata {
            self.objects.get_mut(&id).unwrap().set_metadata(key, value);
        }
    }

    /// get a group of metadata by keys.
    pub fn get_metadata(
        &self,
        id: u128,
        keys: Vec<&str>,
    ) -> Option<HashMap<String, MetadataValue>> {
        self.objects.get(&id).unwrap().get_metadata_map(keys)
    }

    /// Retrieve a slice from the NDArray attached to a DataObject identified by its u128 ID.
    pub fn get_object_slice(&self, id: u128, region: &[Slice]) -> Option<ArrayType> {
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
        obj_regions: Vec<(u128, Vec<Slice>)>,
    ) -> Vec<Option<ArrayType>> {
        obj_regions
            .into_iter()
            .map(|(id, region)| self.get_object_slice(id, &region))
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
