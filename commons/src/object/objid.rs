use fnv::FnvHasher;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU16, Ordering},
    time::{SystemTime, UNIX_EPOCH},
}; // Fast, non-cryptographic hash

lazy_static! {
    static ref SEQUENCE_GENERATOR: AtomicU16 = AtomicU16::new(0);
}

fn get_virtual_node_id(timestamp: u32, object_name: &str) -> u32 {
    let mut hasher = FnvHasher::default();
    // Combine the client-specific identifier with the object name.
    timestamp.hash(&mut hasher);
    object_name.hash(&mut hasher);
    // Map the 64-bit FNV hash into a 24-bit number.
    (hasher.finish() & 0xFFFFFF) as u32
}
/// The total number of CPU cores of Perlmutter: 507,904
/// The largest number of CPU cores a supercomputer may have: 10,649,600 (Sunway TaihuLight)
/// An order of 100 million servers in active operation worldwide.
/// Therefore, we consider 2^24 to be the largest number of virtual nodes we support,
/// and it is already 10% of all the active servers,
/// and even more than what the largest supercomputer can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobalObjectId {
    version: u8,       // 8 bits:  Protocol version
    virtual_node: u32, // 24 bits: Virtual node ID
    timestamp: u64,    // 48 bits: seconds since epoch (enough until year 34,800)
    sequence: u16,     // 16 bits: Sequence number
    name_hash: u32,    // 32 bits: FNV hash of object name
}

impl GlobalObjectId {
    pub fn to_u128(&self) -> u128 {
        ((self.version as u128) << 120) |                   // 8 bits  [127-120]
        ((self.virtual_node as u128 & 0xFFFFFF) << 96) |    // 24 bits [119-96]
        ((self.timestamp as u128 & 0xFFFFFFFFFFFF) << 48) | // 48 bits [95-48]
        ((self.sequence as u128) << 32) |                   // 16 bits [47-32]
        (self.name_hash as u128 & 0xFFFFFFFF) // 32 bits [31-0]
    }

    pub fn from_u128(value: u128) -> Self {
        Self {
            version: ((value >> 120) & 0xFF) as u8,
            virtual_node: ((value >> 96) & 0xFFFFFF) as u32, // Only take 24 bits
            timestamp: ((value >> 48) & 0xFFFFFFFFFFFF) as u64, // 48 bits
            sequence: ((value >> 32) & 0xFFFF) as u16,
            name_hash: (value & 0xFFFFFFFF) as u32,
        }
    }

    pub fn new(object_name: &str) -> Self {
        Self::with_vnode_id(object_name, None)
    }

    pub fn with_vnode_id(object_name: &str, vnode_id: Option<u32>) -> Self {
        let mut hasher = FnvHasher::default();
        object_name.hash(&mut hasher);
        let name_hash = hasher.finish() as u32;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            version: 1,
            virtual_node: vnode_id
                .unwrap_or_else(|| get_virtual_node_id((timestamp >> 32) as u32, object_name)),
            name_hash,
            timestamp,
            sequence: SEQUENCE_GENERATOR.fetch_add(1, Ordering::SeqCst) as u16,
        }
    }
}

pub trait GlobalObjectIdExt {
    fn timestamp(self) -> u64;
    fn vnode_id(self) -> u32;
    fn sequence(self) -> u16;
    fn name_hash(self) -> u32;
    fn version(self) -> u8;
}

impl GlobalObjectIdExt for u128 {
    fn timestamp(self) -> u64 {
        ((self >> 48) & 0xFFFFFFFFFFFF) as u64
    }
    fn vnode_id(self) -> u32 {
        ((self >> 96) & 0xFFFFFF) as u32
    }
    fn sequence(self) -> u16 {
        ((self >> 32) & 0xFFFF) as u16
    }
    fn name_hash(self) -> u32 {
        (self & 0xFFFFFFFF) as u32
    }
    fn version(self) -> u8 {
        ((self >> 120) & 0xFF) as u8
    }
}
