use fnv::FnvHasher;
use lazy_static::lazy_static;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::{
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU32, Ordering},
    time::{SystemTime, UNIX_EPOCH},
}; // Fast, non-cryptographic hash

lazy_static! {
    static ref SEQUENCE_GENERATOR: AtomicU32 = {
        // Create seed from current time and process id for better uniqueness
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let pid = std::process::id() as u64;
        let seed = now.wrapping_mul(pid);

        // Create a well-seeded RNG
        let mut rng = StdRng::seed_from_u64(seed);
        AtomicU32::new(rng.random())
    };
}

/// The object ID is a globally unique identifier (GUID) of an object.
/// The format is [ timestamp (64 bits) | sequence (32 bits) | version (8 bits) | name_hash (24 bits) ]
/// timestamp: microseconds since epoch (enough for 584,000 years)
/// sequence: a number that is unique within a single process and increases monotonically
/// version: the version of the object
/// name_hash: the hash of the object name, used for virtual node ID.
///
/// Notes:
/// We use the name_hash for virtual node ID, since it is FNV hash, which is fast and collision-free,
/// this can ensure an even object distribution on virtual nodes.
///
/// What is the maximum number of virtual nodes we support?
/// 2^24 = 16,777,216.
/// Some facts:
/// The total number of CPU cores of Perlmutter: 507,904
/// The largest number of CPU cores a supercomputer may have: 10,649,600 (Sunway TaihuLight)
/// An order of 100 million servers in active operation worldwide.
///
/// Therefore, we can see that 24 bits is enough for us to cover a huge number of physical nodes
/// and this almost 10% of all the active servers worldwide,
/// and even more than the total number of CPU cores the largest supercomputer has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobalObjectId {
    // [ timestamp (64 bits) | sequence (32 bits) | name_hash (24 bits) | version (8 bits) ]
    timestamp: u64, // 64 bits: microseconds since epoch (enough for 584,000 years)
    sequence: u32,  // 32 bits: Sequence number
    version: u8,    // 8 bits:  Object version
    name_hash: u32, // 24 bits: FNV hash of object name, used for virtual node ID.
}

impl GlobalObjectId {
    pub fn to_u128(&self) -> u128 {
        // [ timestamp (64 bits) | sequence (32 bits) | version (8 bits) | name_hash (24 bits) ]
        ((self.timestamp as u128) << 64) |               // 64 bits [127-64]
        ((self.sequence as u128) << 32) |               // 32 bits [63-32]
        ((self.version as u128) << 24) |                // 8 bits  [31-24]
        (self.name_hash as u128 & 0xFFFFFF) // 24 bits [23-0]
    }

    pub fn from_u128(value: u128) -> Self {
        Self {
            timestamp: ((value >> 64) & 0xFFFFFFFFFFFFFFFF) as u64, // 64 bits
            sequence: ((value >> 32) & 0xFFFFFFFF) as u32,          // 32 bits
            version: ((value >> 24) & 0xFF) as u8,                  // 8 bits
            name_hash: (value & 0xFFFFFF) as u32,                   // 24 bits
        }
    }

    pub fn new(object_name: &str) -> Self {
        Self::with_vnode_id(object_name, None)
    }

    pub fn with_vnode_id(object_name: &str, vnode_id: Option<u32>) -> Self {
        let name_hash = Self::get_name_hash(object_name);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        Self {
            timestamp,
            sequence: SEQUENCE_GENERATOR.fetch_add(1, Ordering::SeqCst) as u32,
            name_hash: vnode_id.unwrap_or(name_hash),
            version: 1,
        }
    }

    pub fn get_name_hash(object_name: &str) -> u32 {
        let mut hasher = FnvHasher::default();
        object_name.hash(&mut hasher);
        hasher.finish() as u32
    }
}

pub trait GlobalObjectIdExt {
    fn timestamp(self) -> u64;
    fn sequence(self) -> u32;
    fn name_hash(self) -> u32;
    fn version(self) -> u8;
    fn vnode_id(self) -> u32;
}

impl GlobalObjectIdExt for u128 {
    fn timestamp(self) -> u64 {
        ((self >> 64) & 0xFFFFFFFFFFFFFFFF) as u64 // 64 bits [127-64]
    }
    fn sequence(self) -> u32 {
        ((self >> 32) & 0xFFFFFFFF) as u32 // 32 bits [63-32]
    }
    fn version(self) -> u8 {
        ((self >> 24) & 0xFF) as u8 // 8 bits [31-24]
    }
    fn name_hash(self) -> u32 {
        (self & 0xFFFFFF) as u32 // 24 bits [23-0]
    }
    fn vnode_id(self) -> u32 {
        self.name_hash() // Same as name_hash, used for virtual node ID
    }
}
