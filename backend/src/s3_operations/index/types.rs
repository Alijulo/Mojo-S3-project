//types.rs
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: String,

    // Monotonic sequence number to order puts/deletes across SSTs
    pub seq_no: u64,

    // Tombstone marker for deletes
    pub is_delete: bool,

    // Optional opaque version identifier (S3-like)
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOp {
    Put(IndexEntry),
    Delete { key: String },
}

#[derive(Debug, Clone)]
pub enum GetResult {
    Found(IndexEntry),
    Deleted(String), // carry key for tombstones
    NotFound,
}


