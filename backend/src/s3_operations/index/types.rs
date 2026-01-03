//types.rs

use serde::{Serialize, Deserialize};
use std::path::PathBuf;

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
    /// A put operation with transaction context
    Put {
        txn_id: String,
        entry: IndexEntry,
        tmp_path: PathBuf,
        final_path: PathBuf,
    },
    /// A delete operation with transaction context
    Delete {
        txn_id: String,
        key: String,
        tmp_path: PathBuf,
        final_path: PathBuf,
    },
    /// A commit marker for a transaction
    Commit {
        txn_id: String,
    },
}

impl WalOp {
    pub fn txn_id(&self) -> &str {
        match self {
            WalOp::Put { txn_id, .. } => txn_id,
            WalOp::Delete { txn_id, .. } => txn_id,
            WalOp::Commit { txn_id } => txn_id,
        }
    }
    pub fn is_commit(&self) -> bool {
        matches!(self, WalOp::Commit { .. })
    }
    pub fn tmp_path(&self) -> Option<&PathBuf> {
        match self {
            WalOp::Put { tmp_path, .. } => Some(tmp_path),
            WalOp::Delete { tmp_path, .. } => Some(tmp_path),
            _ => None,
        }
    }
    pub fn final_path(&self) -> Option<&PathBuf> {
        match self {
            WalOp::Put { final_path, .. } => Some(final_path),
            WalOp::Delete { final_path, .. } => Some(final_path),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum GetResult {
    Found(IndexEntry),
    Deleted(String), // carry key for tombstones
    NotFound,
}


// use serde::{Serialize, Deserialize};

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct IndexEntry {
//     pub key: String,
//     pub size: u64,
//     pub etag: String,
//     pub last_modified: String,

//     // Monotonic sequence number to order puts/deletes across SSTs
//     pub seq_no: u64,

//     // Tombstone marker for deletes
//     pub is_delete: bool,

//     // Optional opaque version identifier (S3-like)
//     pub version: Option<String>,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub enum WalOp {
//     Put(IndexEntry),
//     Delete { key: String },
// }

// #[derive(Debug, Clone)]
// pub enum GetResult {
//     Found(IndexEntry),
//     Deleted(String), // carry key for tombstones
//     NotFound,
// }
