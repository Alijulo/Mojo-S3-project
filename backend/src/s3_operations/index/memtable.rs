// memtable.rs
use crate::index::types::{IndexEntry, WalOp};
use dashmap::DashMap;

#[derive(Default)]
pub struct Memtable {
    pub map: DashMap<String, IndexEntry>,
}

impl Memtable {
    /// Apply a WAL operation with a sequence number (used during WAL replay).
        pub fn apply_with_seq(&self, op: &WalOp, seq_no: u64) {
        match op {
            WalOp::Put { entry, .. } => {
                let mut new_entry = entry.clone();
                new_entry.seq_no = seq_no;
                new_entry.is_delete = false;
                self.map.insert(new_entry.key.clone(), new_entry);
            }

            WalOp::Delete { key, .. } => {
                let tombstone = IndexEntry {
                    key: key.clone(),
                    size: 0,
                    etag: String::new(),
                    last_modified: String::new(),
                    seq_no,
                    is_delete: true,
                    version: None,
                };
                self.map.insert(key.clone(), tombstone);
            }

            WalOp::Commit { .. } => {
                // Commit markers don’t affect the memtable directly
            }
        }
    }
    // pub fn apply_with_seq(&self, op: &WalOp, seq_no: u64) {
    //     match op {
    //         WalOp::Put(entry) => {
    //             let mut new_entry = entry.clone();
    //             new_entry.seq_no = seq_no;
    //             new_entry.is_delete = false;
    //             self.map.insert(new_entry.key.clone(), new_entry);
    //         }

    //         WalOp::Delete { key } => {
    //             let tombstone = IndexEntry {
    //                 key: key.clone(),
    //                 size: 0,
    //                 etag: String::new(),
    //                 last_modified: String::new(),
    //                 seq_no,
    //                 is_delete: true,
    //                 version: None,
    //             };
    //             self.map.insert(key.clone(), tombstone);
    //         }
    //     }
    // }

    /// Apply a WAL operation during real-time writes (not replay).
    pub fn apply(&self, op: &WalOp) {
        match op {
            WalOp::Put { entry, .. } => {
                self.map.insert(entry.key.clone(), entry.clone());
            }
            WalOp::Delete { key, .. } => {
                // During live PUT/DELETE, tombstones should be stored, not removed
                let tombstone = IndexEntry {
                    key: key.clone(),
                    size: 0,
                    etag: String::new(),
                    last_modified: String::new(),
                    seq_no: 0, // real seq assigned by WAL layer
                    is_delete: true,
                    version: None,
                };
                self.map.insert(key.clone(), tombstone);
            }
            WalOp::Commit { txn_id } => {
                // Commit markers don’t affect the memtable directly.
                // You might log them for debugging or metrics if you want visibility.
                tracing::debug!(%txn_id, "memtable saw commit marker");
            }
        }
    }
    // pub fn apply(&self, op: &WalOp) {
    //     match op {
    //         WalOp::Put(entry) => {
    //             self.map.insert(entry.key.clone(), entry.clone());
    //         }
    //         WalOp::Delete { key } => {
    //             // During live PUT/DELETE, tombstones should be stored, not removed
    //             let tombstone = IndexEntry {
    //                 key: key.clone(),
    //                 size: 0,
    //                 etag: String::new(),
    //                 last_modified: String::new(),
    //                 seq_no: 0, // real seq assigned by WAL layer
    //                 is_delete: true,
    //                 version: None,
    //             };
    //             self.map.insert(key.clone(), tombstone);
    //         }
    //     }
    // }

    /// Get a single entry (including tombstones).
    pub fn get(&self, key: &str) -> Option<IndexEntry> {
        self.map.get(key).map(|entry| entry.clone())
    }

    /// Return all entries (including tombstones).
    pub fn iter_all(&self) -> Vec<IndexEntry> {
        self.map.iter().map(|item| item.clone()).collect()
    }

    /// Return all entries matching a given prefix.
    pub fn iter_prefix(&self, prefix: &str) -> Vec<IndexEntry> {
        self.map
            .iter()
            .filter(|item| item.key.starts_with(prefix))
            .map(|item| item.clone())
            .collect()
    }

    /// Clear the entire memtable (used after flush)
    pub fn clear(&self) {
        self.map.clear();
    }
}







// use crate::index::types::{IndexEntry, WalOp};
// use dashmap::DashMap;

// #[derive(Default)]
// pub struct Memtable {
//     pub map: DashMap<String, IndexEntry>,
// }

// impl Memtable {
//     pub fn apply_with_seq(&self, op: &WalOp, seq_no: u64) {
//         match op {
//             WalOp::Put(entry) => {
//                 let mut new_entry = entry.clone();
//                 new_entry.seq_no = seq_no;
//                 new_entry.is_delete = false;
//                 self.map.insert(new_entry.key.clone(), new_entry);
//             }
//             WalOp::Delete { key } => {
//                 let tombstone = IndexEntry {
//                     key: key.clone(),
//                     size: 0,
//                     etag: String::new(),
//                     last_modified: String::new(),
//                     seq_no,
//                     is_delete: true,
//                     version: None,
//                 };
//                 self.map.insert(key.clone(), tombstone);
//             }
//         }
//     }

//     pub fn apply(&self, op: &WalOp) {
//         match op {
//             WalOp::Put(entry) => {
//                 self.map.insert(entry.key.clone(), entry.clone());
//             }
//             WalOp::Delete { key } => {
//                 self.map.remove(key);
//             }
//         }
//     }

//     pub fn get(&self, key: &str) -> Option<IndexEntry> {
//         self.map.get(key).map(|v| v.clone())
//     }

//     pub fn iter_all(&self) -> Vec<IndexEntry> {
//         self.map.iter().map(|v| v.clone()).collect()
//     }

//     pub fn iter_prefix(&self, prefix: &str) -> Vec<IndexEntry> {
//         self.map
//             .iter()
//             .filter(|r| r.key.starts_with(prefix))
//             .map(|r| r.clone())
//             .collect()
//     }

//     pub fn clear(&self) {
//         self.map.clear();
//     }
// }


// use crate::index::types::{IndexEntry, WalOp};
// use dashmap::DashMap;

// #[derive(Default)]
// pub struct Memtable {
//     pub map: DashMap<String, IndexEntry>,
// }

// impl Memtable {
//     // Apply op with a provided seq
//     pub fn apply_with_seq(&self, op: &WalOp, seq_no: u64) {
//         match op {
//             WalOp::Put(entry) => {
//                 let mut new_entry = entry.clone(); // clone the borrowed entry
//                 new_entry.seq_no = seq_no;
//                 new_entry.is_delete = false;
//                 self.map.insert(new_entry.key.clone(), new_entry);
//             }
//             WalOp::Delete { key } => {
//                 let tombstone = IndexEntry {
//                     key: key.clone(),
//                     size: 0,
//                     etag: String::new(),
//                     last_modified: String::new(),
//                     seq_no,
//                     is_delete: true,
//                     version: None,
//                 };
//                 self.map.insert(key.clone(), tombstone);
//             }
//         }
//     }

//     // Legacy: assumes entry already has seq_no/is_delete set
//     pub fn apply(&self, op: &WalOp) {
//         match op {
//             WalOp::Put(entry) => {
//                 self.map.insert(entry.key.clone(), entry.clone());
//             }
//             WalOp::Delete { key } => {
//                 self.map.remove(key);
//             }
//         }
//     }

//     pub fn get(&self, key: &str) -> Option<IndexEntry> {
//         self.map.get(key).map(|v| v.clone())
//     }

//     pub fn iter_all(&self) -> Vec<IndexEntry> {
//         self.map.iter().map(|v| v.clone()).collect()
//     }

//     pub fn iter_prefix(&self, prefix: &str) -> Vec<IndexEntry> {
//         self.map
//             .iter()
//             .filter(|r| r.key.starts_with(prefix))
//             .map(|r| r.clone())
//             .collect()
//     }

//     // Clear all in-memory entries after a successful flush
//     pub fn clear(&self) {
//         self.map.clear();
//     }
// }
