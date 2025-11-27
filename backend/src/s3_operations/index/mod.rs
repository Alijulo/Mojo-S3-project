// mod.rs
pub mod types;
pub mod wal;
pub mod memtable;
pub mod sst;
pub mod manifest;
pub mod compactor;

use crate::index::{
    wal::Wal,
    memtable::Memtable,
    manifest::Manifest,
    compactor::Compactor,
    types::{IndexEntry, WalOp, GetResult},
    sst::SstReader,
};
use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct BucketIndex {
    pub bucket: String,
    pub base: PathBuf,
    pub mem: Arc<Memtable>,
    pub wal: Mutex<Wal>,
    pub manifest: Mutex<Manifest>,
    pub seq_counter: Mutex<u64>, // monotonic per bucket
    pub sst_cache: Mutex<HashMap<String, SstReader>>, // cached SST readers keyed by file_name
}

impl BucketIndex {
    pub async fn open(bucket: &str, base: impl AsRef<Path>) -> Result<Self> {
        let base = base.as_ref().to_path_buf();
        let mpath = base.join("manifest.json");

        let manifest = Manifest::load(&mpath).await?;

        // Load WAL
        let wal_path = base.join("wal").join(&manifest.current_wal);
        let ops = Wal::replay(&wal_path).await?;
        let wal = Wal::open(&wal_path).await?;

        let mem = Arc::new(Memtable::default());
        // Reconstruct seq_counter from replay length or max seq in ops if present
        let mut seq_counter = 0u64;
        for op in &ops {
            // If ops carry seq_no in IndexEntry, use it; otherwise assign monotonically:
            seq_counter += 1;
            mem.apply_with_seq(op, seq_counter);
        }

        Ok(Self {
            bucket: bucket.to_string(),
            base,
            mem,
            wal: Mutex::new(wal),
            manifest: Mutex::new(manifest),
            seq_counter: Mutex::new(seq_counter),
            sst_cache: Mutex::new(HashMap::new()),
        })
    }

    pub async fn put(&self, mut entry: IndexEntry) -> Result<()> {
        // Assign seq
        let mut seq = self.seq_counter.lock().await;
        *seq += 1;
        entry.seq_no = *seq;
        entry.is_delete = false;

        let op = WalOp::Put(entry.clone());
        {
            let mut wal = self.wal.lock().await;
            wal.append(&op).await?;
        }
        self.mem.apply(&op);
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut seq = self.seq_counter.lock().await;
        *seq += 1;

        let op = WalOp::Delete { key: key.to_string() };
        {
            let mut wal = self.wal.lock().await;
            wal.append(&op).await?;
        }
        // Apply tombstone with the seq
        self.mem.apply_with_seq(&op, *seq);
        Ok(())
    }

    // Unified get: consult memtable first, then SSTs in LSM order using cached readers
    pub async fn get(&self, key: &str) -> GetResult {
        // 1) Check memtable first
        match self.mem.get(key) {
            Some(entry) if entry.is_delete => return GetResult::Deleted(entry.key),
            Some(entry) => return GetResult::Found(entry),
            None => {}
        }

        // 2) Fall back to SSTs via manifest (L0 → L1 → …), stop on first match
        let manifest = self.manifest.lock().await;
        let mut cache = self.sst_cache.lock().await;

        for level in &manifest.levels {
            for meta in level {
                // Use cached reader if available; if open fails, skip this file
                let reader = match cache.get_mut(&meta.file_name) {
                    Some(r) => r,
                    None => {
                        let path = self.base.join("index").join(&meta.file_name);
                        match SstReader::open(&path) {
                            Ok(rdr) => {
                                cache.insert(meta.file_name.clone(), rdr);
                                cache.get_mut(&meta.file_name).unwrap()
                            }
                            Err(_) => continue,
                        }
                    }
                };

                let res = reader.get(key);
                if !matches!(res, GetResult::NotFound) {
                    return res;
                }
            }
        }

        GetResult::NotFound
    }

    // Unified prefix listing: merge memtable + SSTs, choose highest seq_no per key, respect tombstones
    pub async fn list_prefix(&self, prefix: &str) -> Vec<GetResult> {
        use std::collections::hash_map::Entry;

        // Merged map: key -> highest-seq IndexEntry or a synthetic tombstone entry
        let mut merged: HashMap<String, IndexEntry> = HashMap::new();

        // 1) Collect from memtable
        for entry in self.mem.iter_prefix(prefix) {
            match merged.entry(entry.key.clone()) {
                Entry::Vacant(v) => {
                    v.insert(entry);
                }
                Entry::Occupied(mut o) => {
                    if entry.seq_no > o.get().seq_no {
                        o.insert(entry);
                    }
                }
            }
        }

        // 2) Collect from SSTs (L0 → L1 → …)
        let manifest = self.manifest.lock().await;
        let mut cache = self.sst_cache.lock().await;

        for level in &manifest.levels {
            for meta in level {
                let reader = match cache.get_mut(&meta.file_name) {
                    Some(r) => r,
                    None => {
                        let path = self.base.join("index").join(&meta.file_name);
                        match SstReader::open(&path) {
                            Ok(rdr) => {
                                cache.insert(meta.file_name.clone(), rdr);
                                cache.get_mut(&meta.file_name).unwrap()
                            }
                            Err(_) => continue,
                        }
                    }
                };

                // SstReader::list_prefix returns Vec<GetResult>, mix into merged
                for res in reader.list_prefix(prefix) {
                    match res {
                        GetResult::Found(entry) => {
                            match merged.entry(entry.key.clone()) {
                                Entry::Vacant(v) => {
                                    v.insert(entry);
                                }
                                Entry::Occupied(mut o) => {
                                    if entry.seq_no > o.get().seq_no {
                                        o.insert(entry);
                                    }
                                }
                            }
                        }
                        GetResult::Deleted(key) => {
                            // Represent tombstone in merged by inserting a minimal tombstone IndexEntry
                            let tomb = IndexEntry {
                                key: key.clone(),
                                size: 0,
                                etag: String::new(),
                                last_modified: String::new(),
                                seq_no: u64::MAX, // ensure tombstone wins if needed
                                is_delete: true,
                                version: None,
                            };
                            merged.insert(key, tomb);
                        }
                        GetResult::NotFound => {
                            // ignore
                        }
                    }
                }
            }
        }

        // 3) Convert merged map into Vec<GetResult>, keeping tombstones explicit
        let mut out: Vec<GetResult> = merged
            .into_iter()
            .map(|(_, entry)| {
                if entry.is_delete {
                    GetResult::Deleted(entry.key)
                } else {
                    GetResult::Found(entry)
                }
            })
            .collect();

        // Optional: sort by key for deterministic output
        out.sort_by(|a, b| match (a, b) {
            (GetResult::Found(e1), GetResult::Found(e2)) => e1.key.cmp(&e2.key),
            (GetResult::Found(e1), GetResult::Deleted(k2)) => e1.key.cmp(k2),
            (GetResult::Deleted(k1), GetResult::Found(e2)) => k1.cmp(&e2.key),
            (GetResult::Deleted(k1), GetResult::Deleted(k2)) => k1.cmp(k2),
            _ => std::cmp::Ordering::Equal,
        });

        out
    }

    // Flush memtable to SST, compact, then rotate WAL and clear memtable
    pub async fn compact(&self) -> Result<()> {
        // 1) Lock manifest and perform flush + compaction
        let mut manifest = self.manifest.lock().await;
        let comp = Compactor::new(&self.base);
        comp.flush_memtable(&self.mem, &mut manifest).await?;
        comp.compact_levels(&mut manifest).await?;

        // 2) Clear memtable after successful flush/compaction
        self.mem.clear();

        // 3) Rotate WAL: derive next name, update manifest, persist
        let new_wal_name = manifest.next_wal_name();
        manifest.current_wal = new_wal_name.clone();
        manifest.atomic_save(self.base.join("manifest.json")).await?;

        // 4) Update SST cache:
        //    - Remove readers for files deleted by compaction.
        //    - Add readers for newly added files lazily on demand (we keep lazy init).
        {
            let cache_names: Vec<String> = manifest
                .levels
                .iter()
                .flat_map(|lvl| lvl.iter().map(|m| m.file_name.clone()))
                .collect();
            let mut cache = self.sst_cache.lock().await;
            // Retain only readers that are still present in manifest
            cache.retain(|fname, _| cache_names.contains(fname));
        }

        // Drop manifest lock before WAL operations
        drop(manifest);

        // 5) Replace WAL writer with a new file
        let mut wal_guard = self.wal.lock().await;
        let new_wal_path = self.base.join("wal").join(&new_wal_name);
        *wal_guard = Wal::open(&new_wal_path).await?;

        // Optional: fsync the WAL directory to persist the new file entry
        #[cfg(unix)]
        {
            let wal_dir = self.base.join("wal");
            tokio::task::block_in_place(|| {
                let d = std::fs::File::open(&wal_dir).unwrap();
                let _ = d.sync_all();
            });
        }

        Ok(())
    }
}



// // mod.rs
// pub mod types;
// pub mod wal;
// pub mod memtable;
// pub mod sst;
// pub mod manifest;
// pub mod compactor;

// use crate::index::{
//     wal::Wal,
//     memtable::Memtable,
//     manifest::Manifest,
//     compactor::Compactor,
//     types::{IndexEntry, WalOp, GetResult},
// };
// use tokio::sync::Mutex;
// use std::sync::Arc;
// use anyhow::Result;
// use std::path::{Path, PathBuf};

// pub struct BucketIndex {
//     pub bucket: String,
//     pub base: PathBuf,
//     pub mem: Arc<Memtable>,
//     pub wal: Mutex<Wal>,
//     pub manifest: Mutex<Manifest>,
//     pub seq_counter: Mutex<u64>, // monotonic per bucket
// }

// impl BucketIndex {
//     pub async fn open(bucket: &str, base: impl AsRef<Path>) -> Result<Self> {
//         let base = base.as_ref().to_path_buf();
//         let mpath = base.join("manifest.json");

//         let manifest = Manifest::load(&mpath).await?;

//         // Load WAL
//         let wal_path = base.join("wal").join(&manifest.current_wal);
//         let ops = Wal::replay(&wal_path).await?;
//         let wal = Wal::open(&wal_path).await?;

//         let mem = Arc::new(Memtable::default());
//         // Reconstruct seq_counter from replay length or max seq in ops if present
//         let mut seq_counter = 0u64;
//         for op in &ops {
//             // If your ops carry seq_no in IndexEntry, you can read it here.
//             // Otherwise, assign monotonically during replay:
//             seq_counter += 1;
//             mem.apply_with_seq(op, seq_counter);
//         }

//         Ok(Self {
//             bucket: bucket.to_string(),
//             base,
//             mem,
//             wal: Mutex::new(wal),
//             manifest: Mutex::new(manifest),
//             seq_counter: Mutex::new(seq_counter),
//         })
//     }

//     pub async fn put(&self, mut entry: IndexEntry) -> Result<()> {
//         // Assign seq
//         let mut seq = self.seq_counter.lock().await;
//         *seq += 1;
//         entry.seq_no = *seq;
//         entry.is_delete = false;

//         let op = WalOp::Put(entry.clone());
//         {
//             let mut wal = self.wal.lock().await;
//             wal.append(&op).await?;
//         }
//         self.mem.apply(&op);
//         Ok(())
//     }

//     pub async fn delete(&self, key: &str) -> Result<()> {
//         let mut seq = self.seq_counter.lock().await;
//         *seq += 1;

//         let op = WalOp::Delete { key: key.to_string() };
//         {
//             let mut wal = self.wal.lock().await;
//             wal.append(&op).await?;
//         }
//         // Apply tombstone with the seq
//         self.mem.apply_with_seq(&op, *seq);
//         Ok(())
//     }

//     // pub fn get(&self, key: &str) -> GetResult {
//     //     match self.mem.get(key) {
//     //         Some(entry) if entry.is_delete => GetResult::Deleted,
//     //         Some(entry) => GetResult::Found(entry),
//     //         None => GetResult::NotFound,
//     //     }
//     // }

//     pub async fn get(&self, key: &str) -> GetResult {
//         // 1) Check memtable first
//         match self.mem.get(key) {
//             Some(entry) if entry.is_delete => return GetResult::Deleted,
//             Some(entry) => return GetResult::Found(entry),
//             None => { /* fall through */ }
//         }

//         // 2) Fall back to SSTs via manifest
//         let mut manifest = self.manifest.lock().await;

//         // Search levels in order (L0, L1, …)
//         for level in manifest.levels.iter() {
//             for meta in level {
//                 let sst_path = self.base.join("index").join(&meta.file_name);
//                 let mut reader = match crate::index::sst::SstReader::open(&sst_path) {
//                     Ok(r) => r,
//                     Err(_) => continue, // skip unreadable SST
//                 };
//                 let res = reader.get(key);
//                 match res {
//                     GetResult::Found(_) => return res,
//                     GetResult::Deleted => return res,
//                     GetResult::NotFound => { /* keep searching */ }
//                 }
//             }
//         }


//         GetResult::NotFound
//     }


//     pub fn list_prefix(&self, prefix: &str) -> Vec<GetResult> {
//         self.mem
//             .iter_prefix(prefix)
//             .into_iter()
//             .map(|entry| {
//                 if entry.is_delete {
//                     GetResult::Deleted
//                 } else {
//                     GetResult::Found(entry)
//                 }
//             })
//             .collect()
//     }

//     // Flush memtable to SST, compact, then rotate WAL and clear memtable
//     pub async fn compact(&self) -> Result<()> {
//         // 1) Lock manifest and perform flush + compaction
//         let mut manifest = self.manifest.lock().await;
//         let comp = Compactor::new(&self.base);
//         comp.flush_memtable(&self.mem, &mut manifest).await?;
//         comp.compact_levels(&mut manifest).await?;

//         // 2) Clear memtable after successful flush/compaction
//         self.mem.clear();

//         // 3) Rotate WAL: derive next name, update manifest, persist, reopen WAL
//         let new_wal_name = manifest.next_wal_name();
//         manifest.current_wal = new_wal_name.clone();
//         manifest.atomic_save(self.base.join("manifest.json")).await?;

//         // Drop manifest lock before WAL operations
//         drop(manifest);

//         // 4) Replace WAL writer with a new file
//         let mut wal_guard = self.wal.lock().await;
//         let new_wal_path = self.base.join("wal").join(&new_wal_name);
//         *wal_guard = Wal::open(&new_wal_path).await?;

//         // Optional: fsync the WAL directory to persist the new file entry
//         #[cfg(unix)]
//         {
//             let wal_dir = self.base.join("wal");
//             tokio::task::block_in_place(|| {
//                 let d = std::fs::File::open(&wal_dir).unwrap();
//                 let _ = d.sync_all();
//             });
//         }

//         Ok(())
//     }
// }
