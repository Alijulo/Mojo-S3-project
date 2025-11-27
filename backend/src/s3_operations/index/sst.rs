// //sst.rs

use crate::index::types::{IndexEntry, GetResult};
use anyhow::Result;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use memmap2::Mmap;
use std::path::{Path, PathBuf};

pub struct SstWriter {
    w: BufWriter<File>,
}

impl SstWriter {
    pub async fn create_atomic(final_path: impl AsRef<Path>) -> Result<(Self, PathBuf)> {
        let final_path = final_path.as_ref();
        let parent = final_path.parent().unwrap();
        tokio::fs::create_dir_all(parent).await?;
        let tmp = parent.join(format!(".tmp.{}", final_path.file_name().unwrap().to_string_lossy()));
        let file = File::options().create(true).write(true).open(&tmp).await?;
        Ok((Self { w: BufWriter::new(file) }, tmp))
    }

    pub async fn write_sorted(&mut self, entries: &mut Vec<IndexEntry>) -> Result<u64> {
        // key ASC, seq_no DESC
        entries.sort_by(|a, b| {
            a.key.cmp(&b.key)
                .then(b.seq_no.cmp(&a.seq_no))
        });

        let bytes = serde_json::to_vec(&entries)?;
        self.w.write_all(&bytes).await?;
        self.w.flush().await?;
        self.w.get_mut().sync_data().await?;
        Ok(bytes.len() as u64)
    }
}

pub struct SstReader {
    mm: Mmap,
    entries: Option<Vec<IndexEntry>>,
}

impl SstReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let mm = unsafe { Mmap::map(&file)? };
        Ok(Self { mm, entries: None })
    }

    fn ensure_entries(&mut self) -> Result<&Vec<IndexEntry>> {
        if self.entries.is_none() {
            let mut v: Vec<IndexEntry> = serde_json::from_slice(&self.mm)?;

            // key ASC, seq_no DESC
            v.sort_by(|a, b| {
                a.key.cmp(&b.key)
                    .then(b.seq_no.cmp(&a.seq_no))
            });

            self.entries = Some(v);
        }
        Ok(self.entries.as_ref().unwrap())
    }

    pub fn get(&mut self, key: &str) -> GetResult {
        let entries = match self.ensure_entries() {
            Ok(v) => v,
            Err(_) => return GetResult::NotFound,
        };

        let start = entries.partition_point(|e| e.key.as_str() < key);
        let end   = entries.partition_point(|e| e.key.as_str() <= key);

        if start == end {
            return GetResult::NotFound;
        }

        // newest version because seq DESC
        let e = entries[start].clone();

        if e.is_delete {
            GetResult::Deleted(e.key)
        } else {
            GetResult::Found(e)
        }
    }

    pub fn list_prefix(&mut self, prefix: &str) -> Vec<GetResult> {
        let entries = match self.ensure_entries() {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };

        let start = entries.partition_point(|e| e.key.as_str() < prefix);

        let mut out = Vec::new();
        let mut i = start;

        while i < entries.len() {
            let e = &entries[i];

            if !e.key.starts_with(prefix) {
                break;
            }

            // newest version
            let newest = e.clone();

            if newest.is_delete {
                out.push(GetResult::Deleted(newest.key.clone()));
            } else {
                out.push(GetResult::Found(newest));
            }

            // skip all versions of the same key
            let next = entries.partition_point(|x| x.key <= e.key);
            i = next;
        }

        out
    }

    pub fn load_all(&mut self) -> Result<Vec<IndexEntry>> {
        let v = self.ensure_entries()?.clone();
        Ok(v)
    }
}

// use crate::index::types::{IndexEntry, GetResult};
// use anyhow::Result;
// use tokio::fs::File;
// use tokio::io::{AsyncWriteExt, BufWriter};
// use memmap2::Mmap;
// use std::path::{Path, PathBuf};

// pub struct SstWriter {
//     w: BufWriter<File>,
// }

// impl SstWriter {
//     pub async fn create_atomic(final_path: impl AsRef<Path>) -> Result<(Self, PathBuf)> {
//         let final_path = final_path.as_ref();
//         let parent = final_path.parent().unwrap();
//         tokio::fs::create_dir_all(parent).await?;
//         let tmp = parent.join(format!(".tmp.{}", final_path.file_name().unwrap().to_string_lossy()));
//         let file = File::options().create(true).write(true).open(&tmp).await?;
//         Ok((Self { w: BufWriter::new(file) }, tmp))
//     }

//     pub async fn write_sorted(mut self, mut entries: Vec<IndexEntry>) -> Result<u64> {
//         entries.sort_by(|a, b| a.key.cmp(&b.key).then(a.seq_no.cmp(&b.seq_no)));
//         let bytes = serde_json::to_vec(&entries)?;
//         self.w.write_all(&bytes).await?;
//         self.w.flush().await?;
//         self.w.get_mut().sync_data().await?;
//         Ok(bytes.len() as u64)
//     }
// }

// pub struct SstReader {
//     mm: Mmap,
//     entries: Option<Vec<IndexEntry>>,
// }

// impl SstReader {
//     pub fn open(path: impl AsRef<Path>) -> Result<Self> {
//         let file = std::fs::File::open(path)?;
//         let mm = unsafe { Mmap::map(&file)? };
//         Ok(Self { mm, entries: None })
//     }

//     fn ensure_entries(&mut self) -> Result<&Vec<IndexEntry>> {
//         if self.entries.is_none() {
//             let mut v: Vec<IndexEntry> = serde_json::from_slice(&self.mm)?;
//             v.sort_by(|a, b| a.key.cmp(&b.key).then(a.seq_no.cmp(&b.seq_no)));
//             self.entries = Some(v);
//         }
//         Ok(self.entries.as_ref().unwrap())
//     }

//     pub fn get(&mut self, key: &str) -> GetResult {
//         let entries = match self.ensure_entries() {
//             Ok(v) => v,
//             Err(_) => return GetResult::NotFound,
//         };
//         let start = entries.partition_point(|e| e.key.as_str() < key);
//         let end   = entries.partition_point(|e| e.key.as_str() <= key);
//         if start == end {
//             return GetResult::NotFound;
//         }
//         let idx = end.saturating_sub(1);
//         let e = entries[idx].clone();
//         if e.is_delete {
//             GetResult::Deleted(e.key)
//         } else {
//             GetResult::Found(e)
//         }
//     }

//     pub fn list_prefix(&mut self, prefix: &str) -> Vec<GetResult> {
//         let entries = match self.ensure_entries() {
//             Ok(v) => v,
//             Err(_) => return Vec::new(),
//         };
//         let start = entries.partition_point(|e| e.key.as_str() < prefix);

//         let mut out = Vec::new();
//         let mut i = start;
//         while i < entries.len() {
//             let e = &entries[i];
//             if !e.key.starts_with(prefix) { break; }
//             let key = e.key.clone();
//             let g_end = entries.partition_point(|x| x.key <= key);
//             let candidate = entries[g_end - 1].clone();
//             if candidate.is_delete {
//                 out.push(GetResult::Deleted(candidate.key));
//             } else {
//                 out.push(GetResult::Found(candidate));
//             }
//             i = g_end;
//         }
//         out
//     }

//     pub fn load_all(&mut self) -> Result<Vec<IndexEntry>> {
//         let v = self.ensure_entries()?.clone();
//         Ok(v)
//     }
// }






// use crate::index::types::{IndexEntry, GetResult};
// use anyhow::Result;
// use tokio::fs::File;
// use tokio::io::{AsyncWriteExt, BufWriter};
// use memmap2::Mmap;
// use std::path::{Path, PathBuf};

// pub struct SstWriter {
//     w: BufWriter<File>,
// }

// impl SstWriter {
//     pub async fn create_atomic(final_path: impl AsRef<Path>) -> Result<(Self, PathBuf)> {
//         let final_path = final_path.as_ref();
//         let parent = final_path.parent().unwrap();
//         tokio::fs::create_dir_all(parent).await?;
//         let tmp = parent.join(format!(".tmp.{}", final_path.file_name().unwrap().to_string_lossy()));
//         let file = File::options().create(true).write(true).open(&tmp).await?;
//         Ok((Self { w: BufWriter::new(file) }, tmp))
//     }

//     pub async fn write_sorted(mut self, mut entries: Vec<IndexEntry>) -> Result<u64> {
//         // Sort primarily by key, secondarily by seq_no
//         entries.sort_by(|a, b| a.key.cmp(&b.key).then(a.seq_no.cmp(&b.seq_no)));
//         let bytes = serde_json::to_vec(&entries)?;
//         self.w.write_all(&bytes).await?;
//         self.w.flush().await?;
//         self.w.get_mut().sync_data().await?;
//         Ok(bytes.len() as u64)
//     }
// }

// pub struct SstReader {
//     mm: Mmap,
//     // Cache decoded entries on first load for repeated lookups
//     entries: Option<Vec<IndexEntry>>,
// }

// impl SstReader {
//     pub fn open(path: impl AsRef<Path>) -> Result<Self> {
//         let file = std::fs::File::open(path)?;
//         let mm = unsafe { Mmap::map(&file)? };
//         Ok(Self { mm, entries: None })
//     }

//     fn ensure_entries(&mut self) -> Result<&Vec<IndexEntry>> {
//         if self.entries.is_none() {
//             let mut v: Vec<IndexEntry> = serde_json::from_slice(&self.mm)?;
//             // Ensure sorted: by key, then seq_no
//             v.sort_by(|a, b| a.key.cmp(&b.key).then(a.seq_no.cmp(&b.seq_no)));
//             self.entries = Some(v);
//         }
//         Ok(self.entries.as_ref().unwrap())
//     }

//     /// Return richer GetResult semantics instead of Option<IndexEntry>
//     pub fn get(&mut self, key: &str) -> GetResult {
//         let entries = match self.ensure_entries() {
//             Ok(v) => v,
//             Err(_) => return GetResult::NotFound,
//         };
//         // Find range of entries with this key using partition points
//         let start = entries.partition_point(|e| e.key.as_str() < key);
//         let end   = entries.partition_point(|e| e.key.as_str() <= key);
//         if start == end {
//             return GetResult::NotFound;
//         }
//         // Highest seq_no is at the end of the range - 1
//         let idx = end.saturating_sub(1);
//         let e = entries[idx].clone();
//         if e.is_delete {
//             GetResult::Deleted
//         } else {
//             GetResult::Found(e)
//         }
//     }

//     /// Efficient prefix listing using partition points, returning Vec<GetResult>
//     pub fn list_prefix(&mut self, prefix: &str) -> Vec<GetResult> {
//         let entries = match self.ensure_entries() {
//             Ok(v) => v,
//             Err(_) => return Vec::new(),
//         };
//         // Lower bound: first key >= prefix
//         let start = entries.partition_point(|e| e.key.as_str() < prefix);

//         let mut out = Vec::new();
//         let mut i = start;
//         while i < entries.len() {
//             let e = &entries[i];
//             if !e.key.starts_with(prefix) { break; }
//             // For each key group, pick highest seq_no
//             let key = e.key.clone();
//             let g_end = entries.partition_point(|x| x.key <= key);
//             let candidate = entries[g_end - 1].clone();
//             if candidate.is_delete {
//                 out.push(GetResult::Deleted);
//             } else {
//                 out.push(GetResult::Found(candidate));
//             }
//             i = g_end;
//         }
//         out
//     }

//     // Full load if needed
//     pub fn load_all(&mut self) -> Result<Vec<IndexEntry>> {
//         let v = self.ensure_entries()?.clone();
//         Ok(v)
//     }
// }
