
// //compactor.rs
use crate::index::{
    memtable::Memtable,
    sst::{SstWriter, SstReader},
    manifest::{Manifest, SstMeta},
    types::IndexEntry,
};
use anyhow::Result;
use std::path::{Path, PathBuf};

pub struct Compactor {
    bucket_path: PathBuf,
    l0_max_files: usize,
}

impl Compactor {
    pub fn new(bucket_path: impl AsRef<Path>) -> Self {
        Self {
            bucket_path: bucket_path.as_ref().to_path_buf(),
            l0_max_files: 4,
        }
    }

    /// Flush current memtable into an L0 SST
    pub async fn flush_memtable(&self, mem: &Memtable, manifest: &mut Manifest) -> Result<()> {
        let mut entries = mem.iter_all();
        if entries.is_empty() {
            return Ok(());
        }

        // SORT: key ASC, seq DESC (newest first)
        entries.sort_by(|a, b| a.key.cmp(&b.key).then(b.seq_no.cmp(&a.seq_no)));

        let file_name = manifest.next_sst_name();
        let sst_path = self.bucket_path.join("index").join(&file_name);

        // Write atomically using reference so we can keep entries afterwards
        let (mut writer, tmp) = SstWriter::create_atomic(&sst_path).await?;
        let size_bytes = writer.write_sorted(&mut entries).await?;
        tokio::fs::rename(&tmp, &sst_path).await?;

        let min_key = entries.first().unwrap().key.clone();
        let max_key = entries.last().unwrap().key.clone();
        let seq_hi = entries.first().unwrap().seq_no;
        let seq_lo = entries.last().unwrap().seq_no;

        manifest.add_file(SstMeta {
            file_name: file_name.clone(),
            level: 0,
            min_key,
            max_key,
            size_bytes,
            entries: entries.len() as u64,
            seq_lo,
            seq_hi,
            checksum: None,
        });

        manifest.atomic_save(self.bucket_path.join("manifest.json")).await?;
        Ok(())
    }

    /// Simple planner: if L0 exceeds threshold, merge all L0 files into a single L1
    pub async fn compact_levels(&self, manifest: &mut Manifest) -> Result<()> {
        let l0 = manifest.levels.get(0).cloned().unwrap_or_default();
        if l0.len() <= self.l0_max_files {
            return Ok(());
        }

        // Load all SST entries
        let mut all: Vec<IndexEntry> = Vec::new();
        for m in &l0 {
            let path = self.bucket_path.join("index").join(&m.file_name);
            let mut reader = SstReader::open(&path)?;
            let mut v = reader.load_all()?;
            all.append(&mut v);
        }

        if all.is_empty() {
            let rm: Vec<String> = l0.iter().map(|m| m.file_name.clone()).collect();
            manifest.remove_files(&rm);
            manifest.atomic_save(self.bucket_path.join("manifest.json")).await?;
            return Ok(());
        }

        // SORT: key ASC, seq DESC (newest first)
        all.sort_by(|a, b| a.key.cmp(&b.key).then(b.seq_no.cmp(&a.seq_no)));

        // MERGE keys (take newest, skip tombstones)
        let mut merged = Vec::new();
        let mut i = 0;
        while i < all.len() {
            let key = &all[i].key;
            let start = i;
            let mut end = i + 1;
            while end < all.len() && all[end].key == *key {
                end += 1;
            }

            let newest = &all[start];
            if !newest.is_delete {
                merged.push(newest.clone());
            }

            i = end;
        }

        if merged.is_empty() {
            let rm: Vec<String> = l0.iter().map(|m| m.file_name.clone()).collect();
            manifest.remove_files(&rm);
            manifest.atomic_save(self.bucket_path.join("manifest.json")).await?;
            return Ok(());
        }

        // Write merged L1 SST
        let file_name = manifest.next_sst_name();
        let target = self.bucket_path.join("index").join(&file_name);

        let (mut writer, tmp) = SstWriter::create_atomic(&target).await?;
        let size_bytes = writer.write_sorted(&mut merged).await?;
        tokio::fs::rename(&tmp, &target).await?;

        let min_key = merged.first().unwrap().key.clone();
        let max_key = merged.last().unwrap().key.clone();
        let seq_hi = merged.first().unwrap().seq_no;
        let seq_lo = merged.last().unwrap().seq_no;

        let meta = SstMeta {
            file_name: file_name.clone(),
            level: 1,
            min_key,
            max_key,
            seq_lo,
            seq_hi,
            size_bytes,
            entries: merged.len() as u64,
            checksum: None,
        };

        let rm: Vec<String> = l0.iter().map(|m| m.file_name.clone()).collect();
        manifest.remove_files(&rm);
        manifest.add_file(meta);
        manifest.atomic_save(self.bucket_path.join("manifest.json")).await?;

        Ok(())
    }
}





// use crate::index::{
//     memtable::Memtable,
//     sst::{SstWriter, SstReader},
//     manifest::{Manifest, SstMeta},
// };
// use anyhow::Result;
// use std::path::{Path, PathBuf};

// pub struct Compactor {
//     bucket_path: PathBuf,
//     l0_max_files: usize,
// }

// impl Compactor {
//     pub fn new(bucket_path: impl AsRef<Path>) -> Self {
//         Self {
//             bucket_path: bucket_path.as_ref().to_path_buf(),
//             l0_max_files: 4,
//         }
//     }

//     // Flush current memtable into an L0 SST
//     pub async fn flush_memtable(&self, mem: &Memtable, manifest: &mut Manifest) -> Result<()> {
//         let mut entries = mem.iter_all();
//         if entries.is_empty() {
//             return Ok(());
//         }

//         let next_sst = manifest.next_sst_name();
//         let sst_path = self.bucket_path.join("index").join(&next_sst);

//         let (writer, tmp_path) = SstWriter::create_atomic(&sst_path).await?;
//         let size_bytes = writer.write_sorted(entries.clone()).await?;

//         // Atomic rename + fsync parent
//         tokio::fs::rename(&tmp_path, &sst_path).await?;
//         #[cfg(unix)]
//         {
//             let dir = sst_path.parent().unwrap();
//             tokio::task::block_in_place(|| {
//                 let d = std::fs::File::open(dir).unwrap();
//                 let _ = d.sync_all();
//             });
//         }

//         entries.sort_by(|a, b| a.key.cmp(&b.key).then(a.seq_no.cmp(&b.seq_no)));
//         let min_key = entries.first().unwrap().key.clone();
//         let max_key = entries.last().unwrap().key.clone();
//         let seq_lo = entries.first().unwrap().seq_no;
//         let seq_hi = entries.last().unwrap().seq_no;

//         let meta = SstMeta {
//             file_name: next_sst.clone(),
//             level: 0,
//             min_key,
//             max_key,
//             size_bytes,
//             seq_lo,
//             seq_hi,
//             entries: entries.len() as u64,
//             checksum: None,
//         };

//         manifest.add_file(meta);
//         manifest.atomic_save(self.bucket_path.join("manifest.json")).await?;
//         Ok(())
//     }

//     // Simple planner: if L0 exceeds threshold, merge all L0 files into a single L1
//     pub async fn compact_levels(&self, manifest: &mut Manifest) -> Result<()> {
//         if manifest.levels.get(0).map(|l| l.len()).unwrap_or(0) <= self.l0_max_files {
//             return Ok(());
//         }

//         let l0 = manifest.levels.get(0).cloned().unwrap_or_default();
//         if l0.is_empty() { return Ok(()); }

//         // Build readers
//         let mut readers = Vec::new();
//         for m in &l0 {
//             let p = self.bucket_path.join("index").join(&m.file_name);
//             readers.push(SstReader::open(&p)?);
//         }

//         // Load all entries, then merge by key selecting highest seq_no; drop tombstones
//         let mut all = Vec::new();
//         for mut r in readers {
//             let mut v = r.load_all()?;
//             all.append(&mut v);
//         }
//         all.sort_by(|a, b| a.key.cmp(&b.key).then(a.seq_no.cmp(&b.seq_no)));

//         let mut merged = Vec::new();
//         let mut i = 0;
//         while i < all.len() {
//             let key = all[i].key.clone();
//             let end = all.partition_point(|x| x.key <= key);
//             // winner: highest seq_no
//             let candidate = all[end - 1].clone();
//             if !candidate.is_delete {
//                 merged.push(candidate);
//             }
//             i = end;
//         }

//         if merged.is_empty() {
//             // All tombstoned: just remove L0 files
//             let remove_names: Vec<String> = l0.iter().map(|m| m.file_name.clone()).collect();
//             manifest.remove_files(&remove_names);
//             manifest.atomic_save(self.bucket_path.join("manifest.json")).await?;
//             return Ok(());
//         }

//         let next_sst = manifest.next_sst_name();
//         let sst_path = self.bucket_path.join("index").join(&next_sst);
//         let (writer, tmp_path) = SstWriter::create_atomic(&sst_path).await?;
//         let size_bytes = writer.write_sorted(merged.clone()).await?;
//         tokio::fs::rename(&tmp_path, &sst_path).await?;
//         #[cfg(unix)]
//         {
//             let dir = sst_path.parent().unwrap();
//             tokio::task::block_in_place(|| {
//                 let d = std::fs::File::open(dir).unwrap();
//                 let _ = d.sync_all();
//             });
//         }

//         let meta = SstMeta {
//             file_name: next_sst.clone(),
//             level: 1,
//             min_key: merged.first().unwrap().key.clone(),
//             max_key: merged.last().unwrap().key.clone(),
//             size_bytes,
//             seq_lo: merged.first().unwrap().seq_no,
//             seq_hi: merged.last().unwrap().seq_no,
//             entries: merged.len() as u64,
//             checksum: None,
//         };

//         let remove_names: Vec<String> = l0.iter().map(|m| m.file_name.clone()).collect();
//         manifest.remove_files(&remove_names);
//         manifest.add_file(meta);
//         manifest.atomic_save(self.bucket_path.join("manifest.json")).await?;
//         Ok(())
//     }
// }
