// src/metadata.rs
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{self, AsyncWriteExt};

#[cfg(unix)]
pub mod impl_ {
    use xattr;
    use std::path::Path;

    // xattr is sync → run in blocking thread
    pub async fn set<P: AsRef<Path>>(path: P, key: &str, value: &[u8]) -> io::Result<()> {
        let path = path.as_ref().to_owned();
        let key = key.to_string();
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || {
            xattr::set(&path, &key, &value).map_err(Into::into)
        })
        .await
        .unwrap_or_else(|e| Err(io::Error::new(io::ErrorKind::Other, e)))
    }

    pub async fn get<P: AsRef<Path>>(path: P, key: &str) -> io::Result<Option<Vec<u8>>> {
        let path = path.as_ref().to_owned();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            xattr::get(&path, &key).map_err(Into::into)
        })
        .await
        .unwrap_or_else(|e| Err(io::Error::new(io::ErrorKind::Other, e)))
    }
}

#[cfg(not(unix))]
pub mod impl_ {
    use std::path::Path;
    use tokio::fs;
    use tokio::io::{self, AsyncWriteExt};

    // Windows: atomic .json file write
    pub async fn set<P: AsRef<Path>>(path: P, _key: &str, value: &[u8]) -> io::Result<()> {
        let json_path = path.as_ref().with_extension("json");
        let temp_path = json_path.with_extension("tmp");

        // Write to temp
        let mut file = fs::File::create(&temp_path).await?;
        file.write_all(value).await?;
        file.flush().await?;
        file.sync_all().await?; // Durability

        // Atomic rename
        fs::rename(&temp_path, &json_path).await
    }

    pub async fn get<P: AsRef<Path>>(path: P, _key: &str) -> io::Result<Option<Vec<u8>>> {
        let json_path = path.as_ref().with_extension("json");
        match fs::read(&json_path).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub use impl_::{get, set};