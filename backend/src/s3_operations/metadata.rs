// src/metadata.rs


#[cfg(unix)]
pub mod impl_ {
    use xattr;
    use std::path::Path;
    use std::io::{self,};

    pub fn set<P: AsRef<Path>>(path: P, key: &str, value: &[u8]) -> io::Result<()> {
        xattr::set(path.as_ref(), key, value).map_err(Into::into)
    }

    pub fn get<P: AsRef<Path>>(path: P, key: &str) -> io::Result<Option<Vec<u8>>> {
        xattr::get(path.as_ref(), key).map_err(Into::into)
    }
}

#[cfg(not(unix))]
pub mod impl_ {
    use std::fs;
    use std::io::{self,};
    use std::path::Path;

    // On Windows: use .s3meta.json file
    pub fn set<P: AsRef<Path>>(path: P, _key: &str, value: &[u8]) -> io::Result<()> {
        let json_path = path.as_ref().with_extension("json");
        fs::write(json_path, value)
    }

    pub fn get<P: AsRef<Path>>(path: P, _key: &str) -> io::Result<Option<Vec<u8>>> {
        let json_path = path.as_ref().with_extension("json");
        match fs::read(&json_path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub use impl_::{get, set};