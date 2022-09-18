use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub trait Storage {
    fn save(&self, data: &[u8]) -> color_eyre::Result<usize>;
    fn load(&self) -> color_eyre::Result<Vec<u8>>;
}

#[derive(Debug, PartialEq, Eq)]
pub struct FileStorageBackend {
    pub log_file_path: PathBuf,
}

impl Default for FileStorageBackend {
    fn default() -> Self {
        Self::new("/tmp/raft.d")
    }
}

impl FileStorageBackend {
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            log_file_path: path.as_ref().to_path_buf(),
        }
    }
}

#[derive(Debug)]
pub struct BufferBackend {
    _inner: Arc<Mutex<Vec<u8>>>,
}

impl Default for BufferBackend {
    fn default() -> Self {
        Self {
            _inner: Arc::new(Mutex::new(Vec::default())),
        }
    }
}

impl Storage for BufferBackend {
    fn save(&self, data: &[u8]) -> color_eyre::Result<usize> {
        let total_bytes = data.len();
        let mut buffer = self._inner.lock().expect("failed to lock internal buffer.");
        buffer.write_all(data)?;
        Ok(total_bytes)
    }

    fn load(&self) -> color_eyre::Result<Vec<u8>> {
        Ok(self
            ._inner
            .lock()
            .expect("Failed to lock internal buffer")
            .to_vec())
    }
}

impl Storage for FileStorageBackend {
    fn save(&self, data: &[u8]) -> color_eyre::Result<usize> {
        let total_bytes = data.len();
        std::fs::write(self.log_file_path.clone(), data)?;
        Ok(total_bytes)
    }
    fn load(&self) -> color_eyre::Result<Vec<u8>> {
        Ok(std::fs::read(self.log_file_path.clone())?)
    }
}
