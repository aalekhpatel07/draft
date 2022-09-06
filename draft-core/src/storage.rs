use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub trait Storage: Default {
    fn save(&mut self, data: &[u8]) -> color_eyre::Result<usize>;
    fn load(&mut self) -> color_eyre::Result<Vec<u8>>;
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

impl PartialEq for BufferBackend {
    /// Determine whether two BufferBackends are equal.
    ///
    /// It is important to first compare the _inner buffer
    /// by reference. Since it is behind an Arc<Mutex<...>>,
    /// if we try to lock the same memory, we deadlock. Only if
    /// the references point to different memory locations, should
    /// we try to lock and compare the inner value.
    fn eq(&self, other: &Self) -> bool {
        // Check by reference first.
        let ptr_equality = Arc::ptr_eq(&self._inner, &other._inner);

        // Fall back to the locking the mutexes and comparing the inner buffer.
        ptr_equality
            || *self
                ._inner
                .lock()
                .expect("Failed to lock the internal buffer.")
                == *other
                    ._inner
                    .lock()
                    .expect("Failed to lock internal buffer.")
    }
}

impl Eq for BufferBackend {}

impl BufferBackend {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            _inner: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
        }
    }
    pub fn new() -> Self {
        Self {
            _inner: Arc::new(Mutex::new(Vec::with_capacity(1024))),
        }
    }
}

impl Default for BufferBackend {
    fn default() -> Self {
        Self {
            _inner: Arc::new(Mutex::new(Vec::default())),
        }
    }
}

impl Storage for BufferBackend {
    fn save(&mut self, data: &[u8]) -> color_eyre::Result<usize> {
        let total_bytes = data.len();
        let mut buffer = self._inner.lock().expect("failed to lock internal buffer.");
        buffer.write_all(data)?;
        Ok(total_bytes)
    }

    fn load(&mut self) -> color_eyre::Result<Vec<u8>> {
        Ok(self
            ._inner
            .lock()
            .expect("Failed to lock internal buffer")
            .to_vec())
    }
}

impl Storage for FileStorageBackend {
    fn save(&mut self, data: &[u8]) -> color_eyre::Result<usize> {
        let total_bytes = data.len();
        std::fs::write(self.log_file_path.clone(), data)?;
        Ok(total_bytes)
    }
    fn load(&mut self) -> color_eyre::Result<Vec<u8>> {
        Ok(std::fs::read(self.log_file_path.clone())?)
    }
}

impl<IOBackend> Storage for IOBackend
where
    IOBackend: Read + Write + Default,
{
    fn save(&mut self, data: &[u8]) -> color_eyre::Result<usize> {
        let total_bytes = data.len();
        self.write_all(data)?;
        Ok(total_bytes)
    }
    fn load(&mut self) -> color_eyre::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.read_to_end(&mut buf)?;
        Ok(buf)
    }
}
