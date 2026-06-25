//! Local filesystem storage backend

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use std::path::PathBuf;

use crate::backends::{BackendConfig, StorageBackend};
use crate::error::Result;
use crate::types::*;

/// Sanitize a user-supplied key to prevent path traversal.
/// Rejects keys containing `..` or absolute path components.
/// Also normalizes path separators to `/` and strips leading `/`.
fn sanitize_key(key: &str) -> std::result::Result<PathBuf, std::io::Error> {
    let trimmed = key.trim_start_matches('/');
    let pb = PathBuf::from(trimmed);
    // Reject any component that is ".." or "."
    for component in pb.components() {
        match component {
            std::path::Component::ParentDir => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("key contains path traversal: {}", key),
                ));
            }
            std::path::Component::RootDir => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("key contains absolute path: {}", key),
                ));
            }
            _ => {}
        }
    }
    Ok(pb)
}

/// Local filesystem storage backend
#[derive(Debug, Clone)]
pub struct LocalBackend {
    pub data_dir: PathBuf,
}

/// Get the actual available disk space for the filesystem containing the data directory.
/// Returns (total_bytes, available_bytes) for the filesystem.
fn get_filesystem_stats(data_dir: &std::path::Path) -> std::io::Result<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let path_bytes = data_dir.as_os_str().as_bytes();
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };

        // statvfs expects a NUL-terminated C string
        let mut c_path = path_bytes.to_vec();
        c_path.push(0);

        let result = unsafe { libc::statvfs(c_path.as_ptr() as *const _, &mut stat) };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }

        // bsize is the fragment size (block size) in bytes
        let fragment_size: u64 = stat.f_bsize as _;
        let total = stat.f_blocks * fragment_size;
        let available = stat.f_bavail * fragment_size;

        Ok((total, available))
    }

    #[cfg(not(unix))]
    {
        // On non-Unix systems, fall back to a large default value
        let _ = data_dir;
        Ok((1_000_000_000_000_u64, 1_000_000_000_000_u64))
    }
}

impl LocalBackend {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }
}

#[async_trait]
impl StorageBackend for LocalBackend {
    fn backend_type(&self) -> BackendType {
        BackendType::Local
    }

    async fn init(&self, _config: BackendConfig) -> Result<()> {
        tokio::fs::create_dir_all(&self.data_dir).await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn put(&self, key: &str, value: Bytes) -> Result<StoreReceipt> {
        let safe_key = sanitize_key(key)
            .map_err(|e| crate::Error::Storage(format!("invalid key: {}", e)))?;
        let path = self.data_dir.join(safe_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, &value).await?;

        let cid = crate::utils::cid::compute_cid(&value);

        Ok(StoreReceipt {
            content_hash: cid,
            backend: BackendType::Local,
            stored_at: chrono::Utc::now(),
            size_bytes: value.len() as u64,
            pinned: false,
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let safe_key = sanitize_key(key)
            .map_err(|e| crate::Error::Storage(format!("invalid key: {}", e)))?;
        let path = self.data_dir.join(safe_key);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let safe_key = sanitize_key(key)
            .map_err(|e| crate::Error::Storage(format!("invalid key: {}", e)))?;
        let path = self.data_dir.join(safe_key);
        // Idempotent delete: don't error if the file doesn't exist
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let safe_key = sanitize_key(key)
            .map_err(|e| crate::Error::Storage(format!("invalid key: {}", e)))?;
        let path = self.data_dir.join(safe_key);
        tokio::fs::try_exists(&path)
            .await
            .map_err(|e| e.into())
    }

    async fn stats(&self) -> Result<BackendStats> {
        use std::sync::atomic::{AtomicU64, Ordering};

        fn walk_dir(
            dir: &std::path::Path,
            total_size: &std::sync::atomic::AtomicU64,
            item_count: &std::sync::atomic::AtomicU64,
        ) -> std::io::Result<()> {
            if dir.is_dir() {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() {
                        let size = entry.metadata()?.len();
                        total_size.fetch_add(size, Ordering::Relaxed);
                        item_count.fetch_add(1, Ordering::Relaxed);
                    } else if path.is_dir() {
                        walk_dir(&path, total_size, item_count)?;
                    }
                }
            }
            Ok(())
        }

        // Get filesystem-level disk space statistics (total capacity + available space)
        let (total_capacity, available_space) = get_filesystem_stats(&self.data_dir)
            .unwrap_or((1_000_000_000_000_u64, 1_000_000_000_000_u64));

        let total = AtomicU64::new(0);
        let count = AtomicU64::new(0);
        walk_dir(&self.data_dir, &total, &count)?;
        let used_space = total.load(Ordering::Relaxed);
        let item_count = count.load(Ordering::Relaxed);

        Ok(BackendStats {
            total_capacity,
            used_space,
            available_space,
            item_count,
        })
    }

    async fn list_keys(&self) -> Result<Vec<String>> {
        let data_dir = self.data_dir.clone();
        tokio::task::spawn_blocking(move || list_keys_sync(&data_dir, &data_dir))
            .await
            .map_err(|e| crate::Error::Storage(format!("list_keys task join error: {}", e)))?
    }
}

/// Recursively list keys (relative paths) under `current`, stripping `base`.
fn list_keys_sync(base: &std::path::Path, current: &std::path::Path) -> std::io::Result<Vec<String>> {
    let mut keys = Vec::new();
    if !current.is_dir() {
        return Ok(keys);
    }
    for entry in std::fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Ok(key) = path.strip_prefix(base) {
                let key_str = key
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                keys.push(key_str);
            }
        } else if path.is_dir() {
            keys.extend(list_keys_sync(base, &path)?);
        }
    }
    Ok(keys)
}
