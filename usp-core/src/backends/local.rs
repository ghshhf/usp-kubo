//! Local filesystem storage backend

use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;

use crate::backends::{BackendConfig, StorageBackend};
use crate::error::Result;
use crate::types::*;

/// Local filesystem storage backend
#[derive(Debug, Clone)]
pub struct LocalBackend {
    pub data_dir: PathBuf,
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
        let path = self.data_dir.join(key);
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
        let path = self.data_dir.join(key);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.data_dir.join(key);
        tokio::fs::remove_file(&path).await?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.data_dir.join(key);
        Ok(path.exists())
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

        // Get available disk space using statvfs on Unix or default fallback
        let available = if cfg!(unix) {
            std::fs::metadata(&self.data_dir)
                .and_then(|m| {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        Ok::<u64, std::io::Error>(m.blocks() * 512) // 512-byte blocks
                    }
                    #[cfg(not(unix))]
                    {
                        Ok(1_000_000_000_000_u64)
                    }
                })
                .unwrap_or(1_000_000_000_000_u64)
        } else {
            1_000_000_000_000_u64
        };

        let total = AtomicU64::new(0);
        let count = AtomicU64::new(0);
        walk_dir(&self.data_dir, &total, &count)?;
        let used_space = total.load(Ordering::Relaxed);
        let item_count = count.load(Ordering::Relaxed);

        Ok(BackendStats {
            total_capacity: available + used_space,
            used_space,
            available_space: available,
            item_count,
        })
    }
}
