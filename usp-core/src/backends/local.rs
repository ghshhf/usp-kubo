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
        Ok(BackendStats {
            total_capacity: 1_000_000_000_000,
            used_space: 0,
            available_space: 1_000_000_000_000,
            item_count: 0,
        })
    }
}
