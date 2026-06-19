//! Cloud S3 storage backend (placeholder)

use async_trait::async_trait;
use bytes::Bytes;

use crate::backends::StorageBackend;
use crate::error::Result;
use crate::types::*;

/// Cloud S3 storage backend - skeleton implementation
#[derive(Debug, Clone)]
pub struct CloudS3Backend;

impl CloudS3Backend {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CloudS3Backend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for CloudS3Backend {
    fn backend_type(&self) -> BackendType {
        BackendType::CloudS3
    }

    async fn init(&self, _config: crate::backends::BackendConfig) -> Result<()> {
        // TODO: Initialize AWS SDK
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn put(&self, _key: &str, value: Bytes) -> Result<StoreReceipt> {
        Ok(StoreReceipt {
            content_hash: crate::utils::cid::compute_cid(&value),
            backend: BackendType::CloudS3,
            stored_at: chrono::Utc::now(),
            size_bytes: value.len() as u64,
            pinned: false,
        })
    }

    async fn get(&self, _key: &str) -> Result<Option<Bytes>> {
        Ok(None)
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    async fn exists(&self, _key: &str) -> Result<bool> {
        Ok(false)
    }

    async fn stats(&self) -> Result<BackendStats> {
        Ok(BackendStats {
            total_capacity: u64::MAX,
            used_space: 0,
            available_space: u64::MAX,
            item_count: 0,
        })
    }
}
