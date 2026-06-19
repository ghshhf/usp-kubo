//! P2P storage backend (placeholder)

use async_trait::async_trait;
use bytes::Bytes;

use crate::backends::StorageBackend;
use crate::error::Result;
use crate::types::*;

/// P2P storage backend - skeleton implementation
#[derive(Debug, Clone)]
pub struct P2PBackend;

impl P2PBackend {
    pub fn new() -> Self {
        Self
    }
}

impl Default for P2PBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for P2PBackend {
    fn backend_type(&self) -> BackendType {
        BackendType::P2P
    }

    async fn init(&self, _config: crate::backends::BackendConfig) -> Result<()> {
        // TODO: Initialize libp2p
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn put(&self, _key: &str, value: Bytes) -> Result<StoreReceipt> {
        Ok(StoreReceipt {
            content_hash: crate::utils::cid::compute_cid(&value),
            backend: BackendType::P2P,
            stored_at: chrono::Utc::now(),
            size_bytes: value.len() as u64,
            pinned: true,
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
        Ok(BackendStats::default())
    }
}
