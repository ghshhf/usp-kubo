//! Storage Hub - unified storage interface

use std::sync::Arc;
use bytes::Bytes;

use crate::backends::StorageBackend;
use crate::router::BackendRouter;
use crate::types::*;
use crate::error::Result;

/// Unified Storage Hub - main entry point for all storage operations
pub struct StorageHub {
    router: Arc<BackendRouter>,
}

impl StorageHub {
    pub fn new() -> Self {
        let router = BackendRouter::new();
        Self { router: Arc::new(router) }
    }

    /// Register a storage backend
    pub async fn register_backend(&self, backend: Arc<dyn StorageBackend>) {
        self.router.register_backend(backend).await;
    }

    /// Store data
    pub async fn put(&self, key: &str, value: Bytes, opts: StorageOptions) -> Result<StoreReceipt> {
        self.router.store(key, value, opts).await
    }

    /// Read data
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        self.router.retrieve(key).await
    }

    /// Delete data
    pub async fn delete(&self, key: &str) -> Result<()> {
        let backends = self.router.backends().await;
        let backends_guard = backends.read().await;
        for backend in backends_guard.values() {
            backend.delete(key).await?;
        }
        Ok(())
    }

    /// Check if key exists
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let backends = self.router.backends().await;
        let backends_guard = backends.read().await;
        for backend in backends_guard.values() {
            if backend.exists(key).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Get storage statistics
    pub async fn stat(&self) -> Result<StorageStats> {
        self.router.stats().await
    }

    /// Pin data (ensure it won't be garbage collected)
    pub async fn pin(&self, _key: &str) -> Result<()> {
        // TODO: implement pin tracking
        Ok(())
    }

    /// Unpin data
    pub async fn unpin(&self, _key: &str) -> Result<()> {
        // TODO: implement pin tracking
        Ok(())
    }
}

impl Default for StorageHub {
    fn default() -> Self {
        Self::new()
    }
}
