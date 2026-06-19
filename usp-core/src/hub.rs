//! Storage Hub - unified storage interface

use std::sync::Arc;
use bytes::Bytes;

use crate::backends::StorageBackend;
use crate::router::BackendRouter;
use crate::types::*;
use crate::error::Result;
use crate::utils::RetryConfig;

/// Unified Storage Hub - main entry point for all storage operations
pub struct StorageHub {
    router: Arc<BackendRouter>,
}

impl StorageHub {
    /// Create a new StorageHub with default config
    pub fn new() -> Self {
        let router = BackendRouter::new();
        Self { router: Arc::new(router) }
    }

    /// Create with custom retry configuration
    pub fn with_retry_config(config: RetryConfig) -> Self {
        let router = BackendRouter::with_retry_config(config);
        Self { router: Arc::new(router) }
    }

    /// Register a storage backend
    pub async fn register_backend(&self, backend: Arc<dyn StorageBackend>) {
        self.router.register_backend(backend).await;
    }

    /// Store data
    pub async fn put(&self, key: &str, value: Bytes, opts: StorageOptions) -> Result<StoreReceipt> {
        tracing::debug!("PUT {}", key);
        self.router.store(key, value, opts).await
    }

    /// Read data
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        tracing::debug!("GET {}", key);
        self.router.retrieve(key).await
    }

    /// Delete data from all registered backends
    pub async fn delete(&self, key: &str) -> Result<()> {
        tracing::debug!("DELETE {}", key);
        self.router.delete_all(key).await
    }

    /// Check if key exists in any backend
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let backends = self.router.backends().await;
        let backends_guard = backends.read().await;
        for backend in backends_guard.values() {
            if backend.exists(key).await.unwrap_or(false) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Get storage statistics across all backends
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
