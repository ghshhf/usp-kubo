//! Storage Hub - unified storage interface

use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backends::StorageBackend;
use crate::error::Result;
use crate::router::BackendRouter;
use crate::types::*;
use crate::utils::RetryConfig;

/// Unified Storage Hub - main entry point for all storage operations
pub struct StorageHub {
    router: Arc<BackendRouter>,
    pinned_keys: Arc<RwLock<HashSet<String>>>,
}

impl StorageHub {
    /// Create a new StorageHub with default config
    pub fn new() -> Self {
        let router = BackendRouter::new();
        Self {
            router: Arc::new(router),
            pinned_keys: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Create with custom retry configuration
    pub fn with_retry_config(config: RetryConfig) -> Self {
        let router = BackendRouter::with_retry_config(config);
        Self {
            router: Arc::new(router),
            pinned_keys: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Register a storage backend and return its type.
    pub async fn register_backend(&self, backend: Arc<dyn StorageBackend>) -> BackendType {
        self.router.register_backend(backend).await
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
        // Query all backends; a successful `true` from any backend short-circuits.
        // We intentionally swallow per-backend errors so a single failing backend
        // does not poison the overall `exists` result for the rest.
        let backends_snapshot = self.router.backends_snapshot().await;
        for (_, backend) in &backends_snapshot {
            match backend.exists(key).await {
                Ok(true) => return Ok(true),
                Ok(false) => continue,
                Err(err) => {
                    tracing::warn!("exists() backend error: {}", err);
                    continue;
                }
            }
        }
        Ok(false)
    }

    /// Get storage statistics across all backends
    pub async fn stat(&self) -> Result<StorageStats> {
        self.router.stats().await
    }

    /// List all keys across all registered backends.
    pub async fn list_keys(&self) -> Vec<String> {
        self.router.list_keys().await
    }

    /// Pin data (ensure it won't be garbage collected)
    pub async fn pin(&self, key: &str) -> Result<()> {
        tracing::debug!("PIN {}", key);
        self.pinned_keys.write().await.insert(key.to_string());
        Ok(())
    }

    /// Unpin data
    pub async fn unpin(&self, key: &str) -> Result<()> {
        tracing::debug!("UNPIN {}", key);
        self.pinned_keys.write().await.remove(key);
        Ok(())
    }

    /// Check if data is pinned
    pub async fn is_pinned(&self, key: &str) -> Result<bool> {
        Ok(self.pinned_keys.read().await.contains(key))
    }
}

impl Default for StorageHub {
    fn default() -> Self {
        Self::new()
    }
}
