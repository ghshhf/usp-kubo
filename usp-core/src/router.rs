//! Backend Router - routes storage requests to appropriate backends

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use bytes::Bytes;

use crate::backends::{StorageBackend, BackendType};
use crate::policy::PolicyEngine;
use crate::types::*;
use crate::error::Result;
use crate::utils::HybridCache;

/// Backend router - selects and routes to appropriate storage backend
pub struct BackendRouter {
    backends: Arc<RwLock<HashMap<BackendType, Arc<dyn StorageBackend>>>>,
    policy_engine: Arc<PolicyEngine>,
    cache: Arc<HybridCache>,
}

impl BackendRouter {
    pub fn new() -> Self {
        Self {
            backends: Arc::new(RwLock::new(HashMap::new())),
            policy_engine: Arc::new(PolicyEngine::new()),
            cache: Arc::new(HybridCache::new(100_000_000)), // 100MB cache
        }
    }

    /// Get reference to backends map
    pub async fn backends(&self) -> Arc<RwLock<HashMap<BackendType, Arc<dyn StorageBackend>>>> {
        self.backends.clone()
    }

    /// Register a backend
    pub async fn register_backend(&self, backend: Arc<dyn StorageBackend>) {
        let mut backends = self.backends.write().await;
        backends.insert(backend.backend_type(), backend);
    }

    /// Select best backend for given key and options
    pub fn select_backend(&self, key: &str, opts: &StorageOptions) -> Result<BackendType> {
        if let Some(hint) = &opts.backend_hint {
            return Ok(*hint);
        }
        self.policy_engine.decide(key, opts)
    }

    /// Store data to selected backend
    pub async fn store(&self, key: &str, value: Bytes, opts: StorageOptions) -> Result<StoreReceipt> {
        let backend_type = self.select_backend(key, &opts)?;
        let backends = self.backends.read().await;
        let backend = backends.get(&backend_type)
            .ok_or_else(|| crate::Error::BackendNotFound(format!("{:?}", backend_type)))?
            .clone();

        let receipt = backend.put(key, value.clone()).await?;

        // Write to cache
        let _ = self.cache.set(key, value).await;

        Ok(receipt)
    }

    /// Retrieve data
    pub async fn retrieve(&self, key: &str) -> Result<Option<Bytes>> {
        // Check cache first
        if let Some(cached) = self.cache.get(key).await? {
            return Ok(Some(cached));
        }

        // Try backends in order
        let backends = self.backends.read().await;
        for (_backend_type, backend) in backends.iter() {
            if let Ok(Some(data)) = backend.get(key).await {
                let _ = self.cache.set(key, data.clone()).await;
                return Ok(Some(data));
            }
        }

        Ok(None)
    }

    /// Get statistics
    pub async fn stats(&self) -> Result<StorageStats> {
        let backends = self.backends.read().await;
        let mut stats = StorageStats::default();

        for (backend_type, backend) in backends.iter() {
            if let Ok(backend_stats) = backend.stats().await {
                stats.backends.insert(*backend_type, backend_stats);
            }
        }

        Ok(stats)
    }
}

impl Default for BackendRouter {
    fn default() -> Self {
        Self::new()
    }
}
