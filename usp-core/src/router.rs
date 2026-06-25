//! Backend Router - routes storage requests to appropriate backends

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use bytes::Bytes;

use crate::backends::{BackendType, StorageBackend};
use crate::error::Result;
use crate::policy::PolicyEngine;
use crate::types::*;
use crate::utils::{with_retry, HybridCache, RetryConfig};

/// Backend router - selects and routes to appropriate storage backend
pub struct BackendRouter {
    backends: Arc<RwLock<HashMap<BackendType, Arc<dyn StorageBackend>>>>,
    policy_engine: Arc<PolicyEngine>,
    cache: Arc<HybridCache>,
    retry_config: RetryConfig,
}

impl BackendRouter {
    pub fn new() -> Self {
        Self {
            backends: Arc::new(RwLock::new(HashMap::new())),
            policy_engine: Arc::new(PolicyEngine::new()),
            cache: Arc::new(HybridCache::new(1_000)), // ~1000 entries, ~100MB assuming ~100KB/entry
            retry_config: RetryConfig::default(),
        }
    }

    /// Create router with custom retry config
    pub fn with_retry_config(config: RetryConfig) -> Self {
        Self {
            backends: Arc::new(RwLock::new(HashMap::new())),
            policy_engine: Arc::new(PolicyEngine::new()),
            cache: Arc::new(HybridCache::new(1_000)),
            retry_config: config,
        }
    }

    /// Get a snapshot of registered backends. Returns an opaque snapshot
    /// so callers cannot mutate the router's internal map.
    pub async fn backends_snapshot(
        &self,
    ) -> Vec<(BackendType, Arc<dyn StorageBackend>)> {
        let guard = self.backends.read().await;
        guard.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    /// Register a backend and return its type for confirmation.
    pub async fn register_backend(&self, backend: Arc<dyn StorageBackend>) -> BackendType {
        let backend_type = backend.backend_type();
        let mut backends = self.backends.write().await;
        backends.insert(backend_type, backend);
        backend_type
    }

    /// List all keys across all registered backends.
    /// Returns a deduplicated list of keys.
    pub async fn list_keys(&self) -> Vec<String> {
        let snapshot = self.backends_snapshot().await;
        let mut all_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        for (_, backend) in &snapshot {
            match backend.list_keys().await {
                Ok(keys) => all_keys.extend(keys),
                Err(err) => tracing::warn!("list_keys on {:?} failed: {}", backend.backend_type(), err),
            }
        }

        let mut keys: Vec<String> = all_keys.into_iter().collect();
        keys.sort();
        keys
    }

    /// Select best backend for given key, options and data size
    pub fn select_backend(
        &self,
        key: &str,
        opts: &StorageOptions,
        size_bytes: u64,
    ) -> Result<BackendType> {
        if let Some(hint) = &opts.backend_hint {
            return Ok(*hint);
        }
        self.policy_engine.decide(key, opts, size_bytes)
    }

    /// Store data to selected backend with retries on transient errors
    pub async fn store(
        &self,
        key: &str,
        value: Bytes,
        opts: StorageOptions,
    ) -> Result<StoreReceipt> {
        let backend_type = self.select_backend(key, &opts, value.len() as u64)?;

        // Find the backend
        let backend = {
            let backends = self.backends.read().await;
            backends
                .get(&backend_type)
                .ok_or_else(|| crate::error::Error::BackendNotFound(format!("{:?}", backend_type)))?
                .clone()
        };

        // Clone values needed in the closure (to satisfy Fn)
        let key_owned = key.to_string();
        let value_clone = value.clone();

        let receipt = with_retry(&self.retry_config, || {
            let backend = backend.clone();
            let k = key_owned.clone();
            let v = value_clone.clone();
            async move {
                tracing::debug!("Store: {} to backend {:?}", k, backend.backend_type());
                backend.put(&k, v).await
            }
        })
        .await?;

        // Write to cache
        let _ = self.cache.set(key, value).await;

        Ok(receipt)
    }

    /// Retrieve data with retries on transient errors
    pub async fn retrieve(&self, key: &str) -> Result<Option<Bytes>> {
        // Check cache first
        if let Some(cached) = self.cache.get(key).await? {
            return Ok(Some(cached));
        }

        // Try backends in order with retries per backend
        let backends_snapshot: Vec<(BackendType, Arc<dyn StorageBackend>)> = {
            let backends = self.backends.read().await;
            backends.iter().map(|(k, v)| (*k, v.clone())).collect()
        };

        for (backend_type, backend) in backends_snapshot {
            let key_owned = key.to_string();
            let backend_clone = backend.clone();

            let result = with_retry(&self.retry_config, || {
                let b = backend_clone.clone();
                let k = key_owned.clone();
                async move {
                    tracing::debug!("Retrieve: {} from {:?}", k, backend_type);
                    b.get(&k).await
                }
            })
            .await;

            match result {
                Ok(Some(data)) => {
                    let _ = self.cache.set(key, data.clone()).await;
                    return Ok(Some(data));
                }
                Ok(None) => continue,
                Err(err) => {
                    // One backend failed - try the next one
                    tracing::warn!("Backend {:?} failed: {}, trying next", backend_type, err);
                    continue;
                }
            }
        }

        Ok(None)
    }

    /// Delete data from all backends
    pub async fn delete_all(&self, key: &str) -> Result<()> {
        let backends_snapshot: Vec<(BackendType, Arc<dyn StorageBackend>)> = {
            let backends = self.backends.read().await;
            backends.iter().map(|(k, v)| (*k, v.clone())).collect()
        };

        let mut last_error: Option<crate::error::Error> = None;
        let key_owned = key.to_string();

        for (backend_type, backend) in backends_snapshot {
            let k = key_owned.clone();
            let backend_clone = backend.clone();

            let result = with_retry(&self.retry_config, || {
                let b = backend_clone.clone();
                let key_ref = k.clone();
                async move { b.delete(&key_ref).await }
            })
            .await;

            if let Err(err) = result {
                tracing::warn!("Delete failed on {:?}: {}", backend_type, err);
                last_error = Some(err);
            }
        }

        if let Some(err) = last_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    /// Get statistics
    pub async fn stats(&self) -> Result<StorageStats> {
        let snapshot = self.backends_snapshot().await;

        let mut stats = StorageStats::default();

        for (backend_type, backend) in &snapshot {
            if let Ok(backend_stats) = backend.stats().await {
                if *backend_type == BackendType::P2P {
                    stats.p2p_peer_count = backend_stats.peer_count;
                    stats.p2p_used_bytes = backend_stats.used_space;
                }
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
