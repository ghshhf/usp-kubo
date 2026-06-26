//! Storage Hub - unified storage interface

use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;

use crate::backends::StorageBackend;
use crate::error::{Error, Result};
use crate::router::BackendRouter;
use crate::types::*;
use crate::utils::RetryConfig;

/// Per-key metadata for TTL / GC.
/// Persisted as `.usp-metadata.json` in the data directory.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StoreMeta {
    created_at: DateTime<Utc>,
    ttl_seconds: u64,
    content_hash: String,
    size_bytes: u64,
    backend: BackendType,
}

/// Unified Storage Hub - main entry point for all storage operations
pub struct StorageHub {
    router: Arc<BackendRouter>,
    pinned_keys: Arc<RwLock<HashSet<String>>>,
    /// In-memory metadata index (key -> StoreMeta)
    metadata: Arc<RwLock<HashMap<String, StoreMeta>>>,
    /// Directory for persisting metadata (.usp-metadata.json)
    data_dir: PathBuf,
}

impl StorageHub {
    /// Create a new StorageHub with default config
    pub fn new() -> Self {
        let router = BackendRouter::new();
        let data_dir = PathBuf::from(".usp-data");
        Self {
            router: Arc::new(router),
            pinned_keys: Arc::new(RwLock::new(HashSet::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            data_dir,
        }
    }

    /// Create with custom retry configuration
    pub fn with_retry_config(config: RetryConfig) -> Self {
        let router = BackendRouter::with_retry_config(config);
        let data_dir = PathBuf::from(".usp-data");
        Self {
            router: Arc::new(router),
            pinned_keys: Arc::new(RwLock::new(HashSet::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            data_dir,
        }
    }

    /// Create with explicit data directory (for metadata persistence)
    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        let router = BackendRouter::new();
        Self {
            router: Arc::new(router),
            pinned_keys: Arc::new(RwLock::new(HashSet::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            data_dir,
        }
    }

    /// Load persisted metadata from `.usp-metadata.json`.
    async fn load_metadata(&self) -> Result<()> {
        let path = self.data_dir.join(".usp-metadata.json");
        if !path.exists() {
            return Ok(());
        }
        let bytes = fs::read(path).await.map_err(|e| Error::Storage(format!("load metadata: {}", e)))?;
        let map: HashMap<String, StoreMeta> = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Storage(format!("parse metadata: {}", e)))?;
        let mut metadata = self.metadata.write().await;
        *metadata = map;
        tracing::debug!("Loaded metadata for {} keys", metadata.len());
        Ok(())
    }

    /// Persist metadata to `.usp-metadata.json`.
    async fn save_metadata(&self) -> Result<()> {
        let path = self.data_dir.join(".usp-metadata.json");
        // Ensure data_dir exists
        if !self.data_dir.exists() {
            fs::create_dir_all(&self.data_dir).await
                .map_err(|e| Error::Storage(format!("create data dir: {}", e)))?;
        }
        let metadata = self.metadata.read().await;
        let bytes = serde_json::to_vec_pretty(&*metadata)
            .map_err(|e| Error::Storage(format!("serialize metadata: {}", e)))?;
        fs::write(&path, &bytes).await
            .map_err(|e| Error::Storage(format!("write metadata: {}", e)))?;
        Ok(())
    }

    /// Register a storage backend and return its type.
    pub async fn register_backend(&self, backend: Arc<dyn StorageBackend>) -> BackendType {
        self.router.register_backend(backend).await
    }

    /// Store data with replica support.
    /// If opts.replicas > 1, writes to multiple backends.
    pub async fn put(&self, key: &str, value: Bytes, opts: StorageOptions) -> Result<StoreReceipt> {
        tracing::debug!("PUT {} (replicas={})", key, opts.replicas);

        let replicas = opts.replicas.max(1) as usize;
        let backends_snapshot = self.router.backends_snapshot().await;

        if backends_snapshot.is_empty() {
            return Err(Error::Storage("no backends registered".to_string()));
        }

        // Write to primary backend (selected by policy or backend_hint)
        let primary_receipt = self.router.store(key, value.clone(), opts.clone()).await?;

        // Persist metadata for TTL/GC
        let meta = StoreMeta {
            created_at: primary_receipt.stored_at,
            ttl_seconds: opts.ttl_seconds,
            content_hash: primary_receipt.content_hash.clone(),
            size_bytes: primary_receipt.size_bytes,
            backend: primary_receipt.backend,
        };
        {
            let mut metadata = self.metadata.write().await;
            metadata.insert(key.to_string(), meta);
        }
        let _ = self.save_metadata().await;

        // Write to additional backends for replicas
        if replicas > 1 {
            let mut written = 1usize;
            for (backend_type, _) in &backends_snapshot {
                if written >= replicas {
                    break;
                }
                if *backend_type == primary_receipt.backend {
                    continue;
                }
                let replica_opts = StorageOptions {
                    backend_hint: Some(*backend_type),
                    ..opts.clone()
                };
                match self.router.store(key, value.clone(), replica_opts).await {
                    Ok(_) => {
                        tracing::debug!("Replica {} written to {:?}", key, backend_type);
                        written += 1;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to write replica to {:?}: {}", backend_type, e);
                    }
                }
            }
        }

        Ok(primary_receipt)
    }

    /// Read data. If TTL is set and expired, deletes the key and returns None.
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        tracing::debug!("GET {}", key);

        // Check TTL before returning data
        if self.is_expired(key).await? {
            tracing::debug!("Key {} expired, deleting", key);
            let _ = self.delete(key).await;
            return Ok(None);
        }

        self.router.retrieve(key).await
    }

    /// Check if a key is expired based on its metadata.
    async fn is_expired(&self, key: &str) -> Result<bool> {
        let metadata = self.metadata.read().await;
        if let Some(meta) = metadata.get(key) {
            if meta.ttl_seconds == 0 {
                return Ok(false); // No TTL
            }
            let now = Utc::now();
            let elapsed = (now - meta.created_at).num_seconds() as u64;
            Ok(elapsed >= meta.ttl_seconds)
        } else {
            Ok(false) // No metadata, assume not expired
        }
    }

    /// Delete data from all registered backends
    pub async fn delete(&self, key: &str) -> Result<()> {
        tracing::debug!("DELETE {}", key);
        self.router.delete_all(key).await?;

        // Also remove metadata
        let mut metadata = self.metadata.write().await;
        metadata.remove(key);
        let _ = self.save_metadata().await;

        Ok(())
    }

    /// Check if key exists in any backend
    pub async fn exists(&self, key: &str) -> Result<bool> {
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
        // Propagate pin to all backends
        let backends = self.router.backends_snapshot().await;
        for (_, backend) in &backends {
            if let Err(e) = backend.pin(key).await {
                tracing::warn!("Failed to pin {} on {:?}: {}", key, backend.backend_type(), e);
            }
        }
        Ok(())
    }

    /// Unpin data
    pub async fn unpin(&self, key: &str) -> Result<()> {
        tracing::debug!("UNPIN {}", key);
        self.pinned_keys.write().await.remove(key);
        // Propagate unpin to all backends
        let backends = self.router.backends_snapshot().await;
        for (_, backend) in &backends {
            if let Err(e) = backend.unpin(key).await {
                tracing::warn!("Failed to unpin {} on {:?}: {}", key, backend.backend_type(), e);
            }
        }
        Ok(())
    }

    /// Check if data is pinned
    pub async fn is_pinned(&self, key: &str) -> Result<bool> {
        Ok(self.pinned_keys.read().await.contains(key))
    }

    /// Garbage collect expired data based on TTL.
    /// Pinned keys are never deleted.
    /// Returns the number of deleted keys.
    pub async fn gc(&self) -> Result<u64> {
        tracing::info!("Starting GC scan");
        let mut deleted = 0u64;

        let keys: Vec<String> = {
            let metadata = self.metadata.read().await;
            metadata.keys().cloned().collect()
        };

        let now = Utc::now();
        let pinned = self.pinned_keys.read().await;

        for key in &keys {
            // Skip pinned keys
            if pinned.contains(key) {
                continue;
            }

            let (ttl_seconds, created_at) = {
                let metadata = self.metadata.read().await;
                match metadata.get(key) {
                    Some(meta) => (meta.ttl_seconds, meta.created_at),
                    None => continue,
                }
            };

            if ttl_seconds == 0 {
                continue; // No TTL, skip
            }

            let elapsed = (now - created_at).num_seconds() as u64;
            if elapsed >= ttl_seconds {
                tracing::debug!("GC: deleting expired key {}", key);
                if let Err(e) = self.delete(key).await {
                    tracing::warn!("GC: failed to delete {}: {}", key, e);
                } else {
                    deleted += 1;
                }
            }
        }

        tracing::info!("GC complete: {} keys deleted", deleted);
        Ok(deleted)
    }
}

impl Default for StorageHub {
    fn default() -> Self {
        Self::new()
    }
}
