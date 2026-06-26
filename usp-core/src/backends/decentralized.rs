//! Decentralized storage backend using IPFS HTTP API
//!
//! Supports storing and retrieving data via IPFS network.
//! Uses IPFS HTTP API for add/pin operations and IPFS Gateway for retrieval.
//!
//! # Offline Degradation
//!
//! When the IPFS node is unreachable, the backend operates in a degraded
//! mode: data is stored locally with the CID, and operations succeed without
//! requiring network connectivity.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use reqwest::{multipart, Client, Url};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backends::{BackendConfig, StorageBackend};
use crate::error::{Error, Result};
use crate::types::*;

/// IPFS HTTP API response for add operation
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct IpfsAddResponse {
    #[serde(rename = "Hash")]
    hash: String,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Size")]
    size: String,
}

/// Stored entry tracking key -> (cid, size_bytes)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StoredEntry {
    cid: String,
    size_bytes: u64,
}

/// Decentralized storage backend via IPFS HTTP API
#[derive(Debug, Clone)]
pub struct DecentralizedStorage {
    gateway_url: Url,
    api_url: Url,
    client: Client,
    /// Local data directory for persisting CID mappings and offline data
    data_dir: PathBuf,
    /// Track locally known CIDs and their sizes
    stored_cids: Arc<RwLock<HashMap<String, StoredEntry>>>,
    /// Track offline data (when IPFS is unreachable)
    offline_data: Arc<RwLock<HashMap<String, Bytes>>>,
    /// Whether IPFS node was reachable at last init
    is_online: Arc<RwLock<bool>>,
}

impl DecentralizedStorage {
    /// Create a new IPFS-backed decentralized storage
    ///
    /// # Arguments
    /// * `gateway_url` - IPFS gateway URL (e.g., `<https://ipfs.io/ipfs/>` or `http://localhost:8080/`)
    /// * `api_url` - IPFS API URL (e.g., `http://127.0.0.1:5001`)
    /// * `data_dir` - Local directory for persisting CID mappings
    pub fn new(gateway_url: &str, api_url: &str, data_dir: PathBuf) -> Result<Self> {
        let gateway_url = Url::parse(gateway_url)
            .map_err(|e| Error::Storage(format!("invalid gateway URL: {}", e)))?;
        let api_url =
            Url::parse(api_url).map_err(|e| Error::Storage(format!("invalid API URL: {}", e)))?;
        Ok(Self {
            gateway_url,
            api_url,
            client: Client::new(),
            data_dir,
            stored_cids: Arc::new(RwLock::new(HashMap::new())),
            offline_data: Arc::new(RwLock::new(HashMap::new())),
            is_online: Arc::new(RwLock::new(false)),
        })
    }

    /// Create with defaults: public IPFS gateway and local IPFS API
    pub fn with_defaults() -> Self {
        Self::new(
            "https://ipfs.io/ipfs/",
            "http://127.0.0.1:5001",
            PathBuf::from(".usp-decentralized"),
        )
        .expect("hardcoded URLs are valid")
    }

    /// Get the path to the CID mapping file
    fn cid_mapping_path(&self) -> PathBuf {
        self.data_dir.join("cid_mappings.json")
    }

    /// Load persisted CID mappings from disk
    async fn load_cid_mappings(&self) -> Result<()> {
        let path = self.cid_mapping_path();
        if path.exists() {
            let data = tokio::fs::read(&path).await?;
            let mappings: HashMap<String, StoredEntry> = serde_json::from_slice(&data)
                .map_err(|e| Error::Storage(format!("failed to parse CID mappings: {}", e)))?;
            let mut cids = self.stored_cids.write().await;
            *cids = mappings;
            tracing::info!("Loaded {} CID mappings from {}", cids.len(), path.display());
        }
        Ok(())
    }

    /// Persist CID mappings to disk
    async fn persist_cid_mappings(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.data_dir).await?;
        let cids = self.stored_cids.read().await;
        let data = serde_json::to_vec(&*cids)
            .map_err(|e| Error::Storage(format!("failed to serialize CID mappings: {}", e)))?;
        tokio::fs::write(self.cid_mapping_path(), data).await?;
        Ok(())
    }

    /// Add data to IPFS via the API endpoint
    async fn add_to_ipfs(&self, data: &[u8]) -> Result<String> {
        let form = multipart::Form::new().part(
            "file",
            multipart::Part::bytes(data.to_vec())
                .file_name("data")
                .mime_str("application/octet-stream")
                .map_err(|e| Error::Storage(format!("invalid multipart: {}", e)))?,
        );

        let url = self
            .api_url
            .join("/api/v0/add")
            .map_err(|e| Error::Storage(format!("failed to build IPFS API URL: {}", e)))?;

        let response = self
            .client
            .post(url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| Error::Network(format!("IPFS API request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Storage(format!(
                "IPFS add failed: {} - {}",
                status, body
            )));
        }

        let result: IpfsAddResponse = response
            .json()
            .await
            .map_err(|e| Error::Storage(format!("failed to parse IPFS response: {}", e)))?;

        Ok(result.hash)
    }

    /// Pin a CID on the local IPFS node
    async fn pin_cid(&self, cid: &str) -> Result<()> {
        let url = self
            .api_url
            .join("/api/v0/pin/add")
            .map_err(|e| Error::Storage(format!("failed to build IPFS pin URL: {}", e)))?;

        let response = self
            .client
            .post(url)
            .query(&[("arg", cid)])
            .send()
            .await
            .map_err(|e| Error::Network(format!("IPFS pin request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::warn!(
                "IPFS pin failed (may already be pinned): {} - {}",
                status,
                body
            );
        }

        Ok(())
    }

    /// Retrieve data from IPFS gateway
    async fn get_from_gateway(&self, cid: &str) -> Result<Option<Bytes>> {
        let url = self
            .gateway_url
            .join(cid)
            .map_err(|e| Error::Storage(format!("failed to build gateway URL: {}", e)))?;

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| Error::Network(format!("IPFS gateway request failed: {}", e)))?;

        match response.status().as_u16() {
            200 => {
                let bytes = response.bytes().await.map_err(|e| {
                    Error::Storage(format!("failed to read gateway response: {}", e))
                })?;
                Ok(Some(bytes))
            }
            404 | 400 => Ok(None),
            status => Err(Error::Storage(format!(
                "IPFS gateway returned unexpected status: {}",
                status
            ))),
        }
    }
}

impl Default for DecentralizedStorage {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[async_trait]
impl StorageBackend for DecentralizedStorage {
    fn backend_type(&self) -> BackendType {
        BackendType::Decentralized
    }

    async fn init(&self, config: BackendConfig) -> Result<()> {
        // Create data directory
        tokio::fs::create_dir_all(&self.data_dir).await?;

        // Load persisted CID mappings
        self.load_cid_mappings().await?;

        // Verify connectivity to IPFS API
        let url = self
            .api_url
            .join("/api/v0/version")
            .map_err(|e| Error::Storage(format!("failed to build version URL: {}", e)))?;

        match self.client.get(url).send().await {
            Ok(resp) if resp.status().is_success() => {
                *self.is_online.write().await = true;
                tracing::info!("IPFS connection verified");
            }
            Ok(resp) => {
                *self.is_online.write().await = false;
                tracing::warn!(
                    "IPFS API returned {} - decentralized storage in degraded mode",
                    resp.status()
                );
            }
            Err(e) => {
                *self.is_online.write().await = false;
                tracing::warn!(
                    "IPFS API unreachable: {} - decentralized storage in offline mode",
                    e
                );
            }
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        // Persist CID mappings on shutdown
        self.persist_cid_mappings().await?;
        Ok(())
    }

    async fn put(&self, key: &str, value: Bytes) -> Result<StoreReceipt> {
        let size_bytes = value.len() as u64;
        let is_online = *self.is_online.read().await;

        if is_online {
            // Online mode: store to IPFS
            match self.add_to_ipfs(&value).await {
                Ok(cid) => {
                    // Pin the CID on local node
                    if let Err(e) = self.pin_cid(&cid).await {
                        tracing::warn!("failed to pin CID {}: {}", cid, e);
                    }

                    // Update local tracking
                    {
                        let mut cids = self.stored_cids.write().await;
                        cids.insert(
                            key.to_string(),
                            StoredEntry {
                                cid: cid.clone(),
                                size_bytes,
                            },
                        );
                    }

                    // Persist mappings
                    self.persist_cid_mappings().await?;

                    return Ok(StoreReceipt {
                        content_hash: cid,
                        backend: BackendType::Decentralized,
                        stored_at: chrono::Utc::now(),
                        size_bytes,
                        pinned: true,
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        "IPFS add failed for {}, falling back to offline mode: {}",
                        key,
                        e
                    );
                    *self.is_online.write().await = false;
                    // Fall through to offline mode
                }
            }
        }

        // Offline mode: store locally
        let cid = crate::utils::cid::compute_cid(&value);

        // Store data locally
        {
            let mut offline = self.offline_data.write().await;
            offline.insert(key.to_string(), value.clone());
        }

        // Update local tracking with correct size
        {
            let mut cids = self.stored_cids.write().await;
            cids.insert(
                key.to_string(),
                StoredEntry {
                    cid: cid.clone(),
                    size_bytes,
                },
            );
        }

        // Persist mappings
        self.persist_cid_mappings().await?;

        tracing::debug!(
            "Stored {} ({} bytes) in offline mode, CID: {}",
            key,
            size_bytes,
            cid
        );

        Ok(StoreReceipt {
            content_hash: cid,
            backend: BackendType::Decentralized,
            stored_at: chrono::Utc::now(),
            size_bytes,
            pinned: false,
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        // Check offline cache first
        {
            let offline = self.offline_data.read().await;
            if let Some(data) = offline.get(key) {
                return Ok(Some(data.clone()));
            }
        }

        // Look up CID for key
        let (cid, _size) = {
            let cids = self.stored_cids.read().await;
            match cids.get(key) {
                Some(entry) => (entry.cid.clone(), entry.size_bytes),
                None => {
                    // Try to construct CID from key directly (if key IS the CID)
                    if crate::utils::cid::is_valid_cid(key) {
                        (key.to_string(), 0)
                    } else {
                        return Ok(None);
                    }
                }
            }
        };

        // Try to get from gateway
        match self.get_from_gateway(&cid).await {
            Ok(Some(data)) => {
                // Cache locally for future offline access
                let mut offline = self.offline_data.write().await;
                offline.insert(key.to_string(), data.clone());
                Ok(Some(data))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                tracing::warn!(
                    "Failed to get {} from IPFS gateway, key not found: {}",
                    key,
                    e
                );
                Ok(None)
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        // Remove from local tracking
        self.stored_cids.write().await.remove(key);

        // Remove from offline cache
        self.offline_data.write().await.remove(key);

        // Persist mappings
        self.persist_cid_mappings().await?;

        // Note: IPFS content is permanent; we only remove local tracking
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        // Check offline cache
        {
            let offline = self.offline_data.read().await;
            if offline.contains_key(key) {
                return Ok(true);
            }
        }

        // Check CID mappings
        Ok(self.stored_cids.read().await.contains_key(key))
    }

    async fn stats(&self) -> Result<BackendStats> {
        let cids = self.stored_cids.read().await;
        let item_count = cids.len() as u64;
        // Calculate actual used space from stored sizes
        let used_space: u64 = cids.values().map(|e| e.size_bytes).sum();

        Ok(BackendStats {
            total_capacity: u64::MAX,
            used_space,
            available_space: u64::MAX,
            item_count,
        })
    }

    async fn list_keys(&self) -> Result<Vec<String>> {
        let cids = self.stored_cids.read().await;
        let mut keys: Vec<String> = cids.keys().cloned().collect();
        keys.sort();
        Ok(keys)
    }
}


    async fn pin(&self, key: &str) -> Result<()> {
        let cid = {
            let cids = self.stored_cids.read().await;
            match cids.get(key) {
                Some(entry) => entry.cid.clone(),
                None => return Err(Error::KeyNotFound(key.to_string())),
            }
        };
        self.pin_cid(&cid).await
    }

    async fn unpin(&self, key: &str) -> Result<()> {
        let cid = {
            let cids = self.stored_cids.read().await;
            match cids.get(key) {
                Some(entry) => entry.cid.clone(),
                None => return Err(Error::KeyNotFound(key.to_string())),
            }
        };
        let url = self.api_url.join("/api/v0/pin/rm")
            .map_err(|e| Error::Storage(format!("failed to build pin/rm URL: {}", e)))?;
        match self.client.post(url).query(&[("arg", &cid)]).send().await {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(_) => Ok(()),
            Err(e) => { tracing::warn!("IPFS unpin failed: {}", e); Ok(()) }
        }
    }

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_ipfs_url_construction() {
        let temp = tempdir().unwrap();
        let storage = DecentralizedStorage::new(
            "https://ipfs.io/ipfs/",
            "http://127.0.0.1:5001",
            temp.path().to_path_buf(),
        )
        .unwrap();

        // Verify URLs are properly constructed
        let add_url = storage.api_url.join("/api/v0/add").unwrap();
        assert_eq!(add_url.as_str(), "http://127.0.0.1:5001/api/v0/add");

        let gateway_url = storage.gateway_url.join("QmTest").unwrap();
        assert_eq!(gateway_url.as_str(), "https://ipfs.io/ipfs/QmTest");
    }

    #[tokio::test]
    async fn test_cid_tracking_with_sizes() {
        let temp = tempdir().unwrap();
        let storage = DecentralizedStorage::new(
            "https://ipfs.io/ipfs/",
            "http://127.0.0.1:5001",
            temp.path().to_path_buf(),
        )
        .unwrap();

        // Simulate storing entries with correct sizes
        storage.stored_cids.write().await.insert(
            "key1".to_string(),
            StoredEntry {
                cid: "QmTestCID1".to_string(),
                size_bytes: 1024,
            },
        );
        storage.stored_cids.write().await.insert(
            "key2".to_string(),
            StoredEntry {
                cid: "QmTestCID2".to_string(),
                size_bytes: 2048,
            },
        );

        assert!(storage.stored_cids.read().await.contains_key("key1"));
        assert_eq!(
            storage.stored_cids.read().await.get("key1").cloned(),
            Some(StoredEntry {
                cid: "QmTestCID1".to_string(),
                size_bytes: 1024
            })
        );

        // Verify stats calculates correctly
        let stats = storage.stats().await.unwrap();
        assert_eq!(stats.item_count, 2);
        assert_eq!(stats.used_space, 3072); // 1024 + 2048
    }

    #[tokio::test]
    async fn test_offline_data_storage() {
        let temp = tempdir().unwrap();
        let storage = DecentralizedStorage::new(
            "https://ipfs.io/ipfs/",
            "http://127.0.0.1:5001",
            temp.path().to_path_buf(),
        )
        .unwrap();

        // Simulate offline storage
        storage
            .offline_data
            .write()
            .await
            .insert("offline-key".to_string(), Bytes::from("test data"));

        // Verify it can be retrieved
        let offline = storage.offline_data.read().await;
        assert_eq!(
            offline.get("offline-key").cloned(),
            Some(Bytes::from("test data"))
        );
    }
}
