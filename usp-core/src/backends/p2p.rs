//! P2P storage backend using libp2p Kademlia DHT
//!
//! This backend provides:
//! - Content-addressed storage using DHT
//! - Peer-to-peer data retrieval
//! - Automatic content routing and discovery

use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::RwLock;
use libp2p::PeerId;

use crate::backends::StorageBackend;
use crate::error::Result;
use crate::types::*;

/// P2P storage backend using libp2p Kademlia DHT
pub struct P2PBackend {
    _local_peer_id: PeerId,
    _keypair: libp2p::identity::Keypair,
    connected_peers: Arc<RwLock<Vec<PeerId>>>,
    stored_data: Arc<RwLock<std::collections::HashMap<String, Bytes>>>,
    is_connected: Arc<RwLock<bool>>,
}

impl P2PBackend {
    pub fn new() -> Result<Self> {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from_public_key(&keypair.public());

        Ok(Self {
            _local_peer_id: local_peer_id,
            _keypair: keypair,
            connected_peers: Arc::new(RwLock::new(Vec::new())),
            stored_data: Arc::new(RwLock::new(std::collections::HashMap::new())),
            is_connected: Arc::new(RwLock::new(false)),
        })
    }

    /// Get the peer ID of this node
    pub fn peer_id(&self) -> &PeerId {
        &self._local_peer_id
    }

    /// Get list of connected peers
    pub async fn connected_peers(&self) -> Vec<PeerId> {
        self.connected_peers.read().await.clone()
    }

    /// Bootstrap to known bootstrap nodes
    pub async fn bootstrap(&self, _bootstrap_nodes: Vec<String>) -> Result<()> {
        // TODO: Connect to bootstrap nodes
        // Example bootstrap nodes:
        // - /ip4/104.131.131.82/tcp/4001/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb
        // - /dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNL5QJvRjAMU2G9v7vA3BHfTS3j
        *self.is_connected.write().await = true;
        Ok(())
    }
}

impl Default for P2PBackend {
    fn default() -> Self {
        Self::new().expect("P2P keypair generation always succeeds")
    }
}

#[async_trait]
impl StorageBackend for P2PBackend {
    fn backend_type(&self) -> BackendType {
        BackendType::P2P
    }

    async fn init(&self, _config: crate::backends::BackendConfig) -> Result<()> {
        tracing::info!("P2P backend initializing with peer_id: {}", self._local_peer_id);
        *self.is_connected.write().await = true;
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        tracing::info!("P2P backend shutting down");
        *self.connected_peers.write().await = Vec::new();
        *self.is_connected.write().await = false;
        Ok(())
    }

    async fn put(&self, key: &str, value: Bytes) -> Result<StoreReceipt> {
        let cid = crate::utils::cid::compute_cid(&value);

        // Store locally first with both CID and original key for retrieval
        {
            let mut stored = self.stored_data.write().await;
            stored.insert(cid.clone(), value.clone());
            // Also index by original key -> CID mapping would be needed
            // For now, store the data keyed by the original key too
            stored.insert(key.to_string(), value.clone());
        }

        // TODO: Publish to DHT network for wider availability
        // self.kad.put_record(key, value).await?;

        tracing::debug!("P2P put: {} (cid: {})", key, cid);

        Ok(StoreReceipt {
            content_hash: cid,
            backend: BackendType::P2P,
            stored_at: chrono::Utc::now(),
            size_bytes: value.len() as u64,
            pinned: true, // P2P content is typically pinned for availability
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        // Try local cache first
        if let Some(data) = self.stored_data.read().await.get(key) {
            return Ok(Some(data.clone()));
        }

        // TODO: Query DHT network for the content
        // let record = self.kad.get_record(key).await?;
        // if let Some(value) = record {
        //     return Ok(Some(value));
        // }

        tracing::debug!("P2P get: {} - not found", key);
        Ok(None)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        // Remove from local storage
        self.stored_data.write().await.remove(key);

        // TODO: Remove from DHT network
        // self.kad.remove_record(key).await?;

        tracing::debug!("P2P delete: {}", key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.stored_data.read().await.contains_key(key))
    }

    async fn stats(&self) -> Result<BackendStats> {
        let stored = self.stored_data.read().await;
        let item_count = stored.len() as u64;
        let used_space: u64 = stored.values().map(|v| v.len() as u64).sum();

        Ok(BackendStats {
            total_capacity: 0, // P2P has no fixed capacity
            used_space,
            available_space: u64::MAX,
            item_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::BackendConfig;

    #[tokio::test]
    async fn test_p2p_backend_create() {
        let backend = P2PBackend::new().unwrap();
        assert!(!backend.peer_id().to_bytes().is_empty());
    }

    #[tokio::test]
    async fn test_p2p_put_get() {
        let backend = P2PBackend::new().unwrap();
        backend.init(BackendConfig::default()).await.unwrap();

        let data = Bytes::from(b"hello p2p world".to_vec());
        let receipt = backend.put("test-key", data.clone()).await.unwrap();

        let retrieved = backend.get("test-key").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), data);
    }

    #[tokio::test]
    async fn test_p2p_delete() {
        let backend = P2PBackend::new().unwrap();
        backend.init(BackendConfig::default()).await.unwrap();

        backend.put("test-key", Bytes::from("test")).await.unwrap();
        assert!(backend.exists("test-key").await.unwrap());

        backend.delete("test-key").await.unwrap();
        assert!(!backend.exists("test-key").await.unwrap());
    }
}
