//! Storage backends module
//!
//! Contains implementations for different storage backends:
//! - Local: Local filesystem storage
//! - P2P: IPFS/LibP2P network storage
//! - CloudS3: S3-compatible cloud storage
//! - Decentralized: Filecoin, Arweave, etc.

use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;

pub mod local;
pub mod p2p;
pub mod cloud;
pub mod decentralized;

pub use local::LocalBackend;
pub use p2p::P2PBackend;
pub use cloud::CloudS3Backend;
pub use decentralized::DecentralizedStorage;

pub use crate::types::{BackendType, BackendStats, StoreReceipt};
use crate::error::Result;

/// Backend configuration
#[derive(Debug, Clone, Default)]
pub enum BackendConfig {
    /// Default configuration
    #[default]
    Default,
    Local {
        data_dir: PathBuf,
        max_cache_size: u64,
    },
    P2P {
        listen_addresses: Vec<String>,
        bootstrap_peers: Vec<String>,
        data_dir: PathBuf,
    },
    CloudS3 {
        endpoint: Option<String>,
        region: String,
        bucket: String,
        access_key_id: String,
        secret_access_key: String,
        path_prefix: Option<String>,
    },
    Decentralized {
        backend: String,
        config: serde_json::Value,
    },
}

/// Storage backend trait - all backends must implement this
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Backend type identifier
    fn backend_type(&self) -> BackendType;

    /// Initialize backend
    async fn init(&self, config: BackendConfig) -> Result<()>;

    /// Shutdown backend
    async fn shutdown(&self) -> Result<()>;

    /// Store data
    async fn put(&self, key: &str, value: Bytes) -> Result<StoreReceipt>;

    /// Read data
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;

    /// Delete data
    async fn delete(&self, key: &str) -> Result<()>;

    /// Check if key exists
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Get backend statistics
    async fn stats(&self) -> Result<BackendStats>;
}
