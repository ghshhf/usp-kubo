//! Core types for USP

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Storage options - determines how data is stored
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageOptions {
    /// Time to live in seconds (0 = permanent)
    pub ttl_seconds: u64,

    /// Number of replicas
    pub replicas: u32,

    /// Storage tier preference
    pub tier: StorageTier,

    /// Whether to encrypt
    pub encrypted: bool,

    /// Custom tags for policy matching
    pub tags: HashMap<String, String>,

    /// Force specific backend
    pub backend_hint: Option<BackendType>,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            ttl_seconds: 0,
            replicas: 1,
            tier: StorageTier::Warm,
            encrypted: false,
            tags: HashMap::new(),
            backend_hint: None,
        }
    }
}

/// Storage tier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum StorageTier {
    /// Hot data: local only
    #[default]
    Hot,
    /// Warm data: local + P2P
    Warm,
    /// Cold data: cloud storage
    Cold,
    /// Archive data: decentralized storage
    Archive,
}

/// Backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum BackendType {
    /// Local filesystem
    #[default]
    Local,
    /// P2P network (IPFS/LibP2P)
    P2P,
    /// S3-compatible cloud storage
    CloudS3,
    /// Decentralized storage
    Decentralized,
}

/// Storage receipt - result of a store operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreReceipt {
    /// Content hash (CID format)
    pub content_hash: String,

    /// Actual backend used
    pub backend: BackendType,

    /// Storage timestamp
    pub stored_at: DateTime<Utc>,

    /// Data size in bytes
    pub size_bytes: u64,

    /// Pin status
    pub pinned: bool,
}

/// Backend statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackendStats {
    /// Total capacity in bytes
    pub total_capacity: u64,

    /// Used space in bytes
    pub used_space: u64,

    /// Available space in bytes
    pub available_space: u64,

    /// Number of stored items
    pub item_count: u64,
}

/// Storage statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageStats {
    /// Per-backend statistics
    pub backends: HashMap<BackendType, BackendStats>,

    /// P2P peer count
    pub p2p_peer_count: u32,

    /// P2P used bytes
    pub p2p_used_bytes: u64,
}
