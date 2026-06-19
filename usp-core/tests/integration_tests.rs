//! Integration tests for USP Core

use bytes::Bytes;
use std::sync::Arc;

#[tokio::test]
async fn test_cid_computation() {
    use usp_core::utils::cid::compute_cid;

    let data = b"test data";
    let cid1 = compute_cid(data);
    let cid2 = compute_cid(data);

    // Same content should produce same CID
    assert_eq!(cid1, cid2);
    assert!(cid1.starts_with("Qm"));
}

#[tokio::test]
async fn test_local_backend_basic() {
    use usp_core::backends::{LocalBackend, StorageBackend, BackendConfig};
    use tempfile::tempdir;

    let temp = tempdir().unwrap();
    let local = LocalBackend::new(temp.path().to_path_buf());

    // Initialize
    local.init(BackendConfig::Default).await.unwrap();

    // Put data
    let data = Bytes::from("Hello, USP!");
    let receipt = local.put("test/key", data.clone()).await.unwrap();
    assert_eq!(receipt.size_bytes, 11); // "Hello, USP!" = 11 bytes

    // Get data
    let retrieved = local.get("test/key").await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), data);

    // Exists check
    assert!(local.exists("test/key").await.unwrap());

    // Delete
    local.delete("test/key").await.unwrap();
    assert!(!local.exists("test/key").await.unwrap());
}

#[tokio::test]
async fn test_p2p_backend_basic() {
    use usp_core::backends::{P2PBackend, StorageBackend, BackendConfig};

    let p2p = P2PBackend::new().unwrap();
    p2p.init(BackendConfig::Default).await.unwrap();

    // Put data
    let data = Bytes::from("P2P test");
    let receipt = p2p.put("p2p/key", data.clone()).await.unwrap();
    assert_eq!(receipt.backend, usp_core::types::BackendType::P2P);

    // Get data
    let retrieved = p2p.get("p2p/key").await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), data);
}

#[tokio::test]
async fn test_policy_engine_default_rules() {
    use usp_core::policy::PolicyEngine;
    use usp_core::types::{StorageOptions, StorageTier};

    let engine = PolicyEngine::new();

    // Small file should match Hot tier rule
    let opts = StorageOptions {
        ttl_seconds: 0,
        replicas: 1,
        tier: StorageTier::Warm,
        encrypted: false,
        tags: std::collections::HashMap::new(),
        backend_hint: None,
    };

    let backend = engine.decide("small.txt", &opts).unwrap();
    assert_eq!(backend, usp_core::types::BackendType::Local);
}
