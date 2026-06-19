//! Integration tests for USP Core

use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[tokio::test]
async fn test_cid_computation() {
    use usp_core::utils::cid::compute_cid;

    let data = b"test data";
    let cid1 = compute_cid(data);
    let cid2 = compute_cid(data);

    assert_eq!(cid1, cid2);
    assert!(cid1.starts_with("Qm"));
}

#[tokio::test]
async fn test_local_backend_basic() {
    use usp_core::backends::{LocalBackend, StorageBackend, BackendConfig};
    use tempfile::tempdir;

    let temp = tempdir().unwrap();
    let local = LocalBackend::new(temp.path().to_path_buf());

    local.init(BackendConfig::Default).await.unwrap();

    let data = Bytes::from("Hello, USP!");
    let receipt = local.put("test/key", data.clone()).await.unwrap();
    assert_eq!(receipt.size_bytes, 11);

    let retrieved = local.get("test/key").await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), data);

    local.delete("test/key").await.unwrap();
    assert!(!local.exists("test/key").await.unwrap());
}

#[tokio::test]
async fn test_p2p_backend_basic() {
    use usp_core::backends::{P2PBackend, StorageBackend, BackendConfig};

    let p2p = P2PBackend::new().unwrap();
    p2p.init(BackendConfig::Default).await.unwrap();

    let data = Bytes::from("P2P test");
    let receipt = p2p.put("p2p/key", data.clone()).await.unwrap();
    assert_eq!(receipt.backend, usp_core::types::BackendType::P2P);

    let retrieved = p2p.get("p2p/key").await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), data);
}

#[tokio::test]
async fn test_policy_engine_default_rules() {
    use usp_core::policy::PolicyEngine;
    use usp_core::types::{StorageOptions, StorageTier};

    let engine = PolicyEngine::new();

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

#[tokio::test]
async fn test_error_severity() {
    use usp_core::error::{Error, ErrorSeverity};

    // Transient errors
    let timeout = Error::Timeout(std::time::Duration::from_secs(1));
    assert!(timeout.is_retriable());
    assert_eq!(timeout.severity(), ErrorSeverity::Transient);

    let unavailable = Error::BackendUnavailable("server down".to_string());
    assert!(unavailable.is_retriable());
    assert_eq!(unavailable.severity(), ErrorSeverity::Transient);

    // Permanent errors
    let not_found = Error::KeyNotFound("missing".to_string());
    assert!(!not_found.is_retriable());
    assert_eq!(not_found.severity(), ErrorSeverity::Permanent);

    let backend_not_found = Error::BackendNotFound("S3".to_string());
    assert!(!backend_not_found.is_retriable());
}

#[tokio::test]
async fn test_retry_config() {
    use usp_core::utils::RetryConfig;

    let config = RetryConfig::default();
    assert_eq!(config.max_retries, 3);

    let none = RetryConfig::none();
    assert_eq!(none.max_retries, 0);

    let agg = RetryConfig::aggressive();
    assert_eq!(agg.max_retries, 5);

    let min = RetryConfig::minimal();
    assert_eq!(min.max_retries, 1);
}

#[tokio::test]
async fn test_storagehub_integration() {
    use usp_core::StorageHub;
    use usp_core::backends::{LocalBackend, StorageBackend, BackendConfig};
    use usp_core::types::StorageOptions;
    use tempfile::tempdir;

    let hub = StorageHub::new();

    // Register local backend
    let temp = tempdir().unwrap();
    let local = Arc::new(LocalBackend::new(temp.path().to_path_buf()));
    local.init(BackendConfig::Default).await.unwrap();
    hub.register_backend(local.clone()).await;

    // Put
    let data = Bytes::from("Test data for integration");
    let receipt = hub.put("integrated/key", data.clone(), StorageOptions::default()).await.unwrap();

    // Get
    let retrieved = hub.get("integrated/key").await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), data);

    // Exists
    assert!(hub.exists("integrated/key").await.unwrap());

    // Stats
    let stats = hub.stat().await.unwrap();
    assert!(!stats.backends.is_empty());

    // Delete
    hub.delete("integrated/key").await.unwrap();
    assert!(!hub.exists("integrated/key").await.unwrap());
}
