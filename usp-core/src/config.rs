//! Configuration management for USP
//!
//! Supports loading configuration from `.usp.toml` files and environment variables.
//!
//! # Config File Format
//!
//! ```toml
//! [storage]
//! data_dir = ".usp-data"
//!
//! [backends.local]
//! enabled = true
//! data_dir = ".usp-data"
//!
//! [backends.p2p]
//! enabled = true
//! listen_addresses = ["/ip4/0.0.0.0/tcp/0"]
//!
//! [backends.s3]
//! enabled = false
//! endpoint = "https://s3.amazonaws.com"
//! region = "us-east-1"
//! bucket = "my-bucket"
//!
//! [backends.decentralized]
//! enabled = false
//! gateway_url = "https://ipfs.io/ipfs/"
//! api_url = "http://127.0.0.1:5001"
//!
//! [policy]
//! default_backend = "local"
//! ```

use bytes::Bytes;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{env, fs};

use crate::backends::StorageBackend;
use crate::StorageHub;

/// Root configuration structure
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    /// Storage settings
    #[serde(default)]
    pub storage: StorageConfig,
    /// Backend configurations
    #[serde(default)]
    pub backends: BackendsConfig,
    /// Policy configuration
    #[serde(default)]
    pub policy: PolicyConfig,
}

/// General storage settings
#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// Data directory for local storage
    #[serde(default)]
    pub data_dir: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: ".usp-data".to_string(),
        }
    }
}

/// Backend-specific configurations
#[derive(Debug, Clone, Deserialize, Default)]
pub struct BackendsConfig {
    /// Local backend configuration
    #[serde(default)]
    pub local: LocalBackendConfig,
    /// P2P backend configuration
    #[serde(default)]
    pub p2p: P2PBackendConfig,
    /// S3 backend configuration
    #[serde(default)]
    pub s3: S3BackendConfig,
    /// Decentralized (IPFS) backend configuration
    #[serde(default)]
    pub decentralized: DecentralizedBackendConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LocalBackendConfig {
    /// Whether this backend is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Data directory
    #[serde(default)]
    pub data_dir: Option<String>,
}

fn default_enabled() -> bool {
    true
}

impl Default for LocalBackendConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            data_dir: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct P2PBackendConfig {
    /// Whether this backend is enabled
    #[serde(default)]
    pub enabled: bool,
    /// Listen addresses (e.g., "/ip4/0.0.0.0/tcp/0")
    #[serde(default)]
    pub listen_addresses: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3BackendConfig {
    /// Whether this backend is enabled
    #[serde(default)]
    pub enabled: bool,
    /// S3 endpoint URL
    #[serde(default)]
    pub endpoint: Option<String>,
    /// AWS region
    #[serde(default = "default_s3_region")]
    pub region: String,
    /// S3 bucket name
    #[serde(default)]
    pub bucket: Option<String>,
    /// Access key ID (or env var: USP_S3_ACCESS_KEY)
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Secret access key (or env var: USP_S3_SECRET_KEY)
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Path prefix for object keys
    #[serde(default)]
    pub path_prefix: Option<String>,
}

fn default_s3_region() -> String {
    "us-east-1".to_string()
}

impl Default for S3BackendConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            region: "us-east-1".to_string(),
            bucket: None,
            access_key_id: None,
            secret_access_key: None,
            path_prefix: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DecentralizedBackendConfig {
    /// Whether this backend is enabled
    #[serde(default)]
    pub enabled: bool,
    /// IPFS gateway URL
    #[serde(default = "default_gateway_url")]
    pub gateway_url: String,
    /// IPFS API URL
    #[serde(default = "default_api_url")]
    pub api_url: String,
}

fn default_gateway_url() -> String {
    "https://ipfs.io/ipfs/".to_string()
}

fn default_api_url() -> String {
    "http://127.0.0.1:5001".to_string()
}

impl Default for DecentralizedBackendConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            gateway_url: "https://ipfs.io/ipfs/".to_string(),
            api_url: "http://127.0.0.1:5001".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PolicyConfig {
    /// Default backend to use
    #[serde(default = "default_backend")]
    pub default_backend: String,
}

fn default_backend() -> String {
    "local".to_string()
}

impl Config {
    /// Load configuration from file and environment variables
    ///
    /// Priority (highest to lowest):
    /// 1. Environment variables
    /// 2. Config file values
    /// 3. Default values
    pub fn load() -> crate::Result<Self> {
        Self::load_from(Path::new(".usp.toml"))
    }

    /// Load configuration from a specific file path
    pub fn load_from(path: &Path) -> crate::Result<Self> {
        if !path.exists() {
            return Ok(Config::default());
        }

        let contents = fs::read_to_string(path)
            .map_err(|e| crate::Error::Storage(format!("failed to read config file: {}", e)))?;

        let config: Config = toml::from_str(&contents)
            .map_err(|e| crate::Error::Storage(format!("failed to parse config file: {}", e)))?;

        // Override with environment variables
        let config = config.apply_env_overrides();

        Ok(config)
    }

    /// Save configuration to a file
    pub fn save_to(&self, path: &Path) -> crate::Result<()> {
        let contents = toml::to_string_pretty(self)
            .map_err(|e| crate::Error::Storage(format!("failed to serialize config: {}", e)))?;
        fs::write(path, contents)
            .map_err(|e| crate::Error::Storage(format!("failed to write config file: {}", e)))?;
        Ok(())
    }

    /// Apply environment variable overrides
    fn apply_env_overrides(self) -> Self {
        // Storage
        let mut storage = self.storage;
        if let Ok(val) = env::var("USP_DATA_DIR") {
            storage.data_dir = val;
        }

        // S3
        let mut s3 = self.backends.s3;
        if let Ok(val) = env::var("USP_S3_ENDPOINT") {
            s3.endpoint = Some(val);
        }
        if let Ok(val) = env::var("USP_S3_REGION") {
            s3.region = val;
        }
        if let Ok(val) = env::var("USP_S3_BUCKET") {
            s3.bucket = Some(val);
        }
        if let Ok(val) = env::var("USP_S3_ACCESS_KEY") {
            s3.access_key_id = Some(val);
        }
        if let Ok(val) = env::var("USP_S3_SECRET_KEY") {
            s3.secret_access_key = Some(val);
        }

        // Decentralized
        let mut decentralized = self.backends.decentralized;
        if let Ok(val) = env::var("USP_IPFS_GATEWAY_URL") {
            decentralized.gateway_url = val;
        }
        if let Ok(val) = env::var("USP_IPFS_API_URL") {
            decentralized.api_url = val;
        }

        Self {
            storage,
            backends: BackendsConfig {
                local: self.backends.local,
                p2p: self.backends.p2p,
                s3,
                decentralized,
            },
            policy: self.policy,
        }
    }

    /// Get the effective data directory
    pub fn data_dir(&self) -> &str {
        &self.storage.data_dir
    }

    /// Initialize all enabled backends based on this configuration.
    ///
    /// This creates the necessary directories, establishes connections,
    /// and returns a `StorageHub` with all backends registered.
    /// If some backends fail to initialize, they are logged as warnings
    /// and the remaining backends are still registered.
    pub async fn init(&self) -> crate::Result<StorageHub> {
        let data_dir = PathBuf::from(self.effective_data_dir());
        let hub = StorageHub::with_data_dir(data_dir);
        let mut errors: Vec<String> = Vec::new();
        let mut initialized_any = false;

        // Local backend
        if self.backends.local.enabled {
            let data_dir = self.effective_data_dir();
            let backend = crate::backends::LocalBackend::new(std::path::Path::new(&data_dir));
            match backend
                .init(crate::backends::BackendConfig::Default)
                .await
            {
                Ok(()) => {
                    hub.register_backend(std::sync::Arc::new(backend)).await;
                    initialized_any = true;
                    tracing::info!("Initialized local backend (dir={})", data_dir);
                }
                Err(e) => {
                    let msg = format!("local backend: {}", e);
                    tracing::warn!("Failed to init local backend: {}", e);
                    errors.push(msg);
                }
            }
        }

        // P2P backend
        if self.backends.p2p.enabled {
            match crate::backends::P2PBackend::new() {
                Ok(backend) => match backend
                    .init(crate::backends::BackendConfig::Default)
                    .await
                {
                    Ok(()) => {
                        hub.register_backend(std::sync::Arc::new(backend)).await;
                        initialized_any = true;
                        tracing::info!("Initialized P2P backend");
                    }
                    Err(e) => {
                        let msg = format!("p2p backend: {}", e);
                        tracing::warn!("Failed to init P2P backend: {}", e);
                        errors.push(msg);
                    }
                },
                Err(e) => {
                    let msg = format!("create p2p backend: {}", e);
                    tracing::warn!("Failed to create P2P backend: {}", e);
                    errors.push(msg);
                }
            }
        }

        // S3 backend
        if self.backends.s3.enabled {
            let bucket = match &self.backends.s3.bucket {
                Some(b) => b.clone(),
                None => {
                    let msg = "s3: bucket not configured".to_string();
                    tracing::warn!("{}", msg);
                    errors.push(msg);
                    "".to_string()
                }
            };
            if !bucket.is_empty() {
                let backend = crate::backends::CloudS3Backend::new();
                let cfg = crate::backends::BackendConfig::CloudS3 {
                    endpoint: self.backends.s3.endpoint.clone(),
                    region: self.backends.s3.region.clone(),
                    bucket: bucket.clone(),
                    access_key_id: env::var("USP_S3_ACCESS_KEY")
                        .ok()
                        .or(self.backends.s3.access_key_id.clone())
                        .unwrap_or_default(),
                    secret_access_key: env::var("USP_S3_SECRET_KEY")
                        .ok()
                        .or(self.backends.s3.secret_access_key.clone())
                        .unwrap_or_default(),
                    path_prefix: self.backends.s3.path_prefix.clone(),
                };
                match backend.init(cfg).await {
                    Ok(()) => {
                        hub.register_backend(std::sync::Arc::new(backend)).await;
                        initialized_any = true;
                        tracing::info!(
                            "Initialized S3 backend (bucket={})",
                            bucket
                        );
                    }
                    Err(e) => {
                        let msg = format!("s3 backend: {}", e);
                        tracing::warn!("Failed to init S3 backend: {}", e);
                        errors.push(msg);
                    }
                }
            }
        }

        // Decentralized (IPFS) backend
        if self.backends.decentralized.enabled {
            let data_dir = std::path::PathBuf::from(&self.storage.data_dir).join(".decentralized");
            match crate::backends::DecentralizedStorage::new(
                &self.backends.decentralized.gateway_url,
                &self.backends.decentralized.api_url,
                data_dir.clone(),
            ) {
                Ok(backend) => match backend
                    .init(crate::backends::BackendConfig::Default)
                    .await
                {
                    Ok(()) => {
                        hub.register_backend(std::sync::Arc::new(backend)).await;
                        initialized_any = true;
                        tracing::info!(
                            "Initialized IPFS backend (api={})",
                            self.backends.decentralized.api_url
                        );
                    }
                    Err(e) => {
                        let msg = format!("ipfs backend: {}", e);
                        tracing::warn!("Failed to init ipfs backend: {}", e);
                        errors.push(msg);
                    }
                },
                Err(e) => {
                    let msg = format!("create ipfs backend: {}", e);
                    tracing::warn!("Failed to create ipfs backend: {}", e);
                    errors.push(msg);
                }
            }
        }

        // Load persisted metadata
        if let Err(e) = hub.load_metadata().await {
            tracing::warn!("Failed to load metadata: {}", e);
        }

        // If no backends were successfully initialized, return an error
        if !initialized_any {
            let err_msg = if errors.is_empty() {
                "no backends are enabled".to_string()
            } else {
                format!("all backends failed to initialize: {}", errors.join("; "))
            };
            return Err(crate::Error::Storage(err_msg));
        }

        // Log summary
        if !errors.is_empty() {
            tracing::warn!(
                "Some backends failed to initialize ({} errors): {}",
                errors.len(),
                errors.join("; ")
            );
        }

        Ok(hub)
    }

    /// Get the effective data directory (local backend dir overrides storage dir)
    fn effective_data_dir(&self) -> String {
        self.backends
            .local
            .data_dir
            .clone()
            .unwrap_or_else(|| self.storage.data_dir.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.storage.data_dir, ".usp-data");
        assert!(config.backends.local.enabled);
        assert!(!config.backends.p2p.enabled);
        assert!(!config.backends.s3.enabled);
        assert!(!config.backends.decentralized.enabled);
    }

    #[test]
    fn test_parse_config() {
        let toml_content = r#"
[storage]
data_dir = ".custom-data"

[backends.local]
enabled = true
data_dir = ".local-storage"

[backends.s3]
enabled = true
region = "eu-west-1"
bucket = "my-bucket"
"#;
        let config: Config = toml::from_str(toml_content).unwrap();
        assert_eq!(config.storage.data_dir, ".custom-data");
        assert!(config.backends.local.enabled);
        assert_eq!(
            config.backends.local.data_dir,
            Some(".local-storage".to_string())
        );
        assert!(config.backends.s3.enabled);
        assert_eq!(config.backends.s3.region, "eu-west-1");
        assert_eq!(config.backends.s3.bucket, Some("my-bucket".to_string()));
    }
}
