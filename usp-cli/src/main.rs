//! USP CLI - Unified Storage Platform command-line interface

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use usp_core::backends::{
    BackendConfig, CloudS3Backend, DecentralizedStorage, LocalBackend, P2PBackend, StorageBackend,
};
use usp_core::types::{BackendType, StorageOptions};
use usp_core::StorageHub;

#[derive(Parser, Debug)]
#[command(name = "usp")]
#[command(
    version,
    about = "Unified Storage Platform - multi-backend storage CLI"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Data directory for local storage
    #[arg(short, long, default_value = ".usp-data")]
    data_dir: PathBuf,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Store a file
    Store {
        /// Key name
        key: String,
        /// File path to store
        file: PathBuf,
    },
    /// Retrieve a file
    Get {
        /// Key name
        key: String,
        /// Output file path
        output: PathBuf,
    },
    /// List all stored keys
    List,
    /// Delete a key
    Delete {
        /// Key name
        key: String,
    },
    /// Show storage statistics
    Stats,
    /// Pin a key
    Pin {
        /// Key name
        key: String,
    },
    /// Unpin a key
    Unpin {
        /// Key name
        key: String,
    },
    /// Initialize a backend (local, p2p, s3, decentralized)
    Init {
        /// Backend type: local, p2p, s3, decentralized
        #[arg(short, long, default_value = "local")]
        backend: String,
    },
}

fn parse_backend(s: &str) -> Result<BackendType> {
    match s.to_lowercase().as_str() {
        "local" => Ok(BackendType::Local),
        "p2p" => Ok(BackendType::P2P),
        "s3" => Ok(BackendType::CloudS3),
        "decentralized" => Ok(BackendType::Decentralized),
        _ => anyhow::bail!(
            "Unknown backend type: {}. Valid types: local, p2p, s3, decentralized",
            s
        ),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    // Initialize storage hub
    let hub = StorageHub::new();

    // Handle Init command separately (exits early)
    if let Commands::Init { backend } = &cli.command {
        let backend_type = parse_backend(backend)?;
        match backend_type {
            BackendType::Local => {
                let local = Arc::new(LocalBackend::new(cli.data_dir.clone()));
                local.init(BackendConfig::Default).await?;
                hub.register_backend(local.clone()).await;
                println!(
                    "Initialized Local backend with data dir: {:?}",
                    cli.data_dir
                );
            }
            BackendType::P2P => {
                let p2p = Arc::new(P2PBackend::new()?);
                p2p.init(BackendConfig::Default).await?;
                hub.register_backend(p2p.clone()).await;
                println!("Initialized P2P backend");
            }
            BackendType::CloudS3 => {
                let endpoint = std::env::var("USP_S3_ENDPOINT").ok();
                let region = std::env::var("USP_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
                let bucket = std::env::var("USP_S3_BUCKET").unwrap_or_else(|_| "usp-bucket".into());
                let access_key =
                    std::env::var("USP_S3_ACCESS_KEY").unwrap_or_else(|_| "test".into());
                let secret_key =
                    std::env::var("USP_S3_SECRET_KEY").unwrap_or_else(|_| "test".into());

                let s3 = Arc::new(CloudS3Backend::new());
                s3.init(BackendConfig::CloudS3 {
                    endpoint,
                    region: region.clone(),
                    bucket: bucket.clone(),
                    access_key_id: access_key,
                    secret_access_key: secret_key,
                    path_prefix: None,
                })
                .await?;
                hub.register_backend(s3.clone()).await;
                println!("Initialized S3 backend (bucket: {})", bucket);
            }
            BackendType::Decentralized => {
                let data_dir = cli.data_dir.join(".decentralized");
                let decentralized = Arc::new(
                    DecentralizedStorage::new(
                        "https://ipfs.io/ipfs/",
                        "http://127.0.0.1:5001",
                        data_dir,
                    )
                    .unwrap(),
                );
                decentralized.init(BackendConfig::Default).await?;
                hub.register_backend(decentralized.clone()).await;
                println!("Initialized Decentralized (IPFS) backend");
            }
        }
        println!("Backend initialized. Use 'usp store' to start storing data.");
        return Ok(());
    }

    // Register local backend by default for Store/Get/List/Delete operations
    let local = Arc::new(LocalBackend::new(cli.data_dir.clone()));
    local.init(BackendConfig::Default).await?;
    hub.register_backend(local.clone()).await;

    match &cli.command {
        Commands::Store { key, file } => {
            let data = tokio::fs::read(file).await?;
            let bytes = Bytes::from(data);
            let receipt = hub.put(key, bytes, StorageOptions::default()).await?;
            println!("Stored: {}", key);
            println!("  Backend: {:?}", receipt.backend);
            println!("  Size: {} bytes", receipt.size_bytes);
            println!("  CID: {}", receipt.content_hash);
        }
        Commands::Get { key, output } => match hub.get(key).await? {
            Some(data) => {
                tokio::fs::write(output, &data).await?;
                println!("Retrieved {} bytes to {:?}", data.len(), output);
            }
            None => {
                anyhow::bail!("Key not found: {}", key);
            }
        },
        Commands::List => {
            let data_dir = &cli.data_dir;
            if data_dir.exists() {
                list_keys_recursive(data_dir.as_path(), data_dir.as_path()).await?;
            } else {
                println!("No data stored yet.");
            }
        }
        Commands::Delete { key } => {
            hub.delete(key).await?;
            println!("Deleted: {}", key);
        }
        Commands::Stats => {
            let stats = hub.stat().await?;
            println!("Storage Statistics");
            println!("===================");
            for (backend_type, backend_stats) in &stats.backends {
                println!("\n{:?}:", backend_type);
                println!(
                    "  Total capacity: {:.2} GB",
                    backend_stats.total_capacity as f64 / 1_000_000_000.0
                );
                println!(
                    "  Used space: {:.2} MB",
                    backend_stats.used_space as f64 / 1_000_000.0
                );
                println!(
                    "  Available: {:.2} GB",
                    backend_stats.available_space as f64 / 1_000_000_000.0
                );
                println!("  Items: {}", backend_stats.item_count);
            }
            println!("\nP2P Peers: {}", stats.p2p_peer_count);
        }
        Commands::Pin { key } => {
            hub.pin(key).await?;
            println!("Pinned: {}", key);
        }
        Commands::Unpin { key } => {
            hub.unpin(key).await?;
            println!("Unpinned: {}", key);
        }
        Commands::Init { .. } => {
            unreachable!();
        }
    }

    Ok(())
}

/// Recursively list keys in the data directory
fn list_keys_sync(base: &Path, current: &Path) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for entry in std::fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let key = path
                .strip_prefix(base)
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .unwrap_or_else(|_| path.file_name().unwrap().to_string_lossy().to_string());
            keys.push(key);
        } else if path.is_dir() {
            keys.extend(list_keys_sync(base, &path)?);
        }
    }
    Ok(keys)
}

/// Recursively list keys in the data directory using async file operations
async fn list_keys_recursive(base: &Path, current: &Path) -> Result<()> {
    let keys = tokio::task::spawn_blocking({
        let base = base.to_path_buf();
        let current = current.to_path_buf();
        move || list_keys_sync(&base, &current)
    })
    .await??;

    for key in keys {
        println!("{}", key);
    }
    Ok(())
}
