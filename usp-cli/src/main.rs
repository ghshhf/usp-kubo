//! USP CLI - Unified Storage Platform command-line interface

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
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
    /// Initialize configuration and backends
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

/// Initialize backends from config and register with the hub.
/// Returns the configured hub (or error if no backends could be initialized).
async fn init_hub_from_config(config: &usp_core::config::Config, data_dir: &PathBuf) -> Result<StorageHub> {
    let hub = StorageHub::new();

    // Always try to init Local backend if enabled (or by default)
    if config.backends.local.enabled {
        let local_dir = config.backends.local.data_dir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.clone());
        let local = Arc::new(LocalBackend::new(local_dir));
        if let Err(e) = local.init(BackendConfig::Default).await {
            tracing::warn!("Failed to init local backend: {}", e);
        } else {
            hub.register_backend(local.clone()).await;
            tracing::debug!("Registered local backend");
        }
    }

    // Init P2P backend if enabled
    if config.backends.p2p.enabled {
        match P2PBackend::new() {
            Ok(p2p) => {
                if let Err(e) = p2p.init(BackendConfig::Default).await {
                    tracing::warn!("Failed to init P2P backend: {}", e);
                } else {
                    let bt = hub.register_backend(Arc::new(p2p)).await;
                    tracing::debug!("Registered {:?} backend", bt);
                }
            }
            Err(e) => tracing::warn!("Failed to create P2P backend: {}", e),
        }
    }

    // Init S3 backend if enabled
    if config.backends.s3.enabled {
        let s3_cfg = &config.backends.s3;
        if s3_cfg.bucket.is_none() {
            tracing::warn!("S3 backend enabled but no bucket configured; skipping");
        } else {
            let s3 = Arc::new(CloudS3Backend::new());
            let cfg = BackendConfig::CloudS3 {
                endpoint: s3_cfg.endpoint.clone(),
                region: s3_cfg.region.clone(),
                bucket: s3_cfg.bucket.clone().unwrap_or_default(),
                access_key_id: std::env::var("USP_S3_ACCESS_KEY")
                    .ok()
                    .or_else(|| s3_cfg.access_key_id.clone())
                    .unwrap_or_default(),
                secret_access_key: std::env::var("USP_S3_SECRET_KEY")
                    .ok()
                    .or_else(|| s3_cfg.secret_access_key.clone())
                    .unwrap_or_default(),
                path_prefix: s3_cfg.path_prefix.clone(),
            };
            if let Err(e) = s3.init(cfg).await {
                tracing::warn!("Failed to init S3 backend: {}", e);
            } else {
                let bt = hub.register_backend(s3.clone()).await;
                tracing::debug!("Registered {:?} backend", bt);
            }
        }
    }

    // Init Decentralized backend if enabled
    if config.backends.decentralized.enabled {
        let dec_dir = data_dir.join(".decentralized");
        match DecentralizedStorage::new(
            &config.backends.decentralized.gateway_url,
            &config.backends.decentralized.api_url,
            dec_dir,
        ) {
            Ok(dec) => {
                if let Err(e) = dec.init(BackendConfig::Default).await {
                    tracing::warn!("Failed to init decentralized backend: {}", e);
                } else {
                    let bt = hub.register_backend(Arc::new(dec)).await;
                    tracing::debug!("Registered {:?} backend", bt);
                }
            }
            Err(e) => tracing::warn!("Failed to create decentralized backend: {}", e),
        }
    }

    Ok(hub)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    // Handle Init command: write config file and initialize backends
    if let Commands::Init { backend } = &cli.command {
        let backend_type = parse_backend(backend)?;
        let mut config = usp_core::config::Config::default();

        match backend_type {
            BackendType::Local => {
                config.backends.local.enabled = true;
                config.backends.local.data_dir = Some(cli.data_dir.clone());
            }
            BackendType::P2P => {
                config.backends.p2p.enabled = true;
            }
            BackendType::CloudS3 => {
                config.backends.s3.enabled = true;
            }
            BackendType::Decentralized => {
                config.backends.decentralized.enabled = true;
            }
        }

        let config_path = std::path::Path::new(".usp.toml");
        config.save_to(config_path)?;
        println!("Configuration written to {}", config_path.display());

        // Actually initialize all enabled backends
        match config.init().await {
            Ok(_hub) => {
                println!("Backend {:?} initialized successfully.", backend_type);
                println!("You can now use 'usp store <key> <file>' to store data.");
            }
            Err(e) => {
                eprintln!("Warning: config written but backend init failed: {}", e);
                eprintln!("You can retry by running 'usp init' again.");
            }
        }
        return Ok(());
    }

    // Load config
    let config = usp_core::config::Config::load()
        .unwrap_or_else(|_| {
            tracing::debug!("No config file found, using defaults");
            usp_core::config::Config::default()
        });

    // Initialize hub with backends from config
    let hub = init_hub_from_config(&config, &cli.data_dir).await?;

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
            let keys = hub.list_keys().await;
            if keys.is_empty() {
                println!("No keys found. Use 'usp store <key> <file>' to store data.");
            } else {
                for key in &keys {
                    println!("{}", key);
                }
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
                if backend_stats.peer_count > 0 {
                    println!("  Peers: {}", backend_stats.peer_count);
                }
            }
            if stats.p2p_peer_count > 0 {
                println!("\nP2P Peers: {}", stats.p2p_peer_count);
            }
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
