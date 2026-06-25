//! USP CLI - Unified Storage Platform command-line interface

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
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

    // Load config and initialize all enabled backends
    let config = usp_core::config::Config::load()
        .unwrap_or_else(|_| {
            tracing::debug!("No config file found, using defaults");
            usp_core::config::Config::default()
        });
    let hub = config.init().await?;

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
