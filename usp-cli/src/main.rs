//! USP CLI - Unified Storage Platform command-line interface

pub mod daemon;

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use usp_core::types::{BackendType, StorageOptions};
use usp_core::StorageHub;

const DEFAULT_DAEMON_ADDR: &str = "127.0.0.1:4222";
const DEFAULT_PID_FILE: &str = ".usp-daemon.pid";

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

    /// Connect to running daemon (auto-detect by default)
    #[arg(long)]
    daemon: bool,

    /// Daemon address (for client mode)
    #[arg(long, default_value = DEFAULT_DAEMON_ADDR)]
    daemon_addr: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Store a file
    Store {
        /// Key name
        key: String,
        /// File path to store
        file: PathBuf,
        /// TTL in seconds (0 = permanent)
        #[arg(short, long, default_value = "0")]
        ttl: u64,
        /// Number of replicas
        #[arg(short, long, default_value = "1")]
        replicas: u32,
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
    /// Pin a key (prevent GC)
    Pin {
        /// Key name
        key: String,
    },
    /// Unpin a key
    Unpin {
        /// Key name
        key: String,
    },
    /// Garbage collect expired data
    Gc,
    /// Run as daemon (for P2P backend)
    Daemon {
        /// PID file path
        #[arg(short, long, default_value = DEFAULT_PID_FILE)]
        pid_file: PathBuf,
        /// Listen address
        #[arg(long, default_value = DEFAULT_DAEMON_ADDR)]
        addr: String,
    },
    /// Stop the running daemon
    StopDaemon,
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

/// Check if daemon is running and we should use it.
fn should_use_daemon(daemon_flag: bool, pid_file: &str) -> bool {
    if daemon_flag {
        return true;
    }
    // Auto-detect: check if PID file exists
    daemon::is_daemon_running(pid_file)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    // Handle Daemon command
    if let Commands::Daemon { pid_file, addr } = &cli.command {
        return daemon::start_daemon(pid_file.clone(), addr.clone()).await;
    }

    // Handle StopDaemon command
    if let Commands::StopDaemon = &cli.command {
        return daemon::stop_daemon(DEFAULT_PID_FILE).await;
    }

    // Handle Init command
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

    // Decide whether to use daemon or local mode
    let use_daemon = should_use_daemon(cli.daemon, DEFAULT_PID_FILE);

    if use_daemon {
        // Daemon mode: send requests to daemon via TCP
        println!("Using daemon at {}", cli.daemon_addr);

        match &cli.command {
            Commands::Store { key, file, ttl, replicas } => {
                let params = serde_json::json!({
                    "key": key,
                    "file": file,
                    "ttl": ttl,
                    "replicas": replicas,
                });
                let resp = daemon::send_to_daemon(&cli.daemon_addr, "put", params).await?;
                if let Some(err) = resp.error {
                    anyhow::bail!("Daemon error: {}", err);
                }
                if let Some(result) = resp.result {
                    println!("Stored: {}", key);
                    if let Some(backend) = result.get("backend") {
                        println!("  Backend: {}", backend);
                    }
                    if let Some(size) = result.get("size_bytes") {
                        println!("  Size: {} bytes", size);
                    }
                    if let Some(hash) = result.get("content_hash") {
                        println!("  CID: {}", hash);
                    }
                    if *replicas > 1 {
                        println!("  Replicas: {}", replicas);
                    }
                    if *ttl > 0 {
                        println!("  TTL: {}s", ttl);
                    }
                }
            }
            Commands::Get { key, output } => {
                let params = serde_json::json!({
                    "key": key,
                    "output": output,
                });
                let resp = daemon::send_to_daemon(&cli.daemon_addr, "get", params).await?;
                if let Some(err) = resp.error {
                    anyhow::bail!("Daemon error: {}", err);
                }
                println!("Retrieved {} to {:?}", key, output);
            }
            Commands::List => {
                let params = serde_json::json!({});
                let resp = daemon::send_to_daemon(&cli.daemon_addr, "list_keys", params).await?;
                if let Some(err) = resp.error {
                    anyhow::bail!("Daemon error: {}", err);
                }
                if let Some(result) = resp.result {
                    if let Some(keys) = result.get("keys").and_then(|v| v.as_array()) {
                        if keys.is_empty() {
                            println!("No keys found.");
                        } else {
                            for k in keys {
                                println!("{}", k.as_str().unwrap_or("?"));
                            }
                        }
                    }
                }
            }
            Commands::Delete { key } => {
                let params = serde_json::json!({ "key": key });
                let resp = daemon::send_to_daemon(&cli.daemon_addr, "delete", params).await?;
                if let Some(err) = resp.error {
                    anyhow::bail!("Daemon error: {}", err);
                }
                println!("Deleted: {}", key);
            }
            Commands::Stats => {
                let params = serde_json::json!({});
                let resp = daemon::send_to_daemon(&cli.daemon_addr, "stat", params).await?;
                if let Some(err) = resp.error {
                    anyhow::bail!("Daemon error: {}", err);
                }
                if let Some(result) = resp.result {
                    println!("Storage Statistics");
                    println!("===================");
                    // Pretty-print the stats JSON
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
            }
            Commands::Pin { key } => {
                let params = serde_json::json!({ "key": key });
                let resp = daemon::send_to_daemon(&cli.daemon_addr, "pin", params).await?;
                if let Some(err) = resp.error {
                    anyhow::bail!("Daemon error: {}", err);
                }
                println!("Pinned: {}", key);
            }
            Commands::Unpin { key } => {
                let params = serde_json::json!({ "key": key });
                let resp = daemon::send_to_daemon(&cli.daemon_addr, "unpin", params).await?;
                if let Some(err) = resp.error {
                    anyhow::bail!("Daemon error: {}", err);
                }
                println!("Unpinned: {}", key);
            }
            Commands::Gc => {
                let params = serde_json::json!({});
                let resp = daemon::send_to_daemon(&cli.daemon_addr, "gc", params).await?;
                if let Some(err) = resp.error {
                    anyhow::bail!("Daemon error: {}", err);
                }
                if let Some(result) = resp.result {
                    if let Some(deleted) = result.get("deleted").and_then(|v| v.as_u64()) {
                        println!("GC complete: {} expired keys deleted", deleted);
                    }
                }
            }
            _ => unreachable!(),
        }
    } else {
        // Local mode: create StorageHub directly
        let config = usp_core::config::Config::load()
            .unwrap_or_else(|_| {
                tracing::debug!("No config file found, using defaults");
                usp_core::config::Config::default()
            });
        let hub = config.init().await?;

        match &cli.command {
            Commands::Store { key, file, ttl, replicas } => {
                let data = tokio::fs::read(file).await?;
                let bytes = Bytes::from(data);
                let opts = StorageOptions {
                    ttl_seconds: *ttl,
                    replicas: *replicas,
                    ..StorageOptions::default()
                };
                let receipt = hub.put(key, bytes, opts).await?;
                println!("Stored: {}", key);
                println!("  Backend: {:?}", receipt.backend);
                println!("  Size: {} bytes", receipt.size_bytes);
                println!("  CID: {}", receipt.content_hash);
                if *replicas > 1 {
                    println!("  Replicas: {}", *replicas);
                }
                if *ttl > 0 {
                    println!("  TTL: {}s", ttl);
                }
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
            Commands::Gc => {
                let deleted = hub.gc().await?;
                println!("GC complete: {} expired keys deleted", deleted);
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}
