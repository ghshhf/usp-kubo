use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

use usp_core::types::*;
use usp_core::StorageHub;

mod daemon;

const DEFAULT_PID_FILE: &str = ".usp-daemon.pid";
const DEFAULT_DAEMON_ADDR: &str = "127.0.0.1:4222";

#[derive(Parser)]
#[command(version, about = "USP-Kubo - Unified Storage Platform CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Connect to daemon (auto-detect if running)
    #[arg(short, long)]
    daemon: bool,

    /// Daemon address
    #[arg(long, default_value = DEFAULT_DAEMON_ADDR)]
    daemon_addr: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Store a file
    Store {
        /// Key name
        key: String,
        /// Input file path
        file: PathBuf,
        /// Time to live in seconds (0 = permanent)
        #[arg(short, long, default_value = "0")]
        ttl: u64,
        /// Number of replicas
        #[arg(short, long, default_value = "1")]
        replicas: u32,
        /// Storage tier: hot, warm, cold, archive
        #[arg(long)]
        tier: Option<String>,
        /// Encrypt data (AES-256-GCM, requires USP_ENCRYPTION_KEY env var)
        #[arg(long)]
        encrypted: bool,
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

fn parse_tier(s: &str) -> Result<StorageTier> {
    match s.to_lowercase().as_str() {
        "hot" => Ok(StorageTier::Hot),
        "warm" => Ok(StorageTier::Warm),
        "cold" => Ok(StorageTier::Cold),
        "archive" => Ok(StorageTier::Archive),
        _ => anyhow::bail!(
            "Unknown tier: {}. Valid tiers: hot, warm, cold, archive",
            s
        ),
    }
}

/// Check if daemon is running by trying to connect.
fn should_use_daemon(daemon_flag: bool, addr: &str) -> bool {
    if !daemon_flag {
        // Auto-detect: try to connect to daemon
        return daemon::is_daemon_running(addr);
    }
    true
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Read encryption key from env var (base64 encoded, 32 bytes)
    let encrypt_key: Option<[u8; 32]> = std::env::var("USP_ENCRYPTION_KEY")
        .ok()
        .and_then(|s| {
            match base64::decode(&s) {
                Ok(bytes) if bytes.len() == 32 => {
                    let mut key = [0u8; 32];
                    key.copy_from_slice(&bytes);
                    Some(key)
                }
                Ok(bytes) => {
                    tracing::warn!(
                        "USP_ENCRYPTION_KEY must be 32 bytes (got {}), encryption disabled",
                        bytes.len()
                    );
                    None
                }
                Err(_) => {
                    tracing::warn!("USP_ENCRYPTION_KEY is not valid base64, encryption disabled");
                    None
                }
            }
        });

    // Init tracing: skip for Daemon (it inits its own with file output)
    if !matches!(&cli.command, Commands::Daemon { .. }) {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();
    }

    // Handle Daemon command
    if let Commands::Daemon { pid_file, addr } = &cli.command {
        return daemon::start_daemon(pid_file.clone(), addr.clone()).await;
    }

    // Handle StopDaemon command
    if let Commands::StopDaemon = &cli.command {
        return daemon::stop_daemon(DEFAULT_PID_FILE).await;
    }

    // Decide: use daemon client or local mode
    let use_daemon = should_use_daemon(cli.daemon, &cli.daemon_addr);

    if use_daemon {
        // Daemon client mode
        match &cli.command {
            Commands::Store { key, file, ttl, replicas, tier, encrypted } => {
                let tier_str = tier.as_deref().unwrap_or("");
                let params = serde_json::json!({
                    "key": key,
                    "file": file,
                    "ttl": ttl,
                    "replicas": replicas,
                    "tier": tier_str,
                    "encrypted": encrypted,
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
                    if let Some(t) = tier {
                        println!("  Tier: {}", t);
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
            Commands::Init { backend } => {
                // Init via daemon: just validate the backend type
                let _ = parse_backend(backend)?;
                println!("Backend '{}' is valid. Use 'usp init --backend {}' in local mode to configure.", backend, backend);
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
        let mut hub = config.init().await?;

        // Set encryption key if present
        if let Some(key) = encrypt_key {
            hub = hub.with_encryption_key(key);
        }

        match &cli.command {
            Commands::Store { key, file, ttl, replicas, tier, encrypted } => {
                let data = tokio::fs::read(file).await?;
                let bytes = Bytes::from(data);
                let tier_opt = if let Some(t) = tier {
                    Some(parse_tier(t)?)
                } else {
                    None
                };
                let opts = StorageOptions {
                    ttl_seconds: *ttl,
                    replicas: *replicas,
                    tier: tier_opt,
                    encrypted: *encrypted,
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
                if let Some(t) = tier {
                    println!("  Tier: {}", t);
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
                    println!("\nTotal: {} keys", keys.len());
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
                for (backend, s) in &stats.backends {
                    println!("{:?}:", backend);
                    println!("  Total capacity: {} bytes", s.total_capacity);
                    println!("  Used space: {} bytes", s.used_space);
                    println!("  Available: {} bytes", s.available_space);
                    println!("  Items: {}", s.item_count);
                    if s.peer_count > 0 {
                        println!("  Peers: {}", s.peer_count);
                    }
                }
                if stats.p2p_peer_count > 0 {
                    println!("\nP2P Network:");
                    println!("  Peers: {}", stats.p2p_peer_count);
                    println!("  Used (P2P): {} bytes", stats.p2p_used_bytes);
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
            Commands::Init { backend } => {
                let backend_type = parse_backend(backend)?;
                println!("Initializing backend: {:?}", backend_type);
                // TODO: implement interactive config initialization
                println!("Edit .usp.toml to configure this backend.");
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}
