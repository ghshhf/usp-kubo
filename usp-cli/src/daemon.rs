//! Daemon module for USP
//!
//! Provides a long-running daemon process that hosts the StorageHub,
//! keeping P2P libp2p Swarm alive across CLI invocations.
//!
//! Communication protocol: length-prefixed JSON over TCP.
//! Default listen address: 127.0.0.1:4222

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::CancellationToken;
use usp_core::StorageHub;

const DEFAULT_DAEMON_ADDR: &str = "127.0.0.1:4222";
const PID_FILE: &str = ".usp-daemon.pid";
const LOG_FILE: &str = ".usp-daemon.log";

/// JSON-RPC style request
#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonRequest {
    pub method: String,
    pub params: serde_json::Value,
}

/// JSON-RPC style response
#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonResponse {
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

// ---- PID file helpers ----

/// Check if a PID file is valid (process is running).
/// On Windows: use tasklist to check.
/// On Unix: use kill(pid, 0) to check.
fn is_pid_alive(pid: u32) -> bool {
    #[cfg(target_os = "windows")]
    {
        use std::process::Command;
        let output = Command::new("tasklist")
            .args(&["/FI", &format!("PID eq {}", pid), "/NH"])
            .output();
        match output {
            Ok(o) => {
                let stdout = String::from_utf8_lossy(&o.stdout);
                stdout.contains(&pid.to_string())
            }
            Err(_) => false,
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        // send signal 0 to check if process exists
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
}

/// Read PID from file and check if the process is still running.
/// Returns `Some(pid)` if alive, `None` if stale.
/// If stale, removes the PID file.
fn check_pid_file(pid_file: &Path) -> Option<u32> {
    if !pid_file.exists() {
        return None;
    }
    let contents = match std::fs::read_to_string(pid_file) {
        Ok(c) => c,
        Err(_) => {
            let _ = std::fs::remove_file(pid_file);
            return None;
        }
    };
    let pid: u32 = match contents.trim().parse() {
        Ok(p) => p,
        Err(_) => {
            let _ = std::fs::remove_file(pid_file);
            return None;
        }
    };
    if is_pid_alive(pid) {
        Some(pid)
    } else {
        // Stale PID file, remove it
        let _ = std::fs::remove_file(pid_file);
        None
    }
}

// ---- Daemon Server ----

/// Start the daemon: init StorageHub, listen on TCP socket, handle requests.
pub async fn start_daemon(pid_file: PathBuf, addr: String) -> Result<()> {
    // Setup logging: stdout + file
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(LOG_FILE)?;

        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(log_file)
            .with_ansi(false)
            .with_filter(tracing_subscriber::EnvFilter::from_default_env());

        let stdout_layer = tracing_subscriber::fmt::layer()
            .with_filter(tracing_subscriber::EnvFilter::from_default_env());

        tracing_subscriber::registry()
            .with(stdout_layer)
            .with(file_layer)
            .init();
    }

    tracing::info!("Logging initialized: stdout + {}", LOG_FILE);

    // Check for stale PID file
    if let Some(old_pid) = check_pid_file(&pid_file) {
        return Err(anyhow!(
            "daemon already running (PID {}). PID file: {}",
            old_pid,
            pid_file.display()
        ));
    }

    // Write PID file
    let pid = std::process::id();
    std::fs::write(&pid_file, pid.to_string())
        .map_err(|e| anyhow!("failed to write pid file: {}", e))?;
    println!("Daemon PID: {} (pid file: {})", pid, pid_file.display());

    // Load config and init StorageHub
    let config = usp_core::config::Config::load()
        .unwrap_or_else(|_| usp_core::config::Config::default());
    let hub = config.init().await?;
    let hub = Arc::new(hub);

    tracing::info!("StorageHub initialized. Listening on {}", addr);

    // Start TCP listener
    let listener = TcpListener::bind(&addr).await
        .map_err(|e| anyhow!("failed to bind {}: {}", addr, e))?;

    println!("Daemon ready on {}. Press Ctrl+C to stop.", addr);

    // Create cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    // Spawn Ctrl+C handler
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("Shutdown signal received (Ctrl+C)");
                cancel_token_clone.cancel();
            }
            Err(e) => {
                tracing::warn!("Failed to listen for ctrl_c: {}", e);
            }
        }
    });

    // Accept connections loop
    tokio::select! {
        _ = async {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer)) => {
                                tracing::debug!("Client connected from {:?}", peer);
                                let hub_clone = Arc::clone(&hub);
                                let token_clone = cancel_token.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = handle_client(stream, hub_clone, token_clone).await {
                                        tracing::warn!("Client handler error: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!("Failed to accept connection: {}", e);
                            }
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        tracing::info!("Shutdown signal received, stopping acceptor...");
                        break;
                    }
                }
            }
        } => {}
        _ = cancel_token.cancelled() => {
            // This branch shouldn't normally be reached, but just in case
        }
    }

    // Cleanup
    cleanup(pid_file).await;
    tracing::info!("Daemon stopped.");
    println!("Daemon stopped.");
    Ok(())
}

/// Cleanup PID file and any other resources
async fn cleanup(pid_file: PathBuf) {
    if pid_file.exists() {
        if let Err(e) = tokio::fs::remove_file(&pid_file).await {
            tracing::warn!("Failed to remove pid file: {}", e);
        } else {
            tracing::debug!("Removed pid file: {}", pid_file.display());
        }
    }
}

async fn handle_client(
    mut stream: TcpStream,
    hub: Arc<StorageHub>,
    cancel_token: CancellationToken,
) -> Result<()> {
    loop {
        // Read length prefix (4 bytes, big-endian)
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
            Err(e) => return Err(anyhow!("read length: {}", e)),
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        if len > 10_000_000 {
            // 10MB max
            let resp = DaemonResponse {
                result: None,
                error: Some("request too large".to_string()),
            };
            send_response(&mut stream, &resp).await?;
            continue;
        }

        // Read exact payload
        let mut data = vec![0u8; len];
        stream
            .read_exact(&mut data)
            .await
            .map_err(|e| anyhow!("read payload: {}", e))?;

        let request: DaemonRequest = serde_json::from_slice(&data)
            .map_err(|e| anyhow!("invalid JSON: {}", e))?;

        tracing::debug!("Request: {:?}", request.method);

        // Handle "stop" method for graceful shutdown
        if request.method == "stop" {
            let resp = DaemonResponse {
                result: Some(serde_json::json!({"stopping": true})),
                error: None,
            };
            let _ = send_response(&mut stream, &resp).await;
            tracing::info!("Received 'stop' RPC, initiating shutdown...");
            cancel_token.cancel();
            break;
        }

        let response = match request.method.as_str() {
            "put" => handle_put(hub.as_ref(), request.params).await,
            "get" => handle_get(hub.as_ref(), request.params).await,
            "delete" => handle_delete(hub.as_ref(), request.params).await,
            "list_keys" => handle_list_keys(hub.as_ref()).await,
            "stat" => handle_stat(hub.as_ref()).await,
            "pin" => handle_pin(hub.as_ref(), request.params).await,
            "unpin" => handle_unpin(hub.as_ref(), request.params).await,
            "gc" => handle_gc(hub.as_ref()).await,
            "ping" => Ok(DaemonResponse {
                result: Some(serde_json::json!({"pong": true, "pid": std::process::id()})),
                error: None,
            }),
            _ => Ok(DaemonResponse {
                result: None,
                error: Some(format!("unknown method: {}", request.method)),
            }),
        };

        send_response(&mut stream, &response?).await?;
    }

    Ok(())
}

async fn send_response(stream: &mut TcpStream, resp: &DaemonResponse) -> Result<()> {
    let json = serde_json::to_vec(resp)
        .map_err(|e| anyhow!("serialize response: {}", e))?;
    let len = (json.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&json).await?;
    Ok(())
}

// ---- Request Handlers ----

async fn handle_put(hub: &StorageHub, params: serde_json::Value) -> Result<DaemonResponse> {
    let key = params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing 'key'"))?;
    let file = params
        .get("file")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing 'file'"))?;
    let ttl = params
        .get("ttl")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let replicas = params
        .get("replicas")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;

    let data = tokio::fs::read(file).await?;
    let bytes = Bytes::from(data);
    let opts = usp_core::types::StorageOptions {
        ttl_seconds: ttl,
        replicas,
        ..Default::default()
    };

    let receipt = hub.put(key, bytes, opts).await?;
    Ok(DaemonResponse {
        result: Some(serde_json::json!({
            "key": key,
            "backend": format!("{:?}", receipt.backend),
            "size_bytes": receipt.size_bytes,
            "content_hash": receipt.content_hash,
        })),
        error: None,
    })
}

async fn handle_get(hub: &StorageHub, params: serde_json::Value) -> Result<DaemonResponse> {
    let key = params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing 'key'"))?;
    let output = params
        .get("output")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing 'output'"))?;

    match hub.get(key).await? {
        Some(data) => {
            tokio::fs::write(output, &data).await?;
            Ok(DaemonResponse {
                result: Some(serde_json::json!({
                    "key": key,
                    "size": data.len(),
                    "output": output,
                })),
                error: None,
            })
        }
        None => Ok(DaemonResponse {
            result: None,
            error: Some(format!("key not found: {}", key)),
        }),
    }
}

async fn handle_delete(hub: &StorageHub, params: serde_json::Value) -> Result<DaemonResponse> {
    let key = params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing 'key'"))?;

    hub.delete(key).await?;
    Ok(DaemonResponse {
        result: Some(serde_json::json!({"deleted": key})),
        error: None,
    })
}

async fn handle_list_keys(hub: &StorageHub) -> Result<DaemonResponse> {
    let keys = hub.list_keys().await;
    Ok(DaemonResponse {
        result: Some(serde_json::json!({ "keys": keys })),
        error: None,
    })
}

async fn handle_stat(hub: &StorageHub) -> Result<DaemonResponse> {
    let stats = hub.stat().await?;
    let json = serde_json::to_value(&stats)
        .map_err(|e| anyhow!("serialize stats: {}", e))?;
    Ok(DaemonResponse {
        result: Some(json),
        error: None,
    })
}

async fn handle_pin(hub: &StorageHub, params: serde_json::Value) -> Result<DaemonResponse> {
    let key = params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing 'key'"))?;
    hub.pin(key).await?;
    Ok(DaemonResponse {
        result: Some(serde_json::json!({"pinned": key})),
        error: None,
    })
}

async fn handle_unpin(hub: &StorageHub, params: serde_json::Value) -> Result<DaemonResponse> {
    let key = params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing 'key'"))?;
    hub.unpin(key).await?;
    Ok(DaemonResponse {
        result: Some(serde_json::json!({"unpinned": key})),
        error: None,
    })
}

async fn handle_gc(hub: &StorageHub) -> Result<DaemonResponse> {
    let deleted = hub.gc().await?;
    Ok(DaemonResponse {
        result: Some(serde_json::json!({"deleted": deleted})),
        error: None,
    })
}

// ---- Daemon Client ----

/// Check if daemon is running by trying to connect.
pub fn is_daemon_running(addr: &str) -> bool {
    let addr: std::net::SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(_) => return false,
    };

    // Try to connect with a 500ms timeout
    match std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(500)) {
        Ok(_) => true,
        Err(_) => false,
    }
}

/// Send a request to the daemon and return the response.
pub async fn send_to_daemon(
    addr: &str,
    method: &str,
    params: serde_json::Value,
) -> Result<DaemonResponse> {
    let mut stream = TcpStream::connect(addr).await
        .map_err(|e| anyhow!("failed to connect to daemon at {}: {}", addr, e))?;

    let request = DaemonRequest {
        method: method.to_string(),
        params,
    };

    // Send request
    let json = serde_json::to_vec(&request)
        .map_err(|e| anyhow!("serialize request: {}", e))?;
    let len = (json.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&json).await?;

    // Read response
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await
        .map_err(|e| anyhow!("failed to read response length: {}", e))?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await
        .map_err(|e| anyhow!("failed to read response: {}", e))?;

    let response: DaemonResponse = serde_json::from_slice(&buf)
        .map_err(|e| anyhow!("invalid response JSON: {}", e))?;

    Ok(response)
}

/// Stop the daemon by sending a "stop" RPC request.
pub async fn stop_daemon(pid_file: &str) -> Result<()> {
    // Try to connect to the daemon and send "stop" request
    match TcpStream::connect(DEFAULT_DAEMON_ADDR).await {
        Ok(mut stream) => {
            let request = DaemonRequest {
                method: "stop".to_string(),
                params: serde_json::json!({}),
            };
            let json = serde_json::to_vec(&request)
                .map_err(|e| anyhow!("serialize stop request: {}", e))?;
            let len = (json.len() as u32).to_be_bytes();
            stream.write_all(&len).await?;
            stream.write_all(&json).await?;

            // Wait a bit for the daemon to stop
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

            // Clean up PID file if still present
            if Path::new(pid_file).exists() {
                let _ = std::fs::remove_file(pid_file);
            }

            println!("Daemon stop signal sent.");
            Ok(())
        }
        Err(e) => {
            // Daemon not running, clean up PID file
            if Path::new(pid_file).exists() {
                let _ = std::fs::remove_file(pid_file);
            }
            Err(anyhow!(
                "daemon not running (connect failed: {}). PID file cleaned up.",
                e
            ))
        }
    }
}
