//! Daemon module for USP
//!
//! Provides a long-running daemon process that hosts the StorageHub,
//! keeping P2P libp2p Swarm alive across CLI invocations.
//!
//! Communication protocol: length-prefixed JSON over TCP.
//! Default listen address: 127.0.0.1:4222

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use usp_core::StorageHub;

const DEFAULT_DAEMON_ADDR: &str = "127.0.0.1:4222";
const PID_FILE: &str = ".usp-daemon.pid";

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

// ---- Daemon Server ----

/// Start the daemon: init StorageHub, listen on TCP socket, handle requests.
pub async fn start_daemon(pid_file: PathBuf, addr: String) -> Result<()> {
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

    println!("StorageHub initialized. Listening on {}...", addr);

    // Start TCP listener
    let listener = TcpListener::bind(&addr).await
        .map_err(|e| anyhow!("failed to bind {}: {}", addr, e))?;

    println!("Daemon ready. Press Ctrl+C to stop.");

    // Wait for Ctrl+C or client connections
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let r = running.clone();

    // Spawn Ctrl+C handler
    tokio::spawn(async move {
        if let Ok(()) = signal::ctrl_c().await {
            println!("\nShutdown signal received.");
            r.store(false, std::sync::atomic::Ordering::SeqCst);
        }
    });

    // Accept connections
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, peer)) => {
                        tracing::debug!("Client connected from {:?}", peer);
                        let hub_clone = Arc::clone(&hub);
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(stream, hub_clone).await {
                                tracing::warn!("Client handler error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("Failed to accept connection: {}", e);
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                if !running.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
            }
        }
    }

    // Cleanup
    let _ = std::fs::remove_file(&pid_file);
    println!("Daemon stopped.");
    Ok(())
}

async fn handle_client(mut stream: TcpStream, hub: Arc<StorageHub>) -> Result<()> {
    let mut buf = BytesMut::with_capacity(1024);

    loop {
        // Read length prefix (4 bytes)
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            break; // Client disconnected
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

        // Read payload
        buf.reserve(len);
        if stream.read_buf(&mut buf).await? < len {
            break; // Incomplete read
        }

        let data = buf.split_to(len);
        let request: DaemonRequest = serde_json::from_slice(&data)
            .map_err(|e| anyhow!("invalid JSON: {}", e))?;

        tracing::debug!("Request: {:?}", request.method);

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
                result: Some(serde_json::json!({"pong": true})),
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

/// Check if daemon is running (PID file exists and process is alive).
pub fn is_daemon_running(pid_file: &str) -> bool {
    if !std::path::Path::new(pid_file).exists() {
        return false;
    }
    // On Unix, we could check if the PID is alive.
    // On Windows, just check if the file exists.
    // The daemon will clean up the PID file on exit.
    true
}

/// Send a request to the daemon and return the response.
pub async fn send_to_daemon(addr: &str, method: &str, params: serde_json::Value) -> Result<DaemonResponse> {
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

/// Stop the daemon by sending a SIGTERM (or deleting PID file).
pub async fn stop_daemon(_pid_file: &str) -> Result<()> {
    // Not fully implemented yet. The daemon exits on Ctrl+C.
    // In the future, send a "stop" RPC request to the daemon.
    Err(anyhow!("stop not yet implemented, use Ctrl+C to stop the daemon"))
}

use std::sync::Arc;
