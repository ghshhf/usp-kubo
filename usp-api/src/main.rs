use anyhow::{anyhow, Result};
use axum::{
    extract::{BodyStream, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Router,
};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use usp_core::StorageHub;

const DEFAULT_ADDR: &str = "127.0.0.1:3000";

#[derive(Parser)]
#[command(version, about = "USP-Kubo REST API Server")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the REST API server
    Serve {
        /// Listen address
        #[arg(short, long, default_value = DEFAULT_ADDR)]
        addr: String,
        /// Auth token (optional, if set, require X-Auth-Token header)
        #[arg(short, long)]
        auth_token: Option<String>,
    },
}

/// Application state shared across handlers
#[derive(Clone)]
struct AppState {
    hub: Arc<StorageHub>,
    auth_token: Option<String>,
}

/// Check if request has valid auth token (if required)
fn check_auth(token: &Option<String>, headers: &axum::http::HeaderMap) -> Result<(), (StatusCode, String)> {
    if let Some(expected) = token {
        let auth_header = headers
            .get("X-Auth-Token")
            .and_then(|h| h.to_str().ok());
        match auth_header {
            Some(t) if t == expected => Ok(()),
            _ => Err((
                StatusCode::UNAUTHORIZED,
                "missing or invalid X-Auth-Token".to_string(),
            )),
        }
    } else {
        Ok(())
    }
}

/// Pretty error response
fn err_response(status: StatusCode, msg: String) -> impl IntoResponse {
    let body = serde_json::json!({ "error": msg });
    (status, axum::Json(body))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let Commands::Serve { addr, auth_token } = &cli.command;

    // Initialize StorageHub
    let config = usp_core::config::Config::load()
        .unwrap_or_else(|_| {
            tracing::debug!("No config file found, using defaults");
            usp_core::config::Config::default()
        });
    let hub = Arc::new(config.init().await?);

    let state = AppState {
        hub: hub.clone(),
        auth_token: auth_token.clone(),
    };

    // Build router
    let app = Router::new()
        .route("/:key", put(store_handler))
        .route("/:key", get(retrieve_handler))
        .route("/:key", delete(delete_handler))
        .route("/list", get(list_handler))
        .route("/stats", get(stats_handler))
        .route("/pin/:key", post(pin_handler))
        .route("/unpin/:key", post(unpin_handler))
        .route("/gc", post(gc_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    // Start server
    let addr: SocketAddr = addr.parse()
        .map_err(|e| anyhow!("invalid address '{}': {}", addr, e))?;
    tracing::info!("Starting USP REST API server on {}", addr);
    if auth_token.is_some() {
        tracing::info!("Auth enabled: set X-Auth-Token header");
    }

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ---- Handlers ----

/// PUT /:key — store data (raw bytes)
async fn store_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: axum::http::HeaderMap,
    body: BodyStream,
) -> impl IntoResponse {
    if let Err((status, msg)) = check_auth(&state.auth_token, &headers) {
        return err_response(status, msg);
    }

    // Read body bytes
    let bytes = match tokio_util::io::StreamReader::new(body.map(|r| {
        r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }))
    .await
    {
        Ok(sr) => {
            let mut data = Vec::new();
            match tokio::io::AsyncReadExt::read_to_end(&mut sr, &mut data).await {
                Ok(_) => data,
                Err(e) => return err_response(StatusCode::BAD_REQUEST, format!("read body: {}", e)),
            }
        }
        Err(e) => return err_response(StatusCode::BAD_REQUEST, format!("read body: {}", e)),
    };

    let opts = usp_core::types::StorageOptions::default();
    match state.hub.put(&key, Bytes::from(bytes), opts).await {
        Ok(receipt) => {
            let body = serde_json::json!({
                "key": key,
                "backend": format!("{:?}", receipt.backend),
                "size_bytes": receipt.size_bytes,
                "content_hash": receipt.content_hash,
            });
            (StatusCode::OK, axum::Json(body))
        }
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
    }
}

/// GET /:key — retrieve data (raw bytes)
async fn retrieve_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = check_auth(&state.auth_token, &headers) {
        return err_response(status, msg);
    }

    match state.hub.get(&key).await {
        Ok(Some(data)) => (
            StatusCode::OK,
            axum::body::FullBody::from(data),
        ).into_response(),
        Ok(None) => err_response(StatusCode::NOT_FOUND, format!("key not found: {}", key)),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
    }
}

/// DELETE /:key — delete data
async fn delete_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = check_auth(&state.auth_token, &headers) {
        return err_response(status, msg);
    }

    match state.hub.delete(&key).await {
        Ok(()) => {
            let body = serde_json::json!({ "deleted": key });
            (StatusCode::OK, axum::Json(body))
        }
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
    }
}

/// GET /list — list all keys
async fn list_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = check_auth(&state.auth_token, &headers) {
        return err_response(status, msg);
    }

    let keys = state.hub.list_keys().await;
    let body = serde_json::json!({ "keys": keys, "count": keys.len() });
    (StatusCode::OK, axum::Json(body))
}

/// GET /stats — storage statistics
async fn stats_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = check_auth(&state.auth_token, &headers) {
        return err_response(status, msg);
    }

    match state.hub.stat().await {
        Ok(stats) => {
            let backends: Vec<serde_json::Value> = stats
                .backends
                .into_iter()
                .map(|(bt, s)| {
                    serde_json::json!({
                        "backend": format!("{:?}", bt),
                        "total_capacity": s.total_capacity,
                        "used_space": s.used_space,
                        "available_space": s.available_space,
                        "item_count": s.item_count,
                        "peer_count": s.peer_count,
                    })
                })
                .collect();
            let body = serde_json::json!({
                "backends": backends,
                "p2p_peer_count": stats.p2p_peer_count,
                "p2p_used_bytes": stats.p2p_used_bytes,
            });
            (StatusCode::OK, axum::Json(body))
        }
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
    }
}

/// POST /pin/:key — pin a key
async fn pin_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = check_auth(&state.auth_token, &headers) {
        return err_response(status, msg);
    }

    match state.hub.pin(&key).await {
        Ok(()) => {
            let body = serde_json::json!({ "pinned": key });
            (StatusCode::OK, axum::Json(body))
        }
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
    }
}

/// POST /unpin/:key — unpin a key
async fn unpin_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = check_auth(&state.auth_token, &headers) {
        return err_response(status, msg);
    }

    match state.hub.unpin(&key).await {
        Ok(()) => {
            let body = serde_json::json!({ "unpinned": key });
            (StatusCode::OK, axum::Json(body))
        }
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
    }
}

/// POST /gc — garbage collect expired data
async fn gc_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = check_auth(&state.auth_token, &headers) {
        return err_response(status, msg);
    }

    match state.hub.gc().await {
        Ok(deleted) => {
            let body = serde_json::json!({ "deleted": deleted });
            (StatusCode::OK, axum::Json(body))
        }
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
    }
}
