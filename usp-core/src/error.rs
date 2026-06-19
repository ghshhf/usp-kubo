//! Error types for USP

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("backend not found: {0}")]
    BackendNotFound(String),

    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("invalid cid: {0}")]
    InvalidCid(String),

    #[error("policy violation: {0}")]
    PolicyViolation(String),

    #[error("timeout after {0:?}")]
    Timeout(std::time::Duration),

    #[error("operation cancelled")]
    Cancelled,
}

pub type Result<T> = std::result::Result<T, Error>;
