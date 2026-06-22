//! Error types for USP

use std::time::Duration;
use thiserror::Error;

/// Error severity - determines if an operation can be retried
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// Transient errors can be safely retried (network, timeout, temporary failure)
    Transient,
    /// Permanent errors should not be retried (not found, invalid input, auth failed)
    Permanent,
}

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
    Timeout(Duration),

    #[error("operation cancelled")]
    Cancelled,

    #[error("backend unavailable: {0}")]
    BackendUnavailable(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("all retries exhausted: last error: {last_error}, attempts: {attempts}")]
    RetriesExhausted { last_error: String, attempts: u32 },
}

impl Error {
    /// Whether this error is transient and can be retried
    pub fn is_retriable(&self) -> bool {
        match self {
            Error::Timeout(_)
            | Error::Cancelled
            | Error::BackendUnavailable(_)
            | Error::Network(_) => true,

            Error::Io(io_err) => {
                matches!(
                    io_err.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                )
            }

            Error::Storage(msg) => {
                // Heuristic: if the message contains transient-looking keywords
                msg.to_ascii_lowercase().contains("temporary")
                    || msg.to_ascii_lowercase().contains("busy")
                    || msg.to_ascii_lowercase().contains("rate limit")
                    || msg.to_ascii_lowercase().contains("unavailable")
            }

            // Non-retriable
            Error::BackendNotFound(_)
            | Error::KeyNotFound(_)
            | Error::Serde(_)
            | Error::InvalidCid(_)
            | Error::PolicyViolation(_)
            | Error::RetriesExhausted { .. } => false,
        }
    }

    /// Returns the severity of this error
    pub fn severity(&self) -> ErrorSeverity {
        if self.is_retriable() {
            ErrorSeverity::Transient
        } else {
            ErrorSeverity::Permanent
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
