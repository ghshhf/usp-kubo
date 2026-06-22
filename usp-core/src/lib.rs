//! USP Core - Unified Storage Platform
//!
//! A meta-storage system that unifies multiple storage backends
//! (local, P2P, cloud, decentralized) under a single API.

pub mod backends;
pub mod config;
pub mod error;
pub mod hub;
pub mod policy;
pub mod router;
pub mod types;
pub mod utils;

pub use error::{Error, Result};
pub use hub::StorageHub;
pub use types::*;
pub use utils::{with_retry, RetryConfig};
