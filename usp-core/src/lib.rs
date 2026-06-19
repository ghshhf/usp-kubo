//! USP Core - Unified Storage Platform
//!
//! A meta-storage system that unifies multiple storage backends
//! (local, P2P, cloud, decentralized) under a single API.

pub mod error;
pub mod types;
pub mod hub;
pub mod router;
pub mod policy;
pub mod backends;
pub mod utils;

pub use error::{Error, Result};
pub use types::*;
