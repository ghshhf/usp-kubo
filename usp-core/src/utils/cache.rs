//! LRU cache implementation

use bytes::Bytes;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Result;

/// Hybrid cache - LRU memory cache
pub struct HybridCache {
    cache: Arc<RwLock<LruCache<String, Bytes>>>,
}

impl HybridCache {
    pub fn new(max_size: usize) -> Self {
        let capacity = NonZeroUsize::new(max_size).unwrap_or(NonZeroUsize::MIN);
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        // Use peek() which only needs &self (no LRU promotion), allowing a read lock
        let cache = self.cache.read().await;
        Ok(cache.peek(key).cloned())
    }

    pub async fn set(&self, key: &str, value: Bytes) -> Result<()> {
        let mut cache = self.cache.write().await;
        cache.put(key.to_string(), value);
        Ok(())
    }

    pub async fn remove(&self, key: &str) -> Result<()> {
        let mut cache = self.cache.write().await;
        cache.pop(key);
        Ok(())
    }

    pub async fn clear(&self) -> Result<()> {
        let mut cache = self.cache.write().await;
        cache.clear();
        Ok(())
    }
}
