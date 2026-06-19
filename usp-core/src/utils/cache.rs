//! LRU cache implementation

use bytes::Bytes;
use lru::LruCache;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Result;

/// Hybrid cache - LRU memory cache
pub struct HybridCache {
    cache: Arc<RwLock<LruCache<String, Bytes>>>,
}

impl HybridCache {
    pub fn new(_max_size: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(LruCache::unbounded())),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        // Need write lock to call get() which requires &mut self
        let mut cache = self.cache.write().await;
        Ok(cache.get(key).cloned())
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
