//! Policy Engine - decides data placement strategy

use glob::Pattern;
use std::collections::HashMap;

use crate::types::*;

/// Placement rule
#[derive(Debug, Clone)]
pub struct PlacementRule {
    /// Key pattern to match
    pub key_pattern: String,

    /// Minimum file size in bytes
    pub min_size: Option<u64>,

    /// Maximum file size in bytes
    pub max_size: Option<u64>,

    /// Required tags for matching
    pub required_tags: HashMap<String, String>,

    /// Target storage tier
    pub target_tier: StorageTier,

    /// Priority (higher = more priority)
    pub priority: u32,
}

impl PlacementRule {
    /// Check if this rule matches the given key, options and data size
    pub fn matches(&self, key: &str, opts: &StorageOptions, size_bytes: u64) -> bool {
        // Check key pattern
        if let Ok(pattern) = Pattern::new(&self.key_pattern) {
            if !pattern.matches(key) {
                return false;
            }
        }

        // Check size constraints
        if let Some(min_size) = self.min_size {
            if size_bytes < min_size {
                return false;
            }
        }

        if let Some(max_size) = self.max_size {
            if size_bytes > max_size {
                return false;
            }
        }

        // Check tags
        for (k, v) in &self.required_tags {
            if opts.tags.get(k) != Some(v) {
                return false;
            }
        }

        true
    }
}

/// Policy engine - decides where to store data
pub struct PolicyEngine {
    rules: Vec<PlacementRule>,
    default_tier: StorageTier,
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl PolicyEngine {
    pub fn new() -> Self {
        let mut rules = Vec::new();

        // Small files go to local (Hot tier)
        rules.push(PlacementRule {
            key_pattern: "*".to_string(),
            min_size: None,
            max_size: Some(1024 * 1024), // < 1MB
            required_tags: HashMap::new(),
            target_tier: StorageTier::Hot,
            priority: 10,
        });

        // Backup tags go to archive (Decentralized)
        let mut backup_tags = HashMap::new();
        backup_tags.insert("type".to_string(), "backup".to_string());
        rules.push(PlacementRule {
            key_pattern: "*".to_string(),
            min_size: Some(100 * 1024 * 1024), // > 100MB
            max_size: None,
            required_tags: backup_tags,
            target_tier: StorageTier::Archive,
            priority: 50,
        });

        // Public content goes to P2P (Warm tier)
        let mut public_tags = HashMap::new();
        public_tags.insert("visibility".to_string(), "public".to_string());
        rules.push(PlacementRule {
            key_pattern: "public/*".to_string(),
            min_size: None,
            max_size: None,
            required_tags: public_tags,
            target_tier: StorageTier::Warm,
            priority: 30,
        });

        // Sort by priority (highest first)
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));

        Self {
            rules,
            default_tier: StorageTier::Warm,
        }
    }

    /// Add a placement rule
    pub fn add_rule(&mut self, rule: PlacementRule) {
        self.rules.push(rule);
        self.rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Decide which backend type to use
    pub fn decide(
        &self,
        key: &str,
        opts: &StorageOptions,
        size_bytes: u64,
    ) -> crate::Result<BackendType> {
        for rule in &self.rules {
            if rule.matches(key, opts, size_bytes) {
                return Ok(self.tier_to_backend(rule.target_tier));
            }
        }
        Ok(self.tier_to_backend(self.default_tier))
    }

    fn tier_to_backend(&self, tier: StorageTier) -> BackendType {
        match tier {
            StorageTier::Hot => BackendType::Local,
            StorageTier::Warm => BackendType::P2P, // P2P for warm data
            StorageTier::Cold => BackendType::CloudS3,
            StorageTier::Archive => BackendType::Decentralized,
        }
    }
}
