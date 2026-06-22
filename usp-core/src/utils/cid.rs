//! CID (Content Identifier) utilities
//!
//! Simple content hashing using sha2

use sha2::{Digest, Sha256};

/// Compute a content hash for data (returns hex string prefixed with Qm)
pub fn compute_cid(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let hash = hasher.finalize();
    // Return as hex string with Qm prefix for compatibility
    format!("Qm{}", hex::encode(hash))
}

/// Check if a string looks like a valid CID
pub fn is_valid_cid(cid_str: &str) -> bool {
    cid_str.starts_with("Qm") && cid_str.len() >= 46
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_cid() {
        let data = b"hello world";
        let cid = compute_cid(data);
        assert!(!cid.is_empty());
        assert!(cid.starts_with("Qm"));
    }

    #[test]
    fn test_is_valid_cid() {
        assert!(is_valid_cid(
            "QmT5NvUtoM5nWFfrQdVrFtvGfKFmG7AHE8P34isapyhCxX"
        ));
        assert!(!is_valid_cid("invalid"));
        assert!(!is_valid_cid("Qm"));
    }
}
