//! CID (Content Identifier) utilities
//!
//! CIDv0 computation: SHA-256 multihash, base58btc encoded.
//! Format: `base58btc(0x12 0x20 <32-byte SHA-256 digest>)`

use multihash::{MultihashDigest, Sha2_256};

/// Compute a CIDv0 for the given data.
///
/// CIDv0 is a base58btc-encoded multihash with SHA-256.
/// The multihash bytes are: `0x12 0x20 <32-byte digest>`.
/// The whole multihash is then base58btc encoded (starts with "Qm").
pub fn compute_cid(data: &[u8]) -> String {
    // Compute SHA-256 multihash
    let hash = Sha2_256::digest(data);
    let mh = hash.into_multihash();
    let bytes = mh.to_bytes();

    // Base58btc encode (uses Bitcoin alphabet, same as CIDv0)
    bs58::encode(bytes).into_string()
}

/// Check if a string looks like a valid CIDv0 or CIDv1.
///
/// CIDv0: base58btc, 46 characters, starts with "Qm"
/// CIDv1: multibase (usually starts with "b" for base32)
pub fn is_valid_cid(cid_str: &str) -> bool {
    // CIDv0: 46 chars, starts with Qm
    if cid_str.starts_with("Qm") && cid_str.len() == 46 {
        return bs58::decode(cid_str).into_vec().is_ok();
    }
    // CIDv1: longer, different format (not checking exhaustively)
    if cid_str.len() > 46 {
        return true; // optimistic
    }
    false
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
        assert_eq!(cid.len(), 46); // CIDv0 is always 46 chars
    }

    #[test]
    fn test_compute_cid_deterministic() {
        let data = b"hello world";
        let cid1 = compute_cid(data);
        let cid2 = compute_cid(data);
        assert_eq!(cid1, cid2);
    }

    #[test]
    fn test_is_valid_cid() {
        // Real CIDv0 for "hello world"
        let cid = compute_cid(b"hello world");
        assert!(is_valid_cid(&cid));

        assert!(!is_valid_cid("invalid"));
        assert!(!is_valid_cid("Qm")); // too short
    }
}
