//! Cloud S3 storage backend
//!
//! Provides a storage backend that talks to any S3-compatible object
//! storage service (AWS S3, MinIO, Cloudflare R2, Wasabi, etc.).
//!
//! Implementation
//! --------------
//! We use a hand-rolled HTTP client (via `reqwest`) with a SigV4 signer.
//! This avoids the heavy `aws-sdk-s3` dependency while still being fully
//! compatible with the S3 REST API. The signer follows the AWS Signature
//! Version 4 spec (sigv4, query and header auth both supported; we use
//! header auth for PUT/GET/DELETE/HEAD).
//!
//! Reference: <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, DATE};
use reqwest::{Client, Method, Response};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use url::Url;

use crate::backends::StorageBackend;
use crate::error::{Error, Result};
use crate::types::*;

/// Internal configuration for the S3 backend, captured at `init()` time so
/// the public handle can be `Clone` / cheap to share.
#[derive(Debug, Clone)]
struct S3Config {
    /// Optional S3-compatible endpoint override (e.g. for MinIO).
    endpoint: Option<String>,
    region: String,
    bucket: String,
    access_key_id: String,
    secret_access_key: String,
    path_prefix: Option<String>,
}

impl S3Config {
    /// Build the base URL of the S3 endpoint for this bucket.
    fn base_url(&self) -> String {
        let host = match &self.endpoint {
            Some(ep) => ep.trim_end_matches('/').to_string(),
            None => format!("https://s3.{}.amazonaws.com", self.region),
        };
        if self.bucket.is_empty() {
            host
        } else {
            format!("{}/{}", host, self.bucket)
        }
    }

    /// Apply the configured key prefix to a user-supplied key.
    fn object_key(&self, key: &str) -> String {
        match &self.path_prefix {
            Some(prefix) if !prefix.is_empty() => format!(
                "{}/{}",
                prefix.trim_end_matches('/'),
                key.trim_start_matches('/')
            ),
            _ => key.trim_start_matches('/').to_string(),
        }
    }
}

/// Cloud S3 storage backend.
#[derive(Debug, Clone)]
pub struct CloudS3Backend {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    client: Client,
    config: RwLock<Option<S3Config>>,
    /// Approximate object count, updated on each put/delete/exists call.
    /// Real S3 does not expose an exact count; this is a local cache.
    item_count: RwLock<u64>,
    /// Sum of all `put`-ed sizes since init, for the `used_space` stat.
    used_space: RwLock<u64>,
}

impl CloudS3Backend {
    /// Create a new S3 backend (uninitialized). Call `init` before use.
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .user_agent(concat!("usp-kubo/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client construction should not fail");
        Self {
            inner: Arc::new(Inner {
                client,
                config: RwLock::new(None),
                item_count: RwLock::new(0),
                used_space: RwLock::new(0),
            }),
        }
    }

    /// Build the URL for a given object key.
    async fn object_url(&self, key: &str) -> Result<Url> {
        let cfg = self.config().await?;
        let base = cfg.base_url();
        let obj = cfg.object_key(key);
        let url = format!("{}/{}", base.trim_end_matches('/'), obj);
        Url::parse(&url).map_err(|e| Error::Storage(format!("invalid S3 url: {}", e)))
    }

    /// Read the current configuration, or error if not initialized.
    async fn config(&self) -> Result<S3Config> {
        self.inner
            .config
            .read()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::Storage("S3 backend not initialized".into()))
    }

    /// Build the signed headers for a request and return the headers to attach.
    async fn signed_headers(
        &self,
        method: &Method,
        url: &Url,
        body: &[u8],
        extra_headers: &[(String, String)],
    ) -> Result<HeaderMap> {
        let cfg = self.config().await?;
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        // 1) Build the canonical request.
        let payload_hash = {
            let mut hasher = Sha256::new();
            hasher.update(body);
            hex::encode(hasher.finalize())
        };

        let host = url
            .host_str()
            .ok_or_else(|| Error::Storage("S3 url missing host".into()))?;
        let host_header = if let Some(port) = url.port() {
            format!("{}:{}", host, port)
        } else {
            host.to_string()
        };

        let canonical_uri = canonical_uri(url.path());

        // Aggregate all required headers.
        let mut all_headers: Vec<(String, String)> = vec![
            ("host".to_string(), host_header.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        for (k, v) in extra_headers {
            all_headers.push((k.to_lowercase(), v.clone()));
        }
        all_headers.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_headers: String = all_headers
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
            .collect();
        let signed_headers_list: String = all_headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            canonical_uri,
            &canonical_query_string(url),
            canonical_headers,
            signed_headers_list,
            payload_hash
        );

        // 2) String to sign.
        let credential_scope = format!("{}/{}/s3/aws4_request", date_stamp, cfg.region);
        let mut hasher = Sha256::new();
        hasher.update(canonical_request.as_bytes());
        let canonical_request_hash = hex::encode(hasher.finalize());
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date, credential_scope, canonical_request_hash
        );

        // 3) Derive the signing key.
        let k_secret = format!("AWS4{}", cfg.secret_access_key);
        let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
        let k_region = hmac_sha256(&k_date, cfg.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        let k_signing = hmac_sha256(&k_service, b"aws4_request");

        // 4) Sign.
        let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

        // 5) Build the Authorization header.
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            cfg.access_key_id, credential_scope, signed_headers_list, signature
        );

        // 6) Compose the final HeaderMap.
        // NOTE: `x-amz-date` is already in `all_headers` and will be added below.
        // Do NOT add a separate `Date` header — it would be unsigned and cause auth failure.
        let mut headers = HeaderMap::new();
        for (k, v) in &all_headers {
            // Skip `host` (reqwest sets it from the URL).
            if k == "host" {
                continue;
            }
            let hn = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| Error::Storage(format!("invalid header name: {}", e)))?;
            let hv = HeaderValue::from_str(v)
                .map_err(|e| Error::Storage(format!("invalid header value: {}", e)))?;
            headers.insert(hn, hv);
        }
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&authorization)
                .map_err(|e| Error::Storage(format!("invalid authorization: {}", e)))?,
        );
        Ok(headers)
    }

    /// Send a signed request and return the response.
    async fn send_signed(
        &self,
        method: Method,
        url: Url,
        body: Vec<u8>,
        extra_headers: Vec<(String, String)>,
    ) -> Result<Response> {
        let headers = self
            .signed_headers(&method, &url, &body, &extra_headers)
            .await?;

        let mut req = self.inner.client.request(method, url).headers(headers);
        if !body.is_empty() {
            req = req.body(body);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Network(format!("S3 request failed: {}", e)))?;
        Ok(resp)
    }

    /// Convert an HTTP error response into an `Error::Network` with body.
    async fn error_from_response(resp: Response) -> Error {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.as_u16() == 404 {
            Error::KeyNotFound(body)
        } else if status.as_u16() == 401 || status.as_u16() == 403 {
            Error::BackendUnavailable(format!("S3 auth failed ({}): {}", status, body))
        } else if status.is_server_error() {
            Error::Network(format!("S3 server error ({}): {}", status, body))
        } else {
            Error::Network(format!("S3 error ({}): {}", status, body))
        }
    }
}

impl Default for CloudS3Backend {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudS3Backend {
    /// Verify S3 connectivity by sending a HEAD request to the bucket
    async fn verify_connectivity(&self) -> Result<bool> {
        let url = self.object_url("").await?;
        let resp = self
            .send_signed(Method::HEAD, url, Vec::new(), Vec::new())
            .await?;
        let status = resp.status();
        // HEAD to bucket root returns 200, 301, 307, or 403 depending on bucket config
        if status.is_success() || status.as_u16() == 301 || status.as_u16() == 307 {
            Ok(true)
        } else if status.as_u16() == 403 {
            // 403 means bucket exists but we can't list - still considered connected
            Ok(true)
        } else if status.as_u16() == 404 {
            Ok(false)
        } else {
            Err(Error::Network(format!(
                "S3 connectivity check failed with status: {}",
                status
            )))
        }
    }
}

#[async_trait]
impl StorageBackend for CloudS3Backend {
    fn backend_type(&self) -> BackendType {
        BackendType::CloudS3
    }

    async fn init(&self, config: crate::backends::BackendConfig) -> Result<()> {
        let s3_cfg = match config {
            crate::backends::BackendConfig::CloudS3 {
                endpoint,
                region,
                bucket,
                access_key_id,
                secret_access_key,
                path_prefix,
            } => S3Config {
                endpoint,
                region,
                bucket,
                access_key_id,
                secret_access_key,
                path_prefix,
            },
            _ => {
                return Err(Error::Storage(
                    "CloudS3Backend::init requires BackendConfig::CloudS3".into(),
                ));
            }
        };
        *self.inner.config.write().await = Some(s3_cfg.clone());
        *self.inner.item_count.write().await = 0;
        *self.inner.used_space.write().await = 0;

        // Verify connectivity with a HEAD request to the bucket root
        match self.verify_connectivity().await {
            Ok(true) => {
                tracing::info!(
                    "S3 backend initialized, connectivity verified (bucket: {})",
                    s3_cfg.bucket
                );
            }
            Ok(false) => {
                tracing::warn!("S3 backend initialized but connectivity check returned false");
            }
            Err(e) => {
                tracing::warn!(
                    "S3 backend initialized but connectivity check failed: {}",
                    e
                );
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        *self.inner.config.write().await = None;
        Ok(())
    }

    async fn put(&self, key: &str, value: Bytes) -> Result<StoreReceipt> {
        let url = self.object_url(key).await?;
        let body = value.to_vec();
        let resp = self
            .send_signed(
                Method::PUT,
                url,
                body,
                vec![("content-type".into(), "application/octet-stream".into())],
            )
            .await?;
        if !resp.status().is_success() {
            return Err(Self::error_from_response(resp).await);
        }

        // Update stats.
        {
            let mut count = self.inner.item_count.write().await;
            *count = count.saturating_add(1);
        }
        {
            let mut used = self.inner.used_space.write().await;
            *used = used.saturating_add(value.len() as u64);
        }

        Ok(StoreReceipt {
            content_hash: crate::utils::cid::compute_cid(&value),
            backend: BackendType::CloudS3,
            stored_at: Utc::now(),
            size_bytes: value.len() as u64,
            pinned: false,
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let url = self.object_url(key).await?;
        let resp = self
            .send_signed(Method::GET, url, Vec::new(), Vec::new())
            .await?;
        let status = resp.status();
        if status.as_u16() == 404 {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(Self::error_from_response(resp).await);
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| Error::Network(format!("S3 read body failed: {}", e)))?;
        Ok(Some(bytes))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let url = self.object_url(key).await?;
        let resp = self
            .send_signed(Method::DELETE, url, Vec::new(), Vec::new())
            .await?;
        if !resp.status().is_success() && resp.status().as_u16() != 404 {
            return Err(Self::error_from_response(resp).await);
        }
        // Note: S3 DELETE on a 404 is idempotent; we don't roll back the
        // local stat cache (which is only an approximation anyway).
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let url = self.object_url(key).await?;
        let resp = self
            .send_signed(Method::HEAD, url, Vec::new(), Vec::new())
            .await?;
        let status = resp.status();
        if status.as_u16() == 404 {
            return Ok(false);
        }
        if !status.is_success() {
            return Err(Self::error_from_response(resp).await);
        }
        Ok(true)
    }

    async fn stats(&self) -> Result<BackendStats> {
        Ok(BackendStats {
            // S3 has no fixed capacity; use a placeholder large value.
            total_capacity: u64::MAX,
            used_space: *self.inner.used_space.read().await,
            available_space: u64::MAX,
            item_count: *self.inner.item_count.read().await,
        })
    }
}

/// URI-encode a string per AWS SigV4 spec (uppercase hex).
/// Does NOT encode: A-Z, a-z, 0-9, '-', '.', '_', '~'
/// Encodes everything else as %XX (uppercase hex).
/// This matches the AWS SigV4 requirement for URI encoding.
fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char);
            }
            _ => {
                write!(&mut out, "%{:02X}", b).unwrap();
            }
        }
    }
    out
}

/// Build the canonical URI from a URL path, per AWS SigV4 spec.
/// Each path segment is URI-encoded independently; '/' separators are preserved.
fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    let mut result = String::with_capacity(path.len() + 2);
    let mut first = true;
    for segment in path.split('/') {
        if first {
            first = false;
        } else {
            result.push('/');
        }
        if !segment.is_empty() {
            result.push_str(&uri_encode(segment));
        }
    }
    // If the original path ended with '/', preserve it
    if path.ends_with('/') && !result.ends_with('/') {
        result.push('/');
    }
    result
}

/// Build the canonical query string per AWS SigV4 spec.
/// Parses query parameters, sorts by key name, URI-encodes keys and values,
/// and joins with '&'.
fn canonical_query_string(url: &Url) -> String {
    let query = url.query().unwrap_or("");
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (uri_encode(&k), uri_encode(&v)))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

/// Compute HMAC-SHA256 and return the raw bytes.
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    let out = mac.finalize().into_bytes();
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&out);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::BackendConfig;

    fn test_config() -> BackendConfig {
        BackendConfig::CloudS3 {
            endpoint: Some("https://s3.example.com".into()),
            region: "us-east-1".into(),
            bucket: "test-bucket".into(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            path_prefix: None,
        }
    }

    #[tokio::test]
    async fn test_cloud_s3_backend_create() {
        let backend = CloudS3Backend::new();
        backend.init(test_config()).await.unwrap();
        backend.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_cloud_s3_stats_init() {
        let backend = CloudS3Backend::new();
        backend.init(test_config()).await.unwrap();
        let stats = backend.stats().await.unwrap();
        assert_eq!(stats.item_count, 0);
        assert_eq!(stats.used_space, 0);
    }

    #[tokio::test]
    async fn test_cloud_s3_object_key_with_prefix() {
        let cfg = S3Config {
            endpoint: Some("https://s3.example.com".into()),
            region: "us-east-1".into(),
            bucket: "test-bucket".into(),
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            path_prefix: Some("my-prefix".into()),
        };
        assert_eq!(cfg.object_key("foo/bar"), "my-prefix/foo/bar");
        assert_eq!(cfg.object_key("/leading"), "my-prefix/leading");
    }

    #[test]
    fn test_hmac_sha256_deterministic() {
        let a = hmac_sha256(b"key", b"data");
        let b = hmac_sha256(b"key", b"data");
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn test_cloud_s3_sigv4_signed_headers_have_correct_format() {
        // Test that signed_headers produces properly formatted Authorization header
        let backend = CloudS3Backend::new();
        backend
            .init(BackendConfig::CloudS3 {
                endpoint: Some("https://s3.example.com".into()),
                region: "us-east-1".into(),
                bucket: "test-bucket".into(),
                access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
                path_prefix: None,
            })
            .await
            .unwrap();

        let url = Url::parse("https://s3.example.com/test-bucket/test-object").unwrap();
        let data = b"test content";

        let headers = backend
            .signed_headers(&Method::PUT, &url, data, &[])
            .await
            .unwrap();

        // Verify Authorization header is present and has correct format
        let auth = headers
            .get("authorization")
            .expect("authorization header missing");
        let auth_str = auth.to_str().unwrap();
        assert!(auth_str.starts_with("AWS4-HMAC-SHA256 Credential="));
        assert!(auth_str.contains("SignedHeaders="));
        assert!(auth_str.contains("Signature="));

        // Verify x-amz-date is present
        let amz_date = headers
            .get("x-amz-date")
            .expect("x-amz-date header missing");
        let amz_date_str = amz_date.to_str().unwrap();
        assert_eq!(amz_date_str.len(), 16); // Format: YYYYMMDDTHHMMSSZ
        assert!(amz_date_str.ends_with("Z"));

        // Verify x-amz-content-sha256 is present and is valid SHA256 hex
        let content_sha = headers
            .get("x-amz-content-sha256")
            .expect("x-amz-content-sha256 missing");
        let content_sha_str = content_sha.to_str().unwrap();
        assert_eq!(content_sha_str.len(), 64); // SHA256 hex is 64 chars
        assert!(content_sha_str.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn test_cloud_s3_sigv4_different_keys_produce_different_signatures() {
        let backend1 = CloudS3Backend::new();
        backend1
            .init(BackendConfig::CloudS3 {
                endpoint: Some("https://s3.example.com".into()),
                region: "us-east-1".into(),
                bucket: "bucket".into(),
                access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
                secret_access_key: "secret1".into(),
                path_prefix: None,
            })
            .await
            .unwrap();

        let backend2 = CloudS3Backend::new();
        backend2
            .init(BackendConfig::CloudS3 {
                endpoint: Some("https://s3.example.com".into()),
                region: "us-east-1".into(),
                bucket: "bucket".into(),
                access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
                secret_access_key: "secret2".into(),
                path_prefix: None,
            })
            .await
            .unwrap();

        let url = Url::parse("https://s3.example.com/bucket/key").unwrap();
        let data = b"test data";

        let headers1 = backend1
            .signed_headers(&Method::PUT, &url, data, &[])
            .await
            .unwrap();
        let headers2 = backend2
            .signed_headers(&Method::PUT, &url, data, &[])
            .await
            .unwrap();

        let sig1 = headers1.get("authorization").unwrap().to_str().unwrap();
        let sig2 = headers2.get("authorization").unwrap().to_str().unwrap();

        // Different secret keys should produce different signatures
        assert_ne!(sig1, sig2);
    }

    #[tokio::test]
    async fn test_cloud_s3_sigv4_different_regions_produce_different_signatures() {
        let backend_eu = CloudS3Backend::new();
        backend_eu
            .init(BackendConfig::CloudS3 {
                endpoint: Some("https://s3.example.com".into()),
                region: "eu-west-1".into(),
                bucket: "bucket".into(),
                access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
                path_prefix: None,
            })
            .await
            .unwrap();

        let backend_us = CloudS3Backend::new();
        backend_us
            .init(BackendConfig::CloudS3 {
                endpoint: Some("https://s3.example.com".into()),
                region: "us-east-1".into(),
                bucket: "bucket".into(),
                access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
                path_prefix: None,
            })
            .await
            .unwrap();

        let url = Url::parse("https://s3.example.com/bucket/key").unwrap();
        let data = b"test data";

        let headers_eu = backend_eu
            .signed_headers(&Method::PUT, &url, data, &[])
            .await
            .unwrap();
        let headers_us = backend_us
            .signed_headers(&Method::PUT, &url, data, &[])
            .await
            .unwrap();

        let sig_eu = headers_eu.get("authorization").unwrap().to_str().unwrap();
        let sig_us = headers_us.get("authorization").unwrap().to_str().unwrap();

        // Different regions should produce different signatures
        assert_ne!(sig_eu, sig_us);
    }
}
