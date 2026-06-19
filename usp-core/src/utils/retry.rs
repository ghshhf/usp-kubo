//! Retry utilities with exponential backoff

use std::future::Future;
use std::time::Duration;

use crate::error::Result;

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for backoff (usually 2.0)
    pub backoff_factor: f64,
    /// Whether to add jitter to the delay
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            backoff_factor: 2.0,
            jitter: true,
        }
    }
}

impl RetryConfig {
    /// Aggressive retry config for transient network issues
    pub fn aggressive() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(10),
            backoff_factor: 2.0,
            jitter: true,
        }
    }

    /// Minimal retry config - only retry once
    pub fn minimal() -> Self {
        Self {
            max_retries: 1,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(100),
            backoff_factor: 1.0,
            jitter: false,
        }
    }

    /// No retries at all
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            backoff_factor: 1.0,
            jitter: false,
        }
    }
}

/// Calculate delay for a specific retry attempt
pub fn calculate_delay(config: &RetryConfig, attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }

    let base = config.initial_delay.as_millis() as f64;
    let factor = config.backoff_factor.powi(attempt as i32 - 1);
    let delay_ms = base * factor;

    let capped = delay_ms.min(config.max_delay.as_millis() as f64);
    let mut delay = Duration::from_millis(capped as u64);

    if config.jitter {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let jitter_range = delay.as_millis() as f64 * 0.2;
        let adjustment = rng.gen_range(-jitter_range..=jitter_range).round() as i64;

        if adjustment > 0 {
            delay += Duration::from_millis(adjustment as u64);
        } else {
            let subtract = adjustment.unsigned_abs();
            if subtract < delay.as_millis() as u64 {
                delay -= Duration::from_millis(subtract);
            } else {
                delay = Duration::from_millis(1);
            }
        }
    }

    delay
}

/// Retry an async operation using the given config
///
/// Only retries on transient/retriable errors.
pub async fn with_retry<F, Fut, T>(config: &RetryConfig, operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error: Option<crate::error::Error> = None;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !err.is_retriable() {
                    return Err(err);
                }

                last_error = Some(err);

                if attempt < config.max_retries {
                    let delay = calculate_delay(config, attempt + 1);
                    tracing::debug!(
                        "Retry attempt {}/{} after {:?}",
                        attempt + 1,
                        config.max_retries,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Err(crate::error::Error::RetriesExhausted {
        last_error: last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        attempts: config.max_retries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_retry_succeeds_immediately() {
        let config = RetryConfig::default();
        let result: Result<i32> = with_retry(&config, || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_fails_on_permanent_error() {
        let config = RetryConfig::default();
        let result: Result<i32> = with_retry(&config, || async {
            Err(crate::error::Error::KeyNotFound("test".to_string()))
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_calculate_delay_grows() {
        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            backoff_factor: 2.0,
            jitter: false,
        };

        let d1 = calculate_delay(&config, 1);
        let d2 = calculate_delay(&config, 2);
        let d3 = calculate_delay(&config, 3);

        assert!(d1 < d2);
        assert!(d2 < d3);
    }

    #[tokio::test]
    async fn test_calculate_delay_respects_max() {
        let config = RetryConfig {
            max_retries: 10,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            backoff_factor: 2.0,
            jitter: false,
        };

        let d = calculate_delay(&config, 10);
        assert!(d <= Duration::from_secs(1));
    }
}
