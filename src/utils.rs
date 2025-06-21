use anyhow::Result;
use std::time::Duration;
use tracing::{debug, warn};

/// Backoff configuration for retry operations
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f64,
    pub max_attempts: usize,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_attempts: 5,
        }
    }
}

/// Retry executor with exponential backoff
#[derive(Clone)]
pub struct RetryExecutor {
    config: BackoffConfig,
}

impl RetryExecutor {
    /// Create a new retry executor with default configuration
    pub fn new() -> Self {
        Self {
            config: BackoffConfig::default(),
        }
    }

    /// Create a retry executor with custom configuration
    pub fn with_config(config: BackoffConfig) -> Self {
        Self { config }
    }

    /// Execute an operation with retry and exponential backoff
    pub async fn execute<F, Fut, T, E>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut delay = self.config.initial_delay;
        let mut last_error = None;

        for attempt in 1..=self.config.max_attempts {
            match operation().await {
                Ok(result) => {
                    if attempt > 1 {
                        debug!("Operation succeeded on attempt {}", attempt);
                    }
                    return Ok(result);
                }
                Err(error) => {
                    warn!("Operation failed on attempt {}: {}", attempt, error);
                    last_error = Some(error);

                    if attempt < self.config.max_attempts {
                        debug!("Retrying in {:?}", delay);
                        tokio::time::sleep(delay).await;

                        delay = std::cmp::min(
                            delay.mul_f64(self.config.multiplier),
                            self.config.max_delay,
                        );
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "Operation failed after {} attempts. Last error: {}",
            self.config.max_attempts,
            last_error.unwrap()
        ))
    }
}
