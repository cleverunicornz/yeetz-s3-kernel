//! Bounded request-local retry strategies and predicates.

use std::time::Duration;

use crate::Error;

/// Defines bounded request-local retry delays.
#[derive(Debug, Clone, Default)]
pub enum RetryStrategy {
    /// Do not retry failed requests.
    #[default]
    None,
    /// Exponential backoff capped at `max_delay`.
    ExponentialBackoff {
        /// Initial delay before the first retry.
        initial_delay: Duration,
        /// Maximum delay between retries.
        max_delay: Duration,
        /// Maximum number of retry attempts.
        max_retries: usize,
        /// Adds 50-100% jitter to the selected delay.
        jitter: bool,
    },
    /// Fixed delay between retry attempts.
    Linear {
        /// Delay before each retry attempt.
        delay: Duration,
        /// Maximum number of retry attempts.
        max_retries: usize,
    },
    /// Caller-provided delay function. Returning `None` stops retrying.
    Custom {
        /// Maximum number of retry attempts.
        max_retries: usize,
        /// Receives the 1-indexed retry attempt.
        delay_fn: fn(attempt: usize) -> Option<Duration>,
    },
}

impl RetryStrategy {
    /// Returns the delay for a 1-indexed retry attempt.
    pub fn delay_for_attempt(&self, attempt: usize) -> Option<Duration> {
        match self {
            Self::None => None,
            Self::ExponentialBackoff {
                initial_delay,
                max_delay,
                max_retries,
                jitter,
            } => {
                if attempt > *max_retries {
                    return None;
                }
                let multiplier = 2u32.saturating_pow(attempt.saturating_sub(1) as u32);
                let delay = initial_delay.saturating_mul(multiplier).min(*max_delay);
                if *jitter {
                    Some(delay.mul_f64(rand::random_range(0.5..=1.0)))
                } else {
                    Some(delay)
                }
            }
            Self::Linear { delay, max_retries } => (attempt <= *max_retries).then_some(*delay),
            Self::Custom {
                max_retries,
                delay_fn,
            } => (attempt <= *max_retries)
                .then(|| delay_fn(attempt))
                .flatten(),
        }
    }

    /// Returns a static maximum retry count when the strategy has one.
    pub fn max_retries(&self) -> Option<usize> {
        match self {
            Self::None => Some(0),
            Self::ExponentialBackoff { max_retries, .. } | Self::Linear { max_retries, .. } => {
                Some(*max_retries)
            }
            Self::Custom { max_retries, .. } => Some(*max_retries),
        }
    }
}

/// Predicate for deciding whether a failed attempt should be retried.
pub trait RetryPredicate: Send + Sync {
    /// Returns true when the error should be retried for the given attempt.
    fn should_retry(&self, error: &Error, attempt: usize) -> bool;
}

/// Retries errors classified as retryable by [`Error::is_retryable`].
#[derive(Debug, Clone, Copy)]
pub struct RetryOnRetryable;

impl RetryPredicate for RetryOnRetryable {
    fn should_retry(&self, error: &Error, _attempt: usize) -> bool {
        error.is_retryable()
    }
}

/// Retries HTTP 5xx errors.
#[derive(Debug, Clone, Copy)]
pub struct RetryOn5xx;

impl RetryPredicate for RetryOn5xx {
    fn should_retry(&self, error: &Error, _attempt: usize) -> bool {
        matches!(error, Error::HttpStatus { status, .. } if status.is_server_error())
    }
}

/// Retries timeout errors.
#[derive(Debug, Clone, Copy)]
pub struct RetryOnTimeout;

impl RetryPredicate for RetryOnTimeout {
    fn should_retry(&self, error: &Error, _attempt: usize) -> bool {
        matches!(error, Error::Timeout(_))
    }
}

/// Retries connection errors.
#[derive(Debug, Clone, Copy)]
pub struct RetryOnConnectionError;

impl RetryPredicate for RetryOnConnectionError {
    fn should_retry(&self, error: &Error, _attempt: usize) -> bool {
        matches!(error, Error::Connect(_))
    }
}

/// OR composition for retry predicates.
pub struct OrPredicate {
    predicates: Vec<Box<dyn RetryPredicate>>,
}

impl OrPredicate {
    /// Creates an OR predicate.
    pub fn new(predicates: Vec<Box<dyn RetryPredicate>>) -> Self {
        Self { predicates }
    }
}

impl RetryPredicate for OrPredicate {
    fn should_retry(&self, error: &Error, attempt: usize) -> bool {
        self.predicates
            .iter()
            .any(|predicate| predicate.should_retry(error, attempt))
    }
}

/// AND composition for retry predicates.
pub struct AndPredicate {
    predicates: Vec<Box<dyn RetryPredicate>>,
}

impl AndPredicate {
    /// Creates an AND predicate.
    pub fn new(predicates: Vec<Box<dyn RetryPredicate>>) -> Self {
        Self { predicates }
    }
}

impl RetryPredicate for AndPredicate {
    fn should_retry(&self, error: &Error, attempt: usize) -> bool {
        self.predicates
            .iter()
            .all(|predicate| predicate.should_retry(error, attempt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use reqwest::StatusCode;

    #[test]
    fn retry_strategies_are_bounded() {
        let exponential = RetryStrategy::ExponentialBackoff {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(250),
            max_retries: 4,
            jitter: false,
        };
        assert_eq!(
            exponential.delay_for_attempt(1),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            exponential.delay_for_attempt(3),
            Some(Duration::from_millis(250))
        );
        assert_eq!(exponential.delay_for_attempt(5), None);

        let linear = RetryStrategy::Linear {
            delay: Duration::from_secs(1),
            max_retries: 2,
        };
        assert_eq!(linear.delay_for_attempt(2), Some(Duration::from_secs(1)));
        assert_eq!(linear.delay_for_attempt(3), None);
        assert_eq!(RetryStrategy::None.delay_for_attempt(1), None);
        assert_eq!(
            RetryStrategy::Custom {
                max_retries: 2,
                delay_fn: |attempt| (attempt == 2).then_some(Duration::from_millis(7)),
            }
            .delay_for_attempt(2),
            Some(Duration::from_millis(7))
        );
        assert_eq!(
            RetryStrategy::Custom {
                max_retries: 2,
                delay_fn: |_| Some(Duration::from_millis(7)),
            }
            .delay_for_attempt(3),
            None
        );
    }

    #[test]
    fn retry_predicates_follow_error_classification() {
        let retryable = Error::HttpStatus {
            status: StatusCode::SERVICE_UNAVAILABLE,
            raw_body: bytes::Bytes::from_static(b"busy"),
            raw_response: "busy".to_string(),
            headers: Box::new(reqwest::header::HeaderMap::new()),
            rate_limit_info: None,
        };
        let not_retryable = Error::DeserializationFailed {
            raw_response: "oops".to_string(),
            serde_error: "invalid".to_string(),
            status: StatusCode::OK,
            headers: Box::new(reqwest::header::HeaderMap::new()),
        };
        assert!(RetryOnRetryable.should_retry(&retryable, 1));
        assert!(RetryOn5xx.should_retry(&retryable, 1));
        assert!(!RetryOnRetryable.should_retry(&not_retryable, 1));
    }
}
