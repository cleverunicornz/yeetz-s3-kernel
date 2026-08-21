//! Provider-neutral rate-limit response metadata parsing.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::header::HeaderMap;

/// Rate-limit metadata parsed from terminal response headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitInfo {
    /// Reset time parsed from `X-RateLimit-Reset` or `RateLimit-Reset`.
    pub reset_at: Option<SystemTime>,
    /// Delay parsed from `Retry-After`.
    pub retry_after: Option<Duration>,
    /// Remaining request count parsed from `X-RateLimit-Remaining`.
    pub remaining: Option<u64>,
}

impl RateLimitInfo {
    /// Parses provider-neutral rate-limit headers.
    pub fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            reset_at: parse_rate_limit_reset(headers),
            retry_after: parse_retry_after(headers),
            remaining: parse_rate_limit_remaining(headers),
        }
    }

    /// Returns true when headers indicate an active rate limit.
    pub fn is_rate_limited(&self) -> bool {
        self.retry_after.is_some() || self.remaining == Some(0)
    }

    /// Returns true when headers carry a request-local wait hint.
    pub fn has_wait_hint(&self) -> bool {
        self.retry_after.is_some() || self.reset_at.is_some()
    }

    /// Returns a request-local wait decision capped by configuration.
    pub fn delay(&self, config: &RateLimitConfig) -> Option<Duration> {
        if !config.enabled {
            return None;
        }
        if config.respect_retry_after
            && let Some(delay) = self.retry_after
        {
            return Some(delay.min(config.max_wait));
        }
        if let Some(reset_at) = self.reset_at
            && let Ok(delay) = reset_at.duration_since(SystemTime::now())
        {
            return Some(delay.min(config.max_wait));
        }
        None
    }
}

/// Request-local rate-limit wait configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitConfig {
    /// Enables parsing and wait selection from rate-limit headers.
    pub enabled: bool,
    /// Maximum wait selected from rate-limit metadata.
    pub max_wait: Duration,
    /// Enables `Retry-After` as the preferred delay source.
    pub respect_retry_after: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_wait: Duration::from_secs(300),
            respect_retry_after: true,
        }
    }
}

impl RateLimitConfig {
    /// Creates a builder for rate-limit configuration.
    pub fn builder() -> RateLimitConfigBuilder {
        RateLimitConfigBuilder::default()
    }

    /// Disables rate-limit wait decisions.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }
}

/// Builder for [`RateLimitConfig`].
#[derive(Debug, Default, Clone)]
pub struct RateLimitConfigBuilder {
    enabled: Option<bool>,
    max_wait: Option<Duration>,
    respect_retry_after: Option<bool>,
}

impl RateLimitConfigBuilder {
    /// Sets whether rate-limit wait selection is enabled.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Sets the maximum selected wait.
    pub fn max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = Some(max_wait);
        self
    }

    /// Sets whether `Retry-After` should be honored.
    pub fn respect_retry_after(mut self, respect: bool) -> Self {
        self.respect_retry_after = Some(respect);
        self
    }

    /// Builds the configuration.
    pub fn build(self) -> RateLimitConfig {
        let default = RateLimitConfig::default();
        RateLimitConfig {
            enabled: self.enabled.unwrap_or(default.enabled),
            max_wait: self.max_wait.unwrap_or(default.max_wait),
            respect_retry_after: self
                .respect_retry_after
                .unwrap_or(default.respect_retry_after),
        }
    }
}

fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let header = headers.get("retry-after")?.to_str().ok()?;
    if let Ok(seconds) = header.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = httpdate::parse_http_date(header).ok()?;
    date.duration_since(SystemTime::now()).ok()
}

fn parse_rate_limit_reset(headers: &HeaderMap) -> Option<SystemTime> {
    for name in ["x-ratelimit-reset", "ratelimit-reset"] {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok())
            && let Ok(timestamp) = value.parse::<u64>()
        {
            return Some(UNIX_EPOCH + Duration::from_secs(timestamp));
        }
    }
    None
}

fn parse_rate_limit_remaining(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("x-ratelimit-remaining")?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn parses_retry_after_seconds_and_caps_delay() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("600"));
        let info = RateLimitInfo::from_headers(&headers);
        assert_eq!(
            info.delay(
                &RateLimitConfig::builder()
                    .max_wait(Duration::from_secs(30))
                    .build()
            ),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn parses_retry_after_http_date_and_rejects_invalid_values() {
        let future = SystemTime::now() + Duration::from_secs(60);
        let mut headers = HeaderMap::new();
        headers.insert(
            "retry-after",
            HeaderValue::from_str(&httpdate::fmt_http_date(future)).unwrap(),
        );
        assert!(RateLimitInfo::from_headers(&headers).retry_after.is_some());

        headers.insert("retry-after", HeaderValue::from_static("not-a-date"));
        assert_eq!(RateLimitInfo::from_headers(&headers).retry_after, None);
    }

    #[test]
    fn parses_reset_headers_remaining_and_disabled_mode() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 30;
        let mut headers = HeaderMap::new();
        headers.insert(
            "ratelimit-reset",
            HeaderValue::from_str(&timestamp.to_string()).unwrap(),
        );
        headers.insert("x-ratelimit-remaining", HeaderValue::from_static("0"));

        let info = RateLimitInfo::from_headers(&headers);
        assert!(info.reset_at.is_some());
        assert_eq!(info.remaining, Some(0));
        assert!(info.is_rate_limited());
        assert_eq!(info.delay(&RateLimitConfig::disabled()), None);
    }

    #[test]
    fn reset_header_alone_is_a_wait_hint_not_active_limit_by_itself() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 30;
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-ratelimit-reset",
            HeaderValue::from_str(&timestamp.to_string()).unwrap(),
        );

        let info = RateLimitInfo::from_headers(&headers);
        assert!(info.reset_at.is_some());
        assert!(!info.is_rate_limited());
        assert!(info.has_wait_hint());
        assert!(
            info.delay(
                &RateLimitConfig::builder()
                    .max_wait(Duration::from_millis(5))
                    .build()
            )
            .is_some()
        );
    }

    #[test]
    fn can_ignore_retry_after_and_use_reset_delay() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 30;
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("5"));
        headers.insert(
            "x-ratelimit-reset",
            HeaderValue::from_str(&timestamp.to_string()).unwrap(),
        );
        let info = RateLimitInfo::from_headers(&headers);
        let delay = info
            .delay(
                &RateLimitConfig::builder()
                    .respect_retry_after(false)
                    .max_wait(Duration::from_secs(60))
                    .build(),
            )
            .expect("reset delay");
        assert!(delay > Duration::from_secs(20));
    }
}
