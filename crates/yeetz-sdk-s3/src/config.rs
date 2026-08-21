//! Configuration for S3-compatible object storage.

use std::path::Path;

use serde::{Deserialize, Serialize};

/// Configuration for connecting to an S3-compatible storage backend.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// AWS region or custom region for S3-compatible services.
    pub region: String,
    /// Endpoint URL (e.g., `https://fsn1.your-objectstorage.com` for Hetzner).
    /// Set to None for standard AWS S3.
    pub endpoint: Option<String>,
    /// Access key ID.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Explicitly allow HTTP (non-TLS) connections. When false (default),
    /// HTTP endpoints are rejected even if the endpoint URL starts with `http://`.
    /// Set to true only for local development (e.g., `MinIO` on localhost).
    #[serde(default)]
    pub allow_insecure_http: bool,
}

impl S3Config {
    /// Load an `S3Config` from strict TOML content.
    ///
    /// The accepted shape is exactly the generic S3 connection fields on
    /// [`S3Config`]. Unknown fields are rejected so domain-specific config files
    /// cannot be reused as object-store connection config by accident.
    pub fn from_toml_str(toml_source: &str) -> Result<Self, String> {
        toml::from_str(toml_source)
            .map_err(|err| format!("failed to parse S3 object-store config: {err}"))
    }

    /// Load an `S3Config` from a strict TOML file.
    pub fn from_toml_file(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path).map_err(|err| {
            format!(
                "failed to read S3 object-store config {}: {err}",
                path.display()
            )
        })?;

        Self::from_toml_str(&source).map_err(|err| format!("{} ({})", err, path.display()))
    }

    /// Create an `S3Config` from standard environment variables.
    ///
    /// Reads: `S3_BUCKET`, `S3_REGION` (defaults to `us-east-1`),
    /// `S3_ENDPOINT` (optional -- omit for AWS), `S3_ACCESS_KEY_ID`,
    /// `S3_SECRET_ACCESS_KEY`.
    ///
    /// This is the canonical entry point for runner- or app-owned callers that
    /// receive credentials via process env. The SDK reads only the caller-provided
    /// request-scoped S3 credentials; app/runtime ownership stays with the caller.
    pub fn from_env() -> Result<Self, String> {
        let bucket = std::env::var("S3_BUCKET").map_err(|_| "S3_BUCKET not set".to_string())?;
        let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let access_key = std::env::var("S3_ACCESS_KEY_ID")
            .map_err(|_| "S3_ACCESS_KEY_ID not set".to_string())?;
        let secret_key = std::env::var("S3_SECRET_ACCESS_KEY")
            .map_err(|_| "S3_SECRET_ACCESS_KEY not set".to_string())?;

        let allow_insecure_http = std::env::var("S3_ALLOW_INSECURE_HTTP")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        match std::env::var("S3_ENDPOINT") {
            Ok(endpoint) => {
                let mut config = Self::custom(bucket, region, endpoint, access_key, secret_key);
                config.allow_insecure_http = allow_insecure_http;
                Ok(config)
            }
            Err(_) => Ok(Self::aws(bucket, region, access_key, secret_key)),
        }
    }

    /// Create config for a custom S3-compatible endpoint (Hetzner, `MinIO`, R2).
    ///
    /// `allow_insecure_http` defaults to `false`. Use [`Self::custom_with_insecure_http`]
    /// for local development with plain HTTP endpoints (`MinIO`, localstack).
    pub fn custom(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: Some(endpoint.into()),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            allow_insecure_http: false,
        }
    }

    /// Create config for a custom endpoint with explicit `allow_insecure_http` control.
    ///
    /// Use this when the insecure HTTP policy needs to be set at construction time
    /// rather than mutated after the fact (e.g., when building from database credentials
    /// where the endpoint scheme determines the policy).
    pub fn custom_with_insecure_http(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        allow_insecure_http: bool,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: Some(endpoint.into()),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            allow_insecure_http,
        }
    }

    /// Create config for standard AWS S3.
    pub fn aws(
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            allow_insecure_http: false,
        }
    }
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("allow_insecure_http", &self.allow_insecure_http)
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

/// Build an env var map for forwarding S3 credentials to subprocess invocations.
///
/// Reads the same env vars as [`S3Config::from_env()`] and returns them as a
/// `HashMap` suitable for injecting into function binary processes.
/// Only includes variables that are actually set in the environment.
#[must_use]
pub fn s3_env_forwarding() -> std::collections::HashMap<String, String> {
    [
        "S3_BUCKET",
        "S3_REGION",
        "S3_ENDPOINT",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "S3_ALLOW_INSECURE_HTTP",
    ]
    .iter()
    .filter_map(|key| std::env::var(key).ok().map(|val| (key.to_string(), val)))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config_toml() -> &'static str {
        r#"
bucket = "artifact-bucket"
region = "us-east-1"
endpoint = "http://127.0.0.1:9000"
access_key_id = "test-access-key"
secret_access_key = "test-secret-key"
allow_insecure_http = true
"#
    }

    #[test]
    fn loads_strict_toml_config() {
        let config = S3Config::from_toml_str(valid_config_toml()).expect("valid TOML config");

        assert_eq!(config.bucket, "artifact-bucket");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(config.access_key_id, "test-access-key");
        assert_eq!(config.secret_access_key, "test-secret-key");
        assert!(config.allow_insecure_http);
    }

    #[test]
    fn loads_config_without_endpoint_for_aws_s3() {
        let config = S3Config::from_toml_str(
            r#"
bucket = "aws-bucket"
region = "us-west-2"
access_key_id = "aws-key"
secret_access_key = "aws-secret"
"#,
        )
        .expect("valid AWS-style TOML config");

        assert_eq!(config.bucket, "aws-bucket");
        assert_eq!(config.region, "us-west-2");
        assert_eq!(config.endpoint, None);
        assert!(!config.allow_insecure_http);
    }

    #[test]
    fn load_from_toml_file_includes_path_context() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("object-store.toml");
        std::fs::write(&config_path, valid_config_toml()).expect("write config");

        let config = S3Config::from_toml_file(&config_path).expect("file config loads");
        assert_eq!(config.bucket, "artifact-bucket");

        std::fs::write(&config_path, "bucket = \"missing\"\n").expect("write invalid config");
        let error = S3Config::from_toml_file(&config_path).expect_err("missing fields rejected");
        assert!(error.contains("object-store.toml"), "error was: {error}");
    }

    #[test]
    fn rejects_missing_required_toml_fields() {
        let error = S3Config::from_toml_str(
            r#"
bucket = "artifact-bucket"
region = "us-east-1"
access_key_id = "test-access-key"
"#,
        )
        .expect_err("missing secret key is rejected");

        assert!(
            error.contains("secret_access_key"),
            "missing field should be named, got: {error}"
        );
    }

    #[test]
    fn rejects_unknown_toml_fields() {
        let error = S3Config::from_toml_str(
            r#"
bucket = "artifact-bucket"
region = "us-east-1"
access_key_id = "test-access-key"
secret_access_key = "test-secret-key"
unexpected_domain_field = "not allowed"
"#,
        )
        .expect_err("unknown field is rejected");

        assert!(
            error.contains("unexpected_domain_field"),
            "unknown field should be named, got: {error}"
        );
    }

    #[test]
    fn rejects_old_bootstrap_manifest_fields() {
        for field in [
            "prefix",
            "runner_discovery_prefix",
            "default_namespace",
            "default_environment",
            "default_builder_node",
        ] {
            let source = format!(
                "{}\n{field} = \"stale-bootstrap-value\"\n",
                valid_config_toml()
            );
            let error = S3Config::from_toml_str(&source).expect_err("stale field rejected");
            assert!(
                error.contains(field),
                "stale field {field} should be named in error, got: {error}"
            );
        }
    }

    #[test]
    fn debug_redacts_credentials() {
        let config = S3Config::from_toml_str(valid_config_toml()).expect("valid TOML config");
        let debug = format!("{config:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("test-access-key"));
        assert!(!debug.contains("test-secret-key"));
    }

    #[test]
    fn from_env_behavior_remains_intact() {
        temp_env::with_vars(
            [
                ("S3_BUCKET", Some("env-bucket")),
                ("S3_REGION", Some("env-region")),
                ("S3_ENDPOINT", Some("http://127.0.0.1:9001")),
                ("S3_ACCESS_KEY_ID", Some("env-key")),
                ("S3_SECRET_ACCESS_KEY", Some("env-secret")),
                ("S3_ALLOW_INSECURE_HTTP", Some("true")),
            ],
            || {
                let config = S3Config::from_env().expect("env config");
                assert_eq!(config.bucket, "env-bucket");
                assert_eq!(config.region, "env-region");
                assert_eq!(config.endpoint.as_deref(), Some("http://127.0.0.1:9001"));
                assert_eq!(config.access_key_id, "env-key");
                assert_eq!(config.secret_access_key, "env-secret");
                assert!(config.allow_insecure_http);
            },
        );
    }

    #[test]
    fn test_s3_env_forwarding_returns_only_set_vars() {
        // In CI/test, none of the S3 vars are typically set, so the map
        // should be empty (or contain only whatever happens to be in the
        // environment). We verify the function runs without panicking and
        // returns a HashMap.
        let env = s3_env_forwarding();
        // All returned keys must be from the expected set
        for key in env.keys() {
            assert!(
                [
                    "S3_BUCKET",
                    "S3_REGION",
                    "S3_ENDPOINT",
                    "S3_ACCESS_KEY_ID",
                    "S3_SECRET_ACCESS_KEY",
                    "S3_ALLOW_INSECURE_HTTP"
                ]
                .contains(&key.as_str()),
                "unexpected key in env forwarding: {key}"
            );
        }
    }
}
