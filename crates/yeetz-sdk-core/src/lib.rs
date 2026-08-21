//! yeetz-sdk-core: Provider-neutral request-scoped SDK primitives for the active Yeetz SDK hierarchy.
//!
//! ## Responsibilities
//! - Own provider-neutral request metadata, client execution, response/error retention, retry decisions, retry delays, and rate-limit header parsing for one SDK call.
//! - Provide JSON and bytes call helpers that callers and SDK crates can compose without moving provider-specific wire contracts into this crate.
//! - Keep all retry waits, timeouts, attempt counters, and rate-limit decisions request-local to the awaiting client future.
//!
//! ## Key Types / APIs
//! - [`Client`] and [`ClientBuilder`] for provider-neutral HTTP execution.
//! - [`RequestMetadata`] and [`RequestAuth`] for request-scoped headers, query params, form data, and auth inputs.
//! - [`Response`] and [`Error`] for terminal response metadata and raw-body preserving failures.
//! - [`RetryStrategy`], [`RetryPredicate`], [`RateLimitConfig`], and [`RateLimitInfo`] for bounded request-local retry and rate-limit behavior.
//!
//! ## Invariants
//! - This crate is an in-process library surface, not a runtime, service, gateway, provider authority, DB owner, workflow owner, runner/operator surface, or compatibility layer.
//! - Provider credentials, OAuth helpers, quota snapshots, DB-backed provider rate-limit state, provider routing, fallback behavior, and provider-specific wire contracts stay in their owning provider surfaces.
//! - No support-both paths, live DB access, broker use, workflow orchestration, background workers, durable retry state, or provider integrations are implemented here.

pub mod client;
pub mod error;
pub mod metadata;
pub mod rate_limit;
pub mod response;
pub mod retry;

pub use client::{Client, ClientBuilder, StartedResponse};
pub use error::{Error, Result};
pub use metadata::{RequestAuth, RequestMetadata};
pub use rate_limit::{RateLimitConfig, RateLimitConfigBuilder, RateLimitInfo};
pub use response::Response;
pub use retry::{
    AndPredicate, OrPredicate, RetryOn5xx, RetryOnConnectionError, RetryOnRetryable,
    RetryOnTimeout, RetryPredicate, RetryStrategy,
};
