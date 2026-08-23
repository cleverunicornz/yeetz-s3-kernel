# yeetz-sdk-core

[![Crates.io](https://img.shields.io/crates/v/yeetz-sdk-core.svg)](https://crates.io/crates/yeetz-sdk-core)
[![Docs.rs](https://docs.rs/yeetz-sdk-core/badge.svg)](https://docs.rs/yeetz-sdk-core)
[![CI](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/LICENSE)

**Provider-neutral HTTP plumbing for the Yeetz SDK hierarchy.** One
small crate owns what every API client otherwise reinvents badly:
executing one logical call with bounded retries, honoring rate-limit
hints, and failing with the *raw body preserved* — so a 500 with a
useful JSON error payload is still readable when it reaches you.

Deliberate scope: request-local auth (`Basic`/`Bearer`) only. No
credential storage or refresh, no OAuth, no signing, no provider wire
contracts, no background workers. Provider SDKs — like
[`yeetz-sdk-s3`] — compose on top.

[`yeetz-sdk-s3`]: https://crates.io/crates/yeetz-sdk-s3

## What's in the box

- **[`Client`]** — cloneable, reqwest-backed. Typed verb helpers
  (`get`, `post`, `post_form`, `put`, `patch`, `delete`, `get_bytes`,
  `post_bytes`) plus the general `call_json` / `call_bytes` /
  `execute_started` entry points over [`RequestMetadata`].
- **[`Response<T>`]** — `data`, `raw_body`, `status`, `headers`,
  `latency`, and `attempts` (how many tries the logical call took).
  Derefs to `T`.
- **[`RetryStrategy`]** — `None`, `ExponentialBackoff` (with jitter),
  `Linear`, or a custom delay function; composed with
  [`RetryPredicate`] impls (`RetryOn5xx`, `RetryOnTimeout`,
  `RetryOnConnectionError`, `OrPredicate`, `AndPredicate`).
- **[`RateLimitConfig`]** — parses `Retry-After` (seconds or
  HTTP-date), `RateLimit-Reset`, and `RateLimit-Remaining`; waits up
  to a bounded `max_wait` instead of hammering a throttled API.
- **[`Error`]** — `HttpStatus` and `DeserializationFailed` carry the
  raw response body, headers, and rate-limit info alongside the
  status; nothing swallows the payload that explains the failure.

[`Client`]: https://docs.rs/yeetz-sdk-core/latest/yeetz_sdk_core/struct.Client.html
[`Response<T>`]: https://docs.rs/yeetz-sdk-core/latest/yeetz_sdk_core/struct.Response.html
[`RetryStrategy`]: https://docs.rs/yeetz-sdk-core/latest/yeetz_sdk_core/retry/enum.RetryStrategy.html
[`RetryPredicate`]: https://docs.rs/yeetz-sdk-core/latest/yeetz_sdk_core/retry/trait.RetryPredicate.html
[`RateLimitConfig`]: https://docs.rs/yeetz-sdk-core/latest/yeetz_sdk_core/rate_limit/struct.RateLimitConfig.html
[`Error`]: https://docs.rs/yeetz-sdk-core/latest/yeetz_sdk_core/enum.Error.html
[`RequestMetadata`]: https://docs.rs/yeetz-sdk-core/latest/yeetz_sdk_core/struct.RequestMetadata.html

## Example

```rust
use std::time::Duration;
use yeetz_sdk_core::{Client, Response, RetryStrategy};

#[derive(serde::Deserialize)]
struct Thing { id: u64 }

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::builder()
    .base_url("https://api.example.com")?
    .bearer_auth("token")
    .retry_strategy(RetryStrategy::ExponentialBackoff {
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(5),
        max_retries: 3,
        jitter: true,
    })
    .timeout(Duration::from_secs(30))
    .build()?;

let res: Response<Thing> = client.get("v1/thing/42").await?;
println!("{} attempt(s), {} raw bytes", res.attempts, res.raw_body.len());
let thing: Thing = res.data;
# let _ = thing;
# Ok(())
# }
```

MSRV: Rust 1.96 (pinned by the workspace `rust-toolchain.toml`).

## License

MIT.
