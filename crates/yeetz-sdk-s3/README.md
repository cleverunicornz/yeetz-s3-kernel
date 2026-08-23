# yeetz-sdk-s3

[![Crates.io](https://img.shields.io/crates/v/yeetz-sdk-s3.svg)](https://crates.io/crates/yeetz-sdk-s3)
[![Docs.rs](https://docs.rs/yeetz-sdk-s3/badge.svg)](https://docs.rs/yeetz-sdk-s3)
[![CI](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/LICENSE)

**Request-scoped S3-compatible object storage mechanics** — the
sanctioned S3 client of the Yeetz storage closure. "Request-scoped"
means credentials and endpoints arrive per connection from the caller
(environment or TOML config), never from application-owned runtime
state; every operation is a plain async call with no background
workers, sessions, or hidden caching.

It exists because the kernel ([`yeetz-s3-kernel`]) must own exactly
one S3 client, and that client needs things the raw SDKs don't offer
uniformly:

- **conditional operations** — put-if-absent creates, `If-Match`
  compare-and-swap, etag-guarded deletes — normalized across
  S3-compatible providers (etags are quote-normalized for
  Hetzner/Ceph compatibility);
- **strict config** — `S3Config::from_toml_str` uses
  `deny_unknown_fields`, so a domain config file can't silently
  double as object-store config; `Debug` redacts credentials;
- **one engine per job** — [`object_store`] powers all data
  operations and presigning; [`aws-sdk-s3`] is used only for the
  multipart upload lifecycle and per-part presigned URLs, which
  `object_store` does not expose.

[`yeetz-s3-kernel`]: https://crates.io/crates/yeetz-s3-kernel
[`object_store`]: https://docs.rs/object_store/latest/object_store/
[`aws-sdk-s3`]: https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/

## Example

```rust
use bytes::Bytes;
use yeetz_sdk_s3::{DownloadWithMeta, ObjectStoreClient, S3Config};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
// From a strict TOML file, from S3_* environment variables, or built directly:
let config = S3Config::custom(
    "bucket",
    "us-east-1",
    "https://s3.example.com",
    "access-key-id",
    "secret-access-key",
);
let client = ObjectStoreClient::new(&config)?;

client.upload("backups/snapshot.tar", Bytes::from(vec![0u8; 42])).await?;

// Read with the etag, then use it for conditional operations:
let DownloadWithMeta { data, etag } =
    client.download_with_etag("backups/snapshot.tar").await?;
# let _ = (data, etag);
# Ok(())
# }
```

## Surface highlights

- **Config**: `S3Config::{from_toml_str, from_toml_file, from_env,
  custom, custom_with_insecure_http, aws}`; `s3_env_forwarding()` to
  hand the `S3_*` variables to subprocesses. Insecure `http://`
  endpoints are rejected unless explicitly allowed.
- **Data**: `upload`, `upload_with_content_type`, `upload_stream`,
  `download`, `download_stream`, `download_file`, `list_prefix` and
  paginated `list_prefix_after*`, `head`, `exists`.
- **Conditional**: `download_with_etag`,
  `download_if_changed(known_etag)`, `upload_conditional*`,
  `delete_conditional(expected_etag)` — the CAS primitives the kernel
  is built on.
- **Presigned URLs**: `signed_url`, `signed_upload_url`,
  `signed_download_url`.
- **Multipart**: `initiate_multipart_upload`,
  `multipart_part_upload_urls`, `complete_multipart_upload`,
  `abort_multipart_upload` — including presigned per-part upload URLs
  for browser-direct uploads.
- **Errors**: `ObjectStoreError` is `#[non_exhaustive]`, with
  `NotFound` and `PreconditionFailed` distinct — a failed CAS never
  looks like a missing object.

The `test-support` feature gates in-memory stores
(`ObjectStoreClient::in_memory`, `shared_in_memory`) and
multipart test hooks — nothing test-only ships in release builds.

MSRV: Rust 1.96 (pinned by the workspace `rust-toolchain.toml`).

## The closure

| Crate | Role |
| --- | --- |
| [`yeetz-s3-kernel`](https://crates.io/crates/yeetz-s3-kernel) | lineages + atomic keyspace |
| [`yeetz-s3-streams`](https://crates.io/crates/yeetz-s3-streams) | append-only event logs |
| [`yeetz-sdk-s3`](https://crates.io/crates/yeetz-sdk-s3) | this crate — S3 client mechanics |
| [`yeetz-sdk-core`](https://crates.io/crates/yeetz-sdk-core) | provider-neutral HTTP foundation |

## License

MIT.
