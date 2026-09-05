# yeetz-s3-kernel

[![CI](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Rust crates for conditional, S3-compatible durable state. The kernel owns the
storage boundary; its companion crates provide streams and request-scoped SDK
mechanics.

## Crates

| Crate | Purpose |
| --- | --- |
| [`yeetz-s3-kernel`](./crates/yeetz-s3-kernel/) | Canonical state kernel, `AtomicKeyspace`, and streamed values |
| [`yeetz-s3-streams`](./crates/yeetz-s3-streams/) | Append-only event logs over the kernel |
| [`yeetz-sdk-s3`](./crates/yeetz-sdk-s3/) | Request-scoped S3-compatible client mechanics |
| [`yeetz-sdk-core`](./crates/yeetz-sdk-core/) | Provider-neutral request-scoped HTTP primitives |

Each package README introduces its public API and usage. Durable executable
rigs live in [`rigs/`](./rigs/).

## Repository knowledge

[`AGENTS.md`](./AGENTS.md) identifies the repository's operating context.
[`situation/`](./situation/) is the canonical source for behavioral promises,
oracles, witnesses, decisions, invariants, and planned reconsiderations.
Start with [`situation/context.md`](./situation/context.md) for the current
implementation map.

## License

MIT — see [LICENSE](LICENSE).
