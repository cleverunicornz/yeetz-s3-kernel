# C-000001 — Part-addressed streamed-value reads

## State

proposed

## Candidate

Reconsider the manifest/chunk representation if a portable backend capability
witness establishes part-addressed reads with the integrity properties required
for a logical streamed value.

## Origin

- `situation/gaps/G-000001-streaming-gc-quiescence.md`
- `situation/decisions/D-000004-streaming-value-v2.md`
- Historical capability witness:
  `https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/32592980637`

## Why consider it

The selected manifest representation preserves conditional publication where
part-addressed reads are unavailable, but it leaves destructive chunk collection
with an operational quiescence requirement. A qualified alternative could
change that tradeoff; it is not assumed to exist.

## Qualification questions

- Can the target surface read a selected part while preserving the needed
  content-integrity evidence across supported S3-compatible backends?
- Can conditional publication and stale-era conflict behavior remain at least as
  strict as P-000004?
- Does the alternative actually remove or reduce G-000001 rather than moving
  cleanup authority to another unproved component?

## Candidate approaches

- Retain manifest publication and add only an explicitly backend-qualified read
  optimization.
- Select a replacement representation only through a superseding decision,
  promise, and oracle if the full capability evidence supports it.

## Disposition

None. The candidate remains proposed pending a capability witness.
