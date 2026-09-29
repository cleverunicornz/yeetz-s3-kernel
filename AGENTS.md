<bedrock-repository>
## yeetz-s3-kernel

- Identity: This repository produces an S3-native storage kernel and the Rust `yeetz-s3-kernel`, `yeetz-s3-streams`, `yeetz-sdk-s3`, and `yeetz-sdk-core` crate closure.
- Ownership: `OWNED`.
- Phase and implementation map: `situation/context.md`.
- Critical invariants:
  - All durable object-storage access owned by this repository flows through the kernel closure: `yeetz-s3-kernel`, `yeetz-sdk-s3`, and `yeetz-sdk-core`. Application and rig code use kernel surfaces rather than raw object-store or S3 adapter APIs. `situation/invariants/I-000001-kernel-storage-boundary.md`.
  - This repository is public. Secure material — our own known vulnerabilities from the security vault, their details, exploitability and affected code paths — is never represented in it: not in code, comments, commits, branches, issues, pull requests, reviews or comments. Public CVE and CWE references are fine. A fix lands normally, with a Promise that describes the property the code keeps, never the vulnerability. `situation/invariants/I-000003-public-secure-material-boundary.md`.
- Verification: Assured: dispatch `gh workflow run ci-dev.yml -f ref=<branch-or-sha> -f task=gates`; the resulting `gates (full set)` observation is retained by PASS witnesses `situation/witnesses/P-000001/W-000019-canonical-lineage-current-gate.md`, `situation/witnesses/P-000002/W-000020-keyspace-current-gate.md`, `situation/witnesses/P-000003/W-000018-append-only-streams-current-gate.md`, `situation/witnesses/P-000004/W-000021-streamed-values-current-gate.md`, `situation/witnesses/P-000005/W-000022-batched-deletion-current-gate.md`, `situation/witnesses/P-000006/W-000016-strict-stream-reads-gate.md`, and `situation/witnesses/P-000007/W-000017-conditional-stream-writes-gate.md`; W-000016 and W-000017 additionally retain direct manual source-leg evidence for clauses their executable cases do not decide; gate claims cite the resulting Actions run URL.
- Tool priority: organization defaults.
- Donor boundary: `96a05336c850895143c297fb47ffb55227b0c4fb`.
</bedrock-repository>
