---
format: aep.planning-md/1
id: story:file-eventlog
kind: story
status: implemented
title: Durable file Eventlog under the shared storage contract
refs:
- provider: ess
  reference: initiative:ess-evolution
relations:
- informed_by: architecture-decision-record:ess-evolution-05-file-and-atomic-groups
- depends_on: story:atomic-append-groups
revision: 9
---
## Context
The authorized ESS evolution design requires a repository-local Eventlog provider before ER and AEP migration. Existing EventStore, AtomicEventStore, event envelopes and projection ports in crates/eventlog-core define the behavior; this adds a provider and physical encoding, not a product entity. ADR ess-evolution-05-file-and-atomic-groups supersedes the predecessor RFC's two-provider restriction.

## Acceptance
The file provider passes every applicable shared storage and atomic-group contract and independent process/crash tests proving durable acknowledgements, serialized writers, exact retries, corruption and divergent-history refusal, disposable caches, and active-store redaction and erasure across reopen.

## Scope
Cited: crates/eventlog-core/src/lib.rs, atomic_group.rs and projection.rs; crates/eventlog-conformance/src/lib.rs and atomic_group.rs; Cargo.toml and existing Rust gate.
Inferred additions: crates/eventlog-file with versioned JSONL transactions, manifest commit boundary, process locks, separately durable referenced content and privacy recovery; provider tests and format documentation.

## Verification
Run unchanged shared conformance and independent crash/reopen, concurrent process, changed-content, tampering and privacy tests. Deliberate mutations must fail. Preserve exact locked source/toolchain receipts. SQL evidence from the handoff remains tied to its prior tested source; a file test is not a new SQL proof.

## Progress

Implemented and published to main as 0d953650f161d0a43df14ce605339d9f519ba525 on 2026-09-10, verified by git ls-remote origin refs/heads/main after the bot-authenticated push. Final mandatory local gate passed: 129 tests, no failures or skips, no missing required cases; formatting and all-target strict Clippy passed. All file-provider tests also passed on Rust 1.91.0. Four deliberate mutations each failed and were reverted. See verification-report:file-provider-20260910 and docs/design/file-provider.md. No release, capacity approval, ER adapter or downstream migration is claimed.

## Publication

Source publication verified on 2026-09-10: https://github.com/beyond10x/eventlog/commit/0d953650f161d0a43df14ce605339d9f519ba525. Both author and committer are b10x-bot[bot]. The operator explicitly requested skipping the expensive remote Actions gate; the commit carries [skip ci], and workflow definitions and Git hooks were preserved. The local proof is the validation basis, not a remote CI success. This interactive operator-directed operation creates no new decomposition. Worktree cleanup follows publication of this receipt.
