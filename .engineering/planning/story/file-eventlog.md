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
revision: 7
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

Implemented and locally verified in the retained candidate. Final mandatory gate with PostgreSQL 17.6, verified TLS and dedicated application role passed: 129 tests, no failures or skips, no missing required cases; formatting and all-target strict Clippy passed. All file-provider tests also passed on Rust 1.91.0. Four deliberate mutations (digest verification, process locking, stale-cache erasure and mixed-intent refusal) each failed their independent test and were reverted. See verification-report:file-provider-20260910 and docs/design/file-provider.md. No source publication, release, capacity approval, ER adapter or downstream migration is claimed.

## Publication

The operator requested source publication on 2026-09-10 and explicitly requested skipping the expensive remote GitHub Actions gate. Publish the verified atomic-group and file-provider candidate to Eventlog main with [skip ci] in the commit message; preserve workflow definitions and Git hooks. Local production-proof evidence remains the validation basis: 129 passed, zero failed/skipped, formatting and strict Clippy passed, and file tests passed on Rust 1.91.0. Source checksums were reverified before publication and stable Rust remains 1.98.1. This is an interactive operator-directed publication, with no new decomposition or critic panel. Record the verified remote source revision after push; no release or downstream adoption is part of this operation.
