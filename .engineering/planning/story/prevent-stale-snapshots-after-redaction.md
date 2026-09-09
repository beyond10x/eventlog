---
format: aep.planning-md/1
id: story:prevent-stale-snapshots-after-redaction
kind: story
status: draft
title: Prevent stale snapshots from restoring redacted state
tags:
- code-review
- priority-p1
relations:
- derived_from: verification-report:repository-hygiene-and-code-review-20260909
- depends_on: story:integrate-reviewed-feature-contracts
scope:
- confidence: cited
  path: crates/eventlog-conformance/src/lib.rs
- confidence: cited
  path: crates/eventlog-core/src/aggregate.rs
- confidence: cited
  path: crates/eventlog-core/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: inferred
  path: crates/eventlog-postgres/src/schema.rs
- confidence: inferred
  path: crates/eventlog-postgres/tests/conformance.rs
- confidence: cited
  path: crates/eventlog-sqlite/src/lib.rs
- confidence: inferred
  path: crates/eventlog-sqlite/tests/repository.rs
revision: 3
---
## Problem

The initial review reproduced a stale snapshot restoring state removed by redaction on both SQLite and PostgreSQL at main commit `734047203ce112b21ad5b9e3ea67fdacb8835def`. A caller loads version 1 and retains its folded state; another caller redacts version 1; the first caller then saves its delayed snapshot. A fresh aggregate load returns the old state because replay begins after the snapshot version.

The review observed count 0 immediately after redaction and count 1 after the delayed snapshot save on both adapters. Source: `verification-report:repository-hygiene-and-code-review-20260909`, P1 finding. This closes a gap in legacy EL-002 and EL-005 rather than reopening all their delivered behavior. It serves organization O2 and O6 through the existing AGENTS.md objectives.

## Acceptance

On both supported adapters, a snapshot derived before a completed redaction cannot make a subsequent aggregate load restore the redacted event's contribution, while snapshots derived from the current history still load correctly.

## Verification

- Add a deterministic shared regression that retains a loaded snapshot, completes redaction, attempts the delayed save, and verifies that a new repository load agrees with a full fold of the redacted stream.
- Exercise the ordinary Repository automatic-snapshot and snapshot_now interleavings, not only handcrafted state; verify an unaffected stream/tenant and a new valid snapshot continue working.
- The regression must fail on the reviewed main commit and pass after the fix; record the mutation proof required by AGENTS.md invariant 5.
- Pass `bash scripts/gate.sh --production-proof` against SQLite and real PostgreSQL with required TLS/role fixtures, formatting, and clippy. Record exact source and command evidence.

## Design constraints

Coordinate snapshot validity with the history it was folded from. A mutex around only the final save statement is insufficient: it still permits an old snapshot to be saved after redact returns. Select the mechanism during design; this story does not prescribe a new persisted entity, field, or public API. Preserve append-only events, additive DDL, tenant isolation, and both supported backends. Consult the normative RFC referenced by AGENTS.md before changing storage semantics; model any newly introduced stored entity through ESS before implementing it.

## Scope

Cited at reviewed main:
- `crates/eventlog-sqlite/src/lib.rs:1028` — unconditional snapshot upsert; redaction invalidation in the same adapter.
- `crates/eventlog-postgres/src/lib.rs:772` — unconditional snapshot upsert; redaction invalidation in the same adapter.
- `crates/eventlog-core/src/aggregate.rs:309` — delayed automatic snapshot write; snapshot_now is in the same module.
- `crates/eventlog-conformance/src/lib.rs:403` — existing redaction/snapshot conformance coverage.
- `docs/stories/EL-002-aggregates-repository-and-snapshots.md` and `docs/stories/EL-005-erasure-redaction-and-snapshot-invalidation.md` — delivered contracts this fixes.

Inferred implementation/test scope: snapshot contract declarations in eventlog-core, additive schema admission if the selected solution requires it, shared conformance exercises, and both adapters' integration tests. Exact schema/API changes remain to be designed.

This story shares `crates/eventlog-postgres/src/lib.rs` with `story:reuse-postgres-connections-after-empty-catch-up`; sequence their edits or explicitly reconcile that shared file. No concurrency is scheduled by filing these stories.
