---
format: aep.planning-md/1
id: story:reuse-postgres-connections-after-empty-catch-up
kind: story
status: implemented
title: Reuse PostgreSQL connections after empty catch-up polls
tags:
- code-review
- priority-p2
relations:
- derived_from: verification-report:repository-hygiene-and-code-review-20260909
- informed_by: story:projections-inline-and-catch-up
scope:
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/tests/conformance.rs
revision: 8
---
## Problem

At reviewed main `734047203ce112b21ad5b9e3ea67fdacb8835def`, run_catch_up quarantines a pooled lease and opens a transaction, then returns Ok without explicitly settling the transaction when no events are eligible or the projector advisory lock is unavailable. Lease::drop consequently closes a healthy connection. Idle polling and contending workers continually reconnect and repeat session/TLS setup.

The public-API reproduction observed applied=0, idle connections falling from 1 to 0, and checked_out=0 after retirement. Source: `verification-report:repository-hygiene-and-code-review-20260909`, P2 finding. This is a follow-up to legacy EL-003 and the already merged pool work, not an unimplemented pool replacement. It serves AGENTS.md objectives O2 and O6 by preserving the log's bounded persistence and projection behavior.

## Acceptance

Successful no-work PostgreSQL catch-up passes, including an empty eligible feed and an unavailable projector lock, complete their transaction and return the healthy connection for reuse, while failed or cancelled settlement continues to quarantine the connection until its driver stops.

## Verification

- Add deterministic real-PostgreSQL regressions for repeated empty polls and unavailable-lock returns; verify the same healthy backend session is reused rather than merely observing a replacement connection count.
- Exercise failed/cancelled transaction settlement and retain the existing quarantine, bounded occupancy, and shutdown guarantees.
- Verify catch-up progress, cursor, projection contents, and advisory-lock semantics remain unchanged.
- Show the new regression fails at the reviewed main commit; record the mutation proof required by AGENTS.md invariant 5.
- Pass `bash scripts/gate.sh --production-proof`, including both adapters, TLS/application-role fixtures, formatting, and clippy, and record exact source/command evidence.

## Design constraints

Explicitly finish the no-work transaction before marking its lease settled. Never call settled merely because the branch intended no writes; only confirmed transaction completion makes the connection reusable. Preserve retirement on rollback errors and cancellation. This changes resource lifecycle handling and introduces no new persisted entity.

## Scope

Cited at reviewed main:
- `crates/eventlog-postgres/src/lib.rs:1191` — empty-event early return in run_catch_up; the unavailable-lock early return is in the same method.
- `crates/eventlog-postgres/src/pool.rs` — Lease::settled and Lease::drop define reuse versus quarantine.
- `crates/eventlog-postgres/tests/conformance.rs` — existing public pool-observation, cancellation, and shutdown regressions.
- `crates/eventlog-postgres/examples/production-proof.rs` — required case selection and evidence runner.
- `docs/stories/EL-003-projections-inline-and-catch-up.md` — delivered catch-up contract.

Inferred edit scope: run_catch_up, focused PostgreSQL integration tests, and the proof runner if a new case must become mandatory. Pool implementation changes are not assumed necessary.

This story shares `crates/eventlog-postgres/src/lib.rs` with `story:prevent-stale-snapshots-after-redaction`; sequence their edits or explicitly reconcile that shared file. No concurrency is scheduled by filing these stories.

## Scope confirmation

Implemented15c049633363dd0f504c7ccaeb36ee01c1bb4bd6 touched exactly src/lib.rs, tests/conformance.rs and examples/production-proof.rs within eventlog-postgres. Inferred src/pool.rs edit was unnecessary; existing quarantine behavior proved correct. Two successful early-return branches await rollback then settle; all errors/cancellation retain quarantine. Three required regressions include eight repeated no-work polls on the same backend PID and a deterministic withheld-rollback response case.
