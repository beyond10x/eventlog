---
format: aep.planning-md/1
id: story:reject-postgres-rewrite-rules
kind: story
status: implemented
title: Reject rewrite rules during PostgreSQL admission
tags:
- code-review
- priority-p1
relations:
- derived_from: review-result:postgres-broader-pass-1
- depends_on: story:prevent-stale-snapshots-after-redaction
scope:
- confidence: cited
  path: crates/eventlog-postgres/src/schema.rs
- confidence: cited
  path: crates/eventlog-postgres/tests/conformance.rs
revision: 7
---
## Problem and reachability

At source15c049633363dd0f504c7ccaeb36ee01c1bb4bd6, schema.rs:144 shape checks omit pg_rewrite. A command-discarding ON INSERT DO INSTEAD NOTHING rule passes public local constructor and verified-TLS application-role open. The same command then appends versions1 and2 with no durable command receipt. Two real PostgreSQL regressions fail, all29 previous conformance cases pass. Exact report/caller/runner output: review-result:postgres-broader-pass-1. This violates append idempotency and serves O2/O6 by preserving trustworthy durable decisions.

## Acceptance

Every admission or registration path rejects a rewrite rule attached to an Eventlog-owned durable or projection table before serving; ordinary supported tables continue to admit, and retries retain their exact durable receipts.

## Implementation and verification

Reject pg_rewrite entries through the common physical-shape validator, not a special check for only commands. Cover local migration/connect, hosted application-role open and projection registration. Retain the two adversarial regressions, add table-class coverage where needed, run mutation proof, then the complete production gate with real TLS and dedicated application role. This tightens existing exact-shape admission; no DDL or new entity.

## Scope

Cited crates/eventlog-postgres/src/schema.rs and tests/conformance.rs. Coordinator owns production-proof required-case roster and docs. Wait for snapshot schema/test edits to integrate, then implement in parallel with the disjoint common-input-validation unit.

## Scope and result confirmation

Commit6bbb06d5a21412a8c51ae69e5462b4eb28d36460 touches exactly schema.rs and tests/conformance.rs. Review discovered that explicit projection migration skipped the shared physical-shape validator, so the fix invokes that check inside the existing migration transaction as well as rejecting pg_rewrite in the common validator. Four focused cases cover all eleven actual durable tables, local and hosted admission, projection migration/create/register, and retained valid receipts. The disabled-guard mutant fails all four cases. Independent review found no additional issue. Final integration09c71ec3165e187f7baf6277ffa40079d863f3cd ran98 cases with no failures/skips. Coordinator added all four cases to the mandatory production roster.
