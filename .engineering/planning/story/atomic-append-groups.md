---
format: aep.planning-md/1
id: story:atomic-append-groups
kind: story
status: implemented
title: Atomic ordered append groups under one durable identity
refs:
- provider: ess
  reference: initiative:ess-evolution
relations:
- informed_by: architecture-decision-record:ess-evolution-05-file-and-atomic-groups
revision: 5
---
## Context
EventStore currently appends one stream per transaction. ER requires ordered multi-entity atomic recording. Existing StreamId, Expected, NewEvent, CommandMeta, AppendResult and Guard semantics are declared in crates/eventlog-core/src/lib.rs; this work adds a storage operation over them, not a product entity.

## Acceptance
SQLite and PostgreSQL commit or roll back every ordered group entry, guard and required projection under one tenant-scoped idempotency identity, return the original retry result, refuse changed content and preserve transaction-local same-stream expectations and all existing single-append behavior.

## Scope
Cited: crates/eventlog-core/src/lib.rs and projection.rs; crates/eventlog-sqlite/src/lib.rs; crates/eventlog-postgres/src/lib.rs and schema.rs; crates/eventlog-conformance; provider tests and existing gates.
Inferred additions: atomic_group modules and additive group bookkeeping tables, transaction-scoped blob reads. No file provider claim in this story; the separate file-provider prerequisite depends on the admitted group port.

## Verification
Shared behavioral cases for ordering/retries/conflicts/rollback/guards/projection visibility, real SQL providers and exact locked gate. Missing PostgreSQL execution is explicit missing evidence.

## Publication

Publish with story:file-eventlog under the operator's 2026-09-10 instruction. The complete candidate passed the local mandatory PostgreSQL/SQLite/file proof with 129 passed and no failed or skipped cases. The operator explicitly waived the expensive remote Actions gate for this source push; [skip ci] will suppress its push trigger. Source checksums match the retained verified candidate. Record remote revision after publication; no release or consumer migration is claimed.
