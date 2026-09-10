---
format: aep.planning-md/1
id: verification-report:atomic-groups-20260910
kind: verification-report
status: draft
title: SQL atomic append group implementation and independent verification
relations:
- verifies: story:atomic-append-groups
revision: 2
---
## Implemented
AtomicEventStore, AppendGroup, StreamAppend and ordered results are additive to EventStore. Both SQL providers share their existing append engine within one group transaction. Fingerprints bind actual ordered content and all metadata. Groups have one tenant-scoped identity and no independently retryable child commands. Transaction-scoped blob reads use the current tenant and connection. PostgreSQL adds physical schema admission, legacy checksum migrations and hosted permissions; erasure includes group bookkeeping.

## Verification
The exact local command was bash scripts/gate.sh --production-proof with PostgreSQL 17.6, the repository's disposable production-fixture TLS setup, a dedicated application role, CARGO_BUILD_JOBS=2, CARGO_PROFILE_DEV_DEBUG=0, CARGO_PROFILE_TEST_DEBUG=0 and CARGO_INCREMENTAL=0. Final gate exit 0. Its mandatory conformance receipt reports 104 passed, 0 failed, 0 ignored/skipped and no missing required cases. Formatting and all-target strict Clippy pass.

Shared assertions cover request order, same-stream expectations, retry identity and changed content, guard/projection rollback and tenant-scoped blob reads. Concurrent tests use reversed request orders and simultaneous identical keys; SQLite uses independently opened database connections. SQLite additionally verifies reopen retries. PostgreSQL verifies original and snapshot-edition migrations and foreign group-table shape refusal.

Mutation checks: hashing only the entry count caused the changed-content assertion to fail (exit 101); committing an otherwise failed SQLite group caused the no-partial-event assertion to fail (exit 101, observed 1 event instead of 0). Both mutations were reverted before the final required gate.

## Evidence location
Exact logs, mandatory proof JSON, final source file hashes, the source patch and mutation outputs are retained in the session's private ess-evolution-20260910 evidence directory. Base revision: 2e59179003f59c5ffa1a564094186642b6b7136e; this is an uncommitted candidate, not evidence that the base commit contains the feature.

## Remaining boundary
No Eventlog file provider, ER executor/adapter or application migration is claimed. No source publication, release, deployment or production capacity evidence is claimed. The new table requires a fenced writer/eraser rollout and hosted DML grants as documented in docs/design/atomic-append-groups.md.

## Final erasure and redaction verification
The required production proof was repeated after adding shared assertions for live tombstones in group retries, tenant erasure of group identities, foreign-tenant isolation and fresh identities after erasure. The gate exited 0: 104 passed, 0 failed, 0 skipped, no missing required cases, with formatting and strict Clippy green. Exact final logs: eventlog-production-gate-privacy.log, production-proof-privacy.json and production-proof-raw-privacy.log in the private session evidence directory.

Removing append_groups from SQLite tenant erasure caused the shared test to fail at the stale group identity assertion (exit 101). The mutation was reverted before this proof. mutation-erasure.log retains the failure; the earlier changed-content and rollback mutations are retained separately. The only later source adjustment clarified the EventStore provider documentation to match ADR 05; no executable code changed after this gate.
