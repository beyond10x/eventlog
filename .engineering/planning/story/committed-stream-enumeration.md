---
format: aep.planning-md/1
id: story:committed-stream-enumeration
kind: story
status: draft
title: Enumerate committed streams independently of feed progress
relations:
- depends_on: story:the-log-and-its-two-backends
revision: 2
---
## Acceptance
File, SQLite and PostgreSQL return byte-ordered keyset pages of the exact tenant/type's committed stream identities immediately after successful writes, even when an unrelated PostgreSQL transaction holds the feed watermark back.

## Finding and design
ER PostgreSQL adapter acceptance failed because ids() used read_feed: an already committed subject was absent while another test held an older transaction (ER tests/persistence.rs recorded_contract, retained er-eventlog-postgres.log). Eventlog AGENTS.md explicitly documents cluster-wide watermark latency. Preserve that feed contract. Add optional EventStore::list_streams for committed inventory, with bounded pages and an exclusive stream-id continuation; providers without it explicitly refuse. Implement it against existing authoritative records and their tenant/type/id index. This is inventory, not a durable change cursor or a snapshot across pages: concurrent inserts before the cursor require a fresh enumeration. No DDL or persisted format changes. Redacted streams still exist; tenant erasure removes them.

## Scope
crates/eventlog-core/src/lib.rs; crates/eventlog-file/src/lib.rs; crates/eventlog-sqlite/src/lib.rs; crates/eventlog-postgres/src/lib.rs; provider tests; crates/eventlog-conformance; CHANGELOG.md; docs/design/committed-stream-enumeration.md.

## Verification
Use one conformance exercise across the providers for ordering, paging, tenant/type boundaries, redaction and erasure. Add a deterministic PostgreSQL test holding an unrelated transaction while asserting a withheld feed and immediately visible inventory. Prove sensitivity by replacing inventory with the watermarked path. Run only affected checks; the operator prohibited another full gate. This is one prerequisite story, not a parallel decomposition.

## Implementation and evidence

Implemented additive EventStore::list_streams and all three concrete providers without schema or persisted-format changes. Providers validate coordinates and use bounded byte-ordered pages; SQL reads existing indexed coordinates directly. The shared inventory exercise and deterministic unrelated-transaction regression passed: file 0.06s, PostgreSQL 0.06s, SQLite under 0.01s. Reintroducing the PostgreSQL feed watermark into inventory made the immediate-visibility assertion fail with an empty list; the mutation was restored and the final focused run passed. Strict Clippy for all targets of the five affected packages passed. Toolchain refresh completed. No full persistence/production gate or release was run. Work is published on its continuation branch before ER consumes it; main integration remains outstanding.

Evidence: local-evidence:ess-evolution-20260910/eventlog-inventory-final.log, eventlog-inventory-mutation.log, eventlog-inventory-clippy.log and eventlog-toolchain-refresh.log. The original ER failure is local-evidence:ess-evolution-20260910/er-eventlog-postgres.log.
