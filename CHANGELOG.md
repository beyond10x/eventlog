# Changelog

All notable changes to this component are recorded here. Versions are component-scoped and released
under bare-version tags such as `0.1.0`.

## [Unreleased]

### Added

- File, SQLite and PostgreSQL expose bounded committed stream inventory for immediate
  enumeration without waiting for feed watermarks. Tenant/type isolation, byte ordering,
  redaction and erasure follow the stored stream coordinates; no schema migration is needed.

## 0.2.0 — 2026-09-10

### Added

- Add the local `eventlog-file` provider with versioned JSONL transactions, process-safe atomic
  groups, durable retry identities, verified blob storage and recoverable privacy rewrites.
  Redacted file-store projections require a complete rebuild before serving new writes; existing
  receipts remain resolvable. File-provider verification and operating limits are documented in
  `docs/design/file-provider.md`.
- SQLite and PostgreSQL implement ordered atomic append groups with one tenant-scoped
  idempotency identity, actual-content retry matching and transaction-local expectations.
  Required projections and group admission commit or roll back with the entire group.
- Projection and guard contexts can read tenant-scoped blobs inside their current transaction.
  Existing single-stream append entrypoints retain their behavior.

### Changed

- Adopt independent common security/privacy gates with signed local evidence and bot delivery.
  Cache Rust proof builds and cancel superseded PR runs while retaining all persistence proof lanes.
- PostgreSQL admits the new group bookkeeping through an additive, physically checked migration;
  previous schema editions remain migratable. Hosted writers require the new table's DML grants.

## 0.1.0 — 2026-09-09

### Added

- Coded guard refusals preserve an owner-selected stable code while rolling back events and
  transactional guard projection writes on both backends.
- Generic effect stages, evidence and boundary coverage inventories, with bounded opaque
  identifiers/codes and an ESS semantic vocabulary.
- Hosted PostgreSQL admission with verified TLS, separate migration/application roles, exact
  schema and projection-roster checks, bounded connection pools and transactional admission counters.
- Required real-backend production proof and comparative capacity/restart tooling with retained
  source, binary and runner identities.

- `eventlog-core`: the event envelope, `StreamId` with a mandatory tenant, `Expected` for
  optimistic concurrency, `CommandMeta` with cause-derived idempotency, and the `EventStore` port.
- `eventlog-sqlite`: the log on SQLite, file and `:memory:`, with per-owner table prefixes.
- `eventlog-postgres`: the log on PostgreSQL 13 or later, with a commit watermark so a feed reader
  cannot skip an event that committed late.
- `eventlog-conformance`: one exercise covering stream versioning, expected-version conflicts,
  idempotent retries, refused key reuse, tenant isolation, feed resumption, snapshots, redaction
  with snapshot invalidation, and tenant erasure.
- `eventlog-core`: `Aggregate`, `DomainEvent`, `Repository` with snapshot-plus-tail loading, a
  single conflict retry, and a snapshot policy; `Applied::Redacted` so a fold stays total after an
  erasure.
- `eventlog-core`: `Projector`, `ProjectionSpec`, `ProjectionStore` and `CatchUpRunner`. Read
  models are declared in Rust and compiled to both dialects; a projection is inline or catch-up,
  never both; a guard reads an inline read model inside the append transaction and a guard over a
  catch-up one is refused.
- Both backends: projection tables, durable cursors, projection rebuild from the log, and
  `pg_try_advisory_xact_lock` around a PostgreSQL catch-up pass.

### Fixed

- Public deserialization and append validation enforce stream, event and claim constructor
  invariants, rejecting invalid batches before any durable or projection write.

- PostgreSQL rejects rewrite rules on every owned durable or projection table, including during
  explicit projection migration, preserving command receipts and idempotent retries.

- Checked snapshot generations prevent delayed caches from restoring redacted or erased state.
  Existing event/snapshot columns remain unchanged; old caches without provenance are ignored.
  The legacy snapshot-save API now refuses writes without an observed generation.
- Repository automatic snapshot failures preserve the successful committed command result.
  Explicit snapshot creation retries one stale generation and reports persistent changes/errors.

- PostgreSQL empty or contended catch-up polls await rollback and reuse the same settled
  connection. Failed or cancelled rollback keeps the connection quarantined.
- PostgreSQL publication ordering and contiguous feed reads preserve lower positions across
  reversed transaction/position order and unrelated transactions holding the watermark.
- Tenant erasure includes retained legacy projection tables and refuses ambiguous namespaces.
