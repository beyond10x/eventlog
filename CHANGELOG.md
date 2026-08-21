# Changelog

All notable changes to this component are recorded here. Versions are component-scoped and released
under `eventlog-v*` tags.

## Unreleased

### Added

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
