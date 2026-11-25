---
title: Release highlights
---

Each section summarizes one release, newest first. Its release notes and
[`CHANGELOG.md`](https://github.com/beyond10x/eventlog/blob/main/CHANGELOG.md) record the exact
changes.

## 0.11.0 — November 25, 2026

This release brings a new PostgreSQL schema edition and a new Tree group file format. It has
breaking changes:

- PostgreSQL stores a digest of each indexed value in a generated column,
  `idx_N_digest BIGINT GENERATED ALWAYS AS (hashtextextended(idx_N, 0)) STORED`, and indexes it.
  The digest also runs on a server in FIPS mode, which refuses `md5()`.
- Every provider indexes an indexed projection value of any length again.
  `EventLogError::IndexedValueTooLarge` is no longer returned.
  `eventlog_core::MAX_INDEXED_VALUE_BYTES` is deprecated.
- Every PostgreSQL projection table gains the index `<table>_key_c`. On a database that is not
  C-collated, a projection page walks this index in byte order.
- PostgreSQL `migrate` rewrites every projection table inside its transaction. Reads and writes
  of a table wait until it commits, for a time that grows with the table.
- Eventlog 0.10.0 refuses a PostgreSQL schema that 0.11.0 migrated. Eventlog 0.11.0 refuses a
  schema that was not migrated. No new grants are needed.
- A PostgreSQL tenant capture holds the owner's publication gate only while its snapshot is
  fixed. Appends, redactions and erasures no longer wait for a capture read.
- A capture whose tenant was erased or had an event redacted during its read is refused with
  `CaptureError::TenantIdentityMissing` or `CaptureError::RedactedHistory`.
- SQLite: `RestoredEvent` has the new field `redacted`. A struct literal adds `redacted: None`.
- SQLite: `SqliteEventStore::group_fingerprint` and `restore_group_fingerprint` are new.
- A new Tree store writes group files in the format `eventlog-tree/group/2`. This format keeps the
  request fingerprint of an atomic group. Eventlog 0.10.0 refuses it.
- A Tree group retry after a redaction or a blob deletion gets its original receipt after a
  reopen.
- Tree `migrate(root, MigrationMode::Apply)` moves an existing store to `eventlog-tree/group/2`.
  `MigrationReport` has three new fields.
- A Tree group that was redacted, or whose blob was deleted, before the migration gets no
  fingerprint.
- Tree stores a tenant whose escaped name is longer than 255 bytes under `tenants/%%<digest>`.
  Eventlog 0.10.0 refuses such a store.
- Tree serves history that a `git checkout`, `pull` or `merge` added while a handle was open only
  after `TreeEventStore::reload` or a reopen. `reload` is new.
- Tree `copy` hands a redacted event to an inline projector as redacted.
- PostgreSQL `projection_find` reuses a prepared statement on each pooled connection from its
  second use, and every pooled session sets `plan_cache_mode = force_custom_plan`. A caller-owned
  connection must be a dedicated session, not one behind a transaction-mode or statement-mode
  pooler. Median `projection_find` latency measured on CI is 0.98 of 0.10.0's (4 runs; the bound
  is 1.05) and 0.906 of the commit before the change.

To upgrade a PostgreSQL owner, stop every 0.10.0 binary of the owner. Run
`PostgresEventStore::migrate` with the migration role. Then start 0.11.0.

To upgrade a Tree store, first upgrade every binary that reads it. Then run `migrate` once on the
store.

Read the [0.11.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.11.0).

## 0.10.0 — November 23, 2026

This release fixes the findings of the 0.9.4 review, adds a KPI gate and has breaking changes:

- A redaction reason must be an owner-defined code, and free text is refused with `Invalid`. The
  Tree store maps reasons stored as free text to `legacy-reason` when it reopens or copies a
  history.
- Every provider refuses an indexed projection value over 2,048 bytes with
  `EventLogError::IndexedValueTooLarge`.
- The File store reports `EventLogError::Committed { cleanup }` when a commit is published but its
  cleanup fails.
- PostgreSQL adds the table `<prefix>_settlement_floor`: after a `pg_restore`, run `migrate`
  before you open the history.
- The PostgreSQL application role keeps only `SELECT` on the migration metadata.
- `canonical_request_hash` is new, with the `c1:` prefix, and `request_hash` is unchanged for
  existing keys.
- `PostgresConnectionAuthority::cancel` is new and reports `Unsupported` by default.
- PostgreSQL serializes inline projectors per prefix, name and tenant.
- Opening a hosted PostgreSQL history takes 152 round trips instead of 1,085.

Read the [0.10.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.10.0).

## 0.9.4 — November 11, 2026

- SQLite: the bundled SQLite moves to 3.54.1 through `rusqlite` 0.41.0. The upgrade fixes
  RUSTSEC-2026-0459, where SQLite before 3.54.0 could return uninitialized memory when it opened
  a database with a `-wal` file of a different page size.
- The SQLite provider's API and on-disk format are unchanged.

Read the [0.9.4 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.9.4).

## 0.9.3 — November 11, 2026

- PostgreSQL: `read_many` answers stream reads in statements of up to 1,000 reads inside one
  snapshot, and `append_group` inserts a group's events in batched statements. 10,000
  single-row reads take about 0.2 s instead of 7 s, and a group of 5,000 events about 0.4 s
  instead of reaching the 10 s deadline.
- Each batched statement's deadline scales with the reads it carries, so a batch is refused
  only where its separate reads would be.

Read the [0.9.3 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.9.3).

## 0.9.2 — October 31, 2026

- No library crate changed. The persistence proof runs with ess 0.75.0, and the generated capture
  types are regenerated with it.
- The documentation site moves to docs-system 0.12.0.

Read the [0.9.2 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.9.2).

## 0.9.1 — October 16, 2026

- PostgreSQL: a connection whose lease is dropped after a deadline or a cancelled call closes on
  the dropping thread, on any runtime. The pool sends the server a cancel for a backend blocked in
  a query, so an abandoned transaction no longer holds its locks until the server's own timeout.
- PostgreSQL: a guarded append or append group whose deadline fires before COMMIT was written
  returns `EventLogError::Deadline`, which is safe to retry, and `UnknownCommit` now means COMMIT
  was sent. No variant changed, but a caller that matches only `UnknownCommit` sees `Deadline` for
  these timeouts.

Read the [0.9.1 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.9.1).

## 0.9.0 — October 14, 2026

- Two opt-in on-disk layouts, chosen only when a store is created. The defaults are unchanged:
  `TreeEventStore::open` still creates `eventlog-tree/2` and `FileEventStore::open` still creates
  `eventlog-file/1`. An existing store keeps its layout and is not migrated, and this release
  reads every layout. Earlier releases refuse the new layouts, so opt in only after every reader
  of the store has 0.9.0.
- Tree store: `TreeLayout::Hashed` (`eventlog-tree/3`) changes the on-disk stream paths to
  `streams/<kk>/<h>`, so every event path fits a Windows Git checkout without long paths for a
  tenant of up to 36 escaped bytes. Tools that read stream directories by path must read them
  through the API.
- File provider: `FileLayout::Segmented` (`eventlog-file/2`) seals the history into segments, so a
  resumed handle compares only the segments it cannot trust: about 7 times fewer bytes after its
  own write in the measured case.
- Fixes:
  - A capture after a write resumes from the writer's view.
  - A resumed handle no longer drops snapshots for another handle's frames.
  - A dropped writer lock is released even with a forked child.
  - Two handles creating one tree store both open.
  - `rustls` 0.23.46.
- The SQLite capture checkpoint is encoded with types generated from the ESS specification.

Read the [0.9.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.9.0).

## 0.8.4 — October 13, 2026

- Tree store: opening a store while another handle commits a group no longer reports a corrupt
  store. The open reads only the groups it listed before reading events, treats a group committed
  after the list as torn, and still refuses a listed group whose event is missing.

Read the [0.8.4 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.8.4).

## 0.8.3 — October 8, 2026

- PostgreSQL: `PostgresEventStore::pool_churn` reports the connections a handle's pool opened,
  the ones it retired by cause and the ones that replaced a retirement. `PoolStatus` is unchanged.
- Capacity laboratory: workers report pool churn, the comparative `<= 8` connection budget counts
  only the workload's own backends, and each configuration starts from an empty
  `pg_stat_statements`.

Read the [0.8.3 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.8.3).

## 0.8.2 — October 8, 2026

- File and PostgreSQL hash each distinct blob content once per handle, as SQLite already did.
  Every read still reads the stored bytes and hashes any bytes that differ from content the
  handle verified, so changed content is still refused. Nothing persisted changes.
- The `ess/effects` specification validates on ess 0.56.0, with its condition unchanged.

Read the [0.8.2 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.8.2).

## 0.8.1 — October 8, 2026

- File re-checks a recently changed committed prefix by comparing bytes instead of hashing them.
  A handle keeps its committed prefix in memory once: budget the committed size of
  `events.jsonl` per handle, up to about twice that as its own appends grow the buffer.
- File finds stream heads, stream windows, retried receipts and redaction targets through a
  per-stream index, and replay is no longer quadratic. Feed, catch-up and projection rebuild still
  walk the store in global order. The on-disk format `eventlog-file/1` is unchanged.

Read the [0.8.1 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.8.1).

## 0.8.0 — October 7, 2026

- SQLite offers durable capture continuity: a capture checkpoint a later process can continue
  from, after `SqliteEventStore::enable_durable_continuity`. A checkpoint continues as unchanged
  or as an append delta only through an unbroken journal chain, and otherwise yields a complete
  capture. Other providers keep complete capture.
- Enabling continuity is one-way for older readers: 0.7.0 and earlier refuse such a store until
  `disable_durable_continuity` removes it.
- SQLite capture deltas carry the stored text a complete capture reads, and trigger checks match
  table names case-insensitively.

Read the [0.8.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.8.0).

## 0.7.0 — October 7, 2026

- Optional provider-owned capture checkpoints distinguish complete observations, unchanged state
  and acknowledged append deltas. SQLite tracks SQL-visible changes across connections in a
  bounded journal and falls back to a complete capture after uncertainty. Checkpoints are
  transient and stay with their provider instance and request scope.

Read the [0.7.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.7.0).

## 0.6.0 — September 27, 2026

- Tree store: the new layout `eventlog-tree/2` may keep a JSON blob as a manifest whose long
  string literals are stored once per tenant, and readers serve exactly the blob's bytes. New
  stores use `eventlog-tree/2`.
- `eventlog_tree::migrate` moves an `eventlog-tree/1` store to `eventlog-tree/2` and verifies the
  store before and after. It does not run on open, and earlier readers refuse migrated stores.

Read the [0.6.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.6.0).

## 0.5.0 — September 26, 2026

- SQLite implements `AtomicEventStore::append_group_guarded_with_blobs`: a group and its blobs
  commit in one transaction. PostgreSQL still refuses the method.
- File and SQLite accept a guarded blob-bearing retry of a group committed through
  `AtomicBlobEventStore::append_group_with_blobs`.
- `eventlog_tree::copy` linearizes a tree store into a SQLite store and keeps event ids, instants,
  receipts, identities, blobs and redactions.
- A SQLite database file hashes each distinct blob content at most once per handle.
- A rolled-back SQLite append returns the restored identities it took, and PostgreSQL
  `stream_identity` no longer commits on every call.

Read the [0.5.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.5.0).

## 0.4.0 — September 24, 2026

- `eventlog-tree`, a file event store whose history merges under version control, joins the
  workspace. Linear providers refuse its fork vocabulary as `Unsupported`: `Expected::Merge`,
  event digests and parents, and capabilities.
- `EventStore::read_many` answers a batch of reads, in one transaction on File.
- File stops re-hashing an unchanged committed journal, and a refused read keeps the handle's
  verified view.
- SQLite validates an existing blob's stored length, integrity hash and edition before a fresh
  atomic group reuses it, which closes the 0.3.0 limitation.
- `Expected`, `Capabilities` and `EventLogError` are `#[non_exhaustive]`: an exhaustive downstream
  `match` needs one wildcard arm.

Read the [0.4.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.4.0).

## 0.3.0 — September 22, 2026

- File joins SQLite and PostgreSQL as a durable provider.
- Native atomic blob/event publication is available across all three providers, and File also
  supports grouped blob binding under one durability barrier.
- Existing-only open, strict File/SQLite inspection, complete tenant capture and inline projection
  administration make provisioning and observation boundaries explicit.
- File reuses verified history while checking the committed prefix for damage. Deferred capture
  verifies content when read.
- SQL blob reads verify integrity and refuse differing bytes under an existing binding.
- Limitation: a fresh SQLite atomic group reusing an existing blob binding compares its bytes but
  does not validate all stored integrity metadata. 0.4.0 adds the check.

Read the [0.3.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.3.0) for the
File method-name collision. The
[full changelog](https://github.com/beyond10x/eventlog/blob/0.3.0/CHANGELOG.md) records earlier
releases.
