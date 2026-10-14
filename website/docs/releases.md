---
title: Release highlights
---

Each section summarizes one release, newest first. Its release notes and
[`CHANGELOG.md`](https://github.com/beyond10x/eventlog/blob/main/CHANGELOG.md) record the exact
changes.

## 0.9.0 — October 14, 2026

- Two opt-in on-disk layouts, chosen only when a store is created. The defaults are unchanged:
  `TreeEventStore::open` still creates `eventlog-tree/2` and `FileEventStore::open` still creates
  `eventlog-file/1`, an existing store keeps its layout, nothing is migrated, and this release
  reads every layout. Earlier releases refuse the new ones, so opt in only once every reader of
  the store has 0.9.0.
- Tree store: `TreeLayout::Hashed` (`eventlog-tree/3`) changes the on-disk stream paths to
  `streams/<kk>/<h>`, so every event path fits a Windows Git checkout without long paths for a
  tenant of up to 36 escaped bytes. Tools that read stream directories by path must read them
  through the API instead.
- File provider: `FileLayout::Segmented` (`eventlog-file/2`) seals the history into segments, so a
  resumed handle compares only the segments it cannot trust: about 7 times fewer bytes after its
  own write in the measured case.
- Fixes: a capture after a write resumes from the writer's view; a resumed handle no longer drops
  snapshots for another handle's frames; a dropped writer lock is released even with a forked
  child; two handles creating one tree store both open; `rustls` 0.23.46.
- The SQLite capture checkpoint is encoded with types generated from the ESS specification.

Read the [0.9.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.9.0).

## 0.8.4 — October 13, 2026

- Tree store: opening a store while another handle commits a group no longer reports a corrupt
  store. The open reads only the groups it listed before reading events; a group committed after
  the list is treated as torn, and a listed group whose event is missing is still refused.

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
  or as an append delta only through an unbroken journal chain; anything else yields a complete
  capture. Other providers keep complete capture.
- Enabling continuity is one-way for older readers: 0.7.0 and earlier refuse such a store until
  `disable_durable_continuity` removes it.
- SQLite capture deltas carry the stored text a complete capture reads, and trigger checks match
  table names without regard to case.

Read the [0.8.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.8.0).

## 0.7.0 — October 7, 2026

- Optional provider-owned capture checkpoints distinguish complete observations, unchanged state
  and acknowledged append deltas. SQLite tracks SQL-visible changes across connections in a
  bounded journal and falls back to a complete capture after uncertainty. Checkpoints are
  transient and stay with their provider instance and request scope.

Read the [0.7.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.7.0).

## 0.6.0 — September 27, 2026

- Tree stores keep long text once. The new layout `eventlog-tree/2` may keep a JSON blob as a
  manifest whose long string literals are stored once per tenant; readers serve exactly the
  blob's bytes. New stores use `eventlog-tree/2`.
- `eventlog_tree::migrate` moves an `eventlog-tree/1` store to `eventlog-tree/2`, verifying before
  and after. It never runs on open, and earlier readers refuse migrated stores.

Read the [0.6.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.6.0).

## 0.5.0 — September 26, 2026

- SQLite implements `AtomicEventStore::append_group_guarded_with_blobs`: a group and its blobs
  commit in one transaction. PostgreSQL still refuses the method.
- File and SQLite accept a guarded blob-bearing retry of a group committed through
  `AtomicBlobEventStore::append_group_with_blobs`.
- `eventlog_tree::copy` linearizes a tree store into a SQLite store, keeping event ids, instants,
  receipts, identities, blobs and redactions.
- A SQLite database file hashes each distinct blob content at most once per handle.
- A rolled-back SQLite append returns the restored identities it took, and PostgreSQL
  `stream_identity` no longer commits on every call.

Read the [0.5.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.5.0).

## 0.4.0 — September 24, 2026

- `eventlog-tree`, a file event store whose history merges under version control, joins the
  workspace, with fork vocabulary (`Expected::Merge`, event digests and parents, capabilities)
  that linear providers refuse as `Unsupported`.
- `EventStore::read_many` answers a batch of reads; File answers it in one transaction.
- File stops re-hashing an unchanged committed journal, and a refused read keeps the handle's
  verified view.
- SQLite validates an existing blob's stored length, integrity hash and edition before a fresh
  atomic group reuses it. This closes the 0.3.0 limitation.
- `Expected`, `Capabilities` and `EventLogError` are `#[non_exhaustive]`: an exhaustive downstream
  `match` needs one wildcard arm.

Read the [0.4.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.4.0).

## 0.3.0 — September 22, 2026

- File joins SQLite and PostgreSQL as a durable provider.
- Native atomic blob/event publication is available across all three providers. File additionally
  supports grouped blob binding under one durability barrier.
- Existing-only open, strict File/SQLite inspection, complete tenant capture and inline projection
  administration make provisioning and observation boundaries explicit.
- File reuses verified history while checking the committed prefix for damage. Deferred capture
  verifies content when read.
- SQL blob reads verify integrity; differing bytes under an existing binding refuse.
- Limitation: a fresh SQLite atomic group reusing an existing blob binding compares its bytes but
  does not validate all stored integrity metadata. 0.4.0 adds the check.

Read the [authoritative 0.3.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.3.0)
for exact changes and the File method-name collision. The
[full changelog](https://github.com/beyond10x/eventlog/blob/0.3.0/CHANGELOG.md)
retains earlier releases as history.
