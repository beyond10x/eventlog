# Changelog

All notable changes to this component are recorded here. Versions are component-scoped and released
under bare-version tags such as `0.1.0`.

## [Unreleased]

## 0.10.0 — 2026-11-23

### Added

- KPI gate: `kpi/kpis.json` defines deterministic work counters and timings per provider,
  `kpi/baseline.json` records them, and the `eventlog-kpi` runner checks the counters
  (`bash scripts/gate.sh --kpi`) and A/B-compares timings between two revisions. The providers'
  counters exist only behind the non-default `kpi-counters` feature; no public API changes.
  CI runs them as two jobs: `KPI / KPI counters` (meant to become a required check) and
  `KPI / KPI timing` (the A/B on pull requests, informational only).
- Core: `Projector::fingerprint`, an optional digest of a projector's configuration and version.
  The default, `None`, changes nothing except the tree store's kept state (below).
- SQLite: `SqliteEventStore::restore_events_at` queues restored events with the global position
  each takes, `positions` reads every event's position and the highest one ever assigned
  (`EventPositions`), and `reserve_positions_through` keeps later appends above a position.
- Core: `EventLogError::Committed { cleanup }`, returned when a commit is published and its
  outcome known but the cleanup after it failed. The command is not to be retried as failed.
- PostgreSQL implements `AtomicEventStore::append_group_guarded_with_blobs`: admission, the
  batch's blob bindings, the members and the receipt commit in one transaction, so a refused
  guard binds no blob and a batch costs one commit instead of one per blob. The sorted digests are
  recorded in the new table `<prefix>_group_batches`; retries follow the shared contract of File
  and SQLite. This is a new schema edition: run `PostgresEventStore::migrate` with the migration
  role before opening with the application role (hosted `open` refuses the previous edition), and
  grant the application role `SELECT, INSERT, UPDATE, DELETE` on the new table.
- PostgreSQL: the same edition adds the index `<prefix>_events_publication` on
  `(committed_xid, global_seq)`. A feed, catch-up or rebuild page finds the first position still
  in flight with an index range scan instead of reading every event of the owner. `migrate`
  builds it inside its transaction, so the events table is locked against writes while it builds.
- PostgreSQL: the same edition adds the index `<prefix>_scope_counters_bytes` on the byte order of
  the admission counter coordinates. Tenant erasure removes the tenant's counters with an index
  range scan instead of reading every counter while it holds the exclusive publication lock.
- PostgreSQL: the same edition adds the table `<prefix>_settlement_floor`, written only by
  `migrate` (see Fixed). Grant the application role `SELECT` on it, nothing more.
- `eventlog_core::MAX_INDEXED_VALUE_BYTES` (2,048) and `indexed_values`: every provider refuses a
  projection row whose indexed field value is longer, before writing, with the new
  `EventLogError::IndexedValueTooLarge { field, bytes, limit }`. On PostgreSQL such a value could
  exceed the B-tree entry limit and abort the append with a backend error; SQLite and the file
  provider accepted it. A projector that indexes longer values must index a shorter key instead.
- PostgreSQL: `PostgresConnectionAuthority::cancel` sends the server-side cancel request for a
  caller-owned session. The pool calls it when it retires an unsettled lease. The default
  reports `Unsupported`, so existing authorities compile unchanged; with it, a retired query on a
  server without disconnect polling (PostgreSQL 13, or a platform without
  `client_connection_check_interval`) can keep running and holding locks after the pool returned
  its permit, until it finishes or reaches its statement or lock timeout.

### Changed

- Every provider's `redact` takes the reason as an owner-defined code, such as
  `subject-request`, and refuses free text with `EventLogError::Invalid` before it writes
  anything. The tombstone keeps the reason after the body is gone, so a name or an address in an
  explanation was read back by everyone who read the redacted event. A code takes the grammar of
  an identity (`validate_identity`): printable ASCII, no space, no `@`; the new
  `validate_redaction_reason` checks it, and the kit defines no codes. Stricter input on a public
  API: a minor release. A tree store an earlier release redacted with free text still opens and
  copies: the replay and `eventlog_tree::copy` redact with the fixed code
  `eventlog_tree::LEGACY_REDACTION_REASON` (`legacy-reason`), and the event file keeps its bytes.
  Consumers whose test fixtures pass free-text reasons and fail on this release: conductor tests
  (`erased on request`), entity-runtime `crates/entity-eventlog/tests/providers.rs`, ekr-store
  tests, service-sdk `service-eventlog` and the `service-host` persistence fixtures. Shared
  exercise case `a_free_text_redaction_reason_is_refused_before_writing`.
- PostgreSQL: schema admission (`open`, `migrate`, projection validation) reads each catalog
  property for all of a check's relations in one query: hosted `open` makes about 14 round trips
  for its schema and privilege checks instead of about 190. Every admission requirement and every
  refusal message is unchanged.
- PostgreSQL: `AtomicBlobEventStore` hashes each blob once (the fingerprint's SHA-256 is the
  integrity column), binds from the caller's bytes without copying the request, and reads a
  stored blob back only when its digest was already bound.
- PostgreSQL: a guard's scope reservation takes one statement per phase (advisory locks in sorted
  order, the counter read, the counter write) instead of three per scope: 64 scopes cost the same
  round trips as one. A scope whose value does not change is not written, so a zero-delta check
  creates no counter row and writes no WAL; the locks and ceiling checks are unchanged.
- PostgreSQL: consistent tenant capture sizes each page of its event, blob and projection-row
  reads from the remaining payload allowance and the largest item that read has admitted, from
  one row up to 1,000. The first page of each read holds one row, so a first body over the
  allowance is refused after one row arrived instead of after a full page (a 1,000-row page of
  8 KB bodies was 8.2 MB on the wire).
- PostgreSQL: `read_stream` and the stream reads of `read_many` still read one row past the page
  to decide `end_of_stream`, but no longer output, detoast or decode that row's event body; the
  next page reads it once.
- Verified blob content: a full memory drops its least recently used entries until a new content
  fits, instead of clearing every entry. Deletion and erasure through the handle still clear it.
- PostgreSQL: capture preflight takes the blob count, byte total and invalid-length count from one
  aggregate, scanning the tenant's blob bindings once instead of three times.

### Fixed

- Tree: a redaction, tenant erasure or blob deletion through one handle is no longer served by
  another open handle of the same store; reads check a privacy token published under the writer
  lock and rebuild from the files, and a read that overlaps a removal is served again.
- Tree: a removal of content deletes every kept state under `.cache/state/`, and a state is kept
  only under the writer lock while no removal has been published since its replay began.
- Tree: a kept state is reused only when every registered projector returns an unchanged
  fingerprint; a projector without one is folded again on each open.
- Tree: a write waits for the writer lock on a blocking worker, so two handles contending on one
  single-thread runtime no longer deadlock.
- Tree: a write whose caller stopped awaiting it between the engine and the files is rebuilt away
  on the next operation instead of being served or answered as deduplicated.
- Tree: a retry of a committed merge returns its receipt; the same key over other heads is an
  idempotency mismatch.
- Tree: erasing a tenant no longer counts another writer's unseen group for another tenant as
  loaded.
- Tree: `read_many` serves the whole batch from one observation, so it never shows half of an
  atomic group.
- Tree: an append hashes each event's envelope once instead of four times.
- Tree: a guarded group retried with its blobs is admitted again before its receipt answers it,
  as `AtomicEventStore::append_group_guarded_with_blobs` promises; a guard that refuses now
  refuses the retry.
- Tree: an open requests file metadata once per history file instead of twice, and not at all
  for a directory.
- Tree: `attach_inline_existing` refuses a projection that `create_projections` did not admit on
  the handle, or admitted with another shape, instead of creating it and reporting success.
- Tree: an open handle that rebuilds its engine (after another handle's commit, a merge, a
  redaction, an erasure or an inline attachment) keeps every served event's global position and
  places history new to it above the highest position it assigned, so a saved feed cursor no
  longer skips events. A rebuild no longer loads a kept state, which another replay ordered.
- Repository: a command the store answers as already recorded is answered from the stream it was
  recorded on. A tenant-wide claim retried under a freshly minted id used to fold the original
  stream's events into the retry id's aggregate and could cache that state under the retry id;
  the outcome now carries the recorded stream's state, nothing is cached for the retry id, and a
  claim recorded on a stream of another aggregate type is refused with `IdempotencyMismatch`.
  The same holds on one stream: a duplicate answered after other writers moved the stream past
  the revision this call loaded no longer skips their events.
- Repository: `load` is read-only. It no longer captures a snapshot generation, which wrote a
  durable generation row for every absent stream read on SQLite and PostgreSQL. It still starts
  from a cache only when the provider's provenance check accepts it; `handle` captures a
  generation only when its policy can write a snapshot, and `snapshot_now` always does.
- Repository: automatic snapshots count the events since the cache a command started from, and
  write at the committed revision. A command of several events used to step over the exact
  multiple and skip the snapshot, so a stream of two-event commands could be folded from zero on
  every command. The generation check is unchanged.
- SQLite and PostgreSQL: within one snapshot generation the cached revision never moves back. A
  delayed checked save of an older fold used to replace a newer cache and move the replay point
  back; it now leaves the newer cache and returns `true`. A cache from an earlier generation is
  still replaced at any revision. Shared exercise `run_snapshot_revision_is_monotonic`.
- SQLite: the populated legacy blob migration walks its `(tenant_id, digest)` cursor with one
  row-value range on the primary key instead of an expanded `OR` that rescanned the tenant's
  earlier digests on every row. Transaction, byte-count and integrity checks are unchanged.
- File: a deduplicated guarded retry of a blob-bearing group still runs its admission, but no
  longer commits what that admission staged. A counter reservation made by the retry used to be
  journaled with zero new events, so each retry charged the quota again; it is now rolled back,
  as SQLite rolls it back, and the original receipt is returned.
- File: forgetting a tenant the history does not hold no longer rewrites the journal or advances
  its epoch, so other open handles keep working; leftover snapshot and object disposal still runs.
- File: a redaction or tenant erasure drops the handle's capture view of the replaced history at
  once, including when the rewrite fails after publishing, instead of keeping it in memory until a
  later call.
- File: `rebuild_projection` refuses, before clearing a row, a projector whose name or table an
  attached inline projector owns ("rebuild requires a catch-up projection") unless it is that
  attached instance itself; it used to replace inline rows with any supplied projector's results.
  `run_catch_up` refuses a differently named projector over an inline table as well.
- File: a delayed `save_snapshot_checked` of an older fold no longer replaces a newer cached
  snapshot of the same generation; it returns true and keeps the newer one, so the replay point
  never moves back. A cache of an earlier generation is still replaced at any revision.
- File: a commit whose object or snapshot cleanup fails after publication returns
  `EventLogError::Committed` instead of `UnknownCommit` (atomic content) or a plain backend error
  (other writes, redaction, tenant erasure). The next commit that changes bindings, or a reopen,
  retries the cleanup.
- PostgreSQL: a redaction or tenant erasure whose deadline fires after `COMMIT` was sent returns
  `UnknownCommit` instead of `Deadline`, as append does; the change may have been applied.
- PostgreSQL: a projection whose `<table>_idx_<n>` index name would exceed 63 bytes gets one
  index per declared field under a shortened name with a digest of the table name. Before,
  PostgreSQL truncated every such name to the same identifier, only the first field was indexed,
  `migrate` succeeded and `open` refused the table. `migrate` now creates the indexes missing by
  definition, whatever their stored names, and compares the table's whole shape before it
  commits, as `open` does.
- PostgreSQL: a positive pool timeout under a whole millisecond is sent to the session rounded up
  (500 µs becomes 1 ms). Before, it was truncated to 0, which PostgreSQL reads as no statement,
  lock or idle-transaction limit.
- PostgreSQL: an owner restored from a logical backup delivers its feed again after `migrate`.
  Restored events keep the source cluster's transaction ids; one id ahead of this cluster's clock
  withheld the restored history and every later event of the owner, in every tenant, from feed,
  catch-up and rebuild until the clock passed it. `migrate` now finds a visible event whose
  transaction id this cluster has not handed out yet and records the highest position as settled
  in `<prefix>_settlement_floor`, under the exclusive publication gate; positions at or below the
  floor count as settled and the watermark applies above it. Hosted `open` refuses such an owner
  until then with `Invalid("PostgreSQL events carry another cluster's transaction ids (a restored
  history); run migrate with the migration role before opening")`. The restore procedure is:
  restore, then run `migrate`.
- PostgreSQL: dropping an operation that holds an unsettled pool lease on a thread outside any
  Tokio runtime no longer panics in the lease destructor. Retirement is spawned on the runtime
  the lease was acquired on, not on the dropping thread's ambient runtime.
- PostgreSQL: a grouped append, with or without blobs, starts its transaction deadline at its
  public entry. The wait for inline registration to freeze, which an inline rebuild can hold
  while it replays, is now spent from the same budget instead of being followed by a fresh full
  timeout, as on the single-append path.
- PostgreSQL: `recorded_command` reads a command receipt and its events in one read-only
  REPEATABLE READ snapshot, so a tenant erasure that commits between the two reads no longer
  returns a receipt with no events or with events of recreated stream coordinates. A receipt
  whose interval lacks an event is refused as a backend error instead of returned short.
- SQLite and PostgreSQL: a projector is driven inline or by catch-up, never both. A catch-up
  runner created before the same projector was registered inline used to fold every event a
  second time. `run_catch_up` now refuses an inline projector, as the file provider already did,
  and inline registration or attachment refuses a projector catch-up has already driven on that
  handle.
- PostgreSQL: an inline projector is no longer folded by two appends at once. Appends to distinct
  streams, through one handle or several, each took only the shared publication gate and their own
  stream lock, so a projector that reads, computes and upserts a shared row could lose an update.
  Every append and append group now takes each attached inline projector's owner/projector/tenant
  lock, ordered by name, after its stream locks and before its guard, and holds it to commit.
  Writes of one tenant therefore serialize from that point while an inline projector is attached.
- PostgreSQL: a blob read whose fetch and check are separated by a commit or a statement notes the
  verified-content generation before it fetches, so a same-handle deletion that settles in between
  no longer leaves the deleted bytes in the handle's memory. `eventlog-core` adds
  `VerifiedContent::generation`, `VerifiedContent::check_stored_since` and `ContentGeneration`;
  `check_stored` is unchanged.
- PostgreSQL and SQLite: inline rebuild no longer holds the tenant's whole decoded history in one
  vector. It validates every page first and then folds page by page, both in the same snapshot,
  so no callback still runs on corrupt history and the shadow-table publication stays atomic.
  `eventlog-core` adds `CapturedOrder`, the page-at-a-time form of `validate_captured_order`.
- PostgreSQL: an idempotency mismatch from `recorded_command` rolls back and keeps its pooled
  connection instead of retiring it as an unsettled lease.

### Security

- PostgreSQL: hosted `open` requires only `SELECT` on the migration metadata
  (`<prefix>_schema_version`, `<prefix>_projection_registry`, `<prefix>_settlement_floor`) and
  refuses an application role that can `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE` them. Erasure
  deletes a tenant from every registered projection, so a role that could edit the roster could
  make erasure skip a table; a role that could raise the settlement floor could let a feed cursor
  pass an append still in flight. Existing deployments that granted DML on all tables must revoke
  it on these three tables after `migrate`: `REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON
  <prefix>_schema_version, <prefix>_projection_registry, <prefix>_settlement_floor FROM
  <application role>`.
- PostgreSQL: a hosted pool holds every physical session it opens after `open` to the database,
  role and schema that admission saw, and to the hosted role rules (no DDL powers, a finite
  connection limit within the replica share). A caller-owned authority that reconnects as another
  principal is refused with `Invalid("pooled PostgreSQL session is not the admitted hosted
  principal")` and the session is retired unserved. Before, only the first session was checked.

### Performance

- File: `read_feed` and catch-up start at the requested position instead of walking the store
  from its first event. Paging 128 events in pages of 8 visits 143 events instead of 1,103, and an
  idle catch-up poll visits none.
- File: an idle `run_catch_up` poll whose cursor is already current commits nothing. Twenty idle
  polls used to add twenty journal frames (369 to 7,901 bytes); a projector without a cursor still
  records its first one.
- File: opening a store reads, chains and replays its journal once instead of twice, under the
  writer lock of that open; recovery, snapshot and object disposal and object hashing are unchanged.
- File: a commit that binds or unbinds no blob no longer lists and synchronizes the blob directory.
  Objects a crash leaves behind are disposed of by the next commit that changes bindings and by the
  complete opener.
- File: a blob batch finds digests it already binds through an index instead of scanning the batch,
  so 1,024 distinct digests no longer cost 523,776 comparisons. A repeated digest with different
  bytes still refuses the group.
- File: `projection_find` reads an in-memory index by tenant, projection, field, value and row
  key instead of every row of the projection, so a lookup of a value no row holds visits no row
  (it used to evaluate the field of every row: 512 for 512 rows). The journal format is unchanged.
- File: deciding which cached snapshots are live builds one set of generations instead of scanning
  every generation per snapshot, so opening a store with 64 live snapshots no longer makes 2,080
  comparisons.
- File: `projection_list` seeks to the first key past its cursor instead of walking the rows from
  the first key, so a page visits the rows it returns and at most one more (paging 512 rows by 32
  used to evaluate 4,864).
- File: an inline registration resumes the handle's verified view instead of opening and replaying
  the whole journal, and keeps that view for the next call (four registrations on a 128-frame store
  used to visit 518 extra frames).
- PostgreSQL: `read_stream`, `read_feed` and `projection_find` send their statement with typed
  parameters, one round trip instead of a prepare and an execute. `projection_find` checks the
  registered declaration inside its row statement and asks the registry only when no row came
  back; the feed reads the settlement floor once per statement; the stream page numbers its rows
  without a derived table. Measured against 0.9.4 (KPI timing A/B, p50) these three reads had
  become 1.33x to 1.86x slower through this release's fixes L34, L39 and L61; they are now
  1.04x (`projection_find`, `read_feed`) and 1.16x (`read_stream`, whose body-less lookahead row
  costs a window function). Results and refusals are unchanged.

### Documentation

- Repository: idempotency covers only a command that wrote events, now stated on
  `Aggregate::decide`, `Outcome` and `EventStore::recorded_command`. A command that decided
  nothing leaves no record under its key, so a retry with the same key is decided again against
  the current state and may write. Behaviour is unchanged; a repository test pins it.

## 0.9.4 — 2026-11-11

### Security

- SQLite: `rusqlite` moves from 0.40.2 to 0.41.0, which brings `libsqlite3-sys` 0.39.1 and
  bundled SQLite 3.54.1. This fixes RUSTSEC-2026-0459 (CVE-2026-119375): SQLite before 3.54.0
  can return uninitialized page-cache memory when it opens a database file together with a
  `-wal` file whose page size differs. The SQLite provider's API and on-disk format are
  unchanged.

## 0.9.3 — 2026-11-11

### Changed

- PostgreSQL: `read_many` answers a batch in one REPEATABLE READ snapshot, with one statement per
  1,000 stream reads and one per 1,000 blob reads, instead of one statement per read. Each result
  is what `read_stream` or `get_blob` returns for the same arguments. Deadlines follow the
  separate calls: each statement runs under the store-operation deadline and the statement timeout
  of one read times the reads it carries, so a batch whose reads each fit alone is not refused for
  its size.
- PostgreSQL: `append_group` takes its stream locks, reads its stream heads and inserts its events
  with one statement per 1,000 streams or events, instead of up to three statements per member;
  a group's retry reads its stored ranges the same way. A single `append` inserts its events in
  one statement per 1,000. Version checks, guards, claims, commands, inline projections and
  atomicity are unchanged, and so is the order positions are drawn in. Each statement that takes a
  group's stream locks may run for the rest of the group's deadline rather than one statement
  timeout, since it waits for up to 1,000 locks in turn; `lock_timeout` still bounds each wait.
  On PostgreSQL 17 on localhost, release build: 10,000 single-row reads through `read_many` fall
  from 5.0–7.3 s to 0.1–0.15 s; a group of 1,000 events from about 2 s to 0.07–0.2 s; a group of
  20,000 events on 4,000 streams, which missed the 10 s deadline, commits in 0.8–2.4 s.
- PostgreSQL: when more than one member of a group refuses, the refusal `append_group` returns
  can now come from a later member. Every member's events are inserted before any member is
  projected, so a later member's content that PostgreSQL itself refuses (for example SQLSTATE
  22P05, a JSON `\u0000`) can be returned instead of an earlier member's inline-projector
  refusal. Before, members were written and projected one at a time and the earliest refusal
  won. Conflicts, guard refusals and malformed members still refuse in member order, and a refused
  group still writes nothing.
- A group still takes one transaction-scoped advisory lock per distinct stream. With PostgreSQL's
  default `max_connections` (100) and `max_locks_per_transaction` (64) the shared lock table holds
  about 7,000 such locks, so a group over 20,000 distinct streams fails with SQLSTATE 53200 (out
  of shared memory) instead of reaching the deadline; a server that must commit such a group
  needs a larger `max_locks_per_transaction`.

## 0.9.2 — 2026-10-31

### Changed

- The persistence proof installs ess 0.75.0 (was 0.61.0), moving through 0.72.0 and 0.74.0, and
  `ess/capture-generated/` is regenerated with it: the `CaptureUpdateKind` variants take their
  declared names and the crate's serde ranges become compatible ranges.
- The documentation site moves to docs-system 0.12.0.
- No library crate changed: the release carries the pins and the version bump.

## 0.9.1 — 2026-10-16

### Fixed

- PostgreSQL: a connection retired from the pool (a lease dropped without settling, after a
  deadline or a cancelled call) now closes when its lease drops, on the dropping thread. Before,
  the close ran in a spawned task, so on a caller's current-thread runtime it waited for the
  caller's next call, and the abandoned transaction stayed open, idle in transaction and holding
  its locks, until the server's own timeout.
- PostgreSQL: a backend blocked inside a query when its lease is retired (for example an append
  waiting on a lock at its deadline) no longer keeps its advisory and row locks. The pool sends the
  server a cancel for it from a thread of its own, and every session sets
  `client_connection_check_interval` (1 s), so a backend whose client is gone aborts its query even
  where no cancel can be sent (a `PostgresConnectionAuthority` connection gets no cancel). A server
  that does not know the setting (PostgreSQL 13) or cannot honour it (no closed-socket detection)
  is still admitted, relying on the cancel alone. Pool capacity still returns only after the
  retirement and its cancel attempt are accounted.
- PostgreSQL: a retirement whose close task is dropped with the caller's runtime (a caller that
  builds a runtime per call) no longer leaves the lease counted as checked out, which had made
  every later `shutdown` miss its deadline.
- PostgreSQL: `append_guarded`, `append_group_guarded` and `append_group_with_blobs_guarded` return
  `EventLogError::Deadline { operation: "store operation" }` when the store-operation deadline fires
  before COMMIT was written, and `UnknownCommit` only once COMMIT was sent. Before, every deadline
  was `UnknownCommit`, even when nothing could have committed. The two outcomes are named
  `eventlog.pool.TimedOutWrite` in `ess/pool/`.

  Not a breaking API change: `EventLogError` has the same variants. It is a behaviour change for a
  caller that matches only `UnknownCommit` and treats every other error as final: such a timeout
  now arrives as `Deadline`, which means "did not commit; retry", and is safe to retry with the
  same command identity. A caller that already handles `Deadline` from the other bounded
  operations needs no change.

## 0.9.0 — 2026-10-14

### Added

- Tree store: an opt-in layout, `eventlog-tree/3`, whose event paths fit a Git checkout on Windows
  without `core.longpaths`. `TreeEventStore::open_in(root, TreeLayout::Hashed)` (and
  `open_in_with_inline`) creates a store whose stream directories are `streams/<kk>/<h>`, `h` the
  first 32 hex characters of the SHA-256 of the escaped `<type>/<id>`. An event path under
  `.engineering/state` for tenant `planning` is then 149 characters for every stream, against 198
  and growing with the stream id before; a test holds it under 260 after the 76-character
  checkout prefix of a Cargo Git dependency. `TreeEventStore::layout` reports a store's layout.
- Nothing changes for a store that does not opt in: `TreeEventStore::open` creates
  `eventlog-tree/2` as before, a store keeps the layout its `store.json` names whatever an open
  asks for, and every layout reads in this release. No migration moves a store between stream
  layouts. Eventlog 0.8.4 and earlier refuse to open an `eventlog-tree/3` store, so opt in only
  once every reader of the store has this release.
- File provider: an opt-in segmented layout, `eventlog-file/2`, so that a resumed handle's
  history check stops re-reading the whole store after each write. `FileEventStore::open_with_layout`
  with the new `#[non_exhaustive]` `FileLayout::Segmented { seal_at }` (or `FileLayout::segmented()`,
  256 KiB) creates a store whose active `events.jsonl` is sealed into `segments/` once it holds
  `seal_at` bytes; a resume skips each sealed segment whose trusted stamp still matches and compares
  the rest byte for byte, as `eventlog-file/1` does for its one file. The layout is chosen only at
  creation: `open` still creates `eventlog-file/1` byte for byte as before, an existing store keeps
  its layout, nothing is migrated, and this release reads both. An earlier release refuses an
  `eventlog-file/2` store by its format. A `seal_at` of 0 is refused. The manifest's `segments`
  record is modelled in `ess/file-layout/`, and a test holds it to the committed schema in
  `ess/file-layout-generated/`. Additive: a minor release.

### Changed

- SQLite durable capture continuity encodes its checkpoint with the Rust types
  `ess generate types` (ess 0.59.0) writes from `ess/capture/` to `ess/capture-generated/`
  (crate `eventlog-capture-types`, built with default features off so `serde_json` keeps its
  default number handling and every stored `request_hash` still matches). The journal entry keeps
  its hand-written encoding, which embeds each row's exact stored text. Bytes written by earlier
  versions restore as before.
- `eventlog-core`: `CaptureLimits` and `CaptureUsage` convert to and from the generated
  `EventlogCaptureCaptureLimits` and `EventlogCaptureCaptureUsage` (`From`, and `TryFrom`, which
  refuses a number that is not a `u64`). Additive.
- CI (`real-backends`) validates every specification under `ess/` and regenerates every
  `ess/*-generated/` output with the pinned ess release, refusing any difference
  (`cargo run --locked -p eventlog-postgres --example ess-drift`).

### Fixed

- File provider: a capture after a write through the same handle starts from the writer's verified
  view of its head instead of re-reading, re-chaining, re-encoding and re-folding the whole history;
  the committed prefix is still compared byte for byte, and any difference falls back to the strict
  re-read.
- File provider: a resumed handle no longer disposes of snapshots after folding another handle's
  frames. Frames of one epoch retire no generation; redaction and tenant erasure, the only
  operations that do, start a new epoch that goes to the complete opener, and `load_snapshot`
  refuses a cache of a retired generation wherever it sits.
- File provider: a writer lock holder releases `writer.lock` when it drops, so a subprocess forked
  by the host no longer keeps the lock alive and `inspect` no longer answers `SourceBusy` for it.
- Tree store: two handles creating one store at the same moment both open. Each stages
  `store.json` under a name of its own and publishes it by hard link, so the first one published
  wins and a later creator uses it; on a filesystem without hard links the publish falls back to
  the previous rename. The hashed layout's Windows path bound holds for a tenant of at most 36
  escaped bytes, as its documentation now states.
- Dependencies: `rustls` 0.23.46 (RUSTSEC-2026-0346 to 0349).

## 0.8.4 — 2026-10-13

### Fixed

- Tree store: opening a store while another handle commits a group no longer reports "tree store
  is corrupt: a group whose event is missing". The open lists each tenant's group files before it
  reads the events, and reads only the listed groups; a group committed after the list is not read
  and its events are torn, as an interrupted write's are. A listed group whose event is missing
  is still refused.

## 0.8.3 — 2026-10-08

### Added

- PostgreSQL: `PostgresEventStore::pool_churn` returns the new `#[non_exhaustive]`, serializable
  `PoolChurn`: the connections the handle's pool has opened, the ones it retired by cause (a lease
  dropped without settling, a client already closed, shutdown) and the connections that replaced a
  retirement. A connection counts as a replacement while earlier non-shutdown retirements
  outnumber earlier replacements; every other connection only fills capacity. `PoolStatus` is
  unchanged. The type is modelled as `eventlog.pool.PoolChurn` in `ess/pool/`, and a test holds
  its encoding to the committed schema in `ess/pool-generated/`. The new default feature
  `pool-churn` only tells the capacity harness that the accessor exists; the accessor is not gated.
- Capacity laboratory: each `capacity` worker reports its pool's churn, and `capacity-sweep` carries
  it per worker into each configuration receipt. The metrics collector records, for a sample over
  eight client connections, each counted backend's `pid`, `backend_start`, `state`,
  `state_change` and `xact_start`, and whether each pid is still counted in the next sample. The
  `<= 8` connection check is unchanged.

### Fixed

- Capacity laboratory: the comparative lane's `<= 8` connection budget counts only the workload's
  own backends. `capacity-sweep` starts every worker of both adapters with
  `application_name=eventlog-capacity-workload`, and the metrics collector reports
  `workload_connections` per sample and `workload_connections_max` per configuration beside the
  unchanged all-backend `connections` and `connections_max_excluding_observer`. The CI service's
  `pg_isready` health check opens short-lived client backends; one sampled beside the eight pool
  connections failed the lane with nine. A maximum of zero workload connections also fails the
  budget. Over-threshold attribution still covers every client backend, and no application name
  is written to the receipt. See `docs/design/comparative-capacity-connection-budget.md`.
- Capacity laboratory: each comparative configuration starts from an empty `pg_stat_statements`.
  The metrics collector resets the extension immediately before the configuration's opening
  statement snapshot, so an eviction caused by statements registered earlier on the same server,
  such as the production gate's, can no longer move `dealloc` inside a configuration and fail its
  envelope. A refused or timed-out reset leaves that snapshot `unavailable`, which fails the
  configuration. The deallocation and `stats_reset` equality checks are unchanged. See
  `docs/design/comparative-capacity-connection-budget.md`.

## 0.8.2 — 2026-10-08

### Performance

- File and PostgreSQL: a handle hashes each distinct blob content once, as SQLite already did,
  instead of on every read. `EventStore::get_blob` and every callback's `ProjectionStore::get_blob`
  still read the bytes from storage on each call and accept them without SHA-256 only when they
  equal, in full, content the handle verified or hashed while writing under the same hash; any
  other bytes are hashed and refused on a mismatch, so content changed after a verified read is
  refused on the next read through the same handle. A guard that reads a batch blob once per member
  of an M-member batch now pays one hash instead of M. The memory is the new provider-neutral
  `eventlog_core::VerifiedContent` (32 MiB per handle, cleared on blob deletion, tenant erasure
  and a failed blob-binding write), which SQLite now uses unchanged in behaviour. Nothing persisted
  changes.

### Fixed

- `ess/effects` validates on ess 0.56.0: the `EffectBoundaryCoverage` invariant that one of
  `records_terminal` and `records_incomplete` holds is written with structured `any:`, since the
  compact predicate form has no `or`. The condition is unchanged.

## 0.8.1 — 2026-10-08

### Performance

- File: a resumed handle whose journal changed in the last two seconds re-checks the committed
  prefix by comparing it byte for byte with the bytes it verified, instead of hashing it with
  SHA-256. Every refusal stands; the read stays and the hash goes. A handle now keeps its
  committed prefix in memory, once, shared between its writer and capture views: the committed
  size of `events.jsonl` after open, up to about twice that as its own appends grow the buffer.
  Nothing persisted and `eventlog-file/1` are unchanged. On the probe in
  https://github.com/beyond10x/eventlog/issues/42 (2,000 one-event appends, 4.8 MB journal),
  reading 40 streams inside the window fell from about 1.4 s to about 60 ms and the run's CPU time
  from about 21 s to 1 s. Per-append wall time is bound by the commit's synchronizations and no
  longer grows with the journal: from 200 to 2,000 events it rose about 1.4 times before and did
  not rise after.
- File: `stream_version`, `read_stream`, the receipt a retried command or group returns and the
  event `redact` rewrites are found through a per-stream index kept beside the events, so each
  reads only the stream asked about (a head lookup reads no event, a window the events it returns
  plus one) instead of every event in the store. Folding history no longer rescans the store for
  each event's order check, which made replay quadratic: 160 events took 12,720 reads, now 0, and
  a complete open of 5,191 events fell from 674 ms to 288 ms (median, one machine). Feed, catch-up
  and projection rebuild still walk the store in global order. `eventlog-file/1` is unchanged.
  https://github.com/beyond10x/eventlog/issues/42

## 0.8.0 — 2026-10-07

### Added

- Durable capture continuity on SQLite: a capture checkpoint a later process can continue from.
  `ConsistentTenantCapture` gains `durable_checkpoint`, `restore_checkpoint` and
  `checkpoint_usage`, default methods returning `None`, so every provider compiles unchanged and
  PostgreSQL, File and Tree keep complete capture. `DurableCaptureCheckpoint` holds the provider's
  bytes and prints only their length. `SqliteEventStore::enable_durable_continuity` installs, in
  one transaction, a `<prefix>_capture_continuity` row (store instance, epoch, random token), a
  `<prefix>_capture_journal` kept within 128 entries and 16 MiB, a partial index of redacted
  events, and provider triggers on the events, blobs, identity and registered projection tables;
  `create_projections` gives each table it creates the same triggers. Each acknowledged atomic
  group writes one journal entry in its own transaction, holding each row value as the exact text
  the row held; a group of a tenant with redacted history writes only its link in the chain. A
  restored checkpoint continues as `Unchanged` or `AppendDelta` only through an unbroken chain of
  such entries, and only when its stream identity is the stored one, the tenant has no redacted
  history, each requested projection passes the complete capture's admission and the result fits
  the request's caps; otherwise the capture is complete. Any other write, a dropped or altered
  trigger, a pruned entry or an older copy of the file gives `Complete`. A cap that a restored
  checkpoint's decoded usage crosses gives `Complete`, never a limit refusal. Standalone `append`,
  `put_blob` and `delete_blob` are not journaled, and redaction and tenant erasure delete every
  entry. The provider's own snapshot writes no longer end in-process continuity. Design:
  `docs/design/durable-capture-continuity.md`. **One-way for older readers:** Eventlog 0.7.0 and
  earlier refuse a store carrying the provider triggers; `disable_durable_continuity` drops them,
  both tables and the index, after which those versions open and attach the store as before.

### Fixed

- SQLite in-process capture deltas carry event bodies and projection row values as a complete
  capture reads them: the stored text, parsed by the capture's own parser, not the value handed
  to the write. A number whose text does not parse back to itself made base plus delta differ
  from a complete capture, and `resulting_usage` differ from its counts.
- SQLite's trigger checks at open, `open_existing`, inline attach, capture admission and strict
  inspection match a trigger's table without regard to case, as SQLite resolves it. A foreign
  trigger that spelled a captured table in another case was admitted.

## 0.7.0 — 2026-10-07

### Added

- Optional provider-owned capture checkpoints distinguish complete observations, unchanged state
  and acknowledged append deltas. SQLite tracks SQL-visible changes across connections and retains
  a bounded journal, with exact cumulative capture limits and complete fallback after uncertainty.
  Checkpoints are transient and cannot be transferred between provider instances or request scopes.
  Raw database-file writes bypassing SQLite are outside this optional warm-read guarantee.

### Changed

- Update existing empty-value assertions for the Rust 1.99 strict lint gate.

## 0.6.0 — 2026-09-27

### Added

- Tree stores keep long text once. A new layout, `eventlog-tree/2`, may keep a blob that is a JSON
  document (or JSON Lines) as a split blob: a manifest under `split-blobs/` whose string literals
  of 1,024 bytes or more are texts under `texts/`, each stored once per tenant under its SHA-256.
  A literal that spells bytes in hexadecimal keeps those bytes once, and JSON inside them is cut
  the same way. Readers serve exactly the blob's bytes under the same digest and refuse a manifest
  whose expansion has another length or SHA-256. New stores are created as `eventlog-tree/2`.
  Design: `docs/design/tree-text-blobs-v0.1.md`.
- `eventlog_tree::migrate(root, MigrationMode::DryRun | Apply)` moves an `eventlog-tree/1` store
  to `eventlog-tree/2`. It verifies the store first, converts one blob at a time with a read-back
  before the raw file goes, then reloads the store and checks every blob, group and event is served
  as before. It is idempotent and never runs on open. On a copy of a 9,682-blob planning store it
  cut history bytes from 180,651,308 to 77,406,062 and the largest file from 75,332,948 to
  5,153,913 bytes.

### Changed

- Every `eventlog-tree/1` store still opens, reads and writes exactly as before. It is refused by
  name if it holds split-blob or text files. `verify`'s V2 rule accepts the migration commit: a
  blob file may move between forms when the head serves the same bytes, and `store.json` may move
  from `eventlog-tree/1` to `eventlog-tree/2` with the same identity.
- Deleting a blob of an `eventlog-tree/2` store removes every text no other blob names.
- New and migrated stores are refused by earlier readers, which accept only
  `eventlog-tree/1`.

## 0.5.0 — 2026-09-26

### Added

- SQLite implements `AtomicEventStore::append_group_guarded_with_blobs`. A group and its blobs
  commit in the group's one `BEGIN IMMEDIATE`. Admission runs before any blob is bound. A
  refused guard, a member conflict or an existing digest bound to other bytes rolls back every
  binding with the events. Equal existing bindings and equal repeats within a batch are reused.
  The sorted digests each commit bound are recorded in an additive
  `<prefix>_group_batches` table. A retry that carries a batch runs admission again and must
  carry exactly those digests. A retry without a batch is the ordinary group retry. An owner
  provisioned before the table existed gets it inside its first such group, and erasing a
  tenant erases its rows. Measured on ext4 under `strace -f -c`: 20 groups of 7 blobs made
  169 fsyncs as `put_blob` × 7 plus a group, and 29 fsyncs through this method. Opening takes
  9 either way, so a group falls from 8 fsyncs to 1. Write time fell from 2.4–3.1 s to
  0.25–0.38 s. PostgreSQL still refuses the method.
- File and SQLite accept a guarded blob-bearing retry of a group committed through
  `AtomicBlobEventStore::append_group_with_blobs`, same key, group and bytes. The retry runs
  admission again and then deduplicates. Before this, the retry was refused as
  `IdempotencyMismatch`, including on a SQLite copy made by `eventlog_tree::copy`, and the only
  way forward was a new key that appended the group twice. The reverse direction still
  conflicts. The shared exercise `run_blob_group_retry_identity` checks both directions.
- `eventlog_tree::copy` linearizes a tree store into a SQLite store:
  - every committed group is appended in the tree's position order, with its original event
    ids, instants and receipt, so each stream is gapless and a retry of a copied command writes
    nothing;
  - tenant identities, blobs (bound and alone) and redactions are copied;
  - each event's tree `{version, digest, parents}` is kept in an origin map beside the event
    rows, not in `data`, so a redaction cannot erase it; `SqliteEventStore::origins` reads it;
  - a target that already holds events for one of the tree's tenants, gave one a different
    stream identity, binds one of its blob digests to other bytes, or holds queued restored
    identities is refused before anything is written.
- SQLite gains an additive `<prefix>_origins` table, `RestoredEvent.origin`,
  `SqliteEventStore::origins` and `SqliteEventStore::stored_stream_identity`, which reads an
  identity without minting one. Erasing a tenant erases its origins. An owner provisioned
  before the table existed reads an empty map, and gains the table inside the first append that
  carries an origin; `open_existing` still creates no table.

### Performance

- A SQLite database file hashes each distinct blob content at most once per handle, instead
  of on every read. Content the handle wrote is not hashed again at all. A handle remembers
  each verified `(integrity_sha256, bytes)` pair, up to 32 MiB. A later row is accepted without
  hashing only if its hash, length and edition match and its bytes are equal in full. Any
  changed row is validated in full, so tampering after a verified read is still refused on the
  same handle. The memory is cleared when the handle deletes a blob, erases a tenant or returns
  an error from a write that bound blobs (a guard or projector panic skips this). A deletion or erasure through another handle does not clear it; such
  entries are never returned but stay in memory until then.
  Measured with 50 reads and 5 captures of one blob: 56 hashes fall to 0 on the writing
  handle, and 55 fall to 1 on a reopened one. Reading 7,000 blobs of 4 KiB took 218–263 ms
  and now takes 114–116 ms.

### Fixed

- A SQLite append or group that rolls back now puts back every restored identity it took, so the
  next append on the handle no longer takes another event's id and instant.
- PostgreSQL `stream_identity` reads an existing identity instead of upserting it. The upsert
  rewrote the row and committed on every call, so every resumed feed page paid a commit flush.

## 0.4.0 — 2026-09-24

### Added

- Fork vocabulary for stores whose history is merged outside them:
  - `Expected::Merge(HeadSetDigest)` and `HeadSetDigest::of`;
  - `RecordedEvent.digest` and `RecordedEvent.parents`, omitted from the wire when empty, so
    existing file, SQLite and PostgreSQL bytes are unchanged;
  - `EventStore::capabilities` with `Capabilities::LINEAR` as the default;
  - `BranchableEventStore::heads`;
  - `validate_captured_branchable_order`, which requires causal order instead of gapless versions.
- `EventLogError::Forked` and `EventLogError::Unsupported`. File, SQLite and PostgreSQL refuse a
  merge expectation as `Unsupported`, and the shared conformance exercise now checks that a
  linear store refuses a merge and writes nothing.

- `eventlog-tree`, a file event store whose history merges under version control:
  - one immutable file per event and one per command, with the command's file as the commit
    point and no store-wide file, so branches that wrote different streams merge without a
    conflict;
  - branches that wrote one stream fork it, and only an `Expected::Merge` over its exact heads
    joins it again;
  - history is replayed on open, in causal order, into SQLite `:memory:`, which serves every
    read and projection;
  - event ids, instants, receipts, tenant identities, blobs and redactions survive reopening;
  - a torn write is never served, and `repair` removes it;
  - an edited file refuses the open;
  - it passes the shared storage, group, guarded blob-group, projection, inline, paging,
    rebuild, input-validation and snapshot exercises;
  - its capabilities declare claims unsupported and positions valid only while open.
- `SqliteEventStore::restore_events`, `restored_pending` and `restore_stream_identity` let a
  provider replay history it keeps elsewhere with the original identities and instants.
- The shared input-validation exercise skips claim lookups on a store whose capabilities
  declare claims unsupported.
- `EventStore::read_many` answers a batch of stream and blob reads. The default loops, so SQLite
  and PostgreSQL are unchanged. The file provider answers the whole batch in one transaction, so
  a caller loading a history object by object resumes the store once instead of once per object.
  A test counts it: 50 reads in one batch hash the prefix once, and 50 separate reads hash it
  50 times.

### Performance

- `eventlog-tree` keeps the state it replayed under `.cache/state/`, named by a digest of every
  history file's stamp, the registered projectors and the running program. An open of an
  unchanged tree loads it and opens no event file; any changed, added or removed file replays
  and verifies the whole tree, and no state is kept while a file is less than two seconds old.
  `.cache/` ignores itself in Git and `verify` never reads it. On a 9,939-file store, a planning
  `list` fell from 5.1 s to 1.2 s.
- `TreeEventStore::open_with_inline` registers inline projectors before the replay, which
  `open` followed by `attach_inline_existing` did twice.
- An in-memory SQLite store checks a stored blob's length and integrity metadata on read, and no
  longer hashes its bytes again: its rows change only through its own statements. A file
  database hashes them as before.
- A blob group's fingerprint and the blob's integrity column share one SHA-256 of its content.
- `SqliteEventStore::image` and `SqliteEventStore::from_image` write and reopen a whole store.

- A file-provider operation no longer re-reads and re-hashes the committed journal while the
  file's inode stamp is unchanged and was taken at least two seconds after the file last changed.
  Two hundred reads over an unchanged 20-frame history now hash 0 prefix bytes; before, each read
  re-hashed the whole prefix. `docs/design/file-provider.md` records what the stamp does and does
  not detect.
- A read that refuses after recording and writing nothing keeps the handle's verified view. The
  next operation resumes instead of reopening the store and hashing every object.

### Changed

- `Expected`, `Capabilities` and `EventLogError` are `#[non_exhaustive]`. A downstream exhaustive
  `match` needs one wildcard arm, once.

### Fixed

- SQLite validates an existing blob's stored length, integrity hash and edition
  before a fresh atomic append group reuses it. A corrupt binding refuses the
  complete transaction, including tentative blobs, events and receipts. Retries
  of an already committed group still return its retained result without reading
  or restoring subsequently corrupted or erased blob content.

### Documentation

- Add a public guide for File, SQLite and PostgreSQL, shorten the README while retaining its
  anchors, and validate and package an exact-commit project site without deployment credentials.

## 0.3.0 — 2026-09-22

### Added

- `ConsistentTenantCapture::capture_tenant_deferred` returns one tenant's complete observation
  while handing blob content out through a reader that reads and hashes it on the read that hands
  it out, rather than reading every bound object before it returns. Every refusal, cap and
  ordering is `capture_tenant`'s; what differs is when content is read, and therefore what an
  observation costs a caller that never looks at one binding. File answers it from the committed
  records, which already name each binding's object and the hash its content must have. The
  default reads every object, as before, so every provider serves the same contract. Bindings are
  still decided under the provider's consistency boundary; content read afterwards that is no
  longer the content the observation bound is refused, never substituted. Measured on a 7,815-blob
  157.0 MB authority, release build, this workstation: a full capture falls from 667/675/706/729 ms
  to 61/61/103 ms (`crates/eventlog-file/tests/measure_capture.rs`,
  `docs/design/file-provider.md`).

- File and SQLite expose existing-only open paths for callers that already hold provider
  authority. They refuse absent or incomplete stores without creating a root, lock, database or
  owner tables; explicit creation and File journal recovery retain their prior behavior.

- PostgreSQL accepts a caller-owned connection authority for custom and mutual-TLS transports
  while retaining the provider's bounded pool, role/schema admission, quarantine and shutdown.

- File, SQLite and PostgreSQL can attach an existing inline projector without writing durable
  state and atomically rebuild its complete tenant row sets through `InlineProjectionAdmin`.
  Rebuild uses the attached instance, complete committed history and active blobs, preserving
  unrelated tenants and publishing selected rows and cursor together.

- `AtomicBlobEventStore` publishes a group's blob bindings and its events in one native
  transaction across File, SQLite and PostgreSQL. `BlobAppendGroup` carries the group and its
  `BlobWrite` batch; the request fingerprint binds the actual bytes, an original receipt is
  resolved before any callback or binding runs, and a failed append rolls tentative content back.
  An append that cannot be classified after its content is published reports
  `EventLogError::UnknownCommit` rather than a success or a plain backend failure. Legacy
  fingerprint and storage formats are unchanged. Opt-in: a provider that does not implement the
  trait is unaffected.

- `AtomicEventStore::append_group_guarded_with_blobs` commits a group and the blobs it binds under
  the group's one durability barrier, with the guard running before a byte of the batch is
  written, so a refused guard publishes neither the group nor a blob. File implements it; the
  other providers take the trait's refusal. A batch of any size costs the frame the group already
  commits and no barrier besides, where `put_blob` costs one barrier per blob. The committed group
  record now carries the sorted digests of the batch it bound, so a retry under a committed
  idempotency key is compared against what that commit actually carried.

- `InspectHistory` reads one tenant's complete decoded history from a store nobody opened for
  writing, granting no writer or recovery authority and creating no root, lock or table.
  `InspectionLimits` states explicit source-byte, event-count and envelope-byte caps where zero is
  a real limit rather than a default; `HistoryInspection` is the decoded result and
  `InspectionError` its payload-free refusals. `FileHistoryInspector` and `SqliteHistoryInspector`
  implement it. A redacted or malformed envelope refuses rather than being returned. On Linux the
  SQLite inspector requires an open-file-description lock before it will admit a source, so a
  native writer cannot run underneath an observation in progress.

### Changed

- File verifies the complete committed history and every active blob once, when the store is
  opened, instead of on every operation. A later operation takes the process lock, compares
  `manifest.json` with the head it observed, and either reuses the frames and fold it already
  verified or reads, chains, folds and blob-verifies only the frames the file gained. A new epoch,
  a pending recovery intent, a shorter or unchained file or an unfolded head still falls back to
  the complete reread. Before reusing anything, the operation re-reads the committed bytes behind
  the head it observed and hashes them against what the opener read, so a committed frame damaged
  in place after open still refuses a read and still refuses the next append without altering the
  history, and a history whose observed prefix was rewritten under a genuine tail is still refused.
  A resumed operation also removes the staging names no durable intent selected. The divergence,
  blob-integrity and disposal refusals are unchanged.

- File reuses that verified history for reads as well. A capture on a handle that already observed
  the committed head re-reads and hashes the committed bytes behind it, folds only the frames the
  file gained since, and decodes, rechains and refolds nothing else; anything it cannot decide that
  way falls back to the strict reader, which refuses exactly as before. The reader changes nothing
  it observes: unlike a resumed transaction it removes no staging name and synchronizes no
  directory, and a reserved recovery entry — including one that cannot be followed — still refuses.
  Content is read and hashed on every capture, because every capture hands those bytes to its
  caller. Measured on a 3,273,126-byte committed history with 1,113 content objects totalling
  11,384,593 bytes, twenty captures of one tenant on one handle: a repeated capture costs 57 ms
  where it cost 140 ms, and returns the identical observation.

### Fixed

- Inline rebuild validates complete stored event history before applying projectors. SQL capture
  and rebuild also read nonpositive stored positions so malformed rows are refused rather than
  silently omitted; failed rebuilds preserve existing projection rows and cursors.

- SQLite selects the exact bundled `rusqlite` 0.40.2 line so composed consumers use one native
  SQLite dependency that satisfies their existing 3.51.3 admission floor. Eventlog schemas,
  envelopes and storage semantics are unchanged.

- SQLite and PostgreSQL verify stored blob length, integrity edition and SHA-256 at ordinary,
  transactional callback and binding readback boundaries. Populated predecessor tables require an
  explicit locked trust-of-observed-bytes migration, with typed acknowledged, unknown-commit and
  post-commit cleanup outcomes.

- Production proof binds every required case to its exact Cargo package and test target, refusing
  cross-provider masking, incomplete or ambiguous execution while retaining workspace and doc tests.

- SQLite and PostgreSQL now refuse different bytes under an existing tenant/digest blob binding
  with `EventLogError::Invalid`, retaining the original content. Identical retries still succeed;
  deletion permits rebinding, and concurrent differing writers have exactly one winner, matching
  the file provider's immutable binding contract.

- A refused SQLite inspection leaves every lock the store already held exactly as it found it — a
  prior writer lock, a prior reader lock, and the source file itself. An inspection that cannot be
  admitted changes nothing it looked at.

- Atomic blob publication writes the same blob-integrity columns the standalone `put_blob` port
  writes, on SQLite and PostgreSQL, and compares a readback through the same validation. Without
  them the binding failed the blobs table's own `integrity_v1 = 1 AND integrity_sha256 IS NOT NULL`
  check at publication time.

### Known

- `FileEventStore` carries two public methods named `append_group_with_blobs`: the inherent one
  taking `(&AppendGroup, &[(String, Vec<u8>)])`, and `AtomicBlobEventStore`'s taking
  `&BlobAppendGroup`. Rust resolves `store.append_group_with_blobs(..)` to the inherent one, so the
  trait method needs `AtomicBlobEventStore::append_group_with_blobs(&store, &request)` to be
  reached. Both arrived in this release, from two lines of work that met at the merge; the two
  capabilities are real and neither is deprecated. Naming is to be settled before either is relied
  on by name.

## 0.2.1 — 2026-09-10

### Changed

- Pin the shared Gates reusable workflow to `2c3c6acd0a6795768446e925a6137e17357f267a`
  (Gates 0.1.2), which publishes the release the workflow on `main` downloads. No check, policy
  field or receipt format changed; the version increment alone invalidates receipts retained
  under 0.1.1.

### Fixed

- Shared Gates 0.1.1 rejects non-automation commit authors before scanning or reusing signed
  evidence, including intermediate commits and merged side branches.

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
