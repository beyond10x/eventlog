# Changelog

All notable changes to this component are recorded here. Versions are component-scoped and released
under bare-version tags such as `0.1.0`.

## [Unreleased]

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
