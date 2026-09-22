# Consistent tenant capture

Status: design accepted after review-result:consistent-tenant-capture-design-pass-2 under ESS
evolution revision1. Implementation remains dependent on accepted SQL integrity and provider proof.
This is a new opt-in conformance contract. It does not change existing feed or redaction APIs.

## Purpose and boundary

A consumer that must compare complete history, bound content and materialized rows cannot prove
one observation by combining separately paginated reads. The provider supplies one owned capture
under one native consistency boundary. The consumer still interprets records and checks that its
materializations agree with history. No product record, runtime type or policy enters Eventlog.

The API is a separate object-safe capability, with no default implementation or pagination fallback:

```rust
pub trait ConsistentTenantCapture: Send + Sync + 'static {
    fn capture_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
        projections: &'a [ProjectionSpec],
        limits: CaptureLimits,
    ) -> BoxFuture<'a, Result<TenantCapture, CaptureError>>;
}
```

File, SQLite and PostgreSQL implement it natively. A consumer requires this trait explicitly;
absence is a capability-selection refusal, not a fake empty result. No callback runs under locks,
and no transaction/connection handle escapes. On any refusal the caller receives no partial value.

The transient Rust values are:

- `CaptureLimits { max_events: u64, max_blobs: u64, max_projection_rows: u64,
  max_payload_bytes: u64 }`, supplied explicitly by the caller, with no default or clamping.
- `TenantCapture { tenant: TenantId, stream_identity: String, events: Vec<RecordedEvent>,
  blobs: Vec<CapturedBlob>, projections: Vec<CapturedProjection> }`.
- `CapturedBlob { digest: String, bytes: Vec<u8> }`.
- `CapturedProjection { specification: ProjectionSpec, rows: Vec<(String, Value)> }`.

Every event for the tenant is returned once in ascending `global_seq`; sequence gaps across
tenants or rolled-back allocations are valid. This is canonical feed position order, not a claim
about chronological completion of concurrent transactions. Every currently bound blob is returned
once in bytewise digest order, including orphans. Every row of each requested projection is
returned once in bytewise key order. Projection results preserve the request's order. Duplicate
projection names and duplicate indexed-field declarations are invalid requests, never merged.
An empty projection request is valid and makes no assertion about unrequested tables.

Snapshot caches, admission counters, command/group receipt tables and catch-up cursors are not
part of this read capability. They are not inferred from an absent result. A consumer needing
receipt equivalence must record/cross-check its own complete command records in the captured
history/materializations; this capability alone does not export Eventlog's whole physical store.

These values have no Serde wire contract, persisted identity or new format version. Minimal typed
coordinate/limit values live in `ess/capture/`. The existing envelope and projection vocabulary
remain the Rust-owned payload types; the model does not invent an entity lifecycle or JSON bag.

## Identity and loss of history

Capture reads the existing tenant stream identity. It never calls `stream_identity`, which lazily
creates one in all providers. A never-provisioned or forgotten tenant returns
`TenantIdentityMissing`; explicitly provisioning an empty tenant gives a valid capture with its
identity and empty content. Re-provisioning after erasure yields a different identity. A stored
identity is valid exactly when it decodes as a nonempty UTF-8 string. Preserve its exact bytes:
do not trim, normalize, require UUID syntax or reject legacy nonempty values. An empty or
undecodable stored identity is corruption, not permission to mint a replacement.

Within a validated provider observation, check identity before the redacted-history condition.
An absent identity returns `TenantIdentityMissing` even if redacted events exist; an invalid
present identity returns identity corruption. Only a valid identity proceeds to the history check.
Ordinary append does not provision identity, so append followed by redaction can reach the absent
identity case. Provisioning its identity changes the refusal to `RedactedHistory`, never success.
Request validation, required store recovery/integrity and operational failures can prevent reaching
this ordering; no precedence over such failures or already proven exceeded limits is promised.

For a valid identified tenant, any event with `redacted_at` present makes the entire tenant capture return `RedactedHistory`,
including requests with no projections. This refusal occurs before exposing any result or bound
blob bytes. Redaction leaves bound blobs and SQL projection rows in the existing providers;
neither the SQL integrity unit nor a checksum proves those rows safe or complete after redaction.
Rebuilding projections does not clear this capture refusal. There is no new dirty-generation
schema or invented full-history proof. Explicit future recovery/import from an acknowledged
boundary is a consumer design, not reconstruction by this reader. Existing ordinary APIs retain
their own redaction behavior.

A deleted referenced blob is absent from the live binding set. Eventlog does not know which
opaque reference needs it. The consumer must refuse loss of required content; it may not invent
records or silently downgrade completeness. Full tenant erasure removes identity and all bindings.

## Exact limits and validation

Each count cap applies to the complete returned tenant set; projection-row count is the sum
across all requested projections. Zero is a real cap. The payload-byte count is exactly the sum
of blob byte lengths, compact `serde_json` UTF-8 encodings of every `RecordedEvent.data`, and
compact encodings of every projection row body. It excludes coordinates, container overhead,
JSON event-envelope fields and projection declarations. Object key order does not change that
length. Checked arithmetic rejects overflow; it never saturates into an admitted value.

These are result-content bounds, not a process peak-memory guarantee. File already replays the
complete journal, and a driver may materialize one row before its decoded size is known. SQL
preflights counts and stored blob lengths inside the snapshot, then decodes incrementally and
checks the exact accumulating payload size before retaining each item. It must not load every
blob through an unbounded `query` and only then test the cap. A malformed length is corruption.
Implementations may refuse a proven exceeded cap early; when several caps are exceeded, no
cross-provider precedence is promised. Every reported limit must actually have been exceeded.
The same rules apply on repeated captures; there is no hidden remembered quota or partial cursor.

Validate constructor-level event/stream/envelope invariants, fallible stored numeric conversions,
tenant equality and event-position/stream-version consistency. Do not silently filter malformed
rows. Each blob passes the existing complete-content validator: File object hash/file type, or
the accepted SQL count/version/lowercase SHA256 validator. Digest identity remains opaque.

Validate each requested projection's syntax, exact registry entry/indexed-field order and actual
physical table/column/index shape without DDL. File also refuses an existing dirty-view marker.
The only current producer of that marker is redaction, which also leaves redacted history.
`RedactedHistory` therefore wins before projection admission, including after rebuild; `Dirty`
remains a declared refusal reason without a currently reachable public producer. Do not create a
new dirty-state writer or change redaction precedence merely to exercise that reason.
SQL must read the actual schema; a name in the registry alone is not shape admission. Preserve
the existing supported physical shape and role/profile requirements, including refusal of
foreign rules or authority, rather than broadening them for capture. Read-only validation must
not create an expected temporary table. Stored projection bodies must decode as their existing
JSON value type. Projection keys preserve every UTF-8 string accepted by existing writers,
including empty strings, whitespace, Unicode and embedded NUL where the provider admits it.
There is no new key grammar, normalization or constructor restriction; return exact decoded bytes
in bytewise order. Undecodable stored keys or bodies are projection corruption, not omission.
No projector executes and no catch-up/rebuild is triggered.

Valid materialized rows may lag catch-up history. Capture promises exact rows at its snapshot,
not that every registered projector is current. No table-to-projector mapping or currentness bit
exists to establish that stronger fact. The consumer checks its own complete-record invariants.

`CaptureError` has these machine-readable variants:

- `InvalidRequest { reason: CaptureRequestRefusal }`, with invalid tenant, invalid projection
  specification or duplicate projection/field as closed reason variants.
- `TenantIdentityMissing`, `RedactedHistory`, and `RecoveryRequired`.
- `ProjectionUnavailable { projection: String, reason: ProjectionCaptureRefusal }`, whose
  reasons are undeclared, declaration mismatch, physical shape mismatch and dirty.
- `LimitExceeded { resource: CaptureResource, limit: u64 }`, with events, blobs, projection rows
  and payload bytes as the four resource variants.
- `Corrupt { material: CaptureMaterial }`, distinguishing journal, identity, event, blob and
  projection. This contains no stored payload or raw invalid field value.
- `Store(EventLogError)` for existing operational errors, preserving Closed, Overloaded and
  Deadline. Read-only capture never reports an acknowledged write or unknown commit.

Provider decoding/integrity failures use the fixed corruption boundary above; do not echo SQL
values or stored JSON through diagnostic messages. Capability absence belongs to consumer
selection because a provider implementing the trait cannot lack its own operation.

## File consistency and a truly non-mutating entry

`FileEventStore` uses its runtime mutex followed by the same existing interprocess `writer.lock`
as all writers, holding both through the complete observation. Use a dedicated read path, not
`transaction` or `Journal::open`: those can initialize a store, recover append/privacy intent,
rewrite files and remove cached/orphan content before invoking a nominal read closure.

Add `FileTenantCapture::open(path)` as a read-only handle implementing only
`ConsistentTenantCapture`. Opening this handle and capturing both use the strict path, so an
inspector need not first call the mutating `FileEventStore::open`. It uses existing files only;
a missing root/lock/manifest is a storage refusal and never initialization. The same implementation
serves captures on an already-open `FileEventStore` without changing its ordinary operations.

The strict path opens the existing lock without create/truncate, takes its exclusive process
lock, then checks files and reads the committed manifest/journal. If `append.json` or
`privacy.json` exists, return `RecoveryRequired` and let a separately authorized ordinary opener
perform recovery. Do not interpret a pending privacy intent as permission to return old bytes.
Malformed/mixed pending intents need not be repaired or diagnosed by capture. Reuse the exact
frame chain, format, store identity, epoch, sequence, length and hash checks; surplus/truncated
journal bytes are corruption. Refuse non-regular required files and existing path violations.
Unselected staging files, stale snapshots and unbound blob objects remain untouched and excluded.

Replay accepted state without cleanup. Validate live selected-tenant blobs without invoking
`clean_blobs` or `clear_snapshots`. Preserve the per-handle observed-manifest extension guard on
subsequent captures and update only the in-memory observation after successful validation.
Missing-identity or redacted-history refusals may be returned from validated state before that
guard; a successful value must pass it. Another process's privacy rewrite can invalidate a handle
even for a surviving tenant, as with the existing provider; reopen explicitly to observe the new
epoch. No refusal permits the reader to reset its observation and retry silently.
Existing file format and canonical bytes stay unchanged. “Non-mutating” means no application
creation, modification, deletion, recovery, truncation or sync of stored files/directories;
operating-system access-time bookkeeping and the existing advisory lock are not durable facts.

## SQLite consistency

Hold the connection mutex and enter `BEGIN IMMEDIATE` before any stored identity/history read.
It waits for an earlier writer and excludes later writers across separately opened handles,
including autocommit identity/blob operations. Perform registry/schema checks, counts and all
reads in this transaction. End it and return only a complete value. There is no DML or DDL in the
capture transaction. Existing busy handling and blocking-worker cancellation behavior remain:
a dropped future may leave its worker finishing, but no result or held connection escapes.

Rebuild shadow population, row replacement and cursor replacement already share the writer
transaction. Capture therefore sees a complete old or new materialization. A deferred reader
that fixes its snapshot before an earlier erasure commits does not satisfy this contract.

## PostgreSQL consistency and cancellation

Acquire a pool lease and quarantine it before attempting the existing publication coordinate as
an exclusive **session** advisory lock. Use exactly the same owner database/schema/prefix-derived
coordinate as current transaction-level publication locks. Acquire it before starting a
`REPEATABLE READ, READ ONLY` transaction; then establish the fresh snapshot and read everything.
Starting repeatable-read first and blocking on a transaction-level lock is incorrect: the fixed
snapshot can predate an erasure that completes during that lock wait.

All compliant event publishers take the publication lock before allocating sequence positions
or other owner locks and retain it until commit/rollback. Once capture holds it exclusively,
prior publishers have settled and later ones cannot allocate. Do not paginate or apply the feed
watermark to this complete snapshot: unrelated database `xmin` can otherwise hide committed
tenant rows. This does not change the ordinary feed's watermark or contiguity rules. Identity
and blob writes that do not take the gate are still observed atomically by the fresh MVCC snapshot.

Commit or roll back the read-only transaction, explicitly unlock and verify successful release,
then and only then mark the lease settled/reusable. If acquire/read/end/unlock fails, leave the
lease quarantined; existing driver retirement releases locks before capacity is returned.
Cancellation while awaiting the lock, holding it, or reading cannot recycle a locked session.
The existing transaction deadline bounds lock acquisition plus the whole capture and cleanup;
a timeout preserves the typed Deadline result and retires the lease. There is no cleanup success
that converts an earlier failure to a capture. A cleanup failure also prevents returning a value.

Registration/admission remains frozen before traffic. Direct SQL writers and mixed old binaries
outside the publication protocol require the existing fenced cutover; no schema checksum can
make them participants. The API does not revoke previously returned owned values after a later
erasure; its guarantee is the single observation ordered by the provider's boundary.

## Acceptance

Shared applicable conformance must exercise provisioned-empty/missing/re-provisioned identity,
append-without-identity followed by redaction (missing identity wins), provision after that history
(redacted history wins), empty/undecodable identity refusal and unchanged legacy nonempty identity.
Round-trip empty, whitespace and Unicode projection keys; provider-specific cases also preserve
every additional admitted key, including embedded NUL where supported, without normalizing bytes.
Exercise
all four exact caps including zero and payload aggregation, tenant isolation, order, orphan blob
inclusion, missing references as absence, duplicate requests and every reachable projection refusal
kind. Pin the redacted-history precedence that makes `Dirty` unreachable with current producers.
It must show an intentionally lagging projection is returned faithfully without claiming currentness.

Provider cases must prove File interprocess serialization and unchanged file entries/bytes on
open/capture/refusal with pending append/privacy intent; SQLite separately opened handles ordered
against append, rebuild and erasure; PostgreSQL reversed XID/sequence publishers plus unrelated
`xmin`, erasure before snapshot, and deadline/cancellation at acquire/hold/read with eventual
driver retirement, recovered pool occupancy and a subsequent writer proceeding.

Across providers, pause rebuild before replacement and forbid mixed rows; redact an event copied
into a projection and require `RedactedHistory` with no value even after rebuild and with no
requested projection; verify corruption/refusal for stored event coordinates, blobs and projection
shape/data. Tests must reach actual native APIs and provider locks, not a simulated capture loop.
Each new regression has a causal mutation or pre-fix observation, retained and restored before gates.

Extend the provider-qualified production roster with all decisive cases. Pass affected suites,
fmt/strict workspace Clippy and `bash scripts/gate.sh --production-proof`, including the real
PostgreSQL17.6/TLS lanes and zero selected-zero/ignored/skipped required cases. Independent design
and code examinations remain required; a model validation or read-only review is not code proof.

## Dependencies and exclusions

Implementation composes with the accepted SQL blob-integrity validator. The corrected SQL
submission currently awaits its second independent examination; using it as a provisional design
base does not accept it. Preserve both planning lineages and compose through governed replay.
No SQL DDL, file format change, Eventlog receipt export, owner schema, new runtime, migration
cutover, source publication or release is included. Provider crash/group qualification, the
consumer adapter and explicit synchronous bridge, and the one AEP migration owner remain required.
