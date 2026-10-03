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

## Retained capture continuity

Status: accepted contract for the bounded warm-capture work. The existing complete capture remains
the baseline contract; implementation and conformance evidence are tracked separately.

An owner may retain a fully verified complete capture and ask its provider what changed. A provider
can answer cheaply only when it proves continuity from that exact observation. Initial open and
reopen require a complete capture and consumer verification. The SQLite warm guarantee covers SQL
changes through the provider and through other connections, including other processes. Direct
modification of database or WAL bytes outside SQLite while a handle remains open is outside this
warm guarantee. Complete capture retains its existing validation behavior.

The additive object-safe method and transient values are:

```rust
fn capture_tenant_since<'a>(
    &'a self,
    tenant: &'a TenantId,
    projections: &'a [ProjectionSpec],
    limits: CaptureLimits,
    previous: Option<&'a CaptureCheckpoint>,
) -> BoxFuture<'a, Result<TenantCaptureUpdate, CaptureError>>;

pub enum TenantCaptureUpdate {
    Complete {
        capture: TenantCapture,
        checkpoint: Option<CaptureCheckpoint>,
    },
    Unchanged {
        checkpoint: CaptureCheckpoint,
    },
    AppendDelta {
        checkpoint: CaptureCheckpoint,
        delta: TenantCaptureDelta,
    },
}

pub struct TenantCaptureDelta {
    pub tenant: TenantId,
    pub stream_identity: String,
    pub events: Vec<RecordedEvent>,
    pub blobs: Vec<CapturedBlob>,
    pub projections: Vec<CapturedProjectionDelta>,
    pub resulting_usage: CaptureUsage,
}

pub struct CapturedProjectionDelta {
    pub specification: ProjectionSpec,
    pub rows: Vec<CapturedRowChange>,
}

pub struct CapturedRowChange {
    pub key: String,
    pub before: Option<Value>,
    pub after: Option<Value>,
}

pub struct CaptureUsage {
    pub events: u64,
    pub blobs: u64,
    pub projection_rows: u64,
    pub payload_bytes: u64,
}
```

The default method ignores `previous`, calls `capture_tenant` and returns
`Complete { capture, checkpoint: None }`. Existing providers and callers continue to work.
`None` is no continuity capability; it never permits an unchanged answer. The existing eager and
deferred capture methods keep their contracts. This method does not defer blob validation.

`CaptureCheckpoint` is an immutable, cloneable, nonserializable type-erased capability. Its public
`new<T: Any + Send + Sync>(value: T)` constructor and `downcast_ref<T: Any>() -> Option<&T>`
accessor permit independent provider implementations. Its field remains private and `Debug`
reveals no stamp. SQLite accepts only its private checkpoint payload type carrying an `Arc` issuer
identity retained by the exact provider connection. A caller can wrap arbitrary data but cannot
construct that private payload or its issuer. Cloning an issued checkpoint preserves authority;
copying numeric counters, a path, database identity or scope does not. Old checkpoints never mutate
when the journal advances. They may become too old for the journal and then require a new complete
capture. Reopening the same file creates a different issuer.

The private payload binds the exact tenant and stream identity, the ordered requested projection
specifications including indexed-field order, all four exact limits, the SQLite observation stamp,
its complete usage totals and a journal position. Projection declarations have no invented version.
A request with another scope, another limit set or an unrecognized checkpoint receives a complete
capture under the new request, with all its normal errors. The diagnostic ESS types under
`ess/capture/` describe update alternatives, accounting and scope facts; they are not a wire
representation from which a checkpoint can be forged.

`Unchanged` means every captured event, bound blob and requested projection row remains the same
at the returned observation. It is not merely unchanged stream head, count or timestamp.
`AppendDelta` means the provider proves the old captured prefix was unchanged and supplies every
addition and projection mutation required to obtain the new complete capture. Its events are the
complete suffix in ascending tenant `global_seq`, with complete acknowledged append groups. Its
blobs are every newly bound blob, including orphan bindings; a blob already bound in the base is
not returned again. Projection results preserve request order. Rows and blob digests are in
bytewise order. Each requested projection appears even when its change list is empty. Each changed
row key appears once: `before` is its value in the base and `after` its final value. Absence is
`None`; present JSON null is `Some(Value::Null)`. A key changed and restored may be retained with
equal before/after values. Blob deletion/replacement and event rewrites require complete capture,
not a delta which silently omits them.

A consumer accepts a checkpoint only after validating the complete value or delta accompanying
it. It retains the exact base until a delta is validated and installed; the provider does not
validate the consumer's domain semantics. Failed consumer verification must not advance its
checkpoint. Concurrent consumer refreshes compare their own generation before installing a new
model/checkpoint pair. The capture makes no freshness promise after its observation boundary.

### SQLite consistency and journal

All continuity state is in memory; no column, trigger, event, format or stored identity is added.
The existing connection mutex protects the connection and orders access to a private journal. If
a separate mutex holds journal metadata, every access takes the connection lock first. Issuer
state cannot be shared across independently opened connections.

After acquiring the connection mutex, begin `BEGIN IMMEDIATE` before reading a stamp. Inside that
transaction read `PRAGMA main.data_version`, the connection's 64-bit total-change counter and
both `PRAGMA main.schema_version` and `PRAGMA temp.schema_version`. The first detects other
connections' SQL commits; the second detects
this connection's DML; schema observation covers DDL that does not increment row changes. This
comparison is valid only on the same connection. SQLite documents these boundaries in
[PRAGMA data_version](https://www.sqlite.org/pragma.html#pragma_data_version) and
[total_changes64](https://www.sqlite.org/c3ref/total_changes.html).

First validate the request. Then, inside the native transaction, either return unchanged, assemble
an uninterrupted retained journal suffix, or run the existing full `observe` path. Every full
capture retains existing identity, redaction, physical projection shape, event, blob and limit
validation. An empty/missing checkpoint always chooses full observation. No callback runs while
capturing; no connection handle escapes. Failed commit/rollback/read returns no update token.

Every establishment of continuity also checks the main and temporary schema catalogs for triggers
and foreign keys. Their implicit side effects could mutate previously captured rows outside the
provider's journal instrumentation. If either schema contains any trigger or foreign-key reference,
complete capture still works but returns `checkpoint: None`. This deliberately conservative
eligibility rule includes unrelated tables. Later main or temporary schema changes invalidate
existing checkpoints. No trigger or schema change is introduced by this check.

Only acknowledged `append_group_guarded`, `append_group_guarded_with_blobs` and
`append_group_with_blobs_guarded` transactions may
extend continuity in the initial implementation. Record the starting stamp after `BEGIN IMMEDIATE`
and before any blob insert, admission callback or projector write. It must match the known journal
end exactly. Gather complete returned events, actually new blob bindings, and every projection
mutation through the provider-controlled projection write interface. Read each touched key's
initial value before its first mutation and retain its final value after all callbacks complete.
The journal records generic storage values, never consumer batch or record concepts.

A fresh acknowledged group has one journal entry. Publish it only after successful `COMMIT`, while
still holding the connection mutex. The external-write stamp retained for that entry is the one
observed while the transaction excluded external writers; do not adopt a post-commit data_version,
which could hide an external commit arriving between COMMIT and journal publication. Total changes
at the completed local transaction boundary account for its own writes. If schema changed, or any
local write was not accounted for, discard continuity. A deduplicated group with no physical
mutation leaves the boundary unchanged and creates no fictitious appended event.

External SQL writes, local unjournaled writes, redaction, erasure/reprovisioning, projection
administration, snapshots, standalone blob mutation, failed transaction, uncertain commit, worker
panic/poison, sequence exhaustion and journal eviction force the next relevant request through
complete observation. It is safe to invalidate for unrelated tenant changes. A confirmed rollback
may therefore cause unnecessary work but never an unchanged claim based on uncertain state.
Direct legacy `append` and administrative paths do not acquire accidental delta eligibility.

The initial journal retains at most 128 committed groups and 16 MiB of accounted retained content
(event data and envelope strings, blob bytes, row keys and before/after JSON bytes). These are
internal retention bounds, not caller capture limits and not a process peak-memory guarantee. An
entry exceeding either bound is not retained; older entries are evicted whole. Any request whose
chain is missing takes the full path. Arithmetic overflow invalidates retention. Every fresh
complete capture establishes an independent base; old checkpoints can still use an uninterrupted
retained suffix and otherwise fall back. Issuer/epoch rollover invalidates old checkpoints.

### Cumulative limits and integrity

`resulting_usage` counts the whole observation after applying the delta, using the original exact
capture accounting. Add every suffix event and newly bound blob, and subtract/add compact JSON
sizes for projection before/after values. Insert/delete changes row counts; update does not.
Checked arithmetic refuses overflow. The resulting totals must fit all four bound request limits;
checking only delta size is invalid. Retained checkpoint accounting is never supplied or changed by
a caller. Changed limits force full observation rather than silently inheriting old admission.

The journal's old row values and resulting counters let consumers update retained models without
rescanning the old capture. Consumers still validate every new event, blob and row against their
own contract and refuse a row delta whose before-value differs from their base. Complete provider
continuity cannot replace that validation. An external SQL mutation breaks continuity even if it
preserves event count/head, uses a valid JSON value, or changes blob content without changing its
stored length. A full capture then applies existing provider validation and the consumer verifies
its record/materialization agreement.

This extension does not add an append precondition asserting that the entire previous capture is
still current when a later append starts. Existing optimistic guards remain unchanged. Mutation
between a successful capture and a later append can still lead to an acknowledged append followed
by failed verification; a broken journal chain must force complete post-append verification so that
this cannot become silent cached success.

### Required verification

The Rust capture exercise covers default full fallback, exact update accounting and reconstruction
of a complete value by applying a delta. SQLite regressions cover:

- initial open/reopen full capture; unchanged performs no event/blob/projection scan;
- one and several complete guarded groups; duplicate group retry; orphan and repeated blobs;
- projection insertion/update/deletion, present null, repeated keys and returned-to-base values;
- all four cumulative limits, zero limits, changed limits and checked overflow;
- foreign provider token, tenant/identity/projection mismatch, stale journal and retention bounds;
- group refusal/rollback/uncertain commit and unjournaled local write fallback;
- external connection event/blob/projection/identity/schema mutation, including unchanged heads;
- writer interleaving before BEGIN, during the held transaction and after COMMIT before publication;
- no incomplete group or partially published checkpoint escapes cancellation or worker failure.

Run compile-valid controls that bypass external stamp comparison, omit a row mutation, omit a blob
binding and account only delta size. Each dedicated regression must fail before restoration.
Existing complete capture and atomic group suites remain unchanged. Record exact command exit
statuses and runner counts. Provider improvement alone does not establish any consumer end-to-end
latency claim; the consumer's actual workload and flat-cost threshold are separate acceptance.
