# Strict read-only history inspection

This additive capability serves O2 and O6: inspect the durable decision record
without altering the evidence. The RFC 0020 storage schema and the existing
provider encodings remain unchanged. The strict-inspection cases and local
production gate are recorded in
[integration evidence](../../.engineering/reviews/strict-inspection-integration.md).
Required remote comparative/restart proof and source publication remain separate.

## API and result

Core defines `InspectHistory`, `InspectionLimits`, `HistoryInspection` and
`InspectionError`. Separate `FileHistoryInspector` and `SqliteHistoryInspector`
types implement the capability. They expose neither `EventStore`, a writer, a
raw connection nor a caller callback. Use existing `TenantId`, `RecordedEvent`
and `BoxFuture` types. The result is transient, not another durable store.

An inspection receives a tenant and explicit limits. It returns that tenant,
the exact optional stored tenant identity, and all its `RecordedEvent` values
in ascending global sequence. Every original envelope field survives. Gaps in
global sequence are legal; per-stream versions retain the provider's ordering.
An absent identity is not minted, and an empty history without an identity says
only that no tenant evidence was recorded. Legacy event history without an
identity directory entry is supported.

Returned envelopes must satisfy the kit's existing field, opaque-identity,
causation-depth and object-body admission rules; event identities must be unique
within the inspected history. File inspection refuses unknown event-envelope
keys before the ordinary state fold can discard them. Application data remains
an arbitrary JSON object, and schema version zero remains supported. These
inspection checks neither alter the stored bytes nor relax ordinary append rules.

`InspectionLimits` contains `source_bytes`, `events` and `envelope_bytes`, each
a nonnegative finite integer. Zero is a real cap. Source admission bounds input
before whole-file allocation; SQL checks lengths before owned row accumulation.
The envelope cap counts UTF-8 bytes of the complete serialized `RecordedEvent`
values in the result, using checked arithmetic. These are admission and result
bounds, not an exact bound on allocator overhead. Limits never return partial
successful history. Implementation must make allocation boundaries explicit.

`InspectionError` has named variants `MissingSource`, `UnsupportedSource`,
`RecoveryRequired`, `CorruptSource`, `RedactedHistory`, `SourceBusy`,
`SourceChanged` and `LimitExceeded`. Errors can carry bounded diagnostic detail;
they never echo retained event data. Preserve original errors where useful as
causes without exposing writer authority. Do not add variants to the existing
`EventLogError` just for this capability.

Admission order is: existing physical source and supported platform/format;
nonblocking native lock or read snapshot and recovery prerequisites; source
limits; structural integrity; matching tenant history; redaction and result
limits. A discovered refusal terminates inspection. No guarantee that every
defect of a multiply invalid source is enumerated is implied. Missing source
cannot create files, and busy acquisition has a finite refusal.

## Provider boundary

File uses the existing manifest, frame decoder and state fold through a strict
non-provisioning entry point. Require the existing root, lock, manifest and log;
hold the native provider writer lock through the entire observation. Reject
pending append or privacy intent entries, including dangling symlinks, and
unproven suffixes. Validate the committed length and digest. Do not recover,
initialize, truncate, collect blobs or remove staging entries. Prefer a read-only
lock descriptor where the supported native primitive permits it; regardless of
descriptor mode, no file bytes or entries may change.

SQLite support is initially Linux-only. Before reading the header or checking
sidecars, acquire a nonblocking Linux open-file-description shared lock covering
the database through the safe `nix` fs API. Hold it through the deferred read
transaction and connection drop. SQLite's native POSIX write locks must conflict
with this guard even in the same process; ordinary shared read locks remain
compatible. This freezes native writers across rollback-mode admission and
prevents a check-then-open switch to WAL. A normal flock is insufficient.
Other platforms receive `UnsupportedSource`. The cases
`inspection_ofd_blocks_native_writers_in_process` and
`inspection_ofd_blocks_native_writer_process` establish lock compatibility.

Closing an independently opened database descriptor can release an existing
same-process SQLite connection's POSIX locks, even when inspection refused.
Therefore a Linux process-lifetime registry retains at most 64 database
descriptors. Reserve capacity before opening; retain every successfully opened
descriptor through all refusals and identity races. Metadata-only lookup may
reuse a retained inode, but no helper may open and close a redundant descriptor.
Exhaustion returns `SourceBusy` before opening. Serialize inspections using a
nonblocking registry mutex; explicitly release the OFD lock after the native
connection drops, while retaining the descriptor until process exit. No unsafe
code, fork, unbounded descriptor retention or connection callback is introduced.
This bounded resource cost is part of the public inspection contract. Callers
needing more distinct sources must use another process. A writer already open
before inspection, and a separate-process contention probe after refusal, must
establish that refusal preserves the existing writer's lock. The executed cases
`inspection_refusal_preserves_existing_process_writer_lock` and
`adversary_sqlite_public_descriptor_bound_keeps_prior_writer_lock` bind this claim.

SQLite uses READ_ONLY without CREATE and one deferred native read transaction.
Admit the exact supported published schema before selecting event/identity rows;
do not run DDL, pragmas that change journal mode, `BEGIN IMMEDIATE`, checkpoint,
ordinary writer construction or live-source `immutable=1`. Initial support is
allowed to be a strict cold-database subset. WAL, SHM or hot-journal conditions
whose safe read has not been demonstrated receive `RecoveryRequired` or
`UnsupportedSource`, never main-file-only success. Opening and drop must not
create or modify sidecars. Ordinary published stores select persistent WAL mode;
measure successful inspection of a cleanly closed store explicitly. If the
platform cannot inspect that state without writes, report the support limit;
do not silently prepare the source. SQLite Unix `readonly_shm=1` may be used only
with tested preservation and controlled URI encoding.

Under concurrent native writers, return a consistent observation or an explicit
busy/changed/recovery refusal. Source-change detection is required wherever the
native boundary alone does not hold the complete observation. Preservation tests
without another writer distinguish inspector writes from writer activity in the
separate concurrency tests. Persistent bytes and directory entries are preserved;
filesystem access timestamps are outside this promise.

The lock contract covers cooperating native SQLite writers. It does not grant
control over arbitrary filesystem replacement by another actor. Check source
path/inode identity at the observation boundary and refuse detected replacement
as `SourceChanged`; never describe an advisory lock as a filesystem freeze.

## Scope and evidence

This is event-history inspection, not complete-store backup or proof of replay.
It does not export command/group/claim tables, blobs, projections, caches or
original lexical SQL/frame bytes. Decoded JSON is not original lexical input.
No new digest may stand in for an original validation receipt. Consumers retain
their source files and refuse conversion when other required evidence is absent.

The value-only ESS in `ess/inspection/` types the new limits, refusal vocabulary
and coordinate projection. `RecordedEvent` remains the existing complete Rust
envelope; the coordinate projection does not claim to reproduce all its fields.
No new persistent entity or domain policy is introduced.

Both real providers must run the same history semantics exercise, with additional
native preservation, recovery, corruption and concurrency cases. Synthetic
published-format fixtures include missing tenant identity. Record pre-fix or
mutation failures and restored green results. Existing conformance remains
mandatory. Exact new provider cases join the production-proof required roster.

The reviewed source may be consumed from an exact published main commit after
required common and repository checks. This unit introduces no tag, release,
version bump, schema migration or downstream deployment.
