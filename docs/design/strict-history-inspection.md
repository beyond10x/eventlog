# Strict read-only history inspection

This additive capability serves O2 and O6: inspect the durable decision record
without altering the evidence. The RFC 0020 storage schema and the existing
provider encodings remain unchanged. Runtime claims below are unexecuted until
the strict-inspection cases and repository proof run.

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
