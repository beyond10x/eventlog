# Verified SQL blob reads and an explicit legacy boundary

Owner: story:sql-blob-read-integrity. Selected by Astra for independent design review under
approved ESS evolution revision1, plan ess-evolution-20260915. No implementation is claimed.
Source analysis used accepted integration18322cbe19f0068e9b3fab84d874d6abea181f3e.
The coordinate model is ess/blob-integrity/. It describes storage metadata, never product payloads.

## Problem and required result

SQLite and PostgreSQL currently store tenant_id, digest, bytes, byte_count and recorded_at for each
blob. Their ordinary read, transactional callback read and put readback return bytes without
checking byte_count or a computed content hash. An existing valid binding can therefore return
changed bytes after a database reopen, including a same-length change.

Every byte-returning SQL path must verify the stored length and a stored, independently computed
SHA256 before returning bytes or acknowledging a put. Public digest spellings remain opaque
identities scoped by tenant. A digest is not a promise that its spelling hashes the content.
The existing immutable-binding contract and concurrent put/delete behavior remain mandatory.

This design owns the SQL integrity extension explicitly left open by blob-binding-parity.md.
It supersedes that completed unit's no-DDL-change constraint for the additive edition below and
its no-UPDATE constraint only for backfilling newly added integrity metadata inside the fenced,
atomic legacy migration described here. Runtime put still never overwrites an existing binding.
No UPDATE may change predecessor tenant_id, digest, bytes, byte_count or recorded_at; no new
events-table UPDATE is authorized. All other immutable-binding constraints remain in force.
It does not change eventlog-file/1, any event envelope, domain policy or ER record format.

## Stored integrity edition

Append exactly these columns after the existing five, preserving their order and meaning:

```sql
integrity_sha256 TEXT,
integrity_v1 INTEGER NOT NULL DEFAULT 1
    CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL)
```

The first column is physically nullable so an admitted populated predecessor can be backfilled
inside one transaction. The second column's enforced check makes the completed edition reject
old five-column INSERTs. Fresh and migrated tables must have the same exact physical shape,
including column order, nullability, defaults and validated checks. No sentinel, missing hash or
permanently permissive migration shape is admitted. Do not rewrite payload bytes or byte_count.

This is the first SQL blob integrity format, explicitly tagged by integrity_v1=1. Its digest is
exactly64 lowercase ASCII hexadecimal characters encoding SHA256 over the complete stored bytes.
The existing PostgreSQL version1 migration-ledger family retains version1 and gets a new exact
checksum edition. That is an incompatible edition for the old exact reader, not a claim of old
writer compatibility. Freeze the original, snapshot and atomic-group edition checksums before
editing the base DDL: their current functions derive from that mutable DDL and must not drift.
Pin the actual evaluated predecessor constants with golden tests, including the no-ledger
pre-admission predecessor already supported by the migration code.

The checksum detects stored-content corruption relative to stored integrity metadata. It is not
a signature or historical authenticity proof against database authority that rewrites bytes and
their checksum together. No caller-selected key, cloud service or SQL extension is introduced.

## One validation rule at every read boundary

Both providers use the same Rust-owned byte/count/hash validation semantics, preferably one small
eventlog-core module alongside the existing storage coordinates. Validate all values with fallible
decoding; malformed or null metadata must produce EventLogError::Backend rather than panic.
Check byte_count is nonnegative and equals the exact byte length, integrity_v1 is1, digest encoding
is exact, and SHA256 of the returned bytes equals integrity_sha256. Persisted corruption uses a
generic Backend description with no payload content. Outgoing length overflow is invalid input.

The complete boundary enumeration is:

| Provider | Ordinary read | Transaction callback read | Put readback |
| --- | --- | --- | --- |
| SQLite | Inner::get_blob | SqliteProjections::get_blob | Inner::put_blob |
| PostgreSQL | PostgresEventStore::get_blob | PostgresProjections::get_blob | PostgresEventStore::put_blob |

Select bytes, byte_count, integrity_sha256 and integrity_v1 together. On put, validate the existing
stored binding first, then compare it with the requested bytes. A corrupt binding returns Backend;
an intact binding to different bytes retains Invalid; identical intact content retains success.
SQLite keeps its immediate transaction. PostgreSQL keeps the same-statement FOR SHARE acquisition
and retry when a concurrent deletion removes the conflicting row. Deletion/rebinding and tenant
erasure retain current authority and atomicity; rebinding computes fresh integrity metadata.

SQLite asks this validator once per distinct content on each handle, not once per read. The
validator is a pure function of the selected row, so a handle keeps each `(integrity_sha256, bytes)`
pair it has verified, or has hashed itself at write time, holding the exact bytes. A later row is
accepted without a new SHA256 only when its hash is a remembered key, its bytes are equal in full
to the remembered bytes, and its byte_count and integrity_v1 pass as above. Any changed byte, hash,
count or edition misses and is validated in full, so a row altered after a verified read on the same
handle is still refused. The memory is bounded (32 MiB per handle) and cleared when the handle
deletes a blob, erases a tenant or returns an error from a write that bound blobs; a guard or
projector panic after binding skips that clearing. A deletion or erasure through
another handle does not clear it: those entries are never returned, since a hit needs an identical
stored row, but stay in that process's memory until one of those events or the handle is dropped. An in-memory database keeps its existing metadata-only check.
PostgreSQL still hashes on every read.

A callback context carries distinct owner-blob and projection-target coordinates. Ordinary,
guarded, grouped, inline and catch-up contexts use the actual owner prefix for both. During
SQLite and PostgreSQL rebuild, the owner-blob prefix remains the original store prefix while the
projection target uses eventlog_rebuild. Blob queries always use the former with the caller's
unchanged tenant; view operations always use the latter. The current single-prefix context
incorrectly derives a nonexistent shadow blob table during rebuild and must be corrected in both
providers. Do not copy blobs into a shadow schema or grant a projector cross-owner authority.

A transactional callback encountering stored corruption must make the enclosing transaction fail,
even if a guard/projector catches the read error. Preserve a failure marker at the provider's
transaction boundary and check it after each guard/projector callback and before any successful
commit. The marker is shared across callbacks in the same transaction, including rebuild's
separate-coordinate context; catching an error cannot clear it. Apply that requirement to every
callback owner, including guarded single/group appends, inline projection and catch-up/rebuild.
No event, claim, command/group receipt, reservation, view or cursor from that transaction survives.
An absent blob remains Ok(None), and existing invalid-input behavior is not a corruption report.
Existing cancellation/quarantine and reservation handling remain intact.

## Admission and migration choices

No ordinary open performs an unbounded scan of already admitted blob data. It verifies physical
shape; each read verifies its row. An explicit legacy import inspects all predecessor rows once.

| Observed blob shape | Default behavior |
| --- | --- |
| No owner schema, existing fresh-store preconditions satisfied | Create the exact current edition |
| Exact supported five-column predecessor, zero blob rows | Upgrade in one transaction |
| Exact supported five-column predecessor, populated | Refuse before mutation, naming the count and explicit migration choice |
| Exact current seven-column edition and matching ledger | Admit; check contents on read |
| Partial columns, altered type/default/check/index or inconsistent ledger | Refuse before mutation |

SQLite adds exact old/current blob-table admission; its existing event/snapshot heuristics do not
prove blob compatibility. Keep admission narrow to this table, checking ordered columns, declared
types, nullability, primary key, defaults, indexes and the integrity check, plus refusing views,
triggers or other hidden behavior on that relation. Do not silently admit a similar foreign table.
PostgreSQL preserves its full relation/security/sequence/collation/projection admission. Compare a
recognized old edition against the old blob shape before changing it, then compare the final shape
against the new expected table. Preserve every accepted historical ledger edition and reject
mixed/partial states; a checksum alone is never shape admission.

Expose the explicit provider-edge policy as a closed Rust enum, LegacyBlobMigration with
RefusePopulated and TrustObservedBytes. Existing open/connect/local/migrate signatures remain as
wrappers selecting RefusePopulated. Add SqliteEventStore::open_with_blob_migration,
PostgresEventStore::local_with_blob_migration and PostgresEventStore::migrate_with_blob_migration.
The local/open variants return the store plus a BlobMigrationReport; the migration-only variant
returns Result<BlobMigrationReport, EventLogError>. Its finite data is upgraded:bool and
trusted_legacy_rows:u64, with no fabricated
history or hidden clock. No serialization format or configuration default is added by these values.
Hosted traffic still opens with the DML-only role after explicit migration; it never takes a trust
option or gains DDL authority. Existing local convenience constructors remain refusal-default.

TrustObservedBytes is an explicit assertion about the complete observed predecessor, not proof of
its historical correctness. Under SQLite BEGIN IMMEDIATE, or PostgreSQL's existing migration
advisory lock plus an owner-blob table lock held through commit:

1. Verify supported old schema/ledger before any persistent mutation.
2. Count and read every predecessor blob in stable tenant/digest order. Validate identity fields,
   byte decoding, nonnegative exact byte_count and all required old column types. A malformed row
   aborts the complete migration. Do not infer missing identities or silently repair metadata.
3. Under TrustObservedBytes only, compute hashes over those exact observed bytes and backfill the
   new hash column. Then add the integrity_v1 column and its constraint.
4. Verify exact final shape, each imported binding and complete row-count conservation. Update the
   PostgreSQL exact edition ledger and commit all DDL/backfill/ledger changes atomically.
5. Return upgraded=true and the exact trusted row count after acknowledged commit. Fresh creation
   or empty legacy upgrade returns upgraded=true/0; already-current admission returns false/0.

Keep memory bounded while scanning/backfilling; do not collect the entire legacy blob table.
No migration may report success before commit acknowledgement. PostgreSQL migration commit-response
loss returns the dedicated EventLogError::BlobMigrationCommitUnknown with no report. Its diagnostic
requires reinspection of the owner schema, not lookup by an invented command identity; ordinary
append UnknownCommit semantics stay unchanged. No success report may be fabricated.
Migration-role failure or cancellation must leave an old admitted edition or the complete new one,
never a committed half-edition. Preserve prior snapshots, groups, events, receipts, views and bytes.

### Acknowledged result and connection cleanup

The migration-only API must retain the acknowledged BlobMigrationReport before shutting down its
temporary pool. Always attempt shutdown, then combine both results explicitly:

| Migration result | Pool shutdown | Returned result |
| --- | --- | --- |
| Acknowledged report | Success | Ok(report) |
| Acknowledged report | Failure | EventLogError::BlobMigrationCompleted { report, cleanup: Box<EventLogError> } |
| Failure, including BlobMigrationCommitUnknown | Either outcome | The original migration failure |

BlobMigrationCompleted means the migration operation completed and only cleanup failed. Preserve
the exact upgraded flag and trusted_legacy_rows count, plus the typed cleanup cause. Its generic
diagnostic must not suggest rollback or encourage repeating a trusted import. Existing migrate
wrappers propagate this variant without discarding the report. This is a concrete transient Rust
error variant, not a new persisted error envelope or migration journal. Successful local/open APIs
retain the pool in the returned store and therefore do not perform this post-commit shutdown.

A retry may observe the already-current edition and return false/0; it describes that retry only
and must never replace the first acknowledged report. If a caller cancels or loses the response,
this API promises no durable receipt or reconstruction of the original trusted count. Reinspection
can establish the current edition; the later owner migration tool owns durable cutover receipts.
Exercise actual acknowledged migration followed by controlled shutdown failure, including the
existing refusal-default wrapper, and prove that a primary migration error is not masked by a
secondary cleanup error. Use the pool's existing bounded shutdown/quarantine boundary.

## Fenced cutover and reader compatibility

All old writer/feed/reader processes must be quiesced before upgrading, as AGENTS.md already
requires for mixed protocol generations. Existing old SQLite binaries can still open the physical
database and read without checksum validation. Already-open old PostgreSQL clients can also issue
unchecked reads. Retaining those physical read paths does not authorize their use after cutover.
Old five-column INSERTs must fail under the new constraint. New opens using an old PostgreSQL
reader's exact schema/checksum policy refuse the new edition. Prove these cases with actual old
DDL and operation shapes before relying on compatibility; do not merely test the new constructor.

No operator confirmation is implemented inside these libraries. The caller explicitly selects
TrustObservedBytes after establishing its own fence and provenance. This task changes no real
owner store; later AEP/ER migration tools own their real cutovers and higher-level receipts.

## Required evidence

Write decisive real SQLite/PostgreSQL regressions before implementation. Preserve every old
immutable-binding/concurrency assertion and execute all applicable new cases in the production
proof roster with exact provider/target/case identity.

- Same-length byte corruption survives a reopen on the old code and is refused after the change;
  test length-only corruption, wrong/malformed/null digest and wrong integrity version separately.
- Exercise all six boundaries, including corrupt put readback refusing before conflict comparison,
  intact retries/conflicts, absent reads, tenant isolation and delete/rebind.
- A callback corruption error, propagated and deliberately caught, prevents every append/group,
  guard/reservation/view/receipt/cursor effect. Include catch-up/rebuild and unchanged success paths.
  Rebuild success must read a present owner blob while writing the shadow view in both providers;
  corruption of that owner blob must abort rebuild even if caught, preserving old views/cursors.
- Exact legacy empty upgrade, populated default refusal without mutation, explicit complete import,
  reopen, malformed-row rollback, concurrent migrators and failure between migration stages.
- Actual old INSERT refusal, old PostgreSQL exact-reader refusal, SQLite physical-read limitation,
  all historical ledger goldens and partial/altered schema refusal before any persistent mutation.
- Preserve the exact acknowledged import report through post-commit pool shutdown failure and
  distinguish it from unknown commit and pre-commit failure. Retry false/0 cannot rewrite that fact.
- Controlled reversible mutations of content hash, count, callback failure marker, old-writer
  constraint and historical-edition recognition must make their unchanged cases fail.

Run affected suites, formatting, strict workspace all-target Clippy and independent review, then
the actual bash scripts/gate.sh --production-proof with disposable PostgreSQL17.6 and hosted TLS.
Preserve raw outputs, terminal statuses, source identities and required-case counts. No skipped
provider or selected-zero result is acceptance. The larger initiative still requires group
crash/unknown/restart qualification, frozen file vectors and all real consumer migrations.
