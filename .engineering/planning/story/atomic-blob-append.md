---
format: aep.planning-md/1
id: story:atomic-blob-append
kind: story
status: active
title: Commit blob bindings and ordered event appends atomically
scope:
- confidence: inferred
  path: crates/eventlog-conformance/src/atomic_blob.rs
- confidence: cited
  path: crates/eventlog-conformance/src/lib.rs
- confidence: inferred
  path: crates/eventlog-core/src/atomic_blob.rs
- confidence: cited
  path: crates/eventlog-core/src/atomic_group.rs
- confidence: cited
  path: crates/eventlog-core/src/lib.rs
- confidence: cited
  path: crates/eventlog-file/src/journal.rs
- confidence: cited
  path: crates/eventlog-file/src/lib.rs
- confidence: cited
  path: crates/eventlog-file/src/state.rs
- confidence: inferred
  path: crates/eventlog-file/tests/adversary_atomic_blob.rs
- confidence: inferred
  path: crates/eventlog-file/tests/atomic_blob.rs
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof.rs
- confidence: cited
  path: crates/eventlog-postgres/src/atomic_group.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: inferred
  path: crates/eventlog-postgres/tests/adversary_atomic_blob.rs
- confidence: inferred
  path: crates/eventlog-postgres/tests/atomic_blob.rs
- confidence: cited
  path: crates/eventlog-sqlite/src/atomic_group.rs
- confidence: cited
  path: crates/eventlog-sqlite/src/lib.rs
- confidence: inferred
  path: crates/eventlog-sqlite/tests/adversary_atomic_blob.rs
- confidence: inferred
  path: crates/eventlog-sqlite/tests/atomic_blob.rs
revision: 23
---
# Atomic blob binding and event append

This additive provider capability serves O2 and O6. It satisfies the existing
RFC 0020 payload boundary while publishing metadata/events and their retained
content in one native transaction. Implementation and conformance agreement are
unexecuted. The detailed inspected scope is
../../.engineering/waves/atomic-content-scope.md.

## Public port

Add BlobWrite { digest: String, bytes: Vec<u8> } and
BlobAppendGroup { group: AppendGroup, blobs: Vec<BlobWrite> }.
AtomicBlobEventStore extends AtomicEventStore with append_group_with_blobs and
append_group_with_blobs_guarded, returning the existing AppendGroupResult.
There is no independent-put fallback or new required method on an existing trait.

The group tenant contains every blob binding and append. Require a nonempty
valid append group and at least one blob. Existing blob-key and event/metadata
validation applies. Duplicate blob keys refuse even with equal bytes. Blob order
does not change semantics: sort by digest for fingerprint and binding. Preserve
the append group's entry order and repeated-stream behavior.

The new fingerprint is domain version eventlog-blob-append-group/1, binding the
unchanged legacy group fingerprint plus sorted digest, checked byte length and
SHA-256 of actual bytes for each blob. A caller's digest/request_hash alone
cannot certify equal request content. Reuse the existing group receipt namespace.
New versus legacy use of one key conflicts; freeze legacy fingerprint vectors.
No DDL change or new File journal operation is required.

## Transaction and retry semantics

Validate the whole request, then enter the provider's native transaction.
Look up the durable group receipt before binding bytes or invoking callbacks.
An exact retry returns original event IDs, positions and ranges as deduplicated.
It repeats neither admission nor projection and does not recreate a blob erased
after the original success. A changed request under that identity refuses.

For a new request compare preexisting bindings against actual bytes. Equal bytes
may be reused; differing bytes refuse. Install tentative blob bindings before
admission and inline projectors so their tenant-confined get_blob sees the new
content. Guard failure, stream conflict, projector failure and later-entry failure
roll back both bindings and events. Existing external bindings are never deleted
as error cleanup. Missing/redacted bytes on a retry do not authorize resurrection.

SQLite uses its existing blob table in the same BEGIN IMMEDIATE as events and
group receipt. A failed response after COMMIT begins is conservatively
UnknownCommit for the new capability; resolve through its original request key.

File writes uniquely named staged physical content and fsyncs it before one
journal transaction containing existing Blob, Event and Group operations.
The committed binding is the public visibility boundary. Known prepublication
failure may remove only this request's unique unbound staging objects; cleanup
failure is an explicit retained unbound artifact. A crash/unknown commit does not
authorize deleting potentially committed bytes. Existing journal recovery first
establishes the manifest state; ordinary cleanup can later collect proven unbound
objects. This promises no losing public binding, not zero physical I/O before a
crash. Reuse the provider's native lock, journal, privacy and integrity machinery.

PostgreSQL implements the same applicable contract under its publication gate
and existing tenant/group/stream ordering, with sorted blob-key locking. Native
unique conflicts and SELECT FOR UPDATE compare existing bytes while preserving
standalone put/delete correctness. Keep cancellation quarantine, exact receipt
resolution and UnknownCommit semantics. No product-specific backend omission.

## Verification and delivery

Shared conformance runs through File, SQLite and PostgreSQL. Cover exact reopen
retry, real changed bytes under a reused caller digest, existing equal/different
content, empty/duplicate/cross-tenant requests, guard/projector/later-stream
rollback, tentative callback reads, legacy/new identity conflicts and retry
after authorized erasure. Independently inspect raw blob bindings as well as
event streams. Concurrent writers and native crash/unknown-commit cases prove
one complete result or named refusal without deleting another writer's content.

Keep existing tests and legacy persisted formats. Add exact new case names to
the production-proof roster. All source checks, independent review, mutation
evidence, PostgreSQL verified-TLS/hosted-role production and comparative/restart
proof precede publication. Consumers update all dependency selectors and locks
to the exact published required-checks-green commit together; no tag is required
for that immutable Git adoption.

## Implementation handback

The implementor's stable source report is
.engineering/reviews/atomic-content-implementation.md. It reports the same-command
whole suite rising from 156 to 176 passed, with no failures or ignored cases,
and the required real PostgreSQL production gate passing. Its restored mutation
sweep caught every recorded mutation behaviorally; development test/harness misses
remain disclosed in the report. These are unit results, not independent approval.

The source preserves existing fingerprints, native schema and File operations.
The new opt-in capability covers File, SQLite and PostgreSQL with receipt-first
retry, tentative callback visibility, rollback, native contention and explicit
unknown outcomes. Original inspection source/cases remain unchanged. Independent
review and required source comparative/restart CI precede consumer adoption.
