# Atomic blob binding with append groups — bounded scope

Read-only assessment, 2026-09-22. Source is the verified published Eventlog main commit
`2e482aa1bbfcad7dc116a3eb1f442bb3512ebe8b`, read with `git show`, plus the assigned
object-blob-scope assessment. No source/test changes, builds, live-store access, planning writes,
fetches, commits or publication. This assigned report is the sole filesystem write. No process
was started that remains running. A verified published main commit containing the eventual
correction is sufficient for the downstream pin; a release tag is not a prerequisite.

## Recommended contract

Add an explicitly selected capability. Keep `AppendGroup`, its fingerprint, its two methods,
single-stream identity behavior and existing on-disk encodings unchanged:

```rust
pub struct BlobWrite {
    pub digest: String,
    pub bytes: Vec<u8>,
}
pub struct BlobAppendGroup {
    pub group: AppendGroup,
    pub blobs: Vec<BlobWrite>,
}
pub trait AtomicBlobEventStore: AtomicEventStore {
    fn append_group_with_blobs<'a>(
        &'a self, request: &'a BlobAppendGroup,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>>;
    fn append_group_with_blobs_guarded<'a>(
        &'a self, request: &'a BlobAppendGroup, admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>>;
}
```

The unguarded method may delegate to the guarded method with `NoGuard`; no append/put loop
fallback is allowed. Both names and this shape are proposed design choices, not existing APIs.
The new trait does not add a required method to existing third-party EventStore implementations.

Use the contained group's tenant for every blob; no second tenant field can disagree. Require
at least one blob and an otherwise valid nonempty AppendGroup. Refuse duplicate digest entries,
even if equal, and validate each digest with the existing field validator. Preserve request order
for stream entries. Treat blobs as an unordered set: sort by digest for fingerprinting and binding.
This makes blob order immaterial and supplies one lock order. Caller digest strings stay opaque
owner addresses; Eventlog does not assume EKR's hashing scheme.

Choose this exact new fingerprint shape, using existing `request_hash`:

```text
{
  "format": "eventlog-blob-append-group/1",
  "group_fingerprint": <unchanged AppendGroup::fingerprint()>,
  "blobs": [
    { "digest": <opaque digest>, "byte_count": <u64>,
      "content_sha256": <lowercase SHA-256 of actual bytes> }
  ]
}
```

Entries are sorted by digest; duplicates refuse before hashing. SHA-256 is already a core
dependency (`eventlog-core/src/lib.rs:486`); length conversion must be checked. Fingerprinting
does not trust CommandMeta.request_hash or the caller's blob digest. The hash and references,
never a duplicate of the blob bytes, enter durable command bookkeeping.

Reuse the existing tenant/group-idempotency-key receipt namespace. A key already used through
legacy append_group conflicts through the new API, and vice versa, because the new discriminator
changes the fingerprint. This deliberately avoids a new table, new journal operation or historical
receipt migration. Legacy single-stream and claim namespaces remain separate as before. Freeze a
legacy fingerprint vector in the new tests so extraction of shared helpers cannot change it.

## Transaction order and outcomes

1. Validate the whole request and compute its actual-content fingerprint before mutation.
2. Enter the existing provider transaction/serialization boundary. Resolve the group receipt first.
   An equal receipt returns the original event IDs/positions/ranges with deduplicated=true. A
   different fingerprint returns IdempotencyMismatch. Neither path binds blobs, runs guards nor
   runs projectors. In particular, an exact retry after authorized blob erasure MUST NOT recreate
   the erased bytes; it acknowledges the original command, not current content availability.
3. For a new command, atomically verify or install every tenant/digest binding. Existing bindings
   must contain exactly equal bytes; a collision refuses with the same explicit Invalid message
   across providers. Never silently accept SQL ON CONFLICT DO NOTHING without reading/comparing.
   Equal reuse preserves the existing binding and its metadata.
4. Run the admission guard once, then ordered stream appends and their inline projectors. Existing
   transaction-local ProjectionStore::get_blob sees the tentative new bindings, while other handles
   cannot see them before commit. This is the explicit new API's guard visibility rule. Guards do
   not re-enter outer EventStore methods.
5. Persist the group receipt and commit the binding(s), events and projection writes together.
   Expected::NoStream remains an expectation on the lineage entry on every owner retry. A later
   expectation, guard, projector, collision or bookkeeping refusal rolls back the entire new
   request. Existing blobs owned/adopted by other successful commands are never deleted.

Conflict and IdempotencyMismatch stay distinct typed outcomes. An uncertain commit is resolved by
reopening and retrying the exact command identity/content, not a newly minted key. No compensating
delete_blob is permitted after a timeout, UnknownCommit or any potentially committed outcome.
Concurrent authorized erasure may remove a committed binding afterward; this API does not turn
blob retention into an eternal guarantee or resurrect content on a receipt retry.

## Provider implementation

**SQLite — applicable, small additive path.** `eventlog-sqlite/src/atomic_group.rs:18–117`
already owns one BEGIN IMMEDIATE, receipt lookup, admission, ordered append and bookkeeping.
Insert/select/compare blobs on that same Connection, using the existing blobs table. The standalone
put_blob at `lib.rs:1391–1436` takes its own mutex and ignores conflicting content; do not call it
inside the group. Extract a private transaction-local binder. No DDL is needed.

Preserve the legacy path's output behavior. Its current finish_transaction (`lib.rs:1776`) returns
Backend after a COMMIT error, with best-effort rollback; that is not evidence of a known abort.
For the new capability, conservatively surface UnknownCommit for an error after COMMIT was
attempted, with no blob compensation; reopen and same-identity resolution is the contract. A
small dedicated finishing helper can do this without silently changing the legacy methods.

**File — applicable, same journal transaction.** The current transaction owns the process/runtime
mutex and Journal lock across callback and publication (`eventlog-file/src/lib.rs:85–141`).
Reuse transaction-local blob integrity checking and existing `Blob {id,hash}` plus `Op::Blob`
(`lib.rs:350–394`, `state.rs:73–82,243–252`). New private payload files are written with unique
IDs and fsynced before journal publication, then blob bindings, append operations and Op::Group
are recorded in ONE existing journal transaction. No payload bytes enter that journal frame.

The decisive public boundary is a committed tenant/digest binding: get_blob on a losing fresh
digest returns None, and no losing events/projections/group receipt exist. A process crash may
leave an unbound physical file. That is private staging, not a published blob. Promise durable
bytes before a binding becomes committed, plus eventual reclamation on recovery, rather than
literally zero disk writes during crashes.

Track the exact newly staged file IDs for this transaction. On a known callback/admission failure
before journal publication begins, clean only those private files, without scanning or deleting
other bindings. If cleanup fails, retain a diagnosable unbound staging artifact; do not claim a
public binding exists or erase shared bytes. Cancellation/crash and any failure after journal
publication starts leave recovery to resolve the committed journal first. Existing recovery keeps
the frame only if manifest equals the after-state, otherwise truncates to before-state
(`journal.rs:315–347`), then current clean_blobs removes files not in recovered bindings. Never
run cleanup using a stale precommit view after an uncertain commit. Existing append UnknownCommit
boundaries at `journal.rs:242–249` must be preserved. Crash hooks/tests need the extra pre-journal
blob-file stage and both sides of journal publication; they do not require a new journal format.

**PostgreSQL — applicable, include the same capability/conformance.** The existing group engine
already takes publication gate, tenant/group identity lock, sorted stream locks and one SQL
transaction (`eventlog-postgres/src/atomic_group.rs`, group_in_transaction and trait impl).
Continue that order, then process blob keys in sorted order. Within the transaction, insert with
ON CONFLICT DO NOTHING, then SELECT the binding FOR UPDATE and compare exact bytes; the unique
constraint/row lock serializes concurrent standalone blob mutations too. Do not rely only on new
advisory locks that existing put_blob/delete_blob never acquire. Preserve the existing publication
gate, transaction timeout, connection quarantine and UnknownCommit resolution. No new table,
column, hosted permission or schema checksum is needed: both blobs and group receipts already
exist. Standalone legacy methods remain unchanged.

AGENTS invariant 2 says every provider implements applicable conformance. PostgreSQL supports
both component capabilities and transactions, so it is applicable; omitting it needs an explicit
capability boundary decision, not a silent passing lane. This read-only assessment establishes
source applicability only, not PostgreSQL runtime proof.

## Acceptance to dispatch

- Shared conformance: success and equal existing-blob reuse; different bytes behind one digest;
  duplicate digest input; empty/invalid/cross-tenant request; one and several blobs; later-stream
  conflict; guard/projector failure after tentative blob binding; tenant isolation; exact retry
  before/after reopen; changed bytes with unchanged caller hashes; old/new group key collision;
  original event IDs/ranges retained; guard/projector not repeated; retry after blob erasure never
  restores bytes. Assert both get_blob and event/projection/receipt behavior.
- Independent File/SQLite handles/processes: competing NoStream first writers with distinct new
  digests leave one complete winner and no losing public binding/event. Equal shared digest races
  preserve the successful binding; losing requests never delete a winner's bytes. Repeat with
  preexisting shared bindings and with content collisions. PostgreSQL runs equivalent concurrency
  cases with reversed request order and sorted locks.
- File crash/reopen: after private file durability, journal intent, partial frame, fsynced frame,
  committed manifest and cleanup. Result after recovery is either no binding/events or complete
  binding/events/receipt; bound files exist with exact bytes. Recovered loser staging is reclaimed.
  Missing/damaged committed files refuse, rather than being treated as fresh absent bindings.
- Provider uncertain-result injection: retry resolves the original identity with no duplicate
  appends and no compensating deletion; successful erasure is not undone by replaying a receipt.
- Fingerprint/format compatibility: old fixed fingerprint vectors and old journal/database
  fixtures remain readable; new group receipts contain only hashes/ranges and no payload marker.

## Scope

```markdown
## Scope

Derived 2026-09-22 by story-scoper for the proposed atomic content capability; no artifact ID was supplied.

- **Primary surface:** Eventlog atomic append engines and transaction-local blob binding — cited.
- **Files:** crates/eventlog-core/src/atomic_group.rs; crates/eventlog-core/src/lib.rs — cited, current public ports/fingerprint and exports.
- **Files:** crates/eventlog-sqlite/src/atomic_group.rs; crates/eventlog-sqlite/src/lib.rs — cited, transaction composition, blob SQL and commit-result classification.
- **Files:** crates/eventlog-file/src/lib.rs; crates/eventlog-file/src/journal.rs — cited, binding/staging cleanup and existing crash probes.
- **Files:** crates/eventlog-postgres/src/atomic_group.rs — cited, lock/transaction/UnknownCommit integration; blob SQL can stay private here without editing legacy lib.rs.
- **Files:** crates/eventlog-conformance/src/lib.rs — cited, shared conformance exports.
- **New tests:** crates/eventlog-conformance/src/atomic_blob_group.rs — inferred, distinct additive capability exercise.
- **Tests:** crates/eventlog-sqlite/tests/atomic_groups.rs; crates/eventlog-postgres/tests/atomic_groups.rs; crates/eventlog-file/tests/conformance.rs; crates/eventlog-file/tests/durability.rs — cited, existing applicable harnesses and concurrency/restart lanes.
- **Documents:** docs/design/atomic-append-groups.md; docs/design/file-provider.md — cited, additive port semantics and physical staging/recovery contract.
- **Confidence:** high for existing mutation points and provider applicability — cited, exact published sources read; the proposed new module name is inferred above.
- **Would collide with:** File/SQLite provider lib.rs and File journal.rs, including the active inspection work's integration surface — cited. Keep a separate unit/tree and sequence integration/review; do not silently add this work to inspection.
```

Scope commands for the coordinator only; `<story-id>` is deliberately unassigned, not an invented
planning ID. The new capability's API and error contract need an owner-recorded decision before
implementation. No command below was executed.

```sh
aep plan artifact scope <story-id> --add crates/eventlog-core/src/atomic_group.rs
aep plan artifact scope <story-id> --add crates/eventlog-core/src/lib.rs
aep plan artifact scope <story-id> --add crates/eventlog-sqlite/src/atomic_group.rs
aep plan artifact scope <story-id> --add crates/eventlog-sqlite/src/lib.rs
aep plan artifact scope <story-id> --add crates/eventlog-file/src/lib.rs
aep plan artifact scope <story-id> --add crates/eventlog-file/src/journal.rs
aep plan artifact scope <story-id> --add crates/eventlog-postgres/src/atomic_group.rs
aep plan artifact scope <story-id> --add crates/eventlog-conformance/src/lib.rs
aep plan artifact scope <story-id> --add crates/eventlog-conformance/src/atomic_blob_group.rs --inferred
aep plan artifact scope <story-id> --add crates/eventlog-sqlite/tests/atomic_groups.rs
aep plan artifact scope <story-id> --add crates/eventlog-postgres/tests/atomic_groups.rs
aep plan artifact scope <story-id> --add crates/eventlog-file/tests/conformance.rs
aep plan artifact scope <story-id> --add crates/eventlog-file/tests/durability.rs
aep plan artifact scope <story-id> --add docs/design/atomic-append-groups.md
aep plan artifact scope <story-id> --add docs/design/file-provider.md
```

## Not established

No behavior was executed. Failure-injection practicality, PostgreSQL hosted proof and bounds under
large payloads remain implementation/test work. No live PostgreSQL configuration was inspected.
No new persisted domain entity or ESS document was identified: requests compose existing blob and
append ports, while private storage formats stay fixed; coordinator must verify its normative
recording route. No existing active inspection branch or unpublished cached capture implementation
was used as a capability prerequisite. EKR object-format migration remains a separate downstream
unit after this additive capability is published and verified at an exact main commit.
