# Inline projection administration

Status: design accepted after review-result:inline-projection-admin-design-pass-2 under
story:inline-projection-administration. Capture and SQL integrity are accepted implementation
dependencies. Administration implementation passes the required provider gates; source examination
and local integration remain pending. This page alone does not admit source implementation.

## Outcome and boundaries

An owner can attach its already admitted inline projector without changing durable state, and can
explicitly rebuild that projector's complete tenant row sets atomically. The provider owns native
coordination, shape validation, blob access and publication. The consumer owns projector semantics
and the comparison that makes its derived rows trustworthy. Eventlog gains no consumer dependency.

The concrete consumer is the recorded Entity Runtime adapter. Its open protocol requires an
already attached handle, native consistent tenant capture and exact comparison of independently
derived rows. Its maintenance protocol verifies authority before rebuild and checks a fresh
capture afterwards. Attachment alone does not permit that adapter to serve.

Existing catch-up, feed, ordinary registration and rebuild APIs keep their contracts. This new
capability neither uses a second projector name to evade inline checks nor unregisters a live
projector. Eventlog's existing formats, envelope, registry, row and cursor representations remain.
There is no persisted projector identity, code digest, cleanliness certificate or new journal op.

## Selected API

Add a separate object-safe capability exported by eventlog-core, implemented natively by File,
SQLite and PostgreSQL. There is no default implementation or pagination fallback.

```rust
pub trait InlineProjectionAdmin: Send + Sync + 'static {
    fn attach_inline_existing(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>>;

    fn rebuild_inline_projection<'a>(
        &'a self,
        projector_name: &'a str,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<InlineRebuildResult, EventLogError>>;
}

pub struct InlineRebuildResult {
    pub applied: u64,
    pub position: u64,
}
```

Rebuild selects the actual attached Arc by exact projector name. It executes that registered
instance; the caller cannot pass different code with the same name/specs. This concretizes the
consumer's earlier preferred Arc-taking proposal without changing its requirement to rebuild the
same registered projector. The consumer contract must adopt this signature before implementation.
Names and specs are the existing Projector and ProjectionSpec contracts, not persistent code IDs.

The two result counts are transient Rust values, with the bounded typed home in ess/inline-admin/.
No Serde format is introduced. `applied` counts every authoritative tenant event passed successfully
to the projector during this rebuild; it is not the number of changed rows. `position` is the actual
last global sequence, or zero for an empty tenant history. Preserve gaps; do not derive position
from count. Use checked counts and fallible conversions of all stored numeric coordinates.

## Structural attachment and the dirty-state distinction

Attachment has no TenantId. Registry and physical shape are global to this provider owner, while
the installed projector set is local to this handle. Validating one tenant and then installing a
global projector would not prove anything about other tenants.

Acquire the same registration/freeze lock used by ordinary inline registration. Refuse a frozen
handle, a duplicate local projector name, invalid names/specs, duplicate projection names or
duplicate indexed fields. Compare every selected spec against its existing registry entry and
exact physical shape, preserving indexed-field order and the accepted privilege/owner profile.
Missing or incompatible admission refuses. Do not create tables or indexes, including temporary
ones, insert registry entries, initialize authority, recover or clean storage.

Install the Arc in local memory only after complete validation succeeds. Refusal, cancellation or
validation failure must not leave a partially installed projector/name set. Hold coordination
through the final local update, with no await between pieces of that update. Attachment completion
reports only structural admission and local installation. It does not establish row freshness,
integrity, historical projector association, or application correctness.

This explicitly corrects the earlier proposal that attachment itself uniformly refuses dirty
state. File persists per-tenant dirty markers; SQL has no equivalent marker. A File restart loses
local registration while preserving dirty markers, so refusing attachment and requiring attachment
before rebuild creates a cycle. Structural attachment preserves those markers. All existing File
projection-use and capture refusals remain, and only an authorized successful rebuild can clear a
marker for its selected tenant/tables. Attachment never returns a clean-state claim.

Uniform consumer readiness remains strict: capture refuses redacted history for a valid identified
tenant on every provider, including SQL and requests for no projections. The accepted capture
implementation supplies this refusal; SQL integrity does not introduce dirty tracking. For nonredacted history, the
consumer compares complete captured row/key sets with a separate deterministic derivation before
serving. A missing, extra, changed or stale row refuses. Cursor equality and registry equality
cannot replace that comparison: inline appends do not advance catch-up cursors, and other handles
can append with different local projector sets. All writers must obey the selected fenced bootstrap
and attachment protocol. Direct or old writers remain outside that operational boundary.

The ER adapter intentionally refuses redacted authority before rebuild. Generic provider rebuild
ordering must not be confused with reconstructing erased application content. An acknowledged
import boundary remains a separate consumer capability.

## Rebuild admission and snapshot

Explicit maintenance excludes ordinary writers, erasure and readers at the owner level until the
post-rebuild comparison completes. The provider also enforces its own real publication boundary;
an input flag is never evidence of exclusion.

Validate tenant and projector name, find the already attached projector, and retain its Arc for the
operation. An absent name refuses, even if its table specs happen to exist. Revalidate its exact
admitted specs and physical shapes without creating or admitting anything. Local registration must
remain unchanged throughout success, refusal and cancellation. Rebuild can run after serving has
frozen registration: it does not change that registration.

Hold registration/freeze coordination for the entire operation, including validation, fold,
publication and cleanup. Lock order is File runtime lock then process lock; SQLite registration
mutex then connection mutex inside the owned blocking worker; PostgreSQL registration mutex then
pool lease, publication session lock, transaction and tenant/projector lock. Selecting an Arc alone
does not exclude a concurrent registration. Never take registration after holding a connection or
publication lock. SQLite guards stay inside its synchronous worker; no std mutex guard crosses the
caller-facing boxed async future. PostgreSQL uses its existing async registration mutex. File uses
its existing runtime mutex. Drop/return releases local coordination after the native operation's
completion or ownership handoff to provider retirement; cancellation grants no durable-state claim.

Within one provider-owned observation, read the complete committed tenant history in increasing
actual global sequence and resolve blobs from active authoritative storage. This is not the
ordinary feed watermark. Validate the same stored event/envelope/numeric invariants as native
capture, without filtering malformed rows. Tenant identity is not created; the generic operation
does not claim an ER binding or generation. The consumer performs its identity/binding checks.

Every row operation given to the projector targets only this tenant and its selected shadow tables.
`get`, `get_for_update`, `find`, `upsert` and `delete` must share that shadow namespace; existing
tenant/spec access controls remain. Reservation authority is absent and refuses. `get_blob` uses
the active provider's blob namespace within this same observation and the qualified complete-content
validator. A missing binding returns None, so the projector can apply its own typed refusal; corrupt
stored bytes fail rather than being exposed. No callback re-enters the outer provider.

The generic projector receives existing RecordedEvent values, including an existing redacted event
when its own semantics support that event. The provider never invents the erased payload. The ER
projector refuses it and no rows publish. This preserves ordinary provider redaction vocabulary
while the stronger ER full-history requirement stays at the consumer boundary.

Fold from empty row sets for every selected spec. Do not seed the fold with potentially damaged
active rows or trust a prior cursor. A successful fold replaces all selected tenant row sets and
the projector's cursor in one publication; File also clears selected dirty markers in that same
publication. Preserve other tenants, other projectors, registry entries, events, receipt state,
identity, blobs and snapshots. No intermediate row prefix becomes visible. A projector/decode/blob
failure aborts every selected replacement. Successful `applied`/`position` describe the published
fold, not work attempted before rollback.

## Native provider mechanics

File attachment uses the strict existing-file reader selected by consistent capture under the
runtime registration lock and existing process lock. Never call the ordinary transaction path
merely to validate: it may clean blobs or snapshots. Missing files, malformed committed state or
pending recovery intent refuse without repair. Preserve the observed-manifest extension guard;
successful validation may update only its in-memory observation.

File rebuild uses the same process exclusion and validated committed state, creates a private
transaction state for the fold, and publishes existing ClearRows/Row/Cursor/DirtyView operations as
one journal transaction. Do not invoke unrelated cleanup as a side effect of this new operation.
Pending append/privacy recovery must be completed through existing explicit administration first.
Retain current frame/hash/manifest and publication-uncertainty behavior. No new eventlog-file/1
operation or canonical representation is needed.

SQLite attachment holds registration coordination and the connection mutex, begins an immediate
transaction, and validates registry/physical shape without DDL or DML. End the transaction before
installing the local Arc, still holding registration coordination. A failed end prevents attachment.
The worker's existing cancellation behavior remains; dropping its caller future does not prove
that its blocking work stopped. All local registration updates are indivisible under the lock.

SQLite rebuild holds the connection mutex and BEGIN IMMEDIATE for validation, full history, blob
reads, shadow population and the final all-table/cursor replacement. Use temporary shadow tables
as existing rebuild does; split active blob coordinates from shadow row coordinates in the
projection context. Update all ordinary/group constructor sites consistently. A separate handle's
append, redaction or erasure cannot cross the writer transaction. A cancelled blocking worker may
finish and publish the whole fold; it must never publish a partial fold or a false completion.

PostgreSQL attachment uses the capture design's direct, non-DDL physical-shape comparator. Hold
registration coordination through validation and local installation. Use a read-only transaction
at REPEATABLE READ isolation for a coherent multi-query registry/catalog observation; READ ONLY
alone does not select a stable snapshot. Deployment admission/DDL remains externally fenced.
Do not reuse the current comparator that constructs expected temporary tables. Quarantine a lease
before transaction work and settle it only after the transaction has ended successfully.

PostgreSQL rebuild acquires and quarantines its pool lease, then takes the existing exclusive
publication coordinate as a session advisory lock before beginning a fresh REPEATABLE READ
read-write transaction. Acquiring this lock after fixing the snapshot could observe state before
an earlier erasure. Take the existing tenant/projector transaction lock next, with publication
always first. Use the same database/schema/owner-prefix coordinates as current append and erasure.

Read every committed tenant event from that snapshot, without WATERMARK or watermarked_source.
Prior compliant publishers have settled; later ones cannot allocate while the exclusive gate is
held. Page within this same snapshot with the existing bounded batch size. Active blobs and shadow
rows use separate coordinates; blobs remain under the same repeatable-read snapshot even when a
blob writer does not take the publication gate. Create temporary shadow row tables, fold, replace
every selected active tenant row set and the cursor, then commit atomically.

After transaction completion, explicitly unlock and verify successful release before settling the
lease. A lock/transaction/unlock failure or cancellation keeps it quarantined; driver retirement
releases locks before pool capacity returns. Reuse the accepted capture lock lifecycle rather than
introducing another publication coordinate. Ordinary feed/catch-up watermark behavior is unchanged.

## Refusal, cancellation and completion

Use existing EventLogError variants. Invalid requests, frozen/duplicate attachment and missing or
incompatible admission return Invalid; callers must not parse display prose into application
outcomes. Storage decode/integrity failures retain the established backend error path, and
projector errors propagate unchanged. No new event envelope or error wire format is introduced.

Before publication, a conclusive validation/projector failure publishes no rows. Once commit or
File manifest publication may have succeeded, loss of acknowledgement is UnknownCommit. A
PostgreSQL timeout before publication retains Deadline; a timeout during or after an attempted
commit is UnknownCommit. Failure releasing the publication lock after commit also cannot claim
noncommit or success; return UnknownCommit and retire the lease. Preserve the existing deadline
budget across acquire, fold, commit and cleanup, rather than restarting it at each phase.

An abandoned caller receives no completion value. In particular, SQLite's continuing blocking
worker and a cancelled PostgreSQL commit cannot be described as rollback from cancellation alone.
Recovery is explicit: keep maintenance, capture authority and rows, compare, and rebuild again when
needed. This operation appends no application event and has no fabricated idempotency receipt.
The ER maintenance procedure reports success only after its separate final capture comparison.

## Qualification

One shared contract runs through the actual new APIs on File, SQLite and PostgreSQL, with native
provider cases where storage/locking must be observed. Retain causal pre-fix or exact mutation
evidence for each decisive regression and restore production code before running gates.

- Attach an admitted projector, then prove unchanged durable bytes/catalog/registry and local
  is_inline success. Missing/drifted/duplicate/frozen and invalid-spec attempts leave no partial
  local installation and no durable write, including temporary DDL on the restricted SQL role.
- For File, present pending append intent and pending privacy intent separately to both attachment
  and rebuild on an existing handle. Each attempt refuses before local installation or row
  publication and preserves exact canonical file bytes/entries, intents and dirty state. Exercise
  the previously attached rebuild case as well as empty local registration; a generic process-death
  or reopen test does not establish these four native refusal paths.
- Race attachment/ordinary registration against rebuild before serving freezes. Each provider's
  registration coordination prevents a set change during replay/publication, and the waiting
  operation proceeds or refuses only after the held operation ends. Include cancellation while
  coordination is held and prove a later eligible operation proceeds without deadlock.
- Preserve File dirty markers through attachment and restart. Projection use/capture still refuses;
  a redaction-aware generic rebuild may repair its rows, while ER's redacted-authority path refuses.
  A stale row with valid schema is never presented as clean attachment evidence.
- Rebuild the actual registered instance by name; missing names and foreign specs refuse. Cover
  empty history, sparse positions, several selected tables, several tenants and preserved unrelated
  projectors. An extra damaged active row disappears only after the complete successful fold.
- A projector reads its own earlier shadow writes through every applicable row read and resolves
  a live active blob. Wrong namespace and missing/corrupt blob mutations fail causally. Cross-tenant
  and unselected-table operations retain existing refusals; reservation authority stays unavailable.
- Fail midway through a multi-table fold and pause immediately before replacement. Other readers
  see the whole old or whole new state; no subset becomes visible. Verify cursor, dirty markers,
  registration and other tenant state alongside rows. Callback failure restores the old state.
- PostgreSQL rebuild includes a committed event hidden from the ordinary feed by unrelated xmin.
  Exercise append and erasure before snapshot acquisition and while replay holds publication,
  plus blob changes during replay to prove one consistent active-blob observation.
- Exercise cancellation during lock wait, replay, commit and unlock, unknown-commit classification,
  eventual driver retirement/capacity recovery and a later writer proceeding. File additionally
  covers process death and reopen; SQLite covers its continuing blocking worker and whole-fold
  visibility after caller cancellation. Refusal never claims a missing runner was executed.
- ER qualification retains initial native capture, exact binding/event/blob validation, independent
  expected-row derivation and final capture comparison. Provider tests cannot substitute for it.

Extend the production roster with every decisive native case and retain selected-zero, skipped,
ignored, malformed and failed-runner refusal. Required gates are affected suites, strict workspace
Clippy/fmt, model validation and the actual bash scripts/gate.sh --production-proof with all real
PostgreSQL17.6/TLS lanes. Design and code examinations precede acceptance. No cutover, source
publication or release is part of this provider story.
