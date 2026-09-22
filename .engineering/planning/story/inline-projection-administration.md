---
format: aep.planning-md/1
id: story:inline-projection-administration
kind: story
status: draft
title: Attach existing inline projections and rebuild them atomically
relations:
- serves: vision:O2
- depends_on: story:consistent-tenant-capture
- depends_on: story:sql-blob-read-integrity
scope:
- confidence: inferred
  path: crates/eventlog-conformance/src/inline_admin.rs
- confidence: cited
  path: crates/eventlog-conformance/src/lib.rs
- confidence: inferred
  path: crates/eventlog-core/src/inline_admin.rs
- confidence: cited
  path: crates/eventlog-core/src/lib.rs
- confidence: cited
  path: crates/eventlog-file/src/lib.rs
- confidence: inferred
  path: crates/eventlog-file/tests/inline_admin.rs
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof/required.rs
- confidence: inferred
  path: crates/eventlog-postgres/src/atomic_group.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/src/schema.rs
- confidence: inferred
  path: crates/eventlog-postgres/tests/inline_admin.rs
- confidence: inferred
  path: crates/eventlog-sqlite/src/atomic_group.rs
- confidence: cited
  path: crates/eventlog-sqlite/src/lib.rs
- confidence: inferred
  path: crates/eventlog-sqlite/tests/inline_admin.rs
- confidence: inferred
  path: docs/design/inline-projection-administration.md
- confidence: cited
  path: ess/inline-admin/domains/inline-admin.yaml
- confidence: cited
  path: ess/inline-admin/system.yaml
revision: 3
---
## Outcome

Provide explicit validate-only attachment and atomic rebuild of already admitted inline
projections across File, SQLite and PostgreSQL. These provider capabilities are required by the
recorded ER adapter's nonmutating open and deterministic derived-index recovery. Eventlog owns
provider admission, snapshot and transaction behavior; it must not depend on ER or encode its
four application-specific row schemas.

## Existing typed authority and demonstrated gaps

Use existing TenantId, Projector, ProjectionSpec, ProjectionStore and EventLogError contracts.
The concrete consumer requirement is entity-runtime@ae5f3c7852627abe7ca85bcf647f3fee27c5370b,
docs/design/eventlog-recorded-indexes-v0.1.md, especially admission and explicit rebuild sections.
This references the existing types and accepted Eventlog18322cbe APIs; it does not invent a new
domain registry, arbitrary evidence envelope or Eventlog-to-ER dependency.

Current register_inline admits/persists schema rather than merely attaching code. Current SQL
rebuild rejects inline projectors; its shadow context can resolve blobs through the wrong namespace,
and PostgreSQL catch-up replay covers a watermarked eligible prefix instead of the required complete
committed history. Root verified these source facts while selecting the ER index contract; exact
paths and implementation dependencies require the bounded scoping step before design/code dispatch.

## Acceptance

- Add an explicit object-safe async attach_inline_existing capability that validates exact already
  admitted registry/physical projection shape and installs the given code in memory only. Missing,
  incompatible or duplicate admission refuses without DDL, journal writes, lazy initialization or
  recovery. Registration locking and serving freeze apply on every provider. Attachment preserves
  provider dirty markers and existing use/capture refusals; it makes no cleanliness or freshness
  claim. Consumer readiness requires native capture and exact independent row comparison.
- Add explicit async inline rebuild selecting the actual registered projector by its name. Hold
  exclusive provider publication coordination, read the complete committed tenant event/live-blob
  authority, and fold events in increasing actual global position into isolated shadow row sets.
  Blob resolution uses authoritative active blob storage while row reads/writes use shadow state.
- Publish the complete replacement row sets atomically, preserving inline registration and other
  tenants/projectors. Return actual count/final authoritative position. Decode/projector/blob
  failures, interruption and cancellation cannot expose partial rows or falsify completion.
  Preserve the existing provider uncertainty and connection-retirement contracts.
- Preserve existing eventlog-file/1 meaning and canonical bytes. Any newly required persisted
  field/operation must receive an explicit format/compatibility decision before implementation.
  Existing pagination and watermarked catch-up remain distinct from complete snapshot proof.
- Qualify read-only refusal, full replay beyond the old watermark, active-blob/shadow-row separation,
  atomic all-table publication, concurrent publication/erasure, malformed/missing content,
  cancellation/crash/reopen and registration freeze on each real provider. Keep decisive fault
  mutations, selected runner counts, exact exits and the full existing production gate.

## Dependencies and boundary

Depends on qualified consistent-tenant-capture and SQL integrity source. Their unfinished evidence
remains blocking; this new owner does not reopen or retry a rejected review, clear a blocker or
authorize source implementation on an unaccepted base. Scoping and concrete provider design can
proceed read-only. Implementation requires the reviewed design and exact accepted dependency pins.
Operational administrative exclusion remains a caller obligation, not an input-flag proof.
No external deployment, real-store cutover or new credential authority is included in this story.

## Scope

Derived 2026-09-15 by `story-scoper`. Every line is **cited** (read from the story or the frozen
trees) or **inferred** (a prospective landing that the binding design may change).

- **Primary surface:** `crates/eventlog-core/src/lib.rs`; `crates/eventlog-file/src/lib.rs`; `crates/eventlog-sqlite/src/lib.rs`; `crates/eventlog-postgres/src/lib.rs` — **cited**
- **Provider admission helper:** `crates/eventlog-postgres/src/schema.rs` — **cited**
- **Shared conformance and production roster:** `crates/eventlog-conformance/src/lib.rs`; `crates/eventlog-postgres/examples/production-proof/required.rs` — **cited**
- **New core capability:** `crates/eventlog-core/src/inline_admin.rs` — **inferred**
- **New shared contract:** `crates/eventlog-conformance/src/inline_admin.rs` — **inferred**
- **Projection-context constructor collisions:** `crates/eventlog-sqlite/src/atomic_group.rs`; `crates/eventlog-postgres/src/atomic_group.rs` — **inferred**
- **Native qualification targets:** `crates/eventlog-file/tests/inline_admin.rs`; `crates/eventlog-sqlite/tests/inline_admin.rs`; `crates/eventlog-postgres/tests/inline_admin.rs` — **inferred**
- **Binding provider design:** `docs/design/inline-projection-administration.md` — **inferred**
- **Transient typed result:** `ess/inline-admin/system.yaml`; `ess/inline-admin/domains/inline-admin.yaml` — **cited**, two-file model validates through the ESS CLI
- **Symbols:** `InlineProjectionAdmin`, `InlineRebuildResult`, `attach_inline_existing`, `rebuild_inline_projection`, `EventStore`, `Projector`, `ProjectionSpec`, `ProjectionStore`, `TenantId`, `EventLogError`, `FileEventStore`, `SqliteEventStore`, `SqliteProjections`, `PostgresEventStore`, `PostgresProjections`, `schema::validate_projection`, `publication_gate`, `watermarked_source` — **cited**
- **Confidence:** medium — **cited** provider landing surfaces and consumer semantics are exact, while trait placement, helper factoring and new test filenames await the required binding design
- **Would collide with:** core capability exports, File/SQLite/PostgreSQL provider administration, PostgreSQL projection-shape validation, SQL projection-context construction, shared conformance exports and the provider-qualified production roster — **cited**

## Selected provider design, pending review

The bounded scope reads accepted Eventlog 18322cbe19f0068e9b3fab84d874d6abea181f3e and
the consumer contract at entity-runtime ae5f3c7852627abe7ca85bcf647f3fee27c5370b.
No provider source eligibility or prerequisite acceptance follows from this scope.

The proposed binding design is docs/design/inline-projection-administration.md. Structural attach
has no TenantId and returns unit; rebuild selects the exact registered instance by name and returns
InlineRebuildResult { applied: u64, position: u64 }. The two-file ESS model describes those transient
counts without a persisted entity, code identity or format. It validates through
`ess specify validate --path ess/inline-admin`: eventlog v1, two files, valid.

The original attach-refuses-dirty wording is corrected explicitly. File's persisted tenant dirty
markers outlive its process-local registration, so that wording cycles with requires-attached
rebuild after restart. SQL has no corresponding marker. Attachment now preserves markers and their
existing refusal; it never attests freshness. ER still requires native capture and exact independent
row comparison before serving. Capture already selects RedactedHistory refusal uniformly, including
SQL; its implementation is still missing. ER intentionally refuses redacted authority before
rebuild and does not reconstruct it from incomplete content. No new dirty-state persistence is added.

The design retains the existing error variants and provider uncertainty. It uses capture's strict
File reader and non-DDL SQL validators, separates active blob from shadow row coordinates, and
selects a fresh PostgreSQL repeatable-read transaction after exclusive session publication lock.
All committed tenant events are replayed, including those hidden by the ordinary feed watermark.
The consumer's earlier Arc-taking proposal must adopt the name-taking seam before implementation.

This story remains draft and depends on qualified capture and SQL integrity source. Scoping is
complete; the provider design is proposed and needs independent examination. Implementation,
mutation evidence, the exact accepted dependency pins and all-provider qualification remain.

## First provider design review corrections

Recorded review-result:inline-projection-admin-design-pass-1 has one blocker and two warnings.
All three changed the proposed binding design. PostgreSQL structural attachment now explicitly
uses REPEATABLE READ, READ ONLY for all registry/catalog reads. Rebuild holds the provider's
registration/freeze coordination for the full operation, with concrete native lock ordering;
SQLite std mutex guards stay inside the existing owned blocking worker. Qualification now includes
pending append and pending privacy intent refusal through both File administration paths, exact
unchanged canonical bytes, and the registration/rebuild race including cancellation.

The original report and findings remain immutable. These are proposed corrections pending the
second/final design review, not provider execution evidence. Captured authority, complete row
comparison, prerequisite qualification and the consumer seam adoption remain required.

## Final provider design review

Review-result:inline-projection-admin-design-pass-2 approves the complete corrected design with
an empty findings block. The first three findings are resolved; no third design review is required.
The binding design is accepted as a design only. The implementation dependencies remain open and
the owner remains draft. Provider source, all native qualification, full production gates and the
consumer seam adoption are not supplied by this review. The root coordinator carries the selected
structural attachment and registered-name rebuild seam into the ER consumer contract separately.

