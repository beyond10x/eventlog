---
format: aep.planning-md/1
id: story:consistent-tenant-capture
kind: story
status: implemented
title: Capture complete tenant history, content and projections consistently
owner: Astra
refs:
- provider: plan
  reference: ess-evolution-20260915
relations:
- serves: vision:O2
- depends_on: story:sql-blob-read-integrity
scope:
- confidence: inferred
  path: crates/eventlog-conformance/src/consistent_capture.rs
- confidence: cited
  path: crates/eventlog-conformance/src/lib.rs
- confidence: inferred
  path: crates/eventlog-core/src/capture.rs
- confidence: cited
  path: crates/eventlog-core/src/lib.rs
- confidence: inferred
  path: crates/eventlog-file/src/capture.rs
- confidence: cited
  path: crates/eventlog-file/src/journal.rs
- confidence: cited
  path: crates/eventlog-file/src/lib.rs
- confidence: cited
  path: crates/eventlog-file/src/projection.rs
- confidence: cited
  path: crates/eventlog-file/src/state.rs
- confidence: inferred
  path: crates/eventlog-file/tests/consistent_capture.rs
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof/required.rs
- confidence: inferred
  path: crates/eventlog-postgres/src/capture.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/src/pool.rs
- confidence: cited
  path: crates/eventlog-postgres/src/schema.rs
- confidence: inferred
  path: crates/eventlog-postgres/tests/consistent_capture.rs
- confidence: inferred
  path: crates/eventlog-sqlite/src/capture.rs
- confidence: cited
  path: crates/eventlog-sqlite/src/lib.rs
- confidence: inferred
  path: crates/eventlog-sqlite/tests/consistent_capture.rs
- confidence: cited
  path: docs/design/consistent-tenant-capture.md
- confidence: cited
  path: ess/capture/domains/capture.yaml
- confidence: cited
  path: ess/capture/system.yaml
revision: 10
---
## Outcome

Read one tenant's complete history, all live bound content and exact requested materializations
under one native File/SQLite/PostgreSQL consistency boundary, with explicit finite content caps
and typed no-value refusals. A read-only file entry permits inspection without initialization,
recovery or cleanup. The consumer still proves its own record/materialization equivalence.

## Authority and typed home

Approved ESS evolution revision1, plan ess-evolution-20260915, SHA256
7579145c3de5a1c6f8088fd7fb804d29dac8903ec505048f3ce595c45023b787; Astra selects this bounded
provider prerequisite under the existing delegation. Binding proposal:
docs/design/consistent-tenant-capture.md. Minimal transient coordinate/limit values are declared
in ess/capture/system.yaml and domains/capture.yaml; validation exited0 before this story's
creation. These are values, with no invented persistent entity, lifecycle or projection format.
Existing RecordedEvent, ProjectionSpec and provider identity/blob types remain their typed homes.

## Acceptance

- Implement separate object-safe ConsistentTenantCapture on all three providers; no default,
  pagination fallback, callbacks or partial success. Return every tenant event in position order,
  live blob binding including orphans in digest order, and requested projection rows in key order.
- Read existing identity without creating it; distinguish empty provisioned and missing tenants.
  Refuse all redacted history, even without requested projections and after rebuild, without
  returning stale materialized or bound content. Preserve existing ordinary APIs and file bytes.
- Apply exact explicit event/blob/total-row/payload caps with checked arithmetic, zero semantics
  and no clamping. Use bounded SQL accumulation, actual complete-content integrity validation and
  exact read-only projection registry/physical-shape admission. Report lagging rows honestly.
- Use the strict existing-file reader for FileTenantCapture::open and capture, retaining the
  existing interprocess lock and extension guard. Pending append/privacy intents refuse without
  initialization, recovery, cleanup or stored-file changes. Missing store files never initialize.
- SQLite uses its mutex plus BEGIN IMMEDIATE across separate handles. PostgreSQL quarantines its
  lease and acquires the existing exclusive session publication lock before a fresh repeatable-read
  read-only transaction; it reads complete history without the ordinary feed watermark, explicitly
  verifies unlock before reuse, and retires unsettled sessions on failure/deadline/cancellation.
- Prove actual provider ordering against append/rebuild/erasure, all finite bounds, absent/invalid
  identity, cross-tenant isolation, orphan and corrupt blobs, projection refusal/lag cases, reverse
  PostgreSQL XID/position plus unrelated xmin, and recovered lock/pool capacity after cancellation.
  Preserve causal controls and no-write file inventory checks. No constructor/fixture-only proof.
- Extend the required provider-qualified roster and pass actual production-proof with all required
  cases selected and PostgreSQL17.6/TLS, affected suites, fmt and strict workspace Clippy. Design
  review and independent code examinations precede acceptance; no narrow result is the full gate.

## Scope

Existing core exports; File lib/journal/state/projection; SQLite lib; PostgreSQL lib/pool/schema;
shared conformance exports and required production roster. Inferred capture modules/provider
integration tests are separate machine-readable entries. Root owns design/model/AEP/roster and
shared patches. This overlaps SQL-integrity provider source and must compose on its final accepted
source. No concurrent provider implementation is authorized while that dependency is unresolved.
Source inspection was fresh-context Sol high; the root proposal corrects two source-scope
assumptions: File's normal opener writes, and SQL integrity adds no redaction dirty marker.

## Dependencies and limits

Depends on story:sql-blob-read-integrity. The corrected submission has actual production165/0/0
but its second independent examination was interrupted by the platform; this draft's provisional
source base is not acceptance. One owner is drafted, not a multi-story decomposition, so a four-lane
decomposition panel adds no set to assess; an independent technical design review remains required.
This unit exports selected logical material, not all Eventlog physical receipts/counters/caches.
Consumer adapter/bridge, provider group/crash qualifications, actual planning-store cutovers and
the one AEP migration remain required under the full initiative. No publication or release.

## Design review resolution

First technical review is recorded verbatim as review-result:consistent-tenant-capture-design-pass-1.
The binding design now orders missing/invalid identity before redacted-history refusal within the
validated observation, explicitly including append without identity followed by redaction.
Stored identities preserve any nonempty UTF-8 string exactly, including legacy values, without a
UUID, trimming or normalization rule; empty/undecodable identities refuse as corruption.
Projection keys preserve every UTF-8 string admitted by existing writers, including empty,
whitespace, Unicode and provider-admitted embedded NUL. No new key grammar is introduced.
Acceptance includes native cases for each precedence and compatibility boundary. These three
clarifications introduce no persisted format or provider write-path change. A second technical
design review remains required; the unresolved SQL integrity dependency still prevents implementation.

## Accepted design boundary

Second technical review approved the revised design with an explicit empty findings list, recorded
verbatim as review-result:consistent-tenant-capture-design-pass-2. The findings ledger carries zero,
adds zero and resolves all three first-pass findings. The only later design edit marks this design
acceptance; it changes no contract semantics or model. No third design pass is required.

This is design readiness, not code or provider acceptance. The SQL blob-integrity dependency still
lacks its required second code examination; implementation remains unadmitted until that dependency
is accepted. Preserve actual production-proof and code-examination requirements for future source.

## Implementation admission after SQL acceptance

SQL integrity is implemented, verified and integrated at
8746a693c6084ab516170278a72c6c8cf5c8e46c, tree f8a283f2f3e969370b3045b53e4eccf72c1706ee.
Both full normal and production gates exited zero against actual PostgreSQL17.6/TLS; common
receipt verified, corrected source hashes identical in integration. The existing supplied second
SQL examination's finding was corrected. Its historical denial remains; no new SQL review.
This supersedes only stale dependency statements above and in the accepted design: capture is
now admitted for implementation, with its full acceptance and two completed design passes intact.

Canonical planning continues from the accepted SQL lineage. The same two provider story identities,
relations, scopes and all four immutable design report bodies were replayed through AEP from
ba0c09874221a659df06894ec4581e5e2fd755a6; their original journal and checkout remain preserved.
No journal merge, direct store edit, restarted design pass or new owner is introduced.

Assignment: complete native consistent tenant capture on all three providers, shared conformance
and required roster, within the existing story scope. Administration is still dependent on qualified
capture. Worker owns only source/tests and roster; root owns planning/design/model and integration.
Exact commands, write paths and stopping condition are recorded in the coordinator's capture
implementation brief. Required provider checks, causal controls and independent source examination
remain mandatory. No real store cutover, publication or release.

## First source examination correction contract

First source examination is closed on 5b4d4f83. Corrected reviewer execution reports211passed,
3failed,0ignored; fmt and strict Clippy pass. The initial four SQL prefix failures were fixture
faults and are retained separately. SQL8746 remains accepted and integrated.

M1 capture corruption and truthful-cap requirements block acceptance: non-TEXT SQLite keyset
coordinates can repeat, and both SQL preflights trust corrupt stored blob lengths as a cap proof.
Correction covers these classes with bounded actual-length observation and incremental reads,
plus the existing File handle-guard precedence clause. Preserve immutable reviewer cases and all
legal stored keys. Clarify Dirty's current unreachability under redaction precedence; do not expand
PostgreSQL support or relax schema admission based on an untested version hypothesis.

Deliver source and regression fixes, actual red/restored-green evidence and both native gates with
real PostgreSQL/TLS. Stop after this complete bounded correction; no inline administration or new
design review. Root freezes, records and uses the one remaining source examination2of2.
Evidence: local-evidence:ess-evolution/waves/0011-provider-capture/source-review-1/report.md and
source-correction-1-brief.md. No broader provider completion or real-store cutover is inferred.

## Final source examination and correction

Final SOURCE2of2 at d15672f4f3497ad839c1fcff9536664d49aea513 is closed. Immutable original
review-result:consistent-tenant-capture-source-pass-2 records four actual assertion failures across
three classes;106 other affected cases pass and final reviewer Clippy/fmt pass. Raw evidence is
local-evidence:ess-evolution/waves/0011-provider-capture/source-review-2/.

Although ordinary kit writers do not construct these stored states, the accepted capture contract
requires their refusal: strict pending-intent entry handling, exact physical-shape admission and
complete malformed owned-coordinate detection. Root accepts the bounded correction of those
classes under this owner. The original report remains unchanged. Both source reviews are now
closed; no third review or SQL review is scheduled. Final corrected gates/common verification and
local integration remain required; administration still requires qualified capture.

## Final acceptance and local integration

Complete native capture is implemented, verified and integrated locally at
 d016adb0c1f8177657c64d00e9e7bdff80bd1d5b, treef1298d1a3b9f9c0947eff4eadcf7f0c4a8f871f8.
Both design and source review budgets are closed. Final three reported corruption/shape/path
classes are fixed, immutable reviewer assertions retained and actual causal reds/restored greens
recorded. Both required repository gates pass; production proof ran PostgreSQL17.6/TLS with
224passed,0failed/skipped/missing. Signed common receipt is verified, final hashes match source
submission and clean fast-forwarded canonical source integration.

Exact receipt: local-evidence:ess-evolution/waves/0011-provider-capture/source-correction-2/integration.md.
This closes capture and releases the existing administration dependency. It does not close all
provider acceptance, ER adapter/migration acceptance, any real cutover, or authorize publication.
No third review was commissioned and no denied SQL work was retried.
