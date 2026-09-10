---
format: aep.planning-md/1
id: story:native-document-queries
kind: story
status: active
title: Query indexed projection documents inside Eventlog transactions
relations:
- depends_on: story:projections-inline-and-catch-up
scope:
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-conformance
- confidence: cited
  path: crates/eventlog-core
- confidence: cited
  path: crates/eventlog-file
- confidence: cited
  path: crates/eventlog-postgres
- confidence: cited
  path: crates/eventlog-sqlite
- confidence: cited
  path: docs/design/document-queries.md
revision: 5
---
## Acceptance
An opted-in PostgreSQL projection supports bounded, tenant-scoped, byte-ordered JSON containment pages both after commit and inside its append transaction, with explicit index migration, read-your-writes and complete rollback on refusal.

## Design
Reuse existing projection JSONB bodies and ProjectionPage, never enumerate and replay streams to filter. Add optional document query methods to EventStore and ProjectionStore, explicitly unsupported by providers without the capability. Preserve existing ProjectionSpec literals and scalar indexes. Projector declares document-query table names as a subset of its projections; PostgreSQL migration adds GIN body and byte-order key indexes only to those tables and records the capability alongside indexed_fields in the existing JSONB migration roster. Hosted registration validates exact roster and physical indexes without DDL. Existing array-form roster entries remain admitted; explicit migration upgrades selected entries to the extended object form. Event rows, feed watermark and blob/privacy boundaries stay unchanged.

The query uses JSONB containment, literal key prefix, exclusive key continuation and bounded page size. A cursor is a storage key, not a query-identity token or snapshot across requests; domain adapters bind it to their query identity. Transaction queries read only inline projections in their own tenant and do not claim predicate locking or serializable capacity enforcement.

## Scope
crates/eventlog-core projection contracts; crates/eventlog-postgres native SQL and schema admission; unsupported-provider registration; shared applicable conformance; focused PostgreSQL integration tests; design documentation and changelog. This is one prerequisite for the ER migration, not a parallel wave or a completed replacement of SQL session APIs. It reuses existing projection/document values rather than introducing a product-domain entity.

## Verification
A real disposable PostgreSQL fixture exercises nested containment, exact numeric comparisons, null versus missing, stable pagination, prefix metacharacters, tenant isolation, own transaction writes, refusal rollback and held feed watermark. Verify old projection admission, opt-in upgrade and missing/wrong index refusal. Prove query/index test sensitivity with restored mutations. Run affected checks only; the operator forbids another full gate.

## Implementation and evidence

Implemented native PostgreSQL containment pages over the existing projection body, shared between EventStore and the append transaction's ProjectionStore. Only explicitly declared document projections gain indexes; existing scalar projection literals and persisted bodies remain unchanged. Migration upgrades the existing roster encoding atomically with the indexes and validates exact physical shape. Hosted registration performs no migration. File and SQLite refuse the optional capability at registration and query boundaries. The transaction read refuses foreign tenants and catch-up projections. Cursor identity binding and serializable cross-stream locking remain adapter/transaction concerns, not claims made by this query API.

Focused restored verification: three PostgreSQL document tests passed in 0.16s; File and SQLite each passed their explicit-refusal exercise. Three schema migration tests, including the previous snapshot/group editions and the new document upgrade, passed in 0.51s. The existing scalar/inline projection exercises passed in 0.27s and the existing paging exercise passed. Removing the SQL containment predicate caused an extra row to appear and failed the shared exercise; removing physical registration comparison let a missing index through and failed admission. Both mutations were restored before final verification. Strict all-target Clippy and Rustdoc passed. A disposable PostgreSQL 17 fixture was used; this is not the hosted TLS/role production proof. No full gate, remote persistence proof, release or main integration was performed.

Evidence: local-evidence:ess-evolution-20260910/eventlog-documents-restored.log; eventlog-documents-schema-final.log; eventlog-documents-scalar-compatibility.log; eventlog-documents-paging-compatibility.log; eventlog-documents-containment-mutation.log; eventlog-documents-admission-mutation.log; eventlog-documents-clippy-verified.log; eventlog-documents-rustdoc.log; eventlog-documents-fixture.txt.

The story remains active until its branch is integrated. Next: consume this native projection capability in the ER Eventlog adapter and complete native transaction-session replacement before retiring SQL storage implementations. This prerequisite does not establish completed ESS evolution or adoption in the consumer.
