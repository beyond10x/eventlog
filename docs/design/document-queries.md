# Indexed projection document queries

PostgreSQL can filter an existing projection's JSONB bodies and return a bounded page of
`(row key, body)` pairs. `EventStore::projection_query` reads committed projection state;
`ProjectionStore::query_documents` reads an inline projection inside the current append, including
that transaction's preceding writes. Neither enumerates streams nor folds event histories to filter.
The transaction variant refuses another tenant and a catch-up projection. It does not lock the
predicate: invariants requiring serialization still need the existing identity/counter lock.

## Declaration and migration

`ProjectionSpec` retains its original two fields and scalar lookup behavior. A projector opts in
selected table names through `Projector::document_projections`; each name must occur exactly once
in that list and belong to its declared projections. File and SQLite explicitly refuse this
capability at registration and through the default query methods. They do not emulate an indexed
query by scanning rows. PostgreSQL's isolated constructor migrates declared projections during
registration, as before. Hosted applications run
`PostgresEventStore::migrate_with_document_projections(config, options, projections, names)` with
the migration role before registering the projector with the DML-only application role.

The upgrade adds a GIN `body jsonb_path_ops` index and a B-tree `(tenant_id, row_key COLLATE "C")`
index on each selected existing projection table. No event table, projection body, column or
persisted event format changes. These indexes cost disk and write work only for opted-in tables;
GIN does not imply every predicate is selective, and PostgreSQL chooses its plan normally.
Empty-object containment in particular need not benefit from the GIN index.

The existing projection registry's JSONB `indexed_fields` value retains its array encoding for
scalar projections. Opted-in rows use `{"fields":[...],"documents":true}`. Admission accepts only
those exact encodings and the matching declared scalar paths. Migration atomically upgrades the
roster and indexes; hosted registration compares their exact physical shape, including operator
classes, collation and valid/ready state. A checksum or roster entry cannot substitute for a
missing index. Repeating a scalar migration preserves an existing document opt-in. No downgrade
or implicit removal is provided: fence old binaries before upgrading a projection they register,
because their old roster reader cannot admit the extended encoding. Large index builds belong in
an explicit migration window; migration does not promise concurrent index creation.

## Query semantics

`ProjectionQuery.matching` uses PostgreSQL JSONB containment. Objects are recursively partial
matches; arrays ignore order and duplicate multiplicity. Numeric comparison preserves JSONB numeric
values rather than converting them to floating point during filtering. A null property is distinct
from an absent one. Owners remain responsible for valid, privacy-safe projection values and for
any precision already lost before the JSON reaches the store.

`prefix` restricts keys literally: `%` and `_` are not wildcards. The prefix itself is excluded,
matching the existing projection-page contract. `after` is an exclusive row key. Both filtering
and ordering use UTF-8 byte order, independent of the database's default deterministic collation.
The ordinary read bound clamps `limit`; zero selects one row. The query fetches one extra match
to return a continuation only when more exist, using the last returned key rather than the extra
row. The continuation is a storage coordinate. A domain adapter must bind it to the entire query
identity before exposing it as an application cursor. Separate calls do not share a snapshot;
concurrent inserts before the cursor require a fresh query.

Inline queries see committed writes even while an unrelated transaction holds back Eventlog's
cluster-wide feed watermark. Catch-up queries retain the runner's lag. Feed, redaction and
snapshot protocols are unchanged. Projection rebuild continues to copy the admitted table and
indexes and atomically replace derived rows; no second source of truth is introduced.

## Evidence boundary

The applicable shared document exercise covers nested containment, large adjacent integers,
null versus missing, byte ordering, literal prefixes, bounded keyset paging, tenant isolation,
scalar lookup compatibility, transaction read-your-writes and refusal rollback. PostgreSQL-specific
tests cover explicit migration, physical index admission and held-feed visibility. These are
focused capability checks; they do not certify a deployment budget, full hosted TLS/role proof,
ER transaction-session replacement or completion of the ESS evolution migration.
