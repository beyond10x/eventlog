# Atomic append groups

Decision: architecture-decision-record:ess-evolution-05-file-and-atomic-groups.
Implementation owner: story:atomic-append-groups.

## Public port

AtomicEventStore extends EventStore without adding a mandatory method to existing implementors.
There is no default implementation that loops over EventStore::append. Select a provider that
implements the capability explicitly.

AppendGroup contains a tenant, ordered StreamAppend entries and one CommandMeta. StreamAppend
contains the existing StreamId, Expected and NewEvent values. GroupRange is durable bookkeeping
over original stream/version coordinates; it stores no second event-body copy.

An empty group, an empty entry, crossed tenants or invalid existing input is refused before
transaction mutation. Stream-scoped CommandMeta.claim cannot represent several result ranges and
is explicitly refused. The group itself supplies the tenant-scoped idempotency identity.

The fingerprint covers actual ordered entries, expectations, event names/schema/data, tenant and
all metadata, including the caller's digest. Keeping a claimed request_hash unchanged cannot hide
changed group content. The group identity namespace is separate from single-stream command and
claim namespaces. A group has no independently retryable child command.

Retries retain original event IDs, positions and ranges and mark the response deduplicated.
Referenced events remain subject to normal active-store redaction; receipts do not preserve a
second unerased copy. Tenant erasure removes group bookkeeping in the same transaction.

## Transaction semantics

SQLite takes BEGIN IMMEDIATE once and applies all entries through the existing append engine.
Its database writer lock serializes independently opened connections and processes.

PostgreSQL takes the existing publication gate first, then the tenant/group-identity lock, then
all participating stream locks in sorted StreamId order. It applies entries in request order,
including repeated streams. A later Exact expectation sees earlier appends in the transaction.
The existing event writer and required inline projectors are shared by both append APIs.
The group commit uses the existing UnknownCommit boundary and connection quarantine protocol.

append_group_guarded runs one group admission guard before the ordered appends. Its transactional
writes and all inline projection writes roll back if any entry, projection or bookkeeping write
fails. Retry bypasses both admission and projection application.

ProjectionStore::get_blob reads within the current tenant and transaction. A callback does not
re-enter EventStore::get_blob while the outer connection or transaction is held. Implementors
without this capability return an explicit refusal through the additive default method.

## Migration

SQLite adds the append_groups table. PostgreSQL admits its complete physical shape and includes
it in hosted DML permission checks and additive schema checksums. Both the pre-snapshot and
snapshot-provenance schema editions remain migratable; the existing base DDL and checksum
functions remain frozen. Partial layouts, unknown checksums and foreign group-table shapes refuse.

Fence old writers and erasers during rollout: an old binary does not know group bookkeeping.
Keeping historical data readable is not permission to mix old and new mutation protocols.
Register projectors and grant the new hosted table's DML permissions before traffic.

## Evidence boundary

Shared assertions exercise same-stream expectations, request order, actual-content retry conflicts,
guard/projector rollback, tenant-scoped blob reads and group/legacy command identity separation.
Both providers run simultaneous reversed-order groups and duplicate identities; SQLite additionally
uses independent file connections and verifies reopen retry. PostgreSQL tests both additive schema
migration and foreign physical-shape refusal.

The file provider, ER recorded executor and application migrations remain separate prerequisites.
This implementation does not claim a file provider or a deployed service.
