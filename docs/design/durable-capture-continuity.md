# Durable capture continuity (SQLite)

Status: accepted for implementation. Source: https://github.com/beyond10x/eventlog/issues/39.
Extends `docs/design/consistent-tenant-capture.md` § *Retained capture continuity*, whose in-process
contract is unchanged.

## Purpose

A consumer that runs one short process per command cannot hold an in-process `CaptureCheckpoint`:
the checkpoint dies with the process, so every command pays a complete capture and a complete
verification whose cost grows with the store. Durable continuity lets a later process ask the
provider whether anything other than its own acknowledged appends changed the tenant's captured
material since a persisted checkpoint, and receive exactly those appends.

## Typed home

The values live in `ess/capture/domains/capture.yaml` (format `ess/23`):
`DurableCaptureCheckpoint`, `CaptureContinuityMark`, `SqliteDurableCheckpoint`,
`JournaledRowChange` and `DurableJournalEntry`. Their JSON Schema projection is committed under
`ess/capture-generated/schema/types/` and regenerated with:

```console
ess generate --path ess/capture --kind schema --out <empty directory>
```

then copying `schema/types/*.schema.json` over the committed files. The Rust types are
hand-written: `ess generate types --target rust` emits a crate that enables
`serde_json/arbitrary_precision`, which Cargo feature unification would turn on for Eventlog and
every consumer and which changes `serde_json::to_vec` output for numbers such as `1.50`. That
changes `request_hash` (`crates/eventlog-core/src/lib.rs`) and therefore every stored idempotency
hash. `story:generate-capture-types-from-ess` records the gap. A test holds the SQLite checkpoint
encoding to the committed schema, and a test pins one `request_hash` value.

## API

Additive. Default trait methods keep every provider compiling unchanged.

```rust
#[derive(Clone, PartialEq, Eq)]
pub struct DurableCaptureCheckpoint(Vec<u8>);

impl DurableCaptureCheckpoint {
    pub fn from_bytes(bytes: Vec<u8>) -> Self;
    pub fn as_bytes(&self) -> &[u8];
}

pub trait ConsistentTenantCapture {
    // existing methods unchanged
    fn durable_checkpoint(&self, checkpoint: &CaptureCheckpoint)
        -> Option<DurableCaptureCheckpoint> { None }
    fn restore_checkpoint(&self, durable: &DurableCaptureCheckpoint)
        -> Option<CaptureCheckpoint> { None }
    fn checkpoint_usage(&self, checkpoint: &CaptureCheckpoint)
        -> Option<CaptureUsage> { None }
}

impl SqliteEventStore {
    pub async fn enable_durable_continuity(&self) -> Result<(), EventLogError>;
    pub async fn disable_durable_continuity(&self) -> Result<(), EventLogError>;
}
```

`Debug` for `DurableCaptureCheckpoint` prints its length, never its bytes.

## Contract

For a restored checkpoint, `capture_tenant_since` returns:

- `Unchanged` only if no write of any kind reached the tenant's captured material (events, blobs,
  stream identity, the requested projection tables) since the checkpoint's observation;
- `AppendDelta` only if every such write was an atomic group this provider's append paths
  acknowledged, by any process, with exactly the delta the in-process path returns (events, blobs
  bound, projection row changes with before and after values, `resulting_usage`);
- `Complete` otherwise.

The observation is the capture the checkpoint was issued with. `durable_checkpoint` encodes the
values read inside that capture's transaction, never values read when it is called, so a foreign
write between the observation and the call is reported, not absorbed. A false `Complete` is always
allowed; a false `Unchanged` or `AppendDelta` never is. The checkpoint returned with any variant is
again eligible for `durable_checkpoint`. `checkpoint_usage` returns the usage bound to a checkpoint
this provider issued or restored: the counts a complete capture of that observation reports.

`restore_checkpoint` returns `None` for truncated, malformed, foreign-provider, other-prefix,
other-store-instance and newer-version bytes, never an error. `durable_checkpoint` returns `None`
on a store where `enable_durable_continuity` has not run, and for a checkpoint taken while a
requested projection table carried no provider trigger.

PostgreSQL, File and Tree keep the defaults (`None`, hence `Complete`).

## SQLite physical design

All names carry the store's table prefix, so owners sharing one file stay independent.

| Object | Shape |
|---|---|
| `{prefix}_capture_continuity` | one row: `instance TEXT` (32 lower-case hex, minted at enable), `epoch INTEGER`, `token TEXT` (32 lower-case hex) |
| `{prefix}_capture_journal` | `position INTEGER PRIMARY KEY`, `tenant_id TEXT`, `from_epoch`, `from_token`, `to_epoch`, `to_token`, `entry TEXT` (a `DurableJournalEntry` as JSON), `bytes INTEGER` |
| trigger per captured table and operation | `AFTER INSERT`, `AFTER UPDATE`, `AFTER DELETE` on `{prefix}_events`, `{prefix}_blobs`, `{prefix}_identity` and every `{prefix}_p_*` projection table, each running `UPDATE {prefix}_capture_continuity SET epoch = epoch + 1, token = lower(hex(randomblob(16)))` |

Trigger names and SQL text are produced by one function; every admission check compares both
exactly. Snapshot, snapshot-generation, command, claim, counter, cursor and registry tables carry
no trigger: they are not captured material.

`enable_durable_continuity` creates the two tables and every trigger in one transaction. It is
idempotent: a second call on an enabled store changes nothing, including the instance. It refuses
a store carrying any trigger the provider does not own. `create_projections` on an enabled store
installs the triggers on each table it creates, in the same transaction. `disable_durable_continuity`
drops every provider trigger and both tables in one transaction; Eventlog 0.7.0 then opens and
attaches the store as before. `open_existing` never creates any of these objects, and provisioning
does not install them.

The three checks that refuse a trigger today (blob table at open, projection table at attach,
strict inspection) and the in-process continuity eligibility check admit exactly the provider's
own triggers and give their present answer for any other trigger. Eventlog 0.7.0 and earlier refuse
a store carrying them; that is why enabling is explicit and reversible.

## Writes

An acknowledged atomic group (`append_group_guarded`, `append_group_guarded_with_blobs`,
`append_group_with_blobs_guarded`) on an enabled store, in its own transaction:

1. after `BEGIN IMMEDIATE` and before any write, reads the mark `(epoch, token)` as `from`;
2. performs its writes, which advance the mark through the triggers;
3. reads the mark as `to` and inserts one journal entry with the group's tenant, its events' global
   positions, the blob digests it newly bound and its projection row changes (before and after
   values, coalesced per key as the in-process journal does);
4. prunes whole entries from the oldest until at most 128 entries and 16 MiB of `bytes` remain.

A deduplicated group with no physical write writes no entry. A refused or rolled-back group writes
nothing. Standalone `append`, `put_blob`, `delete_blob`, projection administration and every other
write path are not journaled: they move the mark without an entry, so the next restored checkpoint
returns `Complete`. `redact` and tenant erasure also delete every journal entry in their own
transaction, because entries hold copies of projection row values.

The provider's own snapshot writes touch no captured table and so end neither durable continuity
nor, after this change, in-process continuity: the snapshot write path re-synchronizes the
in-process stamp inside its own transaction, as an acknowledged group does.

## Restore and continue

`durable_checkpoint` encodes a `SqliteDurableCheckpoint` as JSON: format
`eventlog-sqlite/durable-capture-checkpoint`, version 1, store instance, prefix, scope (tenant,
stream identity, requested projections in order with indexed fields, limits), usage, journal
position, `PRAGMA main.schema_version` and mark, every value read inside the issuing capture's
transaction. The in-process checkpoint carries these values when durable continuity is enabled.

`capture_tenant_since` with a restored checkpoint, inside one `BEGIN IMMEDIATE` transaction:

1. validates the request scope against the checkpoint scope exactly, as for an in-process one;
2. reads instance, mark, schema version and the newest journal position;
3. checks every requested projection table carries the provider's triggers, by name and SQL text;
4. returns `Unchanged` when instance, schema version, mark and position all equal the
   checkpoint's;
5. returns `AppendDelta` when the journal entries after the checkpoint's position start at its mark,
   each entry's `from` equals the previous entry's `to`, and the last `to` equals the current mark.
   Entries of other tenants are links in the chain and contribute nothing to the delta. Events are
   read back by global position; blobs by digest. Usage is accumulated with the existing checked
   accounting and the request's limits apply to the resulting whole observation;
6. returns `Complete` otherwise: a foreign write breaks the chain, a pruned entry leaves a gap, a
   dropped or altered trigger changes the schema version, re-enabling mints a new instance.

A file restored from an older copy and written as many times as the groups it lost carries the
same epoch and position as a newer checkpoint; the random token makes that coincidence a 128-bit
collision.

## Out of scope

Raw file edits that bypass SQLite; a connection that disables triggers or rewrites the continuity
tables consistently; durable continuity for PostgreSQL, File and Tree.

## Acceptance

Real SQLite file; a new `SqliteEventStore` for each step.

- `Unchanged` from a restored checkpoint on an untouched store; an exact `AppendDelta` after groups
  appended by another store instance; `checkpoint_usage` equal to a complete capture's usage.
- `Complete` after each of: an event's data updated, a blob's bytes updated, a blob deleted, the
  stream identity updated, a projection row updated, a projection row inserted, an event inserted
  by SQL, a trigger dropped and recreated, the journal pruned past the checkpoint, an older copy of
  the file restored, and an older copy restored and then written as many times as it lost.
- `Complete` after a foreign write between the observation and the `durable_checkpoint` call.
- `None` from `restore_checkpoint` for truncated, foreign-provider and newer-version bytes, and from
  `durable_checkpoint` before `enable_durable_continuity`.
- `enable_durable_continuity` twice changes nothing the second time, refuses a store carrying a
  foreign trigger, and after `disable_durable_continuity` the store opens and attaches under 0.7.0.
- On an enabled store, a projection created after enable carries the triggers, so a foreign edit to
  it gives `Complete`; a checkpoint taken while a requested projection table carries no provider
  trigger, restored after a foreign edit to that table, gives `Complete`.
- Continuity kept across the provider's own snapshot writes, durable and in-process.
- Redaction and tenant erasure leave no journal entry behind.
- The encoding of a `SqliteDurableCheckpoint` satisfies the committed generated schema.
- `request_hash` of a pinned body containing `1.50` equals its recorded value.
