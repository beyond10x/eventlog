# File provider v1

`eventlog-file::FileEventStore` implements `EventStore` and `AtomicEventStore` over a local
directory. It requires Tokio and a local filesystem with working process locks, atomic rename,
file synchronization and directory synchronization. The current acceptance platform is Linux.
Network filesystems, concurrent Git checkout/merge while a store is open, hardware power-loss
certification and production capacity are not established by these tests.

## Authority and commit

`manifest.json` selects the committed byte length, transaction sequence, digest, store identity
and privacy epoch of `events.jsonl`. Both files are authority. Removing a manifest from an existing
log refuses; it never initializes a replacement history. Each JSONL frame carries a version,
sequence, previous digest and one ordered transaction. SHA-256 covers the canonical serialized
frame with an empty digest field. Unknown physical fields, wrong sequence/previous digest,
missing committed bytes and damaged committed frames refuse without altering the history.

`writer.lock` is permanent and outside the disposable cache. Each operation opens its own lock
file description, takes an exclusive process lock, verifies the journal and folds its operations,
then retains that lock through callbacks and commit. Never unlink the lock file while a process
can use the directory. All writers serialize, including absent streams and reverse-order groups.
Callbacks access blobs and projections through their transaction context. Re-entering the outer
store from a callback is unsupported, as with the SQL providers.

Before append, the provider synchronizes `append.json`, which records the exact planned frame
and the before/after commit coordinates. It writes and synchronizes the frame, replaces the
manifest atomically, synchronizes the directory, and removes the temporary intent before success.
On reopen, an intent plus an exact matching frame prefix proves an uncommitted tail can be
truncated. A complete frame that had not reached manifest publication is also uncommitted.
An unexplained suffix, including concatenated branch histories, refuses. There is no merge driver.
An error after manifest publication can report `UnknownCommit`; resolve the original identity.

An open handle checks that every subsequent history extends the exact head it observed. A
longer fork is not accepted merely because its sequence increased. Privacy starts a new epoch;
other already-open handles must reopen after that operation. Reopening a completely replaced
store cannot establish its ancestry without an external trusted checkpoint. Digests detect
integrity errors; they are not signatures against an actor authorized to rewrite all authority.

## Transactions and derived state

One frame commits events, command/group/claim coordinates, guard reservations, inline projection
writes and required bookkeeping together. Group entries execute in request order, including
repeated streams. A callback or expectation failure publishes nothing. Retries read original
coordinates against current history, so redacted events are not resurrected from receipts.
No independently retryable child identities are created for group entries.

Host admission permits retain their in-process identity and tenant/deployment distinction.
A failed multi-counter reservation changes nothing even if the guard catches its error.
Projectors cannot use guard-only reservation authority. Registration freezes before append.
Every process must install the same required inline projectors before serving, as for SQL hosts.

Indexes are rebuilt in memory from the journal. Projection writes and cursors are recorded in
transactions; the domain event history remains available for an explicit complete projection
rebuild. A rebuild commits all rows and its cursor together. Snapshots live under `.cache`, carry
a persisted history-generation coordinate, and may be deleted or discarded when unreadable.
They are never a substitute for history and unproven snapshot writes refuse.

The v1 implementation rereads and verifies history per operation. This deliberately has no
production throughput or large-history claim. A later accelerator must preserve the same
verification and disposal contracts.

## Content and privacy

Blob bytes live separately in `blobs/`; journal entries contain a tenant/digest binding, object
identity and content hash. Bytes and their directory entry are synchronized before the binding
commits. Reusing a binding for different bytes refuses. Reads and reopen verify active content
hashes. Deletion and tenant erasure remove unreferenced objects from the active directory.
The public digest spelling retains the existing blob-port compatibility; an additional computed
SHA-256 verifies the actual stored bytes.

Redaction and tenant erasure are the only history-rewrite paths. Redaction preserves event IDs,
stream versions and feed positions while replacing the selected body with a tombstone. Tenant
erasure removes its events, receipts, claims, identities, generations, projection rows and blob
bindings. Persisted watermarks prevent feed positions from being reused after erasure.

Privacy writes and synchronizes a sanitized replacement journal, then publishes `privacy.json`
with the exact old/new manifest and replacement digest. Recovery accepts only those two
validated versions and completes the intended replacement. It never falls back to the old body
after the durable privacy intent. The manifest switches atomically and temporary files are
removed and synchronized. Reopen also cleans stale snapshots and unreferenced blob objects,
including a process death before post-commit cleanup finished. A successful privacy call has
removed those obsolete active-store bytes; separately retained Git history and backups remain
archives outside this provider's erasure scope.

Projection rows can contain copies of event bodies. Redaction therefore removes that tenant's
old row writes and cursors, and durably invalidates its registered views. Reads and new appends
requiring those views refuse until each projector completes `rebuild_projection` against the
sanitized events. Existing command/group retries still resolve their durable receipts. Rebuild
failure leaves the invalidation in force. This is an explicit file-provider privacy rule; it
prevents an emptied read model from silently admitting a command against incomplete counters.
Unrelated tenants and unrelated snapshot generations remain intact.

## Evidence

`tests/conformance.rs` invokes eleven unchanged shared contracts. `tests/durability.rs` checks
reopen, original retry identities, cache deletion, independent process writers, conflicting
expectations, blob corruption, missing authority, physical privacy and projection invalidation.
Journal unit tests kill subprocesses at prepared, torn-write, synchronized and published append
boundaries, and at prepared, renamed and published privacy boundaries. They also check that
corruption and unexplained tails are preserved on refusal and that a longer fork does not extend
a previously observed head. These are process-death tests, not simulated drive power failure.
