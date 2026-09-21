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
file description, takes an exclusive process lock, establishes the committed history it will work
against, and retains that lock through callbacks and commit. A resumed handle that cannot establish
its history under that lock is the one exception: every fall-through in `Journal::resume` drops its
lock description and `Journal::open_existing` takes a fresh one, which re-reads and re-validates
everything before the operation proceeds. Never unlink the lock file while a process
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

A handle verifies the complete history and every active blob once, when the store is opened: every
frame is rechained from the zero digest to the manifest digest, every active object is read and
hashed, and stale snapshots and unreferenced objects are disposed of. The opener also keeps a
SHA-256 over the raw committed bytes it read. Each later operation takes the lock, reads
`manifest.json`, and re-reads the committed bytes behind the head it observed and hashes them
against that value. That raw pass is what makes it safe for a handle to trust frames it is not
decoding again: a committed frame damaged in place after the handle verified it fails the
comparison, so the handle neither serves it nor appends after it. When the manifest names the same
store, epoch, sequence, byte length and digest the handle observed and the committed bytes are
still the bytes it verified, the handle reuses the frames and the fold it already has, decoding no
frame and reading no blob. When it names a longer history on the same store and epoch, the handle
reads only the bytes past its observed length, chains them from its observed digest to the new
manifest digest, folds those frames onto the state it had, verifies the objects those frames bind,
and disposes of snapshots they retired. A resumed operation also removes the staging names no
durable intent selected, as a complete open does. Anything else — a new epoch, a pending recovery
intent, a shorter or unchained file, a byte length that disagrees with the manifest, a committed
prefix that is not the bytes this handle verified, or a head this handle never folded — falls back
to the complete reread, which refuses a history that does not extend the observed head exactly as
before. So an operation costs one raw pass over the committed bytes plus what the file gained
since the handle last looked: no frame is decoded twice and no object is read twice.

A consistent tenant capture reuses the same verified history, under the same rules and one
restriction. A handle that has already observed the committed head re-reads and hashes the
committed bytes behind it, folds only the frames past it, and answers the per-handle divergence
guard from that comparison rather than by re-encoding the observed prefix: what the comparison
establishes — the committed prefix is byte for byte the history this handle verified, and the
frames past it chain from the head it observed — is strictly more than the guard asks. Everything
the strict reader refuses before it decodes, the resumed reader reaches too, by handing the
decision back to it: a writers' lock that is not a regular file or cannot be held, a reserved
`append.json` or `privacy.json` entry — present whenever the directory entry is, even when
following it reaches nothing — a manifest on another store or epoch, a file whose length is not
the committed length, a committed prefix that is no longer the bytes the handle verified, or a
tail that does not chain. The restriction is the one that separates a reader from a writer: the
resumed reader removes no staging name and synchronizes no directory, because an inspector that
mutates a store has changed the thing it came to observe. Its cached view is a reader's view and
a transaction cannot use it: it carries the fold and the committed-byte hash, not the frames a
writer appends onto, and not the promise that every object the fold binds has been hashed. Bound
content is read and hashed on every capture, because every capture hands those bytes to its
caller, which is the same rule a read on the write path follows.

## Content and privacy

Blob bytes live separately in `blobs/`; journal entries contain a tenant/digest binding, object
identity and content hash. Bytes and their directory entry are synchronized before the binding
commits. Reusing a binding for different bytes refuses.

**Where the barrier is for a grouped write.** A blob written on its own takes a transaction of its
own, so it takes its own barrier: object bytes synchronized, directory entry synchronized, then the
commit sequence `append.json` → frame → manifest → directory. `append_group_with_blobs` writes the
blobs inside the group's transaction instead, so a batch of any size takes **the group's one
barrier and none besides**: every object is written and synchronized, the object directory is
synchronized **once for the whole batch**, and the bindings are pending operations in the single
frame the group commits. The barrier is therefore in exactly the same place it was — before the
manifest that publishes the frame — and what changed is how many frames the same work costs. The
durability promise is unchanged in both directions: nothing of the batch is observable before that
manifest, and a crash before it leaves object files no committed frame names, which the refusing
transaction itself, the next committed transaction and the complete opener dispose of as
unreferenced. A deduplicated group retry binds nothing, because the original commit already bound
it — and that is checked rather than assumed. A group's identity is its tenant, members and command
meta and deliberately not its batch, so the batch the commit carried is **recorded in the group's
own committed operation**, and a retry's batch is compared against that record.

**Against the record, never against what is bound now.** A digest the commit recorded still counts
as recorded after `delete_blob`, an erasure or a retention sweep has removed the binding: deleting
a blob is its own act and does not make a committed group belong to somebody else's request. A
retry after one deduplicates. Deciding this on live blob state instead turns an ordinary deletion
into `IdempotencyMismatch`, and the only recovery from that is a new idempotency key — which
appends every member of the group a second time into an append-only log.

So: a retry whose batch is the recorded set deduplicates; a retry whose batch is some other set is
not that request and refuses with `IdempotencyMismatch`; a retry carrying **no** batch is the
ordinary group retry, asks nothing about blobs and deduplicates as it always did. A digest that is
still bound is additionally held to its bytes, and differing bytes refuse as `Invalid` exactly as
they would on a fresh commit — the key is not in question there, the content is. Refusing one blob
of a batch refuses the whole group and publishes neither.

**A retry that carries a batch runs admission first.** The batch is part of what such a retry is
asking about, so the guard runs before anything is said about it; otherwise replaying a key one
once committed would report whether a digest is bound, and whether bytes match, with no guard ever
called. A retry carrying no batch does not repeat admission, which is the port's own contract and
what the shared conformance exercise asserts by counting guard calls.

**Who disposes of an object no frame references.** A blob written on its own could not be refused
after it was written — its transaction had nothing after it. A batch written inside a group can:
a member append, an inline projector or an admission guard may all refuse after the objects are
on disk. So the refusing transaction disposes of the objects it wrote before it returns, by object
identity rather than by what its rolled-back in-memory state references. The next committed
transaction and the complete opener remain the disposers for objects a crash leaves behind.

**The guarded form.** `append_group_guarded_with_blobs` is the same commit under an admission
guard, and it is on the `AtomicEventStore` port rather than on this provider because the caller it
exists for — a migration importing many boundaries at once — holds a trait object. Admission runs
before a byte of the batch is written, so a refused guard publishes neither the group nor a blob.

**A provider that cannot make that guarantee refuses instead of weakening it.** The port's default
implementation writes nothing, commits nothing and refuses with `eventlog_core::UNAVAILABLE`; only
this provider overrides it. An earlier default wrote each blob on its own path first and then ran
admission, which meant a refused guard had already published the whole batch on the two SQL
providers — and a caller holding a trait object cannot tell which provider it has, so the method
would have meant one thing here and the opposite there. A blob row is a binding, not scratch:
nothing reference-counts it, nothing sweeps it, and content-addressed storage has no way to take a
published blob back. A caller that wants the slow path still has `put_blob` in a loop followed by
`append_group_guarded`; what it cannot have is that sequence under a name promising the batch was
not published. The shared conformance exercise runs both halves of this against all three
providers. Open and reopen verify every active content hash;
afterwards a read verifies the bytes it reads, and the first sight of a frame another writer
committed verifies the objects that frame binds, so damaged bytes are refused whether they are
read or newly bound. Deletion and tenant erasure remove unreferenced objects from the active directory.
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
a previously observed head. A journal unit test checks that a resumed handle reads only the frames
past its observed head and hands a shorter file, an unchained tail, an unchained fork and a new
privacy epoch back to the complete opener, and a second checks that a resumed handle removes the
staging names no durable intent selected; durability tests check that a blob damaged after open no
longer fails an unrelated transaction while its own read still refuses, that frames another handle
committed are folded and the objects they bind verified, that a longer fork of the same store is
still refused, that a bound object removed after open refuses its read without changing state, and
that a committed frame damaged in place after open refuses the next append without altering the
history. A separate suite checks the same damage against an open handle's reads, its appends and a
history whose observed prefix was rewritten under a genuine tail. A capture unit test counts the
frames a handle decodes, re-encodes and folds and the objects it hashes, and requires ten captures
with no write between them to verify the committed history once and the content they hand out
every time; `tests/consistent_capture.rs` checks that a capture reusing a view still folds what
another writer committed, that a committed frame damaged in place afterwards refuses through the
strict reader without changing a file, that damaged content still refuses the next capture, that a
writers' lock that is no longer a regular file refuses one, and that a reserved recovery entry
appearing after a capture — including one that cannot be followed — refuses the next. A grouped-blob
unit test kills a subprocess at every boundary the joined write has, including the one between
synchronized object files and the unpublished frame that binds them, and requires the group and
every blob of it to be present together or absent together, the unreferenced objects of an
interrupted batch to be gone, and the retry to deduplicate; a second counts the barriers a batch
takes against the barriers the same work takes one blob per transaction, and a third checks that
both paths bind the same digests to the same bytes. These are process-death tests, not simulated drive power failure.
