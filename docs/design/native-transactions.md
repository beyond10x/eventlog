# Native caller transactions

`TransactionalEventStore` is an optional native capability implemented by PostgreSQL. It invokes
one caller callback with an object-safe `TransactionSession`, committing only a successful,
settled callback and returning its chosen Rust value after commit. It lends no raw connection,
credential, executor or commit operation. The generic outer capability is not object-safe; callers
capture owned inputs for the scoped callback. File and SQLite retain their existing interfaces;
there is no fallback that commits each operation independently.

## Work inside the boundary

A session reads bounded stream pages, current heads and committed/staged stream inventory in its
own tenant. These reads share the SQL path used by the outer store and do not use the feed
watermark. Its projection view provides the existing tenant-scoped blob/read-model operations,
including indexed document queries, inline row locks and read-your-writes. It does not turn a
catch-up projection into an inline invariant, or grant deployment-admission authority.

`lock_stream` uses exactly the ordinary/grouped writer's stream lock, including an absent stream.
`lock_identity` serializes a logical identity under tenant and namespace coordinates. Neither
silently imposes a global lock order across arbitrary callback calls: the caller owns that wider
order, and database deadlock/refusal errors must abort or be handled through the documented
savepoint boundary. The publication gate is acquired before every callback lock. Keep callbacks
bounded; that gate also means a long callback holds this owner's feed/rebuild publication back.
External IO and long-running business work belong outside the transaction.

Capture `snapshot_generation` before loading or reusing a recorded prefix. Its shared row lock
protects the captured generation against redaction until transaction end. Stream locking separately
serializes writers. A generation captured in a transaction that later rolls back is not a durable
snapshot receipt, and must still be checked before any later reuse.

`append_group` reuses the existing group fingerprint, lock protocol, ordered entry writes, inline
projectors and durable retry coordinates. The callback may construct its next group from current
reads, including earlier staged results. No group is independently committed. A failed group rolls
back to its savepoint, removing its body/projection prefix, newly acquired locks and group receipt;
the callback can catch the refusal and continue. An exact staged or committed group retry resolves
its original receipt before old expectations, without applying projectors again.

## Counters and erasure

`reserve_sequence(namespace, count)` reserves a positive contiguous range and returns the value
just before that range. Values are bounded by PostgreSQL's signed BIGINT range. A failed reservation
restores its savepoint and a refused outer callback rolls back every reservation.

The existing generic `ScopeCounter` table stores these values under a new `q` coordinate tag:
`q<tenant-byte-length>:<tenant><namespace-byte-length>:<namespace>`. The `t` and `d` admission tags
remain distinct and cannot be addressed by a sequence namespace. No column or table changes.
The ESS admission model records this additional use and the logical document-query registration
flag. Tenant erasure removes both tenant admission and tenant sequence coordinates using their
exact length-prefixed tenant boundary; it cannot erase a neighboring tenant's namespace.

Fence old writer/erasure binaries before enabling this capability: an old eraser does not know
about `q` coordinates, even though the physical table is unchanged. The document-query roster's
existing fenced-upgrade requirement also remains in effect. Hosted role and pool admission are
unchanged; this capability adds no DDL to the application path.

## Refusal, cancellation and retries

Every started session operation carries an unfinished-operation marker. Successful completion
clears it. Ordinary operation errors after entering SQL leave the transaction unusable; preflight
argument refusals may occur before SQL. Groups and sequences are the recoverable operations: their
savepoint is rolled back and released before the marker clears on an error. The returned projection
view uses the same marker rather than exposing its underlying transaction context directly.

If the callback drops a started future, catches a timeout, and then returns success, the marker
forces outer rollback. A callback that propagates an error retains that error. Outer cancellation
or timeout quarantines the connection until its driver stops, using the existing pool lifecycle.
A timeout or lost commit response reports `UnknownCommit`; it never triggers automatic callback
replay or invents a new group identity.

The callback's return value is not a new durable command log. Durable group identities remain the
retry authority. A sequence-only transaction has no deduplicated outer receipt, so blindly rerunning
it after an unknown outcome can reserve another range. An adopter that needs durable recovery must
bind the reservation and its original command identity into the same recorded group.

## Verification boundary

The applicable shared exercise covers ordered staged writes, transaction reads and queries, exact
retries, caught group refusal, outer refusal, counter bounds, tenant separation and counter erasure.
PostgreSQL interleavings cover external invisibility, logical/stream lock contention, redaction
fencing, outer deadline rollback, and caught cancellation of both a partially applied group and a
blocked projection write. Focused mutation checks demonstrate the savepoint, cancellation and
redaction guards. This is not a hosted production proof, deployment budget approval, ER session
adoption or completed SQL facade retirement.
