---
title: Persistence guarantees
---

## Append and retry

An append checks the expected stream revision and commits events, command claims, receipts and
inline projection changes together. A refused append leaves those changes uncommitted.

Keep the original command key and canonical request hash when retrying. An exact retry returns
the original committed receipt and envelopes. Reusing an identity with different input refuses.
`UnknownCommit` means the caller cannot yet classify the outcome; resolve the original command
instead of sending a new identity.

## Atomic groups and blobs

An `AppendGroup` commits its streams together. Independently committed single appends cannot
provide this guarantee.

`AtomicBlobEventStore` publishes the group's blob bindings and events in one native transaction
on File, SQLite and PostgreSQL. Its fingerprint includes the actual content bytes, and a refused
append rolls tentative publication back.

A second opt-in API, `AtomicEventStore::append_group_guarded_with_blobs`, puts a group's bindings
under one durability barrier. File implements it; SQLite and PostgreSQL return the trait's refusal.
Do not infer this capability from support for `AtomicBlobEventStore`.

File also has an inherent `append_group_with_blobs` method. In 0.3.0, call the portable trait
explicitly as `AtomicBlobEventStore::append_group_with_blobs(&store, &request)` to avoid Rust
selecting the inherent method with a different argument shape.

Blob bindings are immutable while present: identical bytes may be retried, different bytes under
the same tenant/digest binding refuse. Providers verify stored length and content integrity when
handing bytes out. Domain payloads belong in content storage; events refer to them.
See the [SQLite 0.3.0 limitation](providers.md#sqlite) before relying on integrity admission
when a fresh atomic group reuses an existing binding.

## Inspection and capture

`InspectHistory` decodes a complete tenant history with explicit source-byte, event-count and
envelope-byte bounds. Zero is a real limit. File and SQLite inspectors create no root, lock or
table and grant no writer or recovery authority. Redacted or malformed envelopes refuse.

`ConsistentTenantCapture` observes history, active blob bindings and projections under the
provider's consistency boundary. Choose explicit `CaptureLimits`.
`capture_tenant` reads and hashes content before returning; `capture_tenant_deferred` hands it
out through a reader that verifies the bytes when requested. File avoids eagerly reading those
objects; the default provider implementation remains eager. Changed content is refused, never
silently substituted.

## Projections and snapshots

Inline projections share the append transaction. Catch-up projections consume the committed feed.
A rebuild publishes the selected tenant's complete replacement rows and cursor atomically,
preserving the old view if it fails and leaving other tenants alone.

A snapshot is a cache. Observe `snapshot_generation` before loading or folding history, then use
`save_snapshot_checked`. A stale generation means redaction or erasure changed the history:
discard the candidate and fold again. The legacy unproven save method refuses.

Automatic repository caching is best effort after a committed append. A failed cache write does
not undo that command. Explicit snapshot creation reports errors.

## Tenant and privacy boundaries

Every stream includes a tenant. Projection callbacks are confined to the transaction's tenant.
The host selects tenant and authority before calling Eventlog; the library does not authenticate
the caller.

Use opaque identifiers. Personal names, addresses and raw payload bytes do not belong in the
durable event envelope. Redaction and erasure have explicit provider protocols; editing journal
frames or database rows directly bypasses them.
