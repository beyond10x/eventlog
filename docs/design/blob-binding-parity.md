# Immutable blob bindings across providers

Owner: story:blob-binding-conflicts. Approved ESS evolution revision 1 requires shared file,
SQLite and PostgreSQL blob semantics before ER records depend on them.

## Decision

Within one tenant, a digest binds to exactly one byte sequence until explicit deletion or tenant
erasure. The first put establishes that binding. Repeating the same bytes succeeds. A put with
different bytes returns `EventLogError::Invalid` and leaves the original bytes intact. Tenants
remain isolated; the same digest may bind different content in different tenants under the existing
opaque digest API. After explicit deletion, a new put may establish a new binding.

This makes the existing file-provider refusal the shared port contract. At the selected source
revision, `eventlog-file/src/lib.rs` checks existing content, while both SQL providers acknowledge
an ignored conflicting insert without checking its content. The shared conformance exercise only
retries identical bytes. This is a conformance extension and SQL behavior correction, not fresh
evidence that either backend has already passed it.

## Compatibility and implementation constraints

Preserve digest spelling, tenant identity, public signatures, DDL and persisted formats, including
`eventlog-file/1`. Do not add an UPDATE, overwrite a blob on conflict, or require callers to change
their digest algorithm. The store validates immutable binding equality; cryptographic validation of
SQL blob contents is a separate qualification question. No product type or ER record belongs here.

The SQL comparison must share an atomic operation or transaction with binding publication. Two
writers racing to publish different bytes must have exactly one winner; the other must refuse
after observing the winning binding. An implementation may not acknowledge conflicting content
because a uniqueness conflict was silently ignored. Erasure and deletion retain their current
authority and isolation boundaries.

The predecessor storage RFC's physical schema and payload-separation rules remain unchanged.
Existing local file-provider and atomic-group designs remain binding.

## Acceptance

Extend shared conformance before the fix and observe SQL failure. Exercise original bytes,
identical retry, different-content refusal, tenant isolation, explicit deletion/rebinding and
concurrent differing writers. Assert the specific error class and retained content. Run the shared
exercise against file, SQLite and a disposable PostgreSQL database. Remove the SQL comparison as
a deliberate mutation, observe the regression fail, then restore the implementation.

The complete repository gate must pass with PostgreSQL actually selected before local integration.
This unit alone does not qualify all group crash, tamper or production-restart requirements in
the larger ESS evolution plan.
