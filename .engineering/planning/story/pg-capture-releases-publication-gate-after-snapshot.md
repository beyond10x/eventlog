---
format: aep.planning-md/3
id: story:pg-capture-releases-publication-gate-after-snapshot
kind: story
status: draft
title: A PostgreSQL capture releases the publication gate after its snapshot and checks the privacy epoch
summary: Review P11 = B; writers of other tenants under one owner prefix wait for the whole capture read
owner: eventlog
revision: 1
---
## Outcome

A PostgreSQL capture releases the publication gate once its snapshot is taken, and checks the
privacy epoch after the read, so an erasure or redaction committed meanwhile is never served. Writers
of other tenants that share the owner prefix no longer wait for the whole capture read.

## Finding

Review P11, eventlog 0.9.4 (`b8582219`), `review.txt:392` in the review folder: PostgreSQL tracked
reads can hold writes for unrelated tenants. The Entity adapter asks for provider changes since a
checkpoint; PostgreSQL inherits the complete-capture fallback and issues no continuity checkpoint.
Each capture takes an exclusive publication lock keyed to the owner prefix while it reads events,
blobs and projections. Source: `entity-runtime/crates/entity-eventlog/src/adapter/tracked.rs:383`,
`eventlog/crates/eventlog-core/src/capture.rs:479`,
`eventlog/crates/eventlog-postgres/src/capture.rs:75`.

## Decision

Assessed by agent el-d (not fixed). Options it put:

- A: a durable continuity journal like SQLite's. New edition, a write on every append. High cost.
- B (default): release the gate once the snapshot is taken, plus a check of the privacy epoch after
  the read for erasure or redaction committed meanwhile. Medium cost, no protocol change. A
  tenant-scoped gate key is the other variant of B; it needs a fenced cutover.
- C: leave it and document it.

Timo, 2026-11-23 00:50-00:52 CET via `host-operator`: P11 = B, as this draft story.

## Acceptance

A test holds a PostgreSQL capture open after its snapshot is taken and shows that an append of a
second tenant under the same owner prefix commits before the capture ends, and that a redaction
committed during the capture read makes that capture end with a refusal or a re-read instead of
returning the redacted body.
