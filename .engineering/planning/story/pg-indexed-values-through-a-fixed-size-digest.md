---
format: aep.planning-md/3
id: story:pg-indexed-values-through-a-fixed-size-digest
kind: story
status: implemented
title: PostgreSQL indexes a fixed-size digest of indexed values, verifies candidates, migrates fenced
summary: Review L78 = B; 0.10.0 only refuses indexed values over 2,048 bytes
owner: eventlog
relations:
- serves: vision:O2
revision: 4
transitions:
- {from: "draft", to: "proposed", at: "2026-11-23T17:45:56Z", actor: "human:timo", revision: 2, decided_on: {"recorded":{"test_result":1}}}
- {from: "proposed", to: "active", at: "2026-11-23T17:45:56Z", actor: "human:timo", revision: 3, decided_on: {"recorded":{"test_result":1}}}
- {from: "active", to: "implemented", at: "2026-11-23T17:45:57Z", actor: "human:timo", revision: 4, decided_on: {"recorded":{"test_result":1}}}
---
## Outcome

PostgreSQL indexes a fixed-size equality digest of each indexed projection value instead of the
value, and verifies every candidate row against the actual value, so an indexed value over 2,048
bytes is accepted again. Existing projection tables move to the digest index through a fenced
migration.

## Finding

Review L78, eventlog 0.9.4 (`b8582219`), `round9/eventlog.txt:44` in the review folder: a large
indexed projection value can abort a PostgreSQL inline append. PostgreSQL puts the whole value into a
B-tree index; a large, poorly compressible value exceeds the index-tuple limit even when the row body
fits, and the inline callback error aborts the enclosing event append. Source:
`crates/eventlog-core/src/projection.rs:159,302,307`, `crates/eventlog-postgres/src/lib.rs:1634,1655`,
`crates/eventlog-postgres/src/schema.rs:744,748`. Fix proposed by the review: a portable size check,
or a fixed-size equality digest with candidate verification.

## What exists already

PR #62 (`fix/review-findings`, release 0.10.0, open; not on `main`) carries commit `9b0b56be`: the
size check only. `eventlog-core` gets `MAX_INDEXED_VALUE_BYTES = 2048`
(`crates/eventlog-core/src/projection.rs:348` on that branch) and every provider (PostgreSQL, SQLite,
File; Tree through these) refuses a longer value with the new
`EventLogError::IndexedValueTooLarge { field, bytes, limit }`. That is a contract change for SQLite
and File, which accepted longer values before. A B-tree entry must fit about a third of a page (2,704
bytes at 8 KiB); a 512-byte tenant plus 2,048 incompressible bytes fit. Tests:
`projection::tests::an_indexed_value_one_byte_over_the_bound_is_refused_by_name`,
`an_oversized_indexed_value_is_refused_before_postgresql_aborts_the_append`.

## Decision

Timo, 2026-11-23 00:50-00:52 CET via `host-operator`: L78 = B, the size check now plus this draft
story for the digest index with a fenced migration.

## Acceptance

On every provider an inline projection indexes and finds by equality a value of 8,000
incompressible bytes under a 512-byte tenant, two distinct values sharing a digest prefix are told
apart by the candidate check, and a PostgreSQL store migrated from the value index answers the same
lookups while a binary built before the change is refused by the fence.
