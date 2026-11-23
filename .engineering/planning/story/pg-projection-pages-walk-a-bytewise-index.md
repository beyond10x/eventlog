---
format: aep.planning-md/3
id: story:pg-projection-pages-walk-a-bytewise-index
kind: story
status: draft
title: PostgreSQL projection pages walk a (tenant_id, row_key COLLATE "C") index, admitted and cut over fenced
summary: Review L22 follow-up; 0.10.0 sorts each page on a non-C database
owner: eventlog
revision: 1
---
## Outcome

On a PostgreSQL database that is not C-collated, a projection page is read from a
`(tenant_id, row_key COLLATE "C")` index instead of sorting the tenant's rows for every page.
PostgreSQL admission knows the index, `migrate` builds it, and binaries that do not know it are
fenced out before the cutover.

## Finding

Review L22, eventlog 0.9.4 (`b8582219`), `round4/eventlog.txt:19` in the review folder: PostgreSQL
prefix pagination can silently omit matching rows. The shared pager stops at the first key outside
the prefix; PostgreSQL ordered keys under the database collation. Under ICU `und` the query returned
`desk/1, Desk/2, desk/3`; the `desk/` prefix with limit 10 returned only `desk/1` and no
continuation. Source: `crates/eventlog-core/src/lib.rs:1079,1087,1096`,
`crates/eventlog-postgres/src/lib.rs:997`, `crates/eventlog-postgres/src/schema.rs:218`. Probe:
`pg-prefix.mjs`, `pg-prefix.txt:8`.

## What exists already

PR #62 (`fix/review-findings`, release 0.10.0, open; not on `main`) carries commit `1473b370`:
`projection_list` compares and orders `row_key COLLATE "C"` (`crates/eventlog-postgres/src/lib.rs:1096`
on that branch), no DDL. The commit message states the cost: on a non-C database the page query sorts
the tenant's rows instead of walking the primary key, and the matching index is left out because it
needs a PostgreSQL admission change and a fenced cutover.

## Decision

Timo, 2026-11-23 00:43/00:44 CET via `host-operator`: L22 = A (keep the `COLLATE "C"` query now) and
file this index as a draft story. Option B as the assessing agent put it: add a
`(tenant_id, row_key COLLATE "C")` index, with changes to PostgreSQL admission in `schema.rs` and
`capture.rs`, a migration rerun and a fenced cutover. Option C (make `row_key` a `COLLATE "C"`
column) was rejected as not additive.

## Acceptance

On a database created with ICU `und` collation, `EXPLAIN` of the projection page query after
`migrate` shows an index scan on the `(tenant_id, row_key COLLATE "C")` index with no Sort node, the
mixed-case `desk/1, Desk/2, desk/3` paging case still returns all three rows, and a binary built
before the change is refused by the fence on the migrated schema.
