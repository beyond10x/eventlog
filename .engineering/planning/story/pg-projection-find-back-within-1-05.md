---
format: aep.planning-md/3
id: story:pg-projection-find-back-within-1-05
kind: story
status: archived
title: PostgreSQL projection_find returns within 1.05x of 0.10.0 with the digest index, FIPS-safe
summary: The digest index costs 1.33x on projection_find (278.8 -> 371.0 us p50), from planning the md5 expression index
relations:
- serves: vision:O2
revision: 4
transitions:
- {from: "draft", to: "proposed", at: "2026-11-23T22:59:20Z", actor: "human:timo", revision: 2}
- {from: "proposed", to: "active", at: "2026-11-23T22:59:20Z", actor: "human:timo", revision: 3}
- {from: "active", to: "archived", at: "2026-11-24T17:37:58Z", actor: "agent:eventlog", revision: 4}
---
## Outcome

`postgres.projection_find` is back within 1.05x of 0.10.0's p50 on the KPI timing A/B, with the
digest index of story:pg-indexed-values-through-a-fixed-size-digest kept (values of any length
indexed, candidates verified), and the value digest works on a PostgreSQL server in FIPS mode.

## Evidence (e1-pg-schema hand-back, 2026-11-23)

- `eventlog-kpi ab` base 72715af1, head 0fe56b5c, 5 interleaved rounds, load 71/97/105:
  `postgres.projection_find` p50 278.8 -> 371.0 us (1.331, limit 1.5).
- pgbench on the KPI shape (1,001 rows, 10 matching), extended protocol, load about 45: old
  value index 0.12 ms; plus `_key_c` 0.13; md5 digest index (shipped) 0.17; digest passed by the
  client 0.16; stored generated digest column 0.145; `hashtextextended` 0.15.
- Warm planning time 0.048 ms with the value index, 0.085 ms with the md5 expression index;
  execution about equal. The cost is planning on the expression index plus about 10 us for the
  extra `_key_c` index.
- PostgreSQL in FIPS mode refuses `md5()`.
- A client-computed md5 passed as a parameter was rejected: on a non-UTF8 database it would miss
  rows silently.

## Options to measure

- A prepared, cached statement for the find (planning once per connection), if the store does not
  already cache it.
- `hashtextextended(idx_N, 0)` instead of md5 (FIPS-safe; needs a new colliding pair for the
  candidate test).
- A stored generated digest column (migrate rewrites each projection table).

## Acceptance

- `eventlog-kpi ab` against the 0.10.0 tag: `postgres.projection_find` ratio at or under 1.05,
  load stated; no other timing regressed.
- The candidate check, the 8,000-byte value test and the migration test stay green; a test runs
  the digest on a function FIPS mode allows, or the store states why md5 is kept.
