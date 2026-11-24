---
format: aep.planning-md/3
id: story:projection-find-holds-its-bound-with-a-prepared-statement
kind: story
status: draft
title: postgres.projection_find holds its 1.05x bound with a prepared statement
summary: Prepared find per connection under force_custom_plan; gated on CI spike run 45368249583
relations:
- serves: vision:O2
- informed_by: story:pg-projection-find-back-within-1-05
scope:
- confidence: cited
  path: .github/workflows/kpi.yml
- confidence: cited
  path: AGENTS.md
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-kpi/src
- confidence: cited
  path: crates/eventlog-postgres/src/find_statements.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/src/pool.rs
- confidence: cited
  path: crates/eventlog-postgres/src/statement_cache.rs
- confidence: cited
  path: docs/design/projection-value-digest.md
- confidence: cited
  path: kpi/baseline.json
- confidence: cited
  path: kpi/kpis.json
revision: 3
---
## Outcome

`postgres.projection_find` p50 of head against Eventlog 0.10.0 is at most 1.05x, as the median of 4
`workflow_dispatch` runs of `KPI / KPI timing` (`base` = the 0.10.0 tag, `rounds` = 20) on the CI
runner. The bound does not change (`docs/design/projection-value-digest.md:15`). The mechanism is a
prepared find per pooled connection under session-wide `plan_cache_mode = force_custom_plan`: the
server no longer parses and analyses the statement on each call; it still plans it every call.
The pull request and the CHANGELOG say so in the wording of design 0.3 (the +7 to +10 us of
planning from the page index and the server-side digest call stays and is paid for by the removed
parse).

## Design and review

- Design v2: `docs/design/projection-find-planning.md` (sections 0-10).
- Independent adversary review of v1, verdict approve with changes, findings F1-F7; design section
  8.2 lists which were taken: `docs/design/projection-find-planning-review.md`.
- Evidence on this story: one `review` record pointing at the review file, one `metric_observation`
  for the attribution run 45365255371. The ab/split ratios above are in this body only.

## Why

Timing runs of head against 0.10.0 (ratio of `postgres.projection_find` p50, ab/split runs on CI):

- 45343324168: 1.074 (earlier ab run).
- 45346744848, 45350502651, 45350518468: 1.049, 1.066, 1.069.

Attribution, CI run 45365255371 (branch `ci/plan-attribution`, head f885cdf3), SQL only, pgbench
extended, 8 rounds, PostgreSQL 17.6: planning per find base 37.0 us, head 44.5 us (+7.5 us). The
extra index `key_c` alone +4.0 us; `hashtextextended()` on the parameter alone +3.1 us. Execution
12.9 us base, 14.0 us head. A text change of the statement cannot remove either structural cause,
so the shape-only outcome is closed (design 0.2).

## Open gate (design section 10, step 1)

Spike, CI run 45368249583 on branch `ci/plan-prepared-spike`: prepared + `force_custom_plan` vs
extended, end to end, and the skewed fixture under `auto` and `force_generic_plan`. Its numbers
were not in when this story was written. Nothing is built until they are read.

- G1 (build gate): the prepared statement under `force_custom_plan`, on head's statement, is at
  most 1.03x of the unprepared 0.10.0 shape. If the spike is pgbench-level, G1 is provisional and a
  scratch branch V1 (the prepared find only) is dispatched 4 times with the same limit.
- G1 passes: build design sections 1-9, follow section 10 steps 2-7.
- G1 fails: nothing of sections 1-4 is built; the fallback is decision D5.
- G2 (fixture gate): the skewed fixture is accepted only if it meets E1 and E2 of design 5.2.

## Acceptance (design section 10)

1. Spike read, G1 and G2 decided, run URLs and numbers recorded here.
2. Story approved by the plan critics before implementation starts.
3. Commit 1: KPI cases and the CI record path against unchanged head: skewed fixture with E1/E2
   asserted, `postgres.projection_find_skewed.tuples_read`, `postgres.projection_find.round_trips`,
   `postgres.projection_find.parses` (baseline 8), `postgres.atomic_group_large.latency`,
   `record --counters` and a `record` dispatch input. `KPI counters` green after the recorded
   baseline is committed; one A/A on 0.10.0 passes; one A/A head vs head gives the noise floor.
4. Red-capable proof: variants G and U as scratch branches, never merged. G turns `KPI counters`
   red on `tuples_read` and the dispatched A/B REGRESSED on the skewed id.
5. The implementation (design sections 1-4, tests 1-9 and U1-U3, docs), `parses` re-recorded to 0
   in the same commit. In-crate tests green, each under 60 s, the step under 120 s; `KPI counters`:
   `parses` 0, `round_trips` 100, `tuples_read` at commit 1's value.
6. Acceptance A/B: 4 dispatched runs, median of the head/0.10.0 ratio of
   `postgres.projection_find.latency` p50 at most 1.05; the skewed id and
   `postgres.atomic_group_large.latency` within 1.05 of commit 1; `postgres.batch_append.throughput`,
   `postgres.group_blobs.latency`, `postgres.read_stream.latency` and
   `postgres.read_feed_page.latency` not REGRESSED against commit 1. Fail: the bound is not
   touched; back to D5.
7. Release 0.11.0 under the 0.3 wording with the measured ratio. The tag is a publication and waits
   for the owner's word.

Out of scope, named on purpose: the transaction-scoped `ProjectionStore::find`
(`crates/eventlog-postgres/src/lib.rs:1896-1950`) still prepares and closes per call (three Syncs
per find) and the bound does not cover it.

## Forks (design section 7)

- D1, public API. Default A: no new public item. B: an opt-out switch on the config.
- D2, fallback visibility. Default A: not public; the refused, prepared and evicted counts exist
  under `#[cfg(test)]` only. B: a field on `PoolChurn` with an ESS change. C: a new public struct
  and accessor.
- D3, plan-mode scope. Default A: session-wide `force_custom_plan`, gated on the
  `postgres.atomic_group_large.latency` A/B (head-after within 1.05 of head-before). If it fails,
  or if G2 fails, the owner accepts the measured cost or takes D5.
- D4, where the bound lives. Default A: the dispatched 20-round runs; the PR timing job stays
  informational; `ab_max_ratio` stays null. B: `ab_max_ratio: 1.05` on
  `postgres.projection_find.latency`.
- D5, the fallback if G1 fails. No default, the owner's. FA: a client-computed digest sent as a
  `bigint` (admission refuses a non-UTF-8 database). FB: drop `key_c` by declaring `row_key`
  `COLLATE "C"` (a new edition and ledger checksum). FA+FB. Filed as decision-blocker
  `decision-blocker:find-fallback-if-the-prepared-gate-fails`, which only bites if G1 fails.

## Scope

Files the design names (section 9): `crates/eventlog-postgres/src/statement_cache.rs` (new),
`crates/eventlog-postgres/src/pool.rs`, `crates/eventlog-postgres/src/lib.rs`,
`crates/eventlog-postgres/src/find_statements.rs` (new, `#[cfg(test)]`),
`crates/eventlog-kpi/src`, `.github/workflows/kpi.yml`, `kpi/kpis.json`, `kpi/baseline.json`,
`CHANGELOG.md`, `AGENTS.md`, `docs/design/projection-value-digest.md`. No schema, ledger or
migration change; no ESS change under D2 A.

Every implementation brief names: the 60 s per-test bound and the 120 s test-step budget; the
10-minute job limit; no new Python in anything that stays in the repository; scripts that measure
or wait start without the `slot` prefix; nothing runs on the shared workstation for timing.
