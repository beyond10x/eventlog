---
format: aep.planning-md/3
id: story:projection-find-holds-its-bound-with-a-prepared-statement
kind: story
status: draft
title: postgres.projection_find holds its 1.05x bound with a prepared statement
summary: 'Product change only: prepared find per connection under force_custom_plan; depends on the measurement kit; G1 provisional'
relations:
- serves: vision:O2
- depends_on: story:projection-find-measurement-kit
- supersedes: story:pg-projection-find-back-within-1-05
scope:
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-postgres/src/find_statements.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/src/pool.rs
- confidence: cited
  path: crates/eventlog-postgres/src/projection_indexes.rs
- confidence: cited
  path: crates/eventlog-postgres/src/schema.rs
- confidence: cited
  path: crates/eventlog-postgres/src/statement_cache.rs
- confidence: cited
  path: crates/eventlog-postgres/tests/
- confidence: cited
  path: docs/design/projection-find-planning.md
- confidence: cited
  path: docs/design/projection-value-digest.md
- confidence: cited
  path: kpi/baseline.json
revision: 7
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

This story is the product change only. The measurement kit (skewed fixture, five KPI ids, the CI
record path, the red-capable variants) is `story:projection-find-measurement-kit`, which this
story depends on. This story supersedes `story:pg-projection-find-back-within-1-05`: its FIPS half
is shipped (`crates/eventlog-postgres/src/projection_indexes.rs:290`, the value digest calls only
`hashtextextended`) and only the 1.05 bound is left, which is this story's outcome.

## Design and review

- Design v2: `docs/design/projection-find-planning.md` (sections 0-10).
- Independent adversary review of v1, verdict approve with changes, findings F1-F7; design section
  8.2 lists which were taken: `docs/design/projection-find-planning-review.md`.
- Evidence on this story: one `review` record pointing at the review file, `metric_observation`
  records for the attribution run 45365255371 and the spike run 45368249583.

## Why

Timing runs of head against 0.10.0 (ratio of `postgres.projection_find` p50, ab/split runs on CI):

- 45343324168: 1.074 (earlier ab run).
- 45346744848, 45350502651, 45350518468: 1.049, 1.066, 1.069.

Attribution, CI run 45365255371 (branch `ci/plan-attribution`, head f885cdf3), SQL only, pgbench
extended, 8 rounds, PostgreSQL 17.6: planning per find base 37.0 us, head 44.5 us (+7.5 us). The
extra index `key_c` alone +4.0 us; `hashtextextended()` on the parameter alone +3.1 us. Execution
12.9 us base, 14.0 us head. A text change of the statement cannot remove either structural cause,
so the shape-only outcome is closed (design 0.2).

## Spike result (evidence)

CI run 45368249583, branch `ci/plan-prepared-spike`, commit 7900bc51, pgbench `-r` latency, 8
rounds:

| Case | Latency |
|---|---|
| E0 base (0.10.0 shape), unprepared | 0.114 ms |
| E1 head, unprepared | 0.131 ms |
| P0 base, prepared, `force_custom_plan` | 0.092 ms |
| P1 head, prepared, `force_custom_plan` | 0.102 ms |

- P1/E0 0.895 (range 0.832 to 0.912); P1/P0 1.109; P1/E1 0.779.
- P2: head prepared with `plan_cache_mode` auto switches to a generic plan; 1.21x of E0, execution
  0.014 -> 0.098 ms.
- Skewed fixture (large tenant, rare value): custom plan 0.0128 ms and 4 blocks, forced generic
  plan 2.043 ms and 417 blocks (160x).

## Build gate G1 (design 0.4)

G1: the prepared statement under `force_custom_plan`, on head's statement, end to end, is at most
1.03x of the unprepared 0.10.0 shape. The spike meets it at pgbench level (P1/E0 0.895). A
pgbench-level run cannot see the client's work, so G1 stays provisional until a scratch branch V1
(the prepared find only) is dispatched 4 times through `KPI / KPI timing` with `base` = the 0.10.0
tag and its median is at most 1.03.

- G1 passes: build design sections 1-9 and follow section 10 steps 5 to 7.
- G1 fails: nothing of sections 1-4 is built; the fallback is decision D5, the owner's, filed as
  `decision-blocker:find-fallback-if-the-prepared-gate-fails`. The bound is not touched. The kit
  story is not affected.

## Acceptance

Each line is observable alone. Commit 3 is the product commit; baseline step 3b is a separate
commit of the `kpi/baseline.json` uploaded by the `record` dispatch of the kit story, never "in
the same commit".

1. The in-crate tests 1-9 and U1-U3 of design section 6 ran against PostgreSQL and none skipped:
   the test step sets `EVENTLOG_TEST_POSTGRES_URL` and `EVENTLOG_REQUIRE_POSTGRES` (without the URL
   these tests return early, so a green run proves nothing), and each test runs under 60 s.
2. The tests this story adds take at most 10 s of the test step and none exceeds 60 s.
3. `KPI counters` after step 3b: `postgres.projection_find.parses` is 0, re-recorded in the
   separate baseline commit 3b.
4. `KPI counters`: `postgres.projection_find.round_trips` is 100.
5. `KPI counters`: `postgres.projection_find_skewed.tuples_read` equals the value the kit recorded.
6. Acceptance A/B, 4 dispatched 20-round `KPI timing` runs, `base` = the 0.10.0 tag: the median of
   the head/0.10.0 ratio of `postgres.projection_find.latency` p50 is at most 1.05.
7. The same ratio of the product commit against the kit commit (`base` = the kit commit), as the
   median of the same 4 runs, is at most 0.95, so the prepared statement is what moves the number
   (the spike gave 0.78 for P1/E1).
8. Each of `postgres.projection_find_skewed.latency`, `postgres.atomic_group_large.latency`,
   `postgres.batch_append.throughput`, `postgres.group_blobs.latency`,
   `postgres.read_stream.latency` and `postgres.read_feed_page.latency`, against the kit commit
   (`base` = the kit commit), has a median ratio over the 4 runs within 1.05 (for the throughput id,
   at least 1/1.05). The median is stated for each id and each run URL is recorded in the pull
   request. The `ab` command's default `REGRESSED` means above 1.5; this story's threshold is the
   stated 1.05, so a `REGRESSED`-free run is not enough.
9. Rustdoc names the dedicated-session requirement, the 64-text cap and the new session setting:
   present, checked by name in the rustdoc of the touched items.
10. `CHANGELOG.md` `[Unreleased]` carries the entry with the measured ratio and the sentence that
    +7 to +10 us of planning stays. The 0.11.0 tag itself is the owner's word and not part of the
    acceptance.

Fail on line 6 or 7: the bound is not touched; back to D5.

Out of scope, named on purpose: the transaction-scoped `ProjectionStore::find`
(`crates/eventlog-postgres/src/lib.rs:1896-1950`) still prepares and closes per call (three Syncs
per find) and the bound does not cover it.

## Forks (design section 7)

- D1, public API. Default A: no new public item. B: an opt-out switch on the config.
- D2, fallback visibility. Default A: not public; the refused, prepared and evicted counts exist
  under `#[cfg(test)]` only. B: a field on `PoolChurn` with an ESS change. C: a new public struct
  and accessor.
- D3, plan-mode scope. Default A: session-wide `force_custom_plan`, gated on the
  `postgres.atomic_group_large.latency` A/B (acceptance line 8). If it fails, or if the kit's
  fixture gate G2 fails, the owner accepts the measured cost or takes D5.
- D4, where the bound lives. Default A: the dispatched 20-round runs; the PR timing job stays
  informational; `ab_max_ratio` stays null. B: `ab_max_ratio: 1.05` on
  `postgres.projection_find.latency`.
- D5, the fallback if G1 fails. No default, the owner's. FA: a client-computed digest sent as a
  `bigint` (admission refuses a non-UTF-8 database). FB: drop `key_c` by declaring `row_key`
  `COLLATE "C"` (a new edition and ledger checksum). FA+FB. Filed as decision-blocker
  `decision-blocker:find-fallback-if-the-prepared-gate-fails`, which blocks this story only; the
  kit story is free.

## Scope

Files the design names (section 9): `crates/eventlog-postgres/src/statement_cache.rs` (new),
`crates/eventlog-postgres/src/pool.rs`, `crates/eventlog-postgres/src/lib.rs`,
`crates/eventlog-postgres/src/find_statements.rs` (new, `#[cfg(test)]`),
`crates/eventlog-postgres/src/projection_indexes.rs` (existing find tests),
`crates/eventlog-postgres/src/schema.rs`, `crates/eventlog-postgres/tests/`, `CHANGELOG.md`,
`docs/design/projection-find-planning.md`, `docs/design/projection-value-digest.md`. The KPI
surfaces (`crates/eventlog-kpi/src`, `.github/workflows/kpi.yml`, `kpi/kpis.json`, `AGENTS.md`)
belong to the kit story; this story touches `kpi/baseline.json` only in step 3b. No schema, ledger
or migration change; no ESS change under D2 A.

Every implementation brief names: the 60 s per-test bound and the 120 s test-step budget; the
10-minute job limit; no new Python in anything that stays in the repository; scripts that measure
or wait start without the `slot` prefix; nothing runs on the shared workstation for timing.
