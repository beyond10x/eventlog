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
  path: .github/workflows/kpi.yml
- confidence: cited
  path: AGENTS.md
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-kpi/src/compare.rs
- confidence: cited
  path: crates/eventlog-kpi/src/definition.rs
- confidence: cited
  path: crates/eventlog-kpi/src/main.rs
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof/required.rs
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
revision: 11
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
pgbench-level run cannot see the client's work, so G1 stays provisional until variant V1 is
measured. V1 is owned by the kit story, not by this one: `story:projection-find-measurement-kit`,
acceptance line 14 (the prepared find only, on a scratch branch, 4 dispatched 20-round runs with
`base` = the 0.10.0 tag, median of the `postgres.projection_find.latency` p50 ratio at most
1.03). This story starts when that line is met.

- V1 passes: build design sections 1-9 and follow section 10 steps 5 to 7.
- V1 fails: nothing of sections 1-4 is built; the fallback is decision D5, the owner's, filed as
  `decision-blocker:find-fallback-if-the-prepared-gate-fails`. The bound is not touched. The kit
  story stays valid.

## Acceptance

Each line is observable alone. Commit 3 is the product commit; baseline step 3b is a separate
commit of the `kpi/baseline.json` uploaded by the `record` dispatch of the kit story, never "in
the same commit".

Dispatches: 8 in total, each a 20-round `KPI / KPI timing` run with `head` = the product commit,
each URL named in the pull request. Group A is 4 runs with `base` = the 0.10.0 tag (line 8). Group
B is 4 runs with `base` = the kit commit (lines 9 and 10).

1. The step that runs the in-crate tests fails when a test in the new modules skips, and the step
   is green. Mechanism, named once: the `Persistence proof` job of
   `.github/workflows/persistence-proof.yml` sets `EVENTLOG_TEST_POSTGRES_URL` at job level and
   runs `bash scripts/gate.sh --production-proof`, which runs every workspace test binary with
   `EVENTLOG_REQUIRE_POSTGRES=1` (`crates/eventlog-postgres/examples/production-proof.rs`); each
   new DB test of design section 6 (tests 1-9 and U1-U3) starts with the repository's skip guard,
   which fails instead of returning early when that variable is set and the URL is missing
   (`crates/eventlog-postgres/src/pool.rs:1449`). The local default lane (`cargo nextest run
   --workspace`) sets neither variable.
2. The new tests are named in `required_cases()`
   (`crates/eventlog-postgres/examples/production-proof/required.rs`), so the job reports one that
   is absent as missing.
3. No test this story adds takes more than 60 s (the nextest lane kills at 60 s,
   `.config/nextest.toml`, and prints the ten slowest).
4. The tests this story adds take at most 10 s in total.
5. `KPI counters` after step 3b: `postgres.projection_find.parses` is 0, re-recorded in the
   separate baseline commit 3b.
6. `KPI counters`: `postgres.projection_find.round_trips` is 100.
7. `KPI counters`: `postgres.projection_find_skewed.tuples_read` equals the value the kit recorded in
   its step 1b.
8. Group A: the median over its 4 runs of the head/0.10.0 ratio of
   `postgres.projection_find.latency` p50 is at most 1.05.
9. Group B: the median over its 4 runs of the product/kit-commit ratio of
   `postgres.projection_find.latency` p50 is at most 0.95, so the prepared statement is what moves
   the number (the spike gave 0.78 for P1/E1).
10. Each of the six ids below is judged separately, as its own check, in group B against the kit
   commit: the median over the 4 runs is within 1.05 (for the throughput id, at least 1/1.05).
   For this story the threshold is 1.05; `REGRESSED` in the `ab` command's default means above
   1.5, so a run without `REGRESSED` is not enough.
   - `postgres.projection_find_skewed.latency`
   - `postgres.atomic_group_large.latency`
   - `postgres.batch_append.throughput`
   - `postgres.group_blobs.latency`
   - `postgres.read_stream.latency`
   - `postgres.read_feed_page.latency`
11. The dedicated-session requirement, the 64-text cap and the new session setting are documented
    on named items. Under D1 default A (no new public item): the requirement on the existing
    public docs of `eventlog_postgres::PostgresConnectionAuthority::connect` and
    `eventlog_postgres::PostgresConfig::caller_owned` (`crates/eventlog-postgres/src/pool.rs`);
    the 64-text cap on the module docs of `crate::statement_cache`
    (`crates/eventlog-postgres/src/statement_cache.rs`, private); the new setting
    (`plan_cache_mode = force_custom_plan`) on the docs of `crate::pool::session_configuration`.
    Check: `slot cargo doc --no-deps -p eventlog-postgres --document-private-items` ends without a
    warning, and `grep -n 'dedicated session' crates/eventlog-postgres/src/pool.rs`,
    `grep -n '64' crates/eventlog-postgres/src/statement_cache.rs` and
    `grep -n 'force_custom_plan' crates/eventlog-postgres/src/pool.rs` each find the doc comment
    on the named item (no CI step runs `cargo doc`, so this is run when the story is closed).
12. `CHANGELOG.md` `[Unreleased]` carries an entry that states the measured ratio of line 8 (the
    median of group A).
13. The same entry carries the sentence that +7 to +10 us of planning stays.

The 0.11.0 tag itself is the owner's word and not part of the acceptance.

Pull request checklist (recorded in the pull request body, not acceptance lines): the median for
each id of line 10 over its 4 runs, and every dispatch URL of groups A and B.

Fail on line 8 or 9: the bound is not touched; back to D5.

Out of scope, named on purpose: the transaction-scoped `ProjectionStore::find`
(`crates/eventlog-postgres/src/lib.rs:1896-1950`) still prepares and closes per call (three Syncs
per find) and the bound does not cover it.

## Forks (design section 7)

- D1, public API. Default A: no new public item. B: an opt-out switch on the config.
- D2, fallback visibility. Default A: not public; the refused, prepared and evicted counts exist
  under `#[cfg(test)]` only. B: a field on `PoolChurn` with an ESS change. C: a new public struct
  and accessor.
- D3, plan-mode scope. Default A: session-wide `force_custom_plan`, gated on the
  `postgres.atomic_group_large.latency` A/B (acceptance line 10). If it fails, or if the kit's
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
`crates/eventlog-postgres/src/schema.rs` (read for the digest function; no schema change),
`crates/eventlog-postgres/tests/`,
`crates/eventlog-postgres/examples/production-proof/required.rs` (the new test names, line 2),
`CHANGELOG.md`, `docs/design/projection-find-planning.md`,
`docs/design/projection-value-digest.md`. The KPI surfaces (`crates/eventlog-kpi/src`,
`.github/workflows/kpi.yml`, `kpi/kpis.json`, `AGENTS.md`) belong to the kit story; this story
touches `kpi/baseline.json` only in step 3b. No schema, ledger or migration change; no ESS change
under D2 A.

Every implementation brief names: the 60 s per-test bound and the 120 s test-step budget; the
10-minute job limit; no new Python in anything that stays in the repository; scripts that measure
or wait start without the `slot` prefix; nothing runs on the shared workstation for timing.

## Measured

Measured on the CI runner (PostgreSQL 17.6), 8 `workflow_dispatch` runs of `KPI / KPI timing`, 20 rounds each, `head` = `be54a2dc` (the product commits plus baseline step 3b). Nothing was measured on the workstation. Run URLs are `https://github.com/beyond10x/eventlog/actions/runs/<id>`.

Group A, `base` = 0.10.0 tag `c0f961dc`, ratio head/base of the p50 of `postgres.projection_find.latency`:

| Run | `ref` | base p50 (us) | head p50 (us) | ratio |
| --- | --- | --- | --- | --- |
| 45597161111 | `main` | 380.5 | 376.1 | 0.988 |
| 45597166908 | `scratch/find-variant-v1` | 390.8 | 380.3 | 0.973 |
| 45597176653 | `scratch/find-variant-g` | 382.1 | 375.0 | 0.981 |
| 45597184669 | `scratch/find-variant-u` | 289.7 | 283.3 | 0.978 |

Median 0.9795 (limit 1.05). Line 8 holds.

Group B, `base` = kit commit `e7d74bd2`, ratio product/kit:

| Run | `ref` | find p50 base / head (us) | find | skewed find | atomic_group_large | batch_append | group_blobs | read_stream | read_feed_page |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45597190946 | `perf/find-measurement-kit` | 408.7 / 382.5 | 0.936 | 0.922 | 1.009 | 0.998 | 1.008 | 0.991 | 0.999 |
| 45597196581 | `plan/find-kit-implemented` | 407.9 / 372.4 | 0.913 | 0.912 | 1.011 | 0.999 | 0.994 | 0.983 | 0.993 |
| 45597201828 | `plan/find-variants-recorded` | 181.4 / 155.7 | 0.859 | 0.788 | 1.011 | 0.988 | 1.011 | 1.020 | 1.021 |
| 45597207155 | `perf/find-prepared-statement` | 330.9 / 297.5 | 0.899 | 0.889 | 1.005 | 1.014 | 0.998 | 0.989 | 1.002 |
| Median | | | 0.906 | 0.9005 | 1.010 | 0.9985 | 1.003 | 0.990 | 1.0005 |

Line 9 holds (0.906 against at most 0.95). Line 10 holds for each of the six ids: the largest median is 1.010 (`postgres.atomic_group_large.latency`, limit 1.05) and the throughput median 0.9985 is above 1/1.05. Absolute p50 values differ between runs because each run lands on its own runner; only the ratio inside a run is compared.

The `KPI counters` job on the scratch refs of runs 45597166908 and 45597176653 is red by design (their variants regress `round_trips` and `tuples_read`); it does not touch the timing job, which succeeded in all 8 runs.

Counters on `be54a2dc` (pull request run 45597043056, `KPI counters`): `postgres.projection_find.parses` 0 (recorded 0), `postgres.projection_find.round_trips` 100, `postgres.projection_find_skewed.tuples_read` 10; green.

Baseline step 3b needed `eventlog-kpi record --counters --ratchet <ids>` (commit `73595b56`): the CI record path kept every recorded counter, so the first record dispatch (run 45585547025) changed only the revision and the time. The second (run 45592619683, `ratchet` = `postgres.projection_find.parses`) wrote `parses` 8 to 0 and nothing else.

Tests (Persistence proof, run 45597042892): the nine database tests of `find_statements` and the ten unit tests of `statement_cache` pass in the `eventlog-postgres` lib binary (79 tests, 18.06 s against 60 tests in 17.37 s before this story), the production proof reports 0 skipped. The libtest output prints no per-test time, so the 10 s total of line 4 is read from the binary's wall time.
