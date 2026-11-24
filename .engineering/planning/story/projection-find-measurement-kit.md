---
format: aep.planning-md/3
id: story:projection-find-measurement-kit
kind: story
status: active
title: 'KPI measurement kit for projection_find: skewed fixture, five ids, a CI record path'
summary: Skewed fixture, five ids, a CI record path and the variants G, U and V1; commit 1 is red UNRECORDED until baseline step 1b
relations:
- serves: vision:O2
- informed_by: story:pg-projection-find-back-within-1-05
scope:
- confidence: cited
  path: .github/workflows/kpi.yml
- confidence: cited
  path: AGENTS.md
- confidence: cited
  path: crates/eventlog-kpi
- confidence: cited
  path: kpi/baseline.json
- confidence: cited
  path: kpi/kpis.json
revision: 11
transitions:
- {from: "draft", to: "proposed", at: "2026-11-24T15:41:03Z", actor: "agent:eventlog", revision: 10, decided_on: {"recorded":{"test_result":5}}}
- {from: "proposed", to: "active", at: "2026-11-24T15:41:03Z", actor: "agent:eventlog", revision: 11, decided_on: {"recorded":{"test_result":5}}}
---
## Outcome

The KPI runner can see the failure that a prepared `projection_find` must never have, and the
repository holds the baseline that proves it: a skewed-tenant fixture, five new KPI ids, and a CI
path that records the baseline on the runner. Except for the V1 line (acceptance line 14), the kit is
valid whether the prepared statement passes its build gate G1 or fails it, because it measures
head as it is today and changes no product code. The kit also owns the measurement of G1 itself,
the scratch branch V1 (acceptance line 14). The product change is the story
`story:projection-find-holds-its-bound-with-a-prepared-statement`, which depends on this one and
starts when line 14 is met.

## Why

Design `docs/design/projection-find-planning.md` section 5 (fixture, KPI cases, counters, red
proof) and section 10 steps 3 and 4. The prepared statement is safe only under
`plan_cache_mode = force_custom_plan`; a generic plan on a skewed tenant reads about 19,000 tuples
instead of about 12 (CI spike run 45368249583: custom 0.0128 ms and 4 blocks, forced generic
2.043 ms and 417 blocks, 160x). No existing KPI sees that. Splitting the kit from the product
means the red-capable proof exists before the change it guards, and the work is not lost if G1
fails and the owner takes the fallback of
`decision-blocker:find-fallback-if-the-prepared-gate-fails`.

## What is built (design 5.2, 5.3, 5.4, 9)

- The skewed fixture (design 5.2): direct SQL, at most 30,000 rows, one large tenant and small
  tenants, `rare` rare table-wide, `VACUUM (ANALYZE)` after seeding. The case checks the
  conditions with `EXPLAIN (ANALYZE, BUFFERS)` on every run and errors, not a regression verdict,
  when they do not hold: E1 (custom plan, index scan, at most 50 tuples read) and E2 (generic
  plan, primary-key walk, at least 100x E1's tuples). E3 (does `auto` flip on this fixture) is
  recorded only.
- Five ids in `kpi/kpis.json`:
  - `postgres.projection_find_skewed.latency` (timing, p50 only, 16 finds of `rare` limit 10 in
    the large tenant per round);
  - `postgres.projection_find_skewed.tuples_read` (counter, per find, from
    `pg_stat_user_tables.seq_tup_read` plus `pg_stat_user_indexes.idx_tup_read`, procedure of
    design 5.3);
  - `postgres.projection_find.round_trips` (counter, 100 = 1.00 on a warm find);
  - `postgres.projection_find.parses` (counter, Parse messages in 8 warm finds; 8 on unchanged
    head, one per find);
  - `postgres.atomic_group_large.latency` (timing).
- `postgres.atomic_group_large.latency` is one `append_group` of 6,500 single-event streams. Why
  that size: a prepared statement under `auto` can switch to a generic plan at its sixth
  execution, and the group's statements run once per `STATEMENT_BATCH` of 1,000 items
  (`crates/eventlog-postgres/src/lib.rs:73`), so a group of at most 5,000 items never reaches the
  sixth execution and the scenario would be green by construction. Admitted limits, read at
  `71f84dbb`: `MAX_EVENTS_PER_APPEND` is 1,024 per entry
  (`crates/eventlog-core/src/lib.rs:78`), and neither `AppendGroup::fingerprint`
  (`crates/eventlog-core/src/atomic_group.rs:32-60`) nor the PostgreSQL group code
  (`crates/eventlog-postgres/src/atomic_group.rs`) caps the number of entries. A group of 6,500
  one-event streams is therefore admitted and runs 7 chunks per statement. The transaction
  timeout is 10 s (`crates/eventlog-postgres/src/pool.rs:230`); the A/A run on the runner shows
  whether the group commits inside it.
- Three variants on scratch branches, never merged, none carried into the product: G (a bad
  cache, forced generic plan, the red proof), U (a throwaway cache with `auto`, an observation) and
  V1 (the prepared find only under `force_custom_plan`, the measurement of the build gate G1:
  at most 1.03x of 0.10.0 per `docs/design/projection-find-planning.md` 0.4).
- `eventlog-kpi record --counters`: rewrites counters only, so the baseline file can hold the
  three new counters (`tuples_read`, `round_trips`, `parses`). The two new timings
  (`postgres.projection_find_skewed.latency`, `postgres.atomic_group_large.latency`) are judged by
  the A/B dispatch against a base revision, not by the baseline file.
- A `record` dispatch input in `.github/workflows/kpi.yml` that runs the record on the CI runner
  and uploads `kpi/baseline.json` as a workflow artifact. A human commits the file; the workflow
  keeps `contents: read`.

## Commits and baseline steps

Baseline commits are named steps, never "in the same commit" as the code they record.

- Commit 1: the kit code, `kpi/kpis.json`, the workflow input. Against unchanged head, with no
  baseline for the three new counters. `KPI counters` is RED on it, by design: a defined, measured
  counter absent from the baseline is `Unrecorded` and fails the gate
  (`crates/eventlog-kpi/src/compare.rs:161`, `:21`). The pull request is not merged on commit 1.
- Step 1b: a separate commit of the `kpi/baseline.json` that the `record` dispatch uploaded. After
  1b, `KPI counters` is green.
- The product story adds its own named step 3b (re-recording `parses` to 0) after its product
  commit.

## Order against other drafts

`story:sqlite-and-file-find-timings-hold-an-a-a-run` (draft) also changes `crates/eventlog-kpi`
and `kpi/kpis.json`. Order: this kit first, so that the new timing ids and the A/A noise floors
recorded here exist before that story changes timing rounds. No `depends_on` edge: the two do not
need each other's result, they only collide on two paths. Both now carry a typed scope, and
`aep plan artifact waves --status draft` shows the kit in wave 1 and that story in wave 2, with
collisions reported on `crates/eventlog-kpi` and `kpi/kpis.json`.

## Acceptance

Each line is observable alone. Every line that needs a CI run says "run URL named in the pull
request": the URL goes in the pull request, not in an acceptance line of its own.

1. The new scenarios run without error in an A/A dispatch of `KPI / KPI timing` with `base` and
   `head` both the 0.10.0 tag (`rounds` = 20). This is the pass condition: the job completes and
   prints a ratio for each new timing id; a scenario that errors on 0.10.0 fails the whole run.
   Run URL named in the pull request.
2. E1 and E2 hold on the CI runner (PostgreSQL 17.6): after step 1b, the step "Counters against
   kpi/baseline.json" of the `KPI counters` job in `.github/workflows/kpi.yml`
   (`bash scripts/gate.sh --kpi`) completes without the skewed-fixture case raising an E1 or E2
   error (a condition that does not hold is an error, not a regression verdict). Run URL named in
   the pull request.
3. The log of that same step prints, for E1 and E2, the tuples read under each condition (the
   custom-plan number and the forced-generic number). Run URL named in the pull request.
4. An A/A dispatch of head against head (`rounds` = 20) records the noise floor of
   `postgres.projection_find_skewed.latency` and `postgres.atomic_group_large.latency`: the ratio
   of each, per run, for at least two runs, written into this story's body. This is a recorded
   observation, not a pass or fail. Run URLs named in the pull request.
5. A `workflow_dispatch` of `KPI` with the new `record` input produces a workflow artifact
   `kpi/baseline.json` that has a value for each of `postgres.projection_find_skewed.tuples_read`,
   `postgres.projection_find.round_trips` and `postgres.projection_find.parses`. Run URL named in
   the pull request.
6. After step 1b, `KPI counters` is green on the branch. Run URL named in the pull request.
7. After step 1b, `kpi/baseline.json` holds a numeric value for each of the three counter ids.
   Expected: `postgres.projection_find.parses` 8 and `postgres.projection_find.round_trips` 100;
   `postgres.projection_find_skewed.tuples_read` is the number the run records in 1b. The line
   checks that each id has a value, not a specific number.
8. The group of `postgres.atomic_group_large.latency` has its size in one constant of
   `crates/eventlog-kpi/src`, at least 6,000 single-event streams, and a unit test asserts that
   the constant is above 5 x `STATEMENT_BATCH` (the test names its copy of 1,000 and
   `crates/eventlog-postgres/src/lib.rs:73`, so the sixth execution of the chunked statement is
   reached).
9. The scenario asserts that the group makes at least 6 executions of the chunked statement, read
   from the counting proxy the runner already uses (Execute messages of the group's repeated
   statement), and errors, not regresses, when it sees fewer. The count is printed in the job log.
   Run URL named in the pull request.
10. `postgres.atomic_group_large.latency` is part of the A/B: the A/A runs of lines 1 and 4 print a
    ratio for it, so a worse head turns it REGRESSED. Run URLs named in the pull request.
11. Variant G (scratch branch, never merged): head's find prepared on every call from the first
    and `SET plan_cache_mode = force_generic_plan` added to `session_configuration`. A draft pull
    request from it makes `KPI counters` red on `postgres.projection_find_skewed.tuples_read`.
    Run URL named in the pull request.
12. The same variant G, in a `workflow_dispatch` of `KPI / KPI timing` with `ref` = the kit
    branch, `base` = the kit commit, `head` = variant G, `rounds` = 20, prints REGRESSED for
    `postgres.projection_find_skewed.latency`. Run URL named in the pull request.
13. Variant U (a throwaway cache on a scratch branch, no `plan_cache_mode` line, never merged) and
    E3 are recorded: the pull request body or this story's body contains the `KPI counters`
    verdict of variant U, the A/B ratio of U on `postgres.projection_find_skewed.latency` and the
    E3 result (does `auto` flip to a generic plan on the skewed fixture), each with its run URL.
    They are obtained the way G's are: a draft pull request from the scratch branch for the
    `KPI counters` verdict, and a `workflow_dispatch` of `KPI / KPI timing` with `base` = the kit
    commit and `head` = U for the ratio. Neither U nor E3 is required to be red; this is a
    recorded observation.
14. Variant V1 (build gate G1; the third throwaway cache on a scratch branch after G and U, never
    merged): head's find prepared per connection under session-wide
    `plan_cache_mode = force_custom_plan`, nothing else. Four dispatched 20-round
    `KPI / KPI timing` runs with `base` = the 0.10.0 tag and `head` = V1: the median of the ratio
    of `postgres.projection_find.latency` p50 is at most 1.03. Run URLs named in the pull request.
    If V1 fails, this line is not met, the product story does not start, and the choice is the
    owner's decision D5 (`decision-blocker:find-fallback-if-the-prepared-gate-fails`); the
    kit's other lines are unaffected.

## Scope

`crates/eventlog-kpi`, `kpi/kpis.json`, `kpi/baseline.json`, `.github/workflows/kpi.yml`, and the
"The KPI gate" section of `AGENTS.md` if it is touched. No product code, no schema, no ESS change.

Every implementation brief names: the 60 s per-test bound and the 120 s test-step budget; the
10-minute job limit; no new Python in anything that stays in the repository (the record path and
CI steps are Rust or POSIX sh); scripts that measure or wait start without the `slot` prefix;
nothing runs on the shared workstation for timing.

## Measured on the runner

Pull request: https://github.com/beyond10x/eventlog/pull/79 (draft). Commit 1 `32668a6c`, baseline step 1b `c8e4a884`, head `d7cc0d26`.

Head against head, 20 rounds, on the CI runner (ratio of p50, head over base; limit 1.50):

| run | head | `postgres.projection_find_skewed.latency` | `postgres.atomic_group_large.latency` |
| --- | --- | --- | --- |
| https://github.com/beyond10x/eventlog/actions/runs/45405997907 | `c8e4a884` | 1.007 | 0.993 |
| https://github.com/beyond10x/eventlog/actions/runs/45411894378 | `d7cc0d26` | 1.020 | 1.004 |

The A/A against the 0.10.0 tag (https://github.com/beyond10x/eventlog/actions/runs/45402541977, base `c0f961dc`, head `c8e4a884`) printed 1.073 and 0.993, so both new scenarios run on 0.10.0. Observed, not a pass or fail.

Skewed fixture on the runner (PostgreSQL 17.6), same lines in every `KPI counters` run:

- E1, custom plan: Index Scan on the digest index, estimated 8 rows, actual 10 rows, 10 tuples read, 8 blocks.
- E2, forced generic plan: Index Scan on the primary key, estimated 155 rows, actual 10 rows, 25,000 tuples read, 420 blocks, 2,500 x E1.
- E3, recorded only: after 6 executions under `plan_cache_mode = auto`, 0 generic and 6 custom plans; `auto` did not flip on this fixture.

Recorded counters (1b): `postgres.projection_find_skewed.tuples_read` 10, `postgres.projection_find.round_trips` 100, `postgres.projection_find.parses` 8. The large group of 6,500 streams ran in about 1.27 s and the chunked statement executed 7 times.
