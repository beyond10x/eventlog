---
format: aep.planning-md/3
id: story:projection-find-measurement-kit
kind: story
status: draft
title: 'KPI measurement kit for projection_find: skewed fixture, five ids, a CI record path'
summary: Valid whether the prepared-find gate G1 passes or fails; commit 1 is red UNRECORDED until baseline step 1b
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
revision: 6
---
## Outcome

The KPI runner can see the failure that a prepared `projection_find` must never have, and the
repository holds the baseline that proves it: a skewed-tenant fixture, five new KPI ids, and a CI
path that records the baseline on the runner. The kit is valid whether the prepared statement
passes its build gate G1 or fails it, because it measures head as it is today and changes no
product code. The product change is the story
`story:projection-find-holds-its-bound-with-a-prepared-statement`, which depends on this one.

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
- `eventlog-kpi record --counters`: rewrites counters only (no timing of the recording machine).
- A `record` dispatch input in `.github/workflows/kpi.yml` that runs the record on the CI runner
  and uploads `kpi/baseline.json` as a workflow artifact. A human commits the file; the workflow
  keeps `contents: read`.

## Commits and baseline steps

Baseline commits are named steps, never "in the same commit" as the code they record.

- Commit 1: the kit code, `kpi/kpis.json`, the workflow input. Against unchanged head, with no
  baseline for the five new ids. `KPI counters` is RED on it, by design: a defined, measured
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
need each other's result, they only collide on two paths, and a typed scope on both lets
`aep plan artifact waves` serialise them.

## Acceptance

Each line is observable alone.

1. The new scenarios run without error in an A/A dispatch of `KPI / KPI timing` with `base` and
   `head` both the 0.10.0 tag (`rounds` = 20). This is the pass condition: the job completes and
   prints a ratio for each new timing id; a scenario that errors on 0.10.0 fails the whole run.
2. On the CI runner (PostgreSQL 17.6) the fixture's E1 and E2 hold, and the job log prints the
   tuples read under each.
3. An A/A dispatch of head against head (`rounds` = 20) records the noise floor of
   `postgres.projection_find_skewed.latency` and `postgres.atomic_group_large.latency`: the ratio
   of each, per run, for at least two runs, written into this story's body. This is a recorded
   observation, not a pass or fail.
4. The `record` dispatch run produces an artifact `kpi/baseline.json` holding the five new ids.
5. After step 1b, `KPI counters` is green on the branch, and the baseline holds `parses` 8,
   `round_trips` 100 and the `tuples_read` value of the good plan.
6. Variant G (scratch branch, never merged): head's find prepared on every call from the first
   and `SET plan_cache_mode = force_generic_plan` added to `session_configuration`. A draft pull
   request from it makes `KPI counters` red on `postgres.projection_find_skewed.tuples_read`.
7. The same variant G, in a `workflow_dispatch` of `KPI / KPI timing` with `ref` = the kit branch,
   `base` = the kit commit, `head` = variant G, `rounds` = 20, prints REGRESSED for
   `postgres.projection_find_skewed.latency`.
8. Variant U (a throwaway cache on a scratch branch, no `plan_cache_mode` line, never merged) has
   its result recorded in this story's body: the `KPI counters` verdict and the A/B ratio on the
   skewed id. E3 (does `auto` flip on the skewed fixture) is recorded with it. Neither is
   required to be red; this is a recorded observation.
9. The run URLs of the A/A runs (acceptance lines 1 and 3), of the `record` run, of G and of U are
   in the pull request.

## Scope

`crates/eventlog-kpi`, `kpi/kpis.json`, `kpi/baseline.json`, `.github/workflows/kpi.yml`, and the
"The KPI gate" section of `AGENTS.md` if it is touched. No product code, no schema, no ESS change.

Every implementation brief names: the 60 s per-test bound and the 120 s test-step budget; the
10-minute job limit; no new Python in anything that stays in the repository (the record path and
CI steps are Rust or POSIX sh); scripts that measure or wait start without the `slot` prefix;
nothing runs on the shared workstation for timing.
