---
format: aep.planning-md/3
id: story:sqlite-and-file-find-timings-hold-an-a-a-run
kind: story
status: draft
title: SQLite and file projection_find timings hold an A/A run
summary: Two A/A runs gave 2.443 and 2.170 for sqlite and file find in one run and 0.99 in the other
revision: 1
---
## Outcome

An A/A run of the KPI timing job (the same revision as base and head) stays inside 0.95-1.05 for
`sqlite.projection_find` and `file.projection_find`, or the two timings carry a noise bound that
says why they cannot.

## Why

Two A/A runs of 0.10.0 (c0f961dc) on the CI runner, 20 rounds each, `KPI timing` by
`workflow_dispatch` (runs 45330231281 and 45332551024):

- `sqlite.projection_find`: 2.443 in run 1 (base spread 153.8 %, head 62.2 %), 0.991 in run 2.
- `file.projection_find`: 2.170 in run 1 (base spread 131.1 %, head 61.3 %), 0.994 in run 2.
- `postgres.projection_find` (1.007, 0.982) and `tree.projection_find` (1.003, 0.999) held in both.

A timing that moves 2.4x between two runs of one revision cannot judge a change of 1.05x. The
`ab.max_ratio` limit of 1.5 hides it.

## Acceptance

- Cause of the run 1 outliers named from the per-round p50 values of both sides (one slow round,
  a cold cache of the first round, or a systematic first-round effect).
- Two further A/A runs on the runner, 20 rounds, both inside 0.95-1.05 for both timings, or a
  bound for them recorded in `kpi/kpis.json` with the A/A numbers.

## Scope

- `crates/eventlog-kpi` (timing rounds), `kpi/kpis.json`. No product code.
