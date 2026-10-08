---
format: aep.planning-md/3
id: story:capacity-proof-counts-a-ninth-connection
kind: story
status: active
title: The comparative capacity proof intermittently observes nine client connections against a budget of eight
relations:
- serves: vision:O2
scope:
- confidence: inferred
  path: CHANGELOG.md
- confidence: inferred
  path: crates/eventlog-postgres/examples/capacity-sweep.rs
- confidence: inferred
  path: crates/eventlog-postgres/examples/capacity.rs
- confidence: inferred
  path: crates/eventlog-postgres/examples/observation/metrics_collector.rs
- confidence: inferred
  path: crates/eventlog-postgres/src/pool.rs
- confidence: inferred
  path: docs/
- confidence: inferred
  path: ess/
revision: 5
transitions:
- {from: "draft", to: "proposed", at: "2026-10-08T10:55:56Z", actor: "human:timo", revision: 3}
- {from: "proposed", to: "active", at: "2026-10-08T10:55:56Z", actor: "human:timo", revision: 4}
---
## Outcome

The required comparative capacity lane fails only when the candidate exceeds its declared connection budget, and never because a sample counts a connection that no pool holds.

## Evidence

`connections_max_excluding_observer <= 8` (`crates/eventlog-postgres/examples/capacity-sweep.rs:193-195`) is the only failing envelope check in every red run read:

| run | commit | configuration | connections | p99 µs | max µs |
|---|---|---|---|---|---|
| 37764623778 | fc773acb (main, 0.8.2) | candidate-8-uniform | 9 | 277,391 | 281,366 |
| 37764623778 | fc773acb | candidate-32-hot | 9 | 875,659 | 1,764,711 |
| 37762409486 attempt 1 | 01c9a62c (release/0.8.2) | capacity-run failed | not read | | |
| 36218101779 | main, 2026-09-26 | candidate-32-hot | 9 | 861,505 | 1,708,812 |
| 36212123349 | main, 2026-09-26 | candidate-32-uniform | 9 | 1,255,029 | 1,587,601 |

Every latency bound held (p99 and max <= 2,000,000 µs); correctness violations, refusals, conflicts, deduplications, PostgreSQL and cgroup failures were 0. In green runs 37761141359, 37762153989 and 37707950466 (0.8.1) every candidate configuration with concurrency 8 or 32 observes exactly 8: the lane runs at its limit. In 37764623778 candidate-8-uniform the 9 is 1 of 242 samples (sample 100, 2,216 ms: active 1, idle 4, idle in transaction 4).

The candidate is two worker processes, each `PostgresEventStore::connect` with `PoolOptions::default()` (`max_connections: 4`, `crates/eventlog-postgres/src/pool.rs:87`), so neither pool can lease a fifth connection.

## Hypothesis (not verified)

A pool retires a connection (quarantine of an unsettled lease, or a closed client) and connects its replacement after the old driver stopped (`pool.rs` `closed_idle_driver_is_joined_before_replacement_connects`). The server backend of the old connection exits asynchronously after the socket closes, so one 20 ms sample of `pg_stat_activity` can list both. If true, the client-side budget held and the sample counted an exiting backend.

## Acceptance

- The lane records, per configuration, the retirements and replacements each worker pool made, and for any sample over budget the `pid` and `backend_start` of each counted backend, so the red can be attributed.
- Either the over-budget sample is shown to be an exiting backend and the check counts only backends a pool can still use, with the reason stated in `docs/`; or a pool is shown to hold more than `max_connections`, and that is fixed test-first in `pool.rs`.
- The connection check is not loosened by changing 8 to 9.

## Cause (verified)

The ninth backend is the CI service container's health check, not a pool. `.github/workflows/persistence-proof.yml` runs `pg_isready -U postgres` every 5 s; each probe opens a short-lived client backend that the collector counted with the workload.

| observation | result |
|---|---|
| 11 local comparative proofs, no health check | 0 samples over 8; every candidate pool opened 4, retired 0, replaced 0 (`PoolChurn`) |
| 1 proof with `pg_isready` every 100 ms, before the fix | candidate-32-hot read 9; the extra backend (pid 2595) was idle 1.2 ms after `backend_start` and absent in the next sample; the 8 pool backends started 4-5 s earlier; pools retired 0 |
| 3 proofs with the probe loop, after the fix | all-backend maximum 9 in 2 runs; workload maximum 8; 12/12 configurations valid in each |

The hypothesis in this story (retirement overlapping a replacement) did not hold: no pool retired a connection in any run.

## Fix

Workers of both adapters connect with `application_name=eventlog-capacity-workload`; the budget check reads the workload maximum and fails at 0 or above 8. The all-backend count and per-backend attribution stay in the receipt. `tests/capacity_budget.rs` holds 9 tagged backends plus one bystander and requires the check to refuse it; a stub of the old all-backend rule fails 2 of its 4 cases. Reason recorded in `docs/design/comparative-capacity-connection-budget.md`.
