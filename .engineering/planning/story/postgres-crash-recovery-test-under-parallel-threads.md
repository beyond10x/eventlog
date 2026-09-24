---
format: aep.planning-md/2
id: story:postgres-crash-recovery-test-under-parallel-threads
kind: story
status: draft
title: The native group crash-recovery test passes under parallel test threads
owner: eventlog
relations:
- informed_by: story:file-eventlog-verifies-once-per-open
revision: 2
---
## Outcome

`atomic_group::native_group_crash::every_native_group_boundary_recovers_one_complete_outcome` in
`crates/eventlog-postgres` passes when the crate's 18 lib tests run in parallel, as `cargo test
--workspace --locked` (the gate) runs them.

## Observation

Coordinator, 2026-09-21 00:42–00:48 CEST, PostgreSQL 17.6-alpine3.22 and 16-alpine, fresh
database per run, `EVENTLOG_TEST_POSTGRES_URL` set:

| run | result |
| --- | --- |
| `cargo test -p eventlog-postgres --lib` (parallel), at c698923 | FAILED 3 of 3 (17 passed, 1 failed, 1.4 s) |
| same, `-- --test-threads=1`, at c698923 | ok 3 of 3 (18 passed, 14 s) |
| the test alone, at c698923 | ok 3 of 3 (7–9 s) |
| `cargo test -p eventlog-postgres --lib` (parallel), at f802eb8 (accepted M1, this crate byte-identical) | FAILED 2 of 3 |

Failure, verbatim: `panicked at crates/eventlog-postgres/src/atomic_group.rs:597:13: assertion
`left == right` failed: group-event-1: no duplicate or missing group member`, `left: []`, right
the three recorded events. The test spawns `current_exe` with `--exact child` and
`EVENTLOG_POSTGRES_GROUP_CHILD_PREFIX` for each crash point; under parallel threads the retry's
`read_feed` sees no events at the first crash point. Mechanism not isolated.

Pre-existing: the crate is unchanged since f802eb8. Inferred, not verified: the M1 gate of
2026-09-18 ran under the `RUST_TEST_THREADS=1` standing cap recorded in
.ess-evolution/ORCHESTRATION.md (withdrawn 2026-09-19), which is why it was green there.

## Acceptance

- The mechanism named (a shared table, a shared global sequence, a prefix collision, or the child
  process's environment), with the failing run reproduced and the fix making 10 parallel runs green.
- `bash scripts/gate.sh` exit 0 without `RUST_TEST_THREADS=1`.

## Scope

cited: crates/eventlog-postgres/src/atomic_group.rs (native_group_crash module, lines 474–600).

## Gate condition, added 2026-09-21T00:55 CEST

## Gate condition, added 2026-09-21T00:55 CEST

The repository's own gate fixture (.github/workflows/persistence-proof.yml:27-37) runs with
`RUST_TEST_THREADS: 1`, so the failure above is outside the gate's defined conditions; it is a
weakness of the test's isolation, not a red gate. The acceptance stands: pass under parallel threads.
