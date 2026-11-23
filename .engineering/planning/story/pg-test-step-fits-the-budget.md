---
format: aep.planning-md/3
id: story:pg-test-step-fits-the-budget
kind: story
status: draft
title: 'The eventlog-postgres test step fits the budget: conformance binary under 120 s, deadline test green under load'
summary: conformance binary 110-134 s under load; batch-read deadline test fails under load on main
owner: eventlog
revision: 2
---
## Outcome

The `eventlog-postgres` test step fits the budget of the beyond10x folder rules (`AGENTS.md` § Tests stay fast: 60 s per test, 120 s per step): the
`tests/conformance.rs` binary runs well inside 120 s under load, and
`a_batch_of_reads_that_each_fit_the_deadline_is_not_refused_for_its_size` passes under load.

## Evidence

Conformance binary (`tests/conformance.rs`), logs in the review coordinator's job folder (job `3bc48db8`, `tmp/`; not in this repository):

| log | run | time |
|---|---|---|
| `el-d2/t18-pg.log:208` | agent el-d2, after L61, load about 100 | 53 pass, 110.03 s |
| `el-h2/final-pg.log` (binary from line 144) | agent el-h2 final gate, load about 150 | 47 pass, 1 fail (TLS env), 134.38 s |
| `el-integ/logs/test-eventlog-postgres-pw.log` | PR #62 integration gate | 48 pass, 7 fail, 123.29 s |
| `el-integ/logs/test-eventlog-postgres-trust.log` | same gate, trust auth | 54 pass, 1 fail, 53.91 s |

The same gate's `test-eventlog-postgres` step ran 426 s and 308 s (`el-integ/logs/summary.txt`). The
coordinator also recorded a gate test step of 182 s over the 120 s budget (`assignment.md` line 78).

Deadline test: on `main` (`b8582219`,
`crates/eventlog-postgres/tests/adversary_batch_read_and_append.rs:76`) it fails under load. Agent
el-d, `el-d/full1.log:151`:

```
read_many refused 72 reads after 1.723568066s with Backend("PostgreSQL connection unavailable"),
although each of them answers separately inside the 762.484875ms deadline (slowest separate read 152.496975ms)
```

The coordinator recorded 552 ms against a 287 ms deadline in another run (`assignment.md` line 78);
el-b saw it too (`el-b/final-eventlog-postgres.log:124`). PR #62 (`fix/review-findings`, open)
carries the rewrite by agent el-gate: `e58a2de5` (deadline accounting proved on a `pg_sleep` floor,
not wall-clock calibration; new negative case) and `0d7cc60a` (lock-wait test released on a floor via
`pg_stat_activity`), 5 of 5 and 7 of 7 x3 under load. It is not on `main` yet, and connection setup
under host load was left to be rechecked at the final gate.

## Acceptance

On a checkout that contains PR #62, `cargo nextest run -p eventlog-postgres` under a load of about
100 runs each test under 60 s, prints its ten slowest tests, finishes the conformance binary in under
120 s, and passes `a_batch_of_reads_that_each_fit_the_deadline_is_not_refused_for_its_size` in 5 of 5
runs.
