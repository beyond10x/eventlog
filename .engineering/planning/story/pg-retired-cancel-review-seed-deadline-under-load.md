---
format: aep.planning-md/3
id: story:pg-retired-cancel-review-seed-deadline-under-load
kind: story
status: implemented
title: eventlog-postgres retired_cancel_review fails under load with a Deadline in its seed
summary: register_inline in seeded_store hits Deadline; 4 of 10 el-h2 runs failed, two different cases
owner: eventlog
relations:
- serves: vision:O2
revision: 5
transitions:
- {from: "draft", to: "proposed", at: "2026-11-23T13:04:52Z", actor: "human:timo", revision: 3, decided_on: {"recorded":{"test_result":1}}}
- {from: "proposed", to: "active", at: "2026-11-23T13:04:52Z", actor: "human:timo", revision: 4, decided_on: {"recorded":{"test_result":1}}}
- {from: "active", to: "implemented", at: "2026-11-23T13:04:52Z", actor: "human:timo", revision: 5, decided_on: {"recorded":{"test_result":1}}}
---
## Outcome

The `eventlog-postgres` test binary `tests/retired_cancel_review.rs` passes every run under load. Its
seed step no longer fails with `Deadline { operation: "store operation" }`.

## Evidence

Agent el-h2, 2026-11-23, on `fix/review-el-h` (now part of PR #62). The failing line is the seed's
`store.register_inline(Arc::new(Tally)).await.unwrap()` in `seeded_store`
(`crates/eventlog-postgres/tests/retired_cancel_review.rs:122:50` at that time), so the failure is in
setup, not in what the cases assert:

```
called `Result::unwrap()` on an `Err` value: Deadline { operation: "store operation" }
```

Runs, logs in the review coordinator's job folder (job `3bc48db8`, `tmp/el-h2/`; not in this repository):

| log | result | failing case |
|---|---|---|
| `l68.log` | 7 pass, 1 fail, 12.1 s | `only_an_unsettled_retirement_sends_a_cancel_and_only_one` |
| `l68-rcr.log` | 7 pass, 1 fail, 15.0 s | `a_retirement_dropped_with_its_callers_runtime_does_not_strand_occupancy` |
| `rcr-twice.log.1` | 7 pass, 1 fail, 20.3 s | same |
| `rcr-twice.log.2` | 7 pass, 1 fail, 17.6 s | same |
| `rcr-base.log.1`, `.2`, `rcr-full.log`, `rcr-twice2.log.1`, `.2` | 8 pass, 8.0-9.6 s | none |
| `rcr-alone.log` | 1 pass, 4.7 s | none |

Two different cases failed at the same seed line, which points at the seed's deadline under load.

## Acceptance

`tests/retired_cancel_review.rs` passes 20 of 20 runs while another build or test runs on the shared
pool, with the deadline under test unchanged and no case's assertion weakened.
