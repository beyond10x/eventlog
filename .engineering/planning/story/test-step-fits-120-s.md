---
format: aep.planning-md/3
id: story:test-step-fits-120-s
kind: story
status: active
title: The workspace test step ends within 120 s and nextest enforces it
summary: eventlog-file 175.6 s and eventlog-postgres up to 166.5 s; add the run-wide 120 s limit once it fits
relations:
- serves: vision:O2
revision: 3
transitions:
- {from: "draft", to: "proposed", at: "2026-11-23T19:44:59Z", actor: "human:timo", revision: 2}
- {from: "proposed", to: "active", at: "2026-11-23T19:44:59Z", actor: "human:timo", revision: 3}
---
## Outcome

The gate's whole test step (`cargo nextest run --workspace --locked`) ends within 120 s on this
host, and `.config/nextest.toml` then fails the run when it goes over 120 s.

## Evidence (2026-11-23, e1-tests hand-back)

- `cargo nextest run -p eventlog-file`: 272 passed in 175.6 s at load about 130; up to 871 s of
  summed test time in a trial workspace run.
- `cargo nextest run -p eventlog-postgres`: 87.4 s at load 89->82, 166.5 s at load about 150.
  The `conformance` and `runtime_context` binaries run alone (`threads-required =
  "num-test-threads"`) because their `static EXCLUSIVE` lock orders nothing under nextest: about
  97 s of the 166.5 s.
- A trial `--workspace` run with a 120 s run-wide limit stopped with 416 of 968 tests not run.

## Acceptance

- `cargo nextest run --workspace --locked` ends within 120 s, measured twice, load stated.
- `.config/nextest.toml` carries the 120 s run-wide limit; a stand-in slow test proves the run
  fails over it.
- No test is removed to get there; a moved test names its new lane.

## Decision taken

D1 of e1-tests, option B (default): the 60 s per-test kill ships now; the run-wide 120 s limit
waits for this story so the gate is not red on every run.
