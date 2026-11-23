---
format: aep.planning-md/3
id: story:file-capture-drop-adversary-flaky-under-load
kind: story
status: implemented
title: eventlog-file adversary2 a_capture_dropped_while_it_retains_once_leaves_one_buffer fails under load
summary: adversary2_compare_once.rs:113 nothing to drop; failed 2 of 4 parallel runs and alone at integration
owner: eventlog
relations:
- serves: vision:O2
revision: 5
transitions:
- {from: "draft", to: "proposed", at: "2026-11-23T13:04:31Z", actor: "human:timo", revision: 3}
- {from: "proposed", to: "active", at: "2026-11-23T13:04:32Z", actor: "human:timo", revision: 4}
- {from: "active", to: "implemented", at: "2026-11-23T13:04:51Z", actor: "human:timo", revision: 5, decided_on: {"recorded":{"test_result":1}}}
---
## Outcome

`journal::adversary2_compare_once::a_capture_dropped_while_it_retains_once_leaves_one_buffer` in
`eventlog-file` passes every run under load, or is rewritten so that what it asserts does not depend
on the capture still being pending in the poll where it begins retaining.

## Evidence

The case fails at `crates/eventlog-file/src/adversary2_compare_once.rs:113` with:

```
the capture finished in the poll that began retaining once: nothing to drop at
```

The loop drops the capture only when a poll returns `Pending` with the retained count raised; under
load the capture completes in that same poll, so there is nothing to drop and the assertion fails.
The test exists on `main` (`adversary2_compare_once.rs:45`).

Runs that failed, logs in the coordinator's job folder (job `3bc48db8`, `tmp/`; not in this repository):

- `el-c2/parallel-after.log:32`, `el-c2/l56-test.log:94`: 2 of the last 4 parallel `eventlog-file`
  runs of agent el-c2 (core+file 313 pass, 1 fail).
- `el-integ/logs/test-eventlog-file.log:52` (12.4 s) and `el-integ/logs/rerun-file-lib.log:9`
  (11.1 s, run alone, `81 filtered out`): the integration gate of PR #62.

## Acceptance

The case passes 20 of 20 runs of the `eventlog-file` lib test binary in parallel with another build
or test on the shared pool, with no assertion weakened and no sleep added.
