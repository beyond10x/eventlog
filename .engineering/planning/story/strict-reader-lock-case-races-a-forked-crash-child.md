---
format: aep.planning-md/3
id: story:strict-reader-lock-case-races-a-forked-crash-child
kind: story
status: implemented
title: The strict-reader lock case fails on its own control when a crash child inherits the descriptor
summary: capture.rs:340 try_lock returns WouldBlock; reproduces at c698923, 3 in 20 under taskset
owner: eventlog
relations:
- informed_by: story:file-eventlog-verifies-once-per-open
- serves: vision:O2
revision: 5
transitions:
- {from: "draft", to: "proposed", at: "2026-10-14T07:22:30Z", actor: "human:timo", revision: 3}
- {from: "proposed", to: "active", at: "2026-10-14T07:22:30Z", actor: "human:timo", revision: 4}
- {from: "active", to: "implemented", at: "2026-10-14T20:52:49Z", actor: "human:timo", revision: 5, decided_on: {"recorded":{"test_result":1}}}
---
## Outcome

`capture::tests::the_strict_reader_holds_the_writer_lock_for_its_whole_life` passes every run, or
the test is rewritten so that the thing it asserts does not depend on a race it cannot control.

## Why

2026-09-21, correction round 1 of story:file-eventlog-verifies-once-per-open. The case fails on its
own **control** — the `try_lock` at `crates/eventlog-file/src/capture.rs:340` returns `WouldBlock`
when it should succeed. The cause named by the implementor: a crash-child subprocess forked inside
the window shares the open file description, so it holds the `flock` until it `exec`s.

Measured under `taskset -c 0,1`, which widens the window:

| state | failures |
| --- | --- |
| base c698923 | 1 in 60 |
| with the correction | 3 in 20 |

It reproduces at c698923, so it is **not** introduced by the correction — the correction changes
the timing, not the mechanism. It was outside the correction's file assignment and was left
standing rather than patched around.

## Acceptance

- The mechanism is confirmed by observation rather than by reading: show the child process holding
  the inherited descriptor, or show the failure disappearing when the descriptor is closed or
  marked `FD_CLOEXEC` before the fork.
- The case passes 60 of 60 under `taskset -c 0,1`, the condition that reproduced it.
- No assertion of the case is weakened and no sleep is added to hide the window.

## Measured again, 2026-09-21 (unit 6 correction round 1)

While diagnosing a flaky new case of its own, the correction found the same inherited-descriptor
mechanism and measured the pre-existing case at **3 failures in 45** `eventlog-file` lib-lane runs.
Its own new case, written around the mechanism, failed 0 of 45. The pre-existing case was left
standing rather than edited inside that unit; this story remains the place to fix it. Source:
`unit-6-capture-verified/correction-1-report.md` in wave-validate-v2-20260920.
