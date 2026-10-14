---
format: aep.planning-md/3
id: story:file-capture-after-a-write-costs-what-was-written
kind: story
status: draft
title: A capture after a write keeps one fold and costs what was written, not the history
relations:
- serves: vision:O2
revision: 1
---
## Outcome

A file capture that starts from the writer's verified view after a write keeps one folded state and
does work proportional to what was written since the view, not to the whole history.

## Why

The adversary review of story:file-capture-after-a-write-resumes (2026-10-14) left two judgement
notes, neither a defect:

- J1: two identical folded states can be kept at once during such a capture
  (`crates/eventlog-file/src/capture.rs:244` at the time of review).
- J2: the capture after a write is still O(history) in bytes compared and in the state cloned from
  the writer's view, though it no longer re-reads, re-chains, re-encodes or re-folds.

## Acceptance

- A cost case counts the folded states retained during a capture after a write: one.
- A cost case shows bytes compared and state cloned after one small write do not grow with the
  history (a store with 10x the frames costs the same within a stated bound), or the design page
  says why they must.
