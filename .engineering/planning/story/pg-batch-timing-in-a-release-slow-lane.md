---
format: aep.planning-md/3
id: story:pg-batch-timing-in-a-release-slow-lane
kind: story
status: draft
title: The batch_timing wall-clock budget runs in a release slow lane, not the debug step
summary: 1 s budget for a release build failed at 1.43 s and 1.32 s in the debug nextest step
revision: 1
---
## Outcome

`batch_timing::ten_thousand_reads_and_a_twenty_thousand_event_group_fit_their_budgets` runs only
in a release-build `slow` lane, and the debug test step no longer runs its wall-clock budget.

## Evidence (2026-11-23, e1-tests hand-back)

- The case asserts a 1 s wall-clock budget written for a release build.
- In the debug nextest step it failed in both runs: "over budget: 10,000 reads: 1.43 s" (load
  89->82) and 1.32 s (load about 150).

## Acceptance

- The debug test step does not run the case; the `slow` lane runs it in a release build and it
  passes there, time printed.
- The 1 s budget and its assertion are unchanged.

## Decision taken

D2 of e1-tests, option A (default).
