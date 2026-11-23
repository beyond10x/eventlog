---
format: aep.planning-md/3
id: story:pg-batch-timing-in-a-release-slow-lane
kind: story
status: implemented
title: The batch_timing wall-clock budget runs in a release slow lane, not the debug step
summary: 1 s budget for a release build failed at 1.43 s and 1.32 s in the debug nextest step
relations:
- serves: vision:O2
revision: 4
transitions:
- {from: "draft", to: "proposed", at: "2026-11-23T19:44:58Z", actor: "human:timo", revision: 2, decided_on: {"recorded":{"test_result":1}}}
- {from: "proposed", to: "active", at: "2026-11-23T19:44:58Z", actor: "human:timo", revision: 3, decided_on: {"recorded":{"test_result":1}}}
- {from: "active", to: "implemented", at: "2026-11-23T19:44:58Z", actor: "human:timo", revision: 4, decided_on: {"recorded":{"test_result":1}}}
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
