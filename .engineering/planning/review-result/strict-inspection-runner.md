---
format: aep.planning-md/2
id: review-result:strict-inspection-runner
kind: review-result
status: active
title: Independent proof runner identity timing review
relations:
- reviews: story:strict-read-only-history-inspection
revision: 1
---
unit: production-proof runner capture ordering; working tree on 5478e87
verdict: nothing found
cases: executed 0→0, red 0; static review only
origin: introduced 0 / pre-existing 0 / undecided 0
wrote-outside-worktree: assigned runner-review.md and managed lease metadata only
needs-coordinator: corrected production gate result

Inherited diff for the reviewed file: `1 file changed, 30 insertions(+), 1 deletion(-)`. Reviewer-authored source/test diff: none. The inherited required-case additions are outside this narrow review; the assigned correction moves one existing executable read and adds its explanation.

No finding in the capture-order correction. `crates/eventlog-postgres/examples/production-proof.rs:39` reads the executable bytes before the nested Cargo invocation at line 40. The retained `Vec<u8>` supplies `Sha256::digest(binary)` at line 215; it is not reread from a path that Cargo can subsequently remove or replace. A failed initial read still returns an error through `?`. The nested command, status/count/required-case/skipped checks at lines 188–194, and final refusal at line 220 remain unchanged by this move.

The earlier retained gate log ends with `production proof refused: No such file or directory (os error 2)` after the backend suite. Moving capture before that runner's own nested build addresses the reported ordering failure while retaining the pre-build runner artifact as the hash input. No assertion that missing identity can be ignored, defaulted or substituted was introduced.

Limits: no source edits, new probes, Cargo execution or live-store actions. The coordinator's corrected production gate was still running during this review; its result is not claimed here. This reviews the observed nested-Cargo replacement sequence, not arbitrary unrelated concurrent executable replacement. Sole report output is the coordinator-assigned `eventlog-inspection/runner-review.md`; raw paths are omitted from this public-ready copy.

Owners: coordinator — runner correction and corrected production gate; reviewer — read-only ordering review and this report.

```findings
[]
```
