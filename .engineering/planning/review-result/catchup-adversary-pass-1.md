---
format: aep.planning-md/1
id: review-result:catchup-adversary-pass-1
kind: review-result
status: active
title: Catch-up adversary pass 1
relations:
- reviews: story:reuse-postgres-connections-after-empty-catch-up
revision: 1
---
unit: story:reuse-postgres-connections-after-empty-catch-up at 15c049633363dd0f504c7ccaeb36ee01c1bb4bd6, base 32bf1596c853cdedfb5477e1f790c060c5bc1d13
verdict: nothing found
cases: executed 76→not rerun, red 0
origin: introduced 0 / pre-existing 0 / undecided 0
wrote-outside-worktree: none
needs-coordinator: none

1. git --no-pager diff --stat

(empty; no tracked file changes)

2. Cases added

None. No concrete failing scenario identified in this bounded review, so no speculative test was added.

3. Suite

Not run. Baseline 76 is the implementor's reported executed count, not a new adversary measurement. No test additions were made, and the charter prohibits running the suite before an adversarial case exists.

4. Judgement findings

None.

5. Attacks

- Read the complete four-line production change, three regressions, and mandatory production-proof roster against the story's acceptance and source base.
- Traced CatchUpRunner::run_once into run_catch_up and verified both successful no-work returns await transaction.rollback before marking the lease reusable.
- Traced query/rollback errors, bounded timeout cancellation, and task abortion: every path before successful rollback retains quarantine; settled is not reached across a failed await.
- Traced Lease::drop and concurrent shutdown: reuse still checks both client closure and pool closure; unsettled connection retirement retains occupancy until its driver stops.
- Checked empty-at-zero and empty-at-nonzero progress, contended projector lock behavior, later append/application, unchanged cursor/projection contents, and same-backend PID checks in the new regressions.
- Checked the proxy case with withheld ROLLBACK response: neither failure nor cancellation can reach settled, and the test observes quarantine before response release and drained occupancy after shutdown.
- Checked all three regression names enter the production-proof mandatory case roster.

6. Outside-worktree paths

None. This report is retained in the assigned tree's target/review-scratch/adversary-report.md. Own managed-worktree lease released at handoff.

```findings
[]
```
