---
format: aep.planning-md/1
id: review-result:rewrite-rule-adversary-pass-1
kind: review-result
status: active
title: Rewrite-rule repair adversary pass 1
relations:
- reviews: story:reject-postgres-rewrite-rules
revision: 1
---
unit: story:reject-postgres-rewrite-rules at 6bbb06d5a21412a8c51ae69e5462b4eb28d36460
verdict: nothing found
cases: executed 92→92, red 0
origin: introduced 0 / pre-existing 0 / undecided 0
wrote-outside-worktree: none
needs-coordinator: none

1. `git --no-pager diff --stat`: empty. No test or implementation file changed during this review.

2. No failing case identified or added.

3. No suite executed by this review. The unchanged count comes from the implementor's completed production gate in schema-repair/implementor-report.md: 92 passed, zero failed/skipped, formatting/clippy green. This review claims no additional runner evidence and does not label the implementor's run its own. The four recorded predicate-mutant failures were read as existing evidence.

4. No confirmed judgement findings. Complete diff reviewed against 5c1ff2690f07ec0b91f14cce307fff2a6b65ddca.

5. Attacked and could not break:
- All eleven durable tables, including snapshot generation metadata, pass through the same catalog shape checker. The class test enumerates actual owned tables rather than maintaining a competing roster.
- Local migration and hosted application admission both reach the new pg_rewrite refusal through existing common shape checks.
- Explicit projection migration now invokes shape after its DDL, within the existing migration transaction; refusal rolls back that transaction. create_projections and inline registration subsequently retain their existing exact shape/roster checks.
- Refusal does not depend on a particular rule name, enabled state, or command kind. It rejects catalog rewrite entries for the exact resolved relation.
- The rule-free control preserves the original durable command receipt and deduplicated retry.
- No persisted schema, checksum, public API, or role capability changed in this unit. Known input-validation work was excluded as assigned to another unit.

6. No files written outside the assigned worktree. This report is retained at target/review-scratch/schema-repair/adversary-report.md. Review lease released; coordinator owns cleanup and publication.

```findings
[]
```
