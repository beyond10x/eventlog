---
format: aep.planning-md/1
id: review-result:sql-blob-integrity-code-pass-2-supplied
kind: review-result
status: active
title: Existing second SQL examination confirms hidden SQLite semantics
relations:
- reviews: story:sql-blob-read-integrity
revision: 1
---
## Existing independent second examination

Exact submission: 209ac320a4c60391e04b18dd2b62e3c626c74325.
Root discovered the externally supplied Claude Code report in ESS evolution wave0006,
review-pass-2-claude/report.md on 2026-09-16. This records the existing second examination;
it is not a third pass or a new review dispatch. Historical platform interruptions are retained.
The report names its model as claude-fable-5-1, not the current worker default Opus.

Root verified test-patch.diff SHA256
14256ef7b2ebaab115f5181dbeafa2214a8f077d277b58207436ca5b3d544b41
and logs/10-sqlite-conformance-suite.log: 19 passed, 3 failed, exit101.
The report's production-proof and workspace gate rows contain placeholders. Those rows are
NOT executed gate evidence or acceptance. The finding and retained test diff are actionable.

## Finding and required correction

SQLite blob admission accepts hidden column COLLATE, primary-key ON CONFLICT and REFERENCES
clauses. The supplied regression demonstrates case-insensitive digest identity aliasing and
foreign-key cascade deletion. Prior required cases remain green in the affected suite.
This violates existing exact old/current physical-shape admission, so it stays in
story:sql-blob-read-integrity rather than becoming an incidental prerequisite or separate review.
Correct the general admission class for both legacy and current schemas, preserve all review
tests and existing behavior, and rerun exact cases, affected packages and both required gates.
No integration or completed acceptance is claimed by receipt of this report.

```findings
- file: crates/eventlog-sqlite/src/lib.rs
  line: 507
  category: acceptance
  severity: blocker
  verdict: CONFIRMED
  origin: undecided
  message: SQLite blob-shape admission accepts hidden COLLATE, primary-key ON CONFLICT and REFERENCES clauses; a NOCASE digest aliases identity and a foreign cascade deletes an admitted binding.
```
