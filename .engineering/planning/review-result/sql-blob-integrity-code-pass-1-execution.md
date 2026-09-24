---
format: aep.planning-md/2
id: review-result:sql-blob-integrity-code-pass-1-execution
kind: review-result
status: active
title: Coordinator execution confirms two independently authored SQL regressions
relations:
- reviews: story:sql-blob-read-integrity
- informed_by: review-result:sql-blob-integrity-code-pass-1-interrupted
revision: 1
---
unit: story:sql-blob-read-integrity at e52572d963781c3af6da8daed5de4522502e4683
verdict: CONFIRMED — two prepared regressions fail
cases: SQLite conformance executed 16→18, red 2
origin: introduced 0 / pre-existing 0 / undecided 2
wrote-outside-worktree: coordinator-owned local execution logs and assigned temporary fixtures
needs-coordinator: correct both failures before further acceptance

## Provenance

This is coordinator execution of the two unchanged test cases authored by the first Sol reviewer.
The reviewer was interrupted by automatic safety checks before execution; its factual incomplete
report remains immutable as review-result:sql-blob-integrity-code-pass-1-interrupted. This report
does not claim the reviewer ran commands or returned a passing verdict. It completes the runtime
part of that same first code examination, not a new independent attack.

Root ran each case alone, then the affected suite. The test source digest compared unchanged
before and after all three commands. Production source remained at the submitted commit. Base
behavior was not separately executed, so both origins remain undecided. Both requirements are
explicitly in this story's selected contract regardless of historical origin.

## Test-only diff

```text
 crates/eventlog-sqlite/tests/conformance.rs | 103 ++++++++++++++++++++++++++++
 1 file changed, 103 insertions(+)
```

Patch SHA256: a60ae790affc3a62d74ffaee5077972ac7ca032c79b91366a68a33db1d9a9c8b.
Direct complete outputs and own exit files are retained under
local-evidence:ess-evolution/waves/0006-eventlog-sql-integrity/review-pass-1-execution/.

## Exact cases and affected suite

`cargo test --locked -p eventlog-sqlite --test conformance review_caught_malformed_blob_metadata_poisons_sqlite_inline_append -- --exact --nocapture`
exited 101: 0 passed, 1 failed, 0 ignored, 17 filtered. The assertion at tests/conformance.rs:516
received Ok(AppendResult) after the inline projector caught a get_blob error from non-integer
persisted byte_count. The returned result contained the newly committed version-1 event. This
violates the required poisoned-transaction refusal before an append can commit.

`cargo test --locked -p eventlog-sqlite --test conformance review_sqlite_admission_requires_an_enforced_integrity_check -- --exact --nocapture`
exited 101: 0 passed, 1 failed, 0 ignored, 17 filtered. The assertion at tests/conformance.rs:710
observed successful open of a seven-column blob table whose constraint name contains the expected
CHECK text but whose actual enforced expression is CHECK (1). The fixture preserves the expected
columns and primary key; SQL substring presence did not prove the required physical constraint.

Only after both exact cases ran, `cargo test --locked -p eventlog-sqlite --test conformance`
exited 101: 16 passed, 2 failed, 0 ignored, 0 filtered, 18 executed. The same two cases failed and
all 16 submitted conformance cases passed. This rules out compilation failure or an unselected case
as the source of either result. No SQL production store or external application was involved.

## Findings

| File and line | Verdict | Origin | Finding |
| --- | --- | --- | --- |
| crates/eventlog-sqlite/src/lib.rs:2270 | CONFIRMED | undecided | A caught blob-row decoding error bypasses the callback poison marker and allows the append transaction to commit. |
| crates/eventlog-sqlite/src/lib.rs:532 | CONFIRMED | undecided | Raw SQL substring matching accepts a blob table without the required enforced integrity constraint. |

The first path is reached by an ordinary registered inline projector calling get_blob and handling
its error; the second by SqliteEventStore::open on an existing owner database. Source inspection
and the actual public-API fixtures establish reachability. Correct the general paths, preserve both
assertions and all earlier checks, and rerun exact cases, affected suites and the full production gate.

```findings
- file: crates/eventlog-sqlite/src/lib.rs
  line: 2270
  category: acceptance
  severity: blocker
  verdict: CONFIRMED
  origin: undecided
  message: A caught blob-row decoding error bypasses the callback poison marker and allows the append transaction to commit.
- file: crates/eventlog-sqlite/src/lib.rs
  line: 532
  category: acceptance
  severity: blocker
  verdict: CONFIRMED
  origin: undecided
  message: Raw SQL substring matching accepts a blob table without the required enforced integrity constraint.
```
