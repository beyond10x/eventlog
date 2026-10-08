---
format: aep.planning-md/3
id: review-result:sqlite-tracked-capture-adversary
kind: review-result
status: archived
title: Projection alias omission in acknowledged capture deltas
relations:
- reviews: story:sqlite-tracked-capture
revision: 2
transitions:
- {from: "active", to: "archived", at: "2026-10-08T11:00:48Z", actor: "human:timo", revision: 2}
---
unit: SQLite capture continuity working tree based on 06c1e99c, review pass 1
verdict: CONFIRMED (corrected by implementor; see final report.md)
cases: isolated probe executed 1, red 1; inherited package baseline 195 from implementor report
origin: introduced 1 / pre-existing 0 / undecided 0
wrote-outside-worktree: none
needs-coordinator: preserve real-provider regression and route fail-closed correction to implementor

1. Reviewer-owned diff

Only NEW crates/eventlog-sqlite/tests/tracked_capture_review.rs was authored. Existing non-test changes belong to the implementation/coordinator and are captured in inherited.patch. No production edit or mutation was made by this reviewer.

2. Actual isolated red before package execution

The real SQLite provider registers projection review_rows with indexed field kind. A public inline projector then writes the same physical table through a valid ProjectionSpec using indexed field other. SQLite acknowledges the append. Full canonical capture contains the new value 2, while capture_tenant_since returns one new event and zero changed rows. The consumer cannot reconstruct the complete capture from this delta.

Command: CARGO_PROFILE_DEV_DEBUG=0 CARGO_BUILD_JOBS=2 cargo test -p eventlog-sqlite --test tracked_capture_review an_acknowledged_projection_alias_cannot_disappear_from_the_requested_delta --locked -- --exact
Exit: 101

```text
   Compiling eventlog-sqlite v0.6.0 (<worktree>/crates/eventlog-sqlite)
    Finished `test` profile [unoptimized] target(s) in 0.25s
     Running tests/tracked_capture_review.rs (target/debug/deps/tracked_capture_review-90b9c85c3d573413)

running 1 test
test an_acknowledged_projection_alias_cannot_disappear_from_the_requested_delta ... FAILED

failures:

---- an_acknowledged_projection_alias_cannot_disappear_from_the_requested_delta stdout ----

thread 'an_acknowledged_projection_alias_cannot_disappear_from_the_requested_delta' (840375) panicked at crates/eventlog-sqlite/tests/tracked_capture_review.rs:108:13:
assertion `left == right` failed: acknowledged write to the same physical projection must not disappear
  left: 0
 right: 1
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    an_acknowledged_projection_alias_cannot_disappear_from_the_requested_delta

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

error: test failed, to rerun pass `-p eventlog-sqlite --test tracked_capture_review`
```

3. Suite boundary

No preemptive suite ran. The isolated red was delivered to the implementor before the subsequent package run. The corrected package run and exact counts are in final report.md.

4. Finding and reachability

crates/eventlog-sqlite/src/tracked_capture.rs:382 — CONFIRMED / introduced / blocker. Filtering journal rows only by full ProjectionSpec equality omits an acknowledged write through a different declaration for a requested physical table. This reaches the real public register_inline and append_group APIs with an ordinary projector; no raw SQL, private-state fabrication or production mutation is used. ProjectionSpec::validate accepts both declarations and the provider acknowledges the write. Before this new continuation path, complete capture returned the actual row. The smallest fail-closed correction returns Complete when requested and changed specs name the same physical table but differ in declaration.

5. Reviewed scope

Accepted SQL-visible boundary, public capability wrapper, issuer/epoch identity, ordered scope/limits, stamp collection inside the write transaction, journal publication after commit, retained-chain checks and cumulative accounting were read. Existing tests/mutant evidence were inspected; no broader guarantee about direct raw-file edits is claimed.

6. Outside-worktree paths

None.

```findings
- file: crates/eventlog-sqlite/src/tracked_capture.rs
  line: 382
  category: integrity
  severity: blocker
  verdict: CONFIRMED
  origin: introduced
  message: Filtering changed rows by full ProjectionSpec equality omits an acknowledged alias write to a requested physical projection table.
```
