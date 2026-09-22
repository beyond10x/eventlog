---
format: aep.planning-md/1
id: review-result:inline-projection-admin-source-pass-2
kind: review-result
status: active
title: Final administration source review accepted
relations:
- reviews: story:inline-projection-administration
revision: 1
---
## Verdict: ACCEPT

Unit `story:inline-projection-administration`, base `d016adb0`, corrected submission `daf6b814` (current worktree HEAD, confirmed via `git rev-parse HEAD`, tree clean). F1 from `source-review-1-claude/report.md` is resolved; no new blocking defect found in the correction.

### F1 resolution

| Provider | Fix location | Mechanism |
|---|---|---|
| SQLite | `crates/eventlog-sqlite/src/inline_admin.rs:197` | `validate_captured_order` runs on the full collected `events` before the `projector.apply` loop (line 210) |
| PostgreSQL | `crates/eventlog-postgres/src/inline_admin.rs:257` | Same, before `projector.apply` loop (line 272) |
| File | `crates/eventlog-file/src/inline_admin.rs:166` | Same, before `projector.apply` loop (line 169) |

All three now match the design's "validate the same stored event/envelope/numeric invariants as native capture, without filtering malformed rows" before any callback.

### Capture-predicate fix (newly changed, examined per brief)

| File:line | Change | Verified |
|---|---|---|
| `eventlog-sqlite/src/capture.rs:306,312` | first-page cursor `0_i64`→`Option<i64>::None`; `global_seq>?2`→`(?2 IS NULL OR global_seq>?2)` | Traced: negative seq fails earlier at `to_u64` (`Backend("stored value is negative")`, `lib.rs:3146`); zero seq now reaches `validate_captured_event`'s `global_seq==0` check instead of being silently excluded by the old `>0` filter |
| `eventlog-postgres/src/capture.rs:289` | Same, with `$2::bigint IS NULL` | Same reasoning; type cast avoids Postgres NULL-inference ambiguity |
| Same pattern in `inline_admin.rs` rebuild loops (sqlite:172, postgres:237) | Same fix | No pagination regression: `after` still advances to the max seq per page (ascending order), so no repeat/infinite-loop risk |

### Invariants assessed (brief's list)

| Invariant | Status |
|---|---|
| Transaction/tenant isolation | Unchanged, correct: SQLite connection-mutex+`BEGIN IMMEDIATE`; Postgres registration→pool lease(quarantine)→publication advisory lock→tenant/projector xact lock→`REPEATABLE READ`, matching design order |
| Validation before projector callbacks | Now satisfied, all 3 providers (see F1 table) |
| Complete history | SQLite/Postgres: full paginated read before fold; File: `state.events` (`BTreeMap<u64,_>`, `state.rs:89`), already ascending, filtered by tenant |
| Nonwriting attachment | `attach_inline_existing` untouched by `daf6b81`; no DDL/DML |
| Shadow publication | Shadow create→fold→delete+insert active→drop (SQLite)/`ON COMMIT DROP` (Postgres), one transaction; failure path rolls back via `finish_transaction`/`transaction.rollback()`, leaving old rows/cursor — confirmed by new regression tests, green |
| Cancellation/recovery | Postgres `commit_started`+timeout→`Deadline`/`UnknownCommit`, lease quarantine/settle unaffected by this diff; SQLite runs in `spawn_blocking` per design |

### Non-blocking observations

1. `eventlog-postgres/tests/inline_admin.rs` corruption test covers 3 mutation forms (non-object, zero, negative seq); SQLite's covers 4 (adds `stream_version_gap`). Shared validator, low risk, coverage gap only.
2. File's new `validate_captured_order` call (`inline_admin.rs:166`) has no dedicated red→green regression test for the "data not object" case — verified by code reading only, not by an executed File-specific test.
3. **Inherited, out of scope** (pre-existing, untouched by this diff, not "capture" or inline rebuild): the same `global_seq>0`-shaped silent-exclusion pattern remains in ordinary `read_feed` (`eventlog-sqlite/src/lib.rs:1718`), `run_catch_up` (`:2285`), and non-inline `rebuild_projection` (`:2392`), and PostgreSQL analogues (`eventlog-postgres/src/lib.rs:561,1164,1265`). Not part of this unit's diff; flagged for the record only.

### Executed vs. not executed

- Executed by me: static read of `daf6b81` diff, current source, and prior evidence files (`source-review-1-claude/report.md`, `f1-correction-20260918/report.md` and its `logs/`).
- Not executed by me: no Cargo, no build, no test run, no new test file written (found no contract failure warranting one; no subagents used, per instruction).
- Relied on, not re-run: `f1-correction-20260918/logs/sqlite-correction-final.log` (2/2 passed), `composed-production-proof.log` (255 passed, 0 failed, 0 skipped, PostgreSQL 17.6), `exits.txt` (all 0 except intentional mutation red).

### Modified files (`daf6b81`, 8 files)

CHANGELOG.md · crates/eventlog-file/src/inline_admin.rs · crates/eventlog-postgres/src/{capture.rs,inline_admin.rs} · crates/eventlog-postgres/tests/inline_admin.rs · crates/eventlog-sqlite/src/{capture.rs,inline_admin.rs} · crates/eventlog-sqlite/tests/inline_admin_review_one.rs

Stopping here per brief — no follow-on review, no new test, no cleanup.
