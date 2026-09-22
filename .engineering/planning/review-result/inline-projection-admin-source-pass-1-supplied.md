---
format: aep.planning-md/1
id: review-result:inline-projection-admin-source-pass-1-supplied
kind: review-result
status: active
title: Supplied first administration examination identifies rebuild validation gap
relations:
- reviews: story:inline-projection-administration
revision: 1
---
unit: story:inline-projection-administration at 43ceaa09ceec610e25891815e33e03e8df92ee28 (base d016adb0)
verdict: NEEDS-CHANGE — one confirmed rebuild/capture invariant divergence; every submitted case stays green
cases: executed 6→7, red 1 (the added case; existing inline_admin suite 6/6 green)
origin: introduced 1 / pre-existing 0 / undecided 0
wrote-outside-worktree: 1 path (this evidence directory)
needs-coordinator: decide whether rebuild must validate stored events like capture, then route the one-line fix to the implementor

## Provenance

Reviewer: Claude Code, model claude-opus-4-8, fresh context, operator-requested on 2026-09-18 after
the platform-side interruption of the assigned gpt-5.6-sol examination (B-ADMIN-REVIEW). Not the
implementor, not a prior reviewer. Read-only over all planning stores; no `aep` command, no commit,
no push, no stash, no `worktree finish`, no cleanup. Managed review tree
`ess-evolution-inline-admin-review-1-claude-20260918`
(`~/.local/state/worktree/trees/b10x/eventlog/ess-evolution-inline-admin-review-1-claude-20260918`),
detached at the exact submission, own lease `claude-adminrev-20260918-2209765` acquired, heartbeaten
and released by this session only. The interrupted Sol reviewer tree and its lease were not touched.

Deviation from `source-review-1-brief.md`: builds used `CARGO_TARGET_DIR=$HOME/.cache/b10x-target/eventlog`
(the operator's standing workstation rule) instead of a tree-local `target/`, and `RUSTC_WRAPPER`
was unset because the inherited `sccache` wrapper refuses a socket path this long. Rust 1.98.1,
`--locked --offline`. PostgreSQL lanes were not executed: the disposable database and heavy build
slot belong to the live M7 and S9 lanes, and contending for them was not worth the finding, which
reproduces identically in SQLite. Nothing else in the brief's execution rules was changed; the added
file is the only test written.

Inputs read: the full unit diff `d016adb0..43ceaa09` (22 files, +3060/-12), the accepted binding
design `docs/design/inline-projection-administration.md` at the submission, `implementation-brief.md`,
`implementation/implementation-result.md`, the three providers' new `inline_admin.rs`, the shared
conformance contract, the ordinary capture path (`crates/*/src/capture.rs`), the core capture
validators (`crates/eventlog-core/src/capture.rs`), `crates/eventlog-file/src/state.rs::replay`, and
every new test file.

## 1. Tree diff

`git --no-pager diff --stat` is empty; the one file written is untracked and test-only:

```text
?? crates/eventlog-sqlite/tests/inline_admin_review_one.rs   (+95, test-only)
```

No production path is touched. No file under attack was mutated, not even briefly.

## 2. The case added

`crates/eventlog-sqlite/tests/inline_admin_review_one.rs`, one case
`rebuild_admits_a_stored_event_that_capture_refuses_as_corrupt`, red now, written before anything ran.

It opens a file-backed SQLite store, admits `AdminProjector`, appends one valid event, then tampers
that committed event's `data` column to a non-object (`[1,2,3]`) through a separate connection —
exactly the durable corruption `validate_captured_event` names (`!event.data.is_object()`,
`crates/eventlog-core/src/capture.rs:353`). It reopens, attaches, and asserts both that native
capture refuses and that the inline rebuild refuses.

Red output captured when the case was written, verbatim, that case alone:

```text
running 1 test
thread 'rebuild_admits_a_stored_event_that_capture_refuses_as_corrupt' panicked at
crates/eventlog-sqlite/tests/inline_admin_review_one.rs:88:5:
rebuild admitted an event that capture refuses as corrupt: Ok(InlineRebuildResult { applied: 1, position: 1 })
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
```

The capture assertion at line 79 passed silently — capture refused, as the design requires. The
rebuild assertion at line 88 is the finding: rebuild returned `Ok(applied: 1)` and published rows
derived from the corrupt event.

## 3. Suite run

`cargo test --locked --offline -p eventlog-sqlite --test inline_admin` (the unit's own SQLite
suite), after the case above existed:

```text
running 6 tests
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.15s
```

The submitted cases stay green; the finding is a gap they do not cover, not a regression in them.
The added case runs in its own target (`--test inline_admin_review_one`), so the executed count is
6→7 across the two targets, red 1.

## 4. Findings

**F1 — the inline rebuild does not validate stored events the way native capture does.**
`crates/eventlog-sqlite/src/inline_admin.rs:785` (the rebuild read loop), and identically
`crates/eventlog-postgres/src/inline_admin.rs:348`.

- **What was measured:** with one committed event whose `data` is a JSON array, SQLite
  `capture_tenant` returns `Err` (via `validate_captured_order` →
  `validate_captured_event`, `crates/eventlog-sqlite/src/capture.rs:132`), while
  `rebuild_inline_projection` returns `Ok(InlineRebuildResult { applied: 1, position: 1 })` and
  publishes derived rows. Assertion at `inline_admin_review_one.rs:88`, exit 101.
- **What reaches it:** `SqliteEventStore::rebuild_inline_projection`, public API, reached by the ER
  maintenance protocol the design names as the concrete consumer. The rebuild reads events with
  `read_event`/`build_event`, which parse `data` with no object check and apply no per-row integrity
  verification; capture reads the same rows and then runs the core validators the rebuild omits.
- **Verdict:** NEEDS-CHANGE. **Origin:** introduced — the inline rebuild is new in this diff and is
  the path the design's "Rebuild admission and snapshot" section binds: "Validate the same stored
  event/envelope/numeric invariants as native capture, without filtering malformed rows." That
  sentence is not satisfied.
- **Severity:** warning, not blocker. The ER consumer's mandatory post-rebuild capture comparison
  would refuse this history before serving, so corrupt-derived rows are not served. But the rebuild
  publishes them into the active table and advances the cursor, and any reader of the active
  projection outside the compare path sees them; the design assigns the invariant check to the
  rebuild itself, not to the later comparison.
- **The fix (named, not applied):** validate each read event with
  `eventlog_core::validate_captured_event` (and the set with `validate_captured_order`) inside the
  rebuild read loop in both `eventlog-sqlite` and `eventlog-postgres`, mapping a failure to the
  backend/corrupt error, exactly as `capture.rs` already does.

PostgreSQL shares the identical omission at `inline_admin.rs:348` (same `read_event` loop, no
validator); it was not executed here because its fixture lane is in use, so it is reported as the
same finding reaching a second provider rather than a second executed case.

## 5. Attacked and could not break

- **File provider, same corruption class.** File rebuild reads through `State::replay`
  (`state.rs:115`), which rejects a duplicated identity or a broken per-stream version chain on
  `Op::Event` apply, so the ordering half of the invariant is enforced there. It does not check
  `data.is_object()` or field validity, but the File on-disk frame hashing makes the SQLite-style
  raw `data` tamper a different, harder attack; not shown here.
- **Structural attachment atomicity.** Duplicate name, duplicate projection, frozen handle, invalid
  spec, and missing admission all refuse before any local install in all three providers; the
  drift/no-partial-install case is directly covered by the submitted suite and I could not defeat it.
- **Selected-table isolation.** The new `selected`/`validate_target` guard refuses an unselected
  table, a reservation and a foreign spec on every row op; the `AdminProjector` foreign-table and
  reservation probes in the shared contract exercise it and I could not get an unselected write to land.
- **Cursor and cross-tenant preservation.** Empty-history, sparse-position, multi-tenant and
  mid-fold-failure paths in the shared contract preserve the unrelated tenant's generation and the
  old rows on failure; I could not make a partial fold visible.

## 6. Paths written outside the worktree

One: this evidence directory,
`~/beyond10x/.ess-evolution/waves/0008-eventlog-inline-admin/source-review-1-claude/`
(`report.md` and `logs/`). Build output went to the shared `~/.cache/b10x-target/eventlog`, which is
the operator's standing target directory and not created by this review. No `/tmp` use.

```findings
- file: crates/eventlog-sqlite/src/inline_admin.rs
  line: 785
  category: acceptance
  severity: warning
  verdict: NEEDS-CHANGE
  origin: introduced
  message: the inline rebuild read loop applies stored events without validate_captured_event/validate_captured_order, so it admits and publishes a durable event that native capture refuses as corrupt; eventlog-postgres/src/inline_admin.rs shares the omission.
```
