---
format: aep.planning-md/2
id: review-result:consistent-tenant-capture-source-pass-2
kind: review-result
status: active
title: Consistent tenant capture final source examination
relations:
- reviews: story:consistent-tenant-capture
revision: 1
---
unit: final source examination of native consistent tenant capture, source `d15672f4f3497ad839c1fcff9536664d49aea513`, base `8746a693c6084ab516170278a72c6c8cf5c8e46c`, plus two reviewer test files
verdict: INFEASIBLE
cases: executed 106→110, red 4
origin: introduced 3 / pre-existing 0 / undecided 0
wrote-outside-worktree: 2 paths
needs-coordinator: none

## 1. `git --no-pager diff --stat`

Empty: both reviewer files are untracked, so the tracked diff remains empty. Final
`git status --short` is:

```text
?? crates/eventlog-file/tests/capture_review_two.rs
?? crates/eventlog-sqlite/tests/capture_review_two.rs
```

Every path is a new test file. No implementation, existing test, manifest, design, model, roster,
planning or production file changed, even temporarily.

| reviewer file | final lines | initial sha256 | hygiene-corrected sha256 |
|---|---:|---|---|
| `crates/eventlog-file/tests/capture_review_two.rs` | 85 | `7454a246bfd13a3fa171ca9593c13d2b73fee1a4ffea9e2553ae3aaadc8acc22` | `5a0d19864cef4c02688d8bab758a2c56c72f77e846dba121f36774e87bb8c163` |
| `crates/eventlog-sqlite/tests/capture_review_two.rs` | 120 | `aab4fb1bd2b560bf3e15e2576dcf29aba94d9e2ee5a50b19c45b05484bb9ad58` | `b2db53f60ee75d96a88b33b5ca2e4efded84785641a3353b98525981f92e01f9` |

The hygiene change replaced one manual `match` with the Clippy-required `let Err(...) = ... else`
form and applied direct Rust formatting. Fixtures and assertions did not change. Root synchronized
the corrected bytes and verified both final hashes before the final rerun. All twelve rows in
`source-correction-1/final-source.sha256` still verify `OK` at the examined source.

Origin is introduced for all three findings. The capture modules are absent at the base; the File
strict opener is added by this unit inside the pre-existing journal module. The examined source diff
contains every source site named below.

## 2. Cases added and focused red results

Four cases were written before any command was run. Root ran each exact command from `READY.md`
alone against byte-matched source `d15672f4f3497ad839c1fcff9536664d49aea513`; every command compiled
and exited 101 at its intended behavior assertion. Raw logs and exit files are under
`source-review-2/execution/`.

### 2.1 File strict entry points

`crates/eventlog-file/tests/capture_review_two.rs:29`
`opening_refuses_a_dangling_pending_intent` asserts that an existing dangling `append.json` entry
returns `RecoveryRequired` and remains untouched. Focused exit: **101**. Captured red output:

```text
running 1 test
test opening_refuses_a_dangling_pending_intent ... FAILED

failures:

---- opening_refuses_a_dangling_pending_intent stdout ----

thread 'opening_refuses_a_dangling_pending_intent' (2952395) panicked at crates/eventlog-file/tests/capture_review_two.rs:41:18:
a dangling pending-intent entry was read past
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    opening_refuses_a_dangling_pending_intent

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.03s

error: test failed, to rerun pass `-p eventlog-file --test capture_review_two`
```

`crates/eventlog-file/tests/capture_review_two.rs:58`
`capture_refuses_a_dangling_pending_intent_created_after_open` asserts the same strict refusal when
a dangling `privacy.json` entry appears after the read-only handle opened. Focused exit: **101**.
Captured red output:

```text
running 1 test
test capture_refuses_a_dangling_pending_intent_created_after_open ... FAILED

failures:

---- capture_refuses_a_dangling_pending_intent_created_after_open stdout ----

thread 'capture_refuses_a_dangling_pending_intent_created_after_open' (2952418) panicked at crates/eventlog-file/tests/capture_review_two.rs:70:5:
capture read past a dangling pending-intent entry
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    capture_refuses_a_dangling_pending_intent_created_after_open

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.03s

error: test failed, to rerun pass `-p eventlog-file --test capture_review_two`
```

These are two measurements of one source branch, at both public strict entry points. After reviewer
hygiene, `final-cases.log` records the same failures at final lines 39 and 70.

### 2.2 SQLite physical admission

`crates/eventlog-sqlite/tests/capture_review_two.rs:27`
`a_quoted_foreign_index_name_is_physical_shape_mismatch` replaces the expected declared-field index
with a structurally equivalent index named `foreign-index`. It asserts the closed typed refusal for
foreign physical shape. Focused exit: **101**. Captured red output:

```text
running 1 test
test a_quoted_foreign_index_name_is_physical_shape_mismatch ... FAILED

failures:

---- a_quoted_foreign_index_name_is_physical_shape_mismatch stdout ----

thread 'a_quoted_foreign_index_name_is_physical_shape_mismatch' (2952697) panicked at crates/eventlog-sqlite/tests/capture_review_two.rs:53:5:
assertion `left == right` failed
  left: Store(Backend("near \"foreign\": syntax error in PRAGMA index_info(foreign-index) at offset 18"))
 right: ProjectionUnavailable { projection: "capture_ledger", reason: PhysicalShapeMismatch }
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    a_quoted_foreign_index_name_is_physical_shape_mismatch

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.05s

error: test failed, to rerun pass `-p eventlog-sqlite --test capture_review_two`
```

### 2.3 SQLite tenant-coordinate completeness

`crates/eventlog-sqlite/tests/capture_review_two.rs:68`
`a_non_text_event_tenant_coordinate_is_corruption_not_absence` first appends through the real store,
then changes only that event's tenant coordinate storage class from TEXT to BLOB while preserving
the exact bytes. It asserts event corruption rather than a successful incomplete history. Focused
exit: **101**. Captured red output:

```text
running 1 test
test a_non_text_event_tenant_coordinate_is_corruption_not_absence ... FAILED

failures:

---- a_non_text_event_tenant_coordinate_is_corruption_not_absence stdout ----

thread 'a_non_text_event_tenant_coordinate_is_corruption_not_absence' (2952742) panicked at crates/eventlog-sqlite/tests/capture_review_two.rs:116:5:
assertion `left == right` failed: a malformed row with this tenant's exact bytes must not disappear from complete history
  left: Ok(TenantCapture { tenant: TenantId("capture-review-two-owner"), stream_identity: "01a0a8f1-7daa-7192-a7f1-bf139760180c", events: [], blobs: [], projections: [] })
 right: Err(Corrupt { material: Event })
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    a_non_text_event_tenant_coordinate_is_corruption_not_absence

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.04s

error: test failed, to rerun pass `-p eventlog-sqlite --test capture_review_two`
```

## 3. Affected suites and reviewer hygiene

After all four cases existed, root ran:

```console
cargo test --locked --offline -p eventlog-file -p eventlog-sqlite --no-fail-fast
```

`execution/affected-suites.exit` is **101**. Its 21 `test result:` lines total **106 passed, 4
failed, 0 ignored, 110 executed**. The four failures are exactly the new reviewer cases. Every
pre-existing File and SQLite target is green, including File conformance 12/12, File consistent
capture 10/10, SQLite conformance 24/24, SQLite consistent capture 8/8, and both first-review
targets 4/4 and 2/2. The before count in the header is those 106 pre-existing cases; adding the four
selected reviewer cases yields the observed 110.

The run's failed-target close is represented by the two new targets only:

```text
error: test failed, to rerun pass `-p eventlog-file --test capture_review_two`
error: test failed, to rerun pass `-p eventlog-sqlite --test capture_review_two`
```

The first strict affected Clippy and formatting attempts found reviewer hygiene only:

- Clippy exit 101: `manual_let_else` in the new File test.
- formatting exit 1: formatting differences only in the two new files.

After correcting those reviewer-owned issues, root ran:

```console
cargo test --locked --offline -p eventlog-file -p eventlog-sqlite --test capture_review_two --no-fail-fast
cargo clippy --locked --offline -p eventlog-file -p eventlog-sqlite --all-targets -- -D warnings
cargo fmt --all -- --check
```

`final-cases.exit` is **101**, with all four intended assertion failures and no compile or fixture
failure. `final-clippy.exit` is **0** and `final-fmt.exit` is **0**. The red result is therefore the
product behavior under test, not reviewer syntax, lint or formatting failure.

## 4. Findings

### F1 — dangling pending-intent entries disappear from the File strict guard

| | |
|---|---|
| **what was measured** | `crates/eventlog-file/tests/capture_review_two.rs:29` and `:58`, focused exits 101 and final combined exit 101: strict open and capture both succeeded past dangling entries at the reserved intent names rather than returning `RecoveryRequired` |
| **what reaches it** | no ordinary kit writer. The state requires a filesystem actor, interrupted external restoration, or storage corruption to leave a dangling symlink at `append.json` or `privacy.json`; no shipped in-kit consumer calls the new capture capability |

`crates/eventlog-file/src/journal.rs:374` calls `Path::exists()`, which follows the symlink and
returns false when its target is absent. The directory entry therefore bypasses the only pending
intent check. This contradicts the binding rules that any existing append/privacy intent returns
`RecoveryRequired`, that opening and capture share the strict path, and that existing path
violations are refused without recovery or mutation. The privacy name is material: the successful
capture can hand back the old committed observation even though the reserved erasure-intent entry
is present.

Named, not applied: inspect both reserved names with `symlink_metadata`; distinguish `NotFound`
from every present directory entry, and return `RecoveryRequired` for any present intent entry
without following or changing it.

**Verdict `INFEASIBLE`, origin `introduced`, severity `warning`.** The violation is measured and is
inside the brief's required corruption handling, but the test constructs a filesystem state no
ordinary writer here creates and no documented current caller reaches.

### F2 — a quoted SQLite index name escapes the typed physical-shape refusal

| | |
|---|---|
| **what was measured** | `crates/eventlog-sqlite/tests/capture_review_two.rs:27`, focused and final exits 101: the observed result is `Store(Backend("near \"foreign\": syntax error in PRAGMA index_info(foreign-index) at offset 18"))`, not `ProjectionUnavailable { reason: PhysicalShapeMismatch }` |
| **what reaches it** | no ordinary kit writer. It requires direct DDL or storage manipulation to replace the kit-created index; ordinary projection creation uses the admitted generated identifier and no shipped in-kit consumer calls capture |

`crates/eventlog-sqlite/src/capture.rs:551` introspects every catalogued index before comparing its
name with the expected generated name. `index_columns` and `index_collations` interpolate that
stored identifier unquoted at `:588` and `:601`. A legal quoted SQLite identifier that is foreign
to the expected shape can therefore cause SQL parsing to fail first, leaking out as an operational
store error rather than the closed machine-readable physical-shape refusal.

Named, not applied: reject an unexpected created-index name before identifier interpolation, or
query the table-valued PRAGMA functions with a bound identifier; keep primary-index introspection
safe and preserve the existing exact shape comparison.

**Verdict `INFEASIBLE`, origin `introduced`, severity `warning`.** The typed contract is violated,
but the stored schema requires foreign DDL that the kit and admitted application role do not issue.

### F3 — SQLite silently omits an owned event whose tenant coordinate is not TEXT

| | |
|---|---|
| **what was measured** | `crates/eventlog-sqlite/tests/capture_review_two.rs:68`, focused and final exits 101: after changing only one event tenant coordinate to BLOB with identical bytes, capture returned `Ok` with `events: []` rather than `Corrupt { material: Event }` |
| **what reaches it** | no ordinary kit writer. The append path binds the tenant as Rust `&str`, so the state requires a direct SQL writer or storage-level corruption; no shipped in-kit consumer calls capture |

The first event preflight at `crates/eventlog-sqlite/src/capture.rs:197`, the redaction query and
the paged event read all use `WHERE tenant_id = ?` with a TEXT-bound tenant. SQLite does not equate
that parameter with a BLOB coordinate carrying the same bytes, so the malformed row is filtered
out before `read_event` and `validate_captured_order` can validate tenant equality. The result is a
successful partial history, contradicting the complete-set, tenant-equality and “do not silently
filter malformed rows” requirements. This is distinct from the corrected row-key pagination bug:
the bad coordinate never enters a page at all.

Named, not applied: inside the same `BEGIN IMMEDIATE` observation, detect a non-TEXT event tenant
coordinate whose byte representation equals the requested tenant before the normal typed count and
read, and return fixed `Corrupt { material: Event }` without exposing the stored value.

**Verdict `INFEASIBLE`, origin `introduced`, severity `warning`.** The incomplete success is
measured and is inside the brief's explicit corrupted-state scope, but only a direct SQL writer or
storage corruption constructs it.

### Reachable callers

`ConsistentTenantCapture::capture_tenant` still has no shipped caller in the repository. Its only
uses are the three provider implementations, the shared conformance exercise and test targets. The
accepted design names the consumer adapter and synchronous bridge as later work. This bounds all
three findings independently of their measured behavior.

## 5. What was attacked and could not be broken

- The first review's corrected File divergence precedence: only missing identity and redacted
  history answer ahead of the per-handle extension guard; other results do not advance observation.
- The first review's corrected SQLite row-key and digest storage-class checks: both refuse typed
  corruption before a foreign coordinate becomes a keyset resume point.
- The corrected SQLite and PostgreSQL blob preflights: actual byte lengths are compared with stored
  metadata before payload-cap refusal, with incremental complete-content validation retained.
- Identity-before-redaction precedence, byte-preserved legacy nonempty identity, empty/invalid
  identity refusal, and no identity minting during capture.
- Exact checked event/blob/row/payload accounting, zero caps, payload-body-only measurement, request
  duplicate rejection, deterministic bytewise result order and no returned partial value.
- File frame-chain, manifest, journal-length and selected-blob validation under the writers' lock;
  ordinary pending intents, non-regular required files, surplus bytes and blob symlinks are refused
  without recovery, cleanup or mutation.
- SQLite `BEGIN IMMEDIATE` before stored reads, native writer exclusion, legal empty/NUL key
  pagination, stored body decoding, declaration matching and recognized physical-shape drift.
- PostgreSQL publication-lock-before-snapshot ordering, no feed watermark, read-only repeatable-read
  observation, verified session unlock before reuse, error/cancellation quarantine, exact role and
  physical-shape admission, and fresh observation after erasure.
- The complete final source diff, all provider capture callers, accepted design, implementation
  brief, first source report, correction disposition and retained final source hashes. No additional
  testable defect was found in PostgreSQL or core capture after the bounded attacks above.

## 6. Every path written outside the worktree

1. `home-path:sha256:829617d9681f8591a5b200d50e4ca8dda2b623bd90020235ca970402578f609e`
2. `home-path:sha256:5172da8be7cc34a3c05dfa48848a5ab44a88de88102f3adbff8f315523553693`

No `/tmp`, build directory, production store, external integration, planning record or root file
was written by this reviewer. Root owns the `execution/` logs and exits. The review lease is
released after this report; the coordinator owns the managed tree and all correction, gates and
acceptance.

## 7. Findings block

```findings
- file: crates/eventlog-file/src/journal.rs
  line: 374
  category: acceptance
  severity: warning
  verdict: INFEASIBLE
  origin: introduced
  message: Path::exists follows a dangling append.json or privacy.json symlink and makes the reserved pending-intent entry disappear, so strict open and capture succeed instead of returning RecoveryRequired
- file: crates/eventlog-sqlite/src/capture.rs
  line: 588
  category: contract-drift
  severity: warning
  verdict: INFEASIBLE
  origin: introduced
  message: a legal quoted foreign index name is interpolated unquoted into PRAGMA index_info before its name is rejected, turning physical shape drift into an operational Store error instead of PhysicalShapeMismatch
- file: crates/eventlog-sqlite/src/capture.rs
  line: 197
  category: acceptance
  severity: warning
  verdict: INFEASIBLE
  origin: introduced
  message: TEXT tenant predicates silently omit an event whose tenant coordinate is BLOB with the same bytes, so capture returns a successful incomplete history instead of event corruption
```
