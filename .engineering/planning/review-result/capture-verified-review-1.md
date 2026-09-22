---
format: aep.planning-md/1
id: review-result:capture-verified-review-1
kind: review-result
status: active
title: 'Independent verification pass 1: does the resumed reader refuse everything the strict reader refuses'
relations:
- reviews: story:file-capture-reuses-the-verified-view
revision: 2
---
unit: story:file-capture-reuses-the-verified-view — commit b5a6e0ae19a5d02616a8c260a1f3d67060317ba3, worktree ess-evolution-capture-verified-review-1-20260921
verdict: red
cases: executed 90→95, red 2
origin: introduced 1, pre-existing 0, undecided 0
wrote-outside-worktree: ~/.cache/ess-wave-v2/u6r1/ (6 paths, §6) and this report
needs-coordinator: no

Two red cases, one defect: `journal::resume_strict` is the only entry point onto a store root that
does not check the root is a physical directory, so a resumed capture reads a store root that
`open_strict`, `Journal::open_with_creation` and the writer's own `Journal::resume` all refuse.
The unit's shipped design document states the opposite as a universal (`docs/design/file-provider.md:87`).
Areas 2, 3 and 4 hold: the six mutations are 6/6 still red on their named cases, re-applied in a
scratch copy of the tree.

---

## 1. `git --no-pager diff --stat` — what this pass touched

```
$ git --no-pager diff --stat
                              (empty)
$ git status --porcelain
?? crates/eventlog-file/tests/capture_verified_review_one.rs
$ git --no-pager log -1 --format='%H %s'
b5a6e0ae19a5d02616a8c260a1f3d67060317ba3 perf(file): a capture reuses the history the handle already verified
```

The one file this pass wrote inside the worktree is a new, untracked test file, so it is absent
from `diff --stat` and present in `status`. No tracked file is modified; no implementation file and
no document was edited. No `git` write command was run at any point. `cargo fmt -p eventlog-file`
was run once to format that file, and `git status --porcelain` above is the reading taken after it.

## 2. The cases added, and the run of each alone, before the suite

`crates/eventlog-file/tests/capture_verified_review_one.rs` (460 lines, 5 cases).

| case | asserts | now |
| --- | --- | --- |
| `every_shape_the_strict_opener_refuses_is_refused_through_the_resumed_path` | for each of ten damaged stores, the refusal a *fresh* `FileTenantCapture::open` (i.e. `journal::open_strict`) makes is the exact refusal a handle **that already has a view** makes about the same damaged store | **red**, 1 of 10 shapes |
| `a_store_root_that_is_no_longer_a_physical_directory_refuses_a_resumed_capture` | the one shape above, alone, with the strict opener's refusal asserted as a control first | **red** |
| `a_moved_epoch_or_a_foreign_store_identity_refuses_a_resumed_capture` | a privacy rewrite of another tenant, and another store's committed files dropped in place, both refuse with the divergence refusal — neither is a shape `open_strict` refuses, so the handle must | green |
| `a_direct_file_write_between_two_captures_is_observed_by_the_second` | the second capture sees a commit landed by plain `fs::write` of `events.jsonl` and `manifest.json`, with no Eventlog writer in the process between the captures | green |
| `no_capture_on_any_path_removes_an_unselected_staging_name` | `.write-<uuid>` and `privacy.next` survive byte-identically across a strict open, a first capture with no view, a resumed capture on an unchanged head, a resumed capture that folds a tail, a refusal, and the fallback capture after it | green |

### 2a. The failing case, run alone, verbatim

```
$ TMPDIR=~/.cache/ess-wave-v2/u6r1/tmp cargo test -p eventlog-file \
    --test capture_verified_review_one -- --exact \
    a_store_root_that_is_no_longer_a_physical_directory_refuses_a_resumed_capture
   Compiling eventlog-file v0.2.1 (~/.local/state/worktree/trees/b10x/eventlog/ess-evolution-capture-verified-review-1-20260921/crates/eventlog-file)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 6.70s
     Running tests/capture_verified_review_one.rs (target/debug/deps/capture_verified_review_one-6b3c8e1db28f9e36)

running 1 test
test a_store_root_that_is_no_longer_a_physical_directory_refuses_a_resumed_capture ... FAILED

failures:

---- a_store_root_that_is_no_longer_a_physical_directory_refuses_a_resumed_capture stdout ----

thread 'a_store_root_that_is_no_longer_a_physical_directory_refuses_a_resumed_capture' (1328872) panicked at crates/eventlog-file/tests/capture_verified_review_one.rs:198:5:
assertion `left == right` failed: a handle with a view read a store root the strict opener refuses to read
  left: None
 right: Some(Store(Backend("file store root is not a physical directory")))
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    a_store_root_that_is_no_longer_a_physical_directory_refuses_a_resumed_capture

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.07s

error: test failed, to rerun pass `-p eventlog-file --test capture_verified_review_one`
EXIT=101
```

`left: None` is the resumed capture's `.err()` — it **succeeded**. The control on the line above it
in the case asserts the strict opener refuses the same root, so the refusal being compared against
is real and is the root check and not some other one.

### 2b. The parity sweep, run alone, verbatim — nine of ten shapes hold

```
$ TMPDIR=~/.cache/ess-wave-v2/u6r1/tmp cargo test -p eventlog-file \
    --test capture_verified_review_one -- --exact \
    every_shape_the_strict_opener_refuses_is_refused_through_the_resumed_path
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.03s
     Running tests/capture_verified_review_one.rs (target/debug/deps/capture_verified_review_one-6b3c8e1db28f9e36)

running 1 test
test every_shape_the_strict_opener_refuses_is_refused_through_the_resumed_path ... FAILED

failures:

---- every_shape_the_strict_opener_refuses_is_refused_through_the_resumed_path stdout ----

thread 'every_shape_the_strict_opener_refuses_is_refused_through_the_resumed_path' (1329161) panicked at crates/eventlog-file/tests/capture_verified_review_one.rs:165:5:
a resumed capture did not reach the refusal the strict opener makes:
  - the store root is no longer a physical directory
     open_strict: Some(Store(Backend("file store root is not a physical directory")))
     resumed:     None
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    every_shape_the_strict_opener_refuses_is_refused_through_the_resumed_path

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.71s

error: test failed, to rerun pass `-p eventlog-file --test capture_verified_review_one`
EXIT=101
```

The case accumulates every mismatch before asserting, so this output is the complete list: the
other nine shapes — lock gone, lock not a regular file, manifest gone, manifest not a regular file,
foreign manifest format, history shorter than the manifest, history with an unexplained tail,
history not a regular file, a committed frame damaged in place — each reach `open_strict`'s exact
error through the resumed path.

### 2c. The three green cases, run alone, verbatim

```
$ ... -- --exact a_direct_file_write_between_two_captures_is_observed_by_the_second \
                 no_capture_on_any_path_removes_an_unselected_staging_name
running 2 tests
test no_capture_on_any_path_removes_an_unselected_staging_name ... FAILED
test a_direct_file_write_between_two_captures_is_observed_by_the_second ... ok
---- no_capture_on_any_path_removes_an_unselected_staging_name stdout ----
thread '...' panicked at crates/eventlog-file/tests/capture_verified_review_one.rs:296:9:
assertion `left == right` failed: after an ordinary writer committed beside them: the unselected staging file was removed or rewritten
test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.20s
EXIT=101
```

**That red is my fixture's fault and not a defect, and it is recorded here rather than quietly
fixed.** The step that failed ran an ordinary `FileEventStore` writer to produce the tail, and an
ordinary writer sweeps unselected staging names — correctly, by design, and that is `Journal::resume`'s
documented job. I rewrote the step to land the tail with a plain `fs::write` of bytes a writer
committed earlier, so the case measures only the reader:

```
$ ... -- --exact no_capture_on_any_path_removes_an_unselected_staging_name
running 1 test
test no_capture_on_any_path_removes_an_unselected_staging_name ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.11s
EXIT=0

$ ... -- --exact a_moved_epoch_or_a_foreign_store_identity_refuses_a_resumed_capture
running 1 test
test a_moved_epoch_or_a_foreign_store_identity_refuses_a_resumed_capture ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out; finished in 0.27s
EXIT=0
```

### 2d. Origin, settled by running against `git archive db608cd`

`git archive db608cd | tar -x -C ~/.cache/ess-wave-v2/u6r1/base`; the same test file
copied in unchanged; this worktree never moved.

```
$ cd ~/.cache/ess-wave-v2/u6r1/base && TMPDIR=.../tmp cargo test -p eventlog-file --test capture_verified_review_one
   Compiling eventlog-file v0.2.1 (~/.cache/ess-wave-v2/u6r1/base/crates/eventlog-file)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 6.48s
     Running tests/capture_verified_review_one.rs

running 4 tests
test a_store_root_that_is_no_longer_a_physical_directory_refuses_a_resumed_capture ... ok
test no_capture_on_any_path_removes_an_unselected_staging_name ... ok
test a_direct_file_write_between_two_captures_is_observed_by_the_second ... ok
test every_shape_the_strict_opener_refuses_is_refused_through_the_resumed_path ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.81s
EXIT=0
```

Green at the base, red on this commit: **introduced**. (Four cases, not five — the epoch/foreign-store
case was written after this run and is green on both sides; it is not a finding.)

## 3. The suite, after the cases in §2 exist

```
$ TMPDIR=~/.cache/ess-wave-v2/u6r1/tmp cargo test -p eventlog-file --no-fail-fast
     Running unittests src/lib.rs
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.43s
     Running tests/capture_review_one.rs
test result: ok. 4 passed; 0 failed; ...
     Running tests/capture_review_two.rs
test result: ok. 2 passed; 0 failed; ...
     Running tests/capture_verified_review_one.rs
failures:
    a_store_root_that_is_no_longer_a_physical_directory_refuses_a_resumed_capture
    every_shape_the_strict_opener_refuses_is_refused_through_the_resumed_path
test result: FAILED. 3 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.30s
     Running tests/conformance.rs
test result: ok. 12 passed; 0 failed; ...
     Running tests/consistent_capture.rs
test result: ok. 16 passed; 0 failed; ...
     Running tests/durability.rs
test result: ok. 12 passed; 0 failed; ...
     Running tests/existing_open.rs
test result: ok. 2 passed; 0 failed; ...
     Running tests/inline_admin.rs
test result: ok. 4 passed; 0 failed; ...
     Running tests/measure_review_two.rs
test result: ok. 2 passed; 0 failed; ...
     Running tests/verify_once_review.rs
test result: ok. 3 passed; 0 failed; ...
     Running tests/verify_once_review_two.rs
test result: ok. 17 passed; 0 failed; ...
   Doc-tests eventlog_file
test result: ok. 0 passed; 0 failed; ...
error: 1 target failed:
EXIT=101
```

16+4+2+**5**+12+16+12+2+4+2+3+17+0 = **95 executed, 2 red**, both mine, both the same defect.

`<before>` is **90**: the `cases:` line the implementing state reported, independently confirmed by
the restored run of the scratch mutation tree in §5, which carries none of my files and summed to
exactly 90 across the same twelve lanes.

Gate steps this pass also ran, so the added file does not cost the unit its green:

| command | exit |
| --- | --- |
| `cargo fmt --all --check` | 0 |
| `cargo clippy -p eventlog-file --all-targets -- -D warnings` | 0 |

**The flake, measured not argued.** The first suite run also reported
`capture::tests::the_strict_reader_holds_the_writer_lock_for_its_whole_life` red — the known
1-in-40 probe race (story:strict-reader-lock-case-races-a-forked-crash-child, implementation report
§8). Run alone three times immediately after: `ok` 3/3; and green in the final suite run above. Not
a finding, and not made worse by the added file, which is in a different test binary.

## 4. Area by area

### Area 1 — refusal parity with `open_strict`. **One break.**

Enumerated by reading `journal::open_strict` (`crates/eventlog-file/src/journal.rs:628-694`), every
refusal it makes before it decodes:

| # | `open_strict` refuses | resumed path reaches it | how |
| --- | --- | --- | --- |
| 1 | root absent | yes | `regular(root/writer.lock)` fails → `None` → fallback |
| 2 | **root present and not a physical directory** | **no** | **nothing asks** |
| 3 | `writer.lock` absent | yes | `regular` fails → `None` |
| 4 | `writer.lock` not a regular file | yes | the hand-written `regular(&lock_path)` (M5) |
| 5 | `writer.lock` cannot be opened / held | yes | `.ok()?` → `None` |
| 6 | a pending `append.json` / `privacy.json`, including a dangling entry | yes | `pending_intent_exists` (M2) |
| 7 | `manifest.json` absent, or not a regular file | yes | `exists()` / `read`→`regular` → `None` |
| 8 | manifest unparsable, or a foreign format | yes | `Err(corrupt())` → `.ok()?` → `None` |
| 9 | `events.jsonl` absent or not a regular file | yes | `regular(&events_path)` → `None` |
| 10 | file length ≠ committed length (short, or an unexplained tail) | yes | metadata compare → `None` |
| 11 | a frame that does not decode or chain | yes | prefix hash / `decode_chain` → `None` |

Rows 1 and 3–11 are asserted by the sweep in §2b and pass. Row 2 is the finding.

`Journal::resume`, the writer's sibling that shares the same extracted walk, opens with
`if !fs::symlink_metadata(root)...is_dir() { return Err(corrupt()) }` at `journal.rs:326`.
`resume_strict` at `journal.rs:554` begins at `root.join("writer.lock")` and never asks. Because a
symlink in a *path component* is always followed, every subsequent check — lock, intents, manifest,
store identity, epoch, length, prefix hash — passes through the link and succeeds, so the capture
succeeds.

Two claims the unit ships that this falsifies:

- `docs/design/file-provider.md:87` — *"Everything the strict reader refuses before it decodes, the
  resumed reader reaches too, by handing the decision back to it"*. This is public source in this
  repository, and the sentence is a universal.
- `crates/eventlog-file/src/journal.rs:552` — *"every refusal belongs to [`open_strict`]"*.
- implementation report §4 — *"All ten of them, read from `journal.rs`: **root absent or not a
  directory**, … The reader reaches nine of them structurally … and reaches the tenth, the lock's
  file kind, by the one explicit `regular()` call M5 deletes. That call is the only hand-written
  member of the class."* The class has **two** hand-written members. One of them is missing, which
  is why M5 was the only mutation that needed a new case and why nothing caught this.

### Area 2 — a foreign append between two captures is observed by the second. **Holds.**

`a_direct_file_write_between_two_captures_is_observed_by_the_second` lands the second head with
plain `fs::write` of `events.jsonl` and `manifest.json` — the brief's "direct file write", stronger
than the unit's own case, which runs an in-process second `FileEventStore` handle. Green: the
second capture returns two events and the object bound behind the view.

Also checked and green: a moved privacy epoch (`forget_tenant` on another tenant, a real epoch
bump) and an entirely different store's committed files dropped in place both refuse with
`Store(Backend("file history diverged from this handle's observed history"))`. Neither is a shape
`open_strict` refuses — it reads both stores happily — so the refusal has to come from the handle,
and the resumed path returns `extended = true` unconditionally. Reading why it is sound:
`resumed_committed` establishes `store ==`, `epoch ==`, `sequence >=` and the observed prefix
byte-identical, which is strictly more than `extends_observed` re-derives, and `decode_chain`'s
closing `sequence != manifest.sequence || previous != manifest.digest` covers the equal-length,
different-digest manifest that the `manifest == *observed` short-circuit would otherwise skip.

### Area 3 — the read path writes nothing. **Holds.**

`opening_and_capturing_change_no_stored_entry_or_byte` reaches the resumed path exactly once and
that capture refuses. `no_capture_on_any_path_removes_an_unselected_staging_name` walks the paths it
does not: a resumed capture that **succeeds** on an unchanged head, a resumed capture that succeeds
after folding a tail, and the fallback capture after a refusal retired the view. `.write-<uuid>`
and `privacy.next` survive byte-identically after all six steps. Green.

### Area 4 — the six mutations, re-applied. **6/6 still red on their named case.**

Applied to a `git archive b5a6e0a` extraction under my scratch, never to the worktree. Script:
`~/.cache/ess-wave-v2/u6r1/tmp/{mutate.py,run-mutations.sh}`; full log `mutations.log`.
Each mutation's anchor is asserted to match exactly once before it is applied, so a mutation that
silently did not land cannot be reported as red.

| # | the one line | case | exit | the assertion that fired |
| --- | --- | --- | --- | --- |
| M1 | `resume_strict` sweeps and syncs | `opening_and_capturing_change_no_stored_entry_or_byte` | 101 | `consistent_capture.rs:155: a refusal changed a file` |
| M2 | intent test back to `Path::exists` | `a_pending_intent_that_appears_after_a_capture_refuses_the_next_one` | 101 | `:809: append.json (dangling: true): a handle with a view read past a reserved entry` |
| M3 | prefix comparison → `if false` | `a_committed_frame_damaged_after_a_capture_refuses_through_the_strict_opener` | 101 | `:685: round 0: a handle does not serve committed bytes it can no longer validate` |
| M4 | fold of the frames past the head, deleted | `a_capture_folds_what_another_writer_committed_since_the_view_was_built` | 101 | `:630: …serves a history the file has already left behind` |
| M5 | `regular(&lock_path)` deleted | `a_writer_lock_that_is_no_longer_a_regular_file_refuses_a_capture` | 101 | `:764: a resumed reader took a lock the strict opener refuses to take` |
| M6 | resumed reader disabled | `capture::tests::repeated_captures_on_one_handle_verify_the_committed_history_once` | 101 | `capture.rs:557: …170 … against …17` |
| — | restored | `cargo test -p eventlog-file` | 0 | 90 passed across twelve lanes |

The predecessor's failure mode — a claimed table that eleven deletions left green — is not repeated
here. The table is accurate for the guards it covers. What it does not cover is the guard that is
absent, which no mutation of present code can reach.

## 5. Findings

Commit covered: `b5a6e0ae19a5d02616a8c260a1f3d67060317ba3`; base compared against
`db608cd4152e6a2370e8ef0e3a562d7a829cf34a` by extraction.

**F1 — `resume_strict` does not require the store root to be a physical directory.**

| | |
| --- | --- |
| **what was measured** | `crates/eventlog-file/tests/capture_verified_review_one.rs:198`, `assertion left == right failed: a handle with a view read a store root the strict opener refuses to read / left: None / right: Some(Store(Backend("file store root is not a physical directory")))`, exit 101. Also shape 1 of 10 in the sweep at `:165`. Green at `db608cd`, where the capture path always called `open_strict`. |
| **what reaches it** | *Nothing found in-repo.* No caller in this repository or in the AEP CLI was shown to produce a symlinked store root; my case builds it. It is the same external-actor model the unit's own shipped cases use for the dangling-intent, displaced-lock and damaged-frame shapes, and the model `open_strict`'s doc comment is written against. The **document** half needs no actor at all: `docs/design/file-provider.md:87` states a universal that is false as written, and that sentence is public source. |
| **defect** | `journal.rs:554` `resume_strict` begins at `root.join("writer.lock")`. `open_strict` (`:629`), `Journal::open_with_creation` (`:154`) and `Journal::resume` (`:326`) each check `fs::symlink_metadata(root)…is_dir()` first. `resume_strict` is the fourth entry point onto the same root and the only one that does not. |
| **integrity impact, stated so it is not overstated** | Bounded. The bytes a capture serves through the link are still checked: store identity and epoch against the observed manifest, the committed prefix against the hash this handle took, and every object against the hash the committed history records. I did not find a way to serve forged content through it. What is broken is the refusal contract and the document that states it. |
| **the correction, named not applied** | one line at the top of `resume_strict`, mirroring `Journal::resume`: `fs::symlink_metadata(root).ok()?.is_dir().then_some(())?;` — returning `None`, so `open_strict` makes the refusal with the evidence where it was found, as the rest of the function does. **Applied in the scratch copy only** (`~/.cache/ess-wave-v2/u6r1/mutant`, `tmp/apply-correction.py`) and run there: `cargo test -p eventlog-file --no-fail-fast` → **exit 0, all thirteen lanes green, 95 passed, 0 failed**, including both of my red cases and every existing case. It needs no change to either document. Fixing the document instead of the code would mean writing down that the reader is laxer than the writer on the same store, which is what the two types and the shared walk exist to avoid. The worktree under review is untouched: `git status --porcelain` still reads one untracked test file. |

| file:line | category | severity | verdict | origin |
| --- | --- | --- | --- | --- |
| `crates/eventlog-file/src/journal.rs:554` | contract-drift | blocker | NEEDS-CHANGE | introduced |

No other finding. The residue below is not a finding and is listed as what was checked, not as
something to fix.

## 6. Reviewed and could not fault

- **The nine other refusal shapes** (§4 table rows 1, 3–11), asserted against `open_strict`'s own
  error value rather than a hand-written expectation.
- **`extended = true` on the resumed path.** `resumed_committed` establishes strictly more than
  `extends_observed` re-derives; read, and probed from the outside by the moved-epoch and
  foreign-store cases.
- **The equal-length, different-digest manifest**, which the `manifest == *observed` short-circuit
  would skip: `decode_chain`'s closing `sequence != manifest.sequence || previous != manifest.digest`
  catches it on the empty tail.
- **`State::replay` vs incremental `fold`** — `replay` is literally a loop of `fold` (`state.rs:115`),
  so the resumed path's state is the same value.
- **View retirement.** `view.take().filter(|v| Some(&v.manifest) == previous.as_ref())` plus setting
  `*view` only after `outcome?`: a refusal leaves no view, and a transaction that moved
  `runtime.observed` retires the reader's. `Runtime.captured` is a separate slot of a separate type
  a transaction cannot accept.
- **The lock is held across `observe`** on both paths, so bound objects are read under it.
- **The counting case pins what it claims.** `ten.blobs_hashed == 10 * one.blobs_hashed` with
  `one.blobs_hashed >= 8` asserted first, so the content decision cannot be changed silently; and
  `ten.frames_reencoded == one.frames_reencoded` is what pins the resumed path skipping
  `extends_observed`. M6 confirms the whole case is load-bearing.
- **No conformance exercise edited** — `crates/eventlog-conformance/` is untouched in the diff, and
  `tests/conformance.rs` (12) and `file_consistent_capture_contract` pass.
- **Not verified, and stated rather than implied:** the CHANGELOG's wall-clock figures (57 ms vs
  140 ms on the 3.27 MB / 11.38 MB store) were not re-measured — the brief assigns the same-copy A/B
  to the sub-operator. `scripts/gate.sh` was not re-run; the implementation report settles its one
  PostgreSQL failure at the base with a disposable database, and that measurement is not disputed
  here.

## 7. Every path written outside the worktree

- `~/beyond10x/.ess-evolution/waves/0005-aep-migration/wave-validate-v2-20260920/unit-6-capture-verified/review-1-report.md` — this file, as the dispatch directs
- `~/.cache/ess-wave-v2/u6r1/base/` — `git archive db608cd` extraction plus a copy of the
  new test file, and its `target/` (the origin run, §2d)
- `~/.cache/ess-wave-v2/u6r1/mutant/` — `git archive b5a6e0a` extraction and its `target/`
  (the mutation runs, §4); restored to pristine and green, then a copy of the new test file added
  and the named one-line correction applied to `src/journal.rs` **in that copy only**, which is the
  state it is left in
- `~/.cache/ess-wave-v2/u6r1/tmp/apply-correction.py`
- `~/.cache/ess-wave-v2/u6r1/pristine/crates/eventlog-file/src/{journal.rs,capture.rs}` —
  the unmodified sources each mutation is reverted from
- `~/.cache/ess-wave-v2/u6r1/tmp/mutate.py`, `run-mutations.sh`
- `~/.cache/ess-wave-v2/u6r1/tmp/mutations.log`, `suite.log`, `suite-final.log`

Nothing under `/tmp`; `TMPDIR` was `~/.cache/ess-wave-v2/u6r1/tmp` for every cargo command.
`CARGO_TARGET_DIR` never set. No `.engineering/` write and no `aep artifact` verb of any kind.
Disk 40 G free and MemAvailable 46 GiB throughout. This report approves nothing.

```findings
- file: crates/eventlog-file/src/journal.rs
  line: 554
  category: contract-drift
  severity: blocker
  verdict: NEEDS-CHANGE
  origin: introduced
  message: resume_strict is the only entry point onto a store root that does not check the root is a physical directory, so a resumed capture reads a root that open_strict, Journal::open_with_creation and the writer's own Journal::resume all refuse, falsifying the universal claim the unit shipped at docs/design/file-provider.md:87 and in the resume_strict doc comment at journal.rs:552.
```
