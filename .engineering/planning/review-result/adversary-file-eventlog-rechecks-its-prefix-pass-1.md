---
format: aep.planning-md/3
id: review-result:adversary-file-eventlog-rechecks-its-prefix-pass-1
kind: review-result
status: archived
title: Adversary pass 1 on the prefix comparison (9920a7db)
tags:
- adversary
relations:
- reviews: story:file-eventlog-rechecks-its-prefix-by-comparison
revision: 2
transitions:
- {from: "active", to: "archived", at: "2026-10-08T11:00:44Z", actor: "human:timo", revision: 2}
---
Recorded verbatim except that the home-directory prefix is written `~`, as the repository's personal-paths rule requires.

unit: story:file-eventlog-rechecks-its-prefix-by-comparison at 9920a7db (el42-compare tree plus my uncommitted test module)
verdict: NEEDS-CHANGE
cases: executed 175→178, red 3
origin: introduced 4 / pre-existing 1 / undecided 0
wrote-outside-worktree: 1 directory (~/.cache/eventlog-el42/compare/adversary/, the assigned scratch)
needs-coordinator: the story's per-append bar is not met and this code cannot meet it. Amend the bar or hold the unit. Also decide whether "held once per handle" is held to its letter.

The comparison is correct: I found no refusal it misses. What fails is the acceptance itself (the per-append bar, and "once per handle") and the memory figure the operator doc gives.

### 1. Diff stat

```
$ git --no-pager diff --stat
 crates/eventlog-file/src/journal.rs | 4 ++++
 1 file changed, 4 insertions(+)
$ git status --short
 M crates/eventlog-file/src/journal.rs
?? crates/eventlog-file/src/adversary_compare_once.rs
?? crates/eventlog-file/tests/security_compare_persisted.rs   (security reviewer's, not mine)
?? crates/eventlog-file/tests/security_compare_prefix.rs      (security reviewer's, not mine)
```

`journal.rs` is an implementation file. The only change to it is a 4-line `#[cfg(test)] #[path = "adversary_compare_once.rs"] mod adversary_compare_once;` declaration, which the brief permits ("a new #[cfg(test)] module in journal.rs"). Everything else is the new test-only file. I touched no non-test code.

### 2. Cases added (`crates/eventlog-file/src/adversary_compare_once.rs`), each run alone first

| Case | Asserts | Now |
|---|---|---|
| A `a_writer_that_rereads_everything_keeps_the_committed_bytes_once` | One handle captures. Another handle on the same root appends. The capture takes that frame in, then this handle appends 5 times. The writer's buffer should still be shared with the capture view (`Arc::strong_count == 2`). | red |
| B `a_capture_after_own_appends_compares_only_the_bytes_its_view_verified` | A capture after this handle's own appends resumes onto its view: compares exactly its observed bytes and chains only the 3 new frames. | red, pre-existing |
| C `a_complete_open_keeps_about_the_committed_size` | After a complete open of a 4.8 MB journal (the issue's probe size), buffer capacity ≤ 1.125 × committed length. | red |

Red output at first run:
```
A: panicked at crates/eventlog-file/src/adversary_compare_once.rs:87:5:
   five appends after a complete reread, the writer keeps its own copy of the committed bytes beside the one the capture's view holds
   left: 1  right: 2          (the control at :73, shared after the first capture, passed)
B: panicked at crates/eventlog-file/src/adversary_compare_once.rs:115:5:
   Cost { frames_chained: 5, frames_reencoded: 5, frames_folded: 5, prefix_bytes_hashed: 0, prefix_bytes_compared: 0, ... }
   left: 0  right: 1415
C: panicked at crates/eventlog-file/src/adversary_compare_once.rs:142:5:
   a complete open keeps 8388608 bytes for a committed prefix of 4832016
```

Case B was written to pin `same_prefix`'s bound to the view's own length. Its first run showed something else: the resumed reader is never reached after a write through the same handle. After that run I edited only its doc comment, to state the real reason; the assertions are unchanged.

Origin of B: a copy of base 32168648, with the same case adapted to `prefix_bytes_hashed`, fails the same way (`frames_chained: 5, frames_reencoded: 5, prefix_bytes_hashed: 0`, log `base-b.log`).

Origin of A and C: introduced by construction. At the base, `Content` is a running SHA-256 and holds no buffer (`git show 32168648:crates/eventlog-file/src/journal.rs`, lines 79-119).

### 3. Suite, run after the cases existed

`cargo test --locked -p eventlog-file --no-fail-fast -- --test-threads=1` → EXIT=101
- lib: `test result: FAILED. 38 passed; 3 failed` (the three cases above). Every integration binary is ok.
- 187 executed in total. Excluding the security reviewer's `security_compare_persisted` (3) and `security_compare_prefix` (6) leaves 178, which is 175 + 3. The 175 comes from the implementor's `final2-test.log`.

Formatting and lint:
- `cargo fmt -p eventlog-file -- --check`: exit 0.
- `cargo clippy --locked -p eventlog-file --all-targets -- -D warnings`: exit 101. The 3 errors are `cast_possible_truncation` in `tests/security_compare_prefix.rs:102,127,212`, which is not my file.
- `cargo clippy --locked -p eventlog-file --lib --profile test -- -D warnings`: exit 0, so my module is clean.

### 4. Findings

| # | file:line | Verdict / origin | What was measured | What reaches it |
|---|---|---|---|---|
| F1 | CHANGELOG.md:18 | NEEDS-CHANGE / introduced | Bar: per append ≤ 5.44 ms (one fifth of 27.21). The coordinator's `wave/verify-compare-loaded.log` at 2,000 events: base 76.40 / 84.29 ms, change 74.48 / 106.14 ms. The CHANGELOG says itself that per-append "did not move measurably". What did improve: in-window read 1,443–1,632 ms → 122–229 ms, and CPU time 21 s → 1.1 s user. | The story's own acceptance. Inference, not measured: each commit runs about 6 fsync/dir-sync barriers, and the bar was set from CPU samples rather than wall time. |
| F2 | src/adversary_compare_once.rs:87 | CONFIRMED / introduced | After the writer rereads everything, it keeps buffer B (`strong_count` 1) while the capture view keeps buffer A: two copies, through every append, until the next capture. The acceptance and CHANGELOG.md:13 say "once". The design doc (file-provider.md, the "until the next capture" sentence) concedes it. | A second writer on the root followed by a capture on this handle. Also a privacy rewrite, a refusal that recorded, inline admin, or a cancelled transaction. Fix: share with `runtime.captured` when `transaction` (lib.rs:262-269) stores a `Verified` built by a complete open. |
| F3 | website/docs/operations.md:23 | CONFIRMED / introduced | Operators are told to budget "about the committed size" per handle (design doc :107 says the same). Measured: 8,388,608 bytes kept for 4,832,016 committed, 1.74×. `read_to_end` at journal.rs:556 grows by doubling, and in-place appends keep doubling. With F2 it is two such buffers. | Every `FileEventStore`. Fix: size the open's read exactly, and state "up to about twice" in both docs. |
| F4 | src/adversary_compare_once.rs:115 | CONFIRMED / pre-existing | On a `FileEventStore`, a capture after a write through the same handle never resumes. It rereads, rechains, re-encodes and refolds the whole history, then `retain_once` drops the full copy it just read. So the unit's reader-side comparison only serves back-to-back captures. | The default writer-plus-capture pattern. Cause: the filter at capture.rs:223-225 compares against `runtime.observed`, which every transaction moves. |
| F5 | crates/eventlog-file/src/lib.rs:2570 | CONFIRMED / introduced | Nothing charges `prefix_bytes_hashed` any more; grep finds only cost.rs:31,72. So the `== 0` asserts at lib.rs:2570, 2588 and 2629 can never fail. A SHA-256 put back on the path without a charge keeps all three green, despite the claim in cost.rs:24-31. I found no case that could see an uncharged hash, so I did not rewrite them. | Regression protection only. |

### 5. Attacked and could not break
- **Stale shared buffer against a rewritten file** (privacy, erasure, redaction): the store/epoch check at journal.rs:762-768 runs before any comparison. A privacy transaction leaves `verified` empty, and the capture filter drops the old-epoch view. Established by reading, not by a test.
- **A view reading bytes another view appended:** `same_prefix` stops at `content.length` (journal.rs:224). `appends_extend_shared_committed_bytes_in_place` would panic on its `unwrap` if that bound widened. This is reasoned; I did not run the mutant.
- **Length mismatches and truncate-then-regrow to the same length:** a file length that differs from the manifest returns `None` (:771). Byte comparison is at least as strict as the hash it replaces.
- **Zero-length guard** (journal.rs:191): its only caller, `retain_once`, never pairs an empty new-epoch view with an old-epoch one. The guard is harmless but unreachable.
- **Concurrency:**
  - The runtime mutex serializes the writer and the capture.
  - `Retained` is never locked across a statement that can panic.
  - `share` takes its two locks in address order.
  - A cancelled transaction can extend the shared buffer, but `absorb` compares before extending, so every view still holds exactly the bytes it verified.
- **Forked tail:** the copy branch of `absorb` is reachable only with a forged same-epoch fork, and the views stay exact when it runs.

### 6. Paths written outside the worktree
- ~/.cache/eventlog-el42/compare/adversary/ (the assigned scratch), containing:
  - logs: build.log, red-a.log, run-a_capture_after_own_appends_compares_only_the_bytes_its_view_verified.log, run-a_complete_open_keeps_about_the_committed_size.log, base-b.log, suite.log, suite-nff.log, fmt.log, clippy.log, clippy-lib.log
  - ~/.cache/eventlog-el42/compare/adversary/base-copy/: a `git archive` of 32168648 plus the adapted case B. Its own `target/` is 226M; I did not delete it.

### 7. Findings block

```findings
- file: CHANGELOG.md
  line: 18
  category: acceptance
  severity: blocker
  verdict: NEEDS-CHANGE
  origin: introduced
  message: per-append at 2,000 events stays 74-106 ms against a story bar of at most 5.44 ms; only in-window reads and CPU time improved, so the acceptance bar is unmet
- file: crates/eventlog-file/src/adversary_compare_once.rs
  line: 87
  category: acceptance
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: after a complete reread the writer keeps its own buffer beside the capture view's, two copies of the history until the next capture, against the acceptance and CHANGELOG "once per handle"
- file: website/docs/operations.md
  line: 23
  category: contract-drift
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: operators are told to budget about the committed size per handle, but a complete open keeps 8,388,608 bytes for 4,832,016 committed (1.74x) and appends grow it by doubling
- file: crates/eventlog-file/src/adversary_compare_once.rs
  line: 115
  category: judgement
  severity: warning
  verdict: CONFIRMED
  origin: pre-existing
  message: a FileEventStore capture after a write through the same handle never resumes and rereads, rechains, re-encodes and refolds the whole history, so the reader-side comparison only serves back-to-back captures
- file: crates/eventlog-file/src/lib.rs
  line: 2570
  category: mutant
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: prefix_bytes_hashed is charged nowhere any more, so its == 0 asserts at lib.rs 2570, 2588 and 2629 cannot fail if a SHA-256 is put back on the path
```
