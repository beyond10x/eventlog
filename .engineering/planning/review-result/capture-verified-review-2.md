---
format: aep.planning-md/1
id: review-result:capture-verified-review-2
kind: review-result
status: active
title: 'Independent verification pass 2: which lines the unit added can be deleted while the suite stays green'
relations:
- reviews: story:file-capture-reuses-the-verified-view
revision: 2
---
unit: story:file-capture-reuses-the-verified-view — commit b5a6e0ae19a5d02616a8c260a1f3d67060317ba3, worktree home-path:sha256:9fb7a0cbaea99ddb6c55d7d287408c61cd17a3710f8df99690692b2a5a614431 (HEAD detached, no tracked file modified)
verdict: red
cases: executed 90→97, red 1
origin: introduced 6, pre-existing 2, undecided 0
wrote-outside-worktree: 2 roots — this report and home-path:sha256:013116d0027a9ec782c1118a9df9244537e19dfd82e3fde0b08b6d0a6d36b34a; full list in §6
needs-coordinator: no

**The unit's own new design paragraph is false, and one line is why.** `docs/design/file-provider.md:87`
says *"Everything the strict reader refuses before it decodes, the resumed reader reaches too."*
`open_strict` refuses a store root that is not a physical directory (`journal.rs:630`), and so does
the writer's `Journal::resume` (`journal.rs:326`) — `resume_strict` (`journal.rs:554`) is that same
prelude with the first check dropped. A handle that has captured once therefore serves an
observation through a symlinked root the strict opener refuses by name. Red case in §2, green at
db608cd, one line to fix.

**Seven of eleven one-line deletions from the code this unit added leave all 90 shipped cases
green** (§3), and **three of those seven are fail-open**: the resumed reader can stop taking the
writers' interprocess lock, can release it before it reads the content it hands out, and can stop
asking whether `events.jsonl` is a regular file — none of it observed by any case. I wrote the two
cases that pin the first and the third; the second I could not express as a reliable case and it is
reported from the mutation matrix. This is the predecessor's defect, smaller: unit 3 had eleven of
fourteen green and four fail-open.

---

## 1. `git --no-pager diff --stat` and `git status --porcelain` — proof of the bound

```
$ cd home-path:sha256:9fb7a0cbaea99ddb6c55d7d287408c61cd17a3710f8df99690692b2a5a614431
$ git --no-pager diff --stat
$ git status --porcelain
?? crates/eventlog-file/tests/capture_review_three.rs
```

`diff --stat` is empty: the one file I wrote is new and I ran no `git add`. `git status --porcelain`
is therefore the bound's proof — **exactly one path, a test file under
`crates/eventlog-file/tests/`**. No implementation file, no document, no conformance exercise and no
`.engineering/` path was touched, and no `git` write command (`commit`, `add`, `stash`, `switch`,
`checkout`, `branch`, `worktree`) was run at any point. Every mutation in §3 was applied to
`git archive` extractions under `home-path:sha256:013116d0027a9ec782c1118a9df9244537e19dfd82e3fde0b08b6d0a6d36b34a`, never to this worktree.

`cargo clippy -p eventlog-file --all-targets -- -D warnings` exits **0** with my file present, and
the file is `rustfmt`-clean, so nothing I added moves the gate.

---

## 2. The cases I added, and the red one, verbatim, run alone before the suite

`crates/eventlog-file/tests/capture_review_three.rs`, seven test functions (six cases plus the
`foreign_writer_child` helper the parent selects by name, the `writer_lock_holder` pattern).

| case | asserts | now | at db608cd |
| --- | --- | --- | --- |
| `a_store_root_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does` (:125) | a root that is no longer a physical directory refuses a resumed capture with exactly the refusal a fresh strict open gives | **RED** | green |
| `a_resumed_capture_takes_the_writers_lock` (:176) | with the lock held by a second open file description in this process, a resumed capture cannot finish, and finishes once it is released | green | green |
| `committed_history_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does` (:219) | `events.jsonl` reached through a link, with the exact committed bytes behind it, refuses a resumed capture as it refuses a strict open | green | green |
| `a_foreign_process_append_is_observed_by_the_next_capture` | a frame and an object committed by a **real second process** between two captures are in the second one | green | green |
| `a_capture_that_fell_back_to_the_strict_opener_leaves_the_next_one_correct` | a fallback refusal writes no byte, deletes no unselected staging name, and leaves a handle that observes the repaired history identically twice | green | green |
| `a_writer_handle_interleaving_transactions_and_captures_observes_each_one` | four rounds of append + `put_blob` + capture on one `FileEventStore`, then a privacy rewrite on that same handle, each observed | green | green |

### The red one, alone, before anything else was run

```
$ cargo test -p eventlog-file --test capture_review_three -- --exact \
    a_store_root_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does --nocapture
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.03s
     Running tests/capture_review_three.rs (target/debug/deps/capture_review_three-6cb7d78872022268)

running 1 test

thread 'a_store_root_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does' (1346432) panicked at crates/eventlog-file/tests/capture_review_three.rs:120:5:
assertion `left == right` failed: a resumed capture observed a store root the strict opener refuses as not a physical directory
  left: "None"
 right: "Some(Store(Backend(\"file store root is not a physical directory\")))"
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test a_store_root_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does ... FAILED

failures:

failures:
    a_store_root_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 5 filtered out; finished in 0.07s

error: test failed, to rerun pass `-p eventlog-file --test capture_review_three`
EXIT=101
```

`left: "None"` is the resumed capture's error: there is none. It returned a value. (`:120` is the
pre-`rustfmt` line; the assertion is at `:125` in the formatted file, unchanged in substance.)

**What was measured.** `crates/eventlog-file/tests/capture_review_three.rs:125`, exit 101. The
capture succeeded where `FileTenantCapture::open` on the same path refuses with
`Store(Backend("file store root is not a physical directory"))`.

**What reaches it.** An operator moving a store directory and leaving a link where it was —
`mv store real && ln -s real store` — while a process holds an open capture handle. `root_path`
(`lib.rs:591`) only makes the path absolute; it does not canonicalize, so the handle keeps the
symlinked path. The fixture does exactly that and changes no byte of the store. I built the state;
I did not find it in a documented workflow, and it is narrow. What is not narrow is that the omitted
check is present in **both** functions `resume_strict` was modelled on, three lines apart:

```
journal.rs:326  Journal::resume    if !fs::symlink_metadata(root).map_err(backend)?.is_dir() { return Err(corrupt()); }
journal.rs:630  open_strict        …is_dir() { return Err(unavailable("file store root is not a physical directory")); }
journal.rs:559  resume_strict      let lock_path = root.join("writer.lock");      ← nothing
```

**The fix, which I did not apply.** One statement as `resume_strict`'s first, mirroring its
siblings and answering `None` so the refusal stays `open_strict`'s:

```rust
if !fs::symlink_metadata(root).ok()?.is_dir() {
    return None;
}
```

### Origin, run rather than read

The same file against a `git archive db608cd` extraction
(`home-path:sha256:227b0887ac93826fb3191d128e8fe11bb0714d6bed6ab434aed46894e5a05a04`, this worktree never moved):

```
$ cargo test -p eventlog-file --test capture_review_three --no-fail-fast
test a_store_root_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does ... ok
test committed_history_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does ... ok
test a_resumed_capture_takes_the_writers_lock ... ok
test a_capture_that_fell_back_to_the_strict_opener_leaves_the_next_one_correct ... ok
test a_foreign_process_append_is_observed_by_the_next_capture ... ok
test a_writer_handle_interleaving_transactions_and_captures_observes_each_one ... ok
test foreign_writer_child ... ok
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.66s
EXIT=0
```

All seven green at db608cd, where every capture is a strict open. **Introduced.**

---

## 3. Which added lines can be deleted while the suite stays green

The question this pass was sent for. Eleven one-line changes to the code this unit added, applied
to a `git archive b5a6e0a` extraction under `home-path:sha256:bfce33d1d42ddafe609b8f95907fa0280c412c760d593756befed9eef2ea1703` — never to
this worktree — each run against the **shipped** 90-case suite with my file absent, then against my
file with the one case that is red unmutated deselected. The tree was restored from saved originals
between every row and the restore row is the control.

| # | the one line deleted or flipped | shipped 90 cases | my cases | fail-open? |
| --- | --- | --- | --- | --- |
| — | restored (control) | **exit 0**, 90 passed | exit 0 | — |
| **A1** | `journal.rs:569` `lock.lock().ok()?;` — the resumed reader never takes the writers' interprocess lock | **exit 0** | **red**: `a_resumed_capture_takes_the_writers_lock` | **yes** |
| A2 | `journal.rs:560` `regular(&lock_path).ok()?;` | exit 101 ✓ | — | — |
| **A3** | `capture.rs:251-252` swapped — `resumed.into_parts()` (which drops the lock) before `observe()`, so content is read unlocked | **exit 0** | exit 0 | **yes**, unpinned by anything including me |
| **A4** | `capture.rs:164-166` the `.filter(|view| … == previous)` retirement, deleted | **exit 0** | exit 0 | no — fail-safe, see below |
| **A6** | `journal.rs:411` `pending_intent_exists(root).unwrap_or(true)` → `unwrap_or(false)` | **exit 0** | exit 0 | in principle yes; state unreachable, see below |
| **A7** | `journal.rs:431` `regular(&events_path)?;` deleted from the shared walk | **exit 0** | **red**: `committed_history_that_became_a_symlink…` | **yes** |
| A8 | `capture.rs:201` `*view = Some(verified);` — db608cd's behaviour | exit 101 ✓ | — | — |
| **A9** | `journal.rs:425` `|| manifest.sequence < observed.sequence` deleted | **exit 0** | exit 0 | no — fail-safe |
| **A10** | `journal.rs:426` `|| manifest.length < observed.length` deleted | **exit 0** | exit 0 | no — fail-safe |
| A12 | `journal.rs:454` the committed-prefix comparison → `if false` (control) | exit 101 ✓ | — | — |
| A13 | `capture.rs:244-248` the fold of the frames past the observed head, deleted (control) | exit 101 ✓ | — | — |

Script and full logs: `home-path:sha256:a7421faef1d998366e3733227a3f7c79819da7a29f78d05296c51c39a3546c24,run-mutations.sh,run-mine.sh,mutations-shipped.log,mine-under-mutation.log}`.

```
$ bash run-mutations.sh <mutate-tree> mutations-shipped.log restore A1 A2 A3 A4 A6 A7 A8 A9 A10 A12 A13
restore EXIT=0        (16+4+2+12+16+12+2+4+2+3+17+0 = 90 passed)
A1 EXIT=0
A2 EXIT=101
A3 EXIT=0
A4 EXIT=0
A6 EXIT=0
A7 EXIT=0
A8 EXIT=101
A9 EXIT=0
A10 EXIT=0
A12 EXIT=101
A13 EXIT=101

$ bash run-mine.sh restore A1 A3 A4 A6 A7 A9 A10      # my file, the red-unmutated case deselected
restore EXIT=0
A1 EXIT=101   test a_resumed_capture_takes_the_writers_lock ... FAILED
A3 EXIT=0
A4 EXIT=0
A6 EXIT=0
A7 EXIT=101   test committed_history_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does ... FAILED
A9 EXIT=0
A10 EXIT=0
```

**A1 — the lock, and why nothing saw it.** The two cases that look like they cover this do not
reach it. `capture::tests::the_strict_reader_holds_the_writer_lock_for_its_whole_life`
(`capture.rs:475`) holds `open_strict` to the rule. `capture_queues_behind_another_process_holding_the_writer_lock`
(`consistent_capture.rs:303`) times `FileTenantCapture::open`, which is `open_strict` again. Both
use a handle with **no view**, so neither reaches `resume_strict` at all. Removing the lock leaves a
reader that reads `manifest.json` and then `events.jsonl` while a writer is between them; the
length check makes most of that degrade into a fallback rather than a wrong answer, but nothing in
the design says a reader may read a store it does not hold, and the `StrictResumed` doc comment says
the opposite. `a_resumed_capture_takes_the_writers_lock` is the missing case; it is green today.

**A3 — the lock released before the content is read — is the one I could not pin.** The comment at
`capture.rs:249-250` states the rule ("the blob objects below are read under it, not after it") and
swapping two statements breaks it with the whole suite and all of my cases green. Content would then
be read with no interprocess lock, so a concurrent writer's `clean_blobs` can delete an object mid
observation and the capture answers `Corrupt{Blob}` about a store that is not corrupt. A case needs
an observation that can be interrupted deterministically inside `observe`; every shape I tried is a
poll race in the direction of a false red, and I would rather report the measurement than ship a
flake. **Named remedy:** hold the probe lock, drive a capture over a fixture whose content read is
long, and require `try_lock` to fail throughout — or accept the mutation matrix as the standing
evidence and record A3 in the unit's own table.

**A7 — `regular(&events_path)` — is unpinned at db608cd too, measured.** Deleting the same line from
`Journal::resume` at the base leaves the base's 84-case suite green (exit 0, log
`home-path:sha256:56ad2eab64440cb3d3e45bfdd2a020fa878e6d73984199e6798d8ba4d5283bea`). So the coverage gap is old; what is new is that a
**capture** now depends on that line. Without it `fs::metadata` follows the link and reports the
target's length, so every remaining question in the walk is answered by the target and the capture
serves history through an entry `open_strict` refuses. Origin is `introduced` under the charter's
second clause — the unit's diff reached a path nothing reached before.

**A4, A9 and A10 are fail-safe, and I tried to break them.** A4: without the filter, `resume_strict`
is handed `previous` from one history and `content` from another, and it hashes exactly
`previous.length` bytes against a digest taken over `view.manifest.length` bytes. Every way the two
can differ changes the length or the bytes — a transaction moves `runtime.observed` and lengthens
the file (`lib.rs:193`), an inline path moves it (`inline_admin.rs:102,201`), a privacy rewrite mints
a new epoch over replaced bytes — so the hash misses and the complete opener decides. A9: a manifest
whose sequence went backwards still has to satisfy `decode_chain`'s closing
`sequence != manifest.sequence` from the observed sequence upward, which it cannot. A10: a shorter
file makes the prefix loop's `read == 0` fire before it has hashed `observed.length` bytes. I found
no construction for any of the three. These three are the same verdict unit 3's pass 2 reached for
`Journal::resume`'s M2, M3 and M11, on two of the same lines.

**A6 is unreachable, which is why it is a note and not a finding with teeth.**
`pending_intent_exists` returns `Err` only when `symlink_metadata(root/append.json)` fails with
something other than `NotFound` — and `resume_strict` has already called
`regular(root/writer.lock)` on the same directory two statements earlier, which fails first on every
error that would produce it (`EACCES` on the root, `ENOTDIR`, root removed). `symlink_metadata` does
not follow a final-component link, so neither `ELOOP` nor a dangling chain reaches it either. I
could not construct a state where the `unwrap_or(true)` decides anything. The clause is correct and
defensive; it is not testable from here.

---

## 4. The suite, after the cases in §2 exist

```
$ cargo test -p eventlog-file --no-fail-fast
     Running unittests src/lib.rs           test result: ok. 16 passed
     Running tests/capture_review_one.rs    test result: ok. 4 passed
     Running tests/capture_review_three.rs  test result: FAILED. 6 passed; 1 failed
thread 'a_store_root_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does' (1457015) panicked at crates/eventlog-file/tests/capture_review_three.rs:120:5:
assertion `left == right` failed: a resumed capture observed a store root the strict opener refuses as not a physical directory
     Running tests/capture_review_two.rs    test result: ok. 2 passed
     Running tests/conformance.rs           test result: ok. 12 passed
     Running tests/consistent_capture.rs    test result: ok. 16 passed
     Running tests/durability.rs            test result: ok. 12 passed
     Running tests/existing_open.rs         test result: ok. 2 passed
     Running tests/inline_admin.rs          test result: ok. 4 passed
     Running tests/measure_review_two.rs    test result: ok. 2 passed
     Running tests/verify_once_review.rs    test result: ok. 3 passed
     Running tests/verify_once_review_two.rs test result: ok. 17 passed
   Doc-tests eventlog_file                  test result: ok. 0 passed
EXIT=101
```

Full output: `home-path:sha256:cbc7f56c08d039a6101184706c3cccd8ed003efd817e9a6ead6268935ec98f34`. **executed 90 → 97**: `<before>` is
the `cases: executed 84→90` the implementing state reported, corroborated by the restore row of §3,
which is this commit's sources with my file absent and prints 90 across the same twelve lanes.
`<after>` is 97, the twelve lanes above with `capture_review_three`'s seven. One red, and it is §2's.

The two lanes that ran no case of mine are unchanged in count, so nothing I added displaced
anything. Only `eventlog-file` was run; the workspace and `scripts/gate.sh` are the implementor's
§8 and I did not re-run them — the PostgreSQL row there is a separate story and re-measuring it was
not worth the disk.

---

## 5. Reviewed and could not fault

- **Refusal parity, the other nine.** Read from `open_strict` (`journal.rs:628-692`) and checked
  against the resumed walk one at a time: root absent (`regular(root/writer.lock)` fails →
  fallback); root a regular file (`ENOTDIR` → fallback); lock absent; lock not a regular file
  (`regular`, and A2 is red); lock not holdable (`lock.lock().ok()?`); a pending `append.json` or
  `privacy.json`, present or dangling (`pending_intent_exists`, `symlink_metadata`); manifest absent;
  `manifest.json` that is a **symlink** (`exists()` follows it, but `read()` calls `regular()` and
  errors → `.ok()?` → fallback → `damaged`); a manifest of the wrong format, another store, another
  epoch, a lower sequence or a shorter length; a file length that is not the committed length; a
  decode failure. Each reaches a refusal. The tenth, the root's kind, is §2.
- **A manifest whose sequence advanced with no bytes.** `manifest.length == observed.length` with
  `manifest.sequence > observed.sequence` reads an empty tail, and `decode_chain`'s closing
  `sequence != manifest.sequence || previous != manifest.digest` (`journal.rs:828`) refuses it.
- **The boundary at zero.** An observed length of 0 hashes no bytes, `Content::empty()` and
  `Content::of(&[])` agree, and the whole file becomes the tail chained from `ZERO`. Correct.
- **`State::replay` versus the incremental fold.** `replay` (`state.rs:115`) *is* `fold` in a loop,
  so a resumed capture's state and a strict one's are the same value by construction, not by
  coincidence.
- **The divergence guard answered as `true` on the resumed path** (`capture.rs:254`). The resumed
  walk establishes byte identity of the first `observed.length` bytes plus a chain from
  `observed.digest` to the manifest; `extends_observed` (`journal.rs:482`) establishes store, epoch,
  sequence ordering and a re-encoded prefix chain. The first implies the second. I could not
  construct a history that resumes and does not extend.
- **The read path writes nothing, on four paths the unit's control does not reach**: a successful
  resumed capture, a capture that falls back after in-place damage, a capture that refuses, and all
  of it with an unselected `.write-<uuid>` and a `privacy.next` present throughout
  (`a_capture_that_fell_back_to_the_strict_opener_leaves_the_next_one_correct`). Nothing is swept,
  nothing is written, and the handle observes the repaired history identically afterwards.
- **A foreign append by a real second process** is observed by the next capture, frames and content
  both — the unit's own case uses a second store in this process.
- **Every path that advances the committed bytes, against the cached view.** `transaction`
  (`lib.rs:193`), `register_inline` (`inline_admin.rs:102`), `rebuild_inline_projection`
  (`inline_admin.rs:201`) and `forget_tenant` all move `runtime.observed` and none of them touches
  `runtime.captured`; the filter at `capture.rs:166` retires it and the complete opener decides.
  `a_writer_handle_interleaving_transactions_and_captures_observes_each_one` drives four rounds plus
  a privacy rewrite on one handle and every observation is the one the file holds.
- **The counting case pins what it claims.** `ten.blobs_hashed == 10 * one.blobs_hashed` with
  `one.blobs_hashed >= 8` asserted as a fixture precondition, keyed per store root so the rest of
  the binary cannot contribute (`cost.rs`). A8 — not storing the view — is red on it.
- **`Runtime.captured` cannot reach a transaction.** `Observed` is a distinct type with private
  fields and `Verified` is untouched; the compiler is what enforces it, as the comment claims.

---

## 6. Every path written outside the worktree

- `home-path:sha256:da2b6364ba61bf82acfe44cf18c1b8d8db5de5f2306e67881000d0fffa002c75` — this file, as the dispatch directs
- `home-path:sha256:56ad2eab64440cb3d3e45bfdd2a020fa878e6d73984199e6798d8ba4d5283bea` — `mutate.py`, `base-mutate.py`, `run-mutations.sh`,
  `run-mine.sh`, `case1-alone.log`, `mutations-shipped.log`, `mutations-withmine.log`,
  `mine-under-mutation.log`, `base-mycases.log`, `suite.log`, `base-journal.rs.orig`, and the
  `TMPDIR` every cargo command in this session used
- `home-path:sha256:95e15393d70494a5ac332b96b19c23d384b835fe6840309aa59bde9ba362a735` — a `git archive db608cd` extraction with my test file
  copied in, its own `target/`, used for every origin run
- `home-path:sha256:a2c82afcf9b77220059176ca5769d7d4c2aa98ec7bd314e708966ef09329415d` — a `git archive b5a6e0a` extraction with my test
  file copied in, its own `target/` and a `.orig/` of the two mutated sources, used for §3

Nothing under `/tmp`. `CARGO_TARGET_DIR` never set. No `git` write command, no `.engineering/`
write, no `aep artifact` verb of any kind. Disk 41 G free and MemAvailable 46 GiB at the start and
no full-workspace build was run. The two scratch extractions carry build directories the coordinator
may delete; this worktree's `target/` is the coordinator's.

---

## 7. Findings

| # | where | verdict | origin | severity | what was measured / what reaches it |
| --- | --- | --- | --- | --- | --- |
| F1 | `journal.rs:559` (`resume_strict`), against `docs/design/file-provider.md:87` | NEEDS-CHANGE | introduced | blocker | red case at `capture_review_three.rs:125`, exit 101: a resumed capture returns a value where `FileTenantCapture::open` returns `Store(Backend("file store root is not a physical directory"))`. Reached by `mv store real && ln -s real store` while a handle is open; `root_path` does not canonicalize. Both siblings keep the check. |
| F2 | `journal.rs:569` | CONFIRMED | introduced | warning | mutation A1: deleting the interprocess lock from the resumed read path leaves all 90 shipped cases green. The two lock cases both use a handle with no view. Case added and green; red under A1. |
| F3 | `capture.rs:251` | CONFIRMED | introduced | warning | mutation A3: releasing the lock before `observe` reads the content leaves all 90 shipped cases *and* my six green. No reliable case; remedy named in §3. |
| F4 | `journal.rs:431` | CONFIRMED | introduced | warning | mutation A7: deleting `regular(&events_path)` leaves all 90 green, and a capture then serves history through a symlinked `events.jsonl`. Also green at db608cd, measured — old line, new dependant. Case added and green; red under A7. |
| F5 | `journal.rs:411` | INFEASIBLE | introduced | note | mutation A6: the `unwrap_or(true)` doubt clause is unpinned, and I could not construct a state reaching it — `regular(root/writer.lock)` fails first on every error that would. |
| F6 | `capture.rs:166` | CONFIRMED | introduced | note | mutation A4: the view-retirement filter is unpinned, as the implementation report §9 discloses. I tried and failed to construct a mismatch that survives the prefix hash; second line of defence. |
| F7 | `journal.rs:425` | CONFIRMED | pre-existing | note | mutation A9 green, as unit 3 pass 2 recorded for the same clause in `Journal::resume`. Fail-safe: `decode_chain`'s closing sequence check refuses it. |
| F8 | `journal.rs:426` | CONFIRMED | pre-existing | note | mutation A10 green, same provenance. Fail-safe: the prefix loop's `read == 0` fires first. |

Nothing here is a `.engineering/` record; the sub-operator writes those.

```findings
- file: crates/eventlog-file/src/journal.rs
  line: 559
  category: contract-drift
  severity: blocker
  verdict: NEEDS-CHANGE
  origin: introduced
  message: resume_strict omits the root-is-a-physical-directory check that both Journal::resume (:326) and open_strict (:630) make, so a handle with a view serves an observation through a symlinked store root that the strict opener refuses by name, falsifying docs/design/file-provider.md:87.
- file: crates/eventlog-file/src/journal.rs
  line: 569
  category: mutant
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: deleting the writers'-lock acquisition from the resumed read path leaves all 90 shipped cases green, because both existing lock cases use a handle with no view and never reach resume_strict.
- file: crates/eventlog-file/src/capture.rs
  line: 251
  category: concurrency
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: releasing the lock before observe() reads the content it hands out leaves all 90 shipped cases and all six of mine green, so the stated rule that blob objects are read under the lock is enforced by nothing.
- file: crates/eventlog-file/src/journal.rs
  line: 431
  category: mutant
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: deleting regular(&events_path) from the shared walk leaves all 90 cases green and lets a capture serve committed history through a symlinked events.jsonl that open_strict refuses; the line is unpinned at db608cd too, but only this unit made a capture depend on it.
- file: crates/eventlog-file/src/journal.rs
  line: 411
  category: mutant
  severity: note
  verdict: INFEASIBLE
  origin: introduced
  message: the unwrap_or(true) doubt clause on the reserved-intent probe is pinned by nothing and I could not construct a state that reaches it, because regular(root/writer.lock) fails first on every error that would make symlink_metadata of the intent names fail with other than NotFound.
- file: crates/eventlog-file/src/capture.rs
  line: 166
  category: mutant
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: the view-retirement filter is pinned by nothing, as the implementation report discloses; I could not construct a manifest mismatch that survives the committed-prefix hash, so it is a fail-safe second line of defence.
- file: crates/eventlog-file/src/journal.rs
  line: 425
  category: mutant
  severity: note
  verdict: CONFIRMED
  origin: pre-existing
  message: the manifest.sequence-went-backwards clause of the shared walk is pinned by nothing and is fail-safe, the same verdict unit 3's second pass reached for it inside Journal::resume.
- file: crates/eventlog-file/src/journal.rs
  line: 426
  category: mutant
  severity: note
  verdict: CONFIRMED
  origin: pre-existing
  message: the manifest.length-shorter clause of the shared walk is pinned by nothing and is fail-safe, refused instead by the prefix loop reading zero bytes before it has hashed observed.length of them.
```
