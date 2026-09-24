---
format: aep.planning-md/2
id: review-result:verify-once-review-1
kind: review-result
status: active
title: 'Independent verification pass 1: a file Eventlog transaction costs what it touches'
relations:
- reviews: story:file-eventlog-verifies-once-per-open
revision: 2
---
unit: story:file-eventlog-verifies-once-per-open — commit c698923038de4413e0bbba3cd91ae108607d6be9, worktree home-path:sha256:d30a97f67dd9c970031b7f1d0664be59b72fc6a61618902ce26b329f11655122 (HEAD detached, no tracked file modified)
verdict: red
cases: executed 60→63, red 3
origin: introduced 5, pre-existing 1, undecided 0
wrote-outside-worktree: 3 roots — home-path:sha256:846ccc829abc0b551289c324ef204d829ca019a39c20039893bf45796faf2347 (logs), home-path:sha256:d951da5fcf29f837c1965cf53e104eec4faf81cdd96775c0688fcd2916f1f253 (git archive of f802eb8 + my test file + its own target/), and this report; full list in §6
needs-coordinator: yes

## 1. `git --no-pager diff --stat`

```
$ cd home-path:sha256:d30a97f67dd9c970031b7f1d0664be59b72fc6a61618902ce26b329f11655122
$ git --no-pager diff --stat
$ git status --porcelain
?? crates/eventlog-file/tests/verify_once_review.rs
```

`diff --stat` is empty because the one file I wrote is new and I am not permitted to run `git add`.
`git status --porcelain` is therefore the proof of the bound: exactly one path, and it is a test
file. No implementation file, no conformance exercise, no document was touched. No git write
command was run at any point.

## 2. The cases I added

All three are in `crates/eventlog-file/tests/verify_once_review.rs`. All three are **red now**.
Each was written before anything was run and each was then run **alone**, in this order, before the
suite.

| test | asserts | now |
| --- | --- | --- |
| `a_damaged_committed_frame_refuses_on_an_open_handle` (`:69`) | design page line 16 — a damaged committed frame refuses | red |
| `an_open_handle_does_not_commit_onto_a_history_it_can_no_longer_validate` (`:105`) | the rest of line 16 — *without altering the history*: either the operation refuses, or the store it wrote into still opens | red |
| `a_rewritten_prefix_does_not_extend_the_head_an_open_handle_observed` (`:166`) | story `## Outcome` — "a chain that does not extend the observed manifest is still refused"; design line 33 | red |

Verbatim, each run alone (`TMPDIR=home-path:sha256:a9ff4945e162727cfe00cd5dca8cc80a34be4bc3316d7c9e27130bd3cb7e279b`, build dir
`<worktree>/target`, `CARGO_TARGET_DIR` never set):

```
############ cargo test -p eventlog-file --test verify_once_review -- --exact a_damaged_committed_frame_refuses_on_an_open_handle
     Running tests/verify_once_review.rs (target/debug/deps/verify_once_review-7cc3c4e557edd1c1)

running 1 test
test a_damaged_committed_frame_refuses_on_an_open_handle ... FAILED

failures:

---- a_damaged_committed_frame_refuses_on_an_open_handle stdout ----

thread 'a_damaged_committed_frame_refuses_on_an_open_handle' (2306839) panicked at crates/eventlog-file/tests/verify_once_review.rs:69:5:
a damaged committed frame must refuse; the handle served Some(Object {"value": Number(1234)}) while events.jsonl holds the damaged frame
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    a_damaged_committed_frame_refuses_on_an_open_handle

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.18s

error: test failed, to rerun pass `-p eventlog-file --test verify_once_review`
EXIT=101
############ cargo test -p eventlog-file --test verify_once_review -- --exact an_open_handle_does_not_commit_onto_a_history_it_can_no_longer_validate
     Running tests/verify_once_review.rs (target/debug/deps/verify_once_review-7cc3c4e557edd1c1)

running 1 test
test an_open_handle_does_not_commit_onto_a_history_it_can_no_longer_validate ... FAILED

failures:

---- an_open_handle_does_not_commit_onto_a_history_it_can_no_longer_validate stdout ----

thread 'an_open_handle_does_not_commit_onto_a_history_it_can_no_longer_validate' (2306921) panicked at crates/eventlog-file/tests/verify_once_review.rs:105:5:
a damaged committed frame must refuse without altering the history; the append was accepted (Ok(1)) and the store it wrote into no longer opens (Some(Backend("file journal integrity check failed; no history was repaired")))
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    an_open_handle_does_not_commit_onto_a_history_it_can_no_longer_validate

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.19s

error: test failed, to rerun pass `-p eventlog-file --test verify_once_review`
EXIT=101
############ cargo test -p eventlog-file --test verify_once_review -- --exact a_rewritten_prefix_does_not_extend_the_head_an_open_handle_observed
     Running tests/verify_once_review.rs (target/debug/deps/verify_once_review-7cc3c4e557edd1c1)

running 1 test
test a_rewritten_prefix_does_not_extend_the_head_an_open_handle_observed ... FAILED

failures:

---- a_rewritten_prefix_does_not_extend_the_head_an_open_handle_observed stdout ----

thread 'a_rewritten_prefix_does_not_extend_the_head_an_open_handle_observed' (2306969) panicked at crates/eventlog-file/tests/verify_once_review.rs:166:5:
a history whose observed prefix is not the history this handle verified does not extend the head it observed and must be refused; the handle served Ok(1) instead
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    a_rewritten_prefix_does_not_extend_the_head_an_open_handle_observed

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.20s

error: test failed, to rerun pass `-p eventlog-file --test verify_once_review`
EXIT=101
```

### Origin, settled by a program and not by reading

The same three cases were run against the **base commit f802eb8**, in a scratch tree built with
`git archive f802eb8 | tar -x -C home-path:sha256:e036105923cf725c94daae78b1fe53bda5897fe6749f765e5e39e373780a5683` with only my test file
copied in. The worktree was never moved; no `checkout`, `switch`, `stash` or `worktree` command was
run.

```
$ cd home-path:sha256:e036105923cf725c94daae78b1fe53bda5897fe6749f765e5e39e373780a5683 && cargo test -p eventlog-file --test verify_once_review
running 3 tests
test an_open_handle_does_not_commit_onto_a_history_it_can_no_longer_validate ... ok
test a_damaged_committed_frame_refuses_on_an_open_handle ... ok
test a_rewritten_prefix_does_not_extend_the_head_an_open_handle_observed ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.21s
EXIT=0
```

Green at f802eb8, red at c698923: **origin `introduced`**, demonstrated.

## 3. The suite, after the cases existed

```
$ cd <worktree> && TMPDIR=home-path:sha256:a9ff4945e162727cfe00cd5dca8cc80a34be4bc3316d7c9e27130bd3cb7e279b cargo test -p eventlog-file --no-fail-fast
     Running unittests src/lib.rs (target/debug/deps/eventlog_file-4cb1bc046873d058)
test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.30s
     Running tests/capture_review_one.rs (target/debug/deps/capture_review_one-79ca1617fee85d5b)
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.08s
     Running tests/capture_review_two.rs (target/debug/deps/capture_review_two-abe7fc270806ea20)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.66s
     Running tests/conformance.rs (target/debug/deps/conformance-d93a67f6e34dd014)
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.57s
     Running tests/consistent_capture.rs (target/debug/deps/consistent_capture-2a3df8e38cfb8ed8)
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.09s
     Running tests/durability.rs (target/debug/deps/durability-74c150dcf088c06a)
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.05s
     Running tests/existing_open.rs (target/debug/deps/existing_open-e477b94f7bb0045f)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.13s
     Running tests/inline_admin.rs (target/debug/deps/inline_admin-552c70fa123f185c)
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.94s
     Running tests/verify_once_review.rs (target/debug/deps/verify_once_review-7cc3c4e557edd1c1)
test result: FAILED. 0 passed; 3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.27s
   Doc-tests eventlog_file
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
SUITE_EXIT=101
```

A red suite here is the successful outcome: **every one of the unit's 60 existing cases passes**,
and the only red lane is the one I added.

`<before>` was taken twice and agrees: the implementing state reported `cases: executed 55→60`, and
a second suite run with my three cases deselected reports the same number.

```
$ cargo test -p eventlog-file --no-fail-fast -- --skip a_damaged_committed_frame --skip an_open_handle_does_not_commit --skip a_rewritten_prefix
executed-without-my-cases: 60
```

Package hygiene, so the gate is not left red for a reason that is not a finding:

```
$ cargo fmt -p eventlog-file -- --check          FMT_EXIT=0
$ cargo clippy -p eventlog-file --all-targets -- -D warnings   CLIPPY_EXIT=0
```

## 4. Findings

### F1 — a damaged committed frame no longer refuses, and the handle keeps serving the pre-damage bytes

| | |
| --- | --- |
| **what was measured** | `crates/eventlog-file/tests/verify_once_review.rs:69`, exit 101. After `open` and one committed append, four bytes inside the committed frame are replaced in place; `manifest.json` is byte-identical and `events.jsonl` is exactly as long as the manifest says. `read_stream` returns `Some(Object {"value": Number(1234)})` — the pre-damage body — with no refusal. |
| **what reaches it** | No in-process caller. The state is one damaged committed frame, which is the threat model the journal's digest chain exists for: design page lines 11–16 are entirely about it, the repository's own journal unit tests construct it, and at f802eb8 every single transaction re-decoded and re-chained the whole file, so the base refused. Reachers are media damage, a partial write, a restore under a live handle, or the concurrent Git checkout the design page names at line 6 as not established. |
| **verdict / origin** | `CONFIRMED` / `introduced` (green at f802eb8, §2) |
| **source** | `crates/eventlog-file/src/journal.rs:325` — `let fresh = if manifest == *observed { Vec::new() }`. The identical-manifest branch opens nothing but the lock and the manifest, exactly as the new design paragraph (lines 60–63) says it will. |

This one is a **documented** trade in the unit's new paragraph. What it is not is consistent with
the paragraph the unit left standing: design page line 16 still says "missing committed bytes and
damaged committed frames refuse without altering the history", unqualified, in the **Authority and
commit** section. The unit rewrote three paragraphs of that page and did not reconcile this one.
The correction is one of two, and it is the author's call which: qualify line 16 to say that after
`open` a handle trusts the committed prefix it verified until it reopens, or make the trust
conditional. I did not apply either.

### F2 — the handle then commits a new frame onto the damaged prefix, and the store it wrote into no longer opens

| | |
| --- | --- |
| **what was measured** | `crates/eventlog-file/tests/verify_once_review.rs:105`, exit 101. Same damage as F1; the handle's next `append` returns `Ok(1)`, and a fresh `FileEventStore::open` of that directory then returns `Backend("file journal integrity check failed; no history was repaired")`. |
| **what reaches it** | The same state as F1, plus one ordinary `append` on the open handle — which is the common case, not a constructed one, once F1's state exists. `Journal::append` (`journal.rs:248`) seeks to `self.manifest.length` and writes; nothing on that path re-reads the prefix. |
| **verdict / origin** | `NEEDS-CHANGE` / `introduced` (green at f802eb8, §2) |
| **source** | `crates/eventlog-file/src/lib.rs:514` — the `enter` resume branch hands back a `Transaction` with full append authority over a prefix this process has not re-read since `open`. |

This is the half that is documented nowhere. The story's `## Outcome` says "The safety envelope in
`docs/design/file-provider.md` is preserved"; design line 16 promises refusal **without altering
the history**. What happens instead is that a recoverable single-frame corruption becomes a
committed history that no opener will accept — the handle wrote a valid frame after an invalid one,
so the chain is broken in the middle and truncating the tail no longer recovers the store. The base
refused *before* writing. Named correction, not applied: re-verify the observed prefix on the write
path — `Journal::append` is the one place it matters and it already holds the lock — or make
`resume` refuse to hand back append authority for a prefix it did not re-chain.

### F3 — a history whose observed prefix is not the history the handle verified is admitted as extending its head

| | |
| --- | --- |
| **what was measured** | `crates/eventlog-file/tests/verify_once_review.rs:166`, exit 101. Handle A commits frame 1 and observes it; handle B commits frame 2, which chains from A's observed digest; the bytes of frame 1 are then replaced with the same number of `x` bytes, leaving `manifest.json` and the committed byte length untouched. A's next `read_stream` returns `Ok` with one event — A folded B's frame onto its cached prefix and served it. Recomputing the chain from zero over the file on disk does not reach A's observed digest, so this history does not extend A's head. |
| **what reaches it** | No in-process caller. The minimum damage that reaches the same defect is F1's single-frame corruption in the observed prefix; the full-prefix overwrite in this case is **constructed by me**, to make the "does not extend the observed head" claim unambiguous rather than a matter of degree. Treat the reach as F1's, not as a separate one. |
| **verdict / origin** | `NEEDS-CHANGE` / `introduced` (green at f802eb8, §2) |
| **source** | `crates/eventlog-file/src/journal.rs:334` — `decode_chain(&tail, &manifest, observed.sequence, &observed.digest)` checks that the tail's first frame *names* the observed digest in its `previous` field. It never recomputes the prefix that digest was supposed to summarise. |

This is the unit's own new contract failing against itself. The design paragraph the unit wrote
closes (lines 66–69) with "falls back to the complete reread, which refuses a history that does not
extend the observed head **exactly as before**", and the story `## Outcome` promises "a chain that
does not extend the observed manifest is still refused". Both are false for any history whose
observed prefix changed while its tail still names the observed digest. The unit's own
`a_longer_fork_of_the_same_store_is_refused_by_an_open_handle` (durability.rs:452) does not reach
this: its fork's second frame chains from the *fork's* first-frame digest, so the literal
comparison catches it. The shape that survives the comparison is the one where the tail is genuine
and the prefix is not. Named correction, not applied: same as F2 — the observed digest has to be
recomputed, not matched as a string, before the handle treats the file as its own history.

### J1 — `crates/eventlog-file/src/state.rs` is outside the story's cited `## Scope`

| | |
| --- | --- |
| **what was measured** | `git --no-pager diff --stat f802eb8 c698923` lists six files; the story's `## Scope` cites five — `CHANGELOG.md`, `crates/eventlog-file/src/journal.rs`, `crates/eventlog-file/src/lib.rs`, `crates/eventlog-file/tests/`, `docs/design/file-provider.md`. `crates/eventlog-file/src/state.rs` (new `State::fold`, `state.rs:124`) is not among them. |
| **what reaches it** | The wave's scope check reads the cited list; the implementor's report does not mention the extra path. |
| **verdict / origin** | `CONFIRMED` / `introduced` |

The change itself looks right to me — `fold` is the natural place to collect the bindings a frame
creates — but the story record should carry the path it actually needed. This is a store edit and
therefore the coordinator's, not mine.

### J2 — the acceptance item `bash scripts/gate.sh` exit 0 is not demonstrated

| | |
| --- | --- |
| **what was measured** | Not measured by me. The implementor's report §7 records `gate.sh` exit 1, stopping at step 1 with two `eventlog-postgres` failures that `expect()` `EVENTLOG_TEST_POSTGRES_URL` rather than skipping. I confirmed from the diff that `crates/eventlog-postgres/` is byte-identical to f802eb8, and that `eventlog-postgres` does not depend on `eventlog-file`. |
| **what reaches it** | The story's `## Acceptance` lists `bash scripts/gate.sh` exit 0 as a requirement. It has not been shown, in this unit or at the base. |
| **verdict / origin** | `INFEASIBLE` / `pre-existing` — it cannot be settled in this session: no PostgreSQL fixture was provisioned, and provisioning one is not a test-file change. |

The residue is real but it is not this unit's defect: two PostgreSQL cases refuse where they should
skip. That is a repository finding and belongs in its own story. **This is the `needs-coordinator`
item.**

### J3 — a resumed handle no longer sweeps `.write-*` staging files or `privacy.next`

| | |
| --- | --- |
| **what was measured** | Read, not run. `Journal::open_with_creation` removes `privacy.next` and any `.write-<uuid>` staging file on every open (`journal.rs:198–208`), under the comment "These files were never selected by a durable intent. They may contain sensitive bytes." `Journal::resume` (`journal.rs:286`) has no equivalent. At f802eb8 every transaction went through `open_existing`, so every transaction swept them; now only a complete open does. |
| **what reaches it** | *Nothing found.* It needs a process death or a `rename` failure between `write_synced` and `fs::rename` inside `atomic_json`, while a handle stays open across it. A crash means a new process, and a new process opens completely and sweeps. I could not build a case that reaches it through the public API. |
| **verdict / origin** | `INFEASIBLE` / `introduced` — the narrowing is in the diff; the reachable consequence is not shown. |

Raised only because the swept files are described in the source as possibly carrying sensitive
bytes, which makes "cleaned less often" worth one line even without a reacher.

## 5. Reviewed and could not fault

- **Cache retirement across all five `observed`-advancing sites.** `runtime.verified.take().filter(|v| Some(&v.manifest) == observed.as_ref())` (lib.rs:134–137) retires it structurally rather than by a hand-maintained list. I traced `register_inline` (lib.rs:1101), `attach_inline_existing` (inline_admin.rs:102), `rebuild_inline_projection` (inline_admin.rs:201) and both capture entry points (capture.rs:150) and found no path that leaves a stale view usable: each either advances the manifest (dropping the view) or leaves it identical with the history unchanged (in which case the view is still correct, since `State` is a pure function of the frames).
- **The cached `State` equals `State::replay` of the cached frames.** Every op that reaches `state` goes through `record`, in the same order the committed frame stores them, and the commit-time `Op::Watermark { position: tx.state.next_position }` that is pushed without being applied is a no-op under `next_position.max(position)` (state.rs:161).
- **A refused transaction leaves no cache**, because the view is `take()`n before `enter` runs, and **a privacy rewrite is never cached** (`cacheable = !tx.privacy`).
- **`blob()` hashes what it reads, every time** (lib.rs:404–421). A bound object altered or removed after `open` refuses on read and commits nothing; the unit's own durability cases reproduce it and they pass.
- **`resume`'s fallbacks all land on the complete opener**: pending `append.json`/`privacy.json`, missing manifest, wrong store or epoch, a shorter sequence or length, a file length that disagrees with the manifest, and a tail that does not chain. Truncation and an unexplained suffix are therefore still caught — by the length comparison at journal.rs:315–318, before anything is trusted.
- **Disposal after a commit.** `clean_blobs` runs whenever `tx.privacy || !tx.pending.is_empty()`, so `delete_blob`, `redact` and `forget_tenant` all still remove unreferenced objects; the "Deletion and tenant erasure remove unreferenced objects" promise holds.
- **No conformance exercise edited.** The diff touches nothing under `crates/eventlog-conformance/`.
- **The 60 existing cases all pass** (§3), including all twelve shared conformance contracts.
- **Measurement claim 7 not re-run.** I had no reason to doubt it: zero blob opens on the identical-manifest branch follows directly from journal.rs:325, and re-running costs a release build. Untested by me, and reported as such.

## 6. Every path written outside the worktree

- `home-path:sha256:f70c978bfd9a5e1552a7d5e9dc95107d0afca41f803ed73ad169e14624138d67` — this report, as the brief's `report:` line directs
- `home-path:sha256:846ccc829abc0b551289c324ef204d829ca019a39c20039893bf45796faf2347` — `case1.log`, `case2.log`, `case3.log`, `cases-final.log`, `suite.log`, `suite-final.log`, `base-run.log`, `fmt.log`, `clippy.log`; also the assigned `TMPDIR` for every cargo invocation
- `home-path:sha256:d951da5fcf29f837c1965cf53e104eec4faf81cdd96775c0688fcd2916f1f253` — a `git archive` extraction of f802eb8 with my test file copied in, plus its own `target/`, used only to settle origin (§2). About 400 MB together with the logs. Nothing was written under `/tmp`.

Nothing was written to the planning store. No `aep plan artifact` command of any kind was run; the
store was read once, with `aep artifact show`.

## 7. Findings block

```findings
- file: crates/eventlog-file/src/journal.rs
  line: 325
  category: integrity
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: the identical-manifest branch reuses the cached fold without re-reading events.jsonl, so a committed frame damaged in place after open is served as if intact, while design page line 16 still promises unqualified that damaged committed frames refuse.
- file: crates/eventlog-file/src/lib.rs
  line: 514
  category: acceptance
  severity: blocker
  verdict: NEEDS-CHANGE
  origin: introduced
  message: the resume branch grants append authority over a prefix the process has not re-read since open, so an ordinary append after in-place damage commits a valid frame onto an invalid one and leaves a store no opener accepts, which is the opposite of "refuse without altering the history".
- file: crates/eventlog-file/src/journal.rs
  line: 334
  category: acceptance
  severity: blocker
  verdict: NEEDS-CHANGE
  origin: introduced
  message: decode_chain matches the tail's previous field against the observed digest as a string and never recomputes the prefix it summarises, so a history whose observed prefix is not the one this handle verified is admitted as extending its head, against the story Outcome and the unit's own new design paragraph.
- file: crates/eventlog-file/src/state.rs
  line: 124
  category: judgement
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: state.rs is changed by this unit but is not among the five paths the story's Scope cites, so the record does not carry a file the change needed.
- file: scripts/gate.sh
  category: acceptance
  severity: warning
  verdict: INFEASIBLE
  origin: pre-existing
  message: the acceptance item "bash scripts/gate.sh exit 0" is undemonstrated because two eventlog-postgres cases expect() EVENTLOG_TEST_POSTGRES_URL instead of skipping; eventlog-postgres is byte-identical to the base, so this is a repository finding and needs a fixture the coordinator owns.
- file: crates/eventlog-file/src/journal.rs
  line: 286
  category: integrity
  severity: note
  verdict: INFEASIBLE
  origin: introduced
  message: resume does not sweep .write-* staging files or privacy.next the way every transaction did at the base, and the source calls those files possibly sensitive, but no path through the public API was found that leaves one behind while a handle stays open.
```
