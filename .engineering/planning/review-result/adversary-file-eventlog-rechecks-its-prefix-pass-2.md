---
format: aep.planning-md/3
id: review-result:adversary-file-eventlog-rechecks-its-prefix-pass-2
kind: review-result
status: archived
title: Adversary pass 2 on the prefix comparison (f6f43177)
tags:
- adversary
relations:
- reviews: story:file-eventlog-rechecks-its-prefix-by-comparison
revision: 2
transitions:
- {from: "active", to: "archived", at: "2026-10-08T11:00:45Z", actor: "human:timo", revision: 2}
---
Recorded verbatim except that the home-directory prefix is written `~`, as the repository's personal-paths rule requires.

unit: story:file-eventlog-rechecks-its-prefix-by-comparison at f6f43177 (the el42-compare tree plus my uncommitted case)
verdict: CONFIRMED
cases: executed 191→192, red 1
origin: introduced 3 / pre-existing 0 / undecided 0
wrote-outside-worktree: 1 directory (~/.cache/eventlog-el42/compare/adversary2/, the assigned scratch, about 1.0G)
needs-coordinator: acceptance bullet 3 still requires "a test asserts zero hashed". Either restore a hash counter or strike that text (finding F2).

**Short answer:** the safety invariant holds. I found no path where adopting a buffer makes a view compare against bytes it did not verify, and none where a dropped view loses a refusal. What breaks is the "held once" claim, under one condition: a capture that is cancelled while it moves its view onto the writer's buffer. Also, nothing in the suite catches a SHA-256 put back on the prefix.

### 1. Diff stat
```
$ git --no-pager diff --stat
 crates/eventlog-file/src/journal.rs | 3 +++
 1 file changed, 3 insertions(+)
?? crates/eventlog-file/src/adversary2_compare_once.rs
```
The `journal.rs` lines are only `#[cfg(test)] #[path = "adversary2_compare_once.rs"] mod adversary2_compare_once;`, which the brief allows. rustfmt placed them above the pass-1 hook. No implementation line changed.

### 2. Case added: `crates/eventlog-file/src/adversary2_compare_once.rs`
`a_capture_dropped_while_it_retains_once_leaves_one_buffer` is **red now**.
- **Setup:** an 8 MB history. The writer's view is present and there is no capture view.
- **What it does:** it polls `capture_tenant` by hand and drops it at the first `Pending` after `retain_once` has cloned the writer's view for its blocking `adopt` task. That moment is when the strong count on the writer's buffer goes above the baseline.
- **What it asserts:** the two views share one buffer.

Run alone, before the suite:
```
panicked at crates/eventlog-file/src/adversary2_compare_once.rs:122:9:
a capture dropped while it retained once left the writer's view (8398104 bytes) and the capture's view (8398104 bytes) in two buffers
test result: FAILED. 0 passed; 1 failed; ... 45 filtered out   EXIT=101
```
- It was red 5 out of 5 times in the scratch copy.
- After the suite I ran `cargo fmt` (it changed only my file and the hook placement). I reran the case alone and it was red the same way.

### 3. Suite, run after the case existed
- `cargo test --locked -p eventlog-file --no-fail-fast -- --test-threads=1`: EXIT=101 over 28 binaries, 192 executed, 191 passed, 1 failed (mine).
- The 191 "before" count is from the implementor's `c1-gate-test.log`.
- `cargo clippy --locked -p eventlog-file --all-targets -- -D warnings`: EXIT=0.
- `cargo fmt -p eventlog-file -- --check`: EXIT=0.

### 4. Findings (at f6f43177)

| # | file:line | Verdict / origin | What was measured | What reaches it |
|---|---|---|---|---|
| F1 | capture.rs:216 (`retain_once`, awaited at :164 and :187) | CONFIRMED / introduced | A capture dropped at the `blocking(adopt)` await leaves the writer's view and the capture's view in two 8.4 MB buffers until the next transaction or capture. At 9920a7db `retain_once` was synchronous (:199, called without `await`; checked by reading, not run). The design doc says at file-provider.md:116 that the comparison happens "inside the operation's blocking file work", which is not true for a capture. Fix: adopt the new view onto the writer's content inside `observed_history`'s blocking closure, the way `enter` does, then drop synchronously. | A `timeout`, `select!` or abort around `capture_tenant`. Nothing I found in this repository does that, and the entity-runtime adapter calls it plainly. |
| F2 | journal.rs:246 (`same_prefix`) | NEEDS-CHANGE / introduced | In a scratch copy I added `let _rehashed = hash(verified);` to `same_prefix`, which puts back a SHA-256 over the whole prefix. All 191 cases stayed green (`mutant-rehash.log`). The correction said the `prefix_bytes_compared == committed` asserts "carry the claim", and this run shows they do not. Acceptance bullets 1 and 3 ("no SHA-256", "a test asserts zero hashed") have no guard. Fix: charge a hashed-bytes counter inside `hash()` and assert it is zero on an in-window resume with no tail. | Any regression that puts a hash back on the resume path. |
| F3 | cost.rs:119, :136 | CONFIRMED / introduced | `every_counter_is_charged` scans source text. In scratch: an uncharged field declared `pub` → red (correct). Declared `pub(crate)` → green. Declared `pub` with only the comment `// cost.prefix_bytes_hashed += 0` in lib.rs → green (`counter-probe.log`). Fix: take the field names from `format!("{:?}", Cost::default())`. | Only a future edit to `Cost`. |

### 5. Attacked and could not break
- **Seeded walk in the scratch copy** (`probe2.log`): 24 seeds × 120 steps over own and other-handle appends, reads, captures, deferred captures, refused captures, refused appends, reopens and redaction rewrites, with random in-place damage. After every step, each check held:
  - at most one buffer between the two views
  - the writer's view decodes as exactly its own manifest's bytes
  - the reader's view is a committed prefix or an older epoch
  - capacity is at most 2× length plus 16K
  - every damaged-prefix read, append and capture refused and changed nothing

  A second walk with 2.1 s sleeps, so the trusted-stamp path runs, passed 4 × 60 steps (`probe1.log`).
- **Adoption across epochs or stores:** every frame carries the store and epoch, so the first frames differ and `adopt` returns false.
- **A longer buffer, or one extended by a cancelled or refused operation:** `absorb` compares those bytes before it shares them. Every buffer extension happens only after a commit, or after a chain verified under the lock.
- **Dropped views:** `keep_reader_once` and `retain_once` drop a view only when the other path would already have fallen back to the complete opener. The same refusals are given.
- **Open sized from the manifest:** a file shorter than the manifest length is refused before `with_capacity`, so the size is bounded by the real file.
- **The privacy residual:** one buffer, the old epoch. It is dropped by the next successful transaction or by any capture. The base held the same data in its decoded fold.

### 6. Paths written outside the worktree
All under `~/.cache/eventlog-el42/compare/adversary2/`:
- `copy/`: a `git archive` of f6f43177 plus my probe modules, with its own `target/`, about 1.0G. Left in place.
- Logs: `copy-build.log`, `probe1.log`, `probe2.log`, `once-copy.log`, `once-copy-dbg.log`, `mutant-rehash.log`, `counter-probe.log`, `red-alone.log`, `red-alone-fmt.log`, `suite.log`, `fmt.log`, `clippy.log`.
- Copies: `cost.rs.orig`, `lib.rs.orig`, `journal.rs.before-fmt`, `once.before-fmt.rs`.

Lease `el42-compare-adv2` is released.

### 7. Findings block
```findings
- file: crates/eventlog-file/src/capture.rs
  line: 216
  category: concurrency
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: a capture dropped at retain_once's blocking adopt await leaves the writer's and the capture's views in two buffers until the next operation, against "held once per handle"
- file: crates/eventlog-file/src/journal.rs
  line: 246
  category: mutant
  severity: warning
  verdict: NEEDS-CHANGE
  origin: introduced
  message: a SHA-256 over the whole prefix put back in same_prefix leaves all 191 cases green, so acceptance bullet 3's "a test asserts zero hashed" is carried by no test
- file: crates/eventlog-file/src/cost.rs
  line: 119
  category: judgement
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: every_counter_is_charged misses an uncharged field declared pub(crate) and accepts a charge that appears only in a comment
```
