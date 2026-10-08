---
format: aep.planning-md/3
id: review-result:security-file-eventlog-rechecks-its-prefix-pass-1
kind: review-result
status: archived
title: Security review pass 1 on the prefix comparison (9920a7db)
tags:
- security
relations:
- reviews: story:file-eventlog-rechecks-its-prefix-by-comparison
revision: 2
transitions:
- {from: "active", to: "archived", at: "2026-10-08T11:00:48Z", actor: "human:timo", revision: 2}
---
Recorded verbatim except that the home-directory prefix is written `~`, as the repository's personal-paths rule requires.

```
unit: story:file-eventlog-rechecks-its-prefix-by-comparison, commit 9920a7db (tree el42-compare, my two untracked test files added)
verdict: NEEDS-CHANGE
cases: executed 178→187, red 0
origin: introduced 4 / pre-existing 0 / undecided 0
wrote-outside-worktree: 4 paths (part 6)
needs-coordinator: acceptance bullet 4 (per append ≤ 5.44 ms) is not met: amend the bar or hold the unit
```

**Answer:** the safety invariant holds on all six points you asked about. Under test, the code refuses every in-place damage I could put in the prefix, at both the head and the base. The unit is still not ready for one non-security reason: its own acceptance bar for per-append time is missed by more than 3x. There is also one suite gap, which my new cases close.

**1. Diff stat**

`git --no-pager diff --stat`:
```
 crates/eventlog-file/src/journal.rs | 4 ++++
```
That `journal.rs` change is **not mine**. It is the adversary's `#[cfg(test)] mod adversary_compare_once;` hook (working-tree line 34), which shifts later `journal.rs` lines by +4. All line numbers below are at 9920a7db. My only changes are two new untracked test files:
- `crates/eventlog-file/tests/security_compare_prefix.rs` (309 lines)
- `crates/eventlog-file/tests/security_compare_persisted.rs` (297 lines)

**2. Cases added (each run alone first)**

| file :: case | asserts | head | base | mutant |
|---|---|---|---|---|
| prefix :: `the_writer_refuses_damage_at_the_first_byte_of_the_second_piece` (:192) | a read and an append refuse damage at byte 65,536 of a prefix over 196 KiB; history unchanged; a fresh open refuses the same bytes | green | green | **red** |
| prefix :: `…_at_the_last_byte_of_the_first_piece` (:198) | same check at byte 65,535 (boundary control) | green | green | green |
| prefix :: `…_in_the_last_frame_past_every_piece` (:204) | same check in the newest frame | green | green | **red** |
| prefix :: `…_at_the_last_committed_byte` (:210) | same check on the final `\n` | green | green | **red** |
| prefix :: `a_capture_sharing_the_writers_bytes_refuses_damage_past_the_first_piece` (:221) | after a capture, own appends and a re-capture that shares the buffer, damage refuses the capture (twice), the read and the append | green | green | **red** |
| prefix :: `a_read_only_capture_refuses_damage_past_the_first_piece` (:284) | a `FileTenantCapture` refuses twice and changes nothing | green | green | **red** |
| persisted :: `persisted_frame_digests_are_sha256_over_canonical_frames_on_every_changed_path` (:173) | frame digests, chain and manifest recomputed with `sha2` after open, capture, own appends, redaction and an append to the new epoch | green | green | green |
| persisted :: `a_privacy_intent_is_bound_by_sha256_over_the_raw_replacement_bytes` (:246) | a hand-written intent with `replacement_digest` = SHA-256 of the raw replacement is completed by the opener | green | green | green |
| persisted :: `a_privacy_intent_bound_by_another_digest_is_refused` (:275) | any other digest refuses and nothing is replaced | green | green | green |

- **Alone runs at head:** prefix `6 passed; 0 failed` EXIT=0. Persisted first run `2 passed; 1 failed`, `called Result::unwrap() on an Err value: RedactedHistory`. That was my fixture's error: capturing a redacted tenant is refused by design. I moved the captures to an unredacted tenant and the rerun gave `3 passed` EXIT=0.
- **Mutant** (scratch copy, one line at `journal.rs:230`): `if read == 0 || (compared == 0 && buffer[..read] != verified[..read]) {`
  - All 175 existing cases stayed green.
  - Mine: `FAILED. 1 passed; 5 failed`.
  - Sample failure: `first byte of the second piece: the writer's resume served a prefix damaged in place: Ok(StreamSlice { events: [...`
- **Base copy:** made with `git archive 32168648`; all 9 cases green, EXIT=0.
- **After the suite:** a cast fix for clippy; the prefix file reran green (6 passed). Clippy `-D warnings` on my targets exits 0, and rustfmt `--check` on my files exits 0.

**3. Suite run**

- **Full run:** `cargo test --locked -p eventlog-file --no-fail-fast -- --test-threads=1` → EXIT=101, executed 187: 184 passed, 3 failed.
  - All 3 failures are the adversary's: `journal::adversary_compare_once::{a_capture_after_own_appends_…, a_complete_open_keeps_about_the_committed_size, a_writer_that_rereads_everything_…}`. None are mine.
- **Before count:** the same command with `--lib` plus all 24 other `--test` targets, and only `security_compare_prefix` and `security_compare_persisted` left out. Executed 178: 175 passed, 3 failed (the same adversary cases).

**4. Findings (commit 9920a7db)**

| # | file:line | what was measured | what reaches it | verdict / origin |
|---|---|---|---|---|
| A | `.engineering/planning/story/file-eventlog-rechecks-its-prefix-by-comparison.md:80` | The bar is per append ≤ 5.44 ms (27.21 / 5). Probe at 2,000 events / 40 streams, alternating: head 35.00 and 17.70 ms, base 30.22 and 27.46 ms (load 7.6–12.6). In-window read: head 91 and 53 ms, base 1,553 and 1,629 ms. The commit and CHANGELOG say per-append "did not move measurably" without saying the bar is missed. | the acceptance statement | NEEDS-CHANGE / introduced |
| B | `crates/eventlog-file/src/journal.rs:230` | No existing case damages a prefix longer than one 64 KiB read. The first-piece-only mutant passes 175/175, and my prefix file turns 5 red against it. | every in-window writer resume and capture resume on any store over 64 KiB | CONFIRMED / introduced |
| C | `website/docs/operations.md:23` | "budget about the committed size … per open handle" understates. After a capture, a writer that rereads everything gets its own buffer (`lib.rs:156-159` → `journal.rs:562`), and only the next capture shares them again (`capture.rs:199-203`). With no further capture the handle holds two copies indefinitely. `file-provider.md:112-113` already says this; the adversary's case measures 2 buffers. | redaction, erasure, a refusal that wrote, or a capture that moved the head, followed by writes | CONFIRMED / introduced |
| D | `crates/eventlog-file/src/journal.rs:204` | `share` compares up to the whole committed prefix under two std Mutexes inside the async capture future (`capture.rs:163,186`). That runs on the tokio worker thread, not in `spawn_blocking`. This is a cost, not a correctness problem. | every `FileEventStore` capture whose views are not already one buffer | CONFIRMED / introduced |

The fix for B is to keep `security_compare_prefix.rs`. A has nothing to fix in code: the bar needs a decision.

**5. Checked and could not fault**

- **(1) Every re-hash path now compares.** The base had one re-hash site (`journal.rs:681` at base), inside `resumed_committed`. Writer resume, strict resume, `redact`, `forget_tenant` and the privacy rewrite all reach it, and it is now `journal.rs:790`: a length check, then an exact compare. Inline admin and `register_inline` do full strict reads at both commits.
- **(2) The shared copy cannot be altered by another view.** The only mutation is `extend_from_slice` (`journal.rs:145`). Each view reads only its own first `length` bytes. `share` requires equal, non-empty prefixes. The lock order has no cycle.
- **(3) Nothing is trusted that the base did not trust.** The stamp path (`journal.rs:779`), `trusted()` and `UNTRUSTED_NS` (`:265`) are unchanged. An own append still clears the stamp (`:622`). A resume restamps only from its own read.
- **(4) Persisted digests are untouched.** `encode`, `hash`, `decode_chain`, `recover_privacy` and `replacement_digest` (`:712`) are outside the diff, and my persisted file recomputes them green at head and base. This takes the story's safety fact to step 4.
- **(5) Memory is bounded as the design doc states.** The doc matches the code, including the two-copy window. The adversary's 8,388,608-byte capacity figure for a 4,832,016-byte prefix is capacity; I did not measure resident memory.
- **(6) The design doc reads no weaker.** The stamp table and envelope are unchanged. `operations.md` now admits the 2 s stamp exception that already existed at base.
- **Existing tests.** The unit's in-source `stamped_resume_cost` edits rename the counter with the same bounds and add assertions; nothing was weakened.

**6. Written outside the worktree**

- `~/.cache/eventlog-el42/compare/security/`: the logs listed above, plus `mutant/` (1.0G, including its `target/`) and `base/` (302M, including its `target/`). Left in place, not deleted.
- `~/.cache/eventlog-el42/probe/el42-compare-sec/` (128M)
- `~/.cache/eventlog-el42/probe/el42-compare-sec-base/` (128M)

Lease `el42-compare-sec` is released.

**7. Findings block**

```findings
- file: .engineering/planning/story/file-eventlog-rechecks-its-prefix-by-comparison.md
  line: 80
  category: acceptance
  severity: blocker
  verdict: NEEDS-CHANGE
  origin: introduced
  message: per append measured 17.70-35.00 ms at head against an acceptance bar of 5.44 ms, so the story's fourth acceptance bullet is unmet
- file: crates/eventlog-file/src/journal.rs
  line: 230
  category: mutant
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: a comparison that checks only the first 64 KiB piece passes all 175 existing cases; tests/security_compare_prefix.rs kills it with 5 red
- file: website/docs/operations.md
  line: 23
  category: contract-drift
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: the per-handle budget omits that a FileEventStore keeps two full copies from a writer's complete reread until its next capture, indefinitely if none follows
- file: crates/eventlog-file/src/journal.rs
  line: 204
  category: judgement
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: share compares up to the whole committed prefix under std mutexes on the async executor thread rather than in spawn_blocking
```
