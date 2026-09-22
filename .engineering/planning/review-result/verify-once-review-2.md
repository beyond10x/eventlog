---
format: aep.planning-md/1
id: review-result:verify-once-review-2
kind: review-result
status: active
title: 'Independent verification pass 2: the resumed-handle gate holds, and nothing was pinning it'
relations:
- reviews: story:file-eventlog-verifies-once-per-open
revision: 2
---
unit: story:file-eventlog-verifies-once-per-open — commit 9f234c5d09b234cf207d5d8a311ac8a642c6bc69, worktree ~/.local/state/worktree/trees/b10x/eventlog/ess-evolution-verify-once-review-2-20260921 (HEAD detached, no tracked file modified)
verdict: green
cases: executed 65→84, red 0
origin: introduced 3, pre-existing 0, undecided 0
wrote-outside-worktree: 4 roots — this report, ~/.cache/ess-wave-v2/el1r2/tmp/, ~/.cache/ess-wave-v2/el1r2/measure/, ~/.cache/ess-wave-v2/el1r2/mutate/; full list in §10
needs-coordinator: yes

**Nothing I could express as a program found an acceptance invariant unenforced.** Seventeen cases
written against the sentences this unit itself wrote — in `docs/design/file-provider.md`, in the
story's `## Outcome` and `## Acceptance`, and in the `CHANGELOG` entry — are all green against the
shipped code, and my own re-measurement corroborates the cost number the story records.

What is not green is **invariant 5**. The story's `## Acceptance` names it explicitly ("AGENTS.md
invariant 5: the new test fails without the fix (mutation applied, watched, reverted)"), and
`Journal::resume` — the function this unit added and the whole of the new safety gate — does not
satisfy it. **Eleven one-line deletions from the code this unit added leave all 65 cases green**
(§5). I wrote the seven cases that pin the seven that matter, verified each goes red under its own
mutation and green with it reverted, and left the remaining four unpinned with a reason. That is
the finding, it is not a merge blocker, and the remedy is adopting a file that already exists and
already passes.

---

## 1. `git status --porcelain` — proof of the bound

```
$ cd ~/.local/state/worktree/trees/b10x/eventlog/ess-evolution-verify-once-review-2-20260921
$ git --no-pager diff --stat
$ git status --porcelain
?? crates/eventlog-file/tests/measure_review_two.rs
?? crates/eventlog-file/tests/verify_once_review_two.rs
```

`diff --stat` is empty: the two files I wrote are new and I ran no `git add`. `git status
--porcelain` is therefore the bound's proof — exactly two paths, both under
`crates/eventlog-file/tests/`. **No implementation file, no document, no conformance exercise and
no `.engineering/` path was touched**, and no `git` write command (`commit`, `add`, `stash`,
`switch`, `checkout`, `branch`, `worktree`) was run at any point. The mutations in §5 were applied
to a `git archive` extraction in scratch, never to this worktree.

---

## 2. Area 1 — what the prefix hash does not cover

The brief's list, one case each. Every one is **green**: for each shape the refusal is reached, and
for each I recorded which check reaches it (read from the source, confirmed by the mutation matrix
in §5 — deleting that check is what makes the case red).

| shape | case | check that refuses | now |
| --- | --- | --- | --- |
| a change entirely past `observed.length` that is not a valid frame | `an_unexplained_tail_past_the_committed_length_is_refused_and_preserved` (:97) | `resume` byte-length comparison (`journal.rs:356`), then the complete opener's `len > manifest.length` | green |
| missing committed bytes, truncated to a frame boundary | `a_truncation_to_a_frame_boundary_is_refused_without_altering_the_history` (:120) | same byte-length comparison, then `len < manifest.length` | green |
| a consistent rollback to an older valid head | `a_consistent_rollback_to_an_earlier_head_does_not_extend_the_observed_head` (:160) | `manifest.sequence < observed.sequence`, then `extends_observed` | green |
| a **same-length** internally consistent replacement history of the same store | `a_same_length_replacement_history_of_the_same_store_is_refused` (:202) | the prefix hash, then `extends_observed` | green |
| a privacy epoch committed by another handle | `a_privacy_epoch_committed_by_another_handle_is_refused_without_altering_the_history` (:253) | the epoch clause, then `extends_observed` | green |
| a manifest naming a different store | `a_manifest_naming_a_different_store_is_refused` (:275) | the store clause, then `extends_observed` | green |
| an epoch moved under **unchanged** committed bytes | `a_manifest_epoch_moved_under_unchanged_committed_bytes_is_refused` (:569) | the epoch clause alone — the prefix hash cannot see this | green |
| a store identity moved under unchanged committed bytes | `a_manifest_store_identity_moved_under_unchanged_committed_bytes_is_refused` (:596) | the store clause alone | green |
| an unknown physical format under unchanged bytes | `a_manifest_format_moved_under_unchanged_committed_bytes_is_refused` (:617) | `manifest.format != FORMAT` alone | green |
| a published append intent | `a_pending_append_intent_is_never_resumed_past` (:640) | the intent clause alone | green |

The last four are the ones worth noticing: **the prefix hash is blind to all four**, because in each
the committed bytes are exactly the bytes the handle verified and only `manifest.json` moved. They
are refused by three clauses of `resume`'s gate and by the intent check — the four clauses §5 shows
nothing else tests.

Each was run alone before the suite. All ten exit 0; logs
`…/el1r2/tmp/cases-alone.log` and `cases-alone-3.log`.

```
$ cargo test -p eventlog-file --test verify_once_review_two -- --exact a_manifest_epoch_moved_under_unchanged_committed_bytes_is_refused
running 1 test
test a_manifest_epoch_moved_under_unchanged_committed_bytes_is_refused ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 16 filtered out; finished in 0.07s
EXIT=0
```

**One fixture failure of my own, reported rather than hidden.** The first run of
`a_truncation_to_a_frame_boundary…` panicked at its own precondition (`the fixture has more than
one frame to truncate to`) because `seeded()` commits one frame and the truncation had nothing to
truncate to. That is a typo, not a finding; I added a second append and re-ran. Verbatim in
`…/el1r2/tmp/cases-alone.log`.

---

## 3. Area 2 — `Content` across every path that advances the committed bytes

One case per path. Each runs the path, then **reads once** (which must succeed — a path that left
the handle refusing a healthy store is as much a defect as one that left it trusting a changed one,
and this read is also what re-establishes the resumed view), then damages a committed frame in
place and requires both the next read and the next append to refuse **and** `events.jsonl` and
`manifest.json` to be byte-identical afterwards.

| path that advances the committed bytes | case | now |
| --- | --- | --- |
| `register_inline` (appends through `Journal::open`, not `transaction()`) | `the_committed_prefix_guard_holds_after_an_inline_registration` (:344) | green |
| `attach_inline_existing` + `rebuild_inline_projection` (appends through `Strict::into_journal`) | `the_committed_prefix_guard_holds_after_an_inline_attach_and_rebuild` (:366) | green |
| `capture_tenant` on `FileEventStore` (moves `runtime.observed` and never touches `runtime.verified`) | `the_committed_prefix_guard_holds_after_a_capture` (:395) | green |
| `redact` — a privacy rewrite, new epoch, `Content::of(replacement)` | `the_committed_prefix_guard_holds_after_a_privacy_rewrite` (:407) | green |
| all of them in sequence, healthy store, no refusal permitted | `a_healthy_store_is_not_refused_after_any_path_that_advances_the_committed_bytes` (:427) | green |

`Content` is correct on every one. The reason is structural and worth recording for the next
reader: `Verified` is constructed in exactly one place (`lib.rs:189`) from one `into_parts()`, so
manifest, frames and content can never disagree; and every way `Content` could go stale is
**fail-safe**, because the hash is taken over exactly `observed.length` bytes — a stale content
makes `resume` return `None` and the complete opener re-verifies. §5 confirms this by measurement:
deleting `self.content.absorb(&line)` from `append` and deleting the cache-retirement filter both
leave a correct store, just a slow one.

---

## 4. Area 4 and area 5 — refusal without altering the history, and the sweep

**Refusal without altering the history** is asserted inside nine of the ten area-1 cases and all
four area-2 guard cases: each snapshots `events.jsonl` and `manifest.json` before the refusal and
requires byte-identity after. Every one holds. `sweep_unselected` runs only after a refusal
decision has already been made in both callers (`journal.rs:228` after `decode`, `journal.rs:391`
after the prefix hash and the chain check), so a refusing open sweeps nothing.

**The converse question the brief asks — is any swept name one a durable intent had selected?** The
one name that can be selected is `privacy.next`, selected the moment `privacy.json` is published.

```
a_resumed_handle_does_not_sweep_a_replacement_a_durable_intent_selected  (:501)   green
```

It builds a real crashed-privacy state (a byte-exact copy of the store, erased in the copy, so the
store identity travels with the replacement; `privacy.next` staged, `privacy.json` published with a
correct `replacement_digest`), runs one operation on the **open handle**, and then requires that a
fresh open succeeds and `events.jsonl` is exactly the replacement bytes. It is green, and §5 shows
it is the only case in the repository that goes red when the `privacy.json` half of `resume`'s
intent check is deleted. Ordering in the source is what makes it safe: `resume` returns `Ok(None)`
on a pending intent at `journal.rs:335-337`, **before** `sweep_unselected` at `:391`.

**A concurrent handle with a `.write-<uuid>` in flight: no.** Every caller of `atomic_json`
(`journal.rs:188, 259, 275, 428, 753`) and the one direct `write_synced` of `privacy.next`
(`:421`) runs while its own `Journal` holds `writer.lock`, and `sweep_unselected` is only ever
called under that same lock. `std::fs::File::lock` is `flock(2)` on a per-open-file-description
basis, so two handles in one process serialize as strictly as two processes do. Read, not run.

---

## 5. The finding: `Journal::resume`'s gate fails AGENTS.md invariant 5

Probed on a `git archive 9f234c5` extraction under `~/.cache/ess-wave-v2/el1r2/mutate`,
never in the worktree. Each row is one deletion from the code **this unit added**, with the rest of
the crate left alone; the tree was restored from saved copies between every row and verified
identical to 9f234c5 at the end.

| # | deletion | `cargo test -p eventlog-file` (the unit's 65 cases) | with my file present |
| --- | --- | --- | --- |
| M1 | `resume`: the `events.jsonl` byte-length comparison with `manifest.json` (`journal.rs:356`) | **exit 0** | red: `an_unexplained_tail_past_the_committed_length…` |
| M2 | `resume`: `manifest.length < observed.length` (`:350`) | **exit 0** | exit 0 — fail-safe, not pinned |
| M3 | `resume`: `manifest.sequence < observed.sequence` (`:349`) | **exit 0** | exit 0 — fail-safe, not pinned |
| M4 | `resume`: `manifest.epoch != observed.epoch` (`:348`) | **exit 0** | red: `a_manifest_epoch_moved_under_unchanged…` |
| M5 | `resume`: `manifest.store != observed.store` (`:347`) | **exit 0** | red: `a_manifest_store_identity_moved_under_unchanged…` |
| M6 | `resume`: the `privacy.json` half of the intent check (`:335`) | **exit 0** | red: `a_resumed_handle_does_not_sweep_a_replacement…` |
| M7 | `resume`: the `append.json` half of the intent check (`:335`) | **exit 0** | red: `a_pending_append_intent_is_never_resumed_past` |
| M8 | `resume`: `manifest.format != FORMAT` (`:344`) | **exit 0** | red: `a_manifest_format_moved_under_unchanged…` |
| M9 | `enter`: blob verification of newly bound objects (`lib.rs:546`) | exit 101 ✓ | — |
| M10 | `enter`: `if advanced { tx.clear_snapshots()? }` (`lib.rs:548`) | **exit 0** | exit 0 — see §7 J2, the branch's stated reason is unreachable |
| M11 | `transaction`: the cache-retirement `.filter(…)` (`lib.rs:141`) | **exit 0** | exit 0 — fail-safe, not pinned |
| M12 | `append`: `self.content.absorb(&line)` (`journal.rs:283`) | exit 101 ✓ | — |
| M13 | `resume`: `observed_content.absorb(&tail)` (`journal.rs:385`) | **exit 0** | red: `a_handle_that_folded_fresh_frames_still_resumes…` |
| M14 | `resume`: the prefix hash comparison (`journal.rs:374`) — control | exit 101 ✓ | — |

Eleven of fourteen leave the suite green. Three are caught, and they are the three the unit and the
correction had reasons to test: M9 and M14 are what findings F1–F3 were about, M12 falls out of
M14's cases. The correction's report §5 records exactly two mutation checks, both of them on the
two checks its own fix introduced. **Nothing mutation-checked the gate `resume` was built out of.**

Verbatim, the four I consider load-bearing and fail-*open*, each one run alone against my file:

```
M1 drop resume byte-length check       exit 101  | an_unexplained_tail_past_the_committed_length_is_refused_and_preserved
M4 drop resume epoch clause            exit 101  | a_manifest_epoch_moved_under_unchanged_committed_bytes_is_refused
M5 drop resume store clause            exit 101  | a_manifest_store_identity_moved_under_unchanged_committed_bytes_is_refused
M7 drop resume append.json clause      exit 101  | a_pending_append_intent_is_never_resumed_past
M8 drop resume format check            exit 101  | a_manifest_format_moved_under_unchanged_committed_bytes_is_refused
M6 drop resume privacy.json clause     exit 101  | a_resumed_handle_does_not_sweep_a_replacement_a_durable_intent_selected
M13 resume absorbs nothing for tail    exit 101  | a_handle_that_folded_fresh_frames_still_resumes_instead_of_rereading_everything
```

and the same seven with the source restored, which is what §8 reports.

Why the four I left unpinned are the right four to leave:

- **M2, M3, M11** are fail-safe. A resumed handle that loses one of these still hashes exactly
  `observed.length` bytes against a value taken over exactly those bytes, so a manifest it should
  have rejected either fails the hash or fails `decode_chain`, and the complete opener decides.
  Pinning them would test the second line of defence, not the first.
- **M10** is a branch whose stated reason cannot arise. See §6.

**M13 is the one nobody would ever notice.** Without `observed_content.absorb(&tail)` every case in
the repository stays green and the store stays correct — but a handle that has once folded another
writer's frames carries a content hash over only its prefix, so **every transaction it makes for
the rest of its life falls back to the complete reread**, which is precisely the O(store) cost this
unit exists to remove. My case
`a_handle_that_folded_fresh_frames_still_resumes_instead_of_rereading_everything` (:667) asserts it
without timing, using the acceptance's own trick: after the handle folds a second writer's frame,
`blobs/` is deleted; a handle still on the resumed path opens no blob it does not touch and does
not notice, and one that fell back re-hashes every active object and fails.

---

## 6. Area 3 — the cost, re-measured independently

Harness `crates/eventlog-file/tests/measure_review_two.rs`, inert without its environment variable
(the `durability::process_writer` pattern), release profile, fixture rebuilt from scratch by me and
copied fresh before each run, on the same i9-10900K without SHA-NI.

```
FIXTURE events.jsonl=1519697 frames=2000 blobs=1000
MEASURE committed_bytes=1519697 open_ms=144.8 read_200_ms=1079.8 per_transaction_ms=5.399
MEASURE committed_bytes=1519697 open_ms=141.6 read_200_ms=1055.3 per_transaction_ms=5.276
MEASURE committed_bytes=1519697 open_ms=145.0 read_200_ms=1056.4 per_transaction_ms=5.282
```

An earlier set of three, same fixture, taken before a cosmetic edit to the harness: 1,100.4 /
1,234.7 / 1,144.0 ms.

| | story `## Outcome` records | I measure |
| --- | --- | --- |
| fixture `events.jsonl` | 1.51 MB, 2,000 frames, 1,000 blobs | 1,519,697 B, 2,000 frames, 1,000 blobs |
| 200 read transactions | 1,045 / 1,044 / 1,171 ms | **1,080 / 1,055 / 1,056 ms** |
| per transaction | "about 5 ms per 1.5 MB" | **5.40 / 5.28 / 5.28 ms** |
| `open` | 256 / 145 / 133 ms | 145 / 142 / 145 ms |

**The number in the story is right**, within 1–3% of mine on the same fixture shape. Whether the
cost is acceptable is not mine to say and I do not say it; the story's own deferral to
`story:incremental-history-digest-for-a-resumed-handle` is where that belongs. I did not measure
`f802eb8` or `c698923` — re-measuring the columns the coordinator already has would have cost two
more release trees for no new information, and the column that needed independent confirmation is
the one for the commit under review.

---

## 7. Judgement findings — text, not store records

All three cover commit **9f234c5** in the worktree named in the header. I wrote nothing to the
planning store and ran no `aep plan artifact` verb of any kind; the store was read once, with
`aep artifact show`.

### J1 — `Journal::resume`'s gate is not covered by any case in the repository

| | |
| --- | --- |
| **what was measured** | §5. Eleven one-line deletions from `crates/eventlog-file/src/journal.rs` and `src/lib.rs` leave `cargo test -p eventlog-file` at exit 0 with all 65 cases passing. Seven of them are guard clauses in `Journal::resume` (`journal.rs:335, 344, 347, 348, 349, 350, 356`) and one is `journal.rs:385`. |
| **what reaches it** | AGENTS.md invariant 5 is a repository invariant, and the story's `## Acceptance` names it by number. `resume` did not exist at `f802eb8` (`git show f802eb8:…/journal.rs` has no `fn resume`); it and all seven clauses arrived at `c698923`, and the correction added two more lines to the same gate. Four of the eight are fail-*open* under deletion: M4, M5 and M8 let a handle adopt a manifest naming a different epoch, a different store or an unknown format over bytes it did verify, and M7 lets a resumed handle walk past another process's published `append.json` and then overwrite it with its own on the next commit. |
| **verdict / origin** | `NEEDS-CHANGE` / `introduced` — **not a merge blocker**: the shipped behaviour is correct on all seventeen of my cases. What has to change is that the guards get cases, and those cases exist, are green, and are mutation-verified one-to-one. |
| **the remedy, named and not applied** | adopt `crates/eventlog-file/tests/verify_once_review_two.rs` as pass 1's file was adopted. It pins seven of the eight, byte-for-byte as it stands, and needs no edit. |

### J2 — the resumed path's `clear_snapshots` is motivated by a state this crate cannot produce

| | |
| --- | --- |
| **what was measured** | `crates/eventlog-file/src/lib.rs:548`. Deleting `if advanced { tx.clear_snapshots()?; }` leaves the suite green (§5, M10) — and so does my file, deliberately. Read, not run, for the reason: the only `Op::Generation` in the crate that **replaces** an existing generation is recorded by `redact` (`lib.rs:1033`), which sets `tx.privacy = true` two lines later and mints a new epoch. `Transaction::generation` (`lib.rs:366`) only mints one for a stream that has none, which retires no cached snapshot. A new epoch fails `resume`'s epoch clause and goes to the complete opener, which calls `clear_snapshots` itself — before `advanced` is ever consulted. |
| **what reaches it** | The branch runs on every advanced resume. Its **stated reason** — "Another writer's frames can retire a generation this handle still has cached" — reaches nothing, because a writer cannot retire a generation without also bumping the epoch. A stale snapshot is in any case refused on load by the generation comparison at `lib.rs:932`, so nothing depends on this call for correctness. |
| **verdict / origin** | `CONFIRMED` / `introduced` — the comment is wrong and will mislead the next reader into thinking cross-handle generation retirement is a same-epoch event. The code itself is harmless. |

### J3 — the design page's lock sentence is no longer true on the fall-through path

| | |
| --- | --- |
| **what was measured** | Read, not run. `docs/design/file-provider.md:18-21`, rewritten by this unit, says each operation "opens its own lock file description, takes an exclusive process lock, establishes the committed history it will work against, and retains that lock through callbacks and commit." On every path where `Journal::resume` returns `Ok(None)` (`journal.rs:336, 340, 352, 357, 369, 375, 388`), the `File` holding `writer.lock` is a local that drops at return — `flock(2)` is released by the last close of the description — and `enter` (`lib.rs:554`) then calls `Journal::open_existing`, which takes the lock again. Two acquisitions, one gap. |
| **what reaches it** | Every fall-through: every refusal, and every ordinary transaction that follows `register_inline`, `rebuild_inline_projection`, a privacy commit or a capture that moved the head. It is a reached path, not a constructed one. |
| **verdict / origin** | `INFEASIBLE` / `introduced` — **I could not show a consequence.** The second acquisition re-reads and re-chains everything and re-runs `extends_observed`, so anything that happened in the gap is re-validated; the only thing that can change is which refusal you get. I could not write a deterministic case for a race whose outcome is the same either way, and I did not write a flaky one. The sentence is still inaccurate, and correcting it costs a doc line. |

---

## 8. The suite, after the cases existed

```
$ cargo test -p eventlog-file --no-fail-fast
     Running unittests src/lib.rs
test result: ok. 15 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.57s
     Running tests/capture_review_one.rs
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.51s
     Running tests/capture_review_two.rs
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.11s
     Running tests/conformance.rs
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.13s
     Running tests/consistent_capture.rs
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.80s
     Running tests/durability.rs
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.10s
     Running tests/existing_open.rs
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.11s
     Running tests/inline_admin.rs
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.71s
     Running tests/measure_review_two.rs
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
     Running tests/verify_once_review.rs
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.27s
     Running tests/verify_once_review_two.rs
test result: ok. 17 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.65s
   Doc-tests eventlog_file
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
SUITE_EXIT=0
```

| command | exit | cases |
| --- | --- | --- |
| `cargo test -p eventlog-file --no-fail-fast` | **0** | 84 |
| the same with my two files deselected (every other target run explicitly) | **0** | **65** |
| `cargo fmt -p eventlog-file -- --check` | **0** | — |
| `cargo clippy -p eventlog-file --all-targets -- -D warnings` | **0** | — |
| `cargo test --workspace --locked --exclude eventlog-postgres` | **0** | 186 in 34 lanes |

`<before> = 65` is taken twice and agrees: the correction's own `cases: executed 60→65`, and a
second run here with my two test targets deselected (`--lib` plus each of the eight pre-existing
integration targets named explicitly), logged at `…/el1r2/tmp/before.log`. `<after> = 84` is
65 + 17 review cases + 2 measurement-harness cases; the harness cases return immediately without
their environment variable, so they execute and cost nothing.

My first `fmt` and `clippy` runs were red **on my own two files only** (`rustfmt` line wrapping and
two `clippy::pedantic` casts in the measurement harness). I fixed my files and re-ran; no
implementation file was reformatted — `cargo fmt -p eventlog-file -- --check` was clean apart from
my two paths before I touched anything.

`bash scripts/gate.sh` was not run: it is review-1 finding J2 (`INFEASIBLE` / `pre-existing`, two
`eventlog-postgres` cases that `expect()` `EVENTLOG_TEST_POSTGRES_URL` instead of skipping), the
brief lists it as already filed and not a finding, and nothing I did touches it. The
`capture::tests::the_strict_reader_holds_the_writer_lock_for_its_whole_life` flake the brief also
lists as filed did not reproduce in any of my four package runs or the workspace run.

---

## 9. Reviewed and could not fault

One line each; this is the part that says what my silence is worth.

- **`resume`'s decision is airtight for every manifest shape I could enumerate.** When `manifest ==
  *observed` the file length equals both lengths and the prefix hash covers the whole file, so the
  bytes are provably the ones verified; when it differs, the only field that can differ without the
  length or sequence moving is `digest`, and an empty tail then fails `decode_chain`'s final
  `previous != manifest.digest`. There is no manifest I could construct that passes the gate over
  bytes the handle had not verified.
- **`Content` cannot disagree with the manifest it travels with.** One construction site
  (`lib.rs:189`), one `into_parts()`, and each of the five `Journal` construction sites establishes
  the invariant independently (`open_with_creation`, `append`, `privacy`, `Resumed::into_journal`,
  `Strict::into_journal`). Checked all five by reading and all five by case (§3).
- **Every way `Content` can go stale is fail-safe**, because the hash is bounded by `observed.length`
  — measured, not assumed: M11 and M12 in §5.
- **The sweep never removes a selected name**, because `resume` returns on a pending intent before
  it sweeps and `open_with_creation` recovers before it sweeps. Case at §4.
- **No `.write-<uuid>` is ever in flight outside the writer lock**: all six writers of a staging
  name hold it, and `flock(2)` is per-description so same-process handles serialize too.
- **A refused transaction leaves no cache and a privacy rewrite is never cached** — `take()` before
  `enter`, `cacheable = !tx.privacy`. Pass 1 checked it; I re-checked it and agree.
- **The blob trade is exactly what the page now says it is.** Objects bound by frames a handle has
  not seen are verified on first sight (`lib.rs:546`, and M9 is caught), objects it verified at open
  are not re-read, and `blob()` hashes what it reads every time.
- **No existing case was weakened.** `git diff f802eb8 9f234c5 --numstat` on `tests/` is `211/1`
  and `172/0`; the single deleted line is a `use` statement. Nothing under
  `crates/eventlog-conformance/` is touched, and all twelve shared contracts pass.
- **The whole workspace outside PostgreSQL is green** with my files present (186 cases, exit 0).

---

## 10. Every path written outside the worktree

- `~/beyond10x/.ess-evolution/waves/0005-aep-migration/wave-validate-v2-20260920/unit-3-eventlog-verify-once/review-2-report.md` — this report, as the brief's `report:` line directs
- `~/.cache/ess-wave-v2/el1r2/tmp/` (108 KB) — the assigned `TMPDIR` for every cargo
  command, and the logs: `build.log`, `cases-alone.log`, `cases-alone-2.log`, `cases-alone-3.log`,
  `suite.log`, `suite2.log`, `suite-final.log`, `before.log`, `workspace.log`, `fmt.log`,
  `fmt2.log`, `clippy.log`, `clippy2.log`, `relbuild.log`, `relbuild2.log`
- `~/.cache/ess-wave-v2/el1r2/measure/` (27 MB) — `fixture/` (the 2,000-frame,
  1,000-blob acceptance fixture) and `run/` (the copy each timed run consumes)
- `~/.cache/ess-wave-v2/el1r2/mutate/` (906 MB) — a `git archive 9f234c5` extraction with
  its own `target/`, plus `journal.rs.orig` and `lib.rs.orig`, used only for §5. **The sources in
  it are restored to 9f234c5 and verified byte-identical**; the worktree was never mutated and
  never moved.

Nothing under `/tmp`. `CARGO_TARGET_DIR` never set — the debug and release builds are in
`<worktree>/target` and the probe's build is in its own tree. Disk never below 40 GB free,
MemAvailable never below 42 GiB. Nothing written under `.engineering/`; no `aep plan artifact`
verb of any kind was run.

**One item for the coordinator beyond J1.** The story record's `scope` header now carries six paths
including `crates/eventlog-file/src/state.rs`, but the `## Scope` section in the body still cites
five and omits it. That is review-1 finding J1 half-applied, it is a store edit and therefore
yours, and I raise it here rather than as a finding because the brief tells me not to re-report
pass 1.

---

## 11. Ledger, pass 1 → pass 2

| | |
| --- | --- |
| **resolved** | F1, F2, F3. Verified by running, not by reading: pass 1's three cases are in the tree byte-identical and green, and §5's M14 confirms the prefix comparison is what makes them green. J3 (the resumed sweep) is fixed and now has a unit test; §5's M6 confirms my own case is the one that covers the intent-ordering half of it. |
| **carried** | J2 (`gate.sh` exit 0 undemonstrated, PostgreSQL fixture) — untouched by this round, the brief lists it as filed. J1 (`state.rs` outside `## Scope`) — half-applied, see §10. |
| **new** | J1 above (`resume`'s gate fails invariant 5 — `NEEDS-CHANGE`, warning), J2 above (the dead `clear_snapshots` motivation — note), J3 above (the design page's lock sentence — note). |
| **new ground covered, nothing found** | the ten manifest and byte shapes of §2, the five advancing paths of §3, the selected-`privacy.next` question of §4, the concurrency question of §4, and the cost number of §6. |

---

## 12. Findings

```findings
- file: crates/eventlog-file/src/journal.rs
  line: 347
  category: mutant
  severity: warning
  verdict: NEEDS-CHANGE
  origin: introduced
  message: eleven one-line deletions from the code this unit added — seven of them clauses of Journal::resume's gate, four of those fail-open — leave all 65 of the unit's cases green, so the whole new safety gate fails AGENTS.md invariant 5, which the story's own Acceptance names; crates/eventlog-file/tests/verify_once_review_two.rs pins seven of them and is green and mutation-verified as it stands.
- file: crates/eventlog-file/src/lib.rs
  line: 548
  category: judgement
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: the resumed path's clear_snapshots is justified by "another writer's frames can retire a generation this handle still has cached", but the only Op::Generation that replaces an existing generation is recorded by redact, which mints a new epoch and therefore sends the resuming handle to the complete opener before the advanced branch is consulted.
- file: docs/design/file-provider.md
  line: 18
  category: contract-drift
  severity: note
  verdict: INFEASIBLE
  origin: introduced
  message: the page says each operation takes the process lock and retains it through callbacks and commit, but every fall-through drops resume's lock description and reacquires the lock in Journal::open_existing, and I could not demonstrate a consequence because the second acquisition re-validates everything.
```
