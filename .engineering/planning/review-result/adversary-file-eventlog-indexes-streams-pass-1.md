---
format: aep.planning-md/3
id: review-result:adversary-file-eventlog-indexes-streams-pass-1
kind: review-result
status: archived
title: Adversary pass 1 on the per-stream index (d01992d0)
tags:
- adversary
relations:
- reviews: story:file-eventlog-indexes-streams
revision: 2
transitions:
- {from: "active", to: "archived", at: "2026-10-08T11:00:44Z", actor: "human:timo", revision: 2}
---
Recorded verbatim except that the home-directory prefix is written `~`, as the repository's personal-paths rule requires.

unit: story:file-eventlog-indexes-streams, tree ~/.local/state/worktree/trees/b10x/eventlog/el42-index at d01992d0 plus two untracked test files
verdict: CONFIRMED
cases: executed 175→179, red 1
origin: introduced 2 / pre-existing 0 / undecided 0
wrote-outside-worktree: 9 paths, all under ~/.cache/eventlog-el42/index/adversary/
needs-coordinator: none

The index itself held up: I could not make the index code give a wrong answer. The only red case is about documentation: the unit's README edit made one README sentence false. The second finding is a weak assertion in the unit's own test.

**1. Diff stat**

`git --no-pager diff --stat` prints nothing, so no tracked file changed. `git status --short` shows only my two new test files:
```
?? crates/eventlog-file/tests/adversary_stream_index.rs
?? crates/eventlog-file/tests/adversary_stream_index_docs.rs
```
I did not touch any implementation file.

**2. Cases added**

`crates/eventlog-file/tests/adversary_stream_index_docs.rs`, case `the_release_highlights_the_readme_names_cover_the_release_it_pins`. **Red now.** It checks that `website/docs/releases.md` has a section for the release that README pins and links (0.8.0). Run alone, before the suite:
```
---- the_release_highlights_the_readme_names_cover_the_release_it_pins stdout ----
thread '...' panicked at crates/eventlog-file/tests/adversary_stream_index_docs.rs:37:5:
README.md says website/docs/releases.md summarizes the 0.8.0 release notes; its sections are ["## 0.3.0 — September 22, 2026", "## After 0.3.0"]
test result: FAILED. 0 passed; 1 failed; ... EXIT=101
```

`crates/eventlog-file/tests/adversary_stream_index.rs` has three cases. **All green**, run alone (3 passed):
- **`two_handles_and_a_fresh_opener_read_every_stream_as_the_feed_holds_it`**: 120 seeded random steps across two writer handles and two tenants with identical stream names. It checks every stream read on three handles against the tenant feed, which walks events in global order and never uses the index. The streams include name pairs that would collide under a naive `","` join. A tally at the end fails the case if any kind of step never ran. Tally: single 54, group 22, group repeating a stream 10, retry 27, group retry 7, redaction 9, erasure 1, handover 62, check 8.
- **`every_window_boundary_answers_with_literal_versions`**: fixed expected versions for `after` of 0, the head, beyond the head and `u64::MAX`, and `limit` of 0, 1 and `usize::MAX`.
- **`a_retried_receipt_is_its_own_range_after_the_stream_moves_on`**: a retried command and a retried group (one that repeats a stream) return exactly their original range after the stream has moved on and another tenant was erased. Checked on the writer and on a reopened handle.

**3. Suite run** (after the cases existed, run again on the final files)

`cargo test --locked --no-fail-fast -p eventlog-file -- --test-threads=1` exited 101: 178 passed, 1 failed, 179 executed. The one failure is `adversary_stream_index_docs`.

The before-count of 175 comes from the implementor's own run (`~/.cache/eventlog-el42/index/gate-final-test.log`), not from a run of mine. `cargo clippy --locked -p eventlog-file --all-targets -- -D warnings` exited 0, and `cargo fmt -p eventlog-file -- --check` exited 0.

**4. Findings** (commit d01992d0)

| # | file:line | verdict | origin | what was measured | what reaches it |
|---|---|---|---|---|---|
| 1 | README.md:17 | CONFIRMED | introduced | README now links the 0.8.0 notes and says the release highlights summarize them. `releases.md` only covers releases up to 0.3.0. The new red case shows it. At base the sentence was true: README pinned 0.3.0 and `releases.md` has a `## 0.3.0` section (read with `git show 32168648:…`). | Anyone reading the README Status section. Fix: add 0.4.0–0.8.0 highlights to `releases.md`, or point line 17 at CHANGELOG. `website/docs/intro.md:21` and `website/docs/quickstart.md` still say 0.3.0; that staleness predates this unit. |
| 2 | crates/eventlog-file/src/state.rs:706 | CONFIRMED | introduced | `assert_eq!(replayed.events.by_stream, folded.events.by_stream)` compares a function with itself: `State::replay` is just `fold` in a loop (state.rs:248-256), so this assertion cannot fail. The real incremental path is a cached view built during transactions, with another handle's frames folded onto it (lib.rs:997-1000), and the unit's cases never run it. | Only the test suite. The per-frame `regrouped` check at :699 is still a real assertion, and my two-handle case now covers the real path (green). |

**5. Attacked and could not break**
- **Index drift:** I found no drift after appends, groups (including one repeating a stream), retries, redactions, erasure followed by re-append, reopening, or one handle resuming onto another handle's frames. All 8 checks plus the final one agreed with the feed on all three handles.
- **Stream keys:** tenants with the same type and id stay separate, and the `a","b`/`c` vs `a`/`b","c` names did not collide. The key is a JSON array, so different names always give different keys.
- **Read order:** within one stream, version order matches global order, because `apply` enforces the next version and an increasing position. So the switch from a scan to the index cannot reorder results.
- **Window edges:** `after` beyond the head and `u64::MAX`, and `limit` 0 and `usize::MAX`, all give the same answers as before. `Excluded(u64::MAX)` with no upper bound does not panic.
- **Retry receipts:** results stop at the end of their range, through both commands and groups.
- **Capture, inspection and inline_admin:** they build `State` by replay and only read it through `values()`/`into_values()`, so they are unaffected.
- **providers.md sentence:** true as written. It only claims what the lookup itself costs. The cost of re-reading the prefix on each call is already stated in `operations.md:21` and `docs/design/file-provider.md:89-93`.

**6. Paths written outside the worktree**

All in `~/.cache/eventlog-el42/index/adversary/`:
- `red-docs.log`
- `index-cases.log`
- `suite.log`
- `suite-nff.log`
- `suite-final.log`
- `clippy.log`
- `clippy-final.log`
- `fmt.log`
- `adversary_stream_index.rs.before-tally`

The tree's own `target/` gained the two new test binaries.

**7. Findings block**

```findings
- file: README.md
  line: 17
  category: contract-drift
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: README now says the release highlights summarize the 0.8.0 notes it links, but website/docs/releases.md ends at 0.3.0; tests/adversary_stream_index_docs.rs is red on it
- file: crates/eventlog-file/src/state.rs
  line: 706
  category: judgement
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: replayed-vs-folded index assertion compares State::replay with State::fold in a loop, the same code, so it cannot fail, and the real cached-view resume path stays unexercised by the unit's cases
```

