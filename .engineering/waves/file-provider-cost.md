# File provider cost wave

Skill: aep:implementing 0.21.2 (wave mode). Approved by the operator on 2026-10-07 as option A for
https://github.com/beyond10x/eventlog/issues/42: keep `eventlog-file` and fix its cost, then push
through the bot and open one pull request that says `Fixes #42`. Serves O2.

## Selection

N=2, both active:

| unit | story | scope |
| --- | --- | --- |
| index | story:file-eventlog-indexes-streams | cited, high |
| compare | story:file-eventlog-rechecks-its-prefix-by-comparison | cited for `journal.rs` and the design page; inferred for `capture.rs` |

`aep plan artifact waves --kind story --status draft` (run before the moves) is
[file-provider-cost-selection.json](file-provider-cost-selection.json). It places the two units in
separate waves on one remaining collision, `crates/eventlog-file/src/lib.rs` (cited). The units
run together with that file split by symbol (below); `cost.rs`, `CHANGELOG.md` and
`docs/design/file-provider.md` were removed from the index story's scope by the coordinator, which
the story's `## Scope` records. The eight unassessed draft stories were not scoped: the operator
named this wave's content.

story:incremental-history-digest-for-a-resumed-handle stays draft and outside the wave. Its scope
found that inside the 2 s stamp window no digest removes the prefix read without a new format; its
`## Finding: the window` section records why. story:file-eventlog-rechecks-its-prefix-by-comparison
takes the reachable part.

Baseline at `de30462b`, throwaway probe outside the repository (one event per append transaction,
1.4 KB payload, 40 streams, release): 2,000 events, 4,811,762 B, appends 54,422 ms (27.21 ms
each), read of every stream 1,342 ms inside the window and 26 ms after it. `perf` at 1,500 events:
96.80% `sha2::sha256::compress256`, 0.30% `State::head`.

## The `lib.rs` split

| unit | owns in `crates/eventlog-file/src/lib.rs` at `de30462b` |
| --- | --- |
| index | line 24 (`use state::…`); `Transaction::result` 478-495; `slice` 1272-1288; `redact` 1602-1628 |
| compare | `Verified` and its doc 55-71; `enter` 972-1052; `mod stamped_resume_cost` 2450-2600 |

Neither unit appends a module at the end of `lib.rs` other than inside `stamped_resume_cost`.
`CHANGELOG.md` `[Unreleased]` is compare's; the index unit hands its line to the coordinator, who
adds it at merge. A `git merge-tree --write-tree` dry run of both unit heads precedes the first
merge.

## Branches and trees

Build directories are each tree's own `target/`. Trees live under the worktree profile's
`~/.local/state/worktree/trees/b10x/eventlog/`.

| role | branch | managed id | scratch | stage |
| --- | --- | --- | --- | --- |
| integration | `wave/file-provider-cost` | `wave-file-cost` | `~/.cache/eventlog-el42/wave` | opening commit `32168648` on `de30462b`; both units merged (`8655c670`, `1ba2af2f`); closing store commit follows |
| unit | `impl/file-eventlog-indexes-streams` | `el42-index` | `~/.cache/eventlog-el42/index` | merged as `8655c670` (unit head `0ca549f4`); adversary pass 1: 2 introduced, both fixed in `609ab0a4` with red-then-green cases and mutation controls; pass 2 not run: the correction changed `README.md` and test code only |
| unit | `impl/file-eventlog-rechecks-its-prefix-by-comparison` | `el42-compare` | `~/.cache/eventlog-el42/compare` | merged as `1ba2af2f` (unit head `2aaac6b7`); acceptance bar corrected (story revision 12); adversary pass 1 (4 introduced, 1 pre-existing filed as story:file-capture-after-a-write-resumes), security pass 1 (invariant held; 4 introduced) and adversary pass 2 (invariant held; 3 introduced, 0 carried) all fixed; correction 2 read by the coordinator: no assertion dropped, the pass-2 case's precondition re-pinned to the fixed state and its assertion kept |

Only the coordinator writes the planning store, in the integration tree.

## Commits this approval authorises

The opening commit (this page, the two new stories, the moves); the unit commits on each `impl/`
branch (implementation, review cases, review fixes); their merges into `wave/file-provider-cost`;
the closing store commit; the bot push of the integration branch and one pull request into `main`,
merged after CI. No tag and no release.

## Agents

`aep:implementor` per unit, then `aep:adversary` per green unit. The compare unit changes the
check that refuses a damaged journal, so it also got `aep:security-reviewer`.

## Gate

Package-scoped on the merged integration tree `1ba2af2f` (operator, 2026-10-07: no full local gate;
the pull request's CI runs the full gate and the persistence proof):

| step | exit | output |
| --- | --- | --- |
| `cargo test --locked -p eventlog-file --no-fail-fast -- --test-threads=1` | 0 | 30 lanes, 204 passed, 0 failed, 0 ignored (171 at base + 9 index + 24 compare) |
| `cargo clippy --locked -p eventlog-file --all-targets -- -D warnings` | 0 | |
| `cargo fmt --all --check` | 0 | |
| `cargo check --locked --workspace --all-targets` | 0 | |

## Verification

| unit | claim | base `32168648` | unit | verdict |
| --- | --- | --- | --- | --- |
| index | a head lookup visits no event, a window its events plus one | 160 of 160 visited; receipt 162 | 0; 2 | VERIFIED (counter test red at base) |
| compare | user CPU, 2,000 events | 20.77 / 20.56 s | 0.95 / 1.27 s | VERIFIED |
| compare | read of every stream inside the window, 2,000 events | 1,382 / 1,425 ms | 56 / 172 ms | VERIFIED |
| compare | per append, 200 → 2,000 events | ×1.37 | ×0.82 | VERIFIED |

Throwaway probe outside the repository, alternating base and unit, 2 rounds, load average 7-13.

## Cost

| agent | tokens | tool uses | wall |
| --- | --- | --- | --- |
| `aep:story-scoper` index | 89,389 | 24 | 2.5 min |
| `aep:story-scoper` digest | 122,610 | 39 | 4.9 min |
| `aep:implementor` index | 212,326 | 132 | 37 min |
| `aep:adversary` index pass 1 | 166,002 | 74 | 19 min |
| `aep:implementor` index correction 1 | 272,136 | 24 | 7.5 min |
| `aep:implementor` compare | 249,330 | 127 | 38 min |
| `aep:adversary` compare pass 1 | 222,527 | 59 | 20 min |
| `aep:security-reviewer` compare pass 1 | 234,109 | 75 | 25 min |
| `aep:implementor` compare correction 1 | 385,610 | 88 | 23 min |
| `aep:adversary` compare pass 2 (after one HTTP 429; the first attempt's usage was not reported) | 222,223 | 20 | 3.1 min |
| `aep:implementor` compare correction 2 | 474,886 | 58 | 39 min |

Token figures for a resumed agent are as the harness reported them at each completion.

## Pre-flight
| check | read |
| --- | --- |
| primary checkout | clean, on `main` at `de30462b` |
| `git worktree list` | primary only |
| free disk on `/` | 13G before the first tree |
| one build | `cargo test -p eventlog-file --no-run`: 10 s, 945M; workspace `--no-run`: 23 s, 3.7G (discarded) |
| compiler cache | `sccache` present, not used: its cache is on the same disk |

## Merge dry run

`git merge-tree --write-tree --merge-base=32168648` of `d01992d0` and `9920a7db`: exit 0, tree
`601e8816`. No call in that tree uses a `State::events` method the index unit removed.

