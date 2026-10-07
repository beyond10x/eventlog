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
| integration | `wave/file-provider-cost` | `wave-file-cost` | `~/.cache/eventlog-el42/wave` | opened on `de30462b` |
| unit | `impl/file-eventlog-indexes-streams` | `el42-index` | `~/.cache/eventlog-el42/index` | planned |
| unit | `impl/file-eventlog-rechecks-its-prefix-by-comparison` | `el42-compare` | `~/.cache/eventlog-el42/compare` | planned |

Only the coordinator writes the planning store, in the integration tree.

## Commits this approval authorises

The opening commit (this page, the two new stories, the moves); the unit commits on each `impl/`
branch (implementation, review cases, review fixes); their merges into `wave/file-provider-cost`;
the closing store commit; the bot push of the integration branch and one pull request into `main`,
merged after CI. No tag and no release.

## Agents

`aep:implementor` per unit, then `aep:adversary` per green unit. The compare unit changes the
check that refuses a damaged journal, so it also gets `aep:security-reviewer`.

## Gate

`bash scripts/gate.sh --production-proof` once on the integration branch against a disposable
`postgres:17.6-alpine3.22` with the `production-fixture` TLS setup, per-step output under the
wave scratch root.

## Pre-flight

| check | read |
| --- | --- |
| primary checkout | clean, on `main` at `de30462b` |
| `git worktree list` | primary only |
| free disk on `/` | 13G before the first tree |
| one build | `cargo test -p eventlog-file --no-run`: 10 s, 945M; workspace `--no-run`: 23 s, 3.7G (discarded) |
| compiler cache | `sccache` present, not used: its cache is on the same disk |
