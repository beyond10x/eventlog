---
format: aep.planning-md/3
id: story:file-eventlog-indexes-streams
kind: story
status: implemented
title: A file Eventlog finds a stream's events without scanning the store
summary: per-stream index for State::head, slice and the per-stream lookups
owner: eventlog
refs:
- provider: github
  reference: beyond10x/eventlog#42
relations:
- serves: vision:O2
- informed_by: story:file-eventlog-verifies-once-per-open
scope:
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: README.md
- confidence: cited
  path: crates/eventlog-file/src/lib.rs
- confidence: cited
  path: crates/eventlog-file/src/state.rs
- confidence: cited
  path: website/docs/providers.md
revision: 18
transitions:
- {from: "draft", to: "proposed", at: "2026-10-07T19:49:26Z", actor: "human:timo", revision: 14}
- {from: "proposed", to: "active", at: "2026-10-07T19:49:26Z", actor: "human:timo", revision: 15}
- {from: "active", to: "implemented", at: "2026-10-07T23:50:41Z", actor: "human:timo", revision: 18, decided_on: {"recorded":{"test_result":1,"review_outcome":2,"verification":1}}}
---
## Outcome

A file Eventlog answers `stream_version`, `read_stream` and the per-stream lookups inside a
transaction from the events of the stream asked about, not by visiting every event the store
holds. A store's per-call cost no longer grows with the events of other streams.

## Why

https://github.com/beyond10x/eventlog/issues/42 reports a downstream CLI whose `events.jsonl`
grew to 5,191 events (7.7 MB) and one full read of one entity type took 3.0 s in a release build
(numbers from the issue, not reproduced here). Two costs grow with the store. The resumed-handle
re-hash is story:incremental-history-digest-for-a-resumed-handle. This story is the other one,
read from the source at `de30462b`:

- `State::head` (`crates/eventlog-file/src/state.rs:155-162`) filters every value of
  `State::events` with `matches_stream` and takes the largest version;
- `slice` (`crates/eventlog-file/src/lib.rs:1273-1290`), which answers `read_stream`, filters every
  event the same way;
- `lib.rs:487` and `lib.rs:1611` scan `State::events` for one stream's events.

`State::events` is a `BTreeMap<u64, RecordedEvent>` keyed by global position
(`state.rs:105`); nothing indexes it by stream.

## Acceptance

- `State` keeps an index from stream to that stream's events, maintained by the same `apply` path
  that writes `State::events`, including every path that changes or removes an event (redaction,
  erasure), so a replayed state and an incrementally folded state hold the same index.
- `State::head`, `slice` and the lookups at `lib.rs:487` and `lib.rs:1611` read the index and no
  longer call `matches_stream` over `State::events`.
- Red first: a test counts events visited per `stream_version` and per `read_stream` window on a
  store holding many streams (the `#[cfg(test)]` cost module, `crates/eventlog-file/src/cost.rs`,
  or an equivalent counter), fails at `de30462b` with a count proportional to the store, and passes
  with a count bounded by the stream's events plus the window. The mutation that restores the scan
  turns it red (AGENTS.md invariant 5).
- Every existing `eventlog-file` test and the shared conformance exercise pass unchanged
  (AGENTS.md invariant 4); the on-disk format `eventlog-file/1` and its canonical bytes do not
  change.
- `README.md` states the current release, 0.8.0, where it says 0.3.0 today (lines 16, 18, 22),
  and `website/docs/providers.md` § File states what a stream read and a head lookup cost.

## Scope

Derived 2026-10-07 by `story-scoper` at `de30462b`. Every line is **cited** (read from the story
or the tree) or **inferred** (a reading that could be wrong).

- **Primary surface:** `crates/eventlog-file`, `State` and its readers — cited
- **Files:** `crates/eventlog-file/src/state.rs` — `State` struct (103-118) for the index field;
  `State::head` (155-162); `apply` `Op::Event` arm (185-194), which calls `head` at 188 and is the
  only insertion into `State::events` at 193 — cited
- **Files:** `crates/eventlog-file/src/lib.rs` — `Transaction::result` (478-495, filter 486-490);
  `slice` (1272-1288); the find in `redact` (1611, inside 1602-1628) — cited
- **Files:** `crates/eventlog-file/src/cost.rs` — `Cost` and its `Sub` impl (18-75), if the counter
  goes there — cited (the story allows an equivalent counter)
- **Symbols:** `State::events`, `State::head`, `State::apply`, `slice`, `Transaction::result`,
  `matches_stream` (`state.rs:291-295`) — cited
- **No edit needed:** `State::replay`/`fold` (`state.rs:131-154`), `stream_version`
  (`lib.rs:1439-1450`), `head` callers at `lib.rs:534` and `1324` — cited
- **Not touched:** `journal.rs`, `capture.rs`, `inspection.rs`, `inline_admin.rs`; their scans are
  tenant-wide, not per stream (`capture.rs:373-377`, `388-392`; `inspection.rs:58-62`;
  `inline_admin.rs:162-168`) — cited
- **Also likely:** `lib.rs:24` import of `matches_stream` becomes unused; a `#[cfg(test)]` module in
  `state.rs` — inferred
- **Documents:** `README.md` lines 16, 18, 22 (0.3.0 → 0.8.0); `website/docs/providers.md` § File
  (20-29) — cited. `CHANGELOG.md` `[Unreleased]`; `docs/design/file-provider.md:55` — inferred
- **Confidence:** high — the story names every per-stream scan site and `git grep` found no other
- **Would collide with:** `State`/`apply` in `state.rs`; `lib.rs` around `result`, `slice`, `redact`;
  `Cost` in `cost.rs`; `CHANGELOG.md` `[Unreleased]`
- **Safety fact:** `State::events` is inserted only at `state.rs:193` and never removed from;
  redaction (`lib.rs:1613-1620`) and erasure (`lib.rs:1636-1642`) rewrite the journal operations
  and replay, so an index maintained in `apply` is identical for replayed and folded states. `State`
  is not serialized (`state.rs:103`), so `eventlog-file/1` bytes cannot change — level 2, unproven
- **Not established:** `apply` calls `head` once per event (`state.rs:188`), so a full replay
  appears quadratic in event count today and the index removes that too — inferred, not measured.
  Tenant-wide scans (`read_feed` `lib.rs:1460-1467`, `catch_up` 936-943, `rebuild_projection`
  1753-1759) also grow with the store and are outside this story.

- **Coordinator correction, 2026-10-07:** the counter lives in `state.rs` (not `cost.rs`), and `CHANGELOG.md` and `docs/design/file-provider.md` belong to story:file-eventlog-rechecks-its-prefix-by-comparison in this wave; the entries were removed so the wave can be derived. `lib.rs` stays shared, split by symbol: this story owns `Transaction::result` (478-495), `slice` (1272-1288), the find in `redact` (1602-1628) and the import at line 24.

- **Implementor confirmation, 2026-10-07 (unit `d01992d0`):** every inferred line checked. The
  `matches_stream` import and function are removed (no other users); the order check in `apply`
  made a full replay quadratic, measured 12,720 = N(N-1)/2 visits at N=160, now 0; insertion is
  only through the new `Events` type in `state.rs`, so the safety fact is now held by the compiler.
  `lib.rs` hunks landed at 21, 482, 1273 and 1608, inside the split. Correction 1 (`609ab0a4`)
  touched only `README.md` and test code in `state.rs`.
