---
format: aep.planning-md/3
id: decision-blocker:on-disk-layout-changes
kind: decision-blocker
status: cleared
title: Two stories need a new on-disk layout
relations:
- blocks: story:incremental-history-digest-for-a-resumed-handle
- blocks: story:tree-store-checks-out-on-windows
revision: 3
transitions:
- {from: "open", to: "cleared", at: "2026-10-14T07:24:21Z", actor: "human:timo", revision: 3}
---
## Question

Two draft stories reach their acceptance only through an on-disk layout change that a released
reader does not understand:

- story:incremental-history-digest-for-a-resumed-handle: its own "Finding: the window" says the
  acceptance (no whole-prefix read, no added trust) is reachable only with a layout whose sealed
  parts age past the 2 s window, such as a segmented journal; `Manifest`, `Frame` and the intents
  are `deny_unknown_fields` and `format` must equal `eventlog-file/1`, so a persisted digest is a
  new file format.
- story:tree-store-checks-out-on-windows: shortening `stream_dir` (`crates/eventlog-tree/src/layout.rs`)
  writes paths that Eventlog 0.8.4 and earlier do not read, so a store written by the new layout is
  unreadable to a consumer still pinned to an older release.

## Options

- A: a new layout version for each, written only on explicit opt-in, the old one read forever;
  a minor release. Cost: two layouts held by the conformance suites.
- B: a migration that moves existing stores and refuses the old layout by name; a minor release
  that every consumer takes at once. Cost: a coordinated upgrade of every owner.
- C: keep both stories in draft. Cost: resume stays O(store bytes) inside the window, and Windows
  consumers keep `core.longpaths`.

## Recommendation

A: no released reader breaks, and each consumer moves when it opts in.

## Decided (2026-10-14)

Option A. Both stories may introduce a new on-disk layout version. A store keeps its current
layout unless its owner opts in; every release keeps reading the old layout; conformance holds both
layouts. No migration and no refusal of the old layout. The release that ships either is a minor
release.
