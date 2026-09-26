---
format: aep.planning-md/2
id: story:tree-store-checks-out-on-windows
kind: story
status: draft
title: A tree store checks out on Windows without long paths
relations:
- serves: vision:O2
revision: 1
---
# A tree store checks out on Windows without long paths

## Outcome

A repository whose planning store is an `eventlog-tree` can be a Cargo git dependency of a Windows build
with Git's default path limit. Every path the layout writes stays short enough that
the runner's `<CARGO_HOME>/git/checkouts/<name>-<hash>/<rev>/` (76 characters for Eventlog on GitHub's Windows runner) plus the path is under 260 characters,
and a check refuses a layout change that breaks that.

## Why

On 2026-09-26 the Entity Runtime 0.24.0 Release run failed on `x86_64-pc-windows-msvc`. Cargo could not
check out Eventlog 0.5.0 (`fe8a0a7e`): `path too long:
'<CARGO_HOME>/git/checkouts/eventlog-3661f9771fa6366c/fe8a0a7/.engineering/state/tenants/planning/streams/er.subject/sha256%3A…'`.
The longest tracked path in Eventlog and in Entity Runtime is 198 characters:
`.engineering/state/tenants/planning/streams/er.subject/sha256%3A<64 hex>/<64 hex>.json`
(`crates/eventlog-tree/src/layout.rs:52`, `stream_dir`). The 76-character checkout prefix plus that path is 274.

Entity Runtime 0.24.1 works around it in its release workflow with `git config --global core.longpaths
true` on Windows. Every other Windows consumer of a repository carrying a tree store needs the same
setting until the layout is shorter.

## Acceptance

- The longest path the layout can produce for a stream event, with the percent-encoded `sha256:` stream
  id, is computed in a test and bounded so that the checkout prefix above plus it is under 260.
- Existing stores still read: either the layout keeps reading the old paths, or a migration moves them and
  the old layout is refused by name.
- Entity Runtime's `core.longpaths` step can be removed and its Windows release build still passes.
