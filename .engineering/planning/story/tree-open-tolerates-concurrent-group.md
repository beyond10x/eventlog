---
format: aep.planning-md/3
id: story:tree-open-tolerates-concurrent-group
kind: story
status: active
title: A tree store opens while another handle commits a group
relations:
- serves: vision:O2
revision: 3
transitions:
- {from: "draft", to: "proposed", at: "2026-10-13T02:42:14Z", actor: "human:timo", revision: 2}
- {from: "proposed", to: "active", at: "2026-10-13T02:42:14Z", actor: "human:timo", revision: 3}
---
# A tree store opens while another handle commits a group

## Outcome

Opening a tree store (`history::load`) while a second handle commits a group never reports
corruption. The open snapshots each tenant's group files first, then reads the events, then reads
only the groups in that snapshot. Events committed after the snapshot are torn, which an open
already tolerates. A group in the snapshot whose event is missing is still refused as corrupt.

## Why

A consumer's CI ran six concurrent writers against one tree store; the sixth open failed with
"tree store is corrupt: a group whose event is missing". The open listed every event file, then
every group file. A writer commits its events first and its group file last, so a commit landing
between the two passes left the reader with a group whose events were not in its event snapshot.

## Acceptance

- `a_group_committed_between_the_passes_is_absent_and_its_events_are_torn`: a test hook between
  the passes commits a group through a second handle; the open succeeds, the group is absent and
  its events are torn. Fails before the fix with the corruption error.
- `a_group_whose_event_is_missing_is_still_refused`: a group file in the snapshot whose event file
  is deleted is refused as corrupt.

## Scope

`crates/eventlog-tree/src/history.rs` (`load`). No public API or on-disk format change. Release
as a patch.

## Specification

The ESS specifications under `ess/` do not model the tree store's open or its torn-write
tolerance; no specification change.
