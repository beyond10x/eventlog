---
format: aep.planning-md/1
id: story:copy-linearizes-a-tree-into-sqlite
kind: story
status: implemented
title: A tree store copies into SQLite in position order, with every event's origin kept beside it
relations:
- serves: vision:O2
revision: 4
---
## Outcome

`eventlog_tree::copy` linearizes an `eventlog-tree` store into a SQLite store: groups in the tree's
position order, each stream gapless, and every event's original `{version, digest, parents}` kept
in an origin table beside the events, so redaction cannot erase it. A refused copy writes nothing.

Design: aep `docs/design/planning-on-entity-runtime-v0.1.md` § 9.6, unit V3 (`copy`; `verify` and
`repair` shipped in 0.4.0).

## Decisions

- A library function, not a CLI: this repository has no CLI crate.
- Origins live in `<prefix>_origins`, written in the event's own transaction through
  `RestoredEvent.origin`, and created on the first append that carries one, so an owner opened
  with `open_existing` gains it without that function creating tables.
- Everything the target's state can decide is checked before the first write: existing events, a
  different stream identity (read with the new non-minting `stored_stream_identity`), a blob
  digest bound to other bytes, restored identities already queued.
- A refused restored append puts back every restored identity it took.

## Not done

- Tenant capture does not carry origins (`TenantCapture` is a core type built in 14 places).
- Postgres: no origin table and no copy; the design defers it.

## Review

Adversary pass 1 returned NEEDS-CHANGE (an owner without the origin table, the restored queue on
refusal, a partial write on a late refusal); all three are fixed and pinned by its tests.
