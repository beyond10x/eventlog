---
id: EL-002
title: Aggregates, repository and snapshots
status: done
depends_on: [EL-001]
---

# EL-002 — Aggregates, repository and snapshots

## Intent

Turn the log into something a domain author writes against: decide, apply, fold, snapshot.

## Acceptance

- `Aggregate` declares `TYPE`, `empty`, `apply`, and `decide`. `decide` returns events or a domain
  error and performs no I/O.
- `Repository::handle` loads (snapshot plus tail), decides, appends at `Expected::Exact(head)`,
  retries exactly once on conflict, and refuses after that rather than looping.
- A snapshot carries a `state_schema_version`. One that fails to deserialise is discarded and the
  fold restarts from zero, never trusted and never repaired in place.
- A property test proves `fold(all events) == fold(snapshot at v, events after v)` for every prefix
  of a generated stream.
- Snapshot cadence is a policy the caller sets, defaulting to every 100 events and on demand.
