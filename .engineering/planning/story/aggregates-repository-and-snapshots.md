---
format: aep.planning-md/1
id: story:aggregates-repository-and-snapshots
kind: story
status: implemented
title: Aggregates, repository and snapshots
tags:
- migrated-legacy
relations:
- depends_on: story:the-log-and-its-two-backends
revision: 4
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

## Migration record

This is completed legacy work, not a new implementation request. The source declares `status: done` at line 4. Its existing contracts are exercised by the 73-case production gate at main `734047203ce112b21ad5b9e3ea67fdacb8835def`, recorded in verification-report:repository-hygiene-and-code-review-20260909. New review findings have separate follow-up stories.

## Provenance

Migrated from `docs/stories/EL-002-aggregates-repository-and-snapshots.md`.

- First written 2026-08-22T00:30:40+02:00; last touched 2026-08-22T00:30:40+02:00; 1 revisions.
- Source status quoted: `status: done` (line 4).
- Source text retained; this artifact is the governed successor.
