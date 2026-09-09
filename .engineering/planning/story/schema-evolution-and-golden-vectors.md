---
format: aep.planning-md/1
id: story:schema-evolution-and-golden-vectors
kind: story
status: implemented
title: Schema evolution and golden vectors
tags:
- migrated-legacy
relations:
- depends_on: story:aggregates-repository-and-snapshots
revision: 4
---

# EL-004 — Schema evolution and golden vectors

## Intent

Once events are the record, an event type is permanent. This is the machinery that makes that
survivable, and the gate that catches the day somebody forgets.

## Acceptance

- An upcaster is a pure function from one `event_schema_version` to the next, registered per event
  name, run on read, and never deleted.
- A module commits golden vectors: the exact stored bytes of every event version it has ever
  written. The gate folds each one through the current aggregate and fails if any does not.
- Adding a required field to an existing `event_schema_version` fails the gate. Additive-optional
  passes.
- `eventlog-testkit` exposes the vector runner so a module writes a test rather than a harness.

## Migration record

This is completed legacy work, not a new implementation request. The source declares `status: done` at line 4. Its existing contracts are exercised by the 73-case production gate at main `734047203ce112b21ad5b9e3ea67fdacb8835def`, recorded in verification-report:repository-hygiene-and-code-review-20260909. New review findings have separate follow-up stories.

## Provenance

Migrated from `docs/stories/EL-004-schema-evolution-and-golden-vectors.md`.

- First written 2026-08-22T00:30:40+02:00; last touched 2026-08-22T00:30:40+02:00; 1 revisions.
- Source status quoted: `status: done` (line 4).
- Source text retained; this artifact is the governed successor.
