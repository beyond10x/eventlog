---
id: EL-004
title: Schema evolution and golden vectors
status: done
depends_on: [EL-002]
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
