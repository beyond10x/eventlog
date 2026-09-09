---
format: aep.planning-md/1
id: story:projections-inline-and-catch-up
kind: story
status: implemented
title: Projections, inline and catch-up
tags:
- migrated-legacy
relations:
- depends_on: story:the-log-and-its-two-backends
revision: 4
---

# EL-003 — Projections, inline and catch-up

## Intent

Every query in every module becomes a projection, so this is the crate surface six modules will
live on. It has to make writing a read model dialect-free, and it has to make the read-your-writes
decision explicit rather than accidental.

## Acceptance

- One `Projector` trait, two runners. Inline runs in the append transaction; catch-up runs from a
  durable position under `pg_try_advisory_lock` on its own name, so a second replica is safe by
  construction rather than by configuration.
- A projection is declared inline or catch-up, never both. Apply is an upsert, so at-least-once
  delivery is safe.
- The projection storage primitive takes key columns, an indexed-field declaration and a JSON body,
  and compiles to both dialects. A projection author writes no SQL; a module may opt into
  hand-written dialect SQL and must declare that it has.
- A guard — an invariant spanning streams — reads an inline projection `FOR UPDATE` inside the
  append transaction. The kit refuses to start when a guard names a projection registered as
  catch-up.
- Any projection can be dropped and rebuilt from the log, and a test does exactly that and compares
  the result byte for byte.

## Migration record

This is completed legacy work, not a new implementation request. The source declares `status: done` at line 4. Its existing contracts are exercised by the 73-case production gate at main `734047203ce112b21ad5b9e3ea67fdacb8835def`, recorded in verification-report:repository-hygiene-and-code-review-20260909. New review findings have separate follow-up stories.

## Provenance

Migrated from `docs/stories/EL-003-projections-inline-and-catch-up.md`.

- First written 2026-08-22T00:30:40+02:00; last touched 2026-08-22T00:30:40+02:00; 2 revisions.
- Source status quoted: `status: done` (line 4).
- Source text retained; this artifact is the governed successor.
