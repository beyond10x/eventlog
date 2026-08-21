---
id: EL-003
title: Projections, inline and catch-up
status: done
depends_on: [EL-001]
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
