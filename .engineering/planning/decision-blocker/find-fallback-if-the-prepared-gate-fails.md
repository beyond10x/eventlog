---
format: aep.planning-md/3
id: decision-blocker:find-fallback-if-the-prepared-gate-fails
kind: decision-blocker
status: open
title: 'Which fallback if the prepared-find gate G1 fails: client digest, C-collated row_key, or both'
relations:
- blocks: story:projection-find-holds-its-bound-with-a-prepared-statement
withholds: metric_observation
revision: 1
---
## Question

If the build gate G1 fails (the prepared statement under `force_custom_plan` is more than 1.03x of
the unprepared 0.10.0 shape in the spike, CI run 45368249583 on branch `ci/plan-prepared-spike`),
which way forward does the owner take for `postgres.projection_find`? Design
`docs/design/projection-find-planning.md` section 0.5 and decision D5. The bound (p50 within 1.05x
of 0.10.0) is not touched.

- FA: a digest computed by the client and sent as a `bigint` parameter. Removes about 3.1 us of
  planning; needs a Rust function equal to the server's `hashtextextended(text, 0)` for every input
  and major, an admission that refuses a non-UTF-8 database, and a probe against drift.
- FB: drop the page index `key_c` by declaring `row_key` with `COLLATE "C"`. Removes about 4.0 us of
  planning and one btree per table; changes the order of `projection_find` on a non-C database, the
  edition and the ledger checksum, and the admission shape comparison.
- FA + FB: about 7.1 us SQL-only; can reach the 1.05 bound, not the 1.03 margin.

No default: architecture and on-disk questions are not covered by a default that proceeds.

## Condition

This blocker only bites if G1 fails. While the spike is open it parks nothing that the story can do
first (reading the spike, the KPI cases and guards of design 5.4 that do not depend on a prepared
statement). It is cleared by the owner's answer.
