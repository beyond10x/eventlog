---
format: aep.planning-md/3
id: decision-blocker:find-fallback-if-the-prepared-gate-fails
kind: decision-blocker
status: open
title: 'Which fallback if the prepared-find gate G1 fails: client digest, C-collated row_key, or both'
relations:
- blocks: story:projection-find-holds-its-bound-with-a-prepared-statement
withholds: metric_observation
revision: 2
---
## Question

If the build gate G1 fails (the prepared statement under `force_custom_plan` is more than 1.03x of
the unprepared 0.10.0 shape, confirmed by the 4 dispatched runs of scratch branch V1 that the
product story names; the pgbench-level spike, CI run 45368249583 on branch
`ci/plan-prepared-spike`, met it at 0.895), which way forward does the owner take for
`postgres.projection_find`? Design `docs/design/projection-find-planning.md` section 0.5 and
decision D5. The bound (p50 within 1.05x of 0.10.0) is not touched.

- FA: a digest computed by the client and sent as a `bigint` parameter. Removes about 3.1 us of
  planning; needs a Rust function equal to the server's `hashtextextended(text, 0)` for every input
  and major, an admission that refuses a non-UTF-8 database, and a probe against drift.
- FB: drop the page index `key_c` by declaring `row_key` with `COLLATE "C"`. Removes about 4.0 us of
  planning and one btree per table; changes the order of `projection_find` on a non-C database, the
  edition and the ledger checksum, and the admission shape comparison.
- FA + FB: about 7.1 us SQL-only; can reach the 1.05 bound, not the 1.03 margin.

No default: architecture and on-disk questions are not covered by a default that proceeds.

## Condition

This blocker only bites if G1 fails. G1 is provisional (the pgbench-level spike passed), so this
blocker stays open and is not cleared: it is cleared by the owner's answer.

The store has no conditional `blocks` edge, so the edge is unconditional and the condition is
stated here. The edge goes to the product story,
`story:projection-find-holds-its-bound-with-a-prepared-statement`, only. The measurement kit,
`story:projection-find-measurement-kit`, is valid whether G1 passes or fails and is not blocked:
it can be built, recorded and merged while this blocker is open. If G1 passes, the question is
moot and the owner clears this blocker so that the product story can move.
