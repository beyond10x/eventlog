---
format: aep.planning-md/3
id: decision-blocker:find-fallback-if-the-prepared-gate-fails
kind: decision-blocker
status: open
title: 'Which fallback if the prepared-find gate G1 fails: client digest, C-collated row_key, or both'
relations:
- blocks: story:projection-find-holds-its-bound-with-a-prepared-statement
withholds: metric_observation
revision: 3
---
## Question

If the build gate G1 fails (the prepared statement under `force_custom_plan` is more than 1.03x of
the unprepared 0.10.0 shape), which way forward does the owner take for
`postgres.projection_find`? Design `docs/design/projection-find-planning.md` section 0.5 and
decision D5. The bound (p50 within 1.05x of 0.10.0) is not touched. G1 is measured by variant V1,
acceptance line 14 of `story:projection-find-measurement-kit`: four dispatched 20-round runs,
`base` = the 0.10.0 tag, median of the `postgres.projection_find.latency` p50 ratio at most 1.03.
The pgbench-level spike (CI run 45368249583 on branch `ci/plan-prepared-spike`) met it at 0.895 and
is provisional.

- FA: a digest computed by the client and sent as a `bigint` parameter. Removes about 3.1 us of
  planning; needs a Rust function equal to the server's `hashtextextended(text, 0)` for every input
  and major, an admission that refuses a non-UTF-8 database, and a probe against drift.
- FB: drop the page index `key_c` by declaring `row_key` with `COLLATE "C"`. Removes about 4.0 us of
  planning and one btree per table; changes the order of `projection_find` on a non-C database, the
  edition and the ledger checksum, and the admission shape comparison.
- FA + FB: about 7.1 us SQL-only; can reach the 1.05 bound, not the 1.03 margin.

No default: architecture and on-disk questions are not covered by a default that proceeds.

## Condition

The store has no conditional `blocks` edge, so the edge is unconditional and the condition is
stated here. The edge goes to the product story,
`story:projection-find-holds-its-bound-with-a-prepared-statement`, only: that story must not start
before V1 passes, so the edge is right while V1 is open. The measurement kit,
`story:projection-find-measurement-kit`, is not blocked: it owns V1, and it can be built, recorded
and merged while this blocker is open.

- V1 passes: the question is moot. This blocker is cleared (`aep plan artifact move <this id> --to
  cleared`) as its own later step, after V1's line is met and recorded, so that the product story
  can move. It is not cleared now, while G1 is provisional.
- V1 fails: this blocker stays open, and it is the owner's decision D5; it is cleared by the
  owner's answer.
