---
format: aep.planning-md/1
id: story:postgres-feed-reads-drain-under-a-held-watermark
kind: story
status: draft
title: Postgres tests that read the feed once fail under a held commit watermark
summary: atomic_group.rs:598 red 3/3 at base under load; e603605 drains 2 sites; 10 candidates, 1 non-member, :551 zero-branch hazard open
owner: eventlog
revision: 1
---
## Outcome

Every postgres test that asserts on the result of a single `read_feed` either drains with a bounded
retry (`eventlog_conformance::drain_at_least` or the `feed_at_least` helper) or is shown to be
immune to a withheld feed. The case at `crates/eventlog-postgres/src/atomic_group.rs:551` proves
that the crash point committed nothing with a positive check, not with a zero-length feed read.

## Why

2026-09-22, unit 12 of wave-validate-v2-20260920. The gate on the unit's branch went red on
`native_group_crash::every_native_group_boundary_recovers_one_complete_outcome`
(`crates/eventlog-postgres/src/atomic_group.rs:598`). The unit proved the red pre-existing: base
content restored with `git show HEAD:<path>`, red 3 of 3; `--exact` alone green 3 of 3;
`--test-threads=1` on the lane 18 of 18 green. The same case passed on the integration head
7fbd37cf about an hour earlier under lower machine load.

Mechanism, as diagnosed by the unit: the commit watermark is cluster-wide. A sibling case holding a
transaction withholds the feed. A case that reads `read_feed` once and asserts on the result reads
a feed that is withheld, not one that is empty. `eventlog_conformance::drain_at_least` exists for
this.

Commit e603605 on the unit-12 branch applies the bounded drain to the two confirmed members:
`atomic_group.rs:598` and `atomic_group.rs:551`. Nothing else was patched.

## Candidates, not confirmed

Sites that read the feed once and assert on it. Each is to be checked, not patched by pattern:

| file | lines |
| --- | --- |
| `crates/eventlog-postgres/tests/inline_admin.rs` | 398 |
| `crates/eventlog-postgres/tests/consistent_capture.rs` | 228 |
| `crates/eventlog-postgres/tests/conformance.rs` | 839, 847, 1238, 1250, 1281, 1661, 1676, 2513 |

Deliberate non-member: `crates/eventlog-postgres/tests/adversary.rs:109-113`. It exists to prove
the withholding and must keep asking exactly once. Do not add a drain there.

## The weaker hazard e603605 does not close

At `atomic_group.rs:551` the same assertion sometimes expects zero events. A withheld feed reads as
zero, so that branch passes for the wrong reason whenever the watermark is held. A retry does not
close this. Closing it needs a positive check that the crash point committed nothing.

## Review pass 1 on the unit-12 branch, 2026-09-22 (security-reviewer, at e603605)

Two findings on e603605, both left standing for this story rather than patched in the wave:

- `atomic_group.rs:575`: for six of the seven crash points `committed == false`, so `expected == 0`
  and `len() >= 0` holds on the first read; the retry returns at once. For those six the assertion
  is the un-drained single read the patch replaced. The hazard is stated nowhere in the source.
- `atomic_group.rs:491`: `feed_at_least` returns on `>=`, which turns the exact-count assertion at
  `:576` into a lower bound. It cannot mask a loss; it can admit a partially visible duplicate.

## Acceptance

- Each candidate line above is either converted to a bounded drain or annotated in this story's
  body with the reason it cannot see a withheld feed.
- `atomic_group.rs:551` asserts the zero-event branch with a positive check that the crash point
  committed nothing, for all seven crash points.
- The assertion at `atomic_group.rs:576` is exact again, or the reason a lower bound is correct is
  written in the test.
- `adversary.rs:109-113` still reads exactly once.
- The postgres lane of `evidence/eventlog-tls-gate.sh` runs green 5 of 5 under a concurrent full
  gate on the same machine, or the run count and the load are recorded with the result.
