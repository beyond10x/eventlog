---
format: aep.planning-md/2
id: story:grouped-blob-writes-take-one-durability-barrier
kind: story
status: implemented
title: Blobs written inside one append group take one durability barrier, not one fsync each
relations:
- serves: vision:O2
revision: 4
---
## Outcome

Writing many blobs inside one atomic append group costs one durability barrier for the group, not
one `fsync` per blob. The provider's durability guarantee is unchanged: nothing in the group is
observable before the barrier, and a crash before it leaves none of the group behind.

## Why

Measured 2026-09-21 by unit 9 of wave-validate-v2-20260920 (AEP), on a copy of the real ESS
planning store through the recorded Eventlog store (ER 8569da2, provider 76aad5e):

| quantity | value |
| --- | --- |
| imports in one ESS migration `apply` | 7,810 (3,222 evidence blobs among them) |
| `fsync` calls in a 5.16 s instrumented run | 1,255, **3.16 s of wall time (61 %)** under `strace -c -w` |
| the same under `strace -c` | 18 ms — CPU time only; `fsync` burns none, which misled a first reading |
| ESS `apply` after one capture per batch (ER story) | ≈ 6.5 min, most of it durability |
| plus one barrier per group (this story) | ≈ 60 s projected |

`put_blob` (`crates/eventlog-file`) syncs each blob as it lands. Inside an append group (ADR 05,
`docs/design/file-provider.md`) the group is what becomes durable, so per-blob barriers buy nothing
the group's own barrier does not.

## Acceptance

- Red first: a counting case (the crate's test hooks) that writes N blobs in one group and asserts
  one barrier for the group, not N; fails at 76aad5e.
- The crash-consistency cases for groups stay green: a group interrupted before its barrier leaves
  no blob or frame behind; after the barrier, all of it is present; the existing conformance suite
  under `crates/eventlog-conformance/` is not edited.
- Byte identity: blob keys and contents are unchanged; `verify` on a store written both ways is
  byte-identical.
- Decisions after pass 2 (2026-09-21, coordinator): the port method
  `AtomicEventStore::append_group_guarded_with_blobs` **fails closed** by default — an inheriting
  provider refuses and publishes nothing; only a provider that implements admission-before-bind
  overrides it (the file provider does). Dedup identity is decided against the digest set the group
  **committed**, never against live blob presence; a committed digest whose blob was later deleted
  still deduplicates. `verify_bound` runs after admission, inside the lock. The `object_syncs`
  charge lives in the function that performs the sync. The module's limit paragraph names every
  claim no case exercises (dropped fsync, mid-batch failure, disposal masking).
- Exception to "the conformance suite is not edited": `eventlog-conformance` gains cases for the
  new port method (refusal binds nothing; dedup by committed digests; a non-implementing provider
  refuses with the fail-closed code), run against all three providers. Nothing existing in that
  crate is changed.
- `docs/design/file-provider.md` states where the barrier is for grouped writes.
- `scripts/gate.sh` under the CI fixture conditions green (evidence/eventlog-tls-gate.sh);
  then Eventlog → ER → AEP re-pin.
- Related, measured afterwards on the ESS copy: if `apply` still misses 10 s, the remaining share is
  named (story:file-capture-blob-digests-without-rehashing is the expected candidate) with the number.
