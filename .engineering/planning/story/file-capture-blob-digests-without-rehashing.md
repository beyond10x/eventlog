---
format: aep.planning-md/2
id: story:file-capture-blob-digests-without-rehashing
kind: story
status: implemented
title: A repeated capture does not re-hash blob content it has verified, or the design page says why it must
relations:
- depends_on: story:file-capture-reuses-the-verified-view
- serves: vision:O2
scope:
- confidence: cited
  path: crates/eventlog-file/src/capture.rs
- confidence: cited
  path: docs/design/file-provider.md
revision: 7
---
## Outcome

A repeated capture on one handle does not re-hash blob content it has already verified, or the
repository records why it must. Either way the decision is written where the next person meets it:
in `docs/design/file-provider.md` and in the counting case that pins it.

## Why

Split from story:file-capture-reuses-the-verified-view on 2026-09-21. That unit made the journal
half of a capture resume from the handle's verified view; the blob half was left as it was, by
decision, and this story holds the decision and its cost.

Measured by the unit 6 implementor on a copy of the real AEP planning store (`events.jsonl`
3,273,126 bytes, 1,113 objects, 11.38 MB, 20 captures, release build, fresh copy per run):

| | before (db608cd) | after (unit 6) |
| --- | --- | --- |
| repeated capture, wall | 140 ms | 57 ms |
| of which blob content hashing | | 43 ms |
| journal frames chained / re-encoded / folded, 10 captures | 170 / 170 / 170 | 17 / 17 / 17 |
| blobs hashed, 10 captures | 10 × blobs | 10 × blobs |

The argument for keeping the hash, as the implementor put it: a capture hands blob bytes out, and
this crate hashes bytes it hands out (`Transaction::blob`; `docs/design/file-provider.md:86`). Not
re-hashing means not re-reading, and not re-reading means holding the store's whole content per
handle: 11.4 MB here, unbounded in general, in a kit every owner may depend on.

The CLI cost this leaves on the table: `aep plan artifact list` on that copy performs about 222
captures (strace, evidence/pin-cost-ab-20260921.md), so roughly 222 × 43 ms ≈ 9.5 s of software
SHA-256 per `list` on a CPU without SHA-NI, out of the post-unit-6 total still to be measured.

## Measured at the CLI, 2026-09-21 (wave sub-operator, evidence/profile-list-reading-20260921.md)

One `aep plan artifact list` with the unit 6 binary (provider 76aad5e) on the migrated real-store
copy, 64 artifacts, 222 captures:

| quantity | value |
| --- | --- |
| blob objects on disk | 1,113, 11,384,593 bytes (same as the unit 6 fixture) |
| `read(2)` calls / bytes per `list` | 503,053 / 3,245,699,133 |
| of which blob content (222 × 11.38 MB) | 2,527,379,646 bytes |
| of which journal prefix re-hash (222 × 3.27 MB) | 726,633,972 bytes, 22 % |
| SHA-256 share of wall time (perf) | 51.17 % of 77.9 s = 39.9 s |
| implied hash rate | 81 MB/s (software SHA-256, i9-10900K, no SHA-NI) |

The prefix re-hash is unit 6's own cost and is what story:incremental-history-digest-for-a-resumed-handle
removes. Unresolved and labelled so: unit 6 measured 43 ms of content hashing per capture on its
fixture (265 MB/s); the CLI shows 140 ms per capture at 81 MB/s. Either the 43 ms was a differential
rather than a direct measure, or the fixture bound fewer objects per tenant. Nobody should plan work
on either number until that is closed.

## Options, for the decision

1. Keep hashing every blob on every capture. Cost as above; integrity of handed-out bytes is proven
   on every read. This is the state after unit 6.
2. A bounded digest cache keyed by file identity (path, size, inode, mtime), holding one 32-byte
   digest per blob, not content. Re-hash only when the identity changed. Weaker: a blob damaged in
   place with identity preserved would be served unverified once. Has to be argued against the
   provider's integrity contract before it is built.
3. Hash on the SHA-NI path where the CPU has it, and accept the cost where it does not. Changes
   nothing on this workstation (i9-10900K, no SHA-NI).

## Acceptance

- One of the options above is chosen and recorded in `docs/design/file-provider.md`, with this
  table beside it.
- If option 2: red first, a case in which a blob is rewritten in place with identity preserved and
  the capture that follows either detects it or the design page says in which cases it does not;
  the counting case's `blobs_hashed` assertion is changed in the same commit that changes the
  behaviour, and nowhere else.
- If option 1 or 3: the counting case stays as it is and the design page says why.
- Any number quoted in the CHANGELOG is measured after the change.
