---
format: aep.planning-md/1
id: story:file-capture-reuses-the-verified-view
kind: story
status: implemented
title: A file-provider capture reuses the handle's verified history instead of reverifying the journal
relations:
- depends_on: story:file-eventlog-verifies-once-per-open
- serves: vision:O2
scope:
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-file/src/capture.rs
- confidence: inferred
  path: crates/eventlog-file/src/journal.rs
- confidence: inferred
  path: crates/eventlog-file/src/lib.rs
- confidence: inferred
  path: docs/design/file-provider.md
revision: 10
---
## Outcome

A read through `FileEventStore::capture_tenant` on a handle that has already verified the committed
history pays only for what the file gained since, the same way a resumed transaction does after
story:file-eventlog-verifies-once-per-open. A CLI process that opens one handle and captures once
per subject no longer reverifies the whole journal and every blob per capture.

## Why

Measured 2026-09-21 by the wave sub-operator on one migrated copy of the AEP planning store
(`events.jsonl` 3,273,126 bytes, 1,116 files under `state/`), the two qualified `aep` binaries
interleaved:

| command | old binary (provider f802eb8) | new binary (provider db608cd) |
| --- | --- | --- |
| `aep plan artifact list`, pair 1 | 92.4 s | 93.5 s |
| `aep plan artifact list`, new alone, two runs | | 92.0 s, 91.5 s |
| `aep plan artifact history`, new alone, two runs | | 93.3 s, 94.3 s |
| `aep plan store verify` | | 2.9 s |

The verify-once change (c698923 → db608cd) applies to the transaction path only:
`FileEventStore::transaction` takes `runtime.verified` (`crates/eventlog-file/src/lib.rs:134-145`)
and `enter` resumes from it (`lib.rs:515-536`). The read path is separate:
`ConsistentTenantCapture for FileEventStore` (`crates/eventlog-file/src/capture.rs:71-88`) calls
`capture(&self.root, &mut runtime.observed, …)`, which opens the journal strictly
(`capture.rs:60-80`, `journal.rs:529 open_strict`) and replays every transaction. `capture.rs`
contains no reference to `Verified`, and `git log f802eb8..db608cd -- crates/eventlog-file/src/capture.rs`
is empty.

Every AEP CLI read reaches that path: `EventlogPlanningStore::load` and `ids`
(`aep-backend-eventlog/src/lib.rs:445-470`) → `RecordedEventlogBridge` → `EventlogRecordedStore::capture_model`
→ `capture_tenant` (`entity-eventlog/src/adapter.rs:475-493`). The CLI opens one handle per process
(`aep-backend-eventlog/src/lib.rs:410`, `entity-eventlog/src/sync.rs:230`), so handle reuse is not the
cause; one strict open per capture is.

## Acceptance

- Red first: a test that opens one handle, runs one capture, then runs N further captures with no
  write between them, and asserts that the journal frames are chained, re-encoded and folded once,
  not N times (count through a hook the crate exposes for tests). It fails at db608cd.
- Blob content is still hashed on every capture, by decision (2026-09-21, coordinator): a capture
  hands blob bytes out, and this crate hashes bytes it hands out (`Transaction::blob`,
  `docs/design/file-provider.md`); not re-hashing would mean not re-reading, which means holding the
  store's whole content per handle, unbounded. The counting case pins that with
  `blobs_hashed == N × blobs`, so changing it is a decision and not a drift. The content half is
  story:file-capture-blob-digests-without-rehashing.
- A capture after a write by another process still detects the change: the resumed path compares
  the committed prefix the way `Journal::resume` does, and a damaged or diverged file falls back to
  the strict opener. Existing capture conformance tests unchanged.
- Measured on the same copy as the table above, `aep plan artifact list` and `history` with a binary
  carrying the change; the numbers go into the CHANGELOG entry, not the ones above.
- `scripts/gate.sh` green; the ER and AEP pins follow, in that order, per
  story:cross-repository-rev-uniformity-and-pin-order in the AEP store.
