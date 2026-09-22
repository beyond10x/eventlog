---
format: aep.planning-md/1
id: story:file-eventlog-verifies-once-per-open
kind: story
status: active
title: A file Eventlog transaction costs what it touches, not the whole store
owner: eventlog
relations:
- informed_by: story:file-eventlog
- depends_on: story:file-eventlog
- serves: vision:O2
scope:
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-file/src/journal.rs
- confidence: cited
  path: crates/eventlog-file/src/lib.rs
- confidence: cited
  path: crates/eventlog-file/src/state.rs
- confidence: cited
  path: crates/eventlog-file/tests/
- confidence: cited
  path: docs/design/file-provider.md
revision: 6
---
## Outcome

A read or write transaction on the file Eventlog costs what it touches plus one raw re-hash of the
committed bytes, not a re-parse and re-verification of the whole store.

The complete committed history and every blob are verified once, when the store is opened. A later
transaction re-reads the raw bytes of `events.jsonl[0..observed.length]` and compares a SHA-256 of
them against the digest taken at open — about **5 ms per 1.5 MB** on a CPU without SHA-NI — and
then re-verifies only what changed on disk since the last observed manifest. It re-parses no frame
it has already folded and opens no blob it does not touch.

Measured, 200 read transactions on the acceptance fixture:

| state | 200 read transactions | blob opens |
| --- | --- | --- |
| f802eb8 (before) | 18.6–25.6 s | 200,000 |
| c698923 (verify-once, before the prefix check) | 7 ms | 0 |
| 9f234c5 (with the prefix check) | 1,045 / 1,044 / 1,171 ms | 0 |

The 218-transaction, 3.2 MB real planning store extrapolates to about **2.4 s against 79.7 s**.

The raw re-hash is still proportional to the size of the committed history, with a far smaller
constant than the re-parse it replaced; making a transaction's cost independent of store size again
needs an incremental or block-wise history digest, which is
story:incremental-history-digest-for-a-resumed-handle and not this story.

The safety envelope in `docs/design/file-provider.md` is preserved, and preserved without
qualification: a history that diverged under a handle is still refused, a corrupt blob is still
refused when read, a chain that does not extend the observed manifest is still refused, and a
committed frame damaged in place after `open` refuses **without altering the history** — the
re-hash is what makes that last one true again after the first verify-once implementation lost it
(review-result:verify-once-review-1, findings F1–F3).

## Measurement this comes from

Coordinator, 2026-09-20 23:50–00:05 CEST, release-built qualified `aep` (sha256 340eb7f0…) against
the retained migrated copy `/var/tmp/ess-copy.PIfLrT/denial-probe` (64 artifacts, `events.jsonl`
3,164,561 bytes / 2,078 frames, 1,053 blobs / 10,640,358 bytes):

| command | wall | user CPU |
| --- | --- | --- |
| `aep plan artifact list` | 79.65 s | 74.16 s |
| `aep plan artifact history <one story>` | 75.52 s | 73.70 s |
| `aep plan store verify` (one transaction) | 2.30 s | 2.17 s |

During one `list`: 218 opens of `events.jsonl`, 227,452 opens of blob files (1,043 per journal open),
457,749 `read` calls, 2,863 MB read (`strace`); 48% of CPU samples in `sha2::sha256::soft::compress`
(`perf`, 38,240 samples; the host CPU, i9-10900K, has no SHA-NI).

Cause, read from `crates/eventlog-file/src/lib.rs:86-113` at f802eb8: `transaction()` calls
`Journal::open` (re-reads and re-verifies the whole chain), `extends()` (recomputes every frame
digest from zero), `State::replay`, and then `for (tenant, digest) in tx.state.blobs.keys() {
tx.blob(..) }`, which reads and hashes every blob — on every transaction. The AEP CLI issues about
one transaction per artifact per command, so a command costs O(artifacts × store bytes).

## Acceptance

- Red first, in `crates/eventlog-file`: after `open`, a second read-only transaction does not re-read
  every blob and does not re-hash every frame. Make it observable without timing: for example, a
  blob file replaced with wrong bytes after `open` no longer fails an unrelated read-only
  transaction, while `blob()` of that digest still fails with the integrity error; and a frame
  appended by another handle is still detected by `extends`. AGENTS.md invariant 5: the new test
  fails without the fix (mutation applied, watched, reverted).
- Every existing conformance and durability test passes unchanged (`crates/eventlog-file/tests/`);
  invariant 4: no conformance exercise is edited.
- Measured in the report, same fixture shape as above (about 2,000 frames, 1,000 blobs of 10 KB,
  200 read transactions), before and after: wall time and blob reads.
- `docs/design/file-provider.md` states when the history and blobs are verified and what a later
  transaction re-checks. CHANGELOG Unreleased entry.
- `bash scripts/gate.sh` exit 0.

## Scope

cited (lib.rs read 2026-09-21): crates/eventlog-file/src/lib.rs, crates/eventlog-file/src/journal.rs,
crates/eventlog-file/tests/, docs/design/file-provider.md, CHANGELOG.md.
