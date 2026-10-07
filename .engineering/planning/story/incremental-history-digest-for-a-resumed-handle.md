---
format: aep.planning-md/3
id: story:incremental-history-digest-for-a-resumed-handle
kind: story
status: draft
title: A resumed handle proves its prefix without reading all of it
summary: replace the whole-prefix re-hash with an incremental or block-wise digest; target 7 ms
owner: eventlog
refs:
- provider: github
  reference: beyond10x/eventlog#42
relations:
- informed_by: story:file-eventlog-verifies-once-per-open
- informed_by: review-result:verify-once-review-1
- serves: vision:O2
revision: 3
---
## Outcome

A resumed file Eventlog handle establishes that the committed prefix is still the history it
verified **without reading the whole prefix** — the cost of a transaction is independent of the
size of the store again. Target: the **7 ms** per 200 read transactions measured at c698923, with
the refusals that c698923 lost still in place.

## Why

story:file-eventlog-verifies-once-per-open removed the per-transaction re-parse and per-transaction
blob verification, taking 200 read transactions from 18.6–25.6 s to 7 ms. Independent pass 1 then
showed the result trusted a prefix it had not re-read: a committed frame damaged in place after
`open` was served intact, an `append` was accepted onto it and left a history no opener accepts,
and `decode_chain` matched the tail's `previous` against the observed digest as a string
(review-result:verify-once-review-1, F1–F3). The correction, bot commit
9f234c5d09b234cf207d5d8a311ac8a642c6bc69, re-reads and re-hashes the raw bytes of
`events.jsonl[0..observed.length]` on every `resume`.

That restores every refusal, and it costs:

| state | 200 read transactions | per transaction | blob opens |
| --- | --- | --- | --- |
| f802eb8 | 18.6–25.6 s | 93–128 ms | 200,000 |
| c698923 | 7 ms | 0.035 ms | 0 |
| 9f234c5 | 1,045 / 1,044 / 1,171 ms | ~5.2 ms | 0 |

302,158,800 bytes are read per 200 transactions over a 1.51 MB history; the host CPU (i9-10900K)
has no SHA-NI, so the hash is software SHA-256. The real 218-transaction, 3.2 MB planning store
extrapolates to about 2.4 s against 79.7 s — a large win, and still O(store bytes) per transaction.

## Acceptance

- A resumed handle refuses every case `crates/eventlog-file/tests/verify_once_review.rs` and the
  durability cases added with the correction refuse today, with the whole-prefix re-hash replaced
  by a digest that does not read the whole prefix — a Merkle or block-wise structure over the
  committed bytes, or a chained digest the writer maintains, whichever the design settles on.
- Red first: each existing refusal case is shown to still fail against a stubbed-out check
  (AGENTS.md invariant 5, mutation applied, watched, reverted).
- Measured on the same fixture as above, before and after: 200 read transactions and blob opens.
  The number to beat is 1,045 ms; the number to approach is 7 ms.
- `docs/design/file-provider.md` states what a resumed handle reads and what it trusts, and the
  safety envelope reads no weaker than it does at 9f234c5.
- No refusal is traded for the speed. A design that reaches 7 ms by trusting more is not this story.

## Finding: the window

Recorded 2026-10-07 by the coordinator of the file-provider-cost wave, from the `story-scoper`
report at `de30462b`. Inferred, level 3 (walked, not run).

- Since 3ae11c01 a resume over an unchanged file whose stamp is at least 2 s old reads 0 prefix
  bytes (`crates/eventlog-file/src/journal.rs:656-662`). The whole-prefix read (:665-676) remains
  inside 2 s of any change, the handle's own writes included (`absorb` clears the stamp,
  :107-111), and after another writer appends (:656 requires `manifest == *observed`).
- Inside the window no digest, in memory or on disk, removes the read: the refusal cases damage
  `events.jsonl` in place at the same length microseconds after a write
  (`tests/verify_once_review.rs:22-37`), the handle holds no trusted stamp then (:107-111,
  :175-184), and the damaged bytes are the only evidence.
- `Manifest`, `Frame` and the intents are `deny_unknown_fields` (:32, :43, :55, :63) and `format`
  must equal `eventlog-file/1` (:12), so a persisted digest is a format change.
- The 1,045 ms baseline in `## Why` may not reproduce at `de30462b`: `measure_review_two.rs`
  builds and reads in separate runs, so its reads likely take the trusted stamp path.

Consequence: this story's acceptance (no whole-prefix read, no added trust) is reachable only with a
layout whose sealed parts age past the window, such as a segmented journal, which is a new format.
story:file-eventlog-rechecks-its-prefix-by-comparison takes the reachable part: the read stays and
the SHA-256 goes.
