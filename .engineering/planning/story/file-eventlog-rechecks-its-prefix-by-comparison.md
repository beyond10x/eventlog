---
format: aep.planning-md/3
id: story:file-eventlog-rechecks-its-prefix-by-comparison
kind: story
status: active
title: A resumed file Eventlog re-checks its prefix by comparison, not SHA-256
summary: in-window resume compares the verified bytes instead of re-hashing them
owner: eventlog
refs:
- provider: github
  reference: beyond10x/eventlog#42
relations:
- serves: vision:O2
- informed_by: story:incremental-history-digest-for-a-resumed-handle
scope:
- confidence: inferred
  path: CHANGELOG.md
- confidence: inferred
  path: crates/eventlog-file/src/capture.rs
- confidence: cited
  path: crates/eventlog-file/src/cost.rs
- confidence: cited
  path: crates/eventlog-file/src/journal.rs
- confidence: cited
  path: crates/eventlog-file/src/lib.rs
- confidence: cited
  path: docs/design/file-provider.md
- confidence: inferred
  path: website/docs/operations.md
revision: 11
transitions:
- {from: "draft", to: "proposed", at: "2026-10-07T19:49:26Z", actor: "human:timo", revision: 10}
- {from: "proposed", to: "active", at: "2026-10-07T19:49:26Z", actor: "human:timo", revision: 11}
---
## Outcome

A resumed file Eventlog handle whose journal changed inside the stamp window re-checks the
committed prefix by comparing its bytes with the bytes it verified, not by computing SHA-256 over
them. Every refusal the re-hash gives today still holds; the per-transaction cost of an
append-heavy workload falls by the ratio of a SHA-256 pass to a byte comparison.

## Why

https://github.com/beyond10x/eventlog/issues/42. Reproduced at `de30462b` with a throwaway probe
(one event per append transaction, 1.4 KB payload, 40 streams, release build):

| events | `events.jsonl` | appends total | per append | read all streams, inside 2 s | after 2 s |
| --- | --- | --- | --- | --- | --- |
| 2,000 | 4,811,762 B | 54,422 ms | 27.21 ms | 1,342 ms | 26 ms |

`perf` over the same probe at 1,500 events: 96.80% of samples in `sha2::sha256::compress256`,
0.30% in `State::head`. The re-hash is `resumed_committed` (`crates/eventlog-file/src/journal.rs`
:665-676), taken whenever the file changed in the last `UNTRUSTED_NS` = 2 s (:137-142), the
handle's own writes included (`absorb` clears the stamp, :107-111).

story:incremental-history-digest-for-a-resumed-handle asks for a digest that does not read the
prefix at all. Its scoper found (level 3, unproven) that inside the window no digest can: the
refusal cases damage `events.jsonl` in place at the same length microseconds after a write, the
handle holds no trusted stamp then, and the damaged bytes are the only evidence. This story takes
the part that is reachable without a format change and without trusting more: the read stays,
the hash goes.

## Acceptance

- Inside the window, `resumed_committed` and the strict resume compare the file's committed prefix
  with the bytes the handle verified, exactly, and treat any difference as today's digest mismatch.
  No SHA-256 is computed over the prefix on that path.
- The verified bytes are held once per handle, shared between `Verified` (`lib.rs:63-71`) and
  `capture::Observed` (`capture.rs:44-48`) rather than copied; `docs/design/file-provider.md`
  states what a resumed handle reads, what it holds in memory and what it trusts, and its safety
  envelope reads no weaker than at `de30462b`. No new crate dependency; `eventlog-file/1` bytes do
  not change.
- Red first: the `#[cfg(test)]` cost module (`crates/eventlog-file/src/cost.rs`) shows prefix bytes
  hashed on an in-window resume; a test asserts zero hashed and the prefix length compared, fails
  at `de30462b`, and passes after. Every refusal case in `tests/verify_once_review.rs`,
  `verify_once_review_two.rs`, `durability.rs`, `stamped_resume.rs` and `consistent_capture.rs`
  still refuses, and each refusal is shown to fail against a comparison stubbed to `true`
  (AGENTS.md invariant 5: mutation applied, watched, reverted).
- Measured with the same probe before and after: per append and in-window read time at 2,000
  events. The bar is per append at most one fifth of 27.21 ms on the same machine.

## Scope

Derived 2026-10-07 from the `story-scoper` report for
story:incremental-history-digest-for-a-resumed-handle at `de30462b`, which reads the same path.
Every line is **cited** (read from a story or the tree) or **inferred** (a reading that could be
wrong).

- **Primary surface:** `crates/eventlog-file/src/journal.rs`, the resume path — cited
- **Files:** `journal.rs` — `Content` :79-119, `Stamp` :121-164, `trusted` :173-184, open builds
  `Content` :424-439, `append` absorbs :497-517, `Journal::resume` :519-563, `privacy` :600,
  `resumed_committed` :605-705, `Resumed`/`StrictResumed`/`resume_strict` :733-829, `open_strict`
  :945, unit test :1407-1516 — cited
- **Files:** `crates/eventlog-file/src/lib.rs` — `Verified` doc :55-71, `enter` :972-1052,
  `mod stamped_resume_cost` :2450-2600 — cited
- **Files:** `crates/eventlog-file/src/cost.rs` `Cost` :19-69 — cited (the acceptance names it)
- **Files:** `crates/eventlog-file/src/capture.rs` — `Observed` :44-48 and :265-349 — inferred
- **Refusal pins, run and mutated, not edited:** `tests/verify_once_review.rs`,
  `verify_once_review_two.rs`, `durability.rs:510`, `stamped_resume.rs`,
  `consistent_capture.rs:690,736` — cited
- **Documents:** `docs/design/file-provider.md` :61-123, :282-293 — cited;
  `website/docs/operations.md` :20-21 and `CHANGELOG.md` `[Unreleased]` — inferred
- **Confidence:** high for `journal.rs` and the design page; medium for `lib.rs`, `capture.rs`
- **Would collide with:** `Cost` in `cost.rs`; the end of `lib.rs` (:2450-2600 is the last
  module); `CHANGELOG.md` `[Unreleased]` — inferred
- **Does not touch:** `state.rs`; `lib.rs` :478-495, :1272-1288, :1602-1628 — inferred
- **Safety fact:** the raw-byte digest exists only in memory: `Content` is `pub(crate)` and only
  `Clone` (`journal.rs:86-87`), held in `Verified` (`lib.rs:70`) and `Observed` (`capture.rs:48`),
  neither serialized; on-disk digests hash re-serialized frames (:1044, :1080), not raw bytes. So
  replacing the re-hash with a comparison changes no persisted byte — level 2, unproven
