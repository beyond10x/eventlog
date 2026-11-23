---
format: aep.planning-md/3
id: story:tree-group-keeps-its-request-fingerprint
kind: story
status: implemented
title: A Tree group keeps its request fingerprint (group/2), so a retry after erasure gets its receipt
summary: Review L71 = B; after a cold reopen an exact retry of an erased group answers IdempotencyMismatch
owner: eventlog
relations:
- serves: vision:O2
revision: 4
transitions:
- {from: "draft", to: "proposed", at: "2026-11-23T13:45:31Z", actor: "human:timo", revision: 2, decided_on: {"recorded":{"test_result":1}}}
- {from: "proposed", to: "active", at: "2026-11-23T13:45:31Z", actor: "human:timo", revision: 3, decided_on: {"recorded":{"test_result":1}}}
- {from: "active", to: "implemented", at: "2026-11-23T13:45:31Z", actor: "human:timo", revision: 4, decided_on: {"recorded":{"test_result":1}}}
---
## Outcome

A Tree group file stores the committed request fingerprint independently of erasable content
(group format `eventlog-tree/group/2`; readers accept 1 and 2), and a cold replay restores the
receipt from that fingerprint and the original result ranges. An exact retry of a group whose content
was later redacted or whose blob was deleted is answered with its receipt after a reopen. The format
change is rolled out fenced.

## Finding

Review L71, eventlog 0.9.4 (`b8582219`), `round9/eventlog.txt:6` in the review folder: cold Tree
replay can change the identity of a committed group after redaction or blob deletion. The receipt
fingerprint is rebuilt during replay from the event bodies and blob bytes still present; a redacted
event supplies the tombstone body and deleted blobs drop out, so an exact retry can return
`IdempotencyMismatch` after reopening. Source: `crates/eventlog-tree/src/history.rs:100,146,360,535`,
`crates/eventlog-tree/src/lib.rs:758,775,785`, `crates/eventlog-core/src/atomic_group.rs:52,61`,
`crates/eventlog-core/src/atomic_blob.rs:57`, `crates/eventlog-sqlite/src/atomic_group.rs:465`.

## Decision

Assessed by agent el-g2 (not fixed: the group file reader is closed, older readers refuse an unknown
field, so storing the fingerprint is a format change). Options it put:

- A: store the request fingerprint in the group file, plus a SQLite restore-receipt API (affects
  `eventlog-sqlite`). Format `eventlog-tree/group/2`, readers accept 1 and 2; `migrate` backfills
  groups whose content is still intact; old groups already redacted or with a deleted blob cannot be
  recovered.
- B (default): document the limit and change nothing. The refusal is loud, never wrong data.
- C: a sidecar receipt file per group; breaks older readers.

Timo, 2026-11-23 about 01:20 CET via `host-operator`: L71 = B, plus this draft story (option A:
group/2 fingerprint, fenced).

Not found: the documentation half of B. The diff of PR #62 (`fix/review-findings`) against `main`
adds no line in `CHANGELOG.md`, `docs/` or `crates/eventlog-tree` that states the
`IdempotencyMismatch` after erasure; this story owns that line until the fix lands.

## Acceptance

A Tree test commits a group with a blob, redacts one of its events and deletes the blob, reopens the
store cold and retries the exact request: it receives the original receipt with the original result
ranges, not `IdempotencyMismatch`, and a group/1 store opened by the new binary still reads.
