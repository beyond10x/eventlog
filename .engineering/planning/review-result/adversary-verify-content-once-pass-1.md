---
format: aep.planning-md/3
id: review-result:adversary-verify-content-once-pass-1
kind: review-result
status: active
title: Adversary pass 1 on verifying blob content once per handle (77e0d563)
relations:
- reviews: story:file-and-postgres-verify-each-content-once
revision: 1
---
needs-revision

Read-integrity claim holds: no read path returns bytes that were not checked against the bound hash. The erasure-memory claim does not hold on PostgreSQL; one real defect, one red test.

unit: commit 77e0d563, story:file-and-postgres-verify-each-content-once
verdict: NEEDS-CHANGE
cases: executed 121→122, red 1 (plus 2 pre-existing fixture-absent reds in both runs)
origin: introduced 2 / pre-existing 0 / undecided 0

Findings
- crates/eventlog-postgres/src/lib.rs:792 — forget_tenant clears the handle's memory before its transaction commits; a same-handle read of that tenant during the in-flight erasure re-remembers the content, and the erasure then commits with the erased tenant's bytes still held, contradicting "an erasure through the handle leaves none of its bytes here". delete_blob (:1059) has the same ordering. File is not affected: it serialises every call on one async lock. Test: verify_content_once::adversary::a_read_during_an_erasure_through_the_same_handle_leaves_erased_bytes_remembered
- crates/eventlog-core/src/verified_content.rs:92-105 — check_stored holds the handle-wide lock across the SHA-256, so concurrent PostgreSQL blob reads (up to 4 pooled connections by default) that hashed in parallel before now hash serially. No test (timing would be flaky).

Red output (case alone, before suite)
panicked at crates/eventlog-postgres/src/verify_content_once/adversary.rs:65:5:
assertion `left == right` failed: an erasure through this handle leaves none of the erased bytes in its memory
  left: (4096, 1)
 right: (0, 0)
test result: FAILED. 0 passed; 1 failed; ... 22 filtered out

Attacked, could not break: hash reuse across tenants/digests; File object id vs blob.hash; remember-before-commit, guard refusal, rollback; budget overflow and clear; byte_count/edition checks on a hit; callback_failed on Backend errors; File resume/open full check; VerifiedContent exposed publicly (per-instance, no domain types).

```findings
- file: crates/eventlog-postgres/src/lib.rs
  line: 792
  category: concurrency
  severity: warning
  verdict: NEEDS-CHANGE
  origin: introduced
  message: forget_tenant (and delete_blob at :1059) clears the memory before commit, so a concurrent same-handle read re-remembers erased content that stays held after the erasure commits, contradicting verified_content.rs:20-22.
- file: crates/eventlog-core/src/verified_content.rs
  line: 92
  category: judgement
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: check_stored holds the handle-wide mutex while hashing, so concurrent PostgreSQL blob reads that ran in parallel before now hash one at a time.
```
