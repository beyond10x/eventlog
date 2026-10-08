---
format: aep.planning-md/3
id: story:file-capture-after-a-write-resumes
kind: story
status: draft
title: A file capture after a write through the same handle resumes from its verified view
summary: capture after own write re-reads, re-chains and re-folds the whole history
owner: eventlog
relations:
- informed_by: review-result:adversary-file-eventlog-rechecks-its-prefix-pass-1
revision: 1
---
## Outcome

A `FileEventStore` capture taken after a write through the same handle resumes from the handle's
verified view instead of re-reading, re-chaining, re-encoding and re-folding the whole history.

## Why

Found by the adversary on story:file-eventlog-rechecks-its-prefix-by-comparison, pass 1
(review-result:adversary-file-eventlog-rechecks-its-prefix-pass-1, finding F4, origin
pre-existing, reproduced on a copy of `32168648`): after a write through the same handle a capture
charged `frames_chained: 5, frames_reencoded: 5, frames_folded: 5` and compared 0 prefix bytes.
The adversary attributes it to the filter at `crates/eventlog-file/src/capture.rs:223-225`
comparing against `runtime.observed`, which every transaction moves (inferred, read not run).
The writer-plus-capture pattern is the default one, so the reader-side comparison of that story
serves only back-to-back captures.

## Acceptance

- A capture after a write through the same handle charges no re-chained, re-encoded or re-folded
  frame for history the handle already verified, shown red first with the cost module.
- Every refusal case in `tests/consistent_capture.rs` and `verify_once_review_two.rs` still
  refuses.
