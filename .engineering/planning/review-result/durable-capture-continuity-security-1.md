---
format: aep.planning-md/3
id: review-result:durable-capture-continuity-security-1
kind: review-result
status: active
title: 'Security pass 1: a redacted row value survives in the journal'
relations:
- reviews: story:durable-capture-continuity
revision: 1
---
unit: story:durable-capture-continuity — commit d9a07279, managed tree durable-continuity
verdict: NEEDS-CHANGE
cases: executed 0→13 in crates/eventlog-sqlite/tests/durable_capture_security.rs, red 5
origin: introduced 4, pre-existing 0, undecided 1
needs-coordinator: origin of finding 2; the same table-name query exists at cf7f61e1 (lib.rs:955, capture.rs:641, inspection.rs:353) but was not run there

The blocker: redaction leaves projection rows in place, and the next group that rewrites such a row writes the redacted value into the journal as its before value, where it outlives the row until pruning or the next redaction. Trigger checks compare tbl_name case-sensitively, so a foreign trigger `ON SECURE_BLOBS` passes open, attach, capture admission and inspection. Edited checkpoint bytes can carry another stream identity (continues) or another usage (LimitExceeded instead of Complete). Incremental BLOB I/O is not named out of scope.

Held: invariant 7 (only redact updates an events table; it clears the journal in its transaction, lib.rs:2028); failed redaction or erasure leaves journal and data consistent; journal and checkpoint bytes hold no event payload or blob bytes; invariant 6 (no ordinary path creates continuity objects; disable after a late projection restores the never-enabled schema); near-miss and TEMP/cross-owner triggers refused or end continuity; prefix and projection names limited to [a-z0-9_] and trigger text compared exactly; about 25 hostile encodings give None or Complete without panic; Debug and error text carry no bytes, tokens or row values.

```findings
- file: crates/eventlog-sqlite/src/durable_capture.rs
  line: 412
  category: acceptance
  severity: blocker
  verdict: NEEDS-CHANGE
  origin: introduced
  message: a group after redaction writes the redacted projection value into a journal entry as its before value, where it outlives the row and the redaction
- file: crates/eventlog-sqlite/src/durable_capture.rs
  line: 149
  category: integrity
  severity: warning
  verdict: CONFIRMED
  origin: undecided
  message: foreign_triggers_on matches tbl_name case-sensitively, so a foreign trigger spelling a captured table in upper case passes open, attach, capture admission and strict inspection
- file: crates/eventlog-sqlite/src/durable_capture.rs
  line: 854
  category: integrity
  severity: warning
  verdict: INFEASIBLE
  origin: introduced
  message: a restored checkpoint's stream identity is never compared with the stored one, so edited bytes continue as Unchanged and an AppendDelta reports the edited identity
- file: crates/eventlog-sqlite/src/durable_capture.rs
  line: 872
  category: integrity
  severity: note
  verdict: INFEASIBLE
  origin: introduced
  message: a caller-edited usage in restored bytes turns a capture under its cap into LimitExceeded instead of Complete
- file: crates/eventlog-sqlite/src/durable_capture.rs
  line: 8
  category: judgement
  severity: note
  verdict: INFEASIBLE
  origin: introduced
  message: incremental BLOB I/O writes captured columns without running triggers, which the stated out-of-scope list does not name
```
