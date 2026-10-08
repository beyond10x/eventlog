---
format: aep.planning-md/3
id: review-result:durable-capture-continuity-adversary-1
kind: review-result
status: archived
title: 'Adversary pass 1: float drift through the journal gives a false AppendDelta'
relations:
- reviews: story:durable-capture-continuity
revision: 2
transitions:
- {from: "active", to: "archived", at: "2026-10-08T11:00:47Z", actor: "human:timo", revision: 2}
---
unit: story:durable-capture-continuity — commit d9a07279, managed tree durable-continuity
verdict: NEEDS-CHANGE
cases: executed 0→17 in crates/eventlog-sqlite/tests/durable_capture_adversary.rs, red 6
origin: introduced 6, pre-existing 0, undecided 1
needs-coordinator: origin of F4 (in-process float drift) not run against the base

Six red cases. The blocker: the durable journal re-serialises a projection row's before value, so a float `serde_json` does not round-trip comes back as a different number and the AppendDelta's before no longer equals the base. The in-process delta carries in-memory floats, so it differs from the complete capture too (F4), and a durable checkpoint issued through it binds the drifted usage (F3). The durable path skipped projection admission (F5). A handle opened before another process enabled continuity refused this store's own bytes (F6). Incremental BLOB I/O fires no trigger (judgement, observed on SQLite 3.53.4 outside the provider).

Held: owners `a` and `a_p` in one file; guard writes inside a group; checkpoint before redaction; tenant erased and re-provisioned; delete_blob then rebind; u64::MAX, i64::MIN, nulls, -0.0, NUL and emoji values; create_projections with a bad later spec; retired instance; another tenant's group in an in-process chain; journal marks around a two-member group with a blob.

```findings
- file: crates/eventlog-sqlite/src/durable_capture.rs
  line: 418
  category: property
  severity: blocker
  verdict: NEEDS-CHANGE
  origin: introduced
  message: journal re-serialises the projection row before value, so for floats serde_json does not round-trip the durable AppendDelta's before differs from the value the base capture holds
- file: docs/design/durable-capture-continuity.md
  category: contract-drift
  severity: warning
  verdict: NEEDS-CHANGE
  origin: introduced
  message: Contract requires the durable delta to equal the in-process delta, but for such floats the in-process delta carries in-memory values while durable and complete captures carry reread values
- file: crates/eventlog-sqlite/src/tracked_capture.rs
  line: 685
  category: contract-drift
  severity: warning
  verdict: NEEDS-CHANGE
  origin: introduced
  message: a durable checkpoint issued through an in-process AppendDelta persists usage measured on in-memory floats, so checkpoint_usage in another process differs from a complete capture
- file: crates/eventlog-sqlite/src/tracked_capture.rs
  line: 456
  category: property
  severity: warning
  verdict: CONFIRMED
  origin: undecided
  message: the in-process journal records in-memory event data and row bodies, so base plus in-process delta differs from the complete capture for floats serde_json does not round-trip
- file: crates/eventlog-sqlite/src/capture.rs
  line: 224
  category: acceptance
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: durable_since skips admit_projection, so a foreign registry deletion continues as Unchanged while a complete capture refuses Undeclared
- file: crates/eventlog-sqlite/src/capture.rs
  line: 83
  category: boundary
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: restore_checkpoint compares the handle's cached instance, so a handle opened before another process enabled rejects this store's own bytes until it captures
- file: docs/design/durable-capture-continuity.md
  category: judgement
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: incremental BLOB I/O rewrites captured columns without firing triggers, so it yields a false Unchanged and is not named out of scope
```
