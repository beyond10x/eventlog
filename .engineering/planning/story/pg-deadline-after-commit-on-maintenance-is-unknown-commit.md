---
format: aep.planning-md/3
id: story:pg-deadline-after-commit-on-maintenance-is-unknown-commit
kind: story
status: draft
title: A PostgreSQL maintenance write that times out after COMMIT returns UnknownCommit
relations:
- serves: vision:O2
- informed_by: story:pg-deadline-before-commit-is-not-unknown-commit
revision: 2
---
## Outcome

PostgreSQL redact, snapshot-generation, checked snapshot save, tenant erasure, catch-up and rebuild
return `EventLogError::UnknownCommit` when their store-operation deadline fires after COMMIT was
written, and `Deadline` only before it.

## Why

Found while fixing the append paths: these operations run inside `bounded(..)`
(`crates/eventlog-postgres/src/lib.rs`), whose outer timeout always maps to `Deadline`, so an expiry
while the COMMIT reply is outstanding tells the caller the work did not commit when it may have.
The append paths and the inline rebuild already split the two outcomes on a commit-started flag.

## Acceptance

- Each operation records whether COMMIT was written and maps an expiry by it, as the append paths
  do (`eventlog.pool.TimedOutWrite`).
- A test per operation withholds the COMMIT reply (a proxy, as the append test does) and asserts
  `UnknownCommit`; each fails on the base commit.

## Demonstrated (2026-10-16)

An adversarial case withholding the COMMIT reply showed `redact` returning
`Deadline { operation: "store operation" }` for a redaction the server committed. The same `bounded`
wrapper also surrounds `put_blob`, so it belongs in this story too. The case is kept outside the
tree until this story starts; it is to be added as its failing-first test.
