---
format: aep.planning-md/3
id: story:pg-deadline-before-commit-is-not-unknown-commit
kind: story
status: implemented
title: A PostgreSQL append that times out before COMMIT returns Deadline, not UnknownCommit
relations:
- serves: vision:O2
revision: 4
transitions:
- {from: "draft", to: "proposed", at: "2026-10-16T12:42:19Z", actor: "human:timo", revision: 2}
- {from: "proposed", to: "active", at: "2026-10-16T12:42:19Z", actor: "human:timo", revision: 3}
- {from: "active", to: "implemented", at: "2026-10-16T16:40:35Z", actor: "human:timo", revision: 4, decided_on: {"recorded":{"test_result":1}}}
---
## Outcome

A PostgreSQL append whose store-operation deadline fires before COMMIT was written returns
`EventLogError::Deadline`; `EventLogError::UnknownCommit` is returned only once COMMIT was sent.

## Why

`append_guarded` (`crates/eventlog-postgres/src/lib.rs`) and `append_group_guarded` /
`append_group_with_blobs_guarded` (`crates/eventlog-postgres/src/atomic_group.rs`) map any
`tokio::time::timeout` expiry to `UnknownCommit`, even when the deadline fired in the guard, the
lock wait or the insert, before COMMIT. A caller then resolves a command identity that cannot have
committed, and cannot tell a safe retry from an ambiguous one.

## Acceptance

- Each of the three paths records whether COMMIT was written; an expiry before it maps to
  `EventLogError::Deadline { operation }`, after it to `UnknownCommit`.
- For each path, a test on the PostgreSQL fixture makes the deadline fire before COMMIT (a guard or
  a held lock that outlasts the transaction timeout) and asserts `Deadline`, and asserts nothing was
  committed. Each fails on the base commit.
- The existing unknown-commit cases still return `UnknownCommit`.
- Specification: `eventlog.pool.TimedOutWrite` names the two outcomes
  (ess/pool/domains/pool.yaml).
- CHANGELOG states whether this breaks callers that match on `UnknownCommit`.
