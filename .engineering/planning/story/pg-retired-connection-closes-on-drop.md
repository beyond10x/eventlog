---
format: aep.planning-md/3
id: story:pg-retired-connection-closes-on-drop
kind: story
status: implemented
title: A retired PostgreSQL connection closes when its lease drops, on any runtime
relations:
- serves: vision:O2
revision: 5
transitions:
- {from: "draft", to: "proposed", at: "2026-10-16T12:42:19Z", actor: "human:timo", revision: 2}
- {from: "proposed", to: "active", at: "2026-10-16T12:42:19Z", actor: "human:timo", revision: 3}
- {from: "active", to: "implemented", at: "2026-10-16T16:40:35Z", actor: "human:timo", revision: 5, decided_on: {"recorded":{"test_result":1}}}
---
## Outcome

A PostgreSQL lease dropped without settling (a timed-out or cancelled operation) closes its
connection synchronously in `Lease::drop`: the transport is released before the drop returns, on
any runtime, so the server ends the session and rolls back its transaction at once.

## Why

`crates/eventlog-postgres/src/pool.rs` retires a quarantined lease by `tokio::spawn`ing a task that
aborts the driver task. On a caller's current-thread runtime neither task runs until the caller
next drives that runtime, so an abandoned transaction stays open, idle in transaction and holding
its locks, until the server's own `idle_in_transaction_session_timeout`. A consumer's ingestion
blocks on those locks.

## Acceptance

- The driver future is owned outside the spawned driver task, and `Lease::drop` (and
  `Connection`'s drop) drops it synchronously when the connection is retired.
- A test on a current-thread runtime times out an append that holds a row lock inside an open
  transaction, never polls that runtime again, and observes from a second, independent connection
  that the abandoned backend is gone from `pg_stat_activity` (or its lock is free) well before the
  server's timeout. The test fails on the base commit.
- Pool accounting is unchanged: capacity returns only after the retirement is accounted; every
  existing pool test passes.
- Specification: `eventlog.pool` records the synchronous close (ess/pool/domains/pool.yaml).

## Blocked backends (added 2026-10-16)

Measured by a consumer on docker PostgreSQL with a pg_stat_activity/pg_locks probe: an append group
dropped at the deadline while its backend waits on a lock stays alive server-side after the client
closes, holding the shared publication-gate advisory lock and its exclusive advisory locks. A
backend waiting on a lock does not read its socket, so a socket close alone does not end it.

- A retired, unsettled lease sends a server-side cancel for its backend, independent of whether
  the caller's runtime is polled again, before its capacity is returned.
- The session sets `client_connection_check_interval`, so a backend whose client is gone aborts its
  running query even when no cancel could be sent.
- A test holds a lock from an independent connection so the append's backend blocks inside a
  query, lets the deadline fire on a current-thread runtime that is not polled again, and, while
  that lock is still held, observes the backend gone from pg_stat_activity and its advisory locks
  gone from pg_locks. The test fails on the base commit and with the socket close alone.
