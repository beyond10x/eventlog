---
format: aep.planning-md/3
id: story:postgres-batch-read-and-append
kind: story
status: active
title: PostgreSQL answers read_many and append_group in batches
relations:
- serves: vision:O2
revision: 3
transitions:
- {from: "draft", to: "proposed", at: "2026-11-11T00:15:08Z", actor: "human:timo", revision: 2}
- {from: "proposed", to: "active", at: "2026-11-11T00:15:13Z", actor: "human:timo", revision: 3}
---
## Outcome

On PostgreSQL, `EventStore::read_many` answers a batch of stream reads with a bounded number of
statements instead of one statement per read, and `append_group` writes a group's events without
one statement per event. A consumer that reads one row each of 10,000 streams, or appends one group
of 20,000 events, pays about what SQLite pays for the same work.

## Why

Spindle's PostgreSQL store scans an entity by reading each instance's stream at a named version.
Spindle's third adversary pass on its PostgreSQL store (spindle `review-result:w19-u1-adversary-3`)
measured, in release builds against PostgreSQL 17 on localhost:

- 10,000 single-row reads: 6.72 s on PostgreSQL, 0.23 s on SQLite (about 0.6 ms per read);
- 50,000 single-row reads: 29.2 s against 1.33 s;
- one append group of 1,000 writes 2.26 s (SQLite 0.22 s), 5,000 writes 14.5 s (1.44 s), and
  20,000 writes fails at the 10 s store-operation deadline.

`read_many` exists on the trait (default: each read in turn). The PostgreSQL provider does not
override it.

## Acceptance

1. The PostgreSQL provider overrides `read_many`: N `Read::Stream` entries are answered by at most
   ceil(N / 1,000) statements, or one statement per batch the provider documents, inside one
   snapshot, and each result equals what `read_stream` returns for the same arguments (limit,
   `after_version`, missing stream, redacted event). `Read::Blob` entries keep their current
   answer.
2. `append_group` on PostgreSQL inserts a group's events in a bounded number of statements; the
   group stays atomic, version checks, guards, claims and commands are unchanged, and the
   conformance suite passes unchanged on every provider.
3. A test on PostgreSQL shows 10,000 single-row reads through `read_many` in under 1 s in a
   release build, and one group of 20,000 events committed inside the default deadline.
4. No DDL change. The SQLite and file providers are unchanged.
5. Released as 0.9.3.

## Scope

- `crates/eventlog-postgres/src/lib.rs`: `read_many`, `append_in_transaction` / group insert path
- `crates/eventlog-postgres/tests/`: the batch-read and group-size cases
- `CHANGELOG.md`
