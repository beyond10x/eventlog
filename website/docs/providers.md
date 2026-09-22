---
title: Choose a provider
---

All three providers implement the applicable Eventlog contracts. In-memory operation is SQLite
`:memory:`; File is a separate durable provider.

| Concern | File | SQLite | PostgreSQL |
|---|---|---|---|
| Storage | Local directory, JSONL journal and content objects | Embedded database | Database server |
| Typical use | Repository-local or single-host state | Embedded applications and disposable tests | Hosted services |
| Concurrency | Persistent process lock serializes access | Immediate write transactions | Bounded pool and transactional locks |
| Atomic append groups | Supported | Supported | Supported |
| `AtomicBlobEventStore` | Supported | Supported | Supported |
| `append_group_guarded_with_blobs` | One group durability barrier | Refuses this optional operation | Refuses this optional operation |
| Strict `InspectHistory` | Supported | Supported; Linux inspection requires an OFD lock | No shipped inspector |
| Complete tenant capture | Supported | Supported | Supported |
| Inline projection rebuild | Supported | Supported | Supported |

## File

Requires a local filesystem with working process locks, atomic rename, file synchronization and
directory synchronization. Linux is the current acceptance platform. Network filesystems,
concurrent Git checkout/merge over an open store, hardware power-loss certification and production
capacity are outside the established proof.

Keep the complete store together. `manifest.json` and the committed prefix of `events.jsonl`
are authority, alongside referenced content. A manifest cannot be recreated from a guess.

## SQLite

Use a database file for persistence and `:memory:` for disposable state. The provider bundles
the pinned SQLite dependency. A successful SQLite test does not establish PostgreSQL behavior.
Existing-only open refuses absent databases or owner tables without creating them.

## PostgreSQL

The convenience `connect` constructor is restricted to loopback, localhost or Unix sockets with
TLS disabled. Hosted composition uses verified TLS roots or caller-owned connection authority,
separate migration and application roles, and an explicit connection budget.

The feed watermark is cluster-wide: a long-running transaction in another owner can delay catch-up.
That delay does not mean events were skipped. Inline projections do not wait for the feed.
See [operating guidance](operations.md#hosted-postgresql-composition).
