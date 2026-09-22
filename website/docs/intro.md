---
slug: /
title: Eventlog
description: Durable event streams with File, SQLite and PostgreSQL providers.
---

Eventlog is a Rust persistence library. Commands append events; applications derive their state
from that history. Projections and snapshots make reads convenient without becoming the record.

Use it when your application needs optimistic concurrency, exact command retries, atomic groups
or rebuildable views. Your application supplies domain rules, authentication and the tenant selected
from trusted authority. Eventlog enforces the storage boundary.

## Start with a provider

- **File:** a local directory with a durable JSONL journal and content objects.
- **SQLite:** an embedded database, including `:memory:` for disposable tests.
- **PostgreSQL:** a server-backed provider with explicit hosted-role and TLS admission.

[Compare the providers](providers.md), then [run the quickstart](quickstart.md).
The current release is [0.3.0](releases.md).

## Learn the contract

[Persistence guarantees](guarantees.md) explains retries, atomic groups, blobs, capture and snapshots.
[Operating Eventlog](operations.md) covers provisioning, inspection, recovery and projections.

Eventlog is consumed as a library. It does not run a hosted service, authorize users or decide
whether a domain command is legal.
