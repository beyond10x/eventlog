# eventlog

Rust libraries for durable event streams and rebuildable application state. Append events, retain
command receipts, and derive projections from committed history. The host supplies domain rules,
authentication and trusted tenant identity.

[Project guide](https://beyond10x.github.io/eventlog/) · [Quickstart](website/docs/quickstart.md)

## Where it sits

Consumed as a library, with File, SQLite and PostgreSQL providers. In-memory operation is SQLite
`:memory:`. [Choose a provider](website/docs/providers.md).

## Status

Version **0.3.0**, under [Apache-2.0](LICENSE). Pin the bare Git tag `0.3.0`.
[Release highlights](website/docs/releases.md) summarize the
[authoritative release notes](https://github.com/beyond10x/eventlog/releases/tag/0.3.0).

## Build, test, run

Install Git and Rust 1.91 or newer. From a checkout of tag `0.3.0`:

```bash
cargo test --locked -p eventlog-file -- --test-threads=1
cargo test --locked -p eventlog-sqlite -- --test-threads=1
```

The full `bash scripts/gate.sh` gate runs tests, formatting and Clippy. Its PostgreSQL cases need
an explicit disposable fixture; some refuse a missing URL while others report a skip.
[Quickstart](website/docs/quickstart.md) · [Production proof](website/docs/operations.md#validate-a-deployment).

## Public input validation

Tenant and stream constructors and deserialization enforce the same checks. Invalid public
append input refuses before durable changes. [Storage guarantees](website/docs/guarantees.md).

## Hosted PostgreSQL composition

Hosted operation requires verified TLS, separate migration and application authority, a complete
projection roster and a bounded connection budget. The local convenience constructor is loopback
only. [Hosted setup and recovery](website/docs/operations.md#hosted-postgresql-composition).

## Atomic storage admission

Trusted guards can reserve storage counters in the append transaction. The permit remains with
the owning host. [Admission and effects](website/docs/operations.md#storage-admission-and-effects).

## Snapshot provenance

Snapshots are disposable caches. Observe the history generation before folding and save through
`save_snapshot_checked`. [Snapshots and projections](website/docs/guarantees.md#projections-and-snapshots).

## Guard refusals and effect metadata

Guards return stable owner-defined refusal codes and roll back refused changes. Effect metadata
records evidence; the application decides effect policy.
[Admission and effects](website/docs/operations.md#storage-admission-and-effects).

## Required proof and comparative laboratory

Run production, capacity and restart fixtures only against disposable storage. Conformance proves
behavior; deployment capacity needs the owner's workload and budgets.
[Proof prerequisites](website/docs/operations.md#validate-a-deployment).

## Layout

| Crate | Owns |
|---|---|
| `eventlog-core` | Envelopes, ports, aggregates, snapshots and projection contracts |
| `eventlog-file` | Local JSONL provider and strict inspection |
| `eventlog-sqlite` | Embedded and in-memory SQLite provider |
| `eventlog-postgres` | Server provider, hosted admission and safe committed feed |
| `eventlog-conformance` | Shared applicable provider exercises |

## Read more

- [Persistence guarantees](website/docs/guarantees.md): atomic groups, blobs, retries and capture.
- [Operations](website/docs/operations.md): provisioning, recovery and projections.
- [CHANGELOG.md](CHANGELOG.md): historical release records.
- [AGENTS.md](AGENTS.md): contributor agreements.

<!-- b10x-docs:start -->
## Documentation

[Eventlog documentation](https://beyond10x.github.io/docs/eventlog/) · [Start](https://beyond10x.github.io/) · [Ecosystem](https://beyond10x.github.io/ecosystem/) · [Impact](https://beyond10x.github.io/changes/) · [Releases](https://beyond10x.github.io/releases/)
<!-- b10x-docs:end -->
