# eventlog

The event-sourcing kit every b10x owner stores durable domain state in. A command produces domain
events, the events are the record, and every read is a fold over them — from a snapshot, from the
log, or from both.

The problem it removes: state tables that are authoritative, and therefore cannot be dropped,
rebuilt, audited or explained. Here they are projections. Every stream coordinate includes an explicit tenant. Projection callbacks are confined to the
current transaction's tenant; the owning host derives that tenant from current authority before
calling the store. Eventlog does not authenticate a caller or grant domain access.

## Where it sits

Consumed as a **library**, not run as a service. It depends on nothing in `beyond10x`; its
consumers are the owner modules that need durable domain state. See
[the public architecture map](https://beyond10x.github.io/architecture/) for where those sit.

Two backends and no third. In-memory is SQLite `:memory:`, which is why a property proved in a test
is proved for the deployment.

## Status

**Unreleased.** Version `0.1.0-dev.1`, `publish = false`, no git tag
cut. The six foundation stories in [`docs/stories/`](docs/stories/README.md) are `done` — the log, aggregates
and snapshots, inline and catch-up projections, schema evolution with golden vectors, erasure and
redaction, and the async store. Everything landed so far is under `## Unreleased` in
[`CHANGELOG.md`](CHANGELOG.md); versions are component-scoped and release under a bare-version
tag — `0.1.0`, the version and nothing else.

## Build, test, run

The gate is **`bash scripts/gate.sh`** — tests, format and clippy, in that order.
Green here is the bar for main.

| step | command |
|---|---|
| tests | `cargo test --workspace --locked` |
| format | `cargo fmt --all --check` |
| lint | `cargo clippy --workspace --all-targets --locked -- -D warnings` |

Rust 1.91, edition 2024, `unsafe_code = "forbid"`.

The PostgreSQL exercise runs only when it is given a database, and reports itself as skipped
otherwise:

```bash
docker run --rm -d --name eventlog-test-pg \
  -e POSTGRES_PASSWORD="$(head -c 18 /dev/urandom | base64 | tr -d '/+=')" \
  -p 127.0.0.1:55999:5432 postgres:17.6-alpine3.22
EVENTLOG_TEST_POSTGRES_URL=postgresql://postgres:<password>@127.0.0.1:55999/postgres cargo test
docker rm -f eventlog-test-pg
```

## Hosted PostgreSQL composition

`PostgresEventStore::connect` remains the isolated local convenience constructor. It accepts only
loopback, localhost or Unix sockets and explicitly disables TLS. Hosted composition uses
`PostgresConfig::verified(url, schema, prefix, roots)` with nonempty trusted roots; the connection
verifies both certificate chain and server name. Credentials stay in the host's configuration.

Run `PostgresEventStore::migrate(config, options, projections)` with the migration role before
opening traffic. It serializes additive migrations, checks the complete old physical shape and
records schema version/checksum plus the exact projection roster. Supply every legacy projection's
name and indexed paths explicitly. Unknown, partial, altered, unlogged, RLS/policy/trigger or
foreign-sequence or inherited-table shapes refuse admission. Event sequence CACHE 1, positive unit increment and
non-cycling BIGSERIAL range are part of admission; column collations must match and be deterministic. Existing event envelopes and the committed-transaction
feed watermark predicate stays unchanged. Readers also stop before the first position withheld
by that predicate: an unrelated transaction can hold xmin between already committed append XIDs,
so filtering individual rows alone could skip a lower position. A separate transaction publication gate prevents a
reader from advancing past an in-flight lower position when transaction-id and position order
differ: append/redaction/erasure hold a shared owner gate; feed/catch-up/rebuild take it exclusively
before a fresh READ COMMITTED query. Connections force that isolation even if inherited URL/role
options differ. All active writers and feed/fold readers must use this protocol; deployment cutover
fences older binaries. Registration compares the persisted roster and physical shape.

Use a separate dedicated application login for `PostgresEventStore::open`. Its schema grants are
USAGE without CREATE; its durable table grants are SELECT/INSERT/UPDATE/DELETE, sequence grants
include USAGE, and its database grants include TEMP for validating/rebuilding projections. It owns
no durable relation or sequence in that schema, belongs to no other role, and has no superuser,
role/database creation, replication or RLS-bypass privilege. This deliberately narrow role profile
also excludes inherited and SET ROLE paths to DDL. Give it a finite CONNECTION LIMIT no greater
than its total replica pool share. An owner must reserve migration, observation and other server
connections separately: `replicas * max_connections + reserved_connections <= database_connections`.

`PoolOptions` defaults to four connections, 32 waiting acquisitions, two-second acquisition and
connection deadlines, five-second statements, two-second locks, ten-second operations and
five-second shutdown. Configure these against the deployment budget. `pool_status()` reports the
current local bounds and occupancy. Saturation returns `Overloaded`; bounded acquisition returns
`Deadline`; shutdown rejects new work with `Closed`. A cancelled or unsettled transaction discards
its connection and retains local permits until its driver has stopped. PostgreSQL's dedicated-role
connection limit also bounds server admission while disconnected sessions settle.

A lost commit response returns `UnknownCommit`. Resolve it by retrying the same stream, command key
and canonical request hash, or querying the owning command claim. The exact original committed
receipt and persisted envelopes are returned on retry. Register projections before `seal()` or the
first append; registration after that point refuses. Catch-up workers serialize by exact owner,
projection and tenant. Rebuild folds into temporary shadow tables and commits only the selected
tenant's replacement and cursor together; readers retain the prior view on failure.

SQLite tenant erasure also discovers projection tables retained by older files without a
projection registry. It matches the literal owner namespace and verifies the legacy table shape.
Ambiguous overlapping namespaces or unsupported matching tables refuse the entire erasure and
roll back its event, receipt and projection deletions; the caller must resolve that ownership
ambiguity before retrying.

## Atomic storage admission

The concrete store issues an `AdmissionPermit` to the trusted owner host. Keep it out of caller
input and domain projector code. A matching append guard can call `ProjectionStore::reserve` with
up to 64 `Reservation` values. Tenant scopes carry an exact `TenantId` plus owner-selected key;
deployment scopes have their own variant and never impersonate a tenant. Coordinates use byte
length prefixes, so arbitrary tenant/service boundaries do not collide. Locks are ordered by
encoded coordinate even for absent counters; all deltas and ceilings pass before any counter
changes, in the event/receipt/claim transaction. Caught refusal rolls back its savepoint; a dropped
reservation future poisons the append. Exact command retry never charges or releases twice.

SQLite provides the same contract with its immediate write transaction. Both backends confine
projector reads and writes to their current tenant and deny reservation access to projector
callbacks. The permit grants a storage operation, not authorization to another tenant. Domain
policy, installation lifecycle membership, current authority and namespace bindings remain with
the owning service. The internal stored shapes have an ESS home under `ess/admission/`.

## Guard refusals and effect metadata

A guard can return `EventLogError::GuardRefused { code }` with an owner-defined stable code.
Both adapters preserve the code and roll back the entire refused append, including writes made
through the guard's transactional projection view.

`EffectStage`, `EffectEvidence` and `EffectBoundaryCoverage` describe generic external-effect
evidence using existing command attribution. Owners explicitly validate this metadata before
embedding it in their events. Identifiers, references, outcome codes and inventory boundary names
must be bounded opaque values; names and addresses are refused. Eventlog does not interpret
owner event bodies or decide effect policy. The semantic vocabulary lives in `ess/effects/`;
the five Rust serde stage forms are tested directly because ESS cannot currently project their
exact internally tagged wire layout.

## Required proof and comparative laboratory

`bash scripts/gate.sh --production-proof` refuses missing URL, hosted-role URL or test CA, a selected
zero lane, ignored tests or a backend skip. Set `EVENTLOG_TEST_POSTGRES_URL`,
`EVENTLOG_TEST_HOSTED_POSTGRES_URL` and `EVENTLOG_TEST_POSTGRES_CA` only for a disposable fixture.
`EVENTLOG_PROOF_REPORT` and `EVENTLOG_PROOF_RAW` retain machine-readable identity/count/server
metadata and exact runner output. The read-only Persistence proof workflow provisions a real
PostgreSQL server and a locally generated TLS CA, then runs this gate and the required comparative/restart envelope. The `comparative-proof` entry
verifies the exact original adapter revision, preserves its runtime source, records source and
binary identities, and refuses absent or failed capacity/recovery receipts. No private test key is an
artifact. `production-fixture --help` describes the explicit disposable-container TLS setup.

The Rust `capacity` worker uses the same source with original and candidate adapters. Its
`capacity-sweep` supervisor records aggregate concurrency 1/8/32 across two processes, uniform
streams and a shared admission guard, warmup and steady samples, exact latency arrays, safe-feed
and projection delay, and raw database/cgroup observations. At aggregate one the two processes run
sequentially. `recovery` and `recovery-sweep` compare exact retained envelopes, retry receipts and
partial projection replay across an explicitly named disposable server restart. Run each example
with `--help` for its argv contract; never point restart fixtures at a deployed owner database.

A conformance receipt does not approve production capacity. A capacity decision requires the
owner's predeclared workload, arrival model, resource/queue/latency/lag/recovery budgets and the
corresponding comparative observations. Internal queue/transaction upper bounds, sampled lock
occupancy and cumulative statement time must keep their actual meanings in that report. SDK and
service fixtures separately prove current authority and old-reader namespace representations.

## Layout

| crate | owns |
|---|---|
| `crates/eventlog-core` | the envelope, `StreamId`, `Expected`, `EventLogError`, the `EventStore` port; `Aggregate`/`Repository`/`SnapshotPolicy`; `Projector`/`ProjectionSpec`/`CatchUpRunner` |
| `crates/eventlog-sqlite` | `SqliteEventStore` — file and `:memory:`, per-owner table prefixes |
| `crates/eventlog-postgres` | PostgreSQL (required proof runs on 17.6), with a commit watermark so a feed reader cannot skip an event that committed late |
| `crates/eventlog-conformance` | the one exercise both backends must pass |

| path | holds |
|---|---|
| `.engineering/planning/` | governed stories, review findings and implementation evidence |
| `docs/stories/` | historical foundation stories with backlinks to current planning records |
| `scripts/` | the repository gate and component checks |

## Read more

- [`docs/stories/README.md`](docs/stories/README.md) — the historical foundation stories and what each delivered.
- [`CHANGELOG.md`](CHANGELOG.md) — every capability the kit has, in the order it arrived.
- [`AGENTS.md`](AGENTS.md) — working agreements and the invariants this kit holds.

<!-- b10x-docs:start -->
## Documentation

[Eventlog documentation](https://beyond10x.github.io/docs/eventlog/) · [Start](https://beyond10x.github.io/) · [Ecosystem](https://beyond10x.github.io/ecosystem/) · [Impact](https://beyond10x.github.io/changes/) · [Releases](https://beyond10x.github.io/releases/)
<!-- b10x-docs:end -->
