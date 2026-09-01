# eventlog

The event-sourcing kit every b10x owner stores durable domain state in. A command produces domain
events, the events are the record, and every read is a fold over them — from a snapshot, from the
log, or from both.

The problem it removes: state tables that are authoritative, and therefore cannot be dropped,
rebuilt, audited or explained. Here they are projections. Tenant isolation is not a permission that
was withheld either — there is no "current tenant" and no `StreamId` constructor that omits one, so
reading another tenant's history is a call that cannot be written.

## Where it sits

Consumed as a **library**, not run as a service. It depends on nothing in `beyond10x`; its
consumers are the owner modules that need durable domain state. See
[atlas](https://github.com/beyond10x/atlas) for where those sit.

Two backends and no third. In-memory is SQLite `:memory:`, which is why a property proved in a test
is proved for the deployment.

## Status

**Unreleased, and the backlog is finished.** Version `0.1.0-dev.1`, `publish = false`, no git tag
cut. All six stories in [`docs/stories/`](docs/stories/README.md) are `done` — the log, aggregates
and snapshots, inline and catch-up projections, schema evolution with golden vectors, erasure and
redaction, and the async store. Everything landed so far is under `## Unreleased` in
[`CHANGELOG.md`](CHANGELOG.md); versions are component-scoped and release under a bare-version
tag — `0.1.0`, the version and nothing else.

## Build, test, run

The gate is **`bash scripts/gate.sh`** — tests, format, clippy and the brand check, in that order.
Green here is the bar for main.

| step | command |
|---|---|
| tests | `cargo test --workspace --locked` |
| format | `cargo fmt --all --check` |
| lint | `cargo clippy --workspace --all-targets --locked -- -D warnings` |
| brand | `bash scripts/check-brand.sh` |

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

## Layout

| crate | owns |
|---|---|
| `crates/eventlog-core` | the envelope, `StreamId`, `Expected`, `EventLogError`, the `EventStore` port; `Aggregate`/`Repository`/`SnapshotPolicy`; `Projector`/`ProjectionSpec`/`CatchUpRunner` |
| `crates/eventlog-sqlite` | `SqliteEventStore` — file and `:memory:`, per-owner table prefixes |
| `crates/eventlog-postgres` | PostgreSQL 13 or later, with a commit watermark so a feed reader cannot skip an event that committed late |
| `crates/eventlog-conformance` | the one exercise both backends must pass |

| path | holds |
|---|---|
| `docs/stories/` | the backlog, one file per story, with a hand-written index |
| `scripts/` | the repository gate and component checks |

## Read more

- [`docs/stories/README.md`](docs/stories/README.md) — the backlog and what each story delivered.
- [`CHANGELOG.md`](CHANGELOG.md) — every capability the kit has, in the order it arrived.
- [`AGENTS.md`](AGENTS.md) — working agreements and the invariants this kit holds.
