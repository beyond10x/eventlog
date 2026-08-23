# daemonloom/eventlog

The persistence kit every Daemonloom owner stores durable domain state in.

A command produces domain events, the events are the record, and every read is a fold over them —
from a snapshot, from the log, or from both. State tables stop being authoritative and become
projections that can be dropped and rebuilt.

Accepted by
[ADR 0055](https://github.com/daemonloom/daemonloom/blob/e01ea676da18fb855814e7621514e0c98fc57c2c/architecture/adr/0055-durable-domain-state-is-a-fold-over-an-event-log.md).
The design, including the normative physical schema, is
[RFC 0020](https://github.com/daemonloom/daemonloom/blob/e01ea676da18fb855814e7621514e0c98fc57c2c/architecture/rfcs/0020-state-is-a-fold-over-an-event-log.md).

## Crates

| Crate | Owns |
|---|---|
| `eventlog-core` | the envelope, `StreamId`, `Expected`, the error taxonomy, the `EventStore` port |
| `eventlog-sqlite` | SQLite, file and `:memory:` |
| `eventlog-postgres` | PostgreSQL 13 or later |
| `eventlog-conformance` | the one exercise both backends must pass |

There are two backends and no third. In-memory is SQLite `:memory:`, which is why a property proved
in a test is proved for the deployment.

## Gate

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

The PostgreSQL exercise runs only when it is given a database, and reports itself as skipped
otherwise:

```bash
docker run --rm -d --name eventlog-test-pg \
  -e POSTGRES_PASSWORD="$(head -c 18 /dev/urandom | base64 | tr -d '/+=')" \
  -p 127.0.0.1:55999:5432 postgres:17.6-alpine3.22
EVENTLOG_TEST_POSTGRES_URL=postgresql://postgres:<password>@127.0.0.1:55999/postgres cargo test
docker rm -f eventlog-test-pg
```

## Backlog

[`docs/stories/`](docs/stories/README.md). The module conversions that consume this kit are
[E-006](https://github.com/daemonloom/daemonloom/blob/e01ea676da18fb855814e7621514e0c98fc57c2c/model/modules/docs/epics/E-006-every-module-persists-as-events.md).
