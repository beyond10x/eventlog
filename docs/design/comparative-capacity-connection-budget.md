# The comparative capacity lane budgets the workload's own connections

Owner: story:capacity-proof-counts-a-ninth-connection. Serves O2, decisions with checkable evidence.

## What the budget counts

The comparative capacity sweep (`crates/eventlog-postgres/examples/capacity-sweep.rs`) runs each
configuration as two `capacity` worker processes. Each worker owns one store with
`PoolOptions::default()`, four connections, so the workload can hold at most eight. That is the
budget, and it is not changed.

The sweep starts every worker of both adapters with a connection string carrying
`application_name=eventlog-capacity-workload`. The original adapter passes the string to
`tokio_postgres::connect`, the candidate's `PostgresConfig::isolated` parses it into its pool
configuration; both keep the parameter on every connection they open
(`tests/capacity_budget.rs` holds the candidate to it).

The metrics collector reports, per 20 ms sample and as a maximum over the configuration:

| Field | Counts |
|---|---|
| `workload_connections` / `workload_connections_max` | client backends carrying the workload's `application_name` |
| `connections` / `connections_max_excluding_observer` | every client backend except the collector's own |

The `<= 8` check reads `workload_connections_max`. A maximum of zero fails it as well: it means the
tag never reached the server, and a count of nothing proves nothing. The all-backend count stays in
the receipt, and a sample over eight on it still records each counted backend's `pid`,
`backend_start`, `state`, `state_change` and `xact_start`, so a bystander remains visible. The
application name is compared on the server and is never written to the receipt.

## Why

The required lane's PostgreSQL service has a health check, `pg_isready -U postgres` every five
seconds (`.github/workflows/persistence-proof.yml`). Each probe opens a short-lived client backend
that the all-backend count includes. A probe that lands inside a sample made nine against eight,
which failed the lane intermittently while both pools reported no retirement and no replacement.
Reproduced locally by running `pg_isready` every 100 ms during one comparative proof: the extra
backend started 5 s after the eight pool backends, was idle a millisecond later and was absent
from the next sample. Without the probe loop, 11 runs never sampled more than eight.

A workload that opens a ninth connection still fails: `tests/capacity_budget.rs` holds nine
tagged backends against the budget and requires the check to refuse them.
