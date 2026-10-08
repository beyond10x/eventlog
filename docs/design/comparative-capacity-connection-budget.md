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

## Statement statistics start empty in each configuration

Each configuration's envelope also requires `pg_stat_statements` to report the same `dealloc` and
the same `stats_reset` in the snapshots taken before and after it. A changed `dealloc` means the
extension evicted entries during the configuration, so the statement timings in the receipt no
longer cover all of its statements.

The counters are cumulative for the whole server. In the required lane the production gate runs
first on the same server and fills the table towards `pg_stat_statements.max` (5,000), so an
eviction lands inside whichever configuration crosses the limit, whatever that configuration did.
CI run 37772785925 failed that way on `baseline-8-hot` (`dealloc` 1 to 2); on `main` the same step
once fell between two configurations and passed.

So the sweep has the metrics collector reset `pg_stat_statements` immediately before each
configuration's opening snapshot, on the snapshot's own connection (`reset_statements` in
`examples/observation/metrics_collector.rs`). Every configuration starts from an empty table and
can only see an eviction by registering more than `pg_stat_statements.max` distinct statements
itself. The reset lives in the collector, not the sweep, because the collector owns the snapshot
the envelope reads: a refused reset (no privilege, no extension) or a timed-out one turns that
snapshot into `unavailable` with `statement_reset_failed` or `statement_reset_timeout`, and the
existing `statements_before` status check fails the configuration visibly. Both equality checks
stay: nothing inside a configuration resets the table again. The standalone `capacity-metrics`
smoke does not reset.

Reproduced locally by filling the table to 4,991 entries and registering a new distinct statement
every 25 ms during one comparative proof: without the reset 8 of 12 configurations were invalid
on `dealloc` alone; with it every configuration read 0 before and after, and all 12 were valid.
`tests/capacity_budget.rs` holds a resetting collection to a fresh `stats_reset` and zero
deallocations, and a role without the reset privilege to an unavailable snapshot.
