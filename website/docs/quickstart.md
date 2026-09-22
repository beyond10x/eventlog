---
title: Quickstart
description: Build the released library and exercise File and SQLite in disposable fixtures.
---

Install Git and Rust **1.91 or newer** before running these commands. Cargo downloads the locked
dependencies on its first run. No database server is needed for these two providers.

## Run the released providers

Choose an empty working directory:

```bash
git clone --branch 0.3.0 --depth 1 https://github.com/beyond10x/eventlog.git
cd eventlog
cargo test --locked -p eventlog-file -- --test-threads=1
cargo test --locked -p eventlog-sqlite -- --test-threads=1
```

Both commands must finish with successful test results. The suites create disposable directories
and databases and exercise the provider contracts, including committed history and refused writes.
This proves the selected local providers; it does not run the PostgreSQL suite.
The commands serialize the released fixtures, including process-lock exercises.

## Add the library

Use the same release tag for every Eventlog crate in a consumer:

```toml
[dependencies]
eventlog-core = { git = "https://github.com/beyond10x/eventlog", tag = "0.3.0" }
eventlog-file = { git = "https://github.com/beyond10x/eventlog", tag = "0.3.0" }
```

The crates are distributed from Git, with `publish = false`. Choose `eventlog-sqlite` or
`eventlog-postgres` instead of `eventlog-file` when that provider fits your deployment.

## Compose an application

1. Open or explicitly provision the provider selected by the host.
2. Register required inline projections before traffic.
3. Construct a `StreamId` from a trusted `TenantId`, stream kind and opaque identifier.
4. Append with the expected revision and stable command metadata.
5. Retain the returned receipt; retry an uncertain result with the original command identity.

The [released conformance examples](https://github.com/beyond10x/eventlog/blob/0.3.0/crates/eventlog-conformance/src/lib.rs)
show event and command construction. Read [guarantees](guarantees.md) before composing multiple
streams or publishing blobs, and [operations](operations.md) before opening a durable store.
