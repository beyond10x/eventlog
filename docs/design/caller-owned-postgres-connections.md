# Caller-owned PostgreSQL connection authority

Status: implementation design for the ESS M2 provider-facade companion.

## Problem

`PostgresConfig::verified` owns URL parsing and constructs a fixed rustls connector with public CA
roots and no client certificate. That is a sound default, but it cannot represent a host that owns
mutual TLS, an HSM-backed connector, a private authentication exchange or other connection setup.
An already connected synchronous `postgres::Client` cannot be reused: this provider is async,
requires one independently driven connection per bounded pool lease, and must own every driver
until quarantine or shutdown completes.

## Typed boundary

`PostgresConnectionAuthority` is a public, object-safe factory. A call receives the pool's exact
connection timeout and returns a future for one `AuthorizedConnection`. The returned value contains
one `tokio_postgres::Client` and its still-unpolled, owned driver future. The constructor accepts the
concrete driver future directly, so a host can use any `MakeTlsConnect` implementation while the
provider does not learn or log its credentials.

The authority declares `PostgresTransportAssurance::Isolated` or `Verified`. This is an explicit
host assertion, matching the trust boundary of accepting a caller-created client. `Verified` means
the authority authenticates the server and rejects plaintext/insecure verification; the provider
still performs its existing database-role, schema, finite role-connection-limit and replica-budget
admission, and holds every later physical session to the database, role and schema that admission
saw. `Isolated` is accepted only by the existing local/migration constructors and remains
ineligible for hosted `open`.

`PostgresConfig::caller_owned(schema, prefix, authority)` validates the schema/prefix and stores
the authority behind `Arc`. Debug remains opaque. Existing `isolated` and `verified` constructors
retain exact behavior and use the same internal connection-source enum.

## Ownership and cancellation

The existing pool remains the sole owner of bounds and lifecycle:

1. its connection and waiter semaphores are acquired before calling the authority;
2. `connect_timeout` bounds the entire authority future even if the authority ignores the supplied
   duration;
3. after establishment the pool immediately spawns and owns the returned driver;
4. the existing session `search_path`, read-only, statement, lock and transaction timeouts are set
   before a lease is returned (whole milliseconds, a fraction rounded up, so a positive timeout
   never becomes PostgreSQL's 0 = none); on a hosted pool the new session must also be the
   admitted database, role and schema and pass the hosted role rules (no DDL powers, finite
   connection limit within the replica share), or it is refused and retired unserved;
5. a cancelled or unsettled lease quarantines its client and capacity permit until its driver has
   stopped; and
6. bounded shutdown closes admission and waits for every idle or checked-out driver retirement.

Server-side cancellation goes through the authority too. The pool has no stream factory for a
caller-owned transport, so when it retires an unsettled lease it calls the authority's `cancel`
with the session's `CancelToken` and the connection timeout, on a thread of its own, and proceeds
whatever the outcome. `cancel` has a default that reports `Unsupported`. With that default a
retired query keeps running server-side, holding its locks, until it finishes or reaches its
statement or lock timeout, unless the server ends it on the closed socket
(`client_connection_check_interval`, PostgreSQL 14 and later on platforms that support it;
PostgreSQL 13 has no such check). The lease's permit returns before that. A host that needs
server work to end with its permit implements `cancel`.

Dropping a not-yet-returned authority future owns and drops its partial connection state. Once an
`AuthorizedConnection` is returned there is no await between taking it and installing its driver
in the pool-owned `Connection`, so cancellation cannot strand an established driver outside pool
accounting. Driver failure closes the paired client and prevents reuse.

The factory creates connections and, when it implements `cancel`, sends cancel requests for
sessions the pool retires. It cannot run SQL through the pool, alter accounting, mark a lease
settled, select a schema/prefix, bypass hosted admission or observe another lease. There is no
callback around transactions or domain operations.

## Compatibility and evidence

No SQL schema, event bytes, receipts, snapshots, projection rows or migrations change. Existing
constructors and public behavior remain source-compatible.

Unit controls prove the authority is invoked only after admission, receives the exact timeout,
cannot exceed connection/waiter bounds, and that timeout/cancellation, driver failure, reuse and
shutdown retain capacity ownership. Actual PostgreSQL proof uses a caller-owned factory to build a
real connection and driver, exercises append/read/reuse, and runs the same hosted role/schema/budget
checks. The existing built-in isolated and verified paths remain in the full provider gate.
