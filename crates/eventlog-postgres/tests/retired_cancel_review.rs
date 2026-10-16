//! Security review of `story:pg-retired-connection-closes-on-drop` (a871d43): conformance checks
//! of the retirement close, the server cancel and the session configuration against the
//! documented contract.
//!
//! Every case except `session_configuration_admits_a_postgresql_13_server` needs
//! `EVENTLOG_TEST_POSTGRES_URL` (a plain loopback fixture); without it each reports that it did
//! not run.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use eventlog_conformance::{TALLY, Tally};
use eventlog_core::{
    BoxFuture, EventLogError, EventStore, Expected, Guard, ProjectionStore, StreamId, TenantId,
};
use eventlog_postgres::{
    AuthorizedConnection, PoolOptions, PostgresConfig, PostgresConnectionAuthority,
    PostgresEventStore, PostgresTransportAssurance,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::NoTls;

fn url() -> Option<String> {
    let value = std::env::var("EVENTLOG_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    assert!(
        value.is_some() || std::env::var_os("EVENTLOG_REQUIRE_POSTGRES").is_none(),
        "required PostgreSQL proof cannot skip an absent database URL"
    );
    value
}

fn with_application(url: &str, application: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}application_name={application}")
}

async fn connect(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("an independent connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn backends(observer: &tokio_postgres::Client, application: &str) -> Vec<i32> {
    observer
        .query(
            "SELECT pid FROM pg_stat_activity WHERE application_name = $1 ORDER BY pid",
            &[&application],
        )
        .await
        .expect("pg_stat_activity")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

async fn fresh(observer: &tokio_postgres::Client, prefix: &str) {
    for suffix in [
        "events",
        "append_groups",
        "commands",
        "claims",
        "identity",
        "snapshots",
        "snapshot_generations",
        "projection_cursors",
        "blobs",
        "scope_counters",
        "schema_version",
        "projection_registry",
        "p_tally",
    ] {
        observer
            .batch_execute(&format!("DROP TABLE IF EXISTS {prefix}_{suffix}"))
            .await
            .expect("fresh test-owned tables");
    }
}

fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller runtime")
}

/// Keeps the append's transaction busy past its deadline, so the lease is dropped unsettled
/// while its client is still open.
struct BusyPastDeadline {
    tenant: TenantId,
}

impl Guard for BusyPastDeadline {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            loop {
                store.get(&TALLY, &self.tenant, "item/held").await?;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }
}

async fn seeded_store(url: &str, prefix: &str, options: PoolOptions) -> PostgresEventStore {
    let store = PostgresEventStore::connect_local(url, prefix, options)
        .await
        .expect("an isolated store");
    store.register_inline(Arc::new(Tally)).await.unwrap();
    store.seal().await;
    let tenant = TenantId::new("review").unwrap();
    store
        .append(
            &StreamId::new(tenant, "item", "held").unwrap(),
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("seed", &serde_json::json!({})),
        )
        .await
        .expect("the seeded tally row");
    store
}

async fn time_out_one_append(store: &PostgresEventStore, stream: &str) {
    let tenant = TenantId::new("review").unwrap();
    let outcome = store
        .append_guarded(
            &StreamId::new(tenant.clone(), "item", stream).unwrap(),
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 2)],
            &eventlog_conformance::meta(stream, &serde_json::json!({})),
            Arc::new(BusyPastDeadline { tenant }),
        )
        .await;
    assert!(
        matches!(
            outcome,
            Err(EventLogError::Deadline { .. } | EventLogError::UnknownCommit)
        ),
        "the append times out: {outcome:?}"
    );
}

/// A loopback TCP relay to the fixture that counts every connection opened through it: the
/// pool's sessions and its cancel requests alike.
async fn counting_proxy(target: SocketAddr) -> (u16, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&accepted);
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                return;
            };
            counted.fetch_add(1, Ordering::AcqRel);
            tokio::spawn(async move {
                // Without nodelay, Nagle and delayed acknowledgement add ~40 ms to a relayed
                // round trip, and schema admission's sequential catalog queries then outrun
                // the store deadline.
                let _ = inbound.set_nodelay(true);
                if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                    let _ = outbound.set_nodelay(true);
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
    (port, accepted)
}

/// The fixture URL rewritten to pass through a relay on `port`.
fn through(url: &str, port: u16, application: &str) -> (String, SocketAddr) {
    let config: tokio_postgres::Config = url.parse().unwrap();
    let host = match &config.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(host) => host.clone(),
        #[cfg(unix)]
        tokio_postgres::config::Host::Unix(_) => panic!("the relay needs a TCP fixture"),
    };
    let target: SocketAddr = format!("{host}:{}", config.get_ports()[0]).parse().unwrap();
    let user = config.get_user().unwrap_or("postgres");
    let password = config
        .get_password()
        .map(|password| format!(":{}", String::from_utf8_lossy(password)))
        .unwrap_or_default();
    let database = config.get_dbname().unwrap_or("postgres");
    (
        format!(
            "postgres://{user}{password}@127.0.0.1:{port}/{database}?application_name={application}"
        ),
        target,
    )
}

/// Invariant 6 and 3: a settled lease and a shutdown-retired idle connection send no cancel; an
/// unsettled lease sends exactly one.
#[test]
fn only_an_unsettled_retirement_sends_a_cancel_and_only_one() {
    const PREFIX: &str = "rcr_cancel_count";
    let Some(url) = url() else {
        eprintln!("skipped: cancel count requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let observer = connect(&url).await;
        fresh(&observer, PREFIX).await;

        // Settled traffic, then shutdown: one session, no cancel connection.
        let (port, accepted) = counting_proxy(through(&url, 0, "x").1).await;
        let (relayed, _) = through(&url, port, "rcr_settled");
        let store = seeded_store(&relayed, PREFIX, PoolOptions::default()).await;
        store.shutdown().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            accepted.load(Ordering::Acquire),
            1,
            "a settled lease and a shutdown-retired idle connection open no cancel connection"
        );

        // One unsettled retirement: its session plus exactly one cancel connection.
        let (port, accepted) = counting_proxy(through(&url, 0, "x").1).await;
        let (relayed, _) = through(&url, port, "rcr_unsettled");
        let store = seeded_store(
            &relayed,
            PREFIX,
            PoolOptions {
                max_connections: 1,
                transaction_timeout: Duration::from_secs(3),
                ..PoolOptions::default()
            },
        )
        .await;
        assert_eq!(accepted.load(Ordering::Acquire), 1);
        time_out_one_append(&store, "unsettled").await;
        let started = Instant::now();
        while store.pool_status().checked_out != 0 && started.elapsed() < Duration::from_secs(5) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            store.pool_status().checked_out,
            0,
            "the retirement completes"
        );
        assert_eq!(store.pool_churn().retired_unsettled_lease, 1);
        assert_eq!(
            accepted.load(Ordering::Acquire),
            2,
            "one session and exactly one cancel request"
        );
        store.shutdown().await.unwrap();
    });
}

/// Invariant 1: a retirement whose spawned close task is dropped with the caller's runtime
/// (a caller that builds a runtime per call) must still leave the pool able to account for it;
/// otherwise `checked_out` stays nonzero forever and every later `shutdown` misses its deadline.
#[test]
fn a_retirement_dropped_with_its_callers_runtime_does_not_strand_occupancy() {
    const PREFIX: &str = "rcr_runtime_drop";
    let Some(url) = url() else {
        eprintln!("skipped: runtime-drop accounting requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let observing = tokio::runtime::Runtime::new().unwrap();
    observing.block_on(async {
        let observer = connect(&url).await;
        fresh(&observer, PREFIX).await;
    });
    let first = current_thread();
    let store = first.block_on(async {
        let store = seeded_store(
            &with_application(&url, "rcr_runtime_drop"),
            PREFIX,
            PoolOptions {
                max_connections: 1,
                transaction_timeout: Duration::from_secs(1),
                shutdown_timeout: Duration::from_secs(3),
                ..PoolOptions::default()
            },
        )
        .await;
        time_out_one_append(&store, "dropped").await;
        store
    });
    // The per-call runtime ends, and every task spawned on it with it.
    drop(first);
    let second = current_thread();
    let (status, shutdown) = second.block_on(async {
        let started = Instant::now();
        while store.pool_status().checked_out != 0 && started.elapsed() < Duration::from_secs(3) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (store.pool_status(), store.shutdown().await)
    });
    eprintln!("after the caller's runtime dropped: {status:?}, shutdown={shutdown:?}");
    assert_eq!(
        status.checked_out, 0,
        "the retired lease's occupancy is never released once its close task is dropped"
    );
    assert!(
        shutdown.is_ok(),
        "shutdown misses its deadline: {shutdown:?}"
    );
}

struct PlainAuthority {
    config: tokio_postgres::Config,
}

impl PostgresConnectionAuthority for PlainAuthority {
    fn assurance(&self) -> PostgresTransportAssurance {
        PostgresTransportAssurance::Isolated
    }
    fn connect(
        &self,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<AuthorizedConnection, EventLogError>> {
        let mut config = self.config.clone();
        config.connect_timeout(timeout);
        Box::pin(async move {
            let (client, driver) = config
                .connect(NoTls)
                .await
                .map_err(|_| EventLogError::Backend("caller-owned connection failed".into()))?;
            Ok(AuthorizedConnection::new(client, driver))
        })
    }
}

/// Story outcome on the caller-owned path, which has no cancel request: a lease blocked on a
/// lock and dropped on a current-thread runtime that is never polled again still ends its
/// backend, through the client-connection check, well before the server limits.
#[test]
fn caller_owned_blocked_backend_ends_after_its_lease_drops() {
    const PREFIX: &str = "rcr_caller_owned";
    const APPLICATION: &str = "rcr_caller_owned";
    let Some(url) = url() else {
        eprintln!("skipped: caller-owned close requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let observing = tokio::runtime::Runtime::new().unwrap();
    let observer = observing.block_on(async {
        let observer = connect(&url).await;
        fresh(&observer, PREFIX).await;
        observer
    });
    let caller = current_thread();
    let config: tokio_postgres::Config = with_application(&url, APPLICATION).parse().unwrap();
    let store = caller.block_on(async {
        let store = PostgresEventStore::local(
            PostgresConfig::caller_owned("public", PREFIX, Arc::new(PlainAuthority { config }))
                .unwrap(),
            PoolOptions {
                max_connections: 1,
                transaction_timeout: Duration::from_secs(2),
                statement_timeout: Duration::from_secs(60),
                lock_timeout: Duration::from_secs(60),
                ..PoolOptions::default()
            },
        )
        .await
        .expect("a caller-owned isolated store");
        store.seal().await;
        store
    });
    let pid = observing.block_on(backends(&observer, APPLICATION));
    assert_eq!(pid.len(), 1, "one pooled session: {pid:?}");
    let holder = observing.block_on(async {
        let holder = connect(&url).await;
        holder
            .batch_execute(&format!(
                "BEGIN; LOCK TABLE {PREFIX}_events IN EXCLUSIVE MODE"
            ))
            .await
            .unwrap();
        holder
    });
    let outcome = caller.block_on(store.append(
        &StreamId::new(TenantId::new("review").unwrap(), "item", "blocked").unwrap(),
        Expected::NoStream,
        &[eventlog_conformance::event("item.received", 1)],
        &eventlog_conformance::meta("blocked", &serde_json::json!({})),
    ));
    let returned = Instant::now();
    assert!(
        matches!(
            outcome,
            Err(EventLogError::Deadline { .. } | EventLogError::UnknownCommit)
        ),
        "the append times out while blocked: {outcome:?}"
    );
    let remaining = observing.block_on(async {
        let mut remaining = backends(&observer, APPLICATION).await;
        while remaining.contains(&pid[0]) && returned.elapsed() < Duration::from_secs(3) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            remaining = backends(&observer, APPLICATION).await;
        }
        remaining
    });
    let elapsed = returned.elapsed();
    observing.block_on(async { holder.batch_execute("ROLLBACK").await.unwrap() });
    assert!(
        !remaining.contains(&pid[0]),
        "the caller-owned blocked backend outlived its dropped lease by {elapsed:?}"
    );
    drop(store);
    drop(caller);
}

/// Invariant 7: the new session setting needs no privilege the restricted application role
/// lacks (`user` context), so it cannot fail hosted admission on a supported server.
#[test]
fn client_connection_check_interval_needs_no_privilege() {
    let Some(url) = url() else {
        eprintln!("skipped: setting context requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let context: String = runtime.block_on(async {
        connect(&url)
            .await
            .query_one(
                "SELECT context FROM pg_settings WHERE name = 'client_connection_check_interval'",
                &[],
            )
            .await
            .unwrap()
            .get(0)
    });
    assert_eq!(context, "user");
}

/// Invariant 7: `lib.rs` documents PostgreSQL 13 or later. A PostgreSQL 13 server answers
/// `SET client_connection_check_interval` with SQLSTATE 42704 (unrecognized configuration
/// parameter), as a 14+ server on a platform without closed-socket events answers a nonzero
/// value with 22023. This minimal protocol fixture answers exactly that and accepts every other
/// session `SET`; a session must still be admitted.
#[test]
fn session_configuration_admits_a_postgresql_13_server() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (outcome, statements) = runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let statements = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&statements);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let _ = postgres_13_session(socket, seen).await;
                });
            }
        });
        let outcome = PostgresEventStore::connect_local(
            &format!("postgres://review@127.0.0.1:{port}/review"),
            "rcr_pgthirteen",
            PoolOptions::default(),
        )
        .await;
        (outcome.err(), statements.load(Ordering::Acquire))
    });
    eprintln!("PostgreSQL 13 fixture: error={outcome:?}, statements={statements}");
    assert!(
        statements > 1,
        "no statement after session configuration reached the server: the session was refused \
         ({outcome:?})"
    );
}

/// Connects through a fixture that refuses the check-interval setting with `code`, and returns
/// the store's refusal. The fixture refuses everything after session setup with XX000, so an
/// admitted session ends there and a refused one ends on `code`.
fn connect_refusing_check_interval(code: &'static str) -> EventLogError {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let seen = Arc::new(AtomicUsize::new(0));
                    let _ = refusing_check_interval(socket, seen, code).await;
                });
            }
        });
        PostgresEventStore::connect_local(
            &format!("postgres://review@127.0.0.1:{port}/review"),
            "rcr_check_interval",
            PoolOptions::default(),
        )
        .await
        .err()
        .expect("the fixture refuses everything after session setup")
    })
}

/// Only "unknown parameter" (42704) and "invalid value" (22023) mean the server cannot check
/// its client socket; with either, the session is admitted and reaches its first statement.
#[test]
fn session_configuration_admits_both_unsupported_check_interval_answers() {
    for code in ["42704", "22023"] {
        let refusal = connect_refusing_check_interval(code);
        assert!(
            matches!(&refusal, EventLogError::Backend(message) if message.contains("SQLSTATE XX000")),
            "a session whose server answers {code} is admitted: {refusal:?}"
        );
    }
}

/// Any other refusal of the check-interval setting still fails the session.
#[test]
fn session_configuration_refuses_any_other_check_interval_error() {
    let refusal = connect_refusing_check_interval("42501");
    assert!(
        matches!(&refusal, EventLogError::Backend(message) if message.contains("SQLSTATE 42501")),
        "a session whose server answers 42501 is refused on that error: {refusal:?}"
    );
}

fn error_response(code: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (field, value) in [
        (b'S', "ERROR"),
        (b'V', "ERROR"),
        (b'C', code),
        (b'M', message),
    ] {
        body.push(field);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    message_bytes(b'E', &body)
}

fn message_bytes(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Serves one session the way a PostgreSQL 13 server answers this kit's session setup; every
/// statement after it is refused with a generic error, which is enough to show admission.
async fn postgres_13_session(
    socket: tokio::net::TcpStream,
    seen: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    refusing_check_interval(socket, seen, "42704").await
}

/// The PostgreSQL 13 fixture, with `SET client_connection_check_interval` refused by `code`.
async fn refusing_check_interval(
    mut socket: tokio::net::TcpStream,
    seen: Arc<AtomicUsize>,
    code: &'static str,
) -> std::io::Result<()> {
    let length = socket.read_i32().await?;
    let mut startup = vec![0; usize::try_from(length - 4).unwrap()];
    socket.read_exact(&mut startup).await?;
    let mut reply = message_bytes(b'R', &0i32.to_be_bytes());
    reply.extend(message_bytes(b'S', b"server_version\x0013.16\x00"));
    reply.extend(message_bytes(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
    reply.extend(message_bytes(b'Z', b"I"));
    socket.write_all(&reply).await?;
    loop {
        let tag = socket.read_u8().await?;
        let length = socket.read_i32().await?;
        let mut body = vec![0; usize::try_from(length - 4).unwrap()];
        socket.read_exact(&mut body).await?;
        let reply = match tag {
            b'Q' => {
                seen.fetch_add(1, Ordering::AcqRel);
                let text = String::from_utf8_lossy(&body);
                let mut reply = if text.contains("client_connection_check_interval") {
                    error_response(
                        code,
                        "client_connection_check_interval refused by the fixture",
                    )
                } else if text.starts_with("SET search_path") {
                    message_bytes(b'C', b"SET\x00")
                } else {
                    error_response("XX000", "fixture answers only session setup")
                };
                reply.extend(message_bytes(b'Z', b"I"));
                reply
            }
            b'S' => {
                seen.fetch_add(1, Ordering::AcqRel);
                let mut reply = error_response("XX000", "fixture answers only session setup");
                reply.extend(message_bytes(b'Z', b"I"));
                reply
            }
            b'X' => return Ok(()),
            _ => continue,
        };
        socket.write_all(&reply).await?;
    }
}
