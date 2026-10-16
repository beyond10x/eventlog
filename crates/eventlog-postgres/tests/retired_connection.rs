//! A retired connection closes when its lease drops, whether or not the caller's runtime is
//! ever polled again (`eventlog.pool.PoolChurn`, `eventlog.pool.TimedOutWrite`).
//!
//! Set `EVENTLOG_TEST_POSTGRES_URL` to run these. Without it each case reports that it did not run.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use eventlog_conformance::{TALLY, Tally};
use eventlog_core::{
    BoxFuture, EventLogError, EventStore, Expected, Guard, ProjectionStore, StreamId, TenantId,
};
use eventlog_postgres::{PoolOptions, PostgresEventStore};
use tokio_postgres::NoTls;

/// How long an abandoned backend may outlive the timed-out call: far shorter than the server's
/// own idle-in-transaction, lock and statement timeouts, which are what would otherwise end it.
const CLOSE_BOUND: Duration = Duration::from_secs(1);

/// Locks a tally row for the rest of the append's transaction, then keeps the session busy until
/// the store's deadline cancels it. The repeated reads keep the server's idle-in-transaction timer
/// from firing while the guard runs, so only a closed transport can end the session early.
struct HoldsRowPastDeadline {
    tenant: TenantId,
}

impl Guard for HoldsRowPastDeadline {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            store
                .get_for_update(&TALLY, &self.tenant, "item/held")
                .await?
                .expect("the seeded tally row");
            loop {
                store.get(&TALLY, &self.tenant, "item/held").await?;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }
}

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

async fn advisory_locks(observer: &tokio_postgres::Client, pid: i32) -> i64 {
    observer
        .query_one(
            "SELECT count(*) FROM pg_locks WHERE pid = $1 AND locktype = 'advisory'",
            &[&pid],
        )
        .await
        .expect("pg_locks")
        .get(0)
}

/// Polls until `pid` leaves `pg_stat_activity` or `CLOSE_BOUND` has passed since `returned`.
async fn await_gone(
    observer: &tokio_postgres::Client,
    application: &str,
    pid: i32,
    returned: Instant,
) -> Vec<i32> {
    let mut remaining = backends(observer, application).await;
    while remaining.contains(&pid) && returned.elapsed() < CLOSE_BOUND {
        tokio::time::sleep(Duration::from_millis(20)).await;
        remaining = backends(observer, application).await;
    }
    remaining
}

/// Drops every table this kit could have created under `prefix`, and checks that no stale
/// session carries the case's application name.
async fn fresh(observer: &tokio_postgres::Client, prefix: &str, application: &str) {
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
    assert_eq!(
        backends(observer, application).await,
        Vec::<i32>::new(),
        "no stale session carries this case's application name"
    );
}

fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller runtime")
}

#[test]
fn timed_out_append_closes_its_connection_without_polling_the_callers_runtime_again() {
    const PREFIX: &str = "retired_close_drop";
    const APPLICATION: &str = "eventlog_retired_close_drop";
    // The operation's own deadline, which the pool also sets as the session's
    // `idle_in_transaction_session_timeout`.
    const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(4);
    let Some(url) = url() else {
        eprintln!("skipped: retired-connection close requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let observing = tokio::runtime::Runtime::new().expect("observer runtime");
    let observer = observing.block_on(async {
        let observer = connect(&url).await;
        fresh(&observer, PREFIX, APPLICATION).await;
        observer
    });

    // The caller's runtime: one thread, driven only while the caller blocks on it.
    let caller = current_thread();
    let tenant = TenantId::new("retired-close").unwrap();
    let store = caller.block_on(async {
        let store = PostgresEventStore::connect_local(
            &with_application(&url, APPLICATION),
            PREFIX,
            PoolOptions {
                max_connections: 1,
                transaction_timeout: TRANSACTION_TIMEOUT,
                ..PoolOptions::default()
            },
        )
        .await
        .expect("an isolated store");
        store.register_inline(Arc::new(Tally)).await.unwrap();
        store.seal().await;
        store
            .append(
                &StreamId::new(tenant.clone(), "item", "held").unwrap(),
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("seed", &serde_json::json!({})),
            )
            .await
            .expect("the seeded tally row");
        store
    });
    let held = observing.block_on(backends(&observer, APPLICATION));
    assert_eq!(held.len(), 1, "one pooled session: {held:?}");

    let outcome = caller.block_on(store.append_guarded(
        &StreamId::new(tenant.clone(), "item", "abandoned").unwrap(),
        Expected::NoStream,
        &[eventlog_conformance::event("item.received", 2)],
        &eventlog_conformance::meta("abandoned", &serde_json::json!({})),
        Arc::new(HoldsRowPastDeadline {
            tenant: tenant.clone(),
        }),
    ));
    let returned = Instant::now();
    assert!(
        matches!(
            outcome,
            Err(EventLogError::Deadline { .. } | EventLogError::UnknownCommit)
        ),
        "the append times out while its guard holds the row: {outcome:?}"
    );

    // `caller` stays alive and is never driven again: no task spawned on it runs from here on.
    let (remaining, lockable) = observing.block_on(async {
        let remaining = await_gone(&observer, APPLICATION, held[0], returned).await;
        let lockable = observer
            .batch_execute(&format!(
                "BEGIN; SET LOCAL lock_timeout = '1ms'; \
                 SELECT 1 FROM {PREFIX}_p_tally FOR UPDATE; ROLLBACK"
            ))
            .await;
        (remaining, lockable)
    });
    let elapsed = returned.elapsed();
    eprintln!(
        "abandoned backend {} after the timeout: remaining={remaining:?}, elapsed={elapsed:?}",
        held[0]
    );
    assert!(
        !remaining.contains(&held[0]),
        "the abandoned backend is still in pg_stat_activity {elapsed:?} after the timed-out \
         call returned; the server's own idle-in-transaction timeout is {TRANSACTION_TIMEOUT:?}"
    );
    assert!(
        lockable.is_ok(),
        "the abandoned transaction's row lock is free: {lockable:?}"
    );
    drop(store);
    drop(caller);
}

#[test]
fn append_blocked_on_a_lock_at_its_deadline_ends_while_the_lock_is_still_held() {
    const PREFIX: &str = "retired_close_blocked";
    const APPLICATION: &str = "eventlog_retired_close_blocked";
    // Server-side limits far beyond the case, so nothing but the pool can end the backend.
    const SERVER_LIMIT: Duration = Duration::from_secs(60);
    let Some(url) = url() else {
        eprintln!("skipped: blocked retired-connection close requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let observing = tokio::runtime::Runtime::new().expect("observer runtime");
    let observer = observing.block_on(async {
        let observer = connect(&url).await;
        fresh(&observer, PREFIX, APPLICATION).await;
        observer
    });

    let caller = current_thread();
    let tenant = TenantId::new("retired-blocked").unwrap();
    let store = caller.block_on(async {
        let store = PostgresEventStore::connect_local(
            &with_application(&url, APPLICATION),
            PREFIX,
            PoolOptions {
                max_connections: 1,
                transaction_timeout: Duration::from_secs(2),
                statement_timeout: SERVER_LIMIT,
                lock_timeout: SERVER_LIMIT,
                ..PoolOptions::default()
            },
        )
        .await
        .expect("an isolated store");
        store.seal().await;
        store
    });
    let pid = observing.block_on(backends(&observer, APPLICATION));
    assert_eq!(pid.len(), 1, "one pooled session: {pid:?}");
    let pid = pid[0];

    // A third, independent session takes a table lock every event insert needs, and keeps it.
    let (holder, holder_pid) = observing.block_on(async {
        let holder = connect(&url).await;
        holder
            .batch_execute(&format!(
                "BEGIN; LOCK TABLE {PREFIX}_events IN EXCLUSIVE MODE"
            ))
            .await
            .expect("the holder's table lock");
        let holder_pid: i32 = holder
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        (holder, holder_pid)
    });

    let outcome = caller.block_on(store.append(
        &StreamId::new(tenant.clone(), "item", "blocked").unwrap(),
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
        "the append times out while blocked on the holder's lock: {outcome:?}"
    );

    // `caller` stays alive and is never driven again; the holder still holds its lock.
    let (remaining, advisory, holder_locks) = observing.block_on(async {
        let remaining = await_gone(&observer, APPLICATION, pid, returned).await;
        let advisory = advisory_locks(&observer, pid).await;
        let holder_locks: i64 = observer
            .query_one(
                "SELECT count(*) FROM pg_locks WHERE pid = $1 AND mode = 'ExclusiveLock' \
                 AND locktype = 'relation' AND granted",
                &[&holder_pid],
            )
            .await
            .unwrap()
            .get(0);
        (remaining, advisory, holder_locks)
    });
    let elapsed = returned.elapsed();
    eprintln!(
        "blocked backend {pid} after the timeout: remaining={remaining:?}, \
         advisory locks={advisory}, elapsed={elapsed:?}"
    );
    assert_eq!(holder_locks, 1, "the holder still holds its table lock");
    assert!(
        !remaining.contains(&pid),
        "the blocked backend is still in pg_stat_activity {elapsed:?} after the timed-out call \
         returned, while the holder still holds its lock"
    );
    assert_eq!(advisory, 0, "the blocked backend's advisory locks are gone");
    observing.block_on(async {
        holder.batch_execute("ROLLBACK").await.unwrap();
    });
    drop(store);
    drop(caller);
}
