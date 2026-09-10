//! The shared exercise against a real PostgreSQL, plus the one property SQLite cannot show.
//!
//! Set `EVENTLOG_TEST_POSTGRES_URL` to run these. Without it they report themselves as not run
//! rather than passing quietly, because a backend nobody exercised is not a backend anybody proved.

use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_postgres::PostgresEventStore;
use tokio_postgres::NoTls;

/// The watermark couples feed visibility across every connection in the instance
/// (`pg_snapshot_xmin` is cluster-wide), so a test holding a transaction open while another
/// asserts on its feed is a race by design. One at a time, deterministically.
static EXCLUSIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn rewrite_rules_are_rejected_on_every_durable_table() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: rewrite-rule admission requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let prefix = "rewrite_classes";
    store(prefix).await.unwrap().shutdown().await.unwrap();
    let observer = client(&url).await;
    let tables = observer.query("SELECT tablename FROM pg_tables WHERE schemaname=current_schema() AND starts_with(tablename, $1) ORDER BY tablename", &[&format!("{prefix}_")]).await.unwrap();
    assert!(!tables.is_empty(), "exercise every actual durable table");
    for row in &tables {
        let table: String = row.get(0);
        observer
            .batch_execute(&format!(
                "CREATE RULE discard_insert AS ON INSERT TO {table} DO INSTEAD NOTHING"
            ))
            .await
            .unwrap();
        match PostgresEventStore::connect(&url, prefix).await {
            Err(eventlog_core::EventLogError::Invalid(_)) => {}
            Err(error) => panic!("{table}: expected shape refusal, got {error:?}"),
            Ok(adapter) => {
                adapter.shutdown().await.unwrap();
                panic!("rewrite rule on durable table {table} was admitted");
            }
        }
        observer
            .batch_execute(&format!("DROP RULE discard_insert ON {table}"))
            .await
            .unwrap();
    }
    let adapter = PostgresEventStore::connect(&url, prefix).await.unwrap();
    let stream = StreamId::new(TenantId::new("supported").unwrap(), "item", "one").unwrap();
    let events = [eventlog_conformance::event("item.received", 1)];
    let meta = eventlog_conformance::meta("same-command", &serde_json::json!({}));
    let first = adapter
        .append(&stream, Expected::Any, &events, &meta)
        .await
        .unwrap();
    let retry = adapter
        .append(&stream, Expected::Any, &events, &meta)
        .await
        .unwrap();
    assert!(retry.deduplicated);
    assert_eq!(retry.events, first.events);
    assert_eq!(
        adapter
            .recorded_command(&stream, &meta.idempotency_key, &meta.request_hash)
            .await
            .unwrap()
            .unwrap()
            .events,
        first.events
    );
    adapter.shutdown().await.unwrap();
    eprintln!(
        "rewrite-rule admission: refused all {} durable tables; ordinary retry retained exact receipt",
        tables.len()
    );
}

#[tokio::test]
async fn rewrite_rules_are_rejected_during_projection_migration_and_registration() {
    use eventlog_postgres::{PoolOptions, PostgresConfig};
    use std::sync::Arc;
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: rewrite-rule projection admission requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let prefix = "rewrite_projection";
    let observer = client(&url).await;
    observer
        .batch_execute("DROP TABLE IF EXISTS rewrite_projection_p_tally")
        .await
        .unwrap();
    let adapter = store(prefix).await.unwrap();
    adapter
        .create_projections(Arc::new(eventlog_conformance::Tally))
        .await
        .unwrap();
    observer.batch_execute("CREATE RULE discard_projection AS ON INSERT TO rewrite_projection_p_tally DO INSTEAD NOTHING").await.unwrap();
    let migration = PostgresEventStore::migrate(
        PostgresConfig::isolated(&url, prefix).unwrap(),
        PoolOptions::default(),
        &[eventlog_conformance::TALLY],
    )
    .await;
    assert!(
        matches!(migration, Err(eventlog_core::EventLogError::Invalid(_))),
        "migration must refuse rewrite rules on declared projections: {migration:?}"
    );
    assert!(matches!(
        adapter
            .create_projections(Arc::new(eventlog_conformance::Tally))
            .await,
        Err(eventlog_core::EventLogError::Invalid(_))
    ));
    assert!(matches!(
        adapter
            .register_inline(Arc::new(eventlog_conformance::Tally))
            .await,
        Err(eventlog_core::EventLogError::Invalid(_))
    ));
    observer
        .batch_execute("DROP RULE discard_projection ON rewrite_projection_p_tally")
        .await
        .unwrap();
    adapter
        .register_inline(Arc::new(eventlog_conformance::Tally))
        .await
        .unwrap();
    adapter.shutdown().await.unwrap();
}

#[tokio::test]
async fn schema_admission_refuses_rules_that_suppress_command_receipts() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: schema rewrite-rule review requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let prefix = "review_rewrite";
    store(prefix).await.unwrap().shutdown().await.unwrap();
    let observer = client(&url).await;
    observer.batch_execute("CREATE RULE discard_receipt AS ON INSERT TO review_rewrite_commands DO INSTEAD NOTHING").await.unwrap();
    match PostgresEventStore::connect(&url, prefix).await {
        Err(eventlog_core::EventLogError::Invalid(_)) => {}
        Err(error) => {
            panic!("schema admission must refuse the unsupported rewrite rule: {error:?}")
        }
        Ok(admitted) => {
            let stream = StreamId::new(TenantId::new("rewrite").unwrap(), "item", "one").unwrap();
            let event = [eventlog_conformance::event("item.received", 1)];
            let meta = eventlog_conformance::meta("same-command", &serde_json::json!({}));
            let first = admitted
                .append(&stream, Expected::Any, &event, &meta)
                .await
                .unwrap();
            let retry = admitted
                .append(&stream, Expected::Any, &event, &meta)
                .await
                .unwrap();
            let receipt = admitted
                .recorded_command(&stream, &meta.idempotency_key, &meta.request_hash)
                .await
                .unwrap();
            eprintln!(
                "admitted rewrite rule: first_version={}, retry_version={}, retry_deduplicated={}, durable_receipt={receipt:?}",
                first.first_version, retry.first_version, retry.deduplicated
            );
            admitted.shutdown().await.unwrap();
            panic!(
                "schema admission accepted a rewrite rule that discards durable command receipts"
            );
        }
    }
}

#[tokio::test]
async fn hosted_schema_admission_refuses_rewrite_rules() {
    use eventlog_postgres::{PoolOptions, PostgresConfig};
    use rustls::pki_types::pem::PemObject;
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: hosted rewrite-rule review requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let ca = std::env::var("EVENTLOG_TEST_POSTGRES_CA").expect("real TLS fixture");
    let app_url =
        std::env::var("EVENTLOG_TEST_HOSTED_POSTGRES_URL").expect("dedicated application role");
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from_pem_file(ca).unwrap())
        .unwrap();
    let observer = client(&url).await;
    observer.batch_execute("DROP SCHEMA IF EXISTS review_rules_hosted CASCADE; CREATE SCHEMA review_rules_hosted; DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='eventlog_test_application') THEN CREATE ROLE eventlog_test_application LOGIN CONNECTION LIMIT 8; END IF; END $$; ALTER ROLE eventlog_test_application CONNECTION LIMIT 8;").await.unwrap();
    PostgresEventStore::migrate(
        PostgresConfig::verified(&url, "review_rules_hosted", "kit", roots.clone()).unwrap(),
        PoolOptions::default(),
        &[],
    )
    .await
    .unwrap();
    observer.batch_execute("CREATE RULE discard_receipt AS ON INSERT TO review_rules_hosted.kit_commands DO INSTEAD NOTHING; GRANT USAGE ON SCHEMA review_rules_hosted TO eventlog_test_application; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA review_rules_hosted TO eventlog_test_application; GRANT USAGE ON ALL SEQUENCES IN SCHEMA review_rules_hosted TO eventlog_test_application").await.unwrap();
    let admitted = PostgresEventStore::open(
        PostgresConfig::verified(&app_url, "review_rules_hosted", "kit", roots).unwrap(),
        PoolOptions::default(),
        20,
        2,
        2,
    )
    .await;
    match admitted {
        Err(eventlog_core::EventLogError::Invalid(_)) => {}
        Err(error) => panic!("expected schema refusal, got {error:?}"),
        Ok(adapter) => {
            adapter.shutdown().await.unwrap();
            panic!(
                "verified TLS/application-role admission accepted a command-discarding rewrite rule"
            );
        }
    }
}

async fn assert_catch_up_reuses_session(contended: bool) {
    use eventlog_core::CatchUpRunner;
    use std::sync::Arc;
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: catch-up reuse requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let prefix = if contended {
        "catchup_locked"
    } else {
        "catchup_empty"
    };
    let observer = client(&url).await;
    observer
        .batch_execute(&format!("DROP TABLE IF EXISTS {prefix}_p_tally"))
        .await
        .unwrap();
    store(prefix).await.unwrap().shutdown().await.unwrap();
    let tagged_url = format!(
        "{url}{}application_name={prefix}",
        if url.contains('?') { '&' } else { '?' }
    );
    let adapter = Arc::new(
        PostgresEventStore::connect_local(
            &tagged_url,
            prefix,
            eventlog_postgres::PoolOptions {
                max_connections: 1,
                ..eventlog_postgres::PoolOptions::default()
            },
        )
        .await
        .unwrap(),
    );
    let runner = CatchUpRunner::new(adapter.clone(), Arc::new(eventlog_conformance::Tally))
        .await
        .unwrap();
    let tenant = TenantId::new("reuse").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    let pid: i32 = observer
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE application_name=$1",
            &[&prefix],
        )
        .await
        .unwrap()
        .get(0);
    let coordinates =
        serde_json::to_string(&[prefix, "projector", "tally", tenant.as_str()]).unwrap();
    if contended {
        observer.query_one("SELECT pg_advisory_lock(hashtextextended(current_database() || ':' || current_schema() || $1,0))", &[&coordinates]).await.unwrap();
    }
    for expected_position in [0, 1] {
        for _ in 0..4 {
            let progress = runner.run_once(&tenant).await.unwrap();
            assert_eq!(progress.applied, 0);
            assert_eq!(
                progress.position,
                if contended { 0 } else { expected_position }
            );
            assert_eq!(progress.more_waiting, contended);
            let state = adapter.pool_status();
            assert_eq!(
                (state.checked_out, state.waiting, state.idle),
                (0, 0, 1),
                "a successful no-work pass must return its settled connection"
            );
            let session = observer
                .query_one(
                    "SELECT pid, state FROM pg_stat_activity WHERE application_name=$1",
                    &[&prefix],
                )
                .await
                .unwrap();
            assert_eq!(session.get::<_, i32>(0), pid, "no replacement session");
            assert_eq!(session.get::<_, String>(1), "idle", "no open transaction");
        }
        if expected_position == 0 {
            if contended {
                observer.query_one("SELECT pg_advisory_unlock(hashtextextended(current_database() || ':' || current_schema() || $1,0))", &[&coordinates]).await.unwrap();
            }
            adapter
                .append(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", 1)],
                    &eventlog_conformance::meta("reuse-command", &serde_json::json!({})),
                )
                .await
                .unwrap();
            let progress = runner.run_once(&tenant).await.unwrap();
            assert_eq!(
                (progress.applied, progress.position, progress.more_waiting),
                (1, 1, false)
            );
            if contended {
                observer.query_one("SELECT pg_advisory_lock(hashtextextended(current_database() || ':' || current_schema() || $1,0))", &[&coordinates]).await.unwrap();
            }
        }
    }
    if contended {
        observer.query_one("SELECT pg_advisory_unlock(hashtextextended(current_database() || ':' || current_schema() || $1,0))", &[&coordinates]).await.unwrap();
    }
    assert_eq!(
        adapter
            .projection_get(&eventlog_conformance::TALLY, &tenant, "item/one")
            .await
            .unwrap()
            .unwrap()["count"],
        1
    );
    let cursor: i64 = observer.query_one(&format!("SELECT global_seq FROM {prefix}_projection_cursors WHERE projection='tally' AND tenant_id=$1"), &[&tenant.as_str()]).await.unwrap().get(0);
    assert_eq!(cursor, 1);
    adapter.shutdown().await.unwrap();
    eprintln!(
        "catch-up reuse: contended={contended}, eight no-work polls retained backend {pid}, cursor=1, tally=1"
    );
}

#[tokio::test]
async fn empty_catch_up_reuses_the_same_settled_session() {
    assert_catch_up_reuses_session(false).await;
}

#[tokio::test]
async fn contended_catch_up_reuses_the_same_settled_session() {
    assert_catch_up_reuses_session(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn unsettled_catch_up_rollback_never_recycles_a_session() {
    use std::{
        io::{Read, Write},
        net::{Shutdown, TcpListener, TcpStream},
        sync::Arc,
        time::Duration,
    };
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: rollback response loss requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let parsed: tokio_postgres::Config = url.parse().unwrap();
    let upstream_port = parsed.get_ports()[0];
    for cancel in [false, true] {
        let prefix = if cancel {
            "catchup_cancel"
        } else {
            "catchup_failed"
        };
        store(prefix).await.unwrap().shutdown().await.unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let proxy_port = listener.local_addr().unwrap().port();
        let (intercepted, observed) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let proxy = std::thread::spawn(move || {
            let (mut downstream, _) = listener.accept().unwrap();
            let mut upstream = TcpStream::connect(("127.0.0.1", upstream_port)).unwrap();
            downstream.set_nodelay(true).unwrap();
            upstream.set_nodelay(true).unwrap();
            downstream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            upstream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut inbound = downstream.try_clone().unwrap();
            let mut outbound = upstream.try_clone().unwrap();
            let forward = std::thread::spawn(move || {
                let _ = std::io::copy(&mut inbound, &mut outbound);
                let _ = outbound.shutdown(Shutdown::Both);
            });
            loop {
                let mut header = [0_u8; 5];
                if upstream.read_exact(&mut header).is_err() {
                    break;
                }
                let length = u32::from_be_bytes(header[1..].try_into().unwrap());
                assert!((4..=16 * 1024 * 1024).contains(&length));
                let mut body = vec![0; usize::try_from(length - 4).unwrap()];
                if upstream.read_exact(&mut body).is_err() {
                    break;
                }
                if header[0] == b'C' && body == b"ROLLBACK\0" {
                    let _ = intercepted.send(());
                    let _ = resume.recv_timeout(Duration::from_secs(5));
                    break;
                }
                if downstream
                    .write_all(&header)
                    .and_then(|()| downstream.write_all(&body))
                    .is_err()
                {
                    break;
                }
            }
            let _ = upstream.shutdown(Shutdown::Both);
            let _ = downstream.shutdown(Shutdown::Both);
            forward.join().unwrap();
        });
        let proxy_url = format!("host=127.0.0.1 port={proxy_port} user=postgres dbname=postgres");
        let adapter = Arc::new(
            PostgresEventStore::connect_local(
                &proxy_url,
                prefix,
                eventlog_postgres::PoolOptions {
                    max_connections: 1,
                    ..eventlog_postgres::PoolOptions::default()
                },
            )
            .await
            .unwrap(),
        );
        let polling = adapter.clone();
        let task = tokio::spawn(async move {
            polling
                .run_catch_up(
                    Arc::new(eventlog_conformance::Tally),
                    &TenantId::new("rollback").unwrap(),
                    10,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), observed)
            .await
            .expect("explicit rollback must reach the server")
            .expect("proxy observed rollback");
        let state = adapter.pool_status();
        assert_eq!(
            (state.checked_out, state.waiting, state.idle),
            (1, 0, 0),
            "a rollback without its response remains quarantined"
        );
        assert!(
            !task.is_finished(),
            "successful settlement requires the response"
        );
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            release.send(()).unwrap();
        } else {
            release.send(()).unwrap();
            assert!(matches!(
                task.await.unwrap(),
                Err(eventlog_core::EventLogError::Backend(_))
            ));
        }
        assert_eq!(
            adapter.pool_status().idle,
            0,
            "unsettled lease cannot recycle"
        );
        adapter.shutdown().await.unwrap();
        let state = adapter.pool_status();
        assert_eq!((state.checked_out, state.waiting, state.idle), (0, 0, 0));
        proxy.join().unwrap();
        eprintln!(
            "catch-up rollback: cancel={cancel}, response withheld with occupancy=1/0/0; shutdown drained=0/0/0"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_observation_preserves_two_connections_and_four_waiters() {
    use eventlog_core::{BoxFuture, EventLogError, Guard, ProjectionStore};
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll, Waker},
        time::Duration,
    };
    struct Hold {
        entered: tokio::sync::mpsc::Sender<()>,
        release: Arc<tokio::sync::Notify>,
    }
    impl Guard for Hold {
        fn check<'a>(
            &'a self,
            _: &'a mut dyn ProjectionStore,
        ) -> BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                let release = self.release.notified();
                tokio::pin!(release);
                release.as_mut().enable();
                self.entered.send(()).await.unwrap();
                release.await;
                Ok(())
            })
        }
    }
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: pool observation requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    store("pool_observation_public")
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let store = Arc::new(
        PostgresEventStore::connect_local(
            &url,
            "pool_observation_public",
            eventlog_postgres::PoolOptions {
                max_connections: 2,
                max_waiters: 4,
                acquisition_timeout: Duration::from_secs(2),
                ..eventlog_postgres::PoolOptions::default()
            },
        )
        .await
        .unwrap(),
    );
    let (entered, mut receiving) = tokio::sync::mpsc::channel(2);
    let release = Arc::new(tokio::sync::Notify::new());
    let mut holders = Vec::new();
    let tenant = TenantId::new("pool-observation").unwrap();
    for id in ["cancelled", "committed"] {
        let store = store.clone();
        let stream = StreamId::new(tenant.clone(), "item", id).unwrap();
        let guard = Arc::new(Hold {
            entered: entered.clone(),
            release: release.clone(),
        });
        holders.push(tokio::spawn(async move {
            store
                .append_guarded(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", 1)],
                    &eventlog_conformance::meta(id, &serde_json::json!({})),
                    guard,
                )
                .await
        }));
    }
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(2), receiving.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let stream = StreamId::new(tenant.clone(), "item", "committed").unwrap();
    let mut queued = (0..4)
        .map(|_| Box::pin(store.stream_version(&stream)))
        .collect::<Vec<_>>();
    for query in &mut queued {
        assert!(matches!(
            query.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }
    let saturated = store.pool_status();
    assert_eq!(
        (saturated.checked_out, saturated.waiting, saturated.idle),
        (2, 4, 0)
    );
    assert_eq!(
        store.stream_version(&stream).await,
        Err(EventLogError::Overloaded)
    );
    let done = Arc::new(AtomicBool::new(false));
    let sampling = store.clone();
    let stopping = done.clone();
    let monitor = tokio::spawn(async move {
        let mut samples = 0;
        while !stopping.load(Ordering::Acquire) {
            let status = sampling.pool_status();
            assert!(
                status.checked_out <= 2
                    && status.waiting <= 4
                    && status.checked_out + status.idle <= 2,
                "{status:?}"
            );
            samples += 1;
            tokio::task::yield_now().await;
        }
        samples
    });
    holders[0].abort();
    release.notify_waiters();
    let committed = holders.pop().unwrap().await.unwrap().unwrap();
    assert_eq!(committed.last_version, 1);
    assert!(holders.pop().unwrap().await.unwrap_err().is_cancelled());
    for query in queued {
        assert_eq!(query.await.unwrap(), Some(1));
    }
    let cancelled = StreamId::new(tenant, "item", "cancelled").unwrap();
    assert_eq!(store.stream_version(&cancelled).await.unwrap(), None);
    let mut queries = Vec::new();
    for _ in 0..64 {
        let store = store.clone();
        let stream = stream.clone();
        queries.push(tokio::spawn(
            async move { store.stream_version(&stream).await },
        ));
    }
    let mut completed = 0;
    let mut overloaded = 0;
    for query in queries {
        match query.await.unwrap() {
            Ok(Some(1)) => completed += 1,
            Err(EventLogError::Overloaded) => overloaded += 1,
            result => panic!("unexpected bounded query result: {result:?}"),
        }
    }
    done.store(true, Ordering::Release);
    let samples = monitor.await.unwrap();
    assert!(samples > 0 && completed > 0);
    assert_eq!(completed + overloaded, 64);
    store.shutdown().await.unwrap();
    let drained = store.pool_status();
    assert_eq!(
        (drained.checked_out, drained.waiting, drained.idle),
        (0, 0, 0)
    );
    assert!(drained.closed);
    eprintln!(
        "public pool profile2/4: queued4, committed1, cancelled1, query_success={completed}, overload={overloaded}, samples={samples}"
    );
}

#[test]
fn isolated_transport_cannot_hide_a_remote_address_behind_localhost() {
    use eventlog_postgres::PostgresConfig;
    for address in ["192.0.2.1", "2001:db8::1"] {
        assert!(
            PostgresConfig::isolated(
                &format!("host=localhost hostaddr={address} user=fixture dbname=fixture"),
                "fixture"
            )
            .is_err(),
            "hostaddr determines the actual plaintext destination"
        );
    }
    assert!(
        PostgresConfig::isolated(
            "host=localhost hostaddr=127.0.0.1 user=fixture dbname=fixture",
            "fixture"
        )
        .is_ok()
    );
}

/// The database to exercise against, when one was given.
///
/// An empty value counts as unset. `Ok("")` from the environment would otherwise send the whole
/// suite at an unusable connection string and report a connection failure as a test failure.
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

/// A bare client for test setup, its connection task spawned like the store spawns its own.
async fn client(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("a connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn store(prefix: &str) -> Option<PostgresEventStore> {
    let url = url()?;
    let sql = client(&url).await;
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
    ] {
        sql.batch_execute(&format!("DROP TABLE IF EXISTS {prefix}_{suffix}"))
            .await
            .expect("fresh test-owned tables");
    }
    let store = PostgresEventStore::connect(&url, prefix)
        .await
        .expect("a reachable PostgreSQL");
    Some(store)
}

#[tokio::test]
async fn the_shared_exercise_passes_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("conformance").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    eventlog_conformance::run(&store).await;
}

#[tokio::test]
async fn a_reader_never_skips_an_event_that_committed_late() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let prefix = "watermark";
    let setup = PostgresEventStore::connect(&url, prefix)
        .await
        .expect("a reachable PostgreSQL");
    setup.drop_tables().await.expect("a clean slate");
    let store = PostgresEventStore::connect(&url, prefix)
        .await
        .expect("a store");
    let tenant = TenantId::new("tenant-a").expect("valid tenant");
    let fast = StreamId::new(tenant.clone(), "item", "fast").expect("valid stream");

    // A second connection takes position 1 and holds its transaction open.
    let mut slow = client(&url).await;
    let held = slow.transaction().await.expect("an open transaction");
    held.execute(
        "INSERT INTO watermark_events (
             tenant_id, stream_type, stream_id, version, event_id, event_name,
             event_schema_version, occurred_at, recorded_at, subject, actor, request_id,
             trace_id, causation_depth, data)
         VALUES ('tenant-a', 'item', 'slow', 1, gen_random_uuid(), 'item.received', 1,
                 now(), now(), 'person-1', 'service-1', 'request-slow', 'trace-slow', 0,
                 '{\"value\": 1}'::jsonb)",
        &[],
    )
    .await
    .expect("the slow writer takes its position");

    // The second writer starts later, takes position 2, and commits first.
    store
        .append(
            &fast,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 2)],
            &eventlog_conformance::meta("fast", &serde_json::json!({"n": 2})),
        )
        .await
        .expect("the fast writer commits");

    // A reader that trusted the sequence alone would take position 2 now and never come back for
    // position 1. It sees nothing instead, and waits.
    let blocked = store.read_feed(&tenant, 0, 10).await.expect("readable");
    assert!(
        blocked.events.is_empty(),
        "a reader must not move past a position that is still in flight"
    );

    held.commit().await.expect("the slow writer commits second");

    let page = store.read_feed(&tenant, 0, 10).await.expect("readable");
    let seen: Vec<&str> = page
        .events
        .iter()
        .map(|event| event.stream_id.as_str())
        .collect();
    assert_eq!(
        seen,
        vec!["slow", "fast"],
        "both writers are delivered, in position order, once both have committed"
    );
}

#[tokio::test]
async fn the_projection_exercise_passes_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("projections").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["projections_p_tally"]).await;
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(store);
    eventlog_conformance::run_projections(&store).await;
}

#[tokio::test]
async fn the_inline_projection_exercise_passes_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("inline").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["inline_p_tally"]).await;
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(store);
    eventlog_conformance::run_inline_projections(&store).await;
}

async fn drop_projection_tables(store: &PostgresEventStore, tables: &[&str]) {
    let Some(url) = url() else { return };
    let client = client(&url).await;
    for table in tables {
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
            .await
            .expect("a clean slate");
    }
    let _ = store;
}

#[tokio::test]
async fn the_paging_rule_holds_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("paging").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["paging_p_tally"]).await;
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(store);
    eventlog_conformance::run_paging(&store).await;
}

#[tokio::test]
async fn the_claim_rule_holds_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("claims").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    eventlog_conformance::run_claims(&store).await;
}

/// The same refusal, where it matters most: a hosted deployment points the log at the database the
/// module already had.
#[tokio::test]
async fn a_table_of_ours_that_somebody_else_made_is_refused_by_name() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let client = client(&url).await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS previous_events;
             CREATE TABLE previous_events (
                 tenant_id TEXT NOT NULL,
                 sequence BIGINT NOT NULL,
                 body JSONB NOT NULL
             );",
        )
        .await
        .expect("the previous store's table");

    let Err(refused) = PostgresEventStore::connect(&url, "previous").await else {
        panic!("a table this kit did not create must not be silently adopted");
    };
    let message = refused.to_string();
    assert!(
        message.contains("previous_events"),
        "the refusal names the table: {message}"
    );
    client
        .batch_execute("DROP TABLE IF EXISTS previous_events")
        .await
        .expect("cleaned up");
}

#[tokio::test]
async fn independent_first_appends_return_contract_outcomes() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let prefix = "first_race";
    let setup = store(prefix).await.expect("store");
    let first = PostgresEventStore::connect(&url, prefix)
        .await
        .expect("first client");
    let second = PostgresEventStore::connect(&url, prefix)
        .await
        .expect("second client");
    let sql = client(&url).await;
    sql.batch_execute("CREATE OR REPLACE FUNCTION first_race_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.05); RETURN NEW; END $$; CREATE TRIGGER delay BEFORE INSERT ON first_race_events FOR EACH ROW EXECUTE FUNCTION first_race_delay()").await.expect("deterministic overlapping writes");
    let tenant = TenantId::new("race-tenant").expect("tenant");
    for same_command in [false, true] {
        let stream = StreamId::new(
            tenant.clone(),
            "item",
            if same_command { "same" } else { "different" },
        )
        .expect("stream");
        let events = [eventlog_conformance::event("item.received", 1)];
        let a = eventlog_conformance::meta("first", &serde_json::json!({"value":1}));
        let b = eventlog_conformance::meta(
            if same_command { "first" } else { "second" },
            &serde_json::json!({"value":1}),
        );
        let (a, b) = tokio::join!(
            first.append(&stream, Expected::NoStream, &events, &a),
            second.append(&stream, Expected::NoStream, &events, &b)
        );
        if same_command {
            let a = a.expect("one committed response");
            let b = b.expect("the same committed response on the independent client");
            assert_eq!(a.events, b.events);
            assert_ne!(a.deduplicated, b.deduplicated);
        } else {
            assert!(
                matches!(
                    (&a, &b),
                    (Ok(_), Err(eventlog_core::EventLogError::Conflict { .. }))
                        | (Err(eventlog_core::EventLogError::Conflict { .. }), Ok(_))
                ),
                "one commit and a typed conflict, got {a:?} / {b:?}"
            );
        }
    }
    setup.drop_tables().await.expect("test cleanup");
    sql.batch_execute("DROP FUNCTION first_race_delay()")
        .await
        .expect("test cleanup");
}

#[tokio::test]
async fn incomplete_companion_schema_is_refused_before_serving() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    client(&url)
        .await
        .batch_execute("DROP TABLE IF EXISTS partial_shape_commands")
        .await
        .expect("clear incomplete fixture");
    for suffix in [
        "events",
        "claims",
        "identity",
        "snapshots",
        "projection_cursors",
        "blobs",
        "scope_counters",
        "schema_version",
    ] {
        client(&url)
            .await
            .batch_execute(&format!("DROP TABLE IF EXISTS partial_shape_{suffix}"))
            .await
            .expect("clear fixture");
    }
    let setup = store("partial_shape").await.expect("store");
    let sql = client(&url).await;
    sql.batch_execute("ALTER TABLE partial_shape_commands DROP COLUMN request_hash")
        .await
        .expect("partial schema fixture");
    assert!(
        PostgresEventStore::connect(&url, "partial_shape")
            .await
            .is_err(),
        "an events column is not complete schema admission"
    );
    setup.drop_tables().await.expect("test cleanup");
}

#[tokio::test]
async fn rebuild_preserves_other_tenants_and_previous_view_on_failure() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("rebuild_isolation").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["rebuild_isolation_p_tally"]).await;
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(store);
    eventlog_conformance::run_rebuild_isolation(&store).await;
}

#[tokio::test]
async fn scope_reservations_are_atomic_and_confined() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("scope_atomic").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    eventlog_conformance::run_scope_atomicity(&store, &store.admission_permit()).await;
}

#[tokio::test]
async fn absent_deployment_scope_races_across_tenants_and_clients() {
    use eventlog_core::{AdmissionScope, Reservation};
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let first = store("deployment_race").await.expect("first");
    let second = PostgresEventStore::connect(&url, "deployment_race")
        .await
        .expect("second");
    for n in 0..8 {
        let a = TenantId::new("tenant-a").expect("tenant");
        let b = TenantId::new("tenant-b").expect("tenant");
        let stream_a = StreamId::new(a.clone(), "item", format!("a-{n}")).expect("stream");
        let stream_b = StreamId::new(b.clone(), "item", format!("b-{n}")).expect("stream");
        let guard = |store: &PostgresEventStore, tenant: TenantId, reverse: bool| {
            let mut reservations = vec![
                Reservation {
                    scope: AdmissionScope::Tenant {
                        tenant,
                        key: format!("local-{n}"),
                    },
                    delta: 1,
                    ceiling: 1,
                },
                Reservation {
                    scope: AdmissionScope::Deployment {
                        key: format!("global-{n}"),
                    },
                    delta: 1,
                    ceiling: 1,
                },
            ];
            if reverse {
                reservations.reverse();
            }
            std::sync::Arc::new(eventlog_conformance::ReserveScopes {
                permit: store.admission_permit(),
                reservations,
            }) as std::sync::Arc<dyn eventlog_core::Guard>
        };
        let events = [eventlog_conformance::event("item.received", 1)];
        let meta = eventlog_conformance::meta("same", &serde_json::json!({}));
        let (a, b) = tokio::join!(
            first.append_guarded(
                &stream_a,
                Expected::NoStream,
                &events,
                &meta,
                guard(&first, a, false)
            ),
            second.append_guarded(
                &stream_b,
                Expected::NoStream,
                &events,
                &meta,
                guard(&second, b, true)
            )
        );
        assert_eq!(
            usize::from(a.is_ok()) + usize::from(b.is_ok()),
            1,
            "one shared reservation: {a:?} / {b:?}"
        );
        assert!(matches!(
            a,
            Ok(_) | Err(eventlog_core::EventLogError::Invalid(_))
        ));
        assert!(matches!(
            b,
            Ok(_) | Err(eventlog_core::EventLogError::Invalid(_))
        ));
    }
    let sql = client(&url).await;
    let counters: i64 = sql
        .query_one(
            "SELECT count(*) FROM deployment_race_scope_counters WHERE held=1",
            &[],
        )
        .await
        .expect("counters")
        .get(0);
    assert_eq!(
        counters, 16,
        "one tenant + one deployment per accepted command, no refused rows"
    );
}

#[tokio::test]
async fn registration_freezes_and_pool_refuses_bounded_overload() {
    use eventlog_core::{BoxFuture, EventLogError, Guard, ProjectionStore};
    use std::{sync::Arc, time::Duration};
    struct Wait {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    impl Guard for Wait {
        fn check<'a>(
            &'a self,
            _: &'a mut dyn ProjectionStore,
        ) -> BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(())
            })
        }
    }
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let setup = store("pool_lifecycle").await.expect("setup");
    setup.shutdown().await.expect("shutdown");
    let options = eventlog_postgres::PoolOptions {
        max_connections: 1,
        max_waiters: 1,
        acquisition_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    let store = Arc::new(
        PostgresEventStore::connect_local(&url, "pool_lifecycle", options)
            .await
            .expect("bounded pool"),
    );
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let waiting = Arc::clone(&store);
    let guard = Arc::new(Wait {
        entered: Arc::clone(&entered),
        release,
    });
    let task = tokio::spawn(async move {
        let stream = StreamId::new(
            TenantId::new("pool-tenant").expect("tenant"),
            "item",
            "held",
        )
        .expect("stream");
        waiting
            .append_guarded(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("held", &serde_json::json!({})),
                guard,
            )
            .await
    });
    entered.notified().await;
    assert!(
        store
            .register_inline(Arc::new(eventlog_conformance::Tally))
            .await
            .is_err(),
        "registry frozen while a transaction is in flight"
    );
    let queued = Arc::clone(&store);
    let waiter = tokio::spawn(async move {
        queued
            .read_feed(&TenantId::new("pool-tenant").expect("tenant"), 0, 10)
            .await
    });
    for _ in 0..100 {
        if store.pool_status().waiting == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(store.pool_status().checked_out, 1);
    assert_eq!(store.pool_status().waiting, 1);
    let refused = store
        .read_feed(&TenantId::new("pool-tenant").expect("tenant"), 0, 10)
        .await;
    assert_eq!(refused, Err(EventLogError::Overloaded));
    assert!(matches!(
        waiter.await.expect("waiter"),
        Err(EventLogError::Deadline { .. })
    ));
    task.abort();
    assert!(task.await.expect_err("cancelled").is_cancelled());
    for _ in 0..100 {
        if store.pool_status().checked_out == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(store.pool_status().checked_out, 0);
    let stream = StreamId::new(
        TenantId::new("pool-tenant").expect("tenant"),
        "item",
        "held",
    )
    .expect("stream");
    assert!(
        store
            .stream_version(&stream)
            .await
            .expect("reconnected")
            .is_none()
    );
    store.shutdown().await.expect("closed");
    assert_eq!(
        store.read_feed(stream.tenant(), 0, 10).await,
        Err(EventLogError::Closed)
    );
}

#[tokio::test]
async fn verified_tls_requires_matching_server_and_separate_application_role() {
    use eventlog_postgres::{PoolOptions, PostgresConfig};
    use rustls::pki_types::pem::PemObject;
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let Ok(ca_path) = std::env::var("EVENTLOG_TEST_POSTGRES_CA") else {
        assert!(
            std::env::var_os("EVENTLOG_REQUIRE_POSTGRES").is_none(),
            "required TLS fixture EVENTLOG_TEST_POSTGRES_CA is missing"
        );
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_CA is not set");
        return;
    };
    let app_url = std::env::var("EVENTLOG_TEST_HOSTED_POSTGRES_URL")
        .expect("explicit test application role URL");
    let cert = rustls::pki_types::CertificateDer::from_pem_file(ca_path).expect("test CA");
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("trusted certificate");
    assert!(
        PostgresConfig::verified(&url, "hosted_owner", "kit", rustls::RootCertStore::empty())
            .is_err()
    );
    let sql = client(&url).await;
    sql.batch_execute("DROP SCHEMA IF EXISTS hosted_owner CASCADE; CREATE SCHEMA hosted_owner; DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='eventlog_test_application') THEN CREATE ROLE eventlog_test_application LOGIN CONNECTION LIMIT 8; END IF; END $$; ALTER ROLE eventlog_test_application CONNECTION LIMIT 8;").await.expect("isolated role/schema fixture");
    let migration = PostgresConfig::verified(&url, "hosted_owner", "kit", roots.clone())
        .expect("verified migration config");
    PostgresEventStore::migrate(
        migration.clone(),
        PoolOptions::default(),
        &[eventlog_conformance::TALLY],
    )
    .await
    .expect("migration role applies schema");
    assert!(
        PostgresEventStore::open(migration, PoolOptions::default(), 16, 2, 8)
            .await
            .is_err(),
        "application constructor refuses schema owner"
    );
    sql.batch_execute("GRANT USAGE ON SCHEMA hosted_owner TO eventlog_test_application; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA hosted_owner TO eventlog_test_application; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA hosted_owner TO eventlog_test_application").await.expect("DML-only role grants");
    let config = PostgresConfig::verified(&app_url, "hosted_owner", "kit", roots.clone())
        .expect("verified application config");
    sql.batch_execute(
        "REVOKE UPDATE ON hosted_owner.kit_snapshot_generations FROM eventlog_test_application",
    )
    .await
    .unwrap();
    assert!(
        PostgresEventStore::open(config.clone(), PoolOptions::default(), 16, 2, 8)
            .await
            .is_err(),
        "snapshot metadata permissions are required before serving"
    );
    sql.batch_execute(
        "GRANT UPDATE ON hosted_owner.kit_snapshot_generations TO eventlog_test_application",
    )
    .await
    .unwrap();
    sql.batch_execute(
        "REVOKE INSERT ON hosted_owner.kit_scope_counters FROM eventlog_test_application",
    )
    .await
    .expect("missing required application permission");
    assert!(
        PostgresEventStore::open(config.clone(), PoolOptions::default(), 16, 2, 8)
            .await
            .is_err(),
        "startup must refuse a role missing required counter DML"
    );
    sql.batch_execute(
        "GRANT INSERT ON hosted_owner.kit_scope_counters TO eventlog_test_application",
    )
    .await
    .expect("restore required DML");
    assert!(
        PostgresEventStore::open(config.clone(), PoolOptions::default(), 7, 2, 1)
            .await
            .is_err(),
        "missing connection reserve cannot be admitted"
    );
    let store = PostgresEventStore::open(config, PoolOptions::default(), 16, 2, 8)
        .await
        .expect("verified DML-only application");
    sql.batch_execute("REVOKE INSERT ON hosted_owner.kit_p_tally FROM eventlog_test_application")
        .await
        .expect("missing declared projection DML");
    assert!(
        store
            .register_inline(std::sync::Arc::new(eventlog_conformance::Tally))
            .await
            .is_err(),
        "registration must refuse missing projection write permission"
    );
    sql.batch_execute("GRANT INSERT ON hosted_owner.kit_p_tally TO eventlog_test_application")
        .await
        .expect("restore projection DML");
    store
        .register_inline(std::sync::Arc::new(eventlog_conformance::Tally))
        .await
        .expect("declared projection requires no owner DDL");
    store.seal().await;
    let stream = StreamId::new(
        TenantId::new("hosted-tenant").expect("tenant"),
        "item",
        "one",
    )
    .expect("stream");
    store
        .append(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("tls", &serde_json::json!({})),
        )
        .await
        .expect("real TLS transaction");
    assert_eq!(
        store
            .projection_get(&eventlog_conformance::TALLY, stream.tenant(), "item/one")
            .await
            .expect("view")
            .expect("row")["count"],
        serde_json::json!(1)
    );
    let parsed: url_shim::Config = url.parse().expect("database configuration");
    let port = parsed.get_ports()[0];
    let wrong = format!(
        "host=wrong.invalid hostaddr=127.0.0.1 port={port} user=eventlog_test_application dbname=postgres"
    );
    let wrong = PostgresConfig::verified(&wrong, "hosted_owner", "kit", roots)
        .expect("nonmatching host config");
    assert!(
        PostgresEventStore::open(wrong, PoolOptions::default(), 16, 2, 8)
            .await
            .is_err(),
        "TLS must verify the server name"
    );
    store.shutdown().await.expect("shutdown");
    sql.batch_execute("ALTER TABLE hosted_owner.kit_events OWNER TO eventlog_test_application")
        .await
        .expect("table owner without schema CREATE");
    let owner_config = PostgresConfig::verified(&app_url, "hosted_owner", "kit", {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(
                rustls::pki_types::CertificateDer::from_pem_file(
                    std::env::var("EVENTLOG_TEST_POSTGRES_CA").expect("CA path"),
                )
                .expect("CA"),
            )
            .expect("root");
        roots
    })
    .expect("owner config");
    assert!(
        PostgresEventStore::open(owner_config, PoolOptions::default(), 16, 2, 8)
            .await
            .is_err(),
        "revoked schema CREATE does not remove table-owner DDL powers"
    );
    sql.batch_execute("ALTER TABLE hosted_owner.kit_events OWNER TO postgres")
        .await
        .expect("restore migration ownership");
}
mod url_shim {
    pub type Config = tokio_postgres::Config;
}

#[tokio::test]
async fn a_feed_cursor_cannot_pass_an_inflight_lower_position_with_a_newer_xid() {
    use eventlog_core::{AdmissionScope, EventLogError, Projector, Reservation};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };
    struct OlderGuard {
        permit: eventlog_core::AdmissionPermit,
        tenant: TenantId,
        assigned: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    }
    impl eventlog_core::Guard for OlderGuard {
        fn check<'a>(
            &'a self,
            store: &'a mut dyn eventlog_core::ProjectionStore,
        ) -> eventlog_core::BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                store
                    .reserve(
                        &self.permit,
                        &[Reservation {
                            scope: AdmissionScope::Tenant {
                                tenant: self.tenant.clone(),
                                key: "older-xid".into(),
                            },
                            delta: 1,
                            ceiling: 1,
                        }],
                    )
                    .await?;
                self.assigned.notify_one();
                self.resume.notified().await;
                Ok(())
            })
        }
    }
    struct LaterWriter {
        inserted: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
        position: Arc<AtomicU64>,
    }
    impl Projector for LaterWriter {
        fn name(&self) -> &'static str {
            "later_writer"
        }
        fn projections(&self) -> &'static [eventlog_core::ProjectionSpec] {
            std::slice::from_ref(&eventlog_conformance::TALLY)
        }
        fn apply<'a>(
            &'a self,
            event: &'a eventlog_core::RecordedEvent,
            store: &'a mut dyn eventlog_core::ProjectionStore,
        ) -> eventlog_core::BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                eventlog_conformance::Tally.apply(event, store).await?;
                if event.stream_id == "b" {
                    self.position.store(event.global_seq, Ordering::Release);
                    self.inserted.notify_one();
                    self.resume.notified().await;
                }
                Ok(())
            })
        }
    }
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let first = Arc::new(store("feed_xid_order").await.expect("fixture"));
    client(&url)
        .await
        .batch_execute("DROP TABLE IF EXISTS feed_xid_order_p_tally")
        .await
        .expect("fresh view");
    let second = Arc::new(
        PostgresEventStore::connect(&url, "feed_xid_order")
            .await
            .expect("independent writer"),
    );
    let parsed: tokio_postgres::Config = url.parse().expect("config");
    let reader_url = format!(
        "host=127.0.0.1 port={} user=postgres dbname=postgres options='-c default_transaction_isolation=serializable'",
        parsed.get_ports()[0]
    );
    let isolated_reader = Arc::new(
        PostgresEventStore::connect(&reader_url, "feed_xid_order")
            .await
            .expect("reader with hostile inherited isolation default"),
    );
    let tenant = TenantId::new("feed-order").expect("tenant");
    let assigned = Arc::new(tokio::sync::Notify::new());
    let resume_first = Arc::new(tokio::sync::Notify::new());
    let inserted = Arc::new(tokio::sync::Notify::new());
    let resume_second = Arc::new(tokio::sync::Notify::new());
    let position = Arc::new(AtomicU64::new(0));
    let projector = Arc::new(LaterWriter {
        inserted: inserted.clone(),
        resume: resume_second.clone(),
        position: position.clone(),
    });
    first
        .register_inline(projector.clone())
        .await
        .expect("first setup");
    second
        .register_inline(projector)
        .await
        .expect("second setup");
    let (writer, first_tenant) = (first.clone(), tenant.clone());
    let guard = Arc::new(OlderGuard {
        permit: first.admission_permit(),
        tenant: tenant.clone(),
        assigned: assigned.clone(),
        resume: resume_first.clone(),
    });
    let older = tokio::spawn(async move {
        writer
            .append_guarded(
                &StreamId::new(first_tenant, "item", "a").expect("stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("a", &serde_json::json!({})),
                guard,
            )
            .await
    });
    assigned.notified().await;
    let second_tenant = tenant.clone();
    let newer = tokio::spawn(async move {
        second
            .append(
                &StreamId::new(second_tenant, "item", "b").expect("stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 2)],
                &eventlog_conformance::meta("b", &serde_json::json!({})),
            )
            .await
    });
    inserted.notified().await;
    resume_first.notify_one();
    let committed = older
        .await
        .expect("older writer")
        .expect("older XID commits later position");
    assert!(
        committed.events[0].global_seq > position.load(Ordering::Acquire),
        "the controlled XID/position order must be reversed"
    );
    let (reader, read_tenant) = (isolated_reader, tenant.clone());
    let mut page = tokio::spawn(async move { reader.read_feed(&read_tenant, 0, 10).await });
    let early = tokio::select! {result=&mut page=>Some(result.expect("reader").expect("feed")),()=tokio::time::sleep(Duration::from_millis(30))=>None};
    if let Some(ref early) = early {
        assert!(
            early.events.is_empty(),
            "cursor would skip the invisible lower position: {early:?}"
        );
    }
    resume_second.notify_one();
    newer
        .await
        .expect("newer writer")
        .expect("later XID commits lower position");
    let visible = if let Some(early) = early {
        first
            .read_feed(&tenant, early.next_position, 10)
            .await
            .expect("resume")
    } else {
        page.await.expect("reader").expect("feed")
    };
    assert_eq!(visible.events.len(), 2, "both positions remain reachable");
    assert!(visible.events[0].global_seq < visible.events[1].global_seq);
}

#[tokio::test]
async fn inline_failure_preserves_all_atomic_state_and_callback_authority() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("inline_atomic").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    client(&url().expect("fixture URL"))
        .await
        .batch_execute("DROP TABLE IF EXISTS inline_atomic_p_tally")
        .await
        .expect("fresh test projection");
    eventlog_conformance::run_inline_failure_atomicity(&store, &store.admission_permit()).await;
}

#[tokio::test]
async fn killed_projector_restarts_competing_workers_without_partial_view_or_cursor() {
    use eventlog_core::{CatchUpRunner, EventLogError, Projector};
    use std::{
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    };
    struct Stalls(PathBuf);
    impl Projector for Stalls {
        fn name(&self) -> &'static str {
            "tally"
        }
        fn projections(&self) -> &'static [eventlog_core::ProjectionSpec] {
            std::slice::from_ref(&eventlog_conformance::TALLY)
        }
        fn apply<'a>(
            &'a self,
            event: &'a eventlog_core::RecordedEvent,
            store: &'a mut dyn eventlog_core::ProjectionStore,
        ) -> eventlog_core::BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                eventlog_conformance::Tally.apply(event, store).await?;
                std::fs::write(
                    &self.0,
                    b"partial view written inside uncommitted transaction",
                )
                .expect("barrier");
                std::future::pending().await
            })
        }
    }
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let tenant = TenantId::new("projector-recovery").expect("tenant");
    if let Some(barrier) = std::env::var_os("EVENTLOG_PROJECTOR_CHILD") {
        let store = Arc::new(
            PostgresEventStore::connect(&url, "process_projector")
                .await
                .expect("child connection"),
        );
        CatchUpRunner::new(store, Arc::new(Stalls(PathBuf::from(barrier))))
            .await
            .expect("child runner")
            .run_once(&tenant)
            .await
            .expect("child is killed before completion");
        return;
    }
    let _exclusive = EXCLUSIVE.lock().await;
    let store = Arc::new(store("process_projector").await.expect("fixture"));
    client(&url)
        .await
        .batch_execute("DROP TABLE IF EXISTS process_projector_p_tally")
        .await
        .expect("fresh projection");
    let runner = CatchUpRunner::new(store.clone(), Arc::new(eventlog_conformance::Tally))
        .await
        .expect("runner")
        .with_batch(3);
    let stream = StreamId::new(tenant.clone(), "item", "retained").expect("stream");
    let mut last = 0;
    for index in 0..20 {
        last = store
            .append(
                &stream,
                Expected::Any,
                &[eventlog_conformance::event("item.received", index)],
                &eventlog_conformance::meta(
                    &format!("command-{index}"),
                    &serde_json::json!({"value":index}),
                ),
            )
            .await
            .expect("retained event")
            .events[0]
            .global_seq;
    }
    let barrier = std::env::temp_dir().join(format!(
        "eventlog-killed-projector-{}-{}",
        std::process::id(),
        time::OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let mut child = std::process::Command::new(std::env::current_exe().expect("binary"))
        .args([
            "--exact",
            "killed_projector_restarts_competing_workers_without_partial_view_or_cursor",
            "--nocapture",
        ])
        .env("EVENTLOG_PROJECTOR_CHILD", &barrier)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("projector process");
    let started = Instant::now();
    while !barrier.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "child wrote inside transaction"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    child.kill().expect("kill during uncommitted batch");
    let killed = child.wait_with_output().expect("child stopped");
    assert!(!killed.status.success());
    std::fs::write(barrier.with_extension("stdout"), killed.stdout).expect("child log");
    std::fs::write(barrier.with_extension("stderr"), killed.stderr).expect("child log");
    assert!(
        store
            .projection_get(&eventlog_conformance::TALLY, &tenant, "item/retained")
            .await
            .expect("old active view")
            .is_none()
    );
    let other = Arc::new(
        PostgresEventStore::connect(&url, "process_projector")
            .await
            .expect("independent resumed connection"),
    );
    let competitor = CatchUpRunner::new(other, Arc::new(eventlog_conformance::Tally))
        .await
        .expect("competing runner")
        .with_batch(4);
    let restarted = Instant::now();
    loop {
        let (first, second) = tokio::join!(runner.run_once(&tenant), competitor.run_once(&tenant));
        let (first, second) = (first.expect("runner"), second.expect("competitor"));
        if first.position.max(second.position) >= last {
            break;
        }
        assert!(
            restarted.elapsed() < Duration::from_secs(10),
            "bounded replay"
        );
    }
    let before = store
        .projection_get(&eventlog_conformance::TALLY, &tenant, "item/retained")
        .await
        .expect("view")
        .expect("row");
    assert_eq!(before["count"], 20);
    assert_eq!(
        store
            .rebuild_projection(Arc::new(eventlog_conformance::Tally), &tenant)
            .await
            .expect("full fold"),
        20
    );
    assert_eq!(
        store
            .projection_get(&eventlog_conformance::TALLY, &tenant, "item/retained")
            .await
            .expect("rebuilt"),
        Some(before)
    );
}

#[tokio::test]
async fn caught_reservation_cancellation_cannot_commit_an_unchecked_append() {
    use eventlog_core::{AdmissionScope, EventLogError, Reservation};
    use std::{sync::Arc, time::Duration};
    struct Catches {
        permit: eventlog_core::AdmissionPermit,
        reservation: Reservation,
        caught: Arc<tokio::sync::Notify>,
    }
    impl eventlog_core::Guard for Catches {
        fn check<'a>(
            &'a self,
            store: &'a mut dyn eventlog_core::ProjectionStore,
        ) -> eventlog_core::BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(30),
                        store.reserve(&self.permit, std::slice::from_ref(&self.reservation))
                    )
                    .await
                    .is_err()
                );
                self.caught.notify_one();
                Ok(())
            })
        }
    }
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let store = store("caught_cancel").await.expect("fixture");
    let tenant = TenantId::new("cancellation").expect("tenant");
    let scope = AdmissionScope::Tenant {
        tenant: tenant.clone(),
        key: "quota".into(),
    };
    let stream = StreamId::new(tenant.clone(), "item", "seed").expect("stream");
    store
        .append_guarded(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 0)],
            &eventlog_conformance::meta("seed", &serde_json::json!({})),
            Arc::new(eventlog_conformance::ReserveScopes {
                permit: store.admission_permit(),
                reservations: vec![Reservation {
                    scope: scope.clone(),
                    delta: 0,
                    ceiling: 1,
                }],
            }),
        )
        .await
        .expect("zero counter");
    let ready = Arc::new(tokio::sync::Notify::new());
    let caught = Arc::new(tokio::sync::Notify::new());
    let (ready_child, caught_child) = (ready.clone(), caught.clone());
    let coordinate = scope.coordinate().expect("coordinate");
    let lock = tokio::spawn(async move {
        let mut sql = client(&url).await;
        let transaction = sql.transaction().await.expect("lock transaction");
        transaction
            .query_one(
                "SELECT held FROM caught_cancel_scope_counters WHERE coordinate=$1 FOR UPDATE",
                &[&coordinate],
            )
            .await
            .expect("counter lock");
        ready_child.notify_one();
        caught_child.notified().await;
        transaction.commit().await.expect("release lock");
    });
    ready.notified().await;
    let attempted = StreamId::new(tenant.clone(), "item", "cancelled").expect("stream");
    let result = store
        .append_guarded(
            &attempted,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("cancelled", &serde_json::json!({})),
            Arc::new(Catches {
                permit: store.admission_permit(),
                reservation: Reservation {
                    scope,
                    delta: 1,
                    ceiling: 1,
                },
                caught,
            }),
        )
        .await;
    assert!(
        matches!(result, Err(EventLogError::Invalid(_))),
        "unfinished reservation must poison append: {result:?}"
    );
    lock.await.expect("lock owner");
    assert!(
        store
            .read_stream(&attempted, 0, 10)
            .await
            .expect("reopen read")
            .events
            .is_empty()
    );
    let sql = client(&std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("URL")).await;
    assert_eq!(
        sql.query_one("SELECT held FROM caught_cancel_scope_counters", &[])
            .await
            .expect("counter unchanged")
            .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
async fn missing_scope_bootstrap_is_atomic_across_independent_processes() {
    use eventlog_core::{AdmissionScope, Reservation};
    use std::{
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    };
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    if let Ok(worker) = std::env::var("EVENTLOG_SCOPE_CHILD") {
        let directory = PathBuf::from(std::env::var_os("EVENTLOG_SCOPE_BARRIER").expect("barrier"));
        let prefix = std::env::var("EVENTLOG_SCOPE_PREFIX").expect("prefix");
        let store = PostgresEventStore::connect(&url, &prefix)
            .await
            .expect("child connection");
        let tenant = TenantId::new(format!("child-{worker}")).expect("tenant");
        let stream = StreamId::new(tenant.clone(), "item", "same").expect("stream");
        let guard = Arc::new(eventlog_conformance::ReserveScopes {
            permit: store.admission_permit(),
            reservations: vec![
                Reservation {
                    scope: AdmissionScope::Tenant {
                        tenant,
                        key: "service/quota".into(),
                    },
                    delta: 1,
                    ceiling: 1,
                },
                Reservation {
                    scope: AdmissionScope::Deployment {
                        key: "shared".into(),
                    },
                    delta: 1,
                    ceiling: 1,
                },
            ],
        });
        std::fs::write(directory.join(format!("ready-{worker}")), b"ready").expect("ready");
        let start = Instant::now();
        while !directory.join("start").exists() {
            assert!(start.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let outcome = store
            .append_guarded(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("command", &serde_json::json!({"value":1})),
                guard,
            )
            .await;
        assert!(
            outcome.is_ok() || matches!(outcome, Err(eventlog_core::EventLogError::Invalid(_))),
            "typed admission result: {outcome:?}"
        );
        std::fs::write(
            directory.join(format!("result-{worker}")),
            if outcome.is_ok() {
                "accepted"
            } else {
                "refused"
            },
        )
        .expect("receipt");
        store.shutdown().await.expect("child shutdown");
        return;
    }
    let _exclusive = EXCLUSIVE.lock().await;
    for index in 0..3_u8 {
        let prefix = format!("process_scope_{}", char::from(b'a' + index));
        store(&prefix)
            .await
            .expect("fresh schema")
            .shutdown()
            .await
            .expect("closed setup");
        let directory = std::env::temp_dir().join(format!(
            "eventlog-scope-{}-{index}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        std::fs::create_dir(&directory).expect("barrier directory");
        let mut children = Vec::new();
        for worker in ["a", "b"] {
            children.push(
                std::process::Command::new(std::env::current_exe().expect("test binary"))
                    .args([
                        "--exact",
                        "missing_scope_bootstrap_is_atomic_across_independent_processes",
                        "--nocapture",
                    ])
                    .env("EVENTLOG_SCOPE_CHILD", worker)
                    .env("EVENTLOG_SCOPE_BARRIER", &directory)
                    .env("EVENTLOG_SCOPE_PREFIX", &prefix)
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                    .expect("independent process"),
            );
        }
        let start = Instant::now();
        while !directory.join("ready-a").exists() || !directory.join("ready-b").exists() {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "both processes ready"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        std::fs::write(directory.join("start"), b"race").expect("release barrier");
        for (index, child) in children.into_iter().enumerate() {
            let output = child.wait_with_output().expect("child completion");
            std::fs::write(
                directory.join(format!("child-{index}.stdout")),
                &output.stdout,
            )
            .expect("raw child log");
            std::fs::write(
                directory.join(format!("child-{index}.stderr")),
                &output.stderr,
            )
            .expect("raw child log");
            assert!(
                output.status.success(),
                "child failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let outcomes = ["a", "b"].map(|worker| {
            std::fs::read_to_string(directory.join(format!("result-{worker}")))
                .expect("child receipt")
        });
        assert_eq!(
            outcomes
                .iter()
                .filter(|value| value.as_str() == "accepted")
                .count(),
            1
        );
        let sql = client(&url).await;
        for suffix in ["events", "commands"] {
            assert_eq!(
                sql.query_one(&format!("SELECT count(*) FROM {prefix}_{suffix}"), &[])
                    .await
                    .expect("durable count")
                    .get::<_, i64>(0),
                1
            );
        }
        let counters = sql
            .query_one(
                &format!("SELECT count(*),sum(held)::bigint FROM {prefix}_scope_counters"),
                &[],
            )
            .await
            .expect("atomic scopes");
        assert_eq!(counters.get::<_, i64>(0), 2);
        assert_eq!(counters.get::<_, i64>(1), 2);
    }
}

#[tokio::test]
async fn lost_commit_response_reconnects_to_exact_durable_receipt() {
    use std::{
        io::{Read, Write},
        net::{Shutdown, TcpListener, TcpStream},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let setup = store("lost_commit").await.expect("fixture");
    setup.shutdown().await.expect("closed");
    let parsed: tokio_postgres::Config = url.parse().expect("config");
    let upstream_port = parsed.get_ports()[0];
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("isolated response-loss proxy");
    let proxy_port = listener.local_addr().expect("address").port();
    let armed = Arc::new(AtomicBool::new(false));
    let intercepted = Arc::new(AtomicBool::new(false));
    let proxy_arm = armed.clone();
    let proxy_intercepted = intercepted.clone();
    let proxy = std::thread::spawn(move || {
        let (mut downstream, _) = listener.accept().expect("one test client");
        let mut upstream = TcpStream::connect(("127.0.0.1", upstream_port)).expect("test upstream");
        downstream.set_nodelay(true).expect("no coalescing");
        upstream.set_nodelay(true).expect("no coalescing");
        downstream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .expect("deadline");
        upstream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .expect("deadline");
        let mut inbound = downstream.try_clone().expect("downstream reader");
        let mut outbound = upstream.try_clone().expect("upstream writer");
        let forward = std::thread::spawn(move || {
            let _ = std::io::copy(&mut inbound, &mut outbound);
            let _ = outbound.shutdown(Shutdown::Both);
        });
        loop {
            let mut header = [0_u8; 5];
            if upstream.read_exact(&mut header).is_err() {
                break;
            }
            let length = u32::from_be_bytes(header[1..].try_into().expect("length"));
            assert!(
                (4..=16 * 1024 * 1024).contains(&length),
                "bounded PostgreSQL frame"
            );
            let mut body = vec![0; usize::try_from(length - 4).expect("frame")];
            if upstream.read_exact(&mut body).is_err() {
                break;
            }
            if header[0] == b'C' && body == b"COMMIT\0" && proxy_arm.load(Ordering::Acquire) {
                proxy_intercepted.store(true, Ordering::Release);
                break;
            }
            if downstream
                .write_all(&header)
                .and_then(|()| downstream.write_all(&body))
                .is_err()
            {
                break;
            }
        }
        let _ = upstream.shutdown(Shutdown::Both);
        let _ = downstream.shutdown(Shutdown::Both);
        forward.join().expect("forwarder stopped");
    });
    let proxy_url = format!("host=127.0.0.1 port={proxy_port} user=postgres dbname=postgres");
    let connected = PostgresEventStore::connect(&proxy_url, "lost_commit")
        .await
        .expect("proxy store");
    let stream = StreamId::new(
        TenantId::new("lost-response").expect("tenant"),
        "item",
        "one",
    )
    .expect("stream");
    let events = [eventlog_conformance::event("item.received", 1)];
    let meta = eventlog_conformance::meta("same-command", &serde_json::json!({"value":1}));
    armed.store(true, Ordering::Release);
    assert_eq!(
        connected
            .append(&stream, Expected::NoStream, &events, &meta)
            .await,
        Err(eventlog_core::EventLogError::UnknownCommit)
    );
    assert!(
        intercepted.load(Ordering::Acquire),
        "the server committed before the response was removed"
    );
    connected.shutdown().await.expect("quarantine drained");
    proxy.join().expect("proxy terminated");
    let reopened = PostgresEventStore::connect(&url, "lost_commit")
        .await
        .expect("reconnect");
    let persisted = reopened
        .read_stream(&stream, 0, 10)
        .await
        .expect("durable read");
    assert_eq!(persisted.events.len(), 1);
    let replay = reopened
        .append(&stream, Expected::NoStream, &events, &meta)
        .await
        .expect("resolve unknown commit by exact retry");
    assert!(replay.deduplicated);
    assert_eq!(replay.events, persisted.events);
}

#[tokio::test]
async fn legacy_populated_schema_migrates_atomically_and_unknown_checksums_refuse() {
    use eventlog_postgres::{PoolOptions, PostgresConfig};
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let original = store("schema_upgrade").await.expect("setup");
    let stream = StreamId::new(
        TenantId::new("schema-tenant").expect("tenant"),
        "item",
        "original",
    )
    .expect("stream");
    let before = original
        .append(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("original", &serde_json::json!({})),
        )
        .await
        .expect("old data");
    original.shutdown().await.expect("old writer stopped");
    let sql = client(&url).await;
    sql.batch_execute("DROP TABLE schema_upgrade_schema_version; DROP TABLE schema_upgrade_scope_counters; DROP TABLE schema_upgrade_projection_registry; DROP TABLE schema_upgrade_snapshot_generations; DROP TABLE schema_upgrade_append_groups").await.expect("exact original populated schema");
    let config = PostgresConfig::isolated(&url, "schema_upgrade").expect("config");
    let (a, b) = tokio::join!(
        PostgresEventStore::migrate(config.clone(), PoolOptions::default(), &[]),
        PostgresEventStore::migrate(config, PoolOptions::default(), &[])
    );
    a.expect("first migrator");
    b.expect("second migrator");
    let migrated = PostgresEventStore::connect(&url, "schema_upgrade")
        .await
        .expect("migrated schema");
    assert_eq!(
        before.events,
        migrated
            .read_stream(&stream, 0, 10)
            .await
            .expect("old reader")
            .events
    );
    migrated.shutdown().await.expect("closed");
    sql.batch_execute("UPDATE schema_upgrade_schema_version SET checksum='unrecognized'")
        .await
        .expect("unknown version fixture");
    assert!(
        PostgresEventStore::connect(&url, "schema_upgrade")
            .await
            .is_err(),
        "unknown version/checksum refuses before serving"
    );
    sql.batch_execute("DROP TABLE schema_upgrade_schema_version; DROP TABLE schema_upgrade_scope_counters; DROP TABLE schema_upgrade_projection_registry; DROP TABLE schema_upgrade_snapshot_generations; DROP TABLE schema_upgrade_append_groups").await.expect("restore old fixture");
    PostgresEventStore::migrate(
        PostgresConfig::isolated(&url, "schema_upgrade").expect("config"),
        PoolOptions::default(),
        &[],
    )
    .await
    .expect("supported old migration remains intact");
}

#[tokio::test]
async fn schema_admission_refuses_triggers_policies_generation_and_foreign_sequences() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let sql = client(&url).await;
    for (prefix, mutation) in [
        (
            "shape_sequence_cache",
            "ALTER SEQUENCE shape_sequence_cache_events_global_seq_seq CACHE 2",
        ),
        (
            "shape_collation",
            "ALTER TABLE shape_collation_events ALTER COLUMN tenant_id TYPE TEXT COLLATE \"C\"",
        ),
        (
            "shape_unlogged",
            "ALTER TABLE shape_unlogged_events SET UNLOGGED",
        ),
        (
            "shape_generated",
            "ALTER TABLE shape_generated_events ADD COLUMN hidden BIGINT GENERATED ALWAYS AS (version + 1) STORED",
        ),
        (
            "shape_trigger",
            "CREATE FUNCTION shape_trigger_hook() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; CREATE TRIGGER hidden BEFORE INSERT ON shape_trigger_events FOR EACH ROW EXECUTE FUNCTION shape_trigger_hook()",
        ),
        (
            "shape_rls",
            "ALTER TABLE shape_rls_events ENABLE ROW LEVEL SECURITY",
        ),
        (
            "shape_sequence",
            "CREATE SEQUENCE shape_sequence_foreign; ALTER TABLE shape_sequence_events ALTER COLUMN global_seq SET DEFAULT nextval('shape_sequence_foreign')",
        ),
    ] {
        // Fixtures use an independently named disposable namespace for each altered shape.
        let setup = store(prefix).await.expect("admitted original shape");
        setup.shutdown().await.expect("closed");
        if prefix == "shape_trigger" {
            sql.batch_execute("DROP FUNCTION IF EXISTS shape_trigger_hook() CASCADE")
                .await
                .expect("fixture reset");
        }
        if prefix == "shape_sequence" {
            sql.batch_execute("DROP SEQUENCE IF EXISTS shape_sequence_foreign CASCADE")
                .await
                .expect("fixture reset");
        }
        sql.batch_execute(mutation)
            .await
            .expect("unknown schema shape");
        assert!(
            PostgresEventStore::connect(&url, prefix).await.is_err(),
            "altered shape {prefix} must refuse before serving"
        );
        if prefix == "shape_trigger" {
            sql.batch_execute("DROP FUNCTION shape_trigger_hook() CASCADE")
                .await
                .expect("fixture cleanup");
        }
        if prefix == "shape_rls" {
            sql.batch_execute("ALTER TABLE shape_rls_events DISABLE ROW LEVEL SECURITY")
                .await
                .expect("fixture cleanup");
        }
        if prefix == "shape_sequence" {
            sql.batch_execute("ALTER TABLE shape_sequence_events ALTER COLUMN global_seq SET DEFAULT nextval('shape_sequence_events_global_seq_seq'); DROP SEQUENCE shape_sequence_foreign").await.expect("fixture cleanup");
        }
    }
}

struct ReviewPoolHold {
    entered: tokio::sync::mpsc::Sender<()>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

impl eventlog_core::Guard for ReviewPoolHold {
    fn check<'a>(
        &'a self,
        _: &'a mut dyn eventlog_core::ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), eventlog_core::EventLogError>> {
        Box::pin(async move {
            let release = self.release.notified();
            tokio::pin!(release);
            release.as_mut().enable();
            self.entered.send(()).await.expect("review receiver");
            release.await;
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn shutdown_cancellation_preserves_public_pool_lifetimes() {
    use eventlog_core::EventLogError;
    use std::{
        future::Future,
        sync::Arc,
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };
    let _exclusive = EXCLUSIVE.lock().await;
    let url = url().expect("required real PostgreSQL review fixture");
    let prefix = "review_pool_shutdown";
    store(prefix).await.unwrap().shutdown().await.unwrap();
    let options = eventlog_postgres::PoolOptions {
        max_connections: 1,
        max_waiters: 1,
        acquisition_timeout: Duration::from_millis(100),
        shutdown_timeout: Duration::from_millis(100),
        ..eventlog_postgres::PoolOptions::default()
    };
    let adapter = Arc::new(
        PostgresEventStore::connect_local(&url, prefix, options.clone())
            .await
            .unwrap(),
    );
    let tenant = TenantId::new("review-tenant").unwrap();
    let stream = StreamId::new(tenant, "item", "held").unwrap();
    let (entered, mut receiver) = tokio::sync::mpsc::channel(1);
    let release = Arc::new(tokio::sync::Notify::new());
    let guard = Arc::new(ReviewPoolHold {
        entered,
        release: release.clone(),
    });
    let writing = adapter.clone();
    let written_stream = stream.clone();
    let writer = tokio::spawn(async move {
        writing
            .append_guarded(
                &written_stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("review-shutdown", &serde_json::json!({})),
                guard,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let mut queued = Box::pin(adapter.stream_version(&stream));
    assert!(matches!(
        queued
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert_eq!(adapter.pool_status().waiting, 1);
    let mut cancelled_shutdown = Box::pin(adapter.shutdown());
    assert!(matches!(
        cancelled_shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert!(adapter.pool_status().closed);
    drop(cancelled_shutdown);
    assert_eq!(queued.await, Err(EventLogError::Closed));
    assert_eq!(adapter.pool_status().waiting, 0);
    assert_eq!(adapter.pool_status().checked_out, 1);
    assert_eq!(
        adapter.stream_version(&stream).await,
        Err(EventLogError::Closed)
    );
    let started = Instant::now();
    let (first, second) = tokio::join!(adapter.shutdown(), adapter.shutdown());
    for result in [first, second] {
        assert_eq!(
            result,
            Err(EventLogError::Deadline {
                operation: "shutdown"
            })
        );
    }
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(adapter.pool_status().checked_out, 1);
    release.notify_waiters();
    let committed = writer.await.unwrap().unwrap();
    assert_eq!(committed.last_version, 1);
    let (first, second) = tokio::join!(adapter.shutdown(), adapter.shutdown());
    first.unwrap();
    second.unwrap();
    let status = adapter.pool_status();
    assert!(status.closed);
    assert_eq!((status.checked_out, status.waiting, status.idle), (0, 0, 0));
    assert_eq!(
        adapter.read_feed(stream.tenant(), 0, 1).await,
        Err(EventLogError::Closed)
    );
    let reopened = PostgresEventStore::connect_local(&url, prefix, options)
        .await
        .unwrap();
    let replay = reopened
        .recorded_command(
            &stream,
            "review-shutdown",
            &eventlog_conformance::meta("review-shutdown", &serde_json::json!({})).request_hash,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.events, committed.events);
    reopened.shutdown().await.unwrap();
    eprintln!(
        "public shutdown cancellation: queued Closed; two bounded shutdown deadlines; accepted command retained; repeated shutdown drained 0/0/0"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn public_queue_cancellation_and_broken_idle_reclaim_exact_capacity() {
    use eventlog_core::EventLogError;
    use std::{
        future::Future,
        sync::Arc,
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };
    let _exclusive = EXCLUSIVE.lock().await;
    let url = url().expect("required real PostgreSQL review fixture");
    let prefix = "review_pool_queue";
    store(prefix).await.unwrap().shutdown().await.unwrap();
    let options = eventlog_postgres::PoolOptions {
        max_connections: 1,
        max_waiters: 1,
        acquisition_timeout: Duration::from_millis(75),
        ..eventlog_postgres::PoolOptions::default()
    };
    let tagged_url = format!(
        "{url}{}application_name=eventlog_pool_review_queue",
        if url.contains('?') { '&' } else { '?' }
    );
    let adapter = Arc::new(
        PostgresEventStore::connect_local(&tagged_url, prefix, options)
            .await
            .unwrap(),
    );
    let stream = StreamId::new(TenantId::new("review-tenant").unwrap(), "item", "queued").unwrap();
    let (entered, mut receiver) = tokio::sync::mpsc::channel(1);
    let release = Arc::new(tokio::sync::Notify::new());
    let writing = adapter.clone();
    let written_stream = stream.clone();
    let guard = Arc::new(ReviewPoolHold {
        entered,
        release: release.clone(),
    });
    let writer = tokio::spawn(async move {
        writing
            .append_guarded(
                &written_stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("review-queue", &serde_json::json!({})),
                guard,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    for _ in 0..4 {
        let mut cancelled = Box::pin(adapter.stream_version(&stream));
        assert!(matches!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let state = adapter.pool_status();
        assert_eq!((state.checked_out, state.waiting, state.idle), (1, 1, 0));
        assert_eq!(
            adapter.stream_version(&stream).await,
            Err(EventLogError::Overloaded)
        );
        drop(cancelled);
        assert_eq!(adapter.pool_status().waiting, 0);
        let started = Instant::now();
        assert_eq!(
            adapter.stream_version(&stream).await,
            Err(EventLogError::Deadline {
                operation: "pool acquisition"
            })
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        let state = adapter.pool_status();
        assert_eq!((state.checked_out, state.waiting, state.idle), (1, 0, 0));
    }
    let mut granted = Box::pin(adapter.stream_version(&stream));
    assert!(matches!(
        granted
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    release.notify_waiters();
    let committed = writer.await.unwrap().unwrap();
    let state = adapter.pool_status();
    assert_eq!((state.checked_out, state.waiting, state.idle), (0, 1, 1));
    drop(granted);
    let state = adapter.pool_status();
    assert_eq!((state.checked_out, state.waiting, state.idle), (0, 0, 1));
    assert_eq!(adapter.stream_version(&stream).await.unwrap(), Some(1));
    let observer = client(&url).await;
    let rows = observer.query("SELECT pid FROM pg_stat_activity WHERE application_name = 'eventlog_pool_review_queue'", &[]).await.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "one idle server connection belongs to this exact test adapter"
    );
    let pid: i32 = rows[0].get(0);
    assert!(
        observer
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let started = Instant::now();
    while observer
        .query_opt("SELECT pid FROM pg_stat_activity WHERE pid = $1", &[&pid])
        .await
        .unwrap()
        .is_some()
    {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the fixture's terminated backend never exited"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut transport_refusals = 0;
    loop {
        match adapter.stream_version(&stream).await {
            Ok(Some(1)) => break,
            Err(EventLogError::Backend(_)) => transport_refusals += 1,
            result => panic!("broken-idle recovery changed its typed outcome: {result:?}"),
        }
        assert!(started.elapsed() < Duration::from_secs(2));
        let state = adapter.pool_status();
        assert!(
            state.checked_out + state.idle <= 1 && state.waiting <= 1,
            "{state:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let replay = adapter
        .recorded_command(
            &stream,
            "review-queue",
            &eventlog_conformance::meta("review-queue", &serde_json::json!({})).request_hash,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.events, committed.events);
    adapter.shutdown().await.unwrap();
    let state = adapter.pool_status();
    assert_eq!((state.checked_out, state.waiting, state.idle), (0, 0, 0));
    eprintln!(
        "public queue: four cancellations, four acquisition deadlines, four overloads, one granted-unpolled cancellation, own idle backend {pid} terminated; reconnect transport_refusals={transport_refusals}"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_history_and_repository_privacy_interleavings() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(adapter) = store("snapshot_races").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let store: std::sync::Arc<dyn EventStore> = std::sync::Arc::new(adapter);
    eventlog_conformance::run_snapshot_generations(store.clone()).await;
    let tenant = eventlog_core::TenantId::new("snapshot-races").unwrap();
    for automatic in [true, false] {
        let id = if automatic {
            "automatic-postgres"
        } else {
            "explicit-postgres"
        };
        let repository =
            eventlog_core::Repository::<eventlog_conformance::SnapshotCounter>::new(store.clone())
                .with_policy(eventlog_core::SnapshotPolicy {
                    every: u64::from(automatic),
                });
        if !automatic {
            repository
                .handle(
                    &tenant,
                    id,
                    &(),
                    &eventlog_conformance::meta(id, &serde_json::json!({})),
                )
                .await
                .unwrap();
        }
        let stream = repository.stream(&tenant, id).unwrap();
        let (observed, resume) = eventlog_conformance::pause_snapshot_serialization(id);
        let owner = tenant.clone();
        let task = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                if automatic {
                    repository
                        .handle(
                            &owner,
                            id,
                            &(),
                            &eventlog_conformance::meta(id, &serde_json::json!({})),
                        )
                        .await
                        .map(|outcome| outcome.version)
                } else {
                    repository.snapshot_now(&owner, id).await
                }
            })
        });
        tokio::task::spawn_blocking(move || {
            observed
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        })
        .await
        .unwrap();
        store.redact(&stream, 1, "privacy").await.unwrap();
        resume.send(()).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), 1);
        let loaded =
            eventlog_core::Repository::<eventlog_conformance::SnapshotCounter>::new(store.clone())
                .load(&tenant, id)
                .await
                .unwrap();
        assert_eq!(
            loaded.state.total, 0,
            "delayed repository cache must agree with redacted fold"
        );
        if automatic {
            assert!(store.load_snapshot(&stream).await.unwrap().is_none());
        } else {
            assert_eq!(
                store.load_snapshot(&stream).await.unwrap().unwrap().state["total"],
                serde_json::json!(0)
            );
        }
    }
}
#[tokio::test]
async fn committed_commands_survive_snapshot_storage_failure() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(adapter) = store("cache_failure").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let store = std::sync::Arc::new(adapter);
    let sql = client(&url().unwrap()).await;
    sql.batch_execute("CREATE OR REPLACE FUNCTION cache_failure_injected() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected cache failure'; END; $$; CREATE TRIGGER cache_failure_injected BEFORE INSERT ON cache_failure_snapshots FOR EACH ROW EXECUTE FUNCTION cache_failure_injected();").await.unwrap();
    let repository =
        eventlog_core::Repository::<eventlog_conformance::SnapshotCounter>::new(store.clone())
            .with_policy(eventlog_core::SnapshotPolicy { every: 1 });
    let tenant = TenantId::new("tenant").unwrap();
    assert_eq!(
        repository
            .handle(
                &tenant,
                "failure",
                &(),
                &eventlog_conformance::meta("failure", &serde_json::json!({}))
            )
            .await
            .unwrap()
            .version,
        1
    );
    assert!(repository.snapshot_now(&tenant, "failure").await.is_err());
    assert_eq!(
        repository
            .load(&tenant, "failure")
            .await
            .unwrap()
            .state
            .total,
        1
    );
    assert_eq!(sql.query_one("SELECT count(*) FROM cache_failure_snapshot_generations WHERE cached_generation IS NOT NULL", &[]).await.unwrap().get::<_, i64>(0), 0, "failed cache write rolls back its proof");
    sql.batch_execute("DROP TRIGGER cache_failure_injected ON cache_failure_snapshots; DROP FUNCTION cache_failure_injected()").await.unwrap();
}
#[tokio::test]
async fn snapshot_capture_waits_for_complete_tenant_erasure() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(adapter) = store("snapshot_erasure").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let store = std::sync::Arc::new(adapter);
    let tenant = TenantId::new("tenant").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    store
        .append(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("one", &serde_json::json!({})),
        )
        .await
        .unwrap();
    let mut sql = client(&url().unwrap()).await;
    let transaction = sql.transaction().await.unwrap();
    transaction
        .query("SELECT * FROM snapshot_erasure_events FOR UPDATE", &[])
        .await
        .unwrap();
    let eraser = store.clone();
    let owner = tenant.clone();
    let erasure = tokio::spawn(async move { eraser.forget_tenant(&owner).await });
    let observer = client(&url().unwrap()).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let waiting: bool = observer.query_one("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE query LIKE 'DELETE FROM snapshot_erasure_events%' AND wait_event_type='Lock')", &[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "erasure must reach its blocked DELETE"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let capturer = store.clone();
    let empty = StreamId::new(tenant.clone(), "item", "empty").unwrap();
    let observed_stream = empty.clone();
    let mut capture =
        tokio::spawn(async move { capturer.snapshot_generation(&observed_stream).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut capture)
            .await
            .is_err(),
        "capture cannot create a token in the middle of erasure"
    );
    transaction.commit().await.unwrap();
    erasure.await.unwrap().unwrap();
    let generation = capture.await.unwrap().unwrap().unwrap();
    assert_eq!(
        store.snapshot_generation(&empty).await.unwrap().unwrap(),
        generation
    );
    assert!(
        store
            .read_stream(&stream, 0, 100)
            .await
            .unwrap()
            .events
            .is_empty()
    );
}
