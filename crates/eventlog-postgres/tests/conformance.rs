//! The shared exercise against a real PostgreSQL, plus the one property SQLite cannot show.
//!
//! Set `EVENTLOG_TEST_POSTGRES_URL` to run these. Without it they report themselves as not run
//! rather than passing quietly, because a backend nobody exercised is not a backend anybody proved.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use eventlog_core::{BoxFuture, EventLogError, EventStore, Expected, StreamId, TenantId};
use eventlog_postgres::{
    AuthorizedConnection, PostgresConnectionAuthority, PostgresEventStore,
    PostgresTransportAssurance,
};
use tokio_postgres::NoTls;

struct VerifiedTlsAuthority {
    config: tokio_postgres::Config,
    tls: rustls::ClientConfig,
    calls: AtomicUsize,
}

impl PostgresConnectionAuthority for VerifiedTlsAuthority {
    fn assurance(&self) -> PostgresTransportAssurance {
        PostgresTransportAssurance::Verified
    }

    fn connect(
        &self,
        timeout: std::time::Duration,
    ) -> BoxFuture<'_, Result<AuthorizedConnection, EventLogError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let mut config = self.config.clone();
        config.connect_timeout(timeout);
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(self.tls.clone());
        Box::pin(async move {
            let (client, driver) = config.connect(tls).await.map_err(|_| {
                EventLogError::Backend("caller-owned verified TLS connection failed".to_owned())
            })?;
            Ok(AuthorizedConnection::new(client, driver))
        })
    }
}

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
    observer.batch_execute("CREATE RULE discard_receipt AS ON INSERT TO review_rules_hosted.kit_commands DO INSTEAD NOTHING; GRANT USAGE ON SCHEMA review_rules_hosted TO eventlog_test_application; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA review_rules_hosted TO eventlog_test_application; REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON review_rules_hosted.kit_schema_version, review_rules_hosted.kit_projection_registry, review_rules_hosted.kit_settlement_floor FROM eventlog_test_application; GRANT USAGE ON ALL SEQUENCES IN SCHEMA review_rules_hosted TO eventlog_test_application").await.unwrap();
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

/// Follow the feed from `cursor` until it has delivered `expected` events, or fail saying so.
///
/// The commit watermark is cluster-wide (`AGENTS.md`, *The watermark couples feed latency across
/// owners*): a transaction open anywhere in the instance, a sibling case in this lane or another
/// gate on the same server, withholds committed events from every feed. One read after every
/// writer committed asks whether the machine happened to be idle. This follows each returned
/// cursor and never rereads a position it has passed, so a cursor that skipped a position still
/// fails here, after the bound, instead of being repaired by a read from the start. It stops at
/// `expected`; the caller's exact assertion on the result is what refuses a surplus.
async fn feed_from(
    store: &PostgresEventStore,
    tenant: &TenantId,
    mut cursor: u64,
    mut delivered: Vec<eventlog_core::RecordedEvent>,
    expected: usize,
) -> Vec<eventlog_core::RecordedEvent> {
    for _ in 0..200 {
        if delivered.len() >= expected {
            return delivered;
        }
        let page = store.read_feed(tenant, cursor, 10).await.expect("readable");
        cursor = page.next_position;
        delivered.extend(page.events);
        if delivered.len() >= expected {
            return delivered;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!(
        "the feed never delivered {expected} events from its cursor; it delivered {}",
        delivered.len()
    );
}

async fn downgrade_blob_table(sql: &tokio_postgres::Client, prefix: &str) {
    sql.batch_execute(&format!(
        "BEGIN;
         ALTER TABLE {prefix}_blobs RENAME TO {prefix}_blobs_current;
         CREATE TABLE {prefix}_blobs (
             tenant_id TEXT NOT NULL,digest TEXT NOT NULL,bytes BYTEA NOT NULL,
             byte_count BIGINT NOT NULL,recorded_at TIMESTAMPTZ NOT NULL,
             PRIMARY KEY(tenant_id,digest));
         INSERT INTO {prefix}_blobs(tenant_id,digest,bytes,byte_count,recorded_at)
             SELECT tenant_id,digest,bytes,byte_count,recorded_at FROM {prefix}_blobs_current;
         DROP TABLE {prefix}_blobs_current;
         UPDATE {prefix}_schema_version
             SET checksum='9d31a95af97bbdb3f675197383851f73758e1459166837171ef367c46530aa1f';
         COMMIT;"
    ))
    .await
    .expect("exact predecessor blob table");
}

async fn store(prefix: &str) -> Option<PostgresEventStore> {
    let url = url()?;
    let sql = client(&url).await;
    for suffix in [
        "events",
        "append_groups",
        "group_batches",
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
        "settlement_floor",
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
    // Read once on purpose: a withheld feed is the outcome asserted, and a transaction held
    // elsewhere in the instance can only withhold more, never deliver the fast writer early.
    let blocked = store.read_feed(&tenant, 0, 10).await.expect("readable");
    assert!(
        blocked.events.is_empty(),
        "a reader must not move past a position that is still in flight"
    );

    held.commit().await.expect("the slow writer commits second");

    let page = feed_from(&store, &tenant, 0, Vec::new(), 2).await;
    let seen: Vec<&str> = page.iter().map(|event| event.stream_id.as_str()).collect();
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

/// PostgreSQL kept a whole indexed value in a B-tree entry of about a third of a page, so a long
/// value under a long tenant aborted the append that wrote it (review L78). The index keys on a
/// fixed-size digest of the value now: 8,000 incompressible bytes under a tenant of the longest
/// accepted length are written with their event and found by their value.
#[tokio::test]
async fn an_8000_byte_indexed_value_under_the_longest_tenant_is_indexed_and_found() {
    struct WritesKind(TenantId, String);
    impl eventlog_core::Guard for WritesKind {
        fn check<'a>(
            &'a self,
            store: &'a mut dyn eventlog_core::ProjectionStore,
        ) -> BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                store
                    .upsert(
                        &eventlog_conformance::TALLY,
                        &self.0,
                        "large",
                        &serde_json::json!({ "kind": self.1 }),
                    )
                    .await
            })
        }
    }
    let Some(store) = store("idx_large").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["idx_large_p_tally"]).await;
    store
        .register_inline(Arc::new(eventlog_conformance::Tally))
        .await
        .expect("registered");
    let tenant = TenantId::new(format!(
        "idx-large-{}",
        "b".repeat(eventlog_core::MAX_FIELD_LEN - "idx-large-".len())
    ))
    .unwrap();
    let mut state: u64 = 0x2545_f491_4f6c_dd1d;
    let noise: String = (0..8_000)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'!' + u8::try_from(state % 94).unwrap())
        })
        .collect();
    let append = |id: &'static str, kind: String| {
        let stream = StreamId::new(tenant.clone(), "item", id).unwrap();
        let store = &store;
        let guard = Arc::new(WritesKind(tenant.clone(), kind));
        async move {
            let outcome = store
                .append_guarded(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", 1)],
                    &eventlog_conformance::meta(id, &serde_json::json!({})),
                    guard,
                )
                .await
                .map(|_| ());
            (outcome, store.stream_version(&stream).await.unwrap())
        }
    };
    assert_eq!(
        append("large", noise.clone()).await,
        (Ok(()), Some(1)),
        "an 8,000-byte indexed value under the longest tenant is written with its event"
    );
    let found = store
        .projection_find(&eventlog_conformance::TALLY, &tenant, "kind", &noise, 10)
        .await
        .unwrap();
    assert_eq!(
        found,
        [serde_json::json!({ "kind": noise })],
        "the row is found by its value"
    );
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
    drop_projection_tables(&store, &["paging_p_tally", "paging_p_page_rows"]).await;
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

/// `count` reservations of `delta` against `ceiling` on distinct tenant scopes named `key-N`.
fn quota_guard(
    store: &PostgresEventStore,
    tenant: &TenantId,
    count: usize,
    delta: i64,
    ceiling: i64,
) -> Arc<dyn eventlog_core::Guard> {
    use eventlog_core::{AdmissionScope, Reservation};
    Arc::new(eventlog_conformance::ReserveScopes {
        permit: store.admission_permit(),
        reservations: (0..count)
            .map(|n| Reservation {
                scope: AdmissionScope::Tenant {
                    tenant: tenant.clone(),
                    key: format!("key-{n}"),
                },
                delta,
                ceiling,
            })
            .collect(),
    })
}

/// A zero-delta reservation is a policy read: it takes the locks and checks the ceilings, and
/// writes nothing — no row for an absent scope, no new row version for an existing one.
#[tokio::test]
async fn a_zero_delta_quota_check_writes_no_counter_rows() {
    let Some(store) = store("quota_read").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let sql = client(&url().expect("fixture URL")).await;
    let tenant = TenantId::new("quota-read").unwrap();
    let append = |id: &'static str, guard| {
        let stream = StreamId::new(tenant.clone(), "item", id).unwrap();
        let store = &store;
        async move {
            store
                .append_guarded(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", 1)],
                    &eventlog_conformance::meta(id, &serde_json::json!({})),
                    guard,
                )
                .await
                .expect("admitted append")
        }
    };
    append("absent", quota_guard(&store, &tenant, 64, 0, 1)).await;
    let rows: i64 = sql
        .query_one("SELECT count(*) FROM quota_read_scope_counters", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 0, "a zero-delta check created counter rows");
    append("held", quota_guard(&store, &tenant, 1, 1, 1)).await;
    let version = || async {
        sql.query_one(
            "SELECT xmin::text, held FROM quota_read_scope_counters",
            &[],
        )
        .await
        .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
        .unwrap()
    };
    let before = version().await;
    append("recheck", quota_guard(&store, &tenant, 1, 0, 1)).await;
    assert_eq!(
        version().await,
        before,
        "a zero-delta check rewrote an unchanged counter"
    );
}

/// Forward one PostgreSQL connection and count the client's request boundaries: a simple
/// `Query` or an extended-protocol `Sync` is one round trip.
async fn count_round_trips(
    client: tokio::net::TcpStream,
    upstream: std::net::SocketAddr,
    round_trips: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = tokio::net::TcpStream::connect(upstream).await?;
    // Each message is forwarded as it arrives; without this, Nagle's algorithm holds every small
    // write for the peer's delayed acknowledgement, about a second per append.
    client.set_nodelay(true)?;
    server.set_nodelay(true)?;
    let (mut from_client, mut to_client) = client.into_split();
    let (mut from_server, mut to_server) = server.into_split();
    tokio::spawn(async move { tokio::io::copy(&mut from_server, &mut to_client).await });
    // Untyped startup packets come first; an SSL or GSS encryption request precedes the real one.
    loop {
        let mut packet = vec![0_u8; 4];
        from_client.read_exact(&mut packet).await?;
        let length = u32::from_be_bytes(packet[..4].try_into().unwrap());
        packet.resize(usize::try_from(length).unwrap(), 0);
        from_client.read_exact(&mut packet[4..]).await?;
        to_server.write_all(&packet).await?;
        let code = u32::from_be_bytes(packet[4..8].try_into().unwrap());
        if code != 80_877_103 && code != 80_877_104 {
            break;
        }
    }
    loop {
        let mut message = vec![0_u8; 5];
        from_client.read_exact(&mut message).await?;
        let length = u32::from_be_bytes(message[1..5].try_into().unwrap());
        message.resize(usize::try_from(length).unwrap() + 1, 0);
        from_client.read_exact(&mut message[5..]).await?;
        if message[0] == b'Q' || message[0] == b'S' {
            round_trips.fetch_add(1, Ordering::AcqRel);
        }
        to_server.write_all(&message).await?;
    }
}

/// A reservation costs one statement per phase — lock, read, write — whatever its scope count,
/// not three per scope: 64 scopes and one scope take the same number of round trips.
#[tokio::test]
async fn a_64_scope_reservation_takes_as_many_round_trips_as_one_scope() {
    let Some(direct) = store("quota_trips").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    direct.shutdown().await.unwrap();
    let url = url().expect("fixture URL");
    let config: tokio_postgres::Config = url.parse().unwrap();
    let tokio_postgres::config::Host::Tcp(host) = &config.get_hosts()[0] else {
        panic!("the round-trip count needs a TCP fixture");
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let upstream = tokio::net::lookup_host((host.as_str(), port))
        .await
        .unwrap()
        .next()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = listener.local_addr().unwrap();
    let round_trips = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&round_trips);
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            tokio::spawn(count_round_trips(client, upstream, Arc::clone(&counter)));
        }
    });
    let proxied = url.replacen(&format!("{host}:{port}"), &proxy.to_string(), 1);
    assert_ne!(proxied, url, "the fixture URL names its host and port");
    let store = PostgresEventStore::connect(&proxied, "quota_trips")
        .await
        .expect("a store behind the counting proxy");
    let tenant = TenantId::new("quota-trips").unwrap();
    let mut costs = Vec::new();
    for (id, scopes) in [("warm", 1), ("one", 1), ("many", 64)] {
        let stream = StreamId::new(tenant.clone(), "item", id).unwrap();
        let before = round_trips.load(Ordering::Acquire);
        store
            .append_guarded(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta(id, &serde_json::json!({})),
                quota_guard(&store, &tenant, scopes, 1, 3),
            )
            .await
            .expect("admitted append");
        costs.push(round_trips.load(Ordering::Acquire) - before);
    }
    assert_eq!(
        costs[2], costs[1],
        "round trips of an append reserving 64 scopes against one reserving 1 (warm-up {})",
        costs[0]
    );
    store.shutdown().await.unwrap();
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
    // Migration metadata is the migration role's: an application role that can write the
    // projection roster can unregister a projection, and erasure then skips its rows.
    for table in [
        "kit_schema_version",
        "kit_projection_registry",
        "kit_settlement_floor",
    ] {
        match PostgresEventStore::open(config.clone(), PoolOptions::default(), 16, 2, 8).await {
            Err(EventLogError::Invalid(reason)) => assert_eq!(
                reason,
                "hosted application role can write migration metadata reserved for the migration role",
                "{table}"
            ),
            Err(other) => panic!("{table}: refused for another reason: {other:?}"),
            Ok(store) => {
                store.shutdown().await.unwrap();
                panic!("{table}: admitted an application role that can write migration metadata");
            }
        }
        sql.batch_execute(&format!(
            "REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON hosted_owner.{table} FROM eventlog_test_application"
        ))
        .await
        .expect("migration metadata becomes read-only");
    }
    for privilege in ["DELETE", "TRUNCATE"] {
        sql.batch_execute(&format!(
            "GRANT {privilege} ON hosted_owner.kit_projection_registry TO eventlog_test_application"
        ))
        .await
        .unwrap();
        match PostgresEventStore::open(config.clone(), PoolOptions::default(), 16, 2, 8).await {
            Err(EventLogError::Invalid(reason)) => assert_eq!(
                reason,
                "hosted application role can write migration metadata reserved for the migration role",
                "{privilege}"
            ),
            Err(other) => panic!("{privilege}: refused for another reason: {other:?}"),
            Ok(store) => {
                store.shutdown().await.unwrap();
                panic!("{privilege}: admitted a role that can empty the projection roster");
            }
        }
        sql.batch_execute(&format!(
            "REVOKE {privilege} ON hosted_owner.kit_projection_registry FROM eventlog_test_application"
        ))
        .await
        .unwrap();
    }
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
    let mut caller_config: tokio_postgres::Config = app_url.parse().expect("application config");
    caller_config.ssl_mode(tokio_postgres::config::SslMode::Require);
    let caller_authority = Arc::new(VerifiedTlsAuthority {
        config: caller_config,
        tls: rustls::ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_no_client_auth(),
        calls: AtomicUsize::new(0),
    });
    let caller_owned = PostgresEventStore::open(
        PostgresConfig::caller_owned("hosted_owner", "kit", caller_authority.clone())
            .expect("caller-owned config"),
        PoolOptions::default(),
        16,
        2,
        8,
    )
    .await
    .expect("caller-owned verified TLS authority is admitted");
    let caller_stream = StreamId::new(
        TenantId::new("caller-owned-tenant").expect("tenant"),
        "item",
        "one",
    )
    .expect("stream");
    caller_owned
        .append(
            &caller_stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("caller-owned-tls", &serde_json::json!({})),
        )
        .await
        .expect("caller-owned TLS append");
    assert_eq!(
        caller_owned
            .read_stream(&caller_stream, 0, 10)
            .await
            .expect("caller-owned TLS read")
            .events
            .len(),
        1
    );
    caller_owned
        .shutdown()
        .await
        .expect("caller-owned shutdown");
    assert!(caller_authority.calls.load(Ordering::Acquire) >= 1);
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

/// A caller-owned authority that asserts verified transport and connects, call by call, as the
/// next principal in its list (the last one repeats).
struct ReconnectingAuthority {
    principals: Vec<tokio_postgres::Config>,
    calls: AtomicUsize,
}

impl PostgresConnectionAuthority for ReconnectingAuthority {
    fn assurance(&self) -> PostgresTransportAssurance {
        PostgresTransportAssurance::Verified
    }

    fn connect(
        &self,
        timeout: std::time::Duration,
    ) -> BoxFuture<'_, Result<AuthorizedConnection, EventLogError>> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        let mut config = self.principals[call.min(self.principals.len() - 1)].clone();
        config.connect_timeout(timeout);
        Box::pin(async move {
            let (client, driver) = config
                .connect(NoTls)
                .await
                .map_err(|error| EventLogError::Backend(error.to_string()))?;
            Ok(AuthorizedConnection::new(client, driver))
        })
    }
}

/// Hosted admission judges the first pooled session; every later physical session must be the
/// same database, role and schema and pass the same role rules before it is served. A
/// caller-owned authority that reconnects as the admitted role is served; one that reconnects as
/// a schema-owning superuser is refused.
#[tokio::test]
async fn a_hosted_pool_refuses_a_reconnected_session_of_another_principal() {
    use eventlog_postgres::{PoolOptions, PostgresConfig};
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let sql = client(&url).await;
    sql.batch_execute(
        "DROP SCHEMA IF EXISTS hosted_rebind CASCADE; CREATE SCHEMA hosted_rebind; \
         DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='eventlog_test_rebind') \
         THEN CREATE ROLE eventlog_test_rebind LOGIN CONNECTION LIMIT 8; END IF; END $$; \
         ALTER ROLE eventlog_test_rebind CONNECTION LIMIT 8;",
    )
    .await
    .expect("isolated role and schema");
    let owner: tokio_postgres::Config = url.parse().unwrap();
    let mut application = owner.clone();
    application.user("eventlog_test_rebind");
    let migration = Arc::new(ReconnectingAuthority {
        principals: vec![owner.clone()],
        calls: AtomicUsize::new(0),
    });
    PostgresEventStore::migrate(
        PostgresConfig::caller_owned("hosted_rebind", "kit", migration).unwrap(),
        PoolOptions::default(),
        &[],
    )
    .await
    .expect("migration role applies the schema");
    sql.batch_execute(
        "GRANT USAGE ON SCHEMA hosted_rebind TO eventlog_test_rebind; \
         GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA hosted_rebind TO eventlog_test_rebind; \
         REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON hosted_rebind.kit_schema_version, hosted_rebind.kit_projection_registry, hosted_rebind.kit_settlement_floor FROM eventlog_test_rebind; \
         GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA hosted_rebind TO eventlog_test_rebind",
    )
    .await
    .expect("DML-only grants");
    let authority = Arc::new(ReconnectingAuthority {
        principals: vec![application.clone(), application, owner],
        calls: AtomicUsize::new(0),
    });
    let store = PostgresEventStore::open(
        PostgresConfig::caller_owned("hosted_rebind", "kit", authority.clone()).unwrap(),
        PoolOptions::default(),
        16,
        2,
        8,
    )
    .await
    .expect("the application role is admitted");
    let tenant = TenantId::new("rebind").unwrap();
    // End the pool's idle session server-side, so its next lease opens a new physical session.
    let reconnect = || async {
        sql.batch_execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='eventlog_test_rebind'",
        )
        .await
        .unwrap();
        for _ in 0..200 {
            let left: i64 = sql
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE usename='eventlog_test_rebind'",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if left == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    let append = |id: &'static str| {
        let stream = StreamId::new(tenant.clone(), "item", id).unwrap();
        let store = &store;
        async move {
            let mut outcome = Err(EventLogError::Closed);
            // A lease that met the terminated session before its driver saw the close fails as a
            // backend error and retires it; the next attempt opens the new session.
            for _ in 0..5 {
                outcome = store
                    .append(
                        &stream,
                        Expected::NoStream,
                        &[eventlog_conformance::event("item.received", 1)],
                        &eventlog_conformance::meta(id, &serde_json::json!({})),
                    )
                    .await
                    .map(|_| ());
                if !matches!(outcome, Err(EventLogError::Backend(_))) {
                    break;
                }
            }
            outcome
        }
    };
    reconnect().await;
    append("same")
        .await
        .expect("a reconnected session of the admitted role is served");
    assert_eq!(authority.calls.load(Ordering::Acquire), 2);
    reconnect().await;
    assert_eq!(
        append("other").await,
        Err(EventLogError::Invalid(
            "pooled PostgreSQL session is not the admitted hosted principal".into()
        )),
        "a session of another principal was served"
    );
    assert!(authority.calls.load(Ordering::Acquire) >= 3);
    store.shutdown().await.unwrap();
}

/// Review L61, decided by Timo on 2026-11-23 (settlement floor). A logical restore copies each
/// event's `committed_xid` from the source cluster. One restored event 100,000 transaction ids
/// ahead of this cluster withholds every later event of the owner, in every tenant, from every
/// feed. A hosted `open` refuses such a store; `migrate` records the settlement floor under the
/// exclusive publication gate, and the feed then delivers the restored and the native events.
#[tokio::test]
async fn a_restored_history_is_settled_by_migrate_and_refused_by_open_until_then() {
    use eventlog_postgres::{PoolOptions, PostgresConfig};
    async fn floor(sql: &tokio_postgres::Client) -> (i64, i64) {
        let row = sql
            .query_one(
                "SELECT COUNT(*), COALESCE(MAX(settled_through), 0) \
                 FROM hosted_restore.kit_settlement_floor",
                &[],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    }
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let sql = client(&url).await;
    sql.batch_execute(
        "DROP SCHEMA IF EXISTS hosted_restore CASCADE; CREATE SCHEMA hosted_restore; \
         DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='eventlog_test_restore') \
         THEN CREATE ROLE eventlog_test_restore LOGIN CONNECTION LIMIT 8; END IF; END $$; \
         ALTER ROLE eventlog_test_restore CONNECTION LIMIT 8;",
    )
    .await
    .expect("isolated role and schema");
    let owner: tokio_postgres::Config = url.parse().unwrap();
    let mut application = owner.clone();
    application.user("eventlog_test_restore");
    let as_principal = |principal: &tokio_postgres::Config| {
        PostgresConfig::caller_owned(
            "hosted_restore",
            "kit",
            Arc::new(ReconnectingAuthority {
                principals: vec![principal.clone()],
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap()
    };
    let migrate = || PostgresEventStore::migrate(as_principal(&owner), PoolOptions::default(), &[]);
    let open =
        || PostgresEventStore::open(as_principal(&application), PoolOptions::default(), 16, 2, 8);
    migrate().await.expect("migration role applies the schema");
    sql.batch_execute(
        "GRANT USAGE ON SCHEMA hosted_restore TO eventlog_test_restore; \
         GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA hosted_restore TO eventlog_test_restore; \
         REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON hosted_restore.kit_schema_version, hosted_restore.kit_projection_registry, hosted_restore.kit_settlement_floor FROM eventlog_test_restore; \
         GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA hosted_restore TO eventlog_test_restore",
    )
    .await
    .expect("DML-only grants");
    assert_eq!(
        floor(&sql).await,
        (0, 0),
        "a native history records no floor"
    );
    let writer = open().await.expect("admitted before the restore");
    let append = |tenant: &'static str| {
        let stream = StreamId::new(TenantId::new(tenant).unwrap(), "item", "one").unwrap();
        let writer = &writer;
        async move {
            writer
                .append(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", 1)],
                    &eventlog_conformance::meta(tenant, &serde_json::json!({})),
                )
                .await
                .expect("appended")
                .events[0]
                .global_seq
        }
    };
    let restored = append("restored").await;
    // The restore: the event keeps the source cluster's transaction id, 100,000 ahead of ours.
    let moved = sql
        .execute(
            "UPDATE hosted_restore.kit_events SET committed_xid = \
             (pg_snapshot_xmax(pg_current_snapshot())::text::bigint + 100000)::text::xid8 \
             WHERE global_seq = $1",
            &[&i64::try_from(restored).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(moved, 1);
    let first = append("tenant-a").await;
    let second = append("tenant-b").await;
    assert!(restored < first && first < second);
    // Until the native appends pass this cluster's watermark an empty feed proves nothing.
    let mut passed = false;
    for _ in 0..200 {
        passed = sql
            .query_one(
                "SELECT bool_and(committed_xid < pg_snapshot_xmin(pg_current_snapshot())) \
                 FROM hosted_restore.kit_events WHERE global_seq > $1",
                &[&i64::try_from(restored).unwrap()],
            )
            .await
            .unwrap()
            .get(0);
        if passed {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(passed, "the native appends never passed the watermark");
    for tenant in ["restored", "tenant-a", "tenant-b"] {
        let page = writer
            .read_feed(&TenantId::new(tenant).unwrap(), 0, 10)
            .await
            .unwrap();
        assert!(
            page.events.is_empty(),
            "{tenant}: the restored transaction id no longer withholds the feed before migrate"
        );
    }
    match open().await {
        Err(EventLogError::Invalid(reason)) => assert_eq!(
            reason,
            "PostgreSQL events carry another cluster's transaction ids (a restored history); \
             run migrate with the migration role before opening"
        ),
        Err(other) => panic!("refused for another reason: {other:?}"),
        Ok(store) => {
            store.shutdown().await.unwrap();
            panic!("admitted a restored history before migrate settled it");
        }
    }
    assert_eq!(floor(&sql).await, (0, 0), "open records no floor");
    // An append still in flight holds the publication gate shared; the floor waits for it, or a
    // position below the floor could commit after the feed passed it.
    let holder = client(&url).await;
    let identity = serde_json::to_string(&["kit", "publication"]).unwrap();
    let key: i64 = holder
        .query_one(
            "SELECT hashtextextended(current_database() || ':' || 'hosted_restore' || $1, 0)",
            &[&identity],
        )
        .await
        .unwrap()
        .get(0);
    holder
        .execute("SELECT pg_advisory_lock_shared($1)", &[&key])
        .await
        .unwrap();
    let settling = migrate();
    tokio::pin!(settling);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), &mut settling)
            .await
            .is_err(),
        "migrate recorded the floor while an append held the publication gate"
    );
    assert_eq!(floor(&sql).await, (0, 0), "no floor while the gate is held");
    holder
        .execute("SELECT pg_advisory_unlock_shared($1)", &[&key])
        .await
        .unwrap();
    settling
        .await
        .expect("migrate settles the restored history");
    let through = i64::try_from(second).unwrap();
    assert_eq!(floor(&sql).await, (1, through));
    migrate().await.expect("a second migrate");
    assert_eq!(
        floor(&sql).await,
        (1, through),
        "nothing foreign is left above the floor"
    );
    let reader = open()
        .await
        .expect("admitted once migrate settled the restore");
    for (tenant, position) in [
        ("restored", restored),
        ("tenant-a", first),
        ("tenant-b", second),
    ] {
        let delivered = feed_from(&reader, &TenantId::new(tenant).unwrap(), 0, Vec::new(), 1).await;
        assert_eq!(
            delivered
                .iter()
                .map(|event| event.global_seq)
                .collect::<Vec<_>>(),
            vec![position],
            "{tenant}"
        );
    }
    let later = StreamId::new(TenantId::new("tenant-a").unwrap(), "item", "two").unwrap();
    let above = reader
        .append(
            &later,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("above", &serde_json::json!({})),
        )
        .await
        .unwrap()
        .events[0]
        .global_seq;
    let delivered = feed_from(
        &reader,
        &TenantId::new("tenant-a").unwrap(),
        first,
        Vec::new(),
        1,
    )
    .await;
    assert_eq!(
        delivered
            .iter()
            .map(|event| event.global_seq)
            .collect::<Vec<_>>(),
        vec![above],
        "an event above the floor is delivered through the watermark"
    );
    // The floor is migration metadata: a role that could raise it could settle an append still in
    // flight, and a feed cursor would pass it.
    sql.batch_execute(
        "GRANT INSERT ON hosted_restore.kit_settlement_floor TO eventlog_test_restore",
    )
    .await
    .unwrap();
    match open().await {
        Err(EventLogError::Invalid(reason)) => assert_eq!(
            reason,
            "hosted application role can write migration metadata reserved for the migration role"
        ),
        Err(other) => panic!("refused for another reason: {other:?}"),
        Ok(store) => {
            store.shutdown().await.unwrap();
            panic!("admitted an application role that can write the settlement floor");
        }
    }
    writer.shutdown().await.unwrap();
    reader.shutdown().await.unwrap();
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
    // Only the newer writer's handle drives the inline projector. An attached projector's lock is
    // taken before the guard and held to commit, so an older writer paused in its guard on a
    // handle that drove it would keep the newer writer out of the projector, and the XID/position
    // reversal this regression stages could not happen.
    second
        .register_inline(projector)
        .await
        .expect("newer writer setup");
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
    // The early read stays a single read: an empty page is what it asserts, and a feed withheld by
    // some other transaction is empty too. What follows must see both positions, so it follows the
    // cursor with a bounded retry rather than asking once whether the instance is idle.
    let visible = if let Some(early) = early {
        feed_from(&first, &tenant, early.next_position, early.events, 2).await
    } else {
        let page = page.await.expect("reader").expect("feed");
        feed_from(&first, &tenant, page.next_position, page.events, 2).await
    };
    assert_eq!(visible.len(), 2, "both positions remain reachable");
    assert!(visible[0].global_seq < visible[1].global_seq);
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
    // A zero counter row for the competing transaction to lock: hold one slot and release it (a
    // zero-delta check writes no row).
    for (id, delta) in [("seed", 1), ("seed-release", -1)] {
        let stream = StreamId::new(tenant.clone(), "item", id).expect("stream");
        store
            .append_guarded(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 0)],
                &eventlog_conformance::meta(id, &serde_json::json!({})),
                Arc::new(eventlog_conformance::ReserveScopes {
                    permit: store.admission_permit(),
                    reservations: vec![Reservation {
                        scope: scope.clone(),
                        delta,
                        ceiling: 1,
                    }],
                }),
            )
            .await
            .expect("zero counter");
    }
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
    assert_eq!(
        store
            .read_stream(&attempted, 0, 10)
            .await
            .expect("reopen read")
            .events,
        [] as [eventlog_core::RecordedEvent; 0]
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
    downgrade_blob_table(&sql, "schema_upgrade").await;
    sql.batch_execute("DROP TABLE schema_upgrade_schema_version; DROP TABLE schema_upgrade_scope_counters; DROP TABLE schema_upgrade_projection_registry; DROP TABLE schema_upgrade_snapshot_generations; DROP TABLE schema_upgrade_append_groups; DROP TABLE schema_upgrade_group_batches; DROP TABLE schema_upgrade_settlement_floor").await.expect("exact original populated schema");
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
    downgrade_blob_table(&sql, "schema_upgrade").await;
    sql.batch_execute("DROP TABLE schema_upgrade_schema_version; DROP TABLE schema_upgrade_scope_counters; DROP TABLE schema_upgrade_projection_registry; DROP TABLE schema_upgrade_snapshot_generations; DROP TABLE schema_upgrade_append_groups; DROP TABLE schema_upgrade_group_batches; DROP TABLE schema_upgrade_settlement_floor").await.expect("restore old fixture");
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
async fn loading_an_absent_aggregate_writes_no_storage_rows() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(adapter) = store("absent_reads").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let store = std::sync::Arc::new(adapter);
    let sql = client(&url().unwrap()).await;
    let repository =
        eventlog_core::Repository::<eventlog_conformance::SnapshotCounter>::new(store.clone());
    let tenant = TenantId::new("tenant").unwrap();
    for i in 0..3 {
        assert!(
            repository
                .load(&tenant, &format!("absent-{i}"))
                .await
                .unwrap()
                .is_new
        );
    }
    assert_eq!(
        sql.query_one(
            "SELECT count(*) FROM absent_reads_snapshot_generations",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0,
        "a read leaves nothing behind"
    );
    repository
        .handle(
            &tenant,
            "present",
            &(),
            &eventlog_conformance::meta("present", &serde_json::json!({})),
        )
        .await
        .unwrap();
    repository.snapshot_now(&tenant, "present").await.unwrap();
    assert_eq!(
        repository
            .load(&tenant, "present")
            .await
            .unwrap()
            .state
            .total,
        1,
        "a read still folds from the proven cache"
    );
}
#[tokio::test]
async fn a_delayed_older_snapshot_does_not_move_the_replay_point_back() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(adapter) = store("snapshot_monotonic").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    eventlog_conformance::run_snapshot_revision_is_monotonic(&adapter).await;
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
    assert_eq!(
        store.read_stream(&stream, 0, 100).await.unwrap().events,
        [] as [eventlog_core::RecordedEvent; 0]
    );
}

#[tokio::test]
async fn concurrent_differing_blob_writers_have_one_winner() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(first) = store("blob_race").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let url = url().expect("selected database");
    let second = PostgresEventStore::connect(&url, "blob_race")
        .await
        .expect("second connection");
    let sql = client(&url).await;
    sql.batch_execute("CREATE OR REPLACE FUNCTION blob_race_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.02); RETURN NEW; END $$; CREATE TRIGGER blob_race_delay BEFORE INSERT ON blob_race_blobs FOR EACH ROW EXECUTE FUNCTION blob_race_delay()").await.expect("overlapping puts");
    eventlog_conformance::run_blob_binding_race(&first, &second).await;
    sql.batch_execute(
        "DROP TRIGGER blob_race_delay ON blob_race_blobs; DROP FUNCTION blob_race_delay()",
    )
    .await
    .expect("remove delay fixture");
}

#[tokio::test]
async fn postgres_blob_integrity_covers_all_read_boundaries_and_binding_controls() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("blob_integrity").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let url = url().expect("selected database");
    let tenant = TenantId::new("blob-integrity").expect("tenant");
    store
        .put_blob(&tenant, "opaque", b"original")
        .await
        .expect("binding");
    store.shutdown().await.expect("close original store");

    let sql = client(&url).await;
    sql.execute(
        "UPDATE blob_integrity_blobs SET bytes=$1 WHERE tenant_id=$2 AND digest=$3",
        &[&b"mutated".as_slice(), &tenant.as_str(), &"opaque"],
    )
    .await
    .expect("same-length corruption");

    let reopened = PostgresEventStore::connect(&url, "blob_integrity")
        .await
        .expect("reopened store");
    assert!(
        matches!(
            reopened.get_blob(&tenant, "opaque").await,
            Err(eventlog_core::EventLogError::Backend(_))
        ),
        "same-length corruption must be refused after reopening"
    );
    assert!(matches!(
        reopened.put_blob(&tenant, "opaque", b"original").await,
        Err(eventlog_core::EventLogError::Backend(_))
    ));
    for hash in [
        "BAD",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        sql.execute(
            "UPDATE blob_integrity_blobs SET bytes=$1,byte_count=7,integrity_sha256=$2 WHERE tenant_id=$3 AND digest='opaque'",
            &[&b"original".as_slice(), &hash, &tenant.as_str()],
        )
        .await
        .unwrap();
        assert!(matches!(
            reopened.get_blob(&tenant, "opaque").await,
            Err(eventlog_core::EventLogError::Backend(_))
        ));
    }
    sql.execute(
        "UPDATE blob_integrity_blobs SET byte_count=8,integrity_sha256='0682c5f2076f099c34cfdd15a9e063849ed437a49677e6fcc5b4198c76575be5' WHERE tenant_id=$1 AND digest='opaque'",
        &[&tenant.as_str()],
    )
    .await
    .unwrap();
    assert_eq!(
        reopened.get_blob(&tenant, "opaque").await.unwrap(),
        Some(b"original".to_vec())
    );
    reopened.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn postgres_callback_blob_corruption_poison_rolls_back_every_owner() {
    use eventlog_core::{
        AdmissionScope, AppendGroup, AtomicEventStore, EventLogError, Reservation, StreamAppend,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    };
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("blob_callback").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let url = url().unwrap();
    let tenant = TenantId::new("blob-callback").unwrap();
    store
        .put_blob(&tenant, "opaque", b"original")
        .await
        .unwrap();
    let mode = Arc::new(AtomicU8::new(0));
    let projector = Arc::new(eventlog_conformance::BlobReadingProjector {
        driver_name: "blob_integrity_inline",
        digest: "opaque".into(),
        mode: Arc::clone(&mode),
    });
    store.register_inline(projector.clone()).await.unwrap();
    let first = StreamId::new(tenant.clone(), "item", "present").unwrap();
    store
        .append(
            &first,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("present", &serde_json::json!({})),
        )
        .await
        .expect("present callback read");
    let before = store
        .projection_get(&eventlog_conformance::BLOB_PROBE, &tenant, "item/present")
        .await
        .unwrap();
    client(&url)
        .await
        .execute(
            "UPDATE blob_callback_blobs SET bytes=$1 WHERE tenant_id=$2 AND digest='opaque'",
            &[&b"mutated".as_slice(), &tenant.as_str()],
        )
        .await
        .unwrap();
    for (mode_value, id) in [(1, "propagated"), (2, "caught")] {
        mode.store(mode_value, Ordering::Release);
        let stream = StreamId::new(tenant.clone(), "item", id).unwrap();
        assert!(matches!(
            store
                .append(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", 1)],
                    &eventlog_conformance::meta(id, &serde_json::json!({}))
                )
                .await,
            Err(EventLogError::Backend(_))
        ));
        assert_eq!(
            store.read_stream(&stream, 0, 10).await.unwrap().events,
            [] as [eventlog_core::RecordedEvent; 0]
        );
        assert!(
            store
                .projection_get(
                    &eventlog_conformance::BLOB_PROBE,
                    &tenant,
                    &format!("item/{id}")
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    let guarded = StreamId::new(tenant.clone(), "item", "guarded").unwrap();
    let guarded_check = Arc::new(eventlog_conformance::BlobReadingGuard {
        digest: "opaque".into(),
        catch_failure: true,
        reservation: Some((
            store.admission_permit(),
            vec![Reservation {
                scope: AdmissionScope::Tenant {
                    tenant: tenant.clone(),
                    key: "blob-single".into(),
                },
                delta: 1,
                ceiling: 1,
            }],
        )),
    });
    assert!(matches!(
        store
            .append_guarded(
                &guarded,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("guarded", &serde_json::json!({})),
                guarded_check.clone()
            )
            .await,
        Err(EventLogError::Backend(_))
    ));
    assert_eq!(
        store.read_stream(&guarded, 0, 10).await.unwrap().events,
        [] as [eventlog_core::RecordedEvent; 0]
    );
    let group_stream = StreamId::new(tenant.clone(), "item", "grouped").unwrap();
    let group = AppendGroup {
        tenant: tenant.clone(),
        appends: vec![StreamAppend {
            stream: group_stream.clone(),
            expected: Expected::NoStream,
            events: vec![eventlog_conformance::event("item.received", 1)],
        }],
        meta: eventlog_conformance::meta("grouped", &serde_json::json!({})),
    };
    let group_check = Arc::new(eventlog_conformance::BlobReadingGuard {
        digest: "opaque".into(),
        catch_failure: true,
        reservation: Some((
            store.admission_permit(),
            vec![Reservation {
                scope: AdmissionScope::Tenant {
                    tenant: tenant.clone(),
                    key: "blob-group".into(),
                },
                delta: 1,
                ceiling: 1,
            }],
        )),
    });
    assert!(matches!(
        store
            .append_group_guarded(&group, group_check.clone())
            .await,
        Err(EventLogError::Backend(_))
    ));
    assert_eq!(
        store
            .read_stream(&group_stream, 0, 10)
            .await
            .unwrap()
            .events,
        [] as [eventlog_core::RecordedEvent; 0]
    );
    let catch_up = Arc::new(eventlog_conformance::BlobReadingProjector {
        driver_name: "blob_integrity_catchup",
        digest: "opaque".into(),
        mode: Arc::clone(&mode),
    });
    store.create_projections(catch_up.clone()).await.unwrap();
    assert!(matches!(
        store.run_catch_up(catch_up.clone(), &tenant, 10).await,
        Err(EventLogError::Backend(_))
    ));
    assert_eq!(
        store
            .projection_get(&eventlog_conformance::BLOB_PROBE, &tenant, "item/present")
            .await
            .unwrap(),
        before
    );
    let rebuild = Arc::new(eventlog_conformance::BlobReadingProjector {
        driver_name: "blob_integrity_rebuild",
        digest: "opaque".into(),
        mode: Arc::clone(&mode),
    });
    assert!(matches!(
        store.rebuild_projection(rebuild.clone(), &tenant).await,
        Err(EventLogError::Backend(_))
    ));
    assert_eq!(
        store
            .projection_get(&eventlog_conformance::BLOB_PROBE, &tenant, "item/present")
            .await
            .unwrap(),
        before
    );
    client(&url)
        .await
        .execute(
            "UPDATE blob_callback_blobs SET bytes=$1,byte_count=8,integrity_sha256='0682c5f2076f099c34cfdd15a9e063849ed437a49677e6fcc5b4198c76575be5' WHERE tenant_id=$2 AND digest='opaque'",
            &[&b"original".as_slice(), &tenant.as_str()],
        )
        .await
        .unwrap();
    mode.store(0, Ordering::Release);
    assert!(
        !store
            .append_guarded(
                &guarded,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("guarded", &serde_json::json!({})),
                guarded_check,
            )
            .await
            .expect("failed callback rolled back command receipt and reservation")
            .deduplicated
    );
    let group_result = store
        .append_group_guarded(&group, group_check)
        .await
        .expect("failed callback rolled back group receipt and reservation");
    assert!(!group_result.deduplicated);
    assert!(store.append_group(&group).await.unwrap().deduplicated);
    assert_eq!(
        store
            .run_catch_up(catch_up, &tenant, 10)
            .await
            .unwrap()
            .applied,
        3
    );
    assert_eq!(store.rebuild_projection(rebuild, &tenant).await.unwrap(), 3);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn postgres_blob_migration_is_exact_explicit_atomic_and_fenced() {
    use eventlog_core::{EventLogError, LegacyBlobMigration};
    use eventlog_postgres::{PoolOptions, PostgresConfig};
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(original) = store("blob_migration").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let url = url().unwrap();
    let tenant = TenantId::new("legacy-tenant").unwrap();
    original
        .put_blob(&tenant, "opaque", b"legacy bytes")
        .await
        .unwrap();
    original.shutdown().await.unwrap();
    let sql = client(&url).await;
    downgrade_blob_table(&sql, "blob_migration").await;
    let config = PostgresConfig::isolated(&url, "blob_migration").unwrap();
    assert!(matches!(
        PostgresEventStore::migrate(config.clone(), PoolOptions::default(), &[]).await,
        Err(EventLogError::Invalid(_))
    ));
    let columns: i64 = sql
        .query_one(
            "SELECT count(*) FROM pg_attribute WHERE attrelid='blob_migration_blobs'::regclass AND attnum>0 AND NOT attisdropped",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        columns, 5,
        "default refusal leaves predecessor shape intact"
    );
    let before = sql
        .query_one(
            "SELECT bytes,byte_count,recorded_at FROM blob_migration_blobs WHERE tenant_id='legacy-tenant' AND digest='opaque'",
            &[],
        )
        .await
        .unwrap();
    let before_bytes: Vec<u8> = before.get(0);
    let before_count: i64 = before.get(1);
    let before_time: time::OffsetDateTime = before.get(2);
    let report = PostgresEventStore::migrate_with_blob_migration(
        config.clone(),
        PoolOptions::default(),
        &[],
        LegacyBlobMigration::TrustObservedBytes,
    )
    .await
    .unwrap();
    assert!(report.upgraded);
    assert_eq!(report.trusted_legacy_rows, 1);
    let after = sql
        .query_one(
            "SELECT bytes,byte_count,recorded_at FROM blob_migration_blobs WHERE tenant_id='legacy-tenant' AND digest='opaque'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(after.get::<_, Vec<u8>>(0), before_bytes);
    assert_eq!(after.get::<_, i64>(1), before_count);
    assert_eq!(after.get::<_, time::OffsetDateTime>(2), before_time);
    assert!(
        sql.execute(
            "INSERT INTO blob_migration_blobs VALUES($1,$2,$3,$4,$5)",
            &[
                &"old-reader",
                &"opaque",
                &b"bytes".as_slice(),
                &5_i64,
                &before_time
            ],
        )
        .await
        .is_err()
    );
    let retry = PostgresEventStore::migrate_with_blob_migration(
        config,
        PoolOptions::default(),
        &[],
        LegacyBlobMigration::TrustObservedBytes,
    )
    .await
    .unwrap();
    assert!(!retry.upgraded);
    assert_eq!(retry.trusted_legacy_rows, 0);

    let malformed = store("blob_migration_bad").await.unwrap();
    malformed
        .put_blob(&tenant, "opaque", b"bytes")
        .await
        .unwrap();
    malformed.shutdown().await.unwrap();
    downgrade_blob_table(&sql, "blob_migration_bad").await;
    sql.execute("UPDATE blob_migration_bad_blobs SET byte_count=99", &[])
        .await
        .unwrap();
    assert!(matches!(
        PostgresEventStore::migrate_with_blob_migration(
            PostgresConfig::isolated(&url, "blob_migration_bad").unwrap(),
            PoolOptions::default(),
            &[],
            LegacyBlobMigration::TrustObservedBytes,
        )
        .await,
        Err(EventLogError::Backend(_))
    ));
    let columns: i64 = sql.query_one("SELECT count(*) FROM pg_attribute WHERE attrelid='blob_migration_bad_blobs'::regclass AND attnum>0 AND NOT attisdropped", &[]).await.unwrap().get(0);
    assert_eq!(columns, 5, "failed import rolls back additive metadata");

    let concurrent = store("blob_migration_concurrent").await.unwrap();
    concurrent.shutdown().await.unwrap();
    downgrade_blob_table(&sql, "blob_migration_concurrent").await;
    let config = PostgresConfig::isolated(&url, "blob_migration_concurrent").unwrap();
    let (first, second) = tokio::join!(
        PostgresEventStore::migrate_with_blob_migration(
            config.clone(),
            PoolOptions::default(),
            &[],
            LegacyBlobMigration::TrustObservedBytes,
        ),
        PostgresEventStore::migrate_with_blob_migration(
            config,
            PoolOptions::default(),
            &[],
            LegacyBlobMigration::TrustObservedBytes,
        )
    );
    let reports = [first.unwrap(), second.unwrap()];
    assert_eq!(reports.iter().filter(|report| report.upgraded).count(), 1);
    assert_eq!(reports.iter().filter(|report| !report.upgraded).count(), 1);
}

#[tokio::test]
async fn postgres_blob_migration_unknown_commit_has_no_report() {
    use eventlog_core::EventLogError;
    use eventlog_postgres::{PoolOptions, PostgresConfig};
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
    let setup = store("blob_migration_unknown").await.unwrap();
    setup.shutdown().await.unwrap();
    let sql = client(&url).await;
    downgrade_blob_table(&sql, "blob_migration_unknown").await;
    let parsed: tokio_postgres::Config = url.parse().unwrap();
    let upstream_port = parsed.get_ports()[0];
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let intercepted = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&intercepted);
    let proxy = std::thread::spawn(move || {
        let (mut downstream, _) = listener.accept().unwrap();
        let mut upstream = TcpStream::connect(("127.0.0.1", upstream_port)).unwrap();
        downstream.set_nodelay(true).unwrap();
        upstream.set_nodelay(true).unwrap();
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
            let mut body = vec![0; usize::try_from(length - 4).unwrap()];
            if upstream.read_exact(&mut body).is_err() {
                break;
            }
            if header[0] == b'C' && body == b"COMMIT\0" {
                observed.store(true, Ordering::Release);
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
    let result = PostgresEventStore::migrate(
        PostgresConfig::isolated(&proxy_url, "blob_migration_unknown").unwrap(),
        PoolOptions::default(),
        &[],
    )
    .await;
    assert_eq!(result, Err(EventLogError::BlobMigrationCommitUnknown));
    assert!(intercepted.load(Ordering::Acquire));
    proxy.join().unwrap();
    let retry = PostgresEventStore::migrate_with_blob_migration(
        PostgresConfig::isolated(&url, "blob_migration_unknown").unwrap(),
        PoolOptions::default(),
        &[],
        eventlog_core::LegacyBlobMigration::TrustObservedBytes,
    )
    .await
    .unwrap();
    assert!(!retry.upgraded);
    assert_eq!(retry.trusted_legacy_rows, 0);
}

async fn review_wait_for_blob_insert_lock(sql: &tokio_postgres::Client, prefix: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let count: i64 = sql.query_one("SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND wait_event = 'advisory' AND query LIKE $1", &[&format!("INSERT INTO {prefix}_blobs%")]).await.unwrap().get(0);
            if count == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }).await.expect("put reached the exact insert pause");
}

#[tokio::test]
async fn review_blob_deleted_after_ignored_insert_is_republished() {
    use std::sync::Arc;
    let _exclusive = EXCLUSIVE.lock().await;
    let first = Arc::new(store("review_blob_delete").await.expect("explicit fixture"));
    let second = PostgresEventStore::connect(&url().unwrap(), "review_blob_delete")
        .await
        .unwrap();
    let sql = client(&url().unwrap()).await;
    let tenant = TenantId::new("review").unwrap();
    first.put_blob(&tenant, "opaque", b"old").await.unwrap();
    sql.batch_execute("SELECT pg_advisory_lock(903151); CREATE OR REPLACE FUNCTION review_blob_delete_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(903151); RETURN NULL; END $$; CREATE TRIGGER review_blob_delete_pause AFTER INSERT ON review_blob_delete_blobs FOR EACH STATEMENT EXECUTE FUNCTION review_blob_delete_pause()").await.unwrap();
    let writer = Arc::clone(&first);
    let owner = tenant.clone();
    let put = tokio::spawn(async move { writer.put_blob(&owner, "opaque", b"new").await });
    review_wait_for_blob_insert_lock(&sql, "review_blob_delete").await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        second.delete_blob(&tenant, "opaque"),
    )
    .await
    .expect("delete is not held by the ignored insert")
    .unwrap();
    assert_eq!(second.get_blob(&tenant, "opaque").await.unwrap(), None);
    sql.batch_execute("SELECT pg_advisory_unlock(903151)")
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), put)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        second.get_blob(&tenant, "opaque").await.unwrap().as_deref(),
        Some(&b"new"[..])
    );
    sql.batch_execute("DROP TRIGGER review_blob_delete_pause ON review_blob_delete_blobs; DROP FUNCTION review_blob_delete_pause()").await.unwrap();
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    eprintln!(
        "forced deletion after ignored INSERT: old binding absent before resume, new binding published after retry"
    );
}

#[tokio::test]
async fn review_blob_cancelled_insert_retires_connection_and_retries() {
    use std::sync::Arc;
    let _exclusive = EXCLUSIVE.lock().await;
    let store = Arc::new(store("review_blob_cancel").await.expect("explicit fixture"));
    let sql = client(&url().unwrap()).await;
    sql.batch_execute("SELECT pg_advisory_lock(903152); CREATE OR REPLACE FUNCTION review_blob_cancel_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(903152); RETURN NEW; END $$; CREATE TRIGGER review_blob_cancel_pause BEFORE INSERT ON review_blob_cancel_blobs FOR EACH ROW EXECUTE FUNCTION review_blob_cancel_pause()").await.unwrap();
    let tenant = TenantId::new("review").unwrap();
    let writer = Arc::clone(&store);
    let owner = tenant.clone();
    let put = tokio::spawn(async move { writer.put_blob(&owner, "opaque", b"cancelled").await });
    review_wait_for_blob_insert_lock(&sql, "review_blob_cancel").await;
    put.abort();
    assert!(put.await.unwrap_err().is_cancelled());
    sql.batch_execute("SELECT pg_advisory_unlock(903152)")
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let count: i64 = sql.query_one("SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query LIKE 'INSERT INTO review_blob_cancel_blobs%'", &[]).await.unwrap().get(0);
            if count == 0 { break; }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }).await.expect("cancelled connection driver retired");
    assert_eq!(store.get_blob(&tenant, "opaque").await.unwrap(), None);
    store.put_blob(&tenant, "opaque", b"retry").await.unwrap();
    assert_eq!(
        store.get_blob(&tenant, "opaque").await.unwrap().as_deref(),
        Some(&b"retry"[..])
    );
    sql.batch_execute("DROP TRIGGER review_blob_cancel_pause ON review_blob_cancel_blobs; DROP FUNCTION review_blob_cancel_pause()").await.unwrap();
    store.shutdown().await.unwrap();
    eprintln!(
        "cancelled pre-publication INSERT: no durable binding, driver retired, next operation succeeds"
    );
}

#[tokio::test]
async fn review_blob_empty_content_and_conflict_rollback_reuse() {
    let _exclusive = EXCLUSIVE.lock().await;
    let first = store("review_blob_empty").await.expect("explicit fixture");
    let second = PostgresEventStore::connect(&url().unwrap(), "review_blob_empty")
        .await
        .unwrap();
    let tenant = TenantId::new("review").unwrap();
    first.put_blob(&tenant, "opaque", b"").await.unwrap();
    second.put_blob(&tenant, "opaque", b"").await.unwrap();
    for _ in 0..32 {
        assert!(matches!(
            first.put_blob(&tenant, "opaque", b"nonempty").await,
            Err(eventlog_core::EventLogError::Invalid(_))
        ));
        assert_eq!(
            second.get_blob(&tenant, "opaque").await.unwrap(),
            Some(Vec::new())
        );
    }
    first.delete_blob(&tenant, "opaque").await.unwrap();
    second
        .put_blob(&tenant, "opaque", b"rebound")
        .await
        .unwrap();
    assert!(matches!(
        first.put_blob(&tenant, "opaque", b"").await,
        Err(eventlog_core::EventLogError::Invalid(_))
    ));
    assert_eq!(
        first.get_blob(&tenant, "opaque").await.unwrap().as_deref(),
        Some(&b"rebound"[..])
    );
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn review2_postgres_admission_refuses_unenforced_check_and_unmigrated_legacy_shape() {
    use eventlog_core::EventLogError;
    use eventlog_postgres::{PoolOptions, PostgresConfig};
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(weakened) = store("reviewtwo_check").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let url = url().unwrap();
    weakened.shutdown().await.unwrap();
    let sql = client(&url).await;
    // Keep the constraint name, weaken the enforced expression to CHECK (true).
    let name: String = sql
        .query_one(
            "SELECT conname FROM pg_constraint WHERE conrelid='reviewtwo_check_blobs'::regclass AND contype='c'",
            &[],
        )
        .await
        .expect("exactly one CHECK on the blob table")
        .get(0);
    sql.batch_execute(&format!(
        "ALTER TABLE reviewtwo_check_blobs DROP CONSTRAINT {name};
         ALTER TABLE reviewtwo_check_blobs ADD CONSTRAINT {name} CHECK (true);"
    ))
    .await
    .unwrap();
    let five_column_insert_accepted = sql
        .execute(
            "INSERT INTO reviewtwo_check_blobs (tenant_id,digest,bytes,byte_count,recorded_at) VALUES ('t','d',$1,1,now())",
            &[&b"x".as_slice()],
        )
        .await
        .is_ok();
    assert!(
        five_column_insert_accepted,
        "fixture control: the weakened table really admits an old five-column INSERT"
    );
    let config = PostgresConfig::isolated(&url, "reviewtwo_check").unwrap();
    assert!(
        matches!(
            PostgresEventStore::migrate(config, PoolOptions::default(), &[]).await,
            Err(EventLogError::Invalid(_))
        ),
        "migration admission must compare the enforced CHECK expression, not its name"
    );
    assert!(
        PostgresEventStore::connect(&url, "reviewtwo_check")
            .await
            .is_err(),
        "an unenforced integrity check must not be served"
    );

    // A populated predecessor is not served by the ordinary constructor without explicit trust.
    let legacy = store("reviewtwo_legacy").await.unwrap();
    let tenant = TenantId::new("legacy").unwrap();
    legacy.put_blob(&tenant, "opaque", b"bytes").await.unwrap();
    legacy.shutdown().await.unwrap();
    downgrade_blob_table(&sql, "reviewtwo_legacy").await;
    assert!(
        matches!(
            PostgresEventStore::connect(&url, "reviewtwo_legacy").await,
            Err(EventLogError::Invalid(_))
        ),
        "connect must refuse a populated predecessor instead of migrating or serving it"
    );
    let columns: i64 = sql
        .query_one(
            "SELECT count(*) FROM pg_attribute WHERE attrelid='reviewtwo_legacy_blobs'::regclass AND attnum>0 AND NOT attisdropped",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(columns, 5, "refusal leaves the predecessor shape untouched");
}

/// A reader resolves the identity before every resumed page, so resolving an existing one must
/// be a read. A write here costs a commit flush per page and serializes readers on the row.
#[tokio::test]
async fn resolving_an_existing_stream_identity_writes_nothing() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("identity_read").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let sql = client(&url().unwrap()).await;
    let tenant = TenantId::new("identity-reader").unwrap();
    let minted = store.stream_identity(&tenant).await.unwrap();
    let version = "SELECT xmin::text || '@' || ctid::text FROM identity_read_identity WHERE tenant_id = 'identity-reader'";
    let before: String = sql.query_one(version, &[]).await.unwrap().get(0);
    for _ in 0..3 {
        assert_eq!(store.stream_identity(&tenant).await.unwrap(), minted);
    }
    let after: String = sql.query_one(version, &[]).await.unwrap().get(0);
    assert_eq!(
        after, before,
        "resolving an existing identity rewrote its row"
    );
    store.shutdown().await.unwrap();
}

/// Readers of a fresh tenant race to mint its identity. Every one of them must come back with
/// the single identity that was stored, and none may fail because another won.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_resolutions_of_a_fresh_tenant_agree_on_one_identity() {
    const CALLERS: usize = 16;
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    store("identity_race")
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let store = Arc::new(
        PostgresEventStore::connect_local(
            &url,
            "identity_race",
            eventlog_postgres::PoolOptions {
                max_connections: CALLERS,
                ..eventlog_postgres::PoolOptions::default()
            },
        )
        .await
        .unwrap(),
    );
    let tenant = TenantId::new("identity-race").unwrap();
    let start = Arc::new(tokio::sync::Barrier::new(CALLERS));
    let callers: Vec<_> = (0..CALLERS)
        .map(|_| {
            let (store, tenant, start) = (store.clone(), tenant.clone(), start.clone());
            tokio::spawn(async move {
                start.wait().await;
                store.stream_identity(&tenant).await
            })
        })
        .collect();
    let mut resolved = std::collections::BTreeSet::new();
    for caller in callers {
        resolved.insert(
            caller
                .await
                .unwrap()
                .expect("a racing first resolution failed"),
        );
    }
    let stored: Vec<String> = client(&url)
        .await
        .query(
            "SELECT stream_identity FROM identity_race_identity WHERE tenant_id = 'identity-race'",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        resolved.into_iter().collect::<Vec<_>>(),
        stored,
        "racing callers must all return the one stored identity"
    );
    store.shutdown().await.unwrap();
}

/// The losing side of the first-resolution race, forced: another session has inserted the
/// identity and not yet committed, so every resolver misses it on read and then conflicts on
/// insert. Each must wait for that commit and return the winner's identity rather than its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resolution_that_loses_the_first_insert_returns_the_winners_identity() {
    const LOSERS: usize = 4;
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    store("identity_loser")
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let store = Arc::new(
        PostgresEventStore::connect_local(
            &url,
            "identity_loser",
            eventlog_postgres::PoolOptions {
                max_connections: LOSERS,
                ..eventlog_postgres::PoolOptions::default()
            },
        )
        .await
        .unwrap(),
    );
    let winner = client(&url).await;
    winner
        .batch_execute(
            "BEGIN; INSERT INTO identity_loser_identity (tenant_id, stream_identity)
             VALUES ('identity-loser', 'winner-identity')",
        )
        .await
        .unwrap();
    let tenant = TenantId::new("identity-loser").unwrap();
    let losers: Vec<_> = (0..LOSERS)
        .map(|_| {
            let (store, tenant) = (store.clone(), tenant.clone());
            tokio::spawn(async move { store.stream_identity(&tenant).await })
        })
        .collect();
    let observer = client(&url).await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    loop {
        let waiting: i64 = observer
            .query_one(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO identity_loser_identity%'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if usize::try_from(waiting).unwrap() == LOSERS {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {waiting} of {LOSERS} resolvers reached the conflicting insert"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    winner.batch_execute("COMMIT").await.unwrap();
    for loser in losers {
        assert_eq!(
            loser.await.unwrap().expect("a losing resolution failed"),
            "winner-identity",
            "a resolver that lost the insert returned an identity nobody stored"
        );
    }
    store.shutdown().await.unwrap();
}
