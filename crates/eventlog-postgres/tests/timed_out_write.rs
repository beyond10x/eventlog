//! `eventlog.pool.TimedOutWrite`: what a PostgreSQL append returns when its store-operation
//! deadline fires before the outcome is known.
//!
//! A deadline that fires before COMMIT was written means the transaction cannot have committed,
//! so the caller receives `Deadline` and may retry. Only a deadline that fires after COMMIT was
//! written leaves the outcome ambiguous and yields `UnknownCommit`.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use eventlog_core::{
    AppendGroup, AtomicBlobEventStore, AtomicEventStore, BoxFuture, EventLogError, EventStore,
    Expected, Guard, ProjectionStore, StreamAppend, StreamId, TenantId,
};
use eventlog_postgres::{PoolOptions, PostgresEventStore};

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture")
}

/// A table prefix unique to this run, so a concurrent user of the same server never shares it.
fn prefix(case: &str) -> String {
    let run: String = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string()
        .bytes()
        .rev()
        .take(12)
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    format!("tow_{run}_{case}")
}

const SHORT: Duration = Duration::from_secs(1);

fn short_deadline() -> PoolOptions {
    PoolOptions {
        transaction_timeout: SHORT,
        ..PoolOptions::default()
    }
}

/// A guard that outlasts the transaction deadline, so the deadline fires inside the guard
/// callback, before COMMIT is ever written.
struct Outlast {
    entered: Arc<AtomicBool>,
}

impl Guard for Outlast {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.entered.store(true, Ordering::Release);
            tokio::time::sleep(SHORT * 60).await;
            Ok(())
        })
    }
}

/// Create the tables with default bounds, then open the store under test with a short deadline.
async fn stores(prefix: &str) -> (PostgresEventStore, PostgresEventStore) {
    let setup = PostgresEventStore::connect(&url(), prefix)
        .await
        .expect("setup store");
    let timed = PostgresEventStore::connect_local(&url(), prefix, short_deadline())
        .await
        .expect("short-deadline store");
    (setup, timed)
}

async fn finish(setup: PostgresEventStore, timed: PostgresEventStore) {
    timed.shutdown().await.expect("closed");
    setup.drop_tables().await.expect("dropped");
    setup.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guarded_append_deadline_before_commit_is_deadline() {
    let prefix = prefix("single");
    let (setup, timed) = stores(&prefix).await;
    let stream = StreamId::new(TenantId::new("timed-out").unwrap(), "item", "one").unwrap();
    let entered = Arc::new(AtomicBool::new(false));
    let outcome = timed
        .append_guarded(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("deadline-single", &serde_json::json!({})),
            Arc::new(Outlast {
                entered: entered.clone(),
            }),
        )
        .await;
    assert!(
        entered.load(Ordering::Acquire),
        "the guard was never reached"
    );
    assert!(
        matches!(outcome, Err(EventLogError::Deadline { .. })),
        "a deadline before COMMIT is not an unknown commit: {outcome:?}"
    );
    assert_eq!(setup.stream_version(&stream).await.unwrap(), None);
    assert_eq!(
        setup
            .read_stream(&stream, 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        0
    );
    finish(setup, timed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guarded_group_deadline_before_commit_is_deadline() {
    let prefix = prefix("group");
    let (setup, timed) = stores(&prefix).await;
    let tenant = TenantId::new("timed-out").unwrap();
    let streams = ["a", "b"].map(|id| StreamId::new(tenant.clone(), "item", id).unwrap());
    let group = AppendGroup {
        tenant: tenant.clone(),
        meta: eventlog_conformance::meta("deadline-group", &serde_json::json!({})),
        appends: streams
            .iter()
            .map(|stream| StreamAppend {
                stream: stream.clone(),
                expected: Expected::NoStream,
                events: vec![eventlog_conformance::event("item.received", 1)],
            })
            .collect(),
    };
    let entered = Arc::new(AtomicBool::new(false));
    let outcome = timed
        .append_group_guarded(
            &group,
            Arc::new(Outlast {
                entered: entered.clone(),
            }),
        )
        .await;
    assert!(
        entered.load(Ordering::Acquire),
        "the guard was never reached"
    );
    assert!(
        matches!(outcome, Err(EventLogError::Deadline { .. })),
        "a deadline before COMMIT is not an unknown commit: {outcome:?}"
    );
    for stream in &streams {
        assert_eq!(setup.stream_version(stream).await.unwrap(), None);
    }
    finish(setup, timed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guarded_blob_group_deadline_before_commit_is_deadline() {
    let prefix = prefix("blobs");
    let (setup, timed) = stores(&prefix).await;
    let request = eventlog_conformance::atomic_blob_request("deadline-blobs", "one", "content");
    let entered = Arc::new(AtomicBool::new(false));
    let outcome = timed
        .append_group_with_blobs_guarded(
            &request,
            Arc::new(Outlast {
                entered: entered.clone(),
            }),
        )
        .await;
    assert!(
        entered.load(Ordering::Acquire),
        "the guard was never reached"
    );
    assert!(
        matches!(outcome, Err(EventLogError::Deadline { .. })),
        "a deadline before COMMIT is not an unknown commit: {outcome:?}"
    );
    assert_eq!(
        setup
            .stream_version(&request.group.appends[0].stream)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        setup
            .get_blob(&request.group.tenant, "content")
            .await
            .unwrap(),
        None
    );
    finish(setup, timed).await;
}

/// The other half of the contract: once COMMIT was written, a deadline that fires while its
/// reply is outstanding is still an unknown commit. A proxy withholds the server's COMMIT reply
/// without closing the connection, so the only thing that ends the wait is the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guarded_append_deadline_after_commit_is_unknown_commit() {
    use std::{
        io::{Read, Write},
        net::{Shutdown, TcpListener, TcpStream},
    };
    let prefix = prefix("after");
    let setup = PostgresEventStore::connect(&url(), &prefix)
        .await
        .expect("setup store");
    let parsed: tokio_postgres::Config = url().parse().unwrap();
    let upstream_port = parsed.get_ports()[0];
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let armed = Arc::new(AtomicBool::new(false));
    let withheld = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (proxy_armed, proxy_withheld, proxy_release) =
        (armed.clone(), withheld.clone(), release.clone());
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
            assert!((4..=16 * 1024 * 1024).contains(&length));
            let mut body = vec![0; usize::try_from(length - 4).unwrap()];
            if upstream.read_exact(&mut body).is_err() {
                break;
            }
            if header[0] == b'C' && body == b"COMMIT\0" && proxy_armed.load(Ordering::Acquire) {
                proxy_withheld.store(true, Ordering::Release);
                // Hold the connection open with the reply withheld until the client gave up.
                for _ in 0..3000 {
                    if proxy_release.load(Ordering::Acquire) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
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
    // The fixture may require a password; the proxied connection carries the same credentials.
    let password = parsed.get_password().map_or_else(String::new, |password| {
        format!(" password={}", String::from_utf8_lossy(password))
    });
    let proxy_url = format!(
        "host=127.0.0.1 port={proxy_port} user={} dbname={}{password}",
        parsed.get_user().unwrap(),
        parsed.get_dbname().unwrap()
    );
    let timed = PostgresEventStore::connect_local(&proxy_url, &prefix, short_deadline())
        .await
        .expect("proxied short-deadline store");
    let stream = StreamId::new(TenantId::new("timed-out").unwrap(), "item", "one").unwrap();
    armed.store(true, Ordering::Release);
    let outcome = timed
        .append_guarded(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("deadline-after", &serde_json::json!({})),
            Arc::new(eventlog_core::NoGuard),
        )
        .await;
    release.store(true, Ordering::Release);
    assert!(
        withheld.load(Ordering::Acquire),
        "the COMMIT reply was never withheld"
    );
    assert_eq!(outcome, Err(EventLogError::UnknownCommit));
    timed.shutdown().await.expect("closed");
    proxy.join().unwrap();
    // The server committed; only its reply was lost.
    assert_eq!(
        setup
            .read_stream(&stream, 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
    setup.drop_tables().await.expect("dropped");
    setup.shutdown().await.expect("closed");
}
