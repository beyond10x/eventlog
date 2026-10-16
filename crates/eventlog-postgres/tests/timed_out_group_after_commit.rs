//! Adversarial cases for `eventlog.pool.TimedOutWrite`.
//!
//! The dangerous direction is a false `Deadline`: a caller told "did not commit" when the server
//! committed. Each case puts a proxy between the store and the fixture that forwards everything
//! except the server's COMMIT reply, so the store-operation deadline fires after COMMIT was
//! written and the transaction committed server-side.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use eventlog_core::{
    AppendGroup, AtomicBlobEventStore, AtomicEventStore, EventLogError, EventStore, Expected,
    NoGuard, StreamAppend, StreamId, TenantId,
};
use eventlog_postgres::{PoolOptions, PostgresEventStore};

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture")
}

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
    format!("towadv_{run}_{case}")
}

const SHORT: Duration = Duration::from_secs(1);

struct Proxy {
    url: String,
    armed: Arc<AtomicBool>,
    withheld: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<()>,
}

/// A one-connection proxy that, once armed, withholds the server's `CommandComplete("COMMIT")`
/// reply and holds the connection open until released.
fn proxy() -> Proxy {
    use std::{
        io::{Read, Write},
        net::{Shutdown, TcpListener, TcpStream},
    };
    let parsed: tokio_postgres::Config = url().parse().unwrap();
    let upstream_port = parsed.get_ports()[0];
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let armed = Arc::new(AtomicBool::new(false));
    let withheld = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (proxy_armed, proxy_withheld, proxy_release) =
        (armed.clone(), withheld.clone(), release.clone());
    let thread = std::thread::spawn(move || {
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
    let password = parsed.get_password().map_or_else(String::new, |password| {
        format!(" password={}", String::from_utf8_lossy(password))
    });
    let url = format!(
        "host=127.0.0.1 port={proxy_port} user={} dbname={}{password}",
        parsed.get_user().unwrap(),
        parsed.get_dbname().unwrap()
    );
    Proxy {
        url,
        armed,
        withheld,
        release,
        thread,
    }
}

async fn proxied(prefix: &str) -> (PostgresEventStore, PostgresEventStore, Proxy) {
    let setup = PostgresEventStore::connect(&url(), prefix)
        .await
        .expect("setup store");
    let proxy = proxy();
    let timed = PostgresEventStore::connect_local(
        &proxy.url,
        prefix,
        PoolOptions {
            transaction_timeout: SHORT,
            ..PoolOptions::default()
        },
    )
    .await
    .expect("proxied short-deadline store");
    (setup, timed, proxy)
}

async fn finish(setup: PostgresEventStore, timed: PostgresEventStore, proxy: Proxy) {
    proxy.release.store(true, Ordering::Release);
    timed.shutdown().await.expect("closed");
    proxy.thread.join().unwrap();
    setup.drop_tables().await.expect("dropped");
    setup.shutdown().await.expect("closed");
}

/// Kills the mutant that drops `commit_started.store(true, ..)` from `append_group_guarded`:
/// no other case times out a group after COMMIT was written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adversary_group_deadline_after_commit_is_unknown_commit() {
    let prefix = prefix("group");
    let (setup, timed, proxy) = proxied(&prefix).await;
    let tenant = TenantId::new("timed-out").unwrap();
    let streams = ["a", "b"].map(|id| StreamId::new(tenant.clone(), "item", id).unwrap());
    let group = AppendGroup {
        tenant: tenant.clone(),
        meta: eventlog_conformance::meta("adv-group-after", &serde_json::json!({})),
        appends: streams
            .iter()
            .map(|stream| StreamAppend {
                stream: stream.clone(),
                expected: Expected::NoStream,
                events: vec![eventlog_conformance::event("item.received", 1)],
            })
            .collect(),
    };
    proxy.armed.store(true, Ordering::Release);
    let outcome = timed.append_group_guarded(&group, Arc::new(NoGuard)).await;
    assert!(
        proxy.withheld.load(Ordering::Acquire),
        "the COMMIT reply was never withheld"
    );
    for stream in &streams {
        assert_eq!(
            setup.stream_version(stream).await.unwrap(),
            Some(1),
            "the server committed the group"
        );
    }
    assert_eq!(
        outcome.map(|_| ()),
        Err(EventLogError::UnknownCommit),
        "a group committed server-side must not be reported as a Deadline"
    );
    finish(setup, timed, proxy).await;
}

/// Kills the same mutant in `append_group_with_blobs_guarded`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adversary_blob_group_deadline_after_commit_is_unknown_commit() {
    let prefix = prefix("blobs");
    let (setup, timed, proxy) = proxied(&prefix).await;
    let request = eventlog_conformance::atomic_blob_request("adv-blobs-after", "one", "content");
    proxy.armed.store(true, Ordering::Release);
    let outcome = timed
        .append_group_with_blobs_guarded(&request, Arc::new(NoGuard))
        .await;
    assert!(
        proxy.withheld.load(Ordering::Acquire),
        "the COMMIT reply was never withheld"
    );
    assert_eq!(
        setup
            .stream_version(&request.group.appends[0].stream)
            .await
            .unwrap(),
        Some(1),
        "the server committed the blob group"
    );
    assert_eq!(
        outcome.map(|_| ()),
        Err(EventLogError::UnknownCommit),
        "a blob group committed server-side must not be reported as a Deadline"
    );
    finish(setup, timed, proxy).await;
}
