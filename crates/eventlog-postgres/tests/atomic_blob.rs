use eventlog_core::{AtomicBlobEventStore, BlobWrite, EventStore};
use eventlog_postgres::PostgresEventStore;

struct HoldBinding {
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}
impl eventlog_core::Guard for HoldBinding {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn eventlog_core::ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), eventlog_core::EventLogError>> {
        Box::pin(async move {
            assert!(store.get_blob("content").await?.is_some());
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_atomic_blob_reuse_serializes_standalone_delete() {
    use std::sync::Arc;
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture");
    let setup = PostgresEventStore::connect(&url, "atomic_delete")
        .await
        .unwrap();
    setup.drop_tables().await.unwrap();
    setup.shutdown().await.unwrap();
    let writer = Arc::new(
        PostgresEventStore::connect(&url, "atomic_delete")
            .await
            .unwrap(),
    );
    let deleter = Arc::new(
        PostgresEventStore::connect(&url, "atomic_delete")
            .await
            .unwrap(),
    );
    let request = eventlog_conformance::atomic_blob_request("guarded", "one", "content");
    writer
        .put_blob(&request.group.tenant, "content", &request.blobs[0].bytes)
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let guard = Arc::new(HoldBinding {
        entered: entered.clone(),
        release: release.clone(),
    });
    let pending_writer = writer.clone();
    let pending_request = request.clone();
    let append = tokio::spawn(async move {
        pending_writer
            .append_group_with_blobs_guarded(&pending_request, guard)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let pending_deleter = deleter.clone();
    let tenant = request.group.tenant.clone();
    let mut deletion =
        tokio::spawn(async move { pending_deleter.delete_blob(&tenant, "content").await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(150), &mut deletion)
            .await
            .is_err(),
        "standalone delete cannot remove a binding while admission holds it"
    );
    release.notify_one();
    append.await.unwrap().unwrap();
    deletion.await.unwrap().unwrap();
    assert_eq!(
        writer
            .get_blob(&request.group.tenant, "content")
            .await
            .unwrap(),
        None
    );
    assert!(
        writer
            .append_group_with_blobs(&request)
            .await
            .unwrap()
            .deduplicated
    );
    assert_eq!(
        writer
            .get_blob(&request.group.tenant, "content")
            .await
            .unwrap(),
        None
    );
    deleter.shutdown().await.unwrap();
    writer.drop_tables().await.unwrap();
    writer.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_atomic_blob_disjoint_streams_share_reversed_blob_sets() {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture");
    let setup = PostgresEventStore::connect(&url, "atomic_order")
        .await
        .unwrap();
    setup.drop_tables().await.unwrap();
    setup.shutdown().await.unwrap();
    let left = PostgresEventStore::connect(&url, "atomic_order")
        .await
        .unwrap();
    let right = PostgresEventStore::connect(&url, "atomic_order")
        .await
        .unwrap();
    for iteration in 0..8 {
        let mut a = eventlog_conformance::atomic_blob_request(
            &format!("left-{iteration}"),
            &format!("left-{iteration}"),
            &format!("shared-a-{iteration}"),
        );
        a.blobs.push(BlobWrite {
            digest: format!("shared-z-{iteration}"),
            bytes: b"shared".to_vec(),
        });
        let mut b = eventlog_conformance::atomic_blob_request(
            &format!("right-{iteration}"),
            &format!("right-{iteration}"),
            &format!("shared-a-{iteration}"),
        );
        b.blobs = a.blobs.iter().rev().cloned().collect();
        let results = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                left.append_group_with_blobs(&a),
                right.append_group_with_blobs(&b)
            )
        })
        .await
        .unwrap();
        assert!(!results.0.unwrap().deduplicated);
        assert!(!results.1.unwrap().deduplicated);
        for blob in &a.blobs {
            assert_eq!(
                left.get_blob(&a.group.tenant, &blob.digest).await.unwrap(),
                Some(blob.bytes.clone())
            );
        }
    }
    right.shutdown().await.unwrap();
    left.drop_tables().await.unwrap();
    left.shutdown().await.unwrap();
}

#[tokio::test]
async fn postgres_atomic_blob_content_contract() {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture");
    let store = PostgresEventStore::connect(&url, "atomic_blobs")
        .await
        .unwrap();
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
    let (cleanup, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    cleanup
        .batch_execute("DROP TABLE IF EXISTS atomic_blobs_p_atomic_blob_probe")
        .await
        .unwrap();
    drop(cleanup);
    driver.await.unwrap().unwrap();
    let store = PostgresEventStore::connect(&url, "atomic_blobs")
        .await
        .unwrap();
    eventlog_conformance::run_atomic_blobs(&store).await;
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_atomic_blob_lost_commit_response_resolves_without_resurrection() {
    use std::{
        io::{Read, Write},
        net::{Shutdown, TcpListener, TcpStream},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture");
    let setup = PostgresEventStore::connect(&url, "atomic_lost")
        .await
        .unwrap();
    setup.drop_tables().await.unwrap();
    setup.shutdown().await.unwrap();
    let setup = PostgresEventStore::connect(&url, "atomic_lost")
        .await
        .unwrap();
    setup.shutdown().await.unwrap();
    let parsed: tokio_postgres::Config = url.parse().unwrap();
    let upstream_port = parsed.get_ports()[0];
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let armed = Arc::new(AtomicBool::new(false));
    let intercepted = Arc::new(AtomicBool::new(false));
    let proxy_arm = armed.clone();
    let proxy_intercepted = intercepted.clone();
    let proxy = std::thread::spawn(move || {
        let (mut downstream, _) = listener.accept().unwrap();
        let mut upstream = TcpStream::connect(("127.0.0.1", upstream_port)).unwrap();
        downstream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        upstream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
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
        forward.join().unwrap();
    });
    let proxy_url = format!(
        "host=127.0.0.1 port={proxy_port} user={} dbname={}",
        parsed.get_user().unwrap(),
        parsed.get_dbname().unwrap()
    );
    let store = PostgresEventStore::connect(&proxy_url, "atomic_lost")
        .await
        .unwrap();
    let request = eventlog_conformance::atomic_blob_request("same-command", "one", "content");
    armed.store(true, Ordering::Release);
    assert_eq!(
        store.append_group_with_blobs(&request).await,
        Err(eventlog_core::EventLogError::UnknownCommit)
    );
    assert!(
        intercepted.load(Ordering::Acquire),
        "server committed before its response was removed"
    );
    store.shutdown().await.unwrap();
    proxy.join().unwrap();
    let store = PostgresEventStore::connect(&url, "atomic_lost")
        .await
        .unwrap();
    let events = store
        .read_stream(&request.group.appends[0].stream, 0, 10)
        .await
        .unwrap()
        .events;
    assert_eq!(events.len(), 1);
    assert_eq!(
        store
            .get_blob(&request.group.tenant, "content")
            .await
            .unwrap(),
        Some(request.blobs[0].bytes.clone())
    );
    let resolved = store.append_group_with_blobs(&request).await.unwrap();
    assert!(resolved.deduplicated);
    assert_eq!(resolved.appends[0].events, events);
    store
        .delete_blob(&request.group.tenant, "content")
        .await
        .unwrap();
    assert!(
        store
            .append_group_with_blobs(&request)
            .await
            .unwrap()
            .deduplicated
    );
    assert_eq!(
        store
            .get_blob(&request.group.tenant, "content")
            .await
            .unwrap(),
        None
    );
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn postgres_atomic_blob_reopen_retry_preserves_erasure_and_receipt() {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture");
    let store = PostgresEventStore::connect(&url, "atomic_reopen")
        .await
        .unwrap();
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
    let request = eventlog_conformance::atomic_blob_request("reopen", "one", "content");
    let store = PostgresEventStore::connect(&url, "atomic_reopen")
        .await
        .unwrap();
    let original = store.append_group_with_blobs(&request).await.unwrap();
    store.shutdown().await.unwrap();
    let store = PostgresEventStore::connect(&url, "atomic_reopen")
        .await
        .unwrap();
    let retried = store.append_group_with_blobs(&request).await.unwrap();
    assert!(retried.deduplicated);
    assert_eq!(retried.appends[0].events, original.appends[0].events);
    store
        .delete_blob(&request.group.tenant, "content")
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let store = PostgresEventStore::connect(&url, "atomic_reopen")
        .await
        .unwrap();
    assert!(
        store
            .append_group_with_blobs(&request)
            .await
            .unwrap()
            .deduplicated
    );
    assert_eq!(
        store
            .get_blob(&request.group.tenant, "content")
            .await
            .unwrap(),
        None
    );
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_atomic_blob_independent_writers_keep_only_complete_winner() {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture");
    for mode in ["fresh", "shared", "existing", "collision"] {
        let setup = PostgresEventStore::connect(&url, "atomic_race")
            .await
            .unwrap();
        setup.drop_tables().await.unwrap();
        setup.shutdown().await.unwrap();
        let left = PostgresEventStore::connect(&url, "atomic_race")
            .await
            .unwrap();
        let right = PostgresEventStore::connect(&url, "atomic_race")
            .await
            .unwrap();
        let mut requests = [
            eventlog_conformance::atomic_blob_request("left", "one", "left-content"),
            eventlog_conformance::atomic_blob_request("right", "one", "right-content"),
        ];
        if mode != "fresh" {
            for (index, request) in requests.iter_mut().enumerate() {
                request.blobs.push(BlobWrite {
                    digest: "shared-content".into(),
                    bytes: if mode == "collision" {
                        vec![u8::try_from(index).unwrap()]
                    } else {
                        b"shared".to_vec()
                    },
                });
            }
        }
        if mode == "existing" {
            left.put_blob(&requests[0].group.tenant, "shared-content", b"shared")
                .await
                .unwrap();
        }
        // Reverse input order while retaining the primary losing key at index zero
        // in the independent expected requests passed to the assertion helper.
        let mut reversed = requests[1].clone();
        reversed.blobs.reverse();
        let (a, b) = tokio::join!(
            left.append_group_with_blobs(&requests[0]),
            right.append_group_with_blobs(&reversed)
        );
        let (observer, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let count: i64 = observer
            .query_one("SELECT COUNT(*) FROM atomic_race_blobs", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, if mode == "fresh" { 1 } else { 2 });
        drop(observer);
        driver.await.unwrap().unwrap();
        eventlog_conformance::assert_atomic_blob_competition(&left, &requests, &[a, b]).await;
        right.shutdown().await.unwrap();
        left.drop_tables().await.unwrap();
        left.shutdown().await.unwrap();
    }
}
