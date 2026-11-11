//! `read_many` and `append_group` on PostgreSQL answer in batches, not one statement per item.
//!
//! The answers must not move: each batched read equals the separate `read_stream`/`get_blob`, and
//! a batched group writes what one statement per event wrote. The statement counts are read off
//! the wire by a proxy between the store and the fixture, which logs every extended-protocol
//! `Bind` and every simple `Query` the store sends.

use std::sync::{Arc, Mutex};

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, EventLogError, EventStore, Expected, Read, ReadResult,
    StreamAppend, StreamId, TenantId,
};
use eventlog_postgres::{PoolOptions, PostgresEventStore};

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL")
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
    format!("brm_{run}_{case}")
}

fn tenant(name: &str) -> TenantId {
    TenantId::new(name).unwrap()
}

fn stream(tenant: &TenantId, id: &str) -> StreamId {
    StreamId::new(tenant.clone(), "item", id).unwrap()
}

/// What the store sent, in order: `B` for each `Bind`, `Q:<text>` for each simple query.
type Wire = Arc<Mutex<Vec<String>>>;

/// A proxy that forwards every connection to the fixture and logs the client's statements.
fn counting_proxy() -> (String, Wire) {
    use std::{
        io::{Read as _, Write as _},
        net::{Shutdown, TcpListener, TcpStream},
    };
    let parsed: tokio_postgres::Config = url().parse().unwrap();
    let upstream_port = parsed.get_ports()[0];
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let wire: Wire = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&wire);
    std::thread::spawn(move || {
        for downstream in listener.incoming() {
            let Ok(mut downstream) = downstream else {
                return;
            };
            let log = Arc::clone(&log);
            std::thread::spawn(move || {
                let mut upstream = TcpStream::connect(("127.0.0.1", upstream_port)).unwrap();
                downstream.set_nodelay(true).unwrap();
                upstream.set_nodelay(true).unwrap();
                let mut replies = upstream.try_clone().unwrap();
                let mut client = downstream.try_clone().unwrap();
                let back = std::thread::spawn(move || {
                    let _ = std::io::copy(&mut replies, &mut client);
                    let _ = client.shutdown(Shutdown::Both);
                });
                // The startup (or cancel) message carries no type byte.
                let mut length = [0_u8; 4];
                if downstream.read_exact(&mut length).is_ok() {
                    let mut body =
                        vec![0; usize::try_from(u32::from_be_bytes(length) - 4).unwrap()];
                    if downstream.read_exact(&mut body).is_ok()
                        && upstream
                            .write_all(&length)
                            .and_then(|()| upstream.write_all(&body))
                            .is_ok()
                    {
                        loop {
                            let mut header = [0_u8; 5];
                            if downstream.read_exact(&mut header).is_err() {
                                break;
                            }
                            let length = u32::from_be_bytes(header[1..].try_into().unwrap());
                            let mut body = vec![0; usize::try_from(length - 4).unwrap()];
                            if downstream.read_exact(&mut body).is_err() {
                                break;
                            }
                            match header[0] {
                                b'B' => log.lock().unwrap().push("B".to_owned()),
                                b'Q' => log.lock().unwrap().push(format!(
                                    "Q:{}",
                                    String::from_utf8_lossy(&body).trim_end_matches('\0')
                                )),
                                _ => {}
                            }
                            if upstream
                                .write_all(&header)
                                .and_then(|()| upstream.write_all(&body))
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                }
                let _ = upstream.shutdown(Shutdown::Both);
                let _ = back.join();
            });
        }
    });
    let password = parsed.get_password().map_or_else(String::new, |password| {
        format!(" password={}", String::from_utf8_lossy(password))
    });
    let url = format!(
        "host=127.0.0.1 port={proxy_port} user={} dbname={}{password}",
        parsed.get_user().unwrap(),
        parsed.get_dbname().unwrap()
    );
    (url, wire)
}

/// Run `work` and return what it sent on the wire.
async fn observed<T>(wire: &Wire, work: impl std::future::Future<Output = T>) -> (T, Vec<String>) {
    let start = wire.lock().unwrap().len();
    let outcome = work.await;
    let sent = wire.lock().unwrap()[start..].to_vec();
    (outcome, sent)
}

fn binds(sent: &[String]) -> usize {
    sent.iter().filter(|message| *message == "B").count()
}

/// The reads each case must answer as `read_stream` would, built over a written fixture.
async fn fixture(store: &PostgresEventStore) -> Vec<Read> {
    let a = tenant("batch-a");
    let b = tenant("batch-b");
    let long = stream(&a, "long");
    let redacted = stream(&a, "redacted");
    let twin = stream(&b, "long");
    let missing = stream(&a, "never-written");
    store
        .append(
            &long,
            Expected::NoStream,
            &(1..=7)
                .map(|n| event("item.changed", n))
                .collect::<Vec<_>>(),
            &meta("long", &serde_json::json!({"n": 1})),
        )
        .await
        .unwrap();
    store
        .append(
            &redacted,
            Expected::NoStream,
            &(1..=3)
                .map(|n| event("item.changed", n))
                .collect::<Vec<_>>(),
            &meta("redacted", &serde_json::json!({"n": 2})),
        )
        .await
        .unwrap();
    store
        .redact(&redacted, 2, "erased on request")
        .await
        .unwrap();
    store
        .append(
            &twin,
            Expected::NoStream,
            &[event("item.changed", 99)],
            &meta("twin", &serde_json::json!({"n": 3})),
        )
        .await
        .unwrap();
    store.put_blob(&a, "present", b"blob bytes").await.unwrap();

    let mut reads = Vec::new();
    for (target, after_version, limit) in [
        (&long, 0, 10),
        (&long, 0, 1),
        (&long, 2, 3),
        (&long, 6, 1),
        (&long, 7, 5),
        (&long, 40, 5),
        (&long, 0, 0),
        (&long, 0, 50_000),
        (&redacted, 0, 10),
        (&redacted, 1, 1),
        (&twin, 0, 10),
        (&missing, 0, 10),
        (&missing, 3, 1),
    ] {
        reads.push(Read::Stream {
            stream: target.clone(),
            after_version,
            limit,
        });
    }
    reads.push(Read::Blob {
        tenant: a.clone(),
        digest: "present".into(),
    });
    reads.push(Read::Blob {
        tenant: a.clone(),
        digest: "absent".into(),
    });
    reads.push(Read::Blob {
        tenant: b.clone(),
        digest: "present".into(),
    });
    reads
}

async fn separately(store: &PostgresEventStore, reads: &[Read]) -> Vec<ReadResult> {
    let mut answers = Vec::with_capacity(reads.len());
    for read in reads {
        answers.push(match read {
            Read::Stream {
                stream,
                after_version,
                limit,
            } => ReadResult::Stream(
                store
                    .read_stream(stream, *after_version, *limit)
                    .await
                    .unwrap(),
            ),
            Read::Blob { tenant, digest } => {
                ReadResult::Blob(store.get_blob(tenant, digest).await.unwrap())
            }
        });
    }
    answers
}

/// Each answer equals the separate call's, in order, across statement-batch boundaries.
#[tokio::test]
async fn read_many_answers_each_read_as_read_stream_would() {
    let store = PostgresEventStore::connect(&url(), &prefix("equal"))
        .await
        .unwrap();
    let cases = fixture(&store).await;
    // 2,500 reads cross two batch boundaries, with every case on either side of each.
    let reads: Vec<Read> = cases.iter().cycle().take(2_500).cloned().collect();
    let batch = store.read_many(&reads).await.unwrap();
    let expected = separately(&store, &cases).await;
    assert_eq!(batch.len(), reads.len());
    for (index, answer) in batch.iter().enumerate() {
        assert_eq!(
            answer,
            &expected[index % cases.len()],
            "read {index} ({:?}) answered differently from the separate call",
            reads[index]
        );
    }
    let redacted = match &batch[8] {
        ReadResult::Stream(slice) => slice.events[1].clone(),
        ReadResult::Blob(_) => unreachable!(),
    };
    assert!(redacted.redacted_at.is_some(), "the redaction is visible");
    assert_eq!(store.read_many(&[]).await.unwrap(), Vec::new());
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

/// A version that cannot be stored is refused for the read that names it, as the separate call
/// refuses it.
#[tokio::test]
async fn read_many_refuses_what_read_stream_refuses() {
    let store = PostgresEventStore::connect(&url(), &prefix("refuse"))
        .await
        .unwrap();
    let a = tenant("batch-a");
    let reads = [
        Read::Stream {
            stream: stream(&a, "one"),
            after_version: 0,
            limit: 1,
        },
        Read::Stream {
            stream: stream(&a, "two"),
            after_version: u64::MAX,
            limit: 1,
        },
    ];
    let separate = store.read_stream(&stream(&a, "two"), u64::MAX, 1).await;
    let batch = store.read_many(&reads).await;
    assert!(matches!(separate, Err(EventLogError::Invalid(_))));
    assert_eq!(
        format!("{:?}", batch.unwrap_err()),
        format!("{:?}", separate.unwrap_err())
    );
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

/// N stream reads are at most ceil(N / 1,000) statements, all inside one REPEATABLE READ
/// transaction, and blob reads join the same snapshot.
#[tokio::test]
async fn read_many_answers_in_bounded_statements_inside_one_snapshot() {
    let name = prefix("wire");
    let (proxied, wire) = counting_proxy();
    let setup = PostgresEventStore::connect(&url(), &name).await.unwrap();
    let cases = fixture(&setup).await;
    let store = PostgresEventStore::connect_local(&proxied, &name, PoolOptions::default())
        .await
        .unwrap();
    let streams: Vec<Read> = cases
        .iter()
        .filter(|read| matches!(read, Read::Stream { .. }))
        .cycle()
        .take(2_500)
        .cloned()
        .collect();
    let blobs: Vec<Read> = cases
        .iter()
        .filter(|read| matches!(read, Read::Blob { .. }))
        .cloned()
        .collect();
    let mut reads = streams;
    reads.extend(blobs);
    // An open, configured session first, so the window holds the batch's messages only.
    store.read_many(&reads[..1]).await.unwrap();
    let (batch, sent) = observed(&wire, store.read_many(&reads)).await;
    assert_eq!(batch.unwrap().len(), reads.len());
    assert!(
        binds(&sent) <= 3 + 1,
        "2,500 stream reads and 3 blob reads took {} statements",
        binds(&sent)
    );
    // Besides opening and closing the one transaction, the batch only paces its statements: each
    // gets the statement timeout of the reads it carries (1,000 x the default 5 s, then 500 x,
    // then 3 x for the blobs).
    let control: Vec<&String> = sent
        .iter()
        .filter(|m| m.starts_with("Q:") && !m.starts_with("Q:SET LOCAL statement_timeout = "))
        .collect();
    assert_eq!(
        control.len(),
        2,
        "one transaction, opened and closed: {control:?}"
    );
    let paces: Vec<&str> = sent
        .iter()
        .filter_map(|m| m.strip_prefix("Q:SET LOCAL statement_timeout = "))
        .collect();
    assert_eq!(paces, ["5000000", "5000000", "2500000", "15000"]);
    assert!(
        sent.first()
            .is_some_and(|m| m.starts_with("Q:START TRANSACTION") && m.contains("REPEATABLE READ")),
        "the batch opens one snapshot before its first read: {:?}",
        sent.first()
    );
    assert_eq!(sent.last().map(String::as_str), Some("Q:COMMIT"));
    store.shutdown().await.unwrap();
    setup.drop_tables().await.unwrap();
    setup.shutdown().await.unwrap();
}

fn wide_group(tenant: &TenantId, key: &str, members: usize) -> AppendGroup {
    let mut appends: Vec<StreamAppend> = (0..members)
        .map(|n| StreamAppend {
            stream: stream(tenant, &format!("instance-{n}")),
            expected: Expected::NoStream,
            events: vec![event("item.changed", i64::try_from(n).unwrap())],
        })
        .collect();
    // One stream repeated, the second time at a version the group itself wrote, and one full
    // append: the in-group head must carry from member to member.
    appends.push(StreamAppend {
        stream: stream(tenant, "instance-0"),
        expected: Expected::Exact(1),
        events: (0..1_024).map(|n| event("item.bulk", n)).collect(),
    });
    appends.push(StreamAppend {
        stream: stream(tenant, "instance-1"),
        expected: Expected::Any,
        events: vec![event("item.changed", -1)],
    });
    AppendGroup {
        tenant: tenant.clone(),
        appends,
        meta: meta(key, &serde_json::json!({ "key": key })),
    }
}

/// A group writes its events in a bounded number of statements, not one or three per member.
#[tokio::test]
async fn append_group_writes_in_bounded_statements() {
    let name = prefix("gwire");
    let (proxied, wire) = counting_proxy();
    let setup = PostgresEventStore::connect(&url(), &name).await.unwrap();
    let store = PostgresEventStore::connect_local(&proxied, &name, PoolOptions::default())
        .await
        .unwrap();
    let t = tenant("batch-group");
    let group = wide_group(&t, "wide", 2_500);
    let events: usize = group.appends.iter().map(|append| append.events.len()).sum();
    let (written, sent) = observed(&wire, store.append_group(&group)).await;
    let written = written.unwrap();
    assert!(!written.deduplicated);
    // Gate, group lock and receipt lookup; per 1,000 streams one lock and one head statement;
    // per 1,000 events one insert; the receipt.
    let bound = 3 + 2 * 2_500_usize.div_ceil(1_000) + events.div_ceil(1_000) + 1;
    assert!(
        binds(&sent) <= bound,
        "a group of {} members and {events} events took {} statements (bound {bound})",
        group.appends.len(),
        binds(&sent)
    );

    let single = stream(&t, "single");
    let full: Vec<_> = (0..1_024).map(|n| event("item.bulk", n)).collect();
    let (appended, sent) = observed(
        &wire,
        store.append(
            &single,
            Expected::NoStream,
            &full,
            &meta("single", &serde_json::json!({})),
        ),
    )
    .await;
    assert_eq!(appended.unwrap().events.len(), 1_024);
    assert!(
        binds(&sent) <= 8,
        "an append of 1,024 events took {} statements",
        binds(&sent)
    );
    store.shutdown().await.unwrap();
    setup.drop_tables().await.unwrap();
    setup.shutdown().await.unwrap();
}

/// What a group returns is what was stored, in the group's order, and it commits as one unit.
#[tokio::test]
async fn a_batched_group_returns_and_stores_what_it_wrote() {
    let store = PostgresEventStore::connect(&url(), &prefix("gequal"))
        .await
        .unwrap();
    let t = tenant("batch-group");
    let group = wide_group(&t, "wide", 1_200);
    let written = store.append_group(&group).await.unwrap();
    assert_eq!(written.appends.len(), group.appends.len());
    let mut previous = 0;
    for (append, request) in written.appends.iter().zip(&group.appends) {
        assert_eq!(append.events.len(), request.events.len());
        assert_eq!(
            append.last_version - append.first_version + 1,
            u64::try_from(request.events.len()).unwrap()
        );
        for (recorded, requested) in append.events.iter().zip(&request.events) {
            assert!(
                recorded.global_seq > previous,
                "positions follow the group's order"
            );
            previous = recorded.global_seq;
            assert_eq!(recorded.name, requested.name);
            assert_eq!(recorded.data, requested.data);
            assert_eq!(recorded.stream_id, request.stream.stream_id());
        }
        // A page holds at most 1,000 events; the 1,024-event member takes two.
        let mut stored = Vec::new();
        let mut after = append.first_version - 1;
        while after < append.last_version {
            let page = store
                .read_stream(&request.stream, after, 1_000)
                .await
                .unwrap();
            if page.events.is_empty() {
                break;
            }
            stored.extend(
                page.events
                    .into_iter()
                    .filter(|event| event.version <= append.last_version),
            );
            after = page.next_version;
        }
        assert_eq!(stored, append.events);
    }
    let repeated = &written.appends[1_200];
    assert_eq!((repeated.first_version, repeated.last_version), (2, 1_025));
    assert_eq!(
        (
            written.appends[1_201].first_version,
            written.appends[1_201].last_version
        ),
        (2, 2)
    );
    let retry = store.append_group(&group).await.unwrap();
    assert!(retry.deduplicated);
    assert_eq!(
        retry
            .appends
            .iter()
            .map(|a| a.events.clone())
            .collect::<Vec<_>>(),
        written
            .appends
            .iter()
            .map(|a| a.events.clone())
            .collect::<Vec<_>>()
    );

    // A late member's conflict refuses the whole group and writes nothing.
    let late = AppendGroup {
        tenant: t.clone(),
        appends: vec![
            StreamAppend {
                stream: stream(&t, "fresh"),
                expected: Expected::NoStream,
                events: vec![event("item.changed", 1)],
            },
            StreamAppend {
                stream: stream(&t, "instance-1"),
                expected: Expected::Exact(2),
                events: vec![event("item.changed", 1)],
            },
            StreamAppend {
                stream: stream(&t, "instance-5"),
                expected: Expected::Exact(7),
                events: vec![event("item.changed", 1)],
            },
        ],
        meta: meta("late", &serde_json::json!({ "key": "late" })),
    };
    let refused = store.append_group(&late).await.unwrap_err();
    assert!(
        matches!(
            refused,
            EventLogError::Conflict {
                expected: 7,
                actual: 1
            }
        ),
        "{refused:?}"
    );
    assert_eq!(
        store.stream_version(&stream(&t, "fresh")).await.unwrap(),
        None
    );
    assert_eq!(
        store
            .stream_version(&stream(&t, "instance-0"))
            .await
            .unwrap(),
        Some(1_025)
    );
    assert_eq!(
        store
            .stream_version(&stream(&t, "instance-1"))
            .await
            .unwrap(),
        Some(2)
    );
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}
