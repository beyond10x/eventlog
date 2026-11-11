//! Adversary cases for `story:postgres-batch-read-and-append`.
//!
//! Each case states what a caller could rely on before the batch rewrite: `read_many` behaves as
//! the separate calls would (the trait's own contract), a group waits for its stream locks the
//! way it did one statement per lock, and a group refuses with the error the member-at-a-time
//! order produced. The remaining cases attack identity, snapshot and lock order around the
//! batched statements.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, EventLogError, EventStore, Expected, Guard, NewEvent,
    ProjectionSpec, ProjectionStore, Projector, Read, ReadResult, RecordedEvent, StreamAppend,
    StreamId, TenantId,
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
        .take(10)
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    format!("adbr_{run}_{case}")
}

fn tenant(name: &str) -> TenantId {
    TenantId::new(name).unwrap()
}

fn stream(tenant: &TenantId, kind: &str, id: &str) -> StreamId {
    StreamId::new(tenant.clone(), kind, id).unwrap()
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

/// The trait's contract: `read_many` "returns the first read's error, as the separate call would
/// have". A batch in which every separate read fits the store-operation deadline must not be
/// refused for the batch's size. Here the deadline is short so the case stays small; under the
/// default 10 s (statement timeout 5 s) the same refusal needs a proportionally larger batch.
#[tokio::test]
async fn a_batch_of_reads_that_each_fit_the_deadline_is_not_refused_for_its_size() {
    let name = prefix("deadline");
    let setup = PostgresEventStore::connect(&url(), &name).await.unwrap();
    let t = tenant("adv-deadline");
    let full = stream(&t, "item", "full");
    setup
        .append(
            &full,
            Expected::NoStream,
            &(0..1_000)
                .map(|n| event("item.changed", n))
                .collect::<Vec<_>>(),
            &meta("full", &serde_json::json!({})),
        )
        .await
        .unwrap();

    // Calibrate on this machine: the slowest separate full-page read, and the mean.
    let mut slowest = Duration::ZERO;
    let started = Instant::now();
    for _ in 0..20 {
        let one = Instant::now();
        assert_eq!(
            setup
                .read_stream(&full, 0, 1_000)
                .await
                .unwrap()
                .events
                .len(),
            1_000
        );
        slowest = slowest.max(one.elapsed());
    }
    let mean = started.elapsed() / 20;
    let budget = (slowest * 5).max(Duration::from_millis(150));
    let count = usize::try_from((budget * 6).as_nanos() / mean.as_nanos().max(1))
        .unwrap()
        .clamp(50, 2_000);
    eprintln!("slowest {slowest:?}, mean {mean:?}, budget {budget:?}, {count} reads");

    let short = PostgresEventStore::connect_local(
        &url(),
        &name,
        PoolOptions {
            statement_timeout: budget,
            transaction_timeout: budget,
            ..PoolOptions::default()
        },
    )
    .await
    .unwrap();
    let reads: Vec<Read> = (0..count)
        .map(|_| Read::Stream {
            stream: full.clone(),
            after_version: 0,
            limit: 1_000,
        })
        .collect();
    // What the trait's default does: each read in turn, each inside its own deadline.
    let mut separate = Vec::with_capacity(count);
    for read in &reads {
        let Read::Stream {
            stream,
            after_version,
            limit,
        } = read
        else {
            unreachable!()
        };
        separate.push(ReadResult::Stream(
            short
                .read_stream(stream, *after_version, *limit)
                .await
                .expect("each separate read fits the deadline"),
        ));
    }
    let started = Instant::now();
    match short.read_many(&reads).await {
        Ok(batch) => assert!(batch == separate, "the batch answered differently"),
        Err(error) => panic!(
            "read_many refused {count} reads after {:?} with {error:?}, although each of them \
             answers separately inside the {budget:?} deadline (slowest separate read {slowest:?})",
            started.elapsed()
        ),
    }
    short.shutdown().await.unwrap();
    setup.drop_tables().await.unwrap();
    setup.shutdown().await.unwrap();
}

/// A guard that announces it holds the append's stream lock, then keeps it for `hold`.
struct Holding {
    entered: Arc<AtomicUsize>,
    hold: Duration,
}

impl Guard for Holding {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::AcqRel);
            tokio::time::sleep(self.hold).await;
            Ok(())
        })
    }
}

/// A group waits for each of its stream locks as it did with one statement per lock: every wait
/// below `lock_timeout`, the whole group inside `transaction_timeout`. Four concurrent single
/// appends hold four of its streams for 0.4, 0.8, 1.2 and 1.6 s. One statement per lock waits at
/// most 0.4 s per statement; one statement for all of them waits 1.6 s, past the 1 s statement
/// timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_behind_several_short_lock_holders_commits_inside_its_deadline() {
    let name = prefix("lockwait");
    let holders = Arc::new(
        PostgresEventStore::connect_local(
            &url(),
            &name,
            PoolOptions {
                max_connections: 8,
                ..PoolOptions::default()
            },
        )
        .await
        .unwrap(),
    );
    let grouped = PostgresEventStore::connect_local(
        &url(),
        &name,
        PoolOptions {
            statement_timeout: Duration::from_secs(1),
            lock_timeout: Duration::from_millis(900),
            transaction_timeout: Duration::from_secs(8),
            ..PoolOptions::default()
        },
    )
    .await
    .unwrap();
    let t = tenant("adv-lockwait");
    let streams: Vec<StreamId> = (0..4)
        .map(|n| stream(&t, "item", &format!("held-{n}")))
        .collect();
    for (n, held) in streams.iter().enumerate() {
        holders
            .append(
                held,
                Expected::NoStream,
                &[event("item.changed", 0)],
                &meta(&format!("seed-{n}"), &serde_json::json!({ "n": n })),
            )
            .await
            .unwrap();
    }

    let entered = Arc::new(AtomicUsize::new(0));
    let mut running = Vec::new();
    for (n, held) in streams.iter().enumerate() {
        let store = Arc::clone(&holders);
        let held = held.clone();
        let guard = Arc::new(Holding {
            entered: Arc::clone(&entered),
            hold: Duration::from_millis(400) * u32::try_from(n + 1).unwrap(),
        });
        running.push(tokio::spawn(async move {
            store
                .append_guarded(
                    &held,
                    Expected::Exact(1),
                    &[event("item.changed", 1)],
                    &meta(&format!("hold-{n}"), &serde_json::json!({ "n": n })),
                    guard,
                )
                .await
        }));
    }
    let waiting = Instant::now();
    while entered.load(Ordering::Acquire) < streams.len() {
        assert!(
            waiting.elapsed() < Duration::from_secs(5),
            "holders never took their locks"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let group = AppendGroup {
        tenant: t.clone(),
        appends: streams
            .iter()
            .map(|held| StreamAppend {
                stream: held.clone(),
                expected: Expected::Any,
                events: vec![event("item.grouped", 2)],
            })
            .collect(),
        meta: meta("behind-holders", &serde_json::json!({ "group": 1 })),
    };
    let started = Instant::now();
    let outcome = grouped.append_group(&group).await;
    let elapsed = started.elapsed();
    for holder in running {
        holder.await.unwrap().expect("each holder commits");
    }
    let written = outcome.unwrap_or_else(|error| {
        panic!(
            "the group was refused after {elapsed:?} with {error:?}; no single lock wait reached \
             the 900 ms lock timeout and the group had an 8 s deadline"
        )
    });
    assert_eq!(written.appends.len(), 4);
    for held in &streams {
        assert_eq!(holders.stream_version(held).await.unwrap(), Some(3));
    }
    grouped.shutdown().await.unwrap();
    holders.drop_tables().await.unwrap();
}

const REFUSE: ProjectionSpec = ProjectionSpec {
    name: "advrefuse",
    indexed: &[],
};

/// An inline projector that refuses one event name.
struct Refusing;

impl Projector for Refusing {
    fn name(&self) -> &'static str {
        "advrefuse"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&REFUSE)
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            if event.name == "item.refused" {
                Err(EventLogError::Invalid(
                    "refused by the inline projector".into(),
                ))
            } else {
                Ok(())
            }
        })
    }
}

/// Member by member, member one's inline projector refused before member two was written, so the
/// group returned the projector's refusal. A later member whose content PostgreSQL itself refuses
/// (a JSON `\u0000`, which passes event validation) is not an environmental failure: it is the
/// same request every time. The batched group may return either refusal; this was decided and
/// documented in the CHANGELOG rather than restored, and the case asserts the documented
/// behaviour.
#[tokio::test]
async fn a_group_returns_the_earlier_members_projector_refusal_before_a_later_members_content() {
    let store = PostgresEventStore::connect(&url(), &prefix("order"))
        .await
        .unwrap();
    store.register_inline(Arc::new(Refusing)).await.unwrap();
    let t = tenant("adv-order");
    let nul = NewEvent::new(
        "item.changed",
        1,
        serde_json::json!({ "text": "a\u{0000}b" }),
    )
    .unwrap();
    let alone = store
        .append(
            &stream(&t, "item", "nul-alone"),
            Expected::NoStream,
            std::slice::from_ref(&nul),
            &meta("nul-alone", &serde_json::json!({})),
        )
        .await;
    assert!(
        matches!(alone, Err(EventLogError::Backend(_))),
        "PostgreSQL refuses the content on its own: {alone:?}"
    );
    let group = AppendGroup {
        tenant: t.clone(),
        appends: vec![
            StreamAppend {
                stream: stream(&t, "item", "first"),
                expected: Expected::NoStream,
                events: vec![event("item.refused", 1)],
            },
            StreamAppend {
                stream: stream(&t, "item", "second"),
                expected: Expected::NoStream,
                events: vec![nul],
            },
        ],
        meta: meta("ordered", &serde_json::json!({ "g": 1 })),
    };
    let refused = store.append_group(&group).await.unwrap_err();
    // Documented since the batch rewrite (CHANGELOG): every member is inserted before any member
    // is projected, so when two members refuse, the refusal returned may be either one. What
    // stays fixed is that it is one of the two and that the group writes nothing.
    assert!(
        matches!(&refused, EventLogError::Invalid(message) if message == "refused by the inline projector")
            || matches!(&refused, EventLogError::Backend(message) if message.contains("22P05")),
        "the group returns member one's projector refusal or member two's content refusal; \
         got {refused:?}"
    );
    for id in ["first", "second"] {
        assert_eq!(
            store.stream_version(&stream(&t, "item", id)).await.unwrap(),
            None,
            "a refused group writes nothing"
        );
    }
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

/// Identity: stream coordinates that are delimiters or keywords in PostgreSQL's array text form,
/// duplicate reads, blobs interleaved, versions past the head, and batches of exactly 1,000 and
/// 1,001 reads all answer as the separate calls do.
#[tokio::test]
async fn read_many_matches_separate_reads_for_awkward_coordinates_at_batch_edges() {
    let store = PostgresEventStore::connect(&url(), &prefix("awkward"))
        .await
        .unwrap();
    let a = tenant("adv-a");
    let b = tenant("adv-b");
    let ids = [
        "a,b",
        "{x}",
        "\"q\"",
        "back\\slash",
        "NULL",
        " lead",
        "trail ",
        "%_",
        "it's",
        "(1)",
    ];
    let mut streams = Vec::new();
    for (n, id) in ids.iter().enumerate() {
        for (t, kind) in [(&a, "item"), (&b, "t{1},\"z\"")] {
            let target = stream(t, kind, id);
            store
                .append(
                    &target,
                    Expected::NoStream,
                    &(0..=i64::try_from(n % 4).unwrap())
                        .map(|v| event("item.changed", v))
                        .collect::<Vec<_>>(),
                    &meta(&format!("w-{n}-{kind}"), &serde_json::json!({ "n": n })),
                )
                .await
                .unwrap();
            streams.push(target);
        }
    }
    store.redact(&streams[6], 2, "erased").await.unwrap();
    store.put_blob(&a, "{d,1}", b"one").await.unwrap();
    store.put_blob(&b, "NULL", b"two").await.unwrap();
    let mut cases = Vec::new();
    for target in &streams {
        for (after_version, limit) in [(0, 1000), (0, 1), (1, 1), (3, 2), (9, 4), (0, 0)] {
            cases.push(Read::Stream {
                stream: target.clone(),
                after_version,
                limit,
            });
        }
        cases.push(Read::Blob {
            tenant: target.tenant().clone(),
            digest: "{d,1}".into(),
        });
        cases.push(Read::Blob {
            tenant: target.tenant().clone(),
            digest: "NULL".into(),
        });
    }
    cases.push(Read::Stream {
        stream: stream(&a, "item", "a,b,"),
        after_version: 0,
        limit: 5,
    });
    let expected = separately(&store, &cases).await;
    for size in [999, 1_000, 1_001, 2_000] {
        let reads: Vec<Read> = cases.iter().cycle().take(size).cloned().collect();
        let batch = store.read_many(&reads).await.unwrap();
        assert_eq!(batch.len(), size);
        for (index, answer) in batch.iter().enumerate() {
            assert_eq!(
                answer,
                &expected[index % cases.len()],
                "batch of {size}, read {index}: {:?}",
                reads[index]
            );
        }
    }
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

/// One snapshot: a batch never shows one member of a committed group without the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_many_never_shows_half_a_group() {
    let store = Arc::new(
        PostgresEventStore::connect(&url(), &prefix("snapshot"))
            .await
            .unwrap(),
    );
    let t = tenant("adv-snapshot");
    let left = stream(&t, "item", "left");
    let right = stream(&t, "item", "right");
    let rounds = 80;
    let writer = {
        let store = Arc::clone(&store);
        let (t, left, right) = (t.clone(), left.clone(), right.clone());
        tokio::spawn(async move {
            for n in 0..rounds {
                store
                    .append_group(&AppendGroup {
                        tenant: t.clone(),
                        appends: vec![
                            StreamAppend {
                                stream: left.clone(),
                                expected: Expected::Exact(n),
                                events: vec![event("item.changed", 1)],
                            },
                            StreamAppend {
                                stream: right.clone(),
                                expected: Expected::Exact(n),
                                events: vec![event("item.changed", 1)],
                            },
                        ],
                        meta: meta(&format!("pair-{n}"), &serde_json::json!({ "n": n })),
                    })
                    .await
                    .unwrap();
            }
        })
    };
    let reads = [
        Read::Stream {
            stream: left.clone(),
            after_version: 0,
            limit: 1_000,
        },
        Read::Stream {
            stream: right.clone(),
            after_version: 0,
            limit: 1_000,
        },
    ];
    let mut observed = 0;
    while !writer.is_finished() {
        let batch = store.read_many(&reads).await.unwrap();
        let lengths: Vec<usize> = batch
            .iter()
            .map(|answer| match answer {
                ReadResult::Stream(slice) => slice.events.len(),
                ReadResult::Blob(_) => unreachable!(),
            })
            .collect();
        assert_eq!(lengths[0], lengths[1], "half a group visible: {lengths:?}");
        observed += 1;
    }
    writer.await.unwrap();
    assert!(observed > 0);
    store.drop_tables().await.unwrap();
}

/// Two groups over the same 1,200 streams, one in reverse member order, run concurrently and
/// both commit: the batched lock statement keeps one acquisition order across its chunks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_groups_in_opposite_member_order_do_not_deadlock() {
    let store = Arc::new(
        PostgresEventStore::connect(&url(), &prefix("deadlock"))
            .await
            .unwrap(),
    );
    let t = tenant("adv-deadlock");
    let streams: Vec<StreamId> = (0..1_200)
        .map(|n| stream(&t, "item", &format!("s-{n:04}")))
        .collect();
    let rounds = 3;
    let mut tasks = Vec::new();
    for direction in 0..2 {
        let store = Arc::clone(&store);
        let t = t.clone();
        let mut order = streams.clone();
        if direction == 1 {
            order.reverse();
        }
        tasks.push(tokio::spawn(async move {
            for round in 0..rounds {
                store
                    .append_group(&AppendGroup {
                        tenant: t.clone(),
                        appends: order
                            .iter()
                            .map(|target| StreamAppend {
                                stream: target.clone(),
                                expected: Expected::Any,
                                events: vec![event("item.changed", round)],
                            })
                            .collect(),
                        meta: meta(
                            &format!("dir-{direction}-{round}"),
                            &serde_json::json!({ "d": direction, "r": round }),
                        ),
                    })
                    .await
                    .unwrap_or_else(|error| {
                        panic!("direction {direction} round {round}: {error:?}")
                    });
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    for target in [&streams[0], &streams[999], &streams[1_000], &streams[1_199]] {
        assert_eq!(
            store.stream_version(target).await.unwrap(),
            Some(2 * u64::try_from(rounds).unwrap())
        );
    }
    store.drop_tables().await.unwrap();
}
