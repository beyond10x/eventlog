//! Adversary pass 1 on story:file-eventlog-indexes-streams.
//!
//! The unit's own cases fold journal frames through `State::fold` and compare the result with
//! `State::replay`, which is the same function in a loop. The incremental fold a handle actually
//! runs is a cached view, built by in-transaction `record` calls, onto which `enter` folds the
//! frames *another handle* committed. These cases drive that path, stream names that differ only
//! across the type/id boundary of the index key, every window boundary, and retried receipts,
//! and compare each per-stream read with the tenant feed (which walks events by position, not
//! through the index) on three handles: the writer, a resumed second writer, and a fresh opener.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AppendResult, AtomicEventStore, EventStore, Expected, MAX_READ_LIMIT, Read,
    ReadResult, RecordedEvent, StreamAppend, StreamId, StreamSlice, TenantId,
};
use eventlog_file::FileEventStore;
use serde_json::json;
use std::{collections::BTreeMap, path::Path};

/// A fixed-seed linear congruential generator, so the run is the same on every machine.
struct Seeded(u64);

impl Seeded {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

fn tenant(name: &str) -> TenantId {
    TenantId::new(name).unwrap()
}

/// The same stream names under two tenants, including pairs that would collide under a key that
/// joined type and id with `","` (`a","b`/`c` against `a`/`b","c`) or with a space.
fn streams(tenants: &[TenantId]) -> Vec<StreamId> {
    let names = [
        ("item", "s0"),
        ("item", "s1"),
        ("a\",\"b", "c"),
        ("a", "b\",\"c"),
        ("x y", "z"),
        ("x", "y z"),
        ("q\\", "\"r"),
    ];
    tenants
        .iter()
        .flat_map(|tenant| {
            names
                .iter()
                .map(|(kind, id)| StreamId::new(tenant.clone(), *kind, *id).unwrap())
        })
        .collect()
}

/// What the tenant feed holds, regrouped by stream: the reference every per-stream read is
/// compared with. The feed walks events by global position and never consults the stream index.
async fn feed_by_stream(
    store: &FileEventStore,
    tenants: &[TenantId],
) -> BTreeMap<StreamId, Vec<RecordedEvent>> {
    let mut regrouped: BTreeMap<StreamId, Vec<RecordedEvent>> = BTreeMap::new();
    for tenant in tenants {
        let mut after = 0;
        loop {
            let page = store
                .read_feed(tenant, after, MAX_READ_LIMIT)
                .await
                .unwrap();
            for event in &page.events {
                regrouped
                    .entry(event.stream().unwrap())
                    .or_default()
                    .push(event.clone());
            }
            after = page.next_position;
            if !page.has_more {
                break;
            }
        }
    }
    regrouped
}

/// The window `read_stream(stream, after, limit)` must answer, derived from the feed alone.
fn expected_window(events: &[RecordedEvent], after: u64, limit: usize) -> StreamSlice {
    let limit = limit.clamp(1, MAX_READ_LIMIT);
    let rest: Vec<_> = events.iter().filter(|e| e.version > after).collect();
    let window: Vec<RecordedEvent> = rest.iter().take(limit).map(|e| (*e).clone()).collect();
    StreamSlice {
        next_version: window.last().map_or(after, |e| e.version),
        end_of_stream: rest.len() <= limit,
        events: window,
    }
}

const LIMITS: [usize; 5] = [0, 1, 2, 3, usize::MAX];

fn afters(head: u64) -> Vec<u64> {
    let mut afters = vec![0, 1, head.saturating_sub(1), head, head + 1, u64::MAX];
    afters.sort_unstable();
    afters.dedup();
    afters
}

/// Every stream read on `store` against the feed `reference`, plus the model's head.
async fn agrees(
    label: &str,
    store: &FileEventStore,
    reference: &BTreeMap<StreamId, Vec<RecordedEvent>>,
    all: &[StreamId],
    heads: &BTreeMap<StreamId, u64>,
) {
    let mut reads = Vec::new();
    let mut wanted = Vec::new();
    for stream in all {
        let events = reference.get(stream).cloned().unwrap_or_default();
        let head = heads.get(stream).copied().unwrap_or(0);
        assert_eq!(
            events.last().map_or(0, |e| e.version),
            head,
            "{label}: the feed's last version of {stream:?} is the model's head"
        );
        assert_eq!(
            store.stream_version(stream).await.unwrap(),
            (head > 0).then_some(head),
            "{label}: stream_version({stream:?})"
        );
        let whole = store.read_stream(stream, 0, MAX_READ_LIMIT).await.unwrap();
        assert_eq!(
            whole,
            expected_window(&events, 0, MAX_READ_LIMIT),
            "{label}: read_stream({stream:?}, 0, MAX)"
        );
        for after in afters(head) {
            for limit in LIMITS {
                reads.push(Read::Stream {
                    stream: stream.clone(),
                    after_version: after,
                    limit,
                });
                wanted.push((
                    format!("{label}: read_many window ({stream:?}, {after}, {limit})"),
                    expected_window(&events, after, limit),
                ));
            }
        }
    }
    let answered = store.read_many(&reads).await.unwrap();
    assert_eq!(answered.len(), wanted.len());
    for (answer, (what, expected)) in answered.into_iter().zip(wanted) {
        assert_eq!(answer, ReadResult::Stream(expected), "{what}");
    }
}

/// One recorded single-stream command, kept so it can be retried.
struct Sent {
    stream: StreamId,
    key: String,
    command: eventlog_core::CommandMeta,
    events: Vec<eventlog_core::NewEvent>,
    first: AppendResult,
}

/// One recorded group, kept so it can be retried.
struct SentGroup {
    group: AppendGroup,
    first: Vec<AppendResult>,
}

/// The receipt a retry returns, derived from the feed: the events of each range as they stand
/// now (a redaction since the original changes the body, never the identity).
fn receipt_from_feed(
    reference: &BTreeMap<StreamId, Vec<RecordedEvent>>,
    stream: &StreamId,
    original: &AppendResult,
) -> Vec<RecordedEvent> {
    reference
        .get(stream)
        .map(|events| {
            events
                .iter()
                .filter(|e| {
                    e.version >= original.first_version && e.version <= original.last_version
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

async fn check(
    label: &str,
    root: &Path,
    writer: &FileEventStore,
    other: &FileEventStore,
    tenants: &[TenantId],
    all: &[StreamId],
    heads: &BTreeMap<StreamId, u64>,
) {
    let fresh = FileEventStore::open(root).await.unwrap();
    let reference = feed_by_stream(&fresh, tenants).await;
    assert_eq!(
        feed_by_stream(writer, tenants).await,
        reference,
        "{label}: the writer's feed is the fresh opener's feed"
    );
    assert_eq!(
        feed_by_stream(other, tenants).await,
        reference,
        "{label}: the resumed handle's feed is the fresh opener's feed"
    );
    agrees(&format!("{label} fresh"), &fresh, &reference, all, heads).await;
    agrees(&format!("{label} writer"), writer, &reference, all, heads).await;
    agrees(&format!("{label} resumed"), other, &reference, all, heads).await;
}

/// The property: whatever sequence of appends, groups (including a group that repeats a stream),
/// retries, redactions and erasures two handles make, every per-stream read on either handle and
/// on a fresh opener is the feed regrouped by stream, every window boundary included, and every
/// retried receipt is the feed's events for the original range.
#[tokio::test(flavor = "multi_thread")]
async fn two_handles_and_a_fresh_opener_read_every_stream_as_the_feed_holds_it() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let tenants = [tenant("index-adv-a"), tenant("index-adv-b")];
    let all = streams(&tenants);
    let mut handles = [
        FileEventStore::open(root).await.unwrap(),
        FileEventStore::open(root).await.unwrap(),
    ];
    let mut heads: BTreeMap<StreamId, u64> = BTreeMap::new();
    let mut sent: Vec<Sent> = Vec::new();
    let mut groups: Vec<SentGroup> = Vec::new();
    let mut random = Seeded(0x00e1_42ad_0000_0001);
    let mut counter = 0_u64;
    // What the run actually exercised: a kind that never ran would leave this case green for
    // nothing, so the end of the case requires every one of them.
    let mut tally: BTreeMap<&str, u64> = BTreeMap::new();
    let mut last_writer = None;

    for step in 0..120_u64 {
        let by = usize::try_from(random.below(2)).unwrap();
        let roll = random.below(100);
        counter += 1;
        if last_writer.is_some_and(|last| last != by) {
            // This handle's cached view is about to be resumed onto the other handle's frames.
            *tally.entry("handover").or_default() += 1;
        }
        last_writer = Some(by);
        if roll < 45 {
            *tally.entry("single").or_default() += 1;
            // One append to one stream, with either expectation.
            let stream = all[usize::try_from(random.below(all.len() as u64)).unwrap()].clone();
            let head = heads.get(&stream).copied().unwrap_or(0);
            let expected = if random.below(2) == 0 {
                Expected::Any
            } else if head == 0 {
                Expected::NoStream
            } else {
                Expected::Exact(head)
            };
            let count = 1 + random.below(3);
            let events: Vec<_> = (0..count)
                .map(|n| event("item.changed", i64::try_from(counter * 10 + n).unwrap()))
                .collect();
            let key = format!("single-{counter}");
            let command = meta(&key, &json!({ "k": counter }));
            let result = handles[by]
                .append(&stream, expected, &events, &command)
                .await
                .unwrap();
            assert!(!result.deduplicated, "step {step}: a fresh key");
            assert_eq!(result.first_version, head + 1, "step {step}");
            assert_eq!(result.last_version, head + count, "step {step}");
            heads.insert(stream.clone(), head + count);
            sent.push(Sent {
                stream,
                key,
                command,
                events,
                first: result,
            });
        } else if roll < 65 {
            // A group in one tenant; one entry in three repeats an earlier entry's stream.
            let tenant = &tenants[usize::try_from(random.below(2)).unwrap()];
            let own: Vec<_> = all.iter().filter(|s| s.tenant() == tenant).collect();
            let mut appends = Vec::new();
            let mut planned: BTreeMap<StreamId, u64> = BTreeMap::new();
            for _ in 0..=random.below(3) {
                let stream = if !appends.is_empty() && random.below(3) == 0 {
                    *tally.entry("group repeating a stream").or_default() += 1;
                    let earlier: &StreamAppend = &appends[0];
                    earlier.stream.clone()
                } else {
                    own[usize::try_from(random.below(own.len() as u64)).unwrap()].clone()
                };
                let head = planned
                    .get(&stream)
                    .copied()
                    .unwrap_or_else(|| heads.get(&stream).copied().unwrap_or(0));
                let count = 1 + random.below(2);
                planned.insert(stream.clone(), head + count);
                appends.push(StreamAppend {
                    stream,
                    expected: if head == 0 {
                        Expected::NoStream
                    } else {
                        Expected::Exact(head)
                    },
                    events: (0..count)
                        .map(|n| event("item.grouped", i64::try_from(counter * 10 + n).unwrap()))
                        .collect(),
                });
            }
            let group = AppendGroup {
                tenant: tenant.clone(),
                appends,
                meta: meta(&format!("group-{counter}"), &json!({ "g": counter })),
            };
            let result = handles[by].append_group(&group).await.unwrap();
            assert!(!result.deduplicated, "step {step}: a fresh group");
            *tally.entry("group").or_default() += 1;
            heads.extend(planned);
            groups.push(SentGroup {
                group,
                first: result.appends,
            });
        } else if roll < 80 && !sent.is_empty() {
            // Retry a single command whose tenant has not been erased since.
            let index = usize::try_from(random.below(sent.len() as u64)).unwrap();
            let original = &sent[index];
            let retried = handles[by]
                .append(
                    &original.stream,
                    Expected::Any,
                    &original.events,
                    &original.command,
                )
                .await
                .unwrap();
            assert!(
                retried.deduplicated,
                "step {step}: retry of {}",
                original.key
            );
            let reference = feed_by_stream(&handles[by], &tenants).await;
            assert_eq!(
                retried.events,
                receipt_from_feed(&reference, &original.stream, &original.first),
                "step {step}: the receipt of {} is the feed's events for its range",
                original.key
            );
            assert_eq!(
                (retried.first_version, retried.last_version),
                (original.first.first_version, original.first.last_version),
                "step {step}"
            );
            *tally.entry("retry").or_default() += 1;
        } else if roll < 90 && !groups.is_empty() {
            let index = usize::try_from(random.below(groups.len() as u64)).unwrap();
            let original = &groups[index];
            let retried = handles[by].append_group(&original.group).await.unwrap();
            assert!(retried.deduplicated, "step {step}: group retry");
            let reference = feed_by_stream(&handles[by], &tenants).await;
            for ((entry, first), again) in original
                .group
                .appends
                .iter()
                .zip(&original.first)
                .zip(&retried.appends)
            {
                assert_eq!(
                    again.events,
                    receipt_from_feed(&reference, &entry.stream, first),
                    "step {step}: a group retry's receipt for {:?}",
                    entry.stream
                );
            }
            *tally.entry("group retry").or_default() += 1;
        } else if roll < 97 {
            // Redact one existing event; the other handle has to be reopened after a rewrite.
            let candidates: Vec<_> = heads.iter().filter(|(_, head)| **head > 0).collect();
            if candidates.is_empty() {
                continue;
            }
            let (stream, head) =
                candidates[usize::try_from(random.below(candidates.len() as u64)).unwrap()];
            let (stream, version) = (stream.clone(), 1 + random.below(*head));
            let redacted = handles[by]
                .redact(&stream, version, "privacy")
                .await
                .unwrap();
            assert_eq!(
                (redacted.stream().unwrap(), redacted.version),
                (stream.clone(), version),
                "step {step}: redact found the event it was asked for"
            );
            *tally.entry("redaction").or_default() += 1;
            handles[1 - by] = FileEventStore::open(root).await.unwrap();
        } else {
            // Erase one tenant, then go on writing the same stream names under it.
            let tenant = tenants[usize::try_from(random.below(2)).unwrap()].clone();
            handles[by].forget_tenant(&tenant).await.unwrap();
            heads.retain(|stream, _| stream.tenant() != &tenant);
            sent.retain(|s| s.stream.tenant() != &tenant);
            groups.retain(|g| g.group.tenant != tenant);
            *tally.entry("erasure").or_default() += 1;
            handles[1 - by] = FileEventStore::open(root).await.unwrap();
        }
        if step % 15 == 14 {
            *tally.entry("check").or_default() += 1;
            check(
                &format!("step {step}"),
                root,
                &handles[by],
                &handles[1 - by],
                &tenants,
                &all,
                &heads,
            )
            .await;
        }
    }
    check(
        "end",
        root,
        &handles[0],
        &handles[1],
        &tenants,
        &all,
        &heads,
    )
    .await;
    for kind in [
        "single",
        "group",
        "group repeating a stream",
        "retry",
        "group retry",
        "redaction",
        "erasure",
        "handover",
        "check",
    ] {
        assert!(
            tally.get(kind).copied().unwrap_or_default() > 0,
            "the run never exercised {kind}: {tally:?}"
        );
    }
    assert!(
        heads.values().filter(|head| **head > 0).count() >= 4,
        "the run ends with streams to read: {heads:?}"
    );
    eprintln!("exercised {tally:?}");
}

/// The window boundaries as literals, on one stream of five events with a twin of the same type
/// and id in another tenant and a collision-prone neighbour in the same tenant.
#[tokio::test(flavor = "multi_thread")]
async fn every_window_boundary_answers_with_literal_versions() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let owner = tenant("index-adv-literal");
    let stream = StreamId::new(owner.clone(), "a\",\"b", "c").unwrap();
    let neighbour = StreamId::new(owner.clone(), "a", "b\",\"c").unwrap();
    let twin = StreamId::new(tenant("index-adv-twin"), "a\",\"b", "c").unwrap();
    for (index, target) in [&stream, &neighbour, &twin, &stream, &stream, &neighbour]
        .into_iter()
        .chain([&stream, &stream])
        .enumerate()
    {
        store
            .append(
                target,
                Expected::Any,
                &[event("item.changed", i64::try_from(index).unwrap())],
                &meta(&format!("literal-{index}"), &json!({ "i": index })),
            )
            .await
            .unwrap();
    }
    assert_eq!(store.stream_version(&stream).await.unwrap(), Some(5));
    assert_eq!(store.stream_version(&neighbour).await.unwrap(), Some(2));
    assert_eq!(store.stream_version(&twin).await.unwrap(), Some(1));
    assert_eq!(
        store
            .stream_version(&StreamId::new(owner.clone(), "a", "c").unwrap())
            .await
            .unwrap(),
        None
    );

    let window = |after: u64, limit: usize| {
        let store = &store;
        let stream = &stream;
        async move {
            let slice = store.read_stream(stream, after, limit).await.unwrap();
            assert!(
                slice.events.iter().all(|e| e.stream().unwrap() == *stream),
                "read_stream({after}, {limit}) returned another stream's event"
            );
            (
                slice.events.iter().map(|e| e.version).collect::<Vec<_>>(),
                slice.next_version,
                slice.end_of_stream,
            )
        }
    };
    assert_eq!(window(0, 0).await, (vec![1], 1, false));
    assert_eq!(window(0, 1).await, (vec![1], 1, false));
    assert_eq!(window(0, 5).await, (vec![1, 2, 3, 4, 5], 5, true));
    assert_eq!(window(0, usize::MAX).await, (vec![1, 2, 3, 4, 5], 5, true));
    assert_eq!(window(3, 1).await, (vec![4], 4, false));
    assert_eq!(window(4, 1).await, (vec![5], 5, true));
    assert_eq!(window(5, 1).await, (vec![], 5, true));
    assert_eq!(window(6, 3).await, (vec![], 6, true));
    assert_eq!(window(u64::MAX, 0).await, (vec![], u64::MAX, true));
    assert_eq!(window(u64::MAX, usize::MAX).await, (vec![], u64::MAX, true));
}

/// A retried command's receipt is exactly its own range, after its stream has gained later events
/// (through a single append and through a group that repeats the stream) and after another tenant
/// holding the same type and id has been erased and written again.
#[tokio::test(flavor = "multi_thread")]
async fn a_retried_receipt_is_its_own_range_after_the_stream_moves_on() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let owner = tenant("index-adv-receipt");
    let other = tenant("index-adv-receipt-twin");
    let stream = StreamId::new(owner.clone(), "item", "r").unwrap();
    let twin = StreamId::new(other.clone(), "item", "r").unwrap();
    let events = [event("item.changed", 1), event("item.changed", 2)];
    let command = meta("receipt", &json!({ "r": 1 }));

    store
        .append(
            &stream,
            Expected::NoStream,
            &[event("item.created", 0)],
            &meta("first", &json!({})),
        )
        .await
        .unwrap();
    let original = store
        .append(&stream, Expected::Exact(1), &events, &command)
        .await
        .unwrap();
    assert_eq!((original.first_version, original.last_version), (2, 3));
    store
        .append(
            &twin,
            Expected::NoStream,
            &events,
            &meta("twin", &json!({})),
        )
        .await
        .unwrap();
    store
        .append(
            &stream,
            Expected::Exact(3),
            &[event("item.changed", 4)],
            &meta("later", &json!({})),
        )
        .await
        .unwrap();
    let group = AppendGroup {
        tenant: owner.clone(),
        appends: vec![
            StreamAppend {
                stream: stream.clone(),
                expected: Expected::Exact(4),
                events: vec![event("item.grouped", 5)],
            },
            StreamAppend {
                stream: stream.clone(),
                expected: Expected::Exact(5),
                events: vec![event("item.grouped", 6), event("item.grouped", 7)],
            },
        ],
        meta: meta("repeat", &json!({ "g": 1 })),
    };
    let grouped = store.append_group(&group).await.unwrap();
    assert_eq!(
        grouped
            .appends
            .iter()
            .map(|r| (r.first_version, r.last_version))
            .collect::<Vec<_>>(),
        vec![(5, 5), (6, 7)]
    );
    store.forget_tenant(&other).await.unwrap();
    store
        .append(
            &twin,
            Expected::NoStream,
            &events,
            &meta("twin-again", &json!({})),
        )
        .await
        .unwrap();

    let reopened = FileEventStore::open(directory.path()).await.unwrap();
    for handle in [&store, &reopened] {
        let retried = handle
            .append(&stream, Expected::Any, &events, &command)
            .await
            .unwrap();
        assert!(retried.deduplicated);
        assert_eq!(
            retried,
            AppendResult {
                deduplicated: true,
                ..original.clone()
            }
        );
        let again = handle.append_group(&group).await.unwrap();
        assert!(again.deduplicated);
        assert_eq!(
            again.appends,
            grouped
                .appends
                .iter()
                .cloned()
                .map(|r| AppendResult {
                    deduplicated: true,
                    ..r
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(
            handle
                .read_stream(&stream, 0, 100)
                .await
                .unwrap()
                .events
                .iter()
                .map(|e| e.version)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6, 7]
        );
        assert_eq!(handle.stream_version(&twin).await.unwrap(), Some(2));
    }
}
