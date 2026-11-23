//! A restored append that rolls back must leave the restored queue as it found it: the append
//! takes identities as it inserts, before it knows whether it will commit.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, EventLogError, EventStore, Expected, ProjectionSpec,
    ProjectionStore, Projector, RecordedEvent, StreamAppend, StreamId, TenantId,
};
use eventlog_sqlite::{EventOrigin, RestoredEvent, SqliteEventStore};
use serde_json::json;
use std::sync::Arc;
use time::OffsetDateTime;

/// An inline projector that refuses every event, after the append has inserted it.
struct Refusing;

impl Projector for Refusing {
    fn name(&self) -> &'static str {
        "refusing"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        &[]
    }

    fn apply<'a>(
        &'a self,
        _event: &'a RecordedEvent,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Err(EventLogError::Backend("the read model refused".into())) })
    }
}

fn restored(id: &str, version: u64) -> RestoredEvent {
    RestoredEvent {
        event_id: id.to_owned(),
        recorded_at: OffsetDateTime::UNIX_EPOCH,
        origin: Some(EventOrigin {
            version,
            digest: format!("digest-{version}"),
            parents: Vec::new(),
        }),
    }
}

fn stream(id: &str) -> StreamId {
    StreamId::new(TenantId::new("tenant-a").unwrap(), "item", id).unwrap()
}

async fn refusing_store() -> SqliteEventStore {
    let store = SqliteEventStore::in_memory("owner").await.unwrap();
    store.register_inline(Arc::new(Refusing)).await.unwrap();
    store
        .restore_events(vec![restored("copied-1", 1), restored("copied-2", 2)])
        .unwrap();
    store
}

#[tokio::test(flavor = "multi_thread")]
async fn an_append_a_projector_refuses_puts_back_every_restored_identity_it_took() {
    let store = refusing_store().await;
    let refused = store
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1), event("item.indexed", 2)],
            &meta("key-1", &json!({})),
        )
        .await
        .expect_err("the refusing projector did not refuse");
    assert!(matches!(refused, EventLogError::Backend(_)), "{refused:?}");
    assert_eq!(
        store.restored_pending().unwrap(),
        2,
        "a rolled-back append kept the restored identities it took"
    );
    assert!(
        store
            .origins(&TenantId::new("tenant-a").unwrap())
            .await
            .unwrap()
            .is_empty(),
        "a rolled-back append left an origin"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_a_projector_refuses_puts_back_every_restored_identity_it_took() {
    let store = refusing_store().await;
    let group = AppendGroup {
        tenant: TenantId::new("tenant-a").unwrap(),
        appends: ["left", "right"]
            .into_iter()
            .map(|id| StreamAppend {
                stream: stream(id),
                expected: Expected::NoStream,
                events: vec![event("item.received", 1)],
            })
            .collect(),
        meta: meta("group-1", &json!({})),
    };
    store
        .append_group(&group)
        .await
        .expect_err("the refusing projector did not refuse");
    assert_eq!(
        store.restored_pending().unwrap(),
        2,
        "a rolled-back group kept the restored identities it took"
    );
}

/// A store rebuilt while its reader is open keeps the positions it served: restored events take
/// the positions they are given, out of replay order and with gaps, and once positions are
/// reserved through an erased event's, the next append lands above it.
#[tokio::test(flavor = "multi_thread")]
async fn restored_events_take_their_given_positions_and_appends_land_above_the_reserved_ones() {
    let store = SqliteEventStore::in_memory("owner").await.unwrap();
    assert!(matches!(
        store.restore_events_at(vec![restored("short", 1)], vec![]),
        Err(EventLogError::Invalid(_))
    ));
    assert!(matches!(
        store.restore_events_at(vec![restored("zero", 1)], vec![0]),
        Err(EventLogError::Invalid(_))
    ));
    assert_eq!(store.restored_pending().unwrap(), 0);

    store
        .restore_events_at(vec![restored("later", 1)], vec![7])
        .unwrap();
    store
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("key-1", &json!({})),
        )
        .await
        .unwrap();
    store
        .restore_events_at(vec![restored("earlier", 1)], vec![3])
        .unwrap();
    store
        .append(
            &stream("y"),
            Expected::NoStream,
            &[event("item.received", 2)],
            &meta("key-2", &json!({})),
        )
        .await
        .unwrap();
    let positions = store.positions().await.unwrap();
    assert_eq!(positions.by_event["later"], 7);
    assert_eq!(positions.by_event["earlier"], 3);
    assert_eq!(positions.high_water, 7);
    let feed = store
        .read_feed(&TenantId::new("tenant-a").unwrap(), 0, 10)
        .await
        .unwrap();
    let order: Vec<(u64, &str)> = feed
        .events
        .iter()
        .map(|event| (event.global_seq, event.event_id.as_str()))
        .collect();
    assert_eq!(order, [(3, "earlier"), (7, "later")]);

    store.reserve_positions_through(12).await.unwrap();
    assert_eq!(store.positions().await.unwrap().high_water, 12);
    let next = store
        .append(
            &stream("z"),
            Expected::NoStream,
            &[event("item.received", 3)],
            &meta("key-3", &json!({})),
        )
        .await
        .unwrap();
    assert_eq!(next.events[0].global_seq, 13);
    store.reserve_positions_through(5).await.unwrap();
    assert_eq!(
        store.positions().await.unwrap().high_water,
        13,
        "a reservation never lowers what was assigned"
    );
}
