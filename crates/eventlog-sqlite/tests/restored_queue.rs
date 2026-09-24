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
