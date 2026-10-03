//! Independently authored probes of capture continuity and physical projection scope.
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, CaptureCheckpoint, CaptureLimits,
    ConsistentTenantCapture, EventLogError, EventStore, Expected, NewEvent, ProjectionSpec,
    ProjectionStore, Projector, RecordedEvent, StreamAppend, StreamId, TenantCaptureUpdate,
    TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use serde_json::json;
use std::sync::Arc;

const DECLARED: ProjectionSpec = ProjectionSpec {
    name: "review_rows",
    indexed: &["kind"],
};
const ALIAS: ProjectionSpec = ProjectionSpec {
    name: "review_rows",
    indexed: &["other"],
};
fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 100,
        max_blobs: 100,
        max_projection_rows: 100,
        max_payload_bytes: 1 << 20,
    }
}
fn tenant() -> TenantId {
    TenantId::new("review").unwrap()
}
fn group(key: &str, alias: bool) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: eventlog_conformance::meta(key, &json!({"alias":alias})),
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant(), "item", "one").unwrap(),
            expected: Expected::Any,
            events: vec![NewEvent::new("item.changed", 1, json!({"alias":alias})).unwrap()],
        }],
    }
}
struct AliasWriter;
impl Projector for AliasWriter {
    fn name(&self) -> &'static str {
        "review_rows"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        &[DECLARED]
    }
    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let alias = event.data["alias"].as_bool().unwrap();
            let spec = if alias { &ALIAS } else { &DECLARED };
            if event.data["delete"] == json!(true) {
                return store.delete(spec, &event.tenant, "row").await;
            }
            store
                .upsert(
                    spec,
                    &event.tenant,
                    "row",
                    &json!({"kind":"a", "other":"b", "value":if alias {2} else {1}}),
                )
                .await
        })
    }
}

#[tokio::test]
async fn deleting_through_a_projection_alias_is_also_observed() {
    let store = SqliteEventStore::in_memory("capture_review").await.unwrap();
    store.register_inline(Arc::new(AliasWriter)).await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    store.append_group(&group("first", false)).await.unwrap();
    let previous = checkpoint(&store).await;
    let mut remove = group("remove", true);
    remove.appends[0].events[0] =
        NewEvent::new("item.changed", 1, json!({"alias":true,"delete":true})).unwrap();
    store.append_group(&remove).await.unwrap();
    let full = store
        .capture_tenant(&tenant(), &[DECLARED], limits())
        .await
        .unwrap();
    assert_eq!(
        full.projections[0].rows,
        [] as [(String, serde_json::Value); 0]
    );
    match store
        .capture_tenant_since(&tenant(), &[DECLARED], limits(), Some(&previous))
        .await
        .unwrap()
    {
        TenantCaptureUpdate::Complete { capture, .. } => assert_eq!(capture, full),
        TenantCaptureUpdate::AppendDelta { delta, .. } => {
            assert_eq!(delta.projections[0].rows.len(), 1);
            assert_eq!(
                delta.projections[0].rows[0].before.as_ref().unwrap()["value"],
                json!(1)
            );
            assert_eq!(delta.projections[0].rows[0].after, None);
            assert_eq!(delta.resulting_usage.projection_rows, 0);
        }
        TenantCaptureUpdate::Unchanged { .. } => panic!("committed deletion was omitted"),
    }
}

#[tokio::test]
async fn interleaved_tenant_journals_never_supply_another_tenants_delta() {
    let store = SqliteEventStore::in_memory("capture_review").await.unwrap();
    store.register_inline(Arc::new(AliasWriter)).await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    let other = TenantId::new("other").unwrap();
    store.stream_identity(&other).await.unwrap();
    store.append_group(&group("first", false)).await.unwrap();
    let previous = checkpoint(&store).await;
    let mut foreign = group("foreign", false);
    foreign.tenant = other.clone();
    foreign.appends[0].stream = StreamId::new(other, "item", "one").unwrap();
    store.append_group(&foreign).await.unwrap();
    store.append_group(&group("second", false)).await.unwrap();
    let full = store
        .capture_tenant(&tenant(), &[DECLARED], limits())
        .await
        .unwrap();
    assert_eq!(full.events.len(), 2);
    assert_eq!(full.projections[0].rows.len(), 1);
    let TenantCaptureUpdate::Complete { capture, .. } = store
        .capture_tenant_since(&tenant(), &[DECLARED], limits(), Some(&previous))
        .await
        .unwrap()
    else {
        panic!("mixed-tenant journal must fail closed to complete capture");
    };
    assert_eq!(capture, full);
    assert!(capture.events.iter().all(|event| event.tenant == tenant()));
}

#[tokio::test]
async fn wrapping_an_issued_checkpoint_cannot_create_new_provider_authority() {
    let store = SqliteEventStore::in_memory("capture_review").await.unwrap();
    store.register_inline(Arc::new(AliasWriter)).await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    store.append_group(&group("first", false)).await.unwrap();
    let previous = checkpoint(&store).await;
    assert!(matches!(
        store
            .capture_tenant_since(&tenant(), &[DECLARED], limits(), Some(&previous.clone()))
            .await
            .unwrap(),
        TenantCaptureUpdate::Unchanged { .. }
    ));
    let forged = CaptureCheckpoint::new(previous);
    let TenantCaptureUpdate::Complete {
        capture,
        checkpoint: Some(_),
    } = store
        .capture_tenant_since(&tenant(), &[DECLARED], limits(), Some(&forged))
        .await
        .unwrap()
    else {
        panic!("public wrapper must not manufacture the private SQLite payload");
    };
    assert_eq!(capture.events.len(), 1);
    assert_eq!(capture.projections[0].rows[0].1["value"], json!(1));
}
async fn checkpoint(store: &SqliteEventStore) -> CaptureCheckpoint {
    match store
        .capture_tenant_since(&tenant(), &[DECLARED], limits(), None)
        .await
        .unwrap()
    {
        TenantCaptureUpdate::Complete {
            checkpoint: Some(checkpoint),
            ..
        } => checkpoint,
        other => panic!("initial capture must issue authority: {other:?}"),
    }
}

#[tokio::test]
async fn an_acknowledged_projection_alias_cannot_disappear_from_the_requested_delta() {
    let store = SqliteEventStore::in_memory("capture_review").await.unwrap();
    store.register_inline(Arc::new(AliasWriter)).await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    store.append_group(&group("first", false)).await.unwrap();
    let previous = checkpoint(&store).await;
    store
        .append_group(&group("second", true))
        .await
        .expect("public projector alias is acknowledged");
    let full = store
        .capture_tenant(&tenant(), &[DECLARED], limits())
        .await
        .unwrap();
    assert_eq!(full.projections[0].rows[0].1["value"], json!(2));
    match store
        .capture_tenant_since(&tenant(), &[DECLARED], limits(), Some(&previous))
        .await
        .unwrap()
    {
        TenantCaptureUpdate::Complete { capture, .. } => assert_eq!(capture, full),
        TenantCaptureUpdate::AppendDelta { delta, .. } => {
            assert_eq!(delta.events.len(), 1);
            assert_eq!(delta.projections.len(), 1);
            assert_eq!(
                delta.projections[0].rows.len(),
                1,
                "acknowledged write to the same physical projection must not disappear"
            );
            let row = &delta.projections[0].rows[0];
            assert_eq!(row.key, "row");
            assert_eq!(row.before.as_ref().unwrap()["value"], json!(1));
            assert_eq!(row.after.as_ref().unwrap()["value"], json!(2));
        }
        TenantCaptureUpdate::Unchanged { .. } => panic!("committed projection was changed"),
    }
}
