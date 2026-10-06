//! Provider authority, append deltas and conservative fallback for warm captures.
use eventlog_core::{
    AppendGroup, AtomicEventStore, CaptureCheckpoint, CaptureError, CaptureLimits, CaptureMaterial,
    ConsistentTenantCapture, EventStore, Expected, NewEvent, StreamAppend, StreamId, TenantCapture,
    TenantCaptureUpdate, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use serde_json::json;
use std::sync::Arc;

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 4096,
        max_blobs: 4096,
        max_projection_rows: 4096,
        max_payload_bytes: 1 << 24,
    }
}
fn tenant() -> TenantId {
    TenantId::new("tracked").unwrap()
}
fn group(key: &str) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: eventlog_conformance::meta(key, &json!({})),
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant(), "item", "one").unwrap(),
            expected: Expected::Any,
            events: vec![NewEvent::new("item.changed", 1, json!({"key":"one","value":1})).unwrap()],
        }],
    }
}
async fn base(store: &SqliteEventStore) -> (TenantCapture, CaptureCheckpoint) {
    match store
        .capture_tenant_since(&tenant(), &[], limits(), None)
        .await
        .unwrap()
    {
        TenantCaptureUpdate::Complete {
            capture,
            checkpoint: Some(checkpoint),
        } => (capture, checkpoint),
        other => panic!("initial SQLite capture must issue continuity authority: {other:?}"),
    }
}
#[tokio::test]
async fn unchanged_capture_reuses_the_exact_provider_observation() {
    let store = SqliteEventStore::in_memory("tracked").await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    let (_, checkpoint) = base(&store).await;
    assert!(matches!(
        store
            .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
            .await
            .unwrap(),
        TenantCaptureUpdate::Unchanged { .. }
    ));
}
#[tokio::test]
async fn warm_capture_detects_external_blob_tampering_without_an_event_head_change() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = SqliteEventStore::open(path.to_str().unwrap(), "tracked")
        .await
        .unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    store.put_blob(&tenant(), "one", b"before").await.unwrap();
    let (_, checkpoint) = base(&store).await;
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE tracked_blobs SET bytes=?1 WHERE digest='one'",
            rusqlite::params![b"broken".as_slice()],
        )
        .unwrap();
    assert!(matches!(
        store
            .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
            .await,
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        })
    ));
}
#[tokio::test]
async fn acknowledged_groups_form_a_complete_delta_and_deduplication_adds_nothing() {
    let store = SqliteEventStore::in_memory("tracked").await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    let (_, checkpoint) = base(&store).await;
    store
        .append_group_guarded_with_blobs(
            &group("g1"),
            Arc::new(eventlog_core::NoGuard),
            &[("orphan".into(), b"content".to_vec())],
        )
        .await
        .unwrap();
    store.append_group(&group("g2")).await.unwrap();
    let next = match store
        .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
        .await
        .unwrap()
    {
        TenantCaptureUpdate::AppendDelta { checkpoint, delta } => {
            assert_eq!(delta.events.len(), 2);
            assert_eq!(delta.blobs.len(), 1);
            assert_eq!(delta.blobs[0].bytes, b"content");
            assert_eq!(delta.resulting_usage.events, 2);
            assert_eq!(delta.resulting_usage.blobs, 1);
            checkpoint
        }
        other => panic!("expected complete append delta: {other:?}"),
    };
    store.append_group(&group("g2")).await.unwrap();
    assert!(matches!(
        store
            .capture_tenant_since(&tenant(), &[], limits(), Some(&next))
            .await
            .unwrap(),
        TenantCaptureUpdate::Unchanged { .. }
    ));
}

const ROWS: eventlog_core::ProjectionSpec = eventlog_core::ProjectionSpec {
    name: "tracked_rows",
    indexed: &[],
};
struct Rows;
impl eventlog_core::Projector for Rows {
    fn name(&self) -> &'static str {
        "tracked_rows"
    }
    fn projections(&self) -> &'static [eventlog_core::ProjectionSpec] {
        &[ROWS]
    }
    fn apply<'a>(
        &'a self,
        event: &'a eventlog_core::RecordedEvent,
        store: &'a mut dyn eventlog_core::ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), eventlog_core::EventLogError>> {
        Box::pin(async move {
            let key = event.data["key"].as_str().unwrap();
            if event.data.get("remove").is_some() {
                store.delete(&ROWS, &event.tenant, key).await
            } else {
                store
                    .upsert(&ROWS, &event.tenant, key, &event.data["value"])
                    .await
            }
        })
    }
}
async fn captured(
    store: &SqliteEventStore,
    specs: &[eventlog_core::ProjectionSpec],
    caps: CaptureLimits,
) -> (TenantCapture, CaptureCheckpoint) {
    match store
        .capture_tenant_since(&tenant(), specs, caps, None)
        .await
        .unwrap()
    {
        TenantCaptureUpdate::Complete {
            capture,
            checkpoint: Some(checkpoint),
        } => (capture, checkpoint),
        other => panic!("expected checkpoint: {other:?}"),
    }
}
fn changed(key: &str, id: &str, value: &serde_json::Value) -> AppendGroup {
    let mut result = group(key);
    result.appends[0].events[0] =
        NewEvent::new("item.changed", 1, json!({"key":id,"value":value})).unwrap();
    result
}
#[tokio::test]
async fn projection_before_after_values_and_usage_reconstruct_the_complete_capture() {
    use eventlog_core::{CaptureBudget, CapturedProjection};
    let store = SqliteEventStore::in_memory("tracked").await.unwrap();
    store.register_inline(Arc::new(Rows)).await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    store
        .append_group(&changed("first", "row", &json!({"large":"original"})))
        .await
        .unwrap();
    let (mut initial, checkpoint) = captured(&store, &[ROWS], limits()).await;
    store
        .append_group(&changed("second", "row", &json!(null)))
        .await
        .unwrap();
    store
        .append_group(&changed("third", "new", &json!(123)))
        .await
        .unwrap();
    let mut remove = group("fourth");
    remove.appends[0].events[0] =
        NewEvent::new("item.changed", 1, json!({"key":"row","remove":true})).unwrap();
    store.append_group(&remove).await.unwrap();
    let TenantCaptureUpdate::AppendDelta { delta, .. } = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&checkpoint))
        .await
        .unwrap()
    else {
        panic!("expected delta")
    };
    assert_eq!(
        delta.projections[0].rows,
        vec![
            eventlog_core::CapturedRowChange {
                key: "new".into(),
                before: None,
                after: Some(json!(123))
            },
            eventlog_core::CapturedRowChange {
                key: "row".into(),
                before: Some(json!({"large":"original"})),
                after: None
            }
        ]
    );
    initial.events.extend(delta.events);
    initial.blobs.extend(delta.blobs);
    let mut rows: std::collections::BTreeMap<_, _> =
        initial.projections[0].rows.clone().into_iter().collect();
    for row in &delta.projections[0].rows {
        assert_eq!(rows.get(&row.key), row.before.as_ref());
        match &row.after {
            Some(value) => {
                rows.insert(row.key.clone(), value.clone());
            }
            None => {
                rows.remove(&row.key);
            }
        }
    }
    initial.projections = vec![CapturedProjection {
        specification: ROWS,
        rows: rows.into_iter().collect(),
    }];
    let full = store
        .capture_tenant(&tenant(), &[ROWS], limits())
        .await
        .unwrap();
    assert_eq!(initial, full);
    let mut budget = CaptureBudget::new(limits());
    for event in &full.events {
        budget.admit_event(event).unwrap();
    }
    for blob in &full.blobs {
        budget.admit_blob(blob.bytes.len() as u64).unwrap();
    }
    for projection in &full.projections {
        for (_, value) in &projection.rows {
            budget.admit_projection_row(value).unwrap();
        }
    }
    assert_eq!(delta.resulting_usage, budget.usage());
}
#[tokio::test]
async fn limits_apply_to_the_complete_result_and_changed_limits_require_full_capture() {
    use eventlog_core::CaptureResource;
    for resource in [
        CaptureResource::Events,
        CaptureResource::Blobs,
        CaptureResource::ProjectionRows,
        CaptureResource::PayloadBytes,
    ] {
        let store = SqliteEventStore::in_memory("tracked").await.unwrap();
        store.register_inline(Arc::new(Rows)).await.unwrap();
        store.stream_identity(&tenant()).await.unwrap();
        store
            .append_group_guarded_with_blobs(
                &changed("first", "one", &json!(1)),
                Arc::new(eventlog_core::NoGuard),
                &[("first".into(), vec![1])],
            )
            .await
            .unwrap();
        let full = store
            .capture_tenant(&tenant(), &[ROWS], limits())
            .await
            .unwrap();
        let mut exact = limits();
        match resource {
            CaptureResource::Events => exact.max_events = 1,
            CaptureResource::Blobs => exact.max_blobs = 1,
            CaptureResource::ProjectionRows => exact.max_projection_rows = 1,
            CaptureResource::PayloadBytes => {
                exact.max_payload_bytes =
                    full.blobs.iter().map(|b| b.bytes.len() as u64).sum::<u64>()
                        + full
                            .events
                            .iter()
                            .map(|e| serde_json::to_vec(&e.data).unwrap().len() as u64)
                            .sum::<u64>()
                        + full
                            .projections
                            .iter()
                            .flat_map(|p| &p.rows)
                            .map(|(_, v)| serde_json::to_vec(v).unwrap().len() as u64)
                            .sum::<u64>();
            }
        }
        let (_, checkpoint) = captured(&store, &[ROWS], exact).await;
        store
            .append_group_guarded_with_blobs(
                &changed("second", "two", &json!(2)),
                Arc::new(eventlog_core::NoGuard),
                &[("second".into(), vec![2])],
            )
            .await
            .unwrap();
        assert!(
            matches!(store.capture_tenant_since(&tenant(),&[ROWS],exact,Some(&checkpoint)).await,
            Err(CaptureError::LimitExceeded {resource:actual,..}) if actual==resource)
        );
        assert!(matches!(
            store
                .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&checkpoint))
                .await
                .unwrap(),
            TenantCaptureUpdate::Complete { .. }
        ));
    }
}
#[tokio::test]
async fn foreign_scope_unjournaled_writes_and_expired_journals_fall_back() {
    let store = SqliteEventStore::in_memory("tracked").await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    let (_, checkpoint) = base(&store).await;
    let foreign = CaptureCheckpoint::new(7_u64);
    assert!(matches!(
        store
            .capture_tenant_since(&tenant(), &[], limits(), Some(&foreign))
            .await
            .unwrap(),
        TenantCaptureUpdate::Complete { .. }
    ));
    let other = SqliteEventStore::in_memory("tracked").await.unwrap();
    other.stream_identity(&tenant()).await.unwrap();
    assert!(matches!(
        other
            .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
            .await
            .unwrap(),
        TenantCaptureUpdate::Complete { .. }
    ));
    store
        .put_blob(&tenant(), "standalone", b"new")
        .await
        .unwrap();
    assert!(matches!(
        store
            .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
            .await
            .unwrap(),
        TenantCaptureUpdate::Complete { .. }
    ));
    let (_, old) = base(&store).await;
    for i in 0..129 {
        store
            .append_group(&group(&format!("group-{i}")))
            .await
            .unwrap();
    }
    assert!(matches!(
        store
            .capture_tenant_since(&tenant(), &[], limits(), Some(&old))
            .await
            .unwrap(),
        TenantCaptureUpdate::Complete { .. }
    ));
}
#[tokio::test]
async fn external_same_head_projection_and_identity_changes_are_never_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = SqliteEventStore::open(path.to_str().unwrap(), "tracked")
        .await
        .unwrap();
    store.register_inline(Arc::new(Rows)).await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    store
        .append_group(&changed("first", "row", &json!(1)))
        .await
        .unwrap();
    let (_, checkpoint) = captured(&store, &[ROWS], limits()).await;
    let external = rusqlite::Connection::open(&path).unwrap();
    external
        .execute(
            "UPDATE tracked_p_tracked_rows SET body='2' WHERE row_key='row'",
            [],
        )
        .unwrap();
    let TenantCaptureUpdate::Complete {
        capture,
        checkpoint: Some(next),
    } = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&checkpoint))
        .await
        .unwrap()
    else {
        panic!("projection change was hidden")
    };
    assert_eq!(capture.projections[0].rows[0].1, json!(2));
    external
        .execute(
            "UPDATE tracked_identity SET stream_identity='changed' WHERE tenant_id='tracked'",
            [],
        )
        .unwrap();
    let TenantCaptureUpdate::Complete { capture, .. } = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&next))
        .await
        .unwrap()
    else {
        panic!("identity change was hidden")
    };
    assert_eq!(capture.stream_identity, "changed");
}
struct Reject;
impl eventlog_core::Guard for Reject {
    fn check<'a>(
        &'a self,
        _: &'a mut dyn eventlog_core::ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), eventlog_core::EventLogError>> {
        Box::pin(async {
            Err(eventlog_core::EventLogError::GuardRefused {
                code: "denied".into(),
            })
        })
    }
}
#[tokio::test]
async fn refused_blob_group_does_not_publish_a_partial_delta() {
    use eventlog_core::{AtomicBlobEventStore, BlobAppendGroup, BlobWrite, EventLogError};
    let store = SqliteEventStore::in_memory("tracked").await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    let (_, checkpoint) = base(&store).await;
    let request = BlobAppendGroup {
        group: group("refused"),
        blobs: vec![BlobWrite {
            digest: "uncommitted".into(),
            bytes: vec![1],
        }],
    };
    assert!(matches!(
        store
            .append_group_with_blobs_guarded(&request, Arc::new(Reject))
            .await,
        Err(EventLogError::GuardRefused { .. })
    ));
    let TenantCaptureUpdate::Complete { capture, .. } = store
        .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
        .await
        .unwrap()
    else {
        panic!("failed transaction must invalidate")
    };
    assert_eq!(capture.events, [] as [eventlog_core::RecordedEvent; 0]);
    assert_eq!(capture.blobs, [] as [eventlog_core::CapturedBlob; 0]);
}
#[tokio::test]
async fn the_atomic_blob_trait_records_its_whole_committed_group() {
    use eventlog_core::{AtomicBlobEventStore, BlobAppendGroup, BlobWrite};
    let store = SqliteEventStore::in_memory("tracked").await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    let (_, checkpoint) = base(&store).await;
    let request = BlobAppendGroup {
        group: group("accepted"),
        blobs: vec![BlobWrite {
            digest: "committed".into(),
            bytes: vec![1],
        }],
    };
    store
        .append_group_with_blobs_guarded(&request, Arc::new(eventlog_core::NoGuard))
        .await
        .unwrap();
    let TenantCaptureUpdate::AppendDelta { delta, .. } = store
        .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
        .await
        .unwrap()
    else {
        panic!("missing atomic blob delta")
    };
    assert_eq!(delta.events.len(), 1);
    assert_eq!(delta.blobs.len(), 1);
}
#[tokio::test]
async fn unsupported_provider_default_always_returns_a_complete_value_without_authority() {
    struct CompleteOnly(Arc<SqliteEventStore>);
    impl ConsistentTenantCapture for CompleteOnly {
        fn capture_tenant<'a>(
            &'a self,
            tenant: &'a TenantId,
            projections: &'a [eventlog_core::ProjectionSpec],
            limits: CaptureLimits,
        ) -> eventlog_core::BoxFuture<'a, Result<TenantCapture, CaptureError>> {
            self.0.capture_tenant(tenant, projections, limits)
        }
    }
    let store = Arc::new(SqliteEventStore::in_memory("tracked").await.unwrap());
    store.stream_identity(&tenant()).await.unwrap();
    let (_, checkpoint) = base(&store).await;
    let port = CompleteOnly(store);
    assert!(matches!(
        port.capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
            .await
            .unwrap(),
        TenantCaptureUpdate::Complete {
            checkpoint: None,
            ..
        }
    ));
}
#[tokio::test]
async fn preexisting_sql_triggers_disable_warm_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = SqliteEventStore::open(path.to_str().unwrap(), "tracked")
        .await
        .unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    rusqlite::Connection::open(&path).unwrap().execute_batch("CREATE TRIGGER surprise AFTER INSERT ON tracked_events BEGIN UPDATE tracked_events SET data='{}' WHERE global_seq<new.global_seq; END").unwrap();
    assert!(matches!(
        store
            .capture_tenant_since(&tenant(), &[], limits(), None)
            .await
            .unwrap(),
        TenantCaptureUpdate::Complete {
            checkpoint: None,
            ..
        }
    ));
}

#[tokio::test]
async fn repeated_blob_binding_and_old_checkpoint_reuse_keep_exact_totals() {
    let store = SqliteEventStore::in_memory("tracked").await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    store
        .put_blob(&tenant(), "orphan", b"content")
        .await
        .unwrap();
    let (_, checkpoint) = base(&store).await;
    for key in ["first", "second"] {
        store
            .append_group_guarded_with_blobs(
                &group(key),
                Arc::new(eventlog_core::NoGuard),
                &[("orphan".into(), b"content".to_vec())],
            )
            .await
            .unwrap();
    }
    for _ in 0..2 {
        let TenantCaptureUpdate::AppendDelta { delta, .. } = store
            .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
            .await
            .unwrap()
        else {
            panic!("expected repeatable delta")
        };
        assert_eq!(delta.events.len(), 2);
        assert_eq!(delta.blobs, [] as [eventlog_core::CapturedBlob; 0]);
        assert_eq!(delta.resulting_usage.blobs, 1);
        assert_eq!(delta.resulting_usage.events, 2);
    }
}

#[tokio::test]
async fn changed_request_scope_and_reopened_connection_require_complete_capture() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scope.db");
    let store = SqliteEventStore::open(path.to_str().unwrap(), "tracked")
        .await
        .unwrap();
    store.register_inline(Arc::new(Rows)).await.unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    let other = TenantId::new("other").unwrap();
    store.stream_identity(&other).await.unwrap();
    let (_, checkpoint) = base(&store).await;
    for (who, specs, caps) in [
        (tenant(), vec![ROWS], limits()),
        (other, vec![], limits()),
        (
            tenant(),
            vec![],
            CaptureLimits {
                max_events: 0,
                ..limits()
            },
        ),
    ] {
        assert!(matches!(
            store
                .capture_tenant_since(&who, &specs, caps, Some(&checkpoint))
                .await
                .unwrap(),
            TenantCaptureUpdate::Complete { .. }
        ));
    }
    drop(store);
    let reopened = SqliteEventStore::open(path.to_str().unwrap(), "tracked")
        .await
        .unwrap();
    assert!(matches!(
        reopened
            .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
            .await
            .unwrap(),
        TenantCaptureUpdate::Complete { .. }
    ));
}

#[tokio::test]
async fn external_prefix_edit_followed_by_own_append_cannot_rejoin_the_old_journal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prefix.db");
    let store = SqliteEventStore::open(path.to_str().unwrap(), "tracked")
        .await
        .unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    store.append_group(&group("first")).await.unwrap();
    let (_, checkpoint) = base(&store).await;
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("UPDATE tracked_events SET data='{}'", [])
        .unwrap();
    store.append_group(&group("second")).await.unwrap();
    let TenantCaptureUpdate::Complete { capture, .. } = store
        .capture_tenant_since(&tenant(), &[], limits(), Some(&checkpoint))
        .await
        .unwrap()
    else {
        panic!("external prefix edit was hidden")
    };
    assert_eq!(capture.events.len(), 2);
    assert_eq!(capture.events[0].data, json!({}));
}
