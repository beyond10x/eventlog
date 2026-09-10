//! Independent expectations for ordered groups; both real SQL providers run these assertions.
use crate::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, EventLogError, Expected, Guard, ProjectionSpec,
    ProjectionStore, Projector, RecordedEvent, StreamAppend, StreamId, TenantId,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const VIEW: ProjectionSpec = ProjectionSpec {
    name: "group_probe",
    indexed: &[],
};

struct Probe;
impl Projector for Probe {
    fn name(&self) -> &'static str {
        "group_probe"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        &[VIEW]
    }
    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            if event.name == "group.fail" {
                return Err(EventLogError::GuardRefused {
                    code: "projector_rejected".into(),
                });
            }
            if store.get_blob("content").await? != Some(b"local-content".to_vec()) {
                return Err(EventLogError::GuardRefused {
                    code: "wrong_blob_scope".into(),
                });
            }
            store
                .upsert(
                    &VIEW,
                    &event.tenant,
                    &event.stream_id,
                    &json!({"version":event.version}),
                )
                .await
        })
    }
}
struct Admission {
    tenant: TenantId,
    calls: Arc<AtomicUsize>,
    refuse: bool,
}
impl Guard for Admission {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let old = store
                .get_for_update(&VIEW, &self.tenant, "admissions")
                .await?
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            store
                .upsert(&VIEW, &self.tenant, "admissions", &json!(old + 1))
                .await?;
            if self.refuse {
                return Err(EventLogError::GuardRefused {
                    code: "admission_rejected".into(),
                });
            }
            Ok(())
        })
    }
}
fn entry(tenant: &TenantId, id: &str, expected: Expected, name: &str) -> StreamAppend {
    StreamAppend {
        stream: StreamId::new(tenant.clone(), "item", id).unwrap(),
        expected,
        events: vec![event(name, 1)],
    }
}

/// # Panics
/// Asserts atomicity, retries, actual-content matching, ordering, projection and blob boundaries.
pub async fn run_atomic_groups(store: &dyn AtomicEventStore) {
    let tenant = TenantId::new("group-tenant").unwrap();
    let foreign = TenantId::new("other-tenant").unwrap();
    store.register_inline(Arc::new(Probe)).await.unwrap();
    store
        .put_blob(&tenant, "content", b"local-content")
        .await
        .unwrap();
    store
        .put_blob(&foreign, "content", b"foreign-content")
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let admission = || {
        Arc::new(Admission {
            tenant: tenant.clone(),
            calls: calls.clone(),
            refuse: false,
        }) as Arc<dyn Guard>
    };
    let group = AppendGroup {
        tenant: tenant.clone(),
        meta: meta("ordered", &json!({})),
        appends: vec![
            entry(&tenant, "z", Expected::NoStream, "group.created"),
            entry(&tenant, "a", Expected::NoStream, "group.created"),
            entry(&tenant, "z", Expected::Exact(1), "group.updated"),
        ],
    };
    let result = store
        .append_group_guarded(&group, admission())
        .await
        .unwrap();
    assert!(!result.deduplicated);
    assert_eq!(
        result
            .appends
            .iter()
            .map(|a| (a.first_version, a.last_version))
            .collect::<Vec<_>>(),
        vec![(1, 1), (1, 1), (2, 2)]
    );
    let events = result
        .appends
        .iter()
        .flat_map(|a| a.events.iter())
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .map(|e| e.stream_id.as_str())
            .collect::<Vec<_>>(),
        vec!["z", "a", "z"]
    );
    assert!(events.windows(2).all(|e| e[0].global_seq < e[1].global_seq));
    let retry = store
        .append_group_guarded(&group, admission())
        .await
        .unwrap();
    assert!(retry.deduplicated);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "retry must not execute the guard"
    );
    for (original, retried) in result.appends.iter().zip(&retry.appends) {
        assert_eq!(original.events, retried.events);
        assert!(retried.deduplicated);
    }
    assert_eq!(
        store
            .projection_get(&VIEW, &tenant, "admissions")
            .await
            .unwrap(),
        Some(json!(1))
    );
    assert_eq!(
        store.projection_get(&VIEW, &tenant, "z").await.unwrap(),
        Some(json!({"version":2}))
    );
    assert_eq!(
        store
            .recorded_command(
                &group.appends[0].stream,
                &group.meta.idempotency_key,
                &group.meta.request_hash
            )
            .await
            .unwrap(),
        None,
        "group has no independent child command identity"
    );

    let mismatch = EventLogError::IdempotencyMismatch {
        key: "ordered".into(),
    };
    let mut changed = group.clone();
    changed.appends[0].events[0] = event("group.changed", 7);
    assert_eq!(store.append_group(&changed).await.unwrap_err(), mismatch);
    changed = group.clone();
    changed.appends.swap(0, 1);
    assert_eq!(store.append_group(&changed).await.unwrap_err(), mismatch);
    changed = group.clone();
    changed.appends[0].expected = Expected::Any;
    assert_eq!(store.append_group(&changed).await.unwrap_err(), mismatch);
    changed = group.clone();
    changed.meta.actor = "different-actor".into();
    assert_eq!(store.append_group(&changed).await.unwrap_err(), mismatch);

    let mut failed = AppendGroup {
        tenant: tenant.clone(),
        meta: meta("rollback", &json!({})),
        appends: vec![
            entry(&tenant, "new", Expected::NoStream, "group.created"),
            entry(&tenant, "z", Expected::Exact(1), "group.updated"),
        ],
    };
    assert_eq!(
        store
            .append_group_guarded(&failed, admission())
            .await
            .unwrap_err(),
        EventLogError::Conflict {
            expected: 1,
            actual: 2
        }
    );
    assert_eq!(
        store
            .read_stream(&failed.appends[0].stream, 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        0
    );
    assert_eq!(
        store.projection_get(&VIEW, &tenant, "new").await.unwrap(),
        None
    );
    assert_eq!(
        store
            .projection_get(&VIEW, &tenant, "admissions")
            .await
            .unwrap(),
        Some(json!(1))
    );
    // An aborted identity was not consumed; correcting it may now commit.
    failed.appends[1].expected = Expected::Exact(2);
    assert!(
        !store
            .append_group_guarded(&failed, admission())
            .await
            .unwrap()
            .deduplicated
    );

    let rejected = AppendGroup {
        tenant: tenant.clone(),
        meta: meta("projector-failure", &json!({})),
        appends: vec![
            entry(
                &tenant,
                "rollback-first",
                Expected::NoStream,
                "group.created",
            ),
            entry(&tenant, "rollback-second", Expected::NoStream, "group.fail"),
        ],
    };
    assert_eq!(
        store
            .append_group_guarded(&rejected, admission())
            .await
            .unwrap_err(),
        EventLogError::GuardRefused {
            code: "projector_rejected".into()
        }
    );
    assert_eq!(
        store
            .projection_get(&VIEW, &tenant, "rollback-first")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .projection_get(&VIEW, &tenant, "admissions")
            .await
            .unwrap(),
        Some(json!(2))
    );
    for entry in &rejected.appends {
        assert_eq!(
            store
                .read_stream(&entry.stream, 0, 100)
                .await
                .unwrap()
                .events
                .len(),
            0
        );
    }
    let rejecting = Arc::new(Admission {
        tenant: tenant.clone(),
        calls: calls.clone(),
        refuse: true,
    });
    assert_eq!(
        store
            .append_group_guarded(&rejected, rejecting)
            .await
            .unwrap_err(),
        EventLogError::GuardRefused {
            code: "admission_rejected".into()
        }
    );
    assert_eq!(
        store
            .projection_get(&VIEW, &tenant, "admissions")
            .await
            .unwrap(),
        Some(json!(2))
    );

    let mut invalid = group.clone();
    invalid.appends.clear();
    assert_eq!(
        store.append_group(&invalid).await.unwrap_err(),
        EventLogError::Invalid("an append group must contain entries".into())
    );
    invalid = group.clone();
    invalid.appends.push(entry(
        &foreign,
        "foreign",
        Expected::NoStream,
        "group.created",
    ));
    assert_eq!(
        store.append_group(&invalid).await.unwrap_err(),
        EventLogError::Invalid("append group crosses tenants".into())
    );

    // Reusing one key in another tenant neither matches nor conflicts with this tenant.
    let foreign = TenantId::new("third-tenant").unwrap();
    store
        .put_blob(&foreign, "content", b"local-content")
        .await
        .unwrap();
    let other = AppendGroup {
        tenant: foreign.clone(),
        meta: group.meta.clone(),
        appends: vec![entry(
            &foreign,
            "other",
            Expected::NoStream,
            "group.created",
        )],
    };
    assert!(!store.append_group(&other).await.unwrap().deduplicated);

    // Receipts reference current history; they must not retain a second unredacted event body.
    let redacted = store
        .redact(&group.appends[0].stream, 1, "erased")
        .await
        .unwrap();
    let retry = store.append_group(&group).await.unwrap();
    assert!(retry.deduplicated);
    assert_eq!(retry.appends[0].events, vec![redacted]);
    store.forget_tenant(&tenant).await.unwrap();
    assert!(
        store.append_group(&other).await.unwrap().deduplicated,
        "other tenant survives"
    );
    store
        .put_blob(&tenant, "content", b"local-content")
        .await
        .unwrap();
    let fresh = store.append_group(&group).await.unwrap();
    assert!(
        !fresh.deduplicated,
        "tenant erasure also removes group identity bookkeeping"
    );
    assert_ne!(
        fresh.appends[0].events[0].event_id,
        result.appends[0].events[0].event_id
    );
}
