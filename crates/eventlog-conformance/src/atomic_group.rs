//! Independent expectations for ordered groups; both real SQL providers run these assertions.
use crate::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, EventLogError, Expected, Guard, NoGuard,
    ProjectionSpec, ProjectionStore, Projector, RecordedEvent, StreamAppend, StreamId, TenantId,
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

/// No digest of `digests` is readable from `store`.
async fn assert_unpublished(
    store: &dyn AtomicEventStore,
    tenant: &TenantId,
    digests: &[(String, Vec<u8>)],
) {
    for (digest, _) in digests {
        assert_eq!(
            store.get_blob(tenant, digest).await.unwrap(),
            None,
            "{digest}: a refused guarded group publishes no blob of its batch"
        );
    }
}

/// A guard that refuses every command it is asked about, and counts what it was asked.
struct RefusingAdmission {
    calls: Arc<AtomicUsize>,
}
impl Guard for RefusingAdmission {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(EventLogError::GuardRefused {
                code: "conformance_refused".into(),
            })
        })
    }
}

/// `AtomicEventStore::append_group_guarded_with_blobs`, on every provider.
///
/// The method is on the port because the caller it exists for holds a trait object and cannot
/// name the provider underneath it. That caller can only rely on what the port promises, so the
/// promise has to hold on all three providers — and the two defects this exercise exists to
/// prevent both went green at every gate step of two earlier rounds precisely because the method
/// had three implementations and no cross-provider exercise.
///
/// **There are two contracts here and the provider says which one applies.** A provider that has
/// not implemented the single-barrier form refuses with [`eventlog_core::UNAVAILABLE`], writes
/// nothing and commits nothing; a provider that has implemented it publishes nothing when the
/// guard refuses, and deduplicates a retry against the batch the commit recorded rather than
/// against whichever blobs happen to be bound at the time. This exercise asks the store which one
/// it is, by the only means a caller has — calling the method — and then holds it to that
/// contract. Both answers are contracts; neither is an excuse.
///
/// **Returns which contract it exercised, and the caller asserts the one it expects.** Without
/// that this exercise would pass a provider that quietly lost its implementation: the refusal
/// branch is a valid contract, so "it refused everything" is indistinguishable from "it has no
/// override any more" unless somebody who knows the provider says which it should be.
///
/// # Panics
/// Asserts the refusing contract — nothing written, nothing committed, the same refusal every
/// time — or the implementing one: a refused guard publishes no blob and appends no member, an
/// admitted commit binds its whole batch, a retry carrying the recorded batch deduplicates, a
/// retry carrying another batch refuses without publishing, and a retry after one of the group's
/// blobs was deleted still deduplicates.
#[must_use]
pub async fn run_guarded_group_blobs(store: &dyn AtomicEventStore) -> bool {
    let tenant = TenantId::new("guarded-group-blobs").unwrap();
    let member = |key: &str, index: usize| StreamAppend {
        stream: StreamId::new(tenant.clone(), "item", format!("{key}-{index}")).unwrap(),
        expected: Expected::Any,
        events: vec![event("item.changed", 1)],
    };
    let group = |key: &str, count: usize| AppendGroup {
        tenant: tenant.clone(),
        meta: meta(key, &json!({})),
        appends: (0..count).map(|index| member(key, index)).collect(),
    };
    let batch = |first: usize, count: usize| {
        (first..first + count)
            .map(|index| {
                (
                    format!("g{index:04}"),
                    format!("guarded-{index}").into_bytes(),
                )
            })
            .collect::<Vec<_>>()
    };
    // Which contract applies. A provider that implements the method commits this probe; one that
    // does not refuses it, and must have published nothing while refusing.
    let probe = store
        .append_group_guarded_with_blobs(&group("probe", 1), Arc::new(NoGuard), &batch(0, 1))
        .await;
    let implemented = match probe {
        Ok(result) => {
            assert!(!result.deduplicated, "the probe is the first commit");
            true
        }
        Err(EventLogError::Invalid(ref message)) if message == eventlog_core::UNAVAILABLE => false,
        Err(other) => panic!("neither contract: {other:?}"),
    };

    if !implemented {
        assert_unpublished(store, &tenant, &batch(0, 1)).await;
        assert_eq!(
            store
                .stream_version(&member("probe", 0).stream)
                .await
                .unwrap(),
            None,
            "a provider that refuses the method commits no member of the group either"
        );
        // The refusal is the whole contract for this provider, and it does not depend on the
        // guard: nothing is written, so there is nothing for a guard to protect.
        let calls = Arc::new(AtomicUsize::new(0));
        let again = store
            .append_group_guarded_with_blobs(
                &group("probe", 1),
                Arc::new(RefusingAdmission {
                    calls: calls.clone(),
                }),
                &batch(0, 1),
            )
            .await
            .expect_err("the refusal is stable");
        assert!(
            matches!(again, EventLogError::Invalid(ref message)
                if message == eventlog_core::UNAVAILABLE),
            "the same refusal every time: {again:?}"
        );
        assert_unpublished(store, &tenant, &batch(0, 1)).await;
        return false;
    }

    // A refused guard publishes neither the group nor a blob.
    let calls = Arc::new(AtomicUsize::new(0));
    let refused = store
        .append_group_guarded_with_blobs(
            &group("refused", 2),
            Arc::new(RefusingAdmission {
                calls: calls.clone(),
            }),
            &batch(10, 3),
        )
        .await
        .expect_err("the guard refuses");
    assert!(
        matches!(refused, EventLogError::GuardRefused { ref code } if code == "conformance_refused"),
        "the refusal is the guard's own: {refused:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "admission ran exactly once"
    );
    assert_unpublished(store, &tenant, &batch(10, 3)).await;
    for index in 0..2 {
        assert_eq!(
            store
                .stream_version(&member("refused", index).stream)
                .await
                .unwrap(),
            None,
            "a refused guard appends no member of the group"
        );
    }

    // An admitted commit binds every digest of its batch.
    let committed = store
        .append_group_guarded_with_blobs(&group("bound", 2), Arc::new(NoGuard), &batch(20, 2))
        .await
        .unwrap();
    assert!(!committed.deduplicated, "the first commit is not a retry");
    for (digest, bytes) in batch(20, 2) {
        assert_eq!(
            store.get_blob(&tenant, &digest).await.unwrap(),
            Some(bytes),
            "{digest}: an admitted guarded group binds every digest of its batch"
        );
    }

    // A retry carrying the batch the commit recorded deduplicates.
    let retry = store
        .append_group_guarded_with_blobs(&group("bound", 2), Arc::new(NoGuard), &batch(20, 2))
        .await
        .unwrap();
    assert!(retry.deduplicated, "the same request twice is one request");

    // A retry carrying a batch the commit did not is not that request, and publishes nothing.
    let other = store
        .append_group_guarded_with_blobs(&group("bound", 2), Arc::new(NoGuard), &batch(30, 1))
        .await
        .expect_err("a batch the commit never carried is a different request");
    assert!(
        matches!(other, EventLogError::IdempotencyMismatch { ref key } if key == "bound"),
        "the refusal names the key: {other:?}"
    );
    assert_unpublished(store, &tenant, &batch(30, 1)).await;

    // Deduplication is decided by what the commit recorded, not by what is bound now. Deleting a
    // blob the group bound is its own act: it must not turn every later retry of a committed
    // group into `IdempotencyMismatch`, whose only recovery is a new key — and a new key over the
    // same members appends every one of them a second time into an append-only log.
    store.delete_blob(&tenant, "g0020").await.unwrap();
    assert_eq!(
        store.get_blob(&tenant, "g0020").await.unwrap(),
        None,
        "the fixture only means something once the blob really is gone"
    );
    let after_deletion = store
        .append_group_guarded_with_blobs(&group("bound", 2), Arc::new(NoGuard), &batch(20, 2))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "a committed group is still itself after one of its blobs was deleted: {error:?}"
            )
        });
    assert!(
        after_deletion.deduplicated,
        "the same key, the same members and the same batch is the same request"
    );
    true
}
