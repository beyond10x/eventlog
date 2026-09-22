//! The provider-independent contract for attaching and rebuilding an admitted inline projector.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use eventlog_core::{
    AdmissionPermit, BoxFuture, EventLogError, EventStore, Expected, InlineProjectionAdmin,
    NewEvent, ProjectionSpec, ProjectionStore, Projector, RecordedEvent, Reservation, StreamId,
    TenantId,
};
use serde_json::{Value, json};

use crate::meta;

pub const ADMIN_LEDGER: ProjectionSpec = ProjectionSpec {
    name: "admin_ledger",
    indexed: &["kind"],
};
pub const ADMIN_SIDECAR: ProjectionSpec = ProjectionSpec {
    name: "admin_sidecar",
    indexed: &[],
};
pub const ADMIN_SPARE: ProjectionSpec = ProjectionSpec {
    name: "admin_spare",
    indexed: &[],
};

/// A stateful fixture: rebuild has to execute this exact attached instance, not caller-supplied
/// code that merely repeats its name and declarations.
pub struct AdminProjector {
    pub generation: Arc<AtomicU64>,
    pub fail_after: Arc<AtomicU64>,
    pub use_foreign_table: Arc<AtomicBool>,
    pub use_reservation: Arc<AtomicBool>,
}

/// A replay fixture that can hold a projector callback while native registration coordination is
/// observed by a second operation.
pub struct CoordinatedProjector {
    armed: AtomicBool,
    entered: AtomicBool,
    released: AtomicBool,
    waker: Mutex<Option<std::task::Waker>>,
}

impl Default for CoordinatedProjector {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: AtomicBool::new(false),
            released: AtomicBool::new(false),
            waker: Mutex::new(None),
        }
    }
}

impl CoordinatedProjector {
    /// Hold the next callback until [`Self::release`] is called.
    pub fn arm(&self) {
        self.entered.store(false, Ordering::Release);
        self.released.store(false, Ordering::Release);
        self.armed.store(true, Ordering::Release);
    }

    /// Whether the armed callback reached the hold point.
    pub fn entered(&self) -> bool {
        self.entered.load(Ordering::Acquire)
    }

    /// Let an armed callback continue.
    pub fn release(&self) {
        self.released.store(true, Ordering::Release);
        if let Some(waker) = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            waker.wake();
        }
    }
}

impl Projector for CoordinatedProjector {
    fn name(&self) -> &'static str {
        "coordinated_admin"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&ADMIN_LEDGER)
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            if self.armed.swap(false, Ordering::AcqRel) {
                self.entered.store(true, Ordering::Release);
                std::future::poll_fn(|context| {
                    let mut waker = self
                        .waker
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if self.released.load(Ordering::Acquire) {
                        std::task::Poll::Ready(())
                    } else {
                        *waker = Some(context.waker().clone());
                        std::task::Poll::Pending
                    }
                })
                .await;
            }
            store
                .upsert(
                    &ADMIN_LEDGER,
                    &event.tenant,
                    &event.stream_id,
                    &json!({"position": event.global_seq, "kind": event.name}),
                )
                .await
        })
    }
}

impl Default for AdminProjector {
    fn default() -> Self {
        Self {
            generation: Arc::new(AtomicU64::new(1)),
            fail_after: Arc::new(AtomicU64::new(u64::MAX)),
            use_foreign_table: Arc::new(AtomicBool::new(false)),
            use_reservation: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Projector for AdminProjector {
    fn name(&self) -> &'static str {
        "admin_projector"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        &[ADMIN_LEDGER, ADMIN_SIDECAR]
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            if event.version >= self.fail_after.load(Ordering::Acquire) {
                return Err(EventLogError::Invalid("injected projector failure".into()));
            }
            if self.use_reservation.load(Ordering::Acquire) {
                store
                    .reserve(&AdmissionPermit::default(), &[] as &[Reservation])
                    .await?;
            }
            let key = event
                .data
                .get("key")
                .and_then(Value::as_str)
                .unwrap_or(event.stream_id.as_str());
            if let Some(digest) = event.data.get("blob").and_then(Value::as_str)
                && store.get_blob(digest).await?.is_none()
            {
                return Err(EventLogError::Invalid(
                    "required fixture blob is absent".into(),
                ));
            }
            let target = if self.use_foreign_table.load(Ordering::Acquire) {
                &ADMIN_SPARE
            } else {
                &ADMIN_LEDGER
            };
            let body = json!({
                "generation": self.generation.load(Ordering::Acquire),
                "kind": event.name,
                "position": event.global_seq,
            });
            store.upsert(target, &event.tenant, key, &body).await?;
            assert_eq!(
                store.get(target, &event.tenant, key).await?,
                Some(body.clone())
            );
            assert_eq!(
                store.get_for_update(target, &event.tenant, key).await?,
                Some(body.clone())
            );
            assert!(
                store
                    .find(target, &event.tenant, "kind", &event.name, 100)
                    .await?
                    .contains(&body)
            );
            store
                .upsert(
                    &ADMIN_SIDECAR,
                    &event.tenant,
                    key,
                    &json!({
                        "generation": self.generation.load(Ordering::Acquire),
                        "data": event.data,
                    }),
                )
                .await?;
            store
                .upsert(&ADMIN_SIDECAR, &event.tenant, "transient", &json!({}))
                .await?;
            store
                .delete(&ADMIN_SIDECAR, &event.tenant, "transient")
                .await
        })
    }
}

/// A second admitted projector used to test all-or-nothing attachment and coordination.
pub struct SpareProjector;
impl Projector for SpareProjector {
    fn name(&self) -> &'static str {
        "missing_admin_projector"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&ADMIN_SPARE)
    }
    fn apply<'a>(
        &'a self,
        _: &'a RecordedEvent,
        _: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Ok(()) })
    }
}

fn event(key: &str, value: u64, blob: Option<&str>) -> NewEvent {
    NewEvent::new(
        "item.recorded",
        1,
        json!({ "key": key, "value": value, "blob": blob }),
    )
    .expect("fixture event")
}

/// Run the common administration contract through the real provider APIs.
///
/// # Panics
///
/// Panics when a provider violates any administration contract assertion.
pub async fn run_inline_admin(store: &Arc<dyn EventStore>, admin: &dyn InlineProjectionAdmin) {
    let missing: Arc<dyn Projector> = Arc::new(SpareProjector);
    assert!(matches!(
        admin.attach_inline_existing(missing).await,
        Err(EventLogError::Invalid(_))
    ));

    let projector = Arc::new(AdminProjector::default());
    store
        .create_projections(projector.clone())
        .await
        .expect("admitted tables");
    store
        .create_projections(Arc::new(SpareProjector))
        .await
        .expect("admitted spare before serving freezes");
    admin
        .attach_inline_existing(projector.clone())
        .await
        .expect("attached admitted projector");
    assert!(store.is_inline(projector.name()).await);
    assert!(matches!(
        admin.attach_inline_existing(projector.clone()).await,
        Err(EventLogError::Invalid(_))
    ));

    let owner = TenantId::new("admin-owner").expect("tenant");
    let neighbour = TenantId::new("admin-neighbour").expect("tenant");
    store
        .put_blob(&owner, "active-blob", b"complete active bytes")
        .await
        .expect("active blob");
    let first = store
        .append(
            &StreamId::new(owner.clone(), "item", "one").expect("stream"),
            Expected::NoStream,
            &[event("one", 1, Some("active-blob")), event("two", 2, None)],
            &meta("admin-one", &json!({})),
        )
        .await
        .expect("owner history");
    store
        .append(
            &StreamId::new(neighbour.clone(), "item", "other").expect("stream"),
            Expected::NoStream,
            &[event("other", 9, None)],
            &meta("admin-other", &json!({})),
        )
        .await
        .expect("unrelated tenant history");
    let last = store
        .append(
            &StreamId::new(owner.clone(), "item", "three").expect("stream"),
            Expected::NoStream,
            &[event("three", 3, None)],
            &meta("admin-three", &json!({})),
        )
        .await
        .expect("sparse owner history");
    let expected_position = last.events[0].global_seq;
    assert!(expected_position > first.events[1].global_seq + 1);

    assert!(matches!(
        admin.attach_inline_existing(Arc::new(SpareProjector)).await,
        Err(EventLogError::Invalid(_))
    ));
    assert!(!store.is_inline("missing_admin_projector").await);
    assert!(matches!(
        admin.rebuild_inline_projection("absent", &owner).await,
        Err(EventLogError::Invalid(_))
    ));

    projector.generation.store(2, Ordering::Release);
    let result = admin
        .rebuild_inline_projection(projector.name(), &owner)
        .await
        .expect("complete rebuild");
    assert_eq!(result.applied, 3);
    assert_eq!(result.position, expected_position);
    for key in ["one", "two", "three"] {
        assert_eq!(
            store
                .projection_get(&ADMIN_LEDGER, &owner, key)
                .await
                .expect("row read")
                .expect("row exists")["generation"],
            2
        );
        assert_eq!(
            store
                .projection_get(&ADMIN_SIDECAR, &owner, key)
                .await
                .expect("sidecar read")
                .expect("sidecar row exists")["generation"],
            2
        );
    }
    assert_eq!(
        store
            .projection_get(&ADMIN_LEDGER, &neighbour, "other")
            .await
            .expect("unrelated row read")
            .expect("unrelated row exists")["generation"],
        1
    );

    let empty = TenantId::new("admin-empty").expect("tenant");
    assert_eq!(
        admin
            .rebuild_inline_projection(projector.name(), &empty)
            .await
            .expect("empty rebuild"),
        eventlog_core::InlineRebuildResult {
            applied: 0,
            position: 0,
        }
    );

    projector.fail_after.store(2, Ordering::Release);
    assert!(matches!(
        admin
            .rebuild_inline_projection(projector.name(), &owner)
            .await,
        Err(EventLogError::Invalid(_))
    ));
    assert_eq!(
        store
            .projection_get(&ADMIN_LEDGER, &owner, "three")
            .await
            .expect("old row read")
            .expect("old row remains")["generation"],
        2
    );
    assert_eq!(
        store
            .projection_get(&ADMIN_SIDECAR, &owner, "three")
            .await
            .expect("old sidecar read")
            .expect("old sidecar remains")["generation"],
        2
    );
    projector.fail_after.store(u64::MAX, Ordering::Release);

    projector.use_foreign_table.store(true, Ordering::Release);
    assert!(matches!(
        admin
            .rebuild_inline_projection(projector.name(), &owner)
            .await,
        Err(EventLogError::Invalid(_))
    ));
    projector.use_foreign_table.store(false, Ordering::Release);
    projector.use_reservation.store(true, Ordering::Release);
    assert!(matches!(
        admin
            .rebuild_inline_projection(projector.name(), &owner)
            .await,
        Err(EventLogError::Invalid(_))
    ));
    projector.use_reservation.store(false, Ordering::Release);

    store
        .delete_blob(&owner, "active-blob")
        .await
        .expect("removed required blob");
    assert!(matches!(
        admin
            .rebuild_inline_projection(projector.name(), &owner)
            .await,
        Err(EventLogError::Invalid(_))
    ));
    assert_eq!(
        store
            .projection_get(&ADMIN_LEDGER, &owner, "one")
            .await
            .expect("old row read")
            .expect("old row remains")["generation"],
        2
    );
}
