//! Same-connection observation authority and a bounded journal of acknowledged atomic groups.
//! Every access is ordered by the provider connection mutex; this mutex never owns a connection.
use eventlog_core::{
    AppendGroupResult, CaptureBudget, CaptureCheckpoint, CaptureError, CaptureLimits,
    CaptureMaterial, CaptureResource, CaptureUsage, CapturedBlob, CapturedProjectionDelta,
    CapturedRowChange, EventLogError, ProjectionSpec, RecordedEvent, TenantCapture,
    TenantCaptureDelta, TenantCaptureUpdate, TenantId,
};
use rusqlite::Connection;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

const MAX_GROUPS: usize = 128;
const MAX_BYTES: usize = 16 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stamp {
    external: i64,
    own: u64,
    schema: i64,
    temp_schema: i64,
}
impl Stamp {
    pub(crate) fn read(connection: &Connection) -> Result<Self, EventLogError> {
        Ok(Self {
            external: connection
                .query_row("PRAGMA main.data_version", [], |r| r.get(0))
                .map_err(crate::backend)?,
            own: connection.total_changes(),
            schema: connection
                .query_row("PRAGMA main.schema_version", [], |r| r.get(0))
                .map_err(crate::backend)?,
            temp_schema: connection
                .query_row("PRAGMA temp.schema_version", [], |r| r.get(0))
                .map_err(crate::backend)?,
        })
    }
}

#[derive(Default)]
pub(crate) struct TrackedCapture {
    state: Mutex<State>,
    #[cfg(test)]
    pub(crate) full_observations: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    after_commit: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
struct State {
    issuer: Arc<()>,
    epoch: Arc<()>,
    end: Option<Stamp>,
    position: u64,
    journal: VecDeque<Entry>,
    bytes: usize,
    active: Option<Pending>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            issuer: Arc::new(()),
            epoch: Arc::new(()),
            end: None,
            position: 0,
            journal: VecDeque::new(),
            bytes: 0,
            active: None,
        }
    }
}
struct Pending {
    before: Stamp,
    after: Option<Stamp>,
    tenant: TenantId,
    events: Vec<RecordedEvent>,
    blobs: BTreeMap<String, CapturedBlob>,
    rows: BTreeMap<(String, String), (ProjectionSpec, CapturedRowChange)>,
    bytes: usize,
}
struct Entry {
    from: u64,
    to: u64,
    pending: Pending,
}
struct Checkpoint {
    issuer: Arc<()>,
    epoch: Arc<()>,
    stamp: Stamp,
    position: u64,
    tenant: TenantId,
    identity: String,
    projections: Vec<ProjectionSpec>,
    limits: CaptureLimits,
    usage: CaptureUsage,
}

impl State {
    fn invalidate(&mut self) {
        self.epoch = Arc::new(());
        self.journal.clear();
        self.bytes = 0;
        self.active = None;
        self.end = None;
        self.position = 0;
    }
    fn synchronize(&mut self, stamp: Stamp) {
        if self.end != Some(stamp) {
            self.invalidate();
            self.end = Some(stamp);
        }
    }
}

impl TrackedCapture {
    pub(crate) fn begin(&self, connection: &Connection, tenant: &TenantId) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Ok(stamp) = Stamp::read(connection) else {
            state.invalidate();
            return;
        };
        state.synchronize(stamp);
        state.active = Some(Pending {
            before: stamp,
            after: None,
            tenant: tenant.clone(),
            events: Vec::new(),
            blobs: BTreeMap::new(),
            rows: BTreeMap::new(),
            bytes: 0,
        });
    }
    pub(crate) fn active(&self) -> bool {
        self.state.lock().is_ok_and(|state| state.active.is_some())
    }
    pub(crate) fn invalidate(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.invalidate();
        }
    }
    pub(crate) fn blob(&self, tenant: &TenantId, digest: &str, bytes: &[u8]) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(active) = state.active.as_mut() else {
            return;
        };
        let size = bytes
            .len()
            .checked_add(digest.len())
            .and_then(|n| active.bytes.checked_add(n));
        if tenant != &active.tenant || size.is_none_or(|n| n > MAX_BYTES) {
            state.invalidate();
            return;
        }
        active.bytes = size.unwrap();
        active.blobs.insert(
            digest.to_owned(),
            CapturedBlob {
                digest: digest.to_owned(),
                bytes: bytes.to_vec(),
            },
        );
    }
    pub(crate) fn row(
        &self,
        tenant: &TenantId,
        projection: ProjectionSpec,
        key: &str,
        before: Option<Value>,
        after: Option<Value>,
    ) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(active) = state.active.as_mut() else {
            return;
        };
        let size = (|| {
            active
                .bytes
                .checked_add(key.len())?
                .checked_add(projection.name.len())?
                .checked_add(before.as_ref().map_or(Some(0), json_size)?)?
                .checked_add(after.as_ref().map_or(Some(0), json_size)?)
        })();
        if tenant != &active.tenant || size.is_none_or(|n| n > MAX_BYTES) {
            state.invalidate();
            return;
        }
        active.bytes = size.unwrap();
        let entry = active
            .rows
            .entry((projection.name.to_owned(), key.to_owned()))
            .or_insert_with(|| {
                (
                    projection,
                    CapturedRowChange {
                        key: key.to_owned(),
                        before,
                        after: None,
                    },
                )
            });
        if entry.0 != projection {
            state.invalidate();
            return;
        }
        entry.1.after = after;
    }
    /// Stage the exact end stamp before COMMIT releases the writer; no external stamp is sampled
    /// afterwards, when another connection could already have changed the database.
    pub(crate) fn seal(
        &self,
        connection: &Connection,
        result: &Result<AppendGroupResult, EventLogError>,
    ) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(active) = state.active.as_mut() else {
            return;
        };
        let (Ok(result), Ok(stamp)) = (result, Stamp::read(connection)) else {
            state.invalidate();
            return;
        };
        if stamp.external != active.before.external
            || stamp.schema != active.before.schema
            || stamp.temp_schema != active.before.temp_schema
        {
            state.invalidate();
            return;
        }
        if result.deduplicated {
            // Admission on the blob-bearing retry may have staged DML that its caller rolls back.
            // If so the conservative path is a new complete capture.
            if stamp != active.before {
                state.invalidate();
                return;
            }
            state.active = None;
            return;
        }
        for event in result.appends.iter().flat_map(|append| &append.events) {
            let Some(size) = serde_json::to_vec(event)
                .ok()
                .and_then(|v| active.bytes.checked_add(v.len()))
            else {
                state.invalidate();
                return;
            };
            if size > MAX_BYTES {
                state.invalidate();
                return;
            }
            active.bytes = size;
            active.events.push(event.clone());
        }
        active.events.sort_by_key(|event| event.global_seq);
        active.after = Some(stamp);
    }
    /// Only a confirmed commit grants a journal entry. Failure discards all continuity.
    pub(crate) fn finish(&self, committed: bool) {
        #[cfg(test)]
        if committed {
            let hook = self.after_commit.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if !committed {
            state.invalidate();
            return;
        }
        let Some(pending) = state.active.take() else {
            return;
        };
        let Some(after) = pending.after else {
            state.invalidate();
            return;
        };
        let Some(to) = state.position.checked_add(1) else {
            state.invalidate();
            return;
        };
        let from = state.position;
        while state.journal.len() >= MAX_GROUPS
            || state
                .bytes
                .checked_add(pending.bytes)
                .is_none_or(|n| n > MAX_BYTES)
        {
            let Some(old) = state.journal.pop_front() else {
                state.invalidate();
                return;
            };
            state.bytes -= old.pending.bytes;
        }
        state.bytes += pending.bytes;
        state.position = to;
        state.end = Some(after);
        state.journal.push_back(Entry { from, to, pending });
    }
    pub(crate) fn complete(
        &self,
        stamp: Stamp,
        capture: &TenantCapture,
        limits: CaptureLimits,
        usage: CaptureUsage,
    ) -> Option<CaptureCheckpoint> {
        let mut state = self.state.lock().ok()?;
        state.synchronize(stamp);
        Some(CaptureCheckpoint::new(Checkpoint {
            issuer: state.issuer.clone(),
            epoch: state.epoch.clone(),
            stamp,
            position: state.position,
            tenant: capture.tenant.clone(),
            identity: capture.stream_identity.clone(),
            projections: capture
                .projections
                .iter()
                .map(|p| p.specification)
                .collect(),
            limits,
            usage,
        }))
    }
    pub(crate) fn since(
        &self,
        stamp: Stamp,
        tenant: &TenantId,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
        previous: &CaptureCheckpoint,
    ) -> Result<Option<TenantCaptureUpdate>, CaptureError> {
        let Some(old) = previous.downcast_ref::<Checkpoint>() else {
            return Ok(None);
        };
        let Ok(mut state) = self.state.lock() else {
            return Ok(None);
        };
        state.synchronize(stamp);
        if !Arc::ptr_eq(&old.issuer, &state.issuer)
            || !Arc::ptr_eq(&old.epoch, &state.epoch)
            || &old.tenant != tenant
            || old.projections != projections
            || old.limits != limits
        {
            return Ok(None);
        }
        if old.stamp == stamp && old.position == state.position {
            return Ok(Some(TenantCaptureUpdate::Unchanged {
                checkpoint: previous.clone(),
            }));
        }
        let mut position = old.position;
        let mut prior_stamp = old.stamp;
        let mut events = Vec::new();
        let mut blobs = BTreeMap::new();
        let mut rows: BTreeMap<(String, String), (ProjectionSpec, CapturedRowChange)> =
            BTreeMap::new();
        for entry in state.journal.iter().filter(|entry| entry.to > old.position) {
            let p = &entry.pending;
            if entry.from != position || p.before != prior_stamp || &p.tenant != tenant {
                return Ok(None);
            }
            for event in &p.events {
                events.push(event.clone());
            }
            for (key, blob) in &p.blobs {
                blobs.insert(key.clone(), blob.clone());
            }
            for (key, (spec, row)) in &p.rows {
                if !projections.contains(spec) {
                    // Writes address the physical table by name. A callback can use a different
                    // indexed-field specification for that same table; it is not an unrelated
                    // projection whose mutation can safely be omitted from this observation.
                    if projections
                        .iter()
                        .any(|requested| requested.name == spec.name)
                    {
                        return Ok(None);
                    }
                    continue;
                }
                if let Some((held_spec, held)) = rows.get_mut(key) {
                    if held_spec != spec || held.after != row.before {
                        return Ok(None);
                    }
                    held.after.clone_from(&row.after);
                } else {
                    rows.insert(key.clone(), (*spec, row.clone()));
                }
            }
            position = entry.to;
            prior_stamp = p.after.expect("published entry is sealed");
        }
        if position != state.position || prior_stamp != stamp {
            return Ok(None);
        }
        let mut delta = TenantCaptureDelta {
            tenant: tenant.clone(),
            stream_identity: old.identity.clone(),
            events,
            blobs: blobs.into_values().collect(),
            projections: projections
                .iter()
                .map(|spec| CapturedProjectionDelta {
                    specification: *spec,
                    rows: rows
                        .values()
                        .filter(|(held, _)| held == spec)
                        .map(|(_, row)| row.clone())
                        .collect(),
                })
                .collect(),
            resulting_usage: old.usage,
        };
        delta.resulting_usage = advanced_usage(old.usage, &delta, limits)?;
        let checkpoint = CaptureCheckpoint::new(Checkpoint {
            issuer: state.issuer.clone(),
            epoch: state.epoch.clone(),
            stamp,
            position: state.position,
            tenant: tenant.clone(),
            identity: old.identity.clone(),
            projections: projections.to_vec(),
            limits,
            usage: delta.resulting_usage,
        });
        Ok(Some(TenantCaptureUpdate::AppendDelta { checkpoint, delta }))
    }
}

fn json_size(value: &Value) -> Option<usize> {
    serde_json::to_vec(value).ok().map(|v| v.len())
}
fn payload_size(value: &Value) -> Result<u64, CaptureError> {
    json_size(value)
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(CaptureError::Corrupt {
            material: CaptureMaterial::Projection,
        })
}
fn advanced_usage(
    mut usage: CaptureUsage,
    delta: &TenantCaptureDelta,
    limits: CaptureLimits,
) -> Result<CaptureUsage, CaptureError> {
    let corrupt = || CaptureError::Corrupt {
        material: CaptureMaterial::Projection,
    };
    // Subtract replaced rows before adding any new content: only the resulting capture is capped.
    for row in delta.projections.iter().flat_map(|p| &p.rows) {
        if let Some(before) = &row.before {
            usage.projection_rows = usage.projection_rows.checked_sub(1).ok_or_else(corrupt)?;
            usage.payload_bytes = usage
                .payload_bytes
                .checked_sub(payload_size(before)?)
                .ok_or_else(corrupt)?;
        }
    }
    let budget = CaptureBudget::new(limits);
    let add = |held: &mut u64, n: u64, resource, limit| -> Result<(), CaptureError> {
        *held = held
            .checked_add(n)
            .ok_or(CaptureError::LimitExceeded { resource, limit })?;
        Ok(())
    };
    for event in &delta.events {
        add(
            &mut usage.events,
            1,
            CaptureResource::Events,
            limits.max_events,
        )?;
        add(
            &mut usage.payload_bytes,
            payload_size(&event.data)?,
            CaptureResource::PayloadBytes,
            limits.max_payload_bytes,
        )?;
    }
    for blob in &delta.blobs {
        add(
            &mut usage.blobs,
            1,
            CaptureResource::Blobs,
            limits.max_blobs,
        )?;
        add(
            &mut usage.payload_bytes,
            blob.bytes.len() as u64,
            CaptureResource::PayloadBytes,
            limits.max_payload_bytes,
        )?;
    }
    for row in delta.projections.iter().flat_map(|p| &p.rows) {
        if let Some(after) = &row.after {
            add(
                &mut usage.projection_rows,
                1,
                CaptureResource::ProjectionRows,
                limits.max_projection_rows,
            )?;
            add(
                &mut usage.payload_bytes,
                payload_size(after)?,
                CaptureResource::PayloadBytes,
                limits.max_payload_bytes,
            )?;
        }
    }
    budget.proven(CaptureResource::Events, usage.events)?;
    budget.proven(CaptureResource::Blobs, usage.blobs)?;
    budget.proven(CaptureResource::ProjectionRows, usage.projection_rows)?;
    budget.proven(CaptureResource::PayloadBytes, usage.payload_bytes)?;
    // Keep the shared accounting type as the limit authority without a second set of cap rules.
    Ok(usage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqliteEventStore;
    use eventlog_core::{
        AppendGroup, AtomicEventStore, ConsistentTenantCapture, EventStore, Expected, NoGuard,
        StreamAppend, StreamId,
    };
    fn tenant() -> TenantId {
        TenantId::new("tracked").unwrap()
    }
    fn limits() -> CaptureLimits {
        CaptureLimits {
            max_events: 1024,
            max_blobs: 1024,
            max_projection_rows: 1024,
            max_payload_bytes: 1 << 24,
        }
    }
    fn group(key: &str) -> AppendGroup {
        AppendGroup {
            tenant: tenant(),
            meta: eventlog_conformance::meta(key, &serde_json::json!({})),
            appends: vec![StreamAppend {
                stream: StreamId::new(tenant(), "item", "one").unwrap(),
                expected: Expected::Any,
                events: vec![eventlog_conformance::event("item.changed", 1)],
            }],
        }
    }
    async fn checkpoint(store: &SqliteEventStore) -> CaptureCheckpoint {
        match store
            .capture_tenant_since(&tenant(), &[], limits(), None)
            .await
            .unwrap()
        {
            TenantCaptureUpdate::Complete {
                checkpoint: Some(checkpoint),
                ..
            } => checkpoint,
            other => panic!("expected checkpoint: {other:?}"),
        }
    }
    #[tokio::test]
    async fn warm_reads_and_guarded_appends_do_not_enter_complete_observation() {
        let store = SqliteEventStore::in_memory("tracked").await.unwrap();
        store.stream_identity(&tenant()).await.unwrap();
        for i in 0..40 {
            store
                .append_group(&group(&format!("seed-{i}")))
                .await
                .unwrap();
        }
        let mut previous = checkpoint(&store).await;
        let count = store
            .inner
            .tracked
            .full_observations
            .load(std::sync::atomic::Ordering::Relaxed);
        for i in 0..20 {
            assert!(matches!(
                store
                    .capture_tenant_since(&tenant(), &[], limits(), Some(&previous))
                    .await
                    .unwrap(),
                TenantCaptureUpdate::Unchanged { .. }
            ));
            store
                .append_group(&group(&format!("new-{i}")))
                .await
                .unwrap();
            match store
                .capture_tenant_since(&tenant(), &[], limits(), Some(&previous))
                .await
                .unwrap()
            {
                TenantCaptureUpdate::AppendDelta { checkpoint, delta } => {
                    assert_eq!(delta.events.len(), 1);
                    previous = checkpoint;
                }
                other => panic!("warm append did not produce delta: {other:?}"),
            }
        }
        assert_eq!(
            store
                .inner
                .tracked
                .full_observations
                .load(std::sync::atomic::Ordering::Relaxed),
            count
        );
    }
    #[tokio::test]
    async fn sql_commit_between_group_commit_and_journal_publication_is_not_adopted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let store = SqliteEventStore::open(path.to_str().unwrap(), "tracked")
            .await
            .unwrap();
        store.stream_identity(&tenant()).await.unwrap();
        store.put_blob(&tenant(), "old", b"before").await.unwrap();
        let previous = checkpoint(&store).await;
        *store.inner.tracked.after_commit.lock().unwrap() = Some(Box::new(move || {
            Connection::open(path)
                .unwrap()
                .execute(
                    "UPDATE tracked_blobs SET bytes=?1 WHERE digest='old'",
                    rusqlite::params![b"broken".as_slice()],
                )
                .unwrap();
        }));
        store.append_group(&group("commit-window")).await.unwrap();
        assert!(matches!(
            store
                .capture_tenant_since(&tenant(), &[], limits(), Some(&previous))
                .await,
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Blob
            })
        ));
    }
    #[tokio::test]
    async fn failed_commit_never_publishes_an_append_delta() {
        let store = SqliteEventStore::in_memory("tracked").await.unwrap();
        store.stream_identity(&tenant()).await.unwrap();
        let previous = checkpoint(&store).await;
        store
            .inner
            .connection
            .lock()
            .unwrap()
            .commit_hook(Some(|| true))
            .unwrap();
        assert!(matches!(
            store
                .append_group_guarded_with_blobs(
                    &group("abort"),
                    Arc::new(NoGuard),
                    &[("tentative".into(), vec![1])]
                )
                .await,
            Err(EventLogError::UnknownCommit)
        ));
        store
            .inner
            .connection
            .lock()
            .unwrap()
            .commit_hook(None::<fn() -> bool>)
            .unwrap();
        let TenantCaptureUpdate::Complete { capture, .. } = store
            .capture_tenant_since(&tenant(), &[], limits(), Some(&previous))
            .await
            .unwrap()
        else {
            panic!("uncertain commit kept authority")
        };
        assert_eq!(capture.events, [] as [eventlog_core::RecordedEvent; 0]);
        assert_eq!(capture.blobs, [] as [eventlog_core::CapturedBlob; 0]);
    }
    #[tokio::test]
    async fn temporary_triggers_and_foreign_keys_disable_checkpoint_issuance() {
        for statement in [
            "CREATE TEMP TRIGGER hidden AFTER INSERT ON main.tracked_events BEGIN UPDATE tracked_events SET data='{}'; END",
            "CREATE TABLE indirect(id TEXT REFERENCES tracked_identity(tenant_id) ON DELETE CASCADE)",
            "CREATE TEMP TABLE parent(id TEXT PRIMARY KEY); CREATE TEMP TABLE child(id TEXT REFERENCES parent(id) ON DELETE CASCADE)",
        ] {
            let store = SqliteEventStore::in_memory("tracked").await.unwrap();
            store.stream_identity(&tenant()).await.unwrap();
            let previous = checkpoint(&store).await;
            store
                .inner
                .connection
                .lock()
                .unwrap()
                .execute_batch(statement)
                .unwrap();
            assert!(
                matches!(
                    store
                        .capture_tenant_since(&tenant(), &[], limits(), Some(&previous))
                        .await
                        .unwrap(),
                    TenantCaptureUpdate::Complete {
                        checkpoint: None,
                        ..
                    }
                ),
                "{statement}"
            );
        }
    }
    #[test]
    fn cumulative_counter_overflow_is_refused() {
        let delta = TenantCaptureDelta {
            tenant: tenant(),
            stream_identity: "id".into(),
            events: vec![],
            blobs: vec![CapturedBlob {
                digest: "d".into(),
                bytes: vec![1],
            }],
            projections: vec![],
            resulting_usage: CaptureUsage::default(),
        };
        let usage = CaptureUsage {
            blobs: u64::MAX,
            ..CaptureUsage::default()
        };
        assert!(matches!(
            advanced_usage(usage, &delta, limits()),
            Err(CaptureError::LimitExceeded {
                resource: CaptureResource::Blobs,
                ..
            })
        ));
    }
}
