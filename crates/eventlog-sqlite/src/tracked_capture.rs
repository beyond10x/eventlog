//! Same-connection observation authority and a bounded journal of acknowledged atomic groups.
//! Every access is ordered by the provider connection mutex; this mutex never owns a connection.
//!
//! A checkpoint carries this in-process authority, a durable binding, or both: see
//! [`crate::durable_capture`] for the half a later process can continue from.
use eventlog_core::{
    AppendGroupResult, CaptureBudget, CaptureCheckpoint, CaptureError, CaptureLimits,
    CaptureMaterial, CaptureResource, CaptureUsage, CapturedBlob, CapturedProjectionDelta,
    CapturedRowChange, EventLogError, ProjectionSpec, RecordedEvent, TenantCaptureDelta,
    TenantCaptureUpdate, TenantId,
};
use rusqlite::Connection;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
};

use crate::durable_capture::DurableBinding;

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

    /// `PRAGMA main.schema_version`, as read inside the transaction this stamp was taken in.
    pub(crate) const fn schema(&self) -> i64 {
        self.schema
    }
}

#[derive(Default)]
pub(crate) struct TrackedCapture {
    state: Mutex<State>,
    /// The durable store instance this handle last read, for refusing other stores' bytes in
    /// `restore_checkpoint` without a database read. Never authority: a capture compares the
    /// stored instance inside its own transaction.
    instance: Mutex<Option<String>>,
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
    /// What the open group writes, for its durable journal entry. Kept apart from `active`,
    /// which in-process invalidation discards mid-transaction.
    draft: Option<Draft>,
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
            draft: None,
        }
    }
}
struct Pending {
    before: Stamp,
    after: Option<Stamp>,
    /// `None` for the provider's own write outside every tenant's captured material, which every
    /// tenant's chain passes through without content.
    tenant: Option<TenantId>,
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

/// The blob digests an open group newly bound and its projection row changes, coalesced per key.
pub(crate) struct Draft {
    pub(crate) tenant: TenantId,
    pub(crate) blobs: BTreeSet<String>,
    pub(crate) rows: BTreeMap<(String, String), (ProjectionSpec, CapturedRowChange)>,
    bytes: usize,
    /// Something the entry would need was not recorded; the group then writes no entry.
    pub(crate) broken: bool,
}

/// One requested projection, owned: a restored checkpoint holds names it decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScopedProjection {
    pub(crate) name: String,
    pub(crate) indexed: Vec<String>,
}
impl ScopedProjection {
    pub(crate) fn of(specification: &ProjectionSpec) -> Self {
        Self {
            name: specification.name.to_owned(),
            indexed: specification
                .indexed
                .iter()
                .map(|field| (*field).to_owned())
                .collect(),
        }
    }
    /// The same name and the same indexed fields in the same order.
    pub(crate) fn is(&self, specification: &ProjectionSpec) -> bool {
        self.name == specification.name
            && self
                .indexed
                .iter()
                .map(String::as_str)
                .eq(specification.indexed.iter().copied())
    }
}

/// The request facts a checkpoint binds.
#[derive(Clone, Debug)]
pub(crate) struct Scope {
    pub(crate) tenant: TenantId,
    pub(crate) identity: String,
    pub(crate) projections: Vec<ScopedProjection>,
    pub(crate) limits: CaptureLimits,
}
impl Scope {
    pub(crate) fn of(
        tenant: &TenantId,
        identity: &str,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
    ) -> Self {
        Self {
            tenant: tenant.clone(),
            identity: identity.to_owned(),
            projections: projections.iter().map(ScopedProjection::of).collect(),
            limits,
        }
    }
    /// A request with another tenant, another projection list or other limits is another scope.
    pub(crate) fn matches(
        &self,
        tenant: &TenantId,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
    ) -> bool {
        &self.tenant == tenant
            && self.limits == limits
            && self.projections.len() == projections.len()
            && self
                .projections
                .iter()
                .zip(projections)
                .all(|(held, requested)| held.is(requested))
    }
}

/// The private payload of every SQLite [`CaptureCheckpoint`].
pub(crate) struct Checkpoint {
    /// Same-connection authority; absent on a checkpoint restored from durable bytes.
    local: Option<Local>,
    pub(crate) scope: Scope,
    pub(crate) usage: CaptureUsage,
    /// The durable values read inside the issuing capture's transaction; absent when durable
    /// continuity was not enabled then, or a captured table lacked the provider's triggers.
    pub(crate) durable: Option<DurableBinding>,
}
struct Local {
    issuer: Arc<()>,
    epoch: Arc<()>,
    stamp: Stamp,
    position: u64,
}
impl Checkpoint {
    /// A checkpoint a later process restored: durable values only, no connection authority.
    pub(crate) const fn restored(
        scope: Scope,
        usage: CaptureUsage,
        durable: DurableBinding,
    ) -> Self {
        Self {
            local: None,
            scope,
            usage,
            durable: Some(durable),
        }
    }
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
    fn local(&self, stamp: Stamp) -> Local {
        Local {
            issuer: self.issuer.clone(),
            epoch: self.epoch.clone(),
            stamp,
            position: self.position,
        }
    }
    /// Make room for one more entry of `bytes`, evicting whole entries from the oldest.
    fn make_room(&mut self, bytes: usize) -> bool {
        while self.journal.len() >= MAX_GROUPS
            || self.bytes.checked_add(bytes).is_none_or(|n| n > MAX_BYTES)
        {
            let Some(old) = self.journal.pop_front() else {
                return false;
            };
            self.bytes -= old.pending.bytes;
        }
        true
    }
}

impl Draft {
    /// Account `bytes` more retained content, or mark the draft broken past the bound.
    fn add(&mut self, bytes: Option<usize>) -> bool {
        match bytes.and_then(|n| self.bytes.checked_add(n)) {
            Some(n) if n <= MAX_BYTES => {
                self.bytes = n;
                true
            }
            _ => {
                self.broken = true;
                false
            }
        }
    }
}

impl TrackedCapture {
    /// Start recording an atomic group's transaction; `durable` also drafts its journal entry.
    pub(crate) fn begin(&self, connection: &Connection, tenant: &TenantId, durable: bool) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.draft = durable.then(|| Draft {
            tenant: tenant.clone(),
            blobs: BTreeSet::new(),
            rows: BTreeMap::new(),
            bytes: 0,
            broken: false,
        });
        let Ok(stamp) = Stamp::read(connection) else {
            state.invalidate();
            return;
        };
        state.synchronize(stamp);
        state.active = Some(Pending {
            before: stamp,
            after: None,
            tenant: Some(tenant.clone()),
            events: Vec::new(),
            blobs: BTreeMap::new(),
            rows: BTreeMap::new(),
            bytes: 0,
        });
    }
    /// Whether a projection write must read its row's value before it changes it.
    pub(crate) fn active(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.active.is_some() || state.draft.is_some())
    }
    pub(crate) fn invalidate(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.invalidate();
        }
    }
    /// A row's value before a write could not be read: neither journal can describe the write.
    pub(crate) fn unknown_before(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.invalidate();
            if let Some(draft) = state.draft.as_mut() {
                draft.broken = true;
            }
        }
    }
    /// The open group's durable draft, handed to the journal writer inside the transaction.
    pub(crate) fn take_draft(&self) -> Option<Draft> {
        self.state.lock().ok()?.draft.take()
    }
    pub(crate) fn blob(&self, tenant: &TenantId, digest: &str, bytes: &[u8]) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(draft) = state.draft.as_mut() {
            if tenant != &draft.tenant {
                draft.broken = true;
            } else if draft.add(Some(digest.len())) {
                draft.blobs.insert(digest.to_owned());
            }
        }
        let Some(active) = state.active.as_mut() else {
            return;
        };
        let size = bytes
            .len()
            .checked_add(digest.len())
            .and_then(|n| active.bytes.checked_add(n));
        if Some(tenant) != active.tenant.as_ref() || size.is_none_or(|n| n > MAX_BYTES) {
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
        let size = (|| {
            key.len()
                .checked_add(projection.name.len())?
                .checked_add(before.as_ref().map_or(Some(0), json_size)?)?
                .checked_add(after.as_ref().map_or(Some(0), json_size)?)
        })();
        if let Some(draft) = state.draft.as_mut() {
            if tenant != &draft.tenant {
                draft.broken = true;
            } else if draft.add(size) {
                let mut mismatch = false;
                coalesce(
                    &mut draft.rows,
                    projection,
                    key,
                    before.clone(),
                    after.clone(),
                    &mut mismatch,
                );
                draft.broken |= mismatch;
            }
        }
        let Some(active) = state.active.as_mut() else {
            return;
        };
        let held = size.and_then(|n| active.bytes.checked_add(n));
        if Some(tenant) != active.tenant.as_ref() || held.is_none_or(|n| n > MAX_BYTES) {
            state.invalidate();
            return;
        }
        active.bytes = held.unwrap();
        let mut mismatch = false;
        coalesce(
            &mut active.rows,
            projection,
            key,
            before,
            after,
            &mut mismatch,
        );
        if mismatch {
            state.invalidate();
        }
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
        state.draft = None;
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
        if !state.make_room(pending.bytes) {
            state.invalidate();
            return;
        }
        state.bytes += pending.bytes;
        state.position = to;
        state.end = Some(after);
        state.journal.push_back(Entry { from, to, pending });
    }
    /// Carry continuity across one of the provider's own committed writes outside every tenant's
    /// captured material, such as a snapshot, the way an acknowledged group carries it.
    ///
    /// `before` was read after `BEGIN IMMEDIATE` and `after` before `COMMIT`. A journal whose end
    /// is not `before` already missed a write, and is left for the next capture to discard.
    pub(crate) fn neutral(&self, before: Stamp, after: Stamp) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.end != Some(before) || state.active.is_some() {
            return;
        }
        if after.external != before.external
            || after.schema != before.schema
            || after.temp_schema != before.temp_schema
        {
            state.invalidate();
            return;
        }
        if after == before {
            return;
        }
        let Some(to) = state.position.checked_add(1) else {
            state.invalidate();
            return;
        };
        if !state.make_room(0) {
            state.invalidate();
            return;
        }
        let from = state.position;
        state.position = to;
        state.end = Some(after);
        state.journal.push_back(Entry {
            from,
            to,
            pending: Pending {
                before,
                after: Some(after),
                tenant: None,
                events: Vec::new(),
                blobs: BTreeMap::new(),
                rows: BTreeMap::new(),
                bytes: 0,
            },
        });
    }
    /// Issue a checkpoint for an observation made at `stamp` inside the current transaction.
    pub(crate) fn issue(
        &self,
        stamp: Stamp,
        scope: Scope,
        usage: CaptureUsage,
        durable: Option<DurableBinding>,
    ) -> Option<CaptureCheckpoint> {
        let mut state = self.state.lock().ok()?;
        state.synchronize(stamp);
        Some(CaptureCheckpoint::new(Checkpoint {
            local: Some(state.local(stamp)),
            scope,
            usage,
            durable,
        }))
    }
    /// Continue from an in-process checkpoint through this handle's own journal.
    ///
    /// `durable` reads the current durable binding, inside the same transaction, for a new
    /// checkpoint; it is called only when one is issued.
    pub(crate) fn since(
        &self,
        stamp: Stamp,
        tenant: &TenantId,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
        previous: &CaptureCheckpoint,
        durable: &mut dyn FnMut() -> Result<Option<DurableBinding>, CaptureError>,
    ) -> Result<Option<TenantCaptureUpdate>, CaptureError> {
        let Some(old) = previous.downcast_ref::<Checkpoint>() else {
            return Ok(None);
        };
        let Some(local) = old.local.as_ref() else {
            return Ok(None);
        };
        let Ok(mut state) = self.state.lock() else {
            return Ok(None);
        };
        state.synchronize(stamp);
        if !Arc::ptr_eq(&local.issuer, &state.issuer)
            || !Arc::ptr_eq(&local.epoch, &state.epoch)
            || !old.scope.matches(tenant, projections, limits)
        {
            return Ok(None);
        }
        if local.stamp == stamp && local.position == state.position {
            return Ok(Some(TenantCaptureUpdate::Unchanged {
                checkpoint: previous.clone(),
            }));
        }
        let mut position = local.position;
        let mut prior_stamp = local.stamp;
        let mut contributed = false;
        let mut events = Vec::new();
        let mut blobs = BTreeMap::new();
        let mut rows: BTreeMap<(String, String), (ProjectionSpec, CapturedRowChange)> =
            BTreeMap::new();
        for entry in state
            .journal
            .iter()
            .filter(|entry| entry.to > local.position)
        {
            let p = &entry.pending;
            if entry.from != position
                || p.before != prior_stamp
                || p.tenant.as_ref().is_some_and(|held| held != tenant)
            {
                return Ok(None);
            }
            contributed |= p.tenant.is_some();
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
        let authority = state.local(stamp);
        let issue = |usage, durable| {
            CaptureCheckpoint::new(Checkpoint {
                local: Some(authority),
                scope: old.scope.clone(),
                usage,
                durable,
            })
        };
        if !contributed {
            // Only the provider's own writes outside captured material: the same observation.
            return Ok(Some(TenantCaptureUpdate::Unchanged {
                checkpoint: issue(old.usage, durable()?),
            }));
        }
        let mut delta = TenantCaptureDelta {
            tenant: tenant.clone(),
            stream_identity: old.scope.identity.clone(),
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
        let checkpoint = issue(delta.resulting_usage, durable()?);
        Ok(Some(TenantCaptureUpdate::AppendDelta { checkpoint, delta }))
    }
    pub(crate) fn note_instance(&self, instance: Option<String>) {
        if let Ok(mut held) = self.instance.lock() {
            *held = instance;
        }
    }
    pub(crate) fn instance(&self) -> Option<String> {
        self.instance.lock().ok()?.clone()
    }
}

/// Record one row change, keeping the first `before` and the last `after` per projection and key.
/// A second declaration of the same table under one key is a write neither journal can describe.
fn coalesce(
    rows: &mut BTreeMap<(String, String), (ProjectionSpec, CapturedRowChange)>,
    projection: ProjectionSpec,
    key: &str,
    before: Option<Value>,
    after: Option<Value>,
    mismatch: &mut bool,
) {
    let entry = rows
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
        *mismatch = true;
        return;
    }
    entry.1.after = after;
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
pub(crate) fn advanced_usage(
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
