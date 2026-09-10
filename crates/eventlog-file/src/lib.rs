#![forbid(unsafe_code)]
//! Repository-local Eventlog. JSONL transactions are authoritative; snapshots are disposable.
mod journal;
mod projection;
mod state;

use eventlog_core::{
    AdmissionPermit, AppendGroup, AppendGroupResult, AppendResult, AtomicEventStore, BoxFuture,
    CatchUpProgress, Claim, ClaimedCommand, CommandMeta, EventLogError, EventStore, Expected,
    FeedPage, GroupRange, Guard, NewEvent, NoGuard, ProjectionSpec, ProjectionStore, Projector,
    RecordedEvent, Snapshot, SnapshotGeneration, StreamId, StreamSlice, TenantId, bounded_limit,
    new_event_id, redaction_tombstone, validate_append, validate_field,
};
use journal::{Journal, backend, hash};
use serde_json::Value;
use state::{Blob, Op, State, key, matches_stream, stream_key};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use time::OffsetDateTime;
use tokio::sync::Mutex;

/// One directory owns one log. All handles and processes serialize at its persistent writer lock.
pub struct FileEventStore {
    root: PathBuf,
    runtime: Mutex<Runtime>,
    permit: AdmissionPermit,
}
#[derive(Default)]
struct Runtime {
    inline: Vec<Arc<dyn Projector>>,
    frozen: bool,
    observed: Option<journal::Manifest>,
}
struct Transaction {
    journal: Journal,
    state: State,
    pending: Vec<Op>,
    inline: Vec<Arc<dyn Projector>>,
    root: PathBuf,
    privacy: bool,
    permit: AdmissionPermit,
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, EventLogError> + Send + 'static,
) -> Result<T, EventLogError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| backend("file store worker panicked"))?
}
impl FileEventStore {
    /// Give this grant only to the trusted host that constructs append guards.
    pub fn admission_permit(&self) -> AdmissionPermit {
        self.permit.clone()
    }
    /// Open the store and verify its complete committed history before admitting calls.
    /// # Errors
    /// Refuses corrupt history, unknown formats and inaccessible storage.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, EventLogError> {
        let root = if path.as_ref().is_absolute() {
            path.as_ref().to_owned()
        } else {
            std::env::current_dir().map_err(backend)?.join(path)
        };
        let path = root.clone();
        let journal = blocking(move || Journal::open(&path)).await?;
        State::replay(&journal.transactions)?;
        let observed = Some(journal.manifest.clone());
        drop(journal);
        let store = Self {
            root,
            runtime: Mutex::new(Runtime {
                observed,
                ..Runtime::default()
            }),
            permit: AdmissionPermit::default(),
        };
        store.transaction(|_| Box::pin(async { Ok(()) })).await?;
        Ok(store)
    }
    async fn transaction<T, F>(&self, work: F) -> Result<T, EventLogError>
    where
        T: Send,
        F: for<'a> FnOnce(&'a mut Transaction) -> BoxFuture<'a, Result<T, EventLogError>> + Send,
    {
        let mut runtime = self.runtime.lock().await;
        let path = self.root.clone();
        let journal = blocking(move || Journal::open(&path)).await?;
        if let Some(observed) = &runtime.observed
            && !journal.extends(observed)?
        {
            return Err(backend(
                "file history diverged from this handle's observed history",
            ));
        }
        let state = State::replay(&journal.transactions)?;
        let mut tx = Transaction {
            journal,
            state,
            pending: Vec::new(),
            inline: runtime.inline.clone(),
            root: self.root.clone(),
            privacy: false,
            permit: self.permit.clone(),
        };
        tx = blocking(move || {
            tx.clear_snapshots()?;
            tx.clean_blobs()?;
            for (tenant, digest) in tx.state.blobs.keys() {
                tx.blob(&TenantId::new(tenant)?, digest)?;
            }
            Ok(tx)
        })
        .await?;
        let result = work(&mut tx).await?;
        let manifest = blocking(move || {
            if !tx.pending.is_empty() {
                tx.pending.push(Op::Watermark {
                    position: tx.state.next_position,
                });
            }
            if tx.privacy {
                if !tx.pending.is_empty() {
                    tx.journal
                        .transactions
                        .push(serde_json::to_value(&tx.pending).map_err(backend)?);
                }
                tx.journal.privacy(tx.journal.transactions.clone())?;
                tx.clear_snapshots()?;
            } else if !tx.pending.is_empty() {
                tx.journal
                    .append(serde_json::to_value(&tx.pending).map_err(backend)?)?;
            }
            tx.clean_blobs()?;
            Ok(tx.journal.manifest.clone())
        })
        .await?;
        runtime.observed = Some(manifest);
        Ok(result)
    }
    async fn freeze(&self) {
        self.runtime.lock().await.frozen = true;
    }
}
impl Transaction {
    fn record(&mut self, op: Op) -> Result<(), EventLogError> {
        self.state.apply(op.clone())?;
        self.pending.push(op);
        Ok(())
    }
    fn result(&self, range: &GroupRange, deduplicated: bool) -> AppendResult {
        AppendResult {
            first_version: range.first_version,
            last_version: range.last_version,
            events: self
                .state
                .events
                .values()
                .filter(|e| {
                    matches_stream(e, &range.stream)
                        && e.version >= range.first_version
                        && e.version <= range.last_version
                })
                .cloned()
                .collect(),
            deduplicated,
        }
    }
    fn command(
        &self,
        stream: &StreamId,
        id: &str,
        digest: &str,
    ) -> Result<Option<AppendResult>, EventLogError> {
        self.state
            .commands
            .get(&key(&[&stream_key(stream), id]))
            .map(|(stored, range)| {
                if stored != digest {
                    return Err(EventLogError::IdempotencyMismatch { key: id.into() });
                }
                Ok(self.result(range, true))
            })
            .transpose()
    }
    fn claim(&self, tenant: &TenantId, claim: &Claim) -> Result<Option<GroupRange>, EventLogError> {
        self.state
            .claims
            .get(&key(&[tenant.as_str(), &claim.scope, &claim.key]))
            .map(|(digest, range)| {
                if digest != &claim.digest {
                    return Err(EventLogError::IdempotencyMismatch {
                        key: claim.key.clone(),
                    });
                }
                Ok(range.clone())
            })
            .transpose()
    }
    async fn append(
        &mut self,
        stream: &StreamId,
        expected: Expected,
        events: &[NewEvent],
        meta: &CommandMeta,
    ) -> Result<AppendResult, EventLogError> {
        let head = self.state.head(stream);
        let wanted = match expected {
            Expected::Any => head,
            Expected::NoStream => 0,
            Expected::Exact(v) => v,
        };
        if wanted != head {
            return Err(EventLogError::Conflict {
                expected: wanted,
                actual: head,
            });
        }
        let first = head
            .checked_add(1)
            .ok_or_else(|| backend("stream version overflow"))?;
        let mut written = Vec::new();
        for event in events {
            let version = first
                .checked_add(written.len() as u64)
                .ok_or_else(|| backend("stream version overflow"))?;
            let global_seq = self
                .state
                .next_position
                .checked_add(1)
                .ok_or_else(|| backend("feed position overflow"))?;
            let recorded = RecordedEvent {
                global_seq,
                tenant: stream.tenant().clone(),
                stream_type: stream.stream_type().into(),
                stream_id: stream.stream_id().into(),
                version,
                event_id: new_event_id(),
                name: event.name.clone(),
                schema_version: event.schema_version,
                occurred_at: meta.occurred_at,
                recorded_at: OffsetDateTime::now_utc(),
                subject: meta.subject.clone(),
                actor: meta.actor.clone(),
                request_id: meta.request_id.clone(),
                trace_id: meta.trace_id.clone(),
                causation_id: meta.causation_id.clone(),
                causation_depth: meta.causation_depth,
                redacted_at: None,
                data: event.data.clone(),
            };
            self.record(Op::Event {
                event: recorded.clone(),
            })?;
            written.push(recorded);
        }
        for projector in self.inline.clone() {
            for spec in projector.projections() {
                projection::View {
                    admission: false,
                    tx: self,
                    tenant: stream.tenant(),
                }
                .validate(spec, stream.tenant())?;
            }
            for event in &written {
                let mut projection = projection::View {
                    admission: false,
                    tx: self,
                    tenant: stream.tenant(),
                };
                projector.apply(event, &mut projection).await?;
            }
        }
        Ok(AppendResult {
            first_version: first,
            last_version: head + written.len() as u64,
            events: written,
            deduplicated: false,
        })
    }
    fn register(&mut self, projector: &dyn Projector) -> Result<(), EventLogError> {
        let mut names = BTreeSet::new();
        for spec in projector.projections() {
            spec.validate()?;
            if !names.insert(spec.name) {
                return Err(EventLogError::Invalid(
                    "duplicate projection in one registration".into(),
                ));
            }
            let indexed: Vec<String> = spec.indexed.iter().map(|s| (*s).into()).collect();
            if let Some(old) = self.state.projections.get(spec.name) {
                if old != &indexed {
                    return Err(EventLogError::Invalid(
                        "projection registration shape changed".into(),
                    ));
                }
            } else {
                self.record(Op::Projection {
                    name: spec.name.into(),
                    indexed,
                })?;
            }
        }
        Ok(())
    }
    fn generation(&mut self, stream: &StreamId) -> Result<String, EventLogError> {
        if let Some(id) = self.state.generations.get(&stream_key(stream)) {
            return Ok(id.clone());
        }
        let id = new_event_id();
        self.record(Op::Generation {
            stream: stream.clone(),
            id: id.clone(),
        })?;
        Ok(id)
    }
    fn snapshot_path(&self, stream: &StreamId) -> PathBuf {
        self.root
            .join(".cache")
            .join(hash(stream_key(stream).as_bytes()))
    }
    fn clear_snapshots(&self) -> Result<(), EventLogError> {
        let cache = self.root.join(".cache");
        if cache.exists() {
            directory(&cache)?;
            for entry in fs::read_dir(&cache).map_err(backend)? {
                let entry = entry.map_err(backend)?;
                if !entry.file_type().map_err(backend)?.is_file() {
                    return Err(backend("snapshot cache contains a non-file entry"));
                }
                if entry.file_type().map_err(backend)?.is_file() {
                    let live = fs::read(entry.path())
                        .ok()
                        .and_then(|bytes| serde_json::from_slice::<(String, Snapshot)>(&bytes).ok())
                        .is_some_and(|(generation, _)| {
                            self.state.generations.values().any(|id| id == &generation)
                        });
                    if !live {
                        fs::remove_file(entry.path()).map_err(backend)?;
                    }
                }
            }
            fs::File::open(cache)
                .and_then(|f| f.sync_all())
                .map_err(backend)?;
        }
        Ok(())
    }
    fn blob(&self, tenant: &TenantId, digest: &str) -> Result<Option<Vec<u8>>, EventLogError> {
        let Some(blob) = self
            .state
            .blobs
            .get(&(tenant.as_str().into(), digest.into()))
        else {
            return Ok(None);
        };
        validate_object(&blob.id)?;
        let path = self.root.join("blobs").join(&blob.id);
        if !fs::symlink_metadata(&path).map_err(backend)?.is_file() {
            return Err(backend("blob is not a regular file"));
        }
        let bytes = fs::read(path).map_err(backend)?;
        if hash(&bytes) != blob.hash {
            return Err(backend("referenced blob integrity check failed"));
        }
        Ok(Some(bytes))
    }
    fn clean_blobs(&self) -> Result<(), EventLogError> {
        let directory = self.root.join("blobs");
        if !directory.exists() {
            return Ok(());
        }
        self::directory(&directory)?;
        let live: BTreeSet<_> = self.state.blobs.values().map(|b| b.id.as_str()).collect();
        for entry in fs::read_dir(&directory).map_err(backend)? {
            let entry = entry.map_err(backend)?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| backend("invalid blob object name"))?;
            validate_object(name)?;
            if !entry.file_type().map_err(backend)?.is_file() {
                return Err(backend("blob is not a regular file"));
            }
            if !live.contains(name) {
                fs::remove_file(entry.path()).map_err(backend)?;
            }
        }
        fs::File::open(directory)
            .and_then(|f| f.sync_all())
            .map_err(backend)
    }
    async fn catch_up(
        &mut self,
        projector: &dyn Projector,
        tenant: &TenantId,
        batch: usize,
    ) -> Result<CatchUpProgress, EventLogError> {
        if self.inline.iter().any(|p| p.name() == projector.name()) {
            return Err(EventLogError::Invalid("projection is driven inline".into()));
        }
        let position = *self
            .state
            .cursors
            .get(&(tenant.as_str().into(), projector.name().into()))
            .unwrap_or(&0);
        let mut events: Vec<_> = self
            .state
            .events
            .values()
            .filter(|e| &e.tenant == tenant && e.global_seq > position)
            .take(bounded_limit(batch) + 1)
            .cloned()
            .collect();
        let more_waiting = events.len() > bounded_limit(batch);
        events.truncate(bounded_limit(batch));
        for event in &events {
            projector
                .apply(
                    event,
                    &mut projection::View {
                        admission: false,
                        tx: self,
                        tenant,
                    },
                )
                .await?;
        }
        let position = events.last().map_or(position, |e| e.global_seq);
        self.record(Op::Cursor {
            tenant: tenant.clone(),
            name: projector.name().into(),
            position,
        })?;
        Ok(CatchUpProgress {
            applied: events.len() as u64,
            position,
            more_waiting,
        })
    }
}
fn validate_object(id: &str) -> Result<(), EventLogError> {
    if id.len() != 36 || !id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        return Err(backend("invalid blob object name"));
    }
    Ok(())
}

fn directory(path: &Path) -> Result<(), EventLogError> {
    if !fs::symlink_metadata(path).map_err(backend)?.is_dir() {
        return Err(backend("store directory is not a physical directory"));
    }
    Ok(())
}

impl AtomicEventStore for FileEventStore {
    fn append_group_guarded<'a>(
        &'a self,
        group: &'a AppendGroup,
        guard: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        Box::pin(async move {
            let digest = group.fingerprint()?;
            let group = group.clone();
            self.freeze().await;
            self.transaction(move |tx| {
                Box::pin(async move {
                    if let Some((old, ranges)) = tx
                        .state
                        .groups
                        .get(&key(&[group.tenant.as_str(), &group.meta.idempotency_key]))
                    {
                        if old != &digest {
                            return Err(EventLogError::IdempotencyMismatch {
                                key: group.meta.idempotency_key.clone(),
                            });
                        }
                        return Ok(AppendGroupResult {
                            appends: ranges.iter().map(|r| tx.result(r, true)).collect(),
                            deduplicated: true,
                        });
                    }
                    guard
                        .check(&mut projection::View {
                            admission: true,
                            tx,
                            tenant: &group.tenant,
                        })
                        .await?;
                    let mut appends = Vec::new();
                    let mut ranges = Vec::new();
                    for entry in &group.appends {
                        let result = tx
                            .append(&entry.stream, entry.expected, &entry.events, &group.meta)
                            .await?;
                        ranges.push(GroupRange {
                            stream: entry.stream.clone(),
                            first_version: result.first_version,
                            last_version: result.last_version,
                        });
                        appends.push(result);
                    }
                    tx.record(Op::Group {
                        tenant: group.tenant,
                        key: group.meta.idempotency_key,
                        digest,
                        ranges,
                    })?;
                    Ok(AppendGroupResult {
                        appends,
                        deduplicated: false,
                    })
                })
            })
            .await
        })
    }
}

impl EventStore for FileEventStore {
    fn append<'a>(
        &'a self,
        stream: &'a StreamId,
        expected: Expected,
        events: &'a [NewEvent],
        meta: &'a CommandMeta,
    ) -> BoxFuture<'a, Result<AppendResult, EventLogError>> {
        self.append_guarded(stream, expected, events, meta, Arc::new(NoGuard))
    }
    fn append_guarded<'a>(
        &'a self,
        stream: &'a StreamId,
        expected: Expected,
        events: &'a [NewEvent],
        meta: &'a CommandMeta,
        guard: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendResult, EventLogError>> {
        Box::pin(async move {
            validate_append(events, meta)?;
            let (stream, events, meta) = (stream.clone(), events.to_vec(), meta.clone());
            self.freeze().await;
            self.transaction(move |tx| {
                Box::pin(async move {
                    if let Some(claim) = &meta.claim
                        && let Some(range) = tx.claim(stream.tenant(), claim)?
                    {
                        return Ok(tx.result(&range, true));
                    }
                    if let Some(result) =
                        tx.command(&stream, &meta.idempotency_key, &meta.request_hash)?
                    {
                        return Ok(result);
                    }
                    let head = tx.state.head(&stream);
                    let wanted = match expected {
                        Expected::Any => head,
                        Expected::NoStream => 0,
                        Expected::Exact(v) => v,
                    };
                    if head != wanted {
                        return Err(EventLogError::Conflict {
                            expected: wanted,
                            actual: head,
                        });
                    }
                    guard
                        .check(&mut projection::View {
                            admission: true,
                            tx,
                            tenant: stream.tenant(),
                        })
                        .await?;
                    let result = tx.append(&stream, expected, &events, &meta).await?;
                    tx.record(Op::Command {
                        stream: stream.clone(),
                        key: meta.idempotency_key,
                        digest: meta.request_hash,
                        first: result.first_version,
                        last: result.last_version,
                    })?;
                    if let Some(claim) = meta.claim {
                        tx.record(Op::Claim {
                            tenant: stream.tenant().clone(),
                            scope: claim.scope,
                            key: claim.key,
                            digest: claim.digest,
                            range: GroupRange {
                                stream,
                                first_version: result.first_version,
                                last_version: result.last_version,
                            },
                        })?;
                    }
                    Ok(result)
                })
            })
            .await
        })
    }
    fn recorded_command<'a>(
        &'a self,
        stream: &'a StreamId,
        id: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<AppendResult>, EventLogError>> {
        let (stream, id, digest) = (stream.clone(), id.to_owned(), digest.to_owned());
        Box::pin(
            self.transaction(move |tx| Box::pin(async move { tx.command(&stream, &id, &digest) })),
        )
    }
    fn recorded_claim<'a>(
        &'a self,
        tenant: &'a TenantId,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Option<ClaimedCommand>, EventLogError>> {
        let (tenant, claim) = (tenant.clone(), claim.clone());
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                Ok(tx.claim(&tenant, &claim)?.map(|range| ClaimedCommand {
                    stream: range.stream,
                    first_version: range.first_version,
                    last_version: range.last_version,
                }))
            })
        }))
    }
    fn read_stream<'a>(
        &'a self,
        stream: &'a StreamId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<StreamSlice, EventLogError>> {
        let stream = stream.clone();
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                let mut events: Vec<_> = tx
                    .state
                    .events
                    .values()
                    .filter(|e| matches_stream(e, &stream) && e.version > after)
                    .take(bounded_limit(limit) + 1)
                    .cloned()
                    .collect();
                let end_of_stream = events.len() <= bounded_limit(limit);
                events.truncate(bounded_limit(limit));
                Ok(StreamSlice {
                    next_version: events.last().map_or(after, |e| e.version),
                    events,
                    end_of_stream,
                })
            })
        }))
    }
    fn stream_version<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<u64>, EventLogError>> {
        let stream = stream.clone();
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                let head = tx.state.head(&stream);
                Ok((head > 0).then_some(head))
            })
        }))
    }
    fn list_streams<'a>(
        &'a self,
        tenant: &'a TenantId,
        stream_type: &'a str,
        after_id: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<StreamId>, EventLogError>> {
        let (tenant, stream_type, after) = (
            tenant.clone(),
            stream_type.to_owned(),
            after_id.map(str::to_owned),
        );
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                StreamId::new(
                    tenant.clone(),
                    &stream_type,
                    after.as_deref().unwrap_or("_"),
                )?;
                let ids: std::collections::BTreeSet<_> = tx
                    .state
                    .events
                    .values()
                    .filter(|event| event.tenant == tenant && event.stream_type == stream_type)
                    .map(|event| event.stream_id.as_str())
                    .filter(|id| after.as_deref().is_none_or(|after| *id > after))
                    .collect();
                ids.into_iter()
                    .take(bounded_limit(limit))
                    .map(|id| StreamId::new(tenant.clone(), &stream_type, id))
                    .collect()
            })
        }))
    }

    fn read_feed<'a>(
        &'a self,
        tenant: &'a TenantId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<FeedPage, EventLogError>> {
        let tenant = tenant.clone();
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                let mut events: Vec<_> = tx
                    .state
                    .events
                    .values()
                    .filter(|e| e.tenant == tenant && e.global_seq > after)
                    .take(bounded_limit(limit) + 1)
                    .cloned()
                    .collect();
                let has_more = events.len() > bounded_limit(limit);
                events.truncate(bounded_limit(limit));
                Ok(FeedPage {
                    next_position: events.last().map_or(after, |e| e.global_seq),
                    events,
                    has_more,
                })
            })
        }))
    }
    fn stream_identity<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<String, EventLogError>> {
        let tenant = tenant.clone();
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                if let Some(id) = tx.state.identities.get(tenant.as_str()) {
                    return Ok(id.clone());
                }
                let id = new_event_id();
                tx.record(Op::Identity {
                    tenant,
                    id: id.clone(),
                })?;
                Ok(id)
            })
        }))
    }
    fn snapshot_generation<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<SnapshotGeneration>, EventLogError>> {
        let stream = stream.clone();
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move { Ok(Some(tx.generation(&stream)?.parse()?)) })
        }))
    }
    fn save_snapshot<'a>(
        &'a self,
        _: &'a StreamId,
        _: &'a Snapshot,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async {
            Err(EventLogError::Invalid(
                "unproven snapshots are refused".into(),
            ))
        })
    }
    fn save_snapshot_checked<'a>(
        &'a self,
        stream: &'a StreamId,
        snapshot: &'a Snapshot,
        generation: &'a SnapshotGeneration,
    ) -> BoxFuture<'a, Result<bool, EventLogError>> {
        let (stream, snapshot, generation) = (
            stream.clone(),
            snapshot.clone(),
            generation.as_uuid().to_string(),
        );
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                if tx.state.generations.get(&stream_key(&stream)) != Some(&generation) {
                    return Ok(false);
                }
                let path = tx.snapshot_path(&stream);
                fs::create_dir_all(tx.root.join(".cache")).map_err(backend)?;
                directory(&tx.root.join(".cache"))?;
                fs::write(
                    path,
                    serde_json::to_vec(&(generation, snapshot)).map_err(backend)?,
                )
                .map_err(backend)?;
                Ok(true)
            })
        }))
    }
    fn load_snapshot<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<Snapshot>, EventLogError>> {
        let stream = stream.clone();
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                let Ok(bytes) = fs::read(tx.snapshot_path(&stream)) else {
                    return Ok(None);
                };
                let Ok((generation, snapshot)) =
                    serde_json::from_slice::<(String, Snapshot)>(&bytes)
                else {
                    return Ok(None);
                };
                Ok(
                    (tx.state.generations.get(&stream_key(&stream)) == Some(&generation))
                        .then_some(snapshot),
                )
            })
        }))
    }
    fn put_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        let (tenant, digest, bytes) = (tenant.clone(), digest.to_owned(), bytes.to_vec());
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                validate_field("blob digest", &digest)?;
                if let Some(old) = tx.blob(&tenant, &digest)? {
                    if old != bytes {
                        return Err(EventLogError::Invalid(
                            "blob digest already names different content".into(),
                        ));
                    }
                    return Ok(());
                }
                let blob = Blob {
                    id: new_event_id(),
                    hash: hash(&bytes),
                };
                let directory = tx.root.join("blobs");
                fs::create_dir_all(&directory).map_err(backend)?;
                self::directory(&directory)?;
                let path = directory.join(&blob.id);
                blocking(move || {
                    use std::io::Write;
                    let mut file = fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(path)
                        .map_err(backend)?;
                    file.write_all(&bytes)
                        .and_then(|()| file.sync_all())
                        .map_err(backend)?;
                    fs::File::open(directory)
                        .and_then(|f| f.sync_all())
                        .map_err(backend)
                })
                .await?;
                tx.record(Op::Blob {
                    tenant,
                    digest,
                    object: Some(blob),
                })
            })
        }))
    }
    fn get_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, EventLogError>> {
        let (tenant, digest) = (tenant.clone(), digest.to_owned());
        Box::pin(self.transaction(move |tx| Box::pin(async move { tx.blob(&tenant, &digest) })))
    }
    fn delete_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        let (tenant, digest) = (tenant.clone(), digest.to_owned());
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                tx.record(Op::Blob {
                    tenant,
                    digest,
                    object: None,
                })
            })
        }))
    }
    fn redact<'a>(
        &'a self,
        stream: &'a StreamId,
        version: u64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<RecordedEvent, EventLogError>> {
        let (stream, reason) = (stream.clone(), reason.to_owned());
        Box::pin(self.transaction(move |tx| Box::pin(async move {
            validate_field("redaction reason", &reason)?;
            let mut event = tx.state.events.values().find(|e| matches_stream(e, &stream) && e.version == version).cloned().ok_or(EventLogError::NotFound)?;
            event.data = redaction_tombstone(&reason); event.redacted_at = Some(OffsetDateTime::now_utc());
            for transaction in &mut tx.journal.transactions {
                let mut ops: Vec<Op> = serde_json::from_value(transaction.clone()).map_err(backend)?;
                for op in &mut ops { if let Op::Event { event: old } = op && old.event_id == event.event_id { *old = event.clone(); } }
                // Derived rows can carry the old body too. Remove them and require replay.
                ops.retain(|op| !matches!(op, Op::Row { tenant, .. } | Op::Cursor { tenant, .. } if tenant == stream.tenant()));
                *transaction = serde_json::to_value(ops).map_err(backend)?;
            }
            tx.state = State::replay(&tx.journal.transactions)?;
            for name in tx.state.projections.keys().cloned().collect::<Vec<_>>() {
                tx.record(Op::DirtyView { tenant: stream.tenant().clone(), name, dirty: true })?;
            }
            tx.record(Op::Generation { stream, id: new_event_id() })?;
            tx.privacy = true;
            Ok(event)
        })))
    }
    fn forget_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        let tenant = tenant.clone();
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                for transaction in &mut tx.journal.transactions {
                    let mut ops: Vec<Op> =
                        serde_json::from_value(transaction.clone()).map_err(backend)?;
                    ops.retain(|op| op.tenant() != Some(&tenant));
                    *transaction = serde_json::to_value(ops).map_err(backend)?;
                }
                tx.state = State::replay(&tx.journal.transactions)?;
                tx.privacy = true;
                Ok(())
            })
        }))
    }
    fn create_projections(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        Box::pin(
            self.transaction(move |tx| Box::pin(async move { tx.register(projector.as_ref()) })),
        )
    }
    fn register_inline(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        Box::pin(async move {
            // Hold registration across admission so a concurrent append cannot freeze midway.
            let mut runtime = self.runtime.lock().await;
            if runtime.frozen {
                return Err(EventLogError::Invalid(
                    "inline registration is frozen after serving begins".into(),
                ));
            }
            if runtime.inline.iter().any(|old| {
                old.name() == projector.name()
                    || old
                        .projections()
                        .iter()
                        .any(|s| projector.projections().iter().any(|new| new.name == s.name))
            }) {
                return Err(EventLogError::Invalid("duplicate inline projection".into()));
            }
            let path = self.root.clone();
            let journal = blocking(move || Journal::open(&path)).await?;
            if let Some(observed) = &runtime.observed
                && !journal.extends(observed)?
            {
                return Err(backend(
                    "file history diverged from this handle's observed history",
                ));
            }
            let mut tx = Transaction {
                state: State::replay(&journal.transactions)?,
                journal,
                pending: Vec::new(),
                inline: runtime.inline.clone(),
                root: self.root.clone(),
                privacy: false,
                permit: self.permit.clone(),
            };
            tx.register(projector.as_ref())?;
            let manifest = blocking(move || {
                if !tx.pending.is_empty() {
                    tx.journal
                        .append(serde_json::to_value(tx.pending).map_err(backend)?)?;
                }
                Ok(tx.journal.manifest.clone())
            })
            .await?;
            runtime.observed = Some(manifest);
            runtime.inline.push(projector);
            Ok(())
        })
    }
    fn is_inline<'a>(&'a self, name: &'a str) -> BoxFuture<'a, bool> {
        Box::pin(async move {
            self.runtime
                .lock()
                .await
                .inline
                .iter()
                .any(|p| p.name() == name)
        })
    }
    fn run_catch_up<'a>(
        &'a self,
        projector: Arc<dyn Projector>,
        tenant: &'a TenantId,
        batch: usize,
    ) -> BoxFuture<'a, Result<CatchUpProgress, EventLogError>> {
        let tenant = tenant.clone();
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move { tx.catch_up(projector.as_ref(), &tenant, batch).await })
        }))
    }
    fn rebuild_projection<'a>(
        &'a self,
        projector: Arc<dyn Projector>,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<u64, EventLogError>> {
        let tenant = tenant.clone();
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                tx.register(projector.as_ref())?;
                for spec in projector.projections() {
                    tx.record(Op::DirtyView {
                        tenant: tenant.clone(),
                        name: spec.name.into(),
                        dirty: false,
                    })?;
                    tx.record(Op::ClearRows {
                        tenant: tenant.clone(),
                        name: spec.name.into(),
                    })?;
                }
                let events: Vec<_> = tx
                    .state
                    .events
                    .values()
                    .filter(|e| e.tenant == tenant)
                    .cloned()
                    .collect();
                for event in &events {
                    projector
                        .apply(
                            event,
                            &mut projection::View {
                                admission: false,
                                tx,
                                tenant: &tenant,
                            },
                        )
                        .await?;
                }
                tx.record(Op::Cursor {
                    tenant,
                    name: projector.name().into(),
                    position: events.last().map_or(0, |e| e.global_seq),
                })?;
                Ok(events.len() as u64)
            })
        }))
    }
    fn projection_get<'a>(
        &'a self,
        spec: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        let (spec, tenant, key) = (*spec, tenant.clone(), key.to_owned());
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                projection::View {
                    admission: false,
                    tx,
                    tenant: &tenant,
                }
                .get(&spec, &tenant, &key)
                .await
            })
        }))
    }
    fn projection_find<'a>(
        &'a self,
        spec: &'a ProjectionSpec,
        tenant: &'a TenantId,
        field: &'a str,
        value: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Value>, EventLogError>> {
        let (spec, tenant, field, value) =
            (*spec, tenant.clone(), field.to_owned(), value.to_owned());
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                projection::View {
                    admission: false,
                    tx,
                    tenant: &tenant,
                }
                .find(&spec, &tenant, &field, &value, limit)
                .await
            })
        }))
    }
    fn projection_list<'a>(
        &'a self,
        spec: &'a ProjectionSpec,
        tenant: &'a TenantId,
        after: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(String, Value)>, EventLogError>> {
        let (spec, tenant, after) = (*spec, tenant.clone(), after.map(str::to_owned));
        Box::pin(self.transaction(move |tx| {
            Box::pin(async move {
                projection::View {
                    admission: false,
                    tx,
                    tenant: &tenant,
                }
                .validate(&spec, &tenant)?;
                Ok(tx
                    .state
                    .rows
                    .iter()
                    .filter(|((t, n, k), _)| {
                        t == tenant.as_str()
                            && n == spec.name
                            && after.as_ref().is_none_or(|a| k > a)
                    })
                    .take(bounded_limit(limit))
                    .map(|((_, _, key), body)| (key.clone(), body.clone()))
                    .collect())
            })
        }))
    }
}
