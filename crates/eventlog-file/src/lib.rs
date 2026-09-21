#![forbid(unsafe_code)]
//! Repository-local Eventlog. JSONL transactions are authoritative; snapshots are disposable.
mod capture;
#[cfg(test)]
mod cost;
mod inline_admin;
mod journal;
mod projection;
mod state;

pub use capture::FileTenantCapture;

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
    /// The history behind `observed`, already verified by this handle. Present only while every
    /// frame in it has been chained and every object its fold binds has been hashed, so a later
    /// transaction may reuse it and pay only for what the file gained since.
    verified: Option<Verified>,
    /// The same history as a *reader* verified it. A separate slot, and a separate type that a
    /// transaction cannot accept, because a capture makes the weaker promise: see
    /// [`capture::Observed`]. Keeping it here rather than in `verified` also means a read never
    /// costs a writer the view its open paid for.
    captured: Option<capture::Observed>,
}
/// One handle's verified view of the committed history, carrying the head it was folded from.
///
/// The head travels with the view because `runtime.observed` is advanced from five places
/// (`transaction`, `register_inline`, `attach_inline_existing`, `rebuild_inline_projection` and
/// the capture entry point) and only one of them folds the frames it advances over. A cache that
/// had to be invalidated by hand at each of the others would be one edit away from serving a stale
/// fold; instead the view is used only when its own manifest is still the observed head, so any
/// site that moves that head — today's or tomorrow's — retires the view by moving it.
struct Verified {
    manifest: journal::Manifest,
    transactions: Vec<Value>,
    state: State,
    /// The hash of the committed bytes these frames were decoded from. A resumed transaction
    /// re-reads exactly those bytes and refuses to reuse the view when they are not them, so a
    /// committed frame damaged in place after `open` is never served and never appended onto.
    content: journal::Content,
}
struct Transaction {
    journal: Journal,
    state: State,
    pending: Vec<Op>,
    inline: Vec<Arc<dyn Projector>>,
    root: PathBuf,
    privacy: bool,
    permit: AdmissionPermit,
    /// Object files this transaction created, by object id, whether or not it goes on to commit
    /// them. A transaction that refuses has already applied its own `Op::Blob`s to `state`, so
    /// `clean_blobs` would read those objects as live; this is the list that says otherwise.
    written: Vec<String>,
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
        Self::open_with_creation(path.as_ref(), true).await
    }

    /// Open an already provisioned store without creating a missing root, lock or manifest.
    /// A pending committed intent is still recovered as part of ordinary store opening.
    /// # Errors
    /// Refuses missing or corrupt authority, unknown formats and inaccessible storage.
    pub async fn open_existing(path: impl AsRef<Path>) -> Result<Self, EventLogError> {
        Self::open_with_creation(path.as_ref(), false).await
    }

    async fn open_with_creation(path: &Path, create: bool) -> Result<Self, EventLogError> {
        let root = root_path(path)?;
        let path = root.clone();
        let journal = blocking(move || {
            if create {
                Journal::open(&path)
            } else {
                Journal::open_existing(&path)
            }
        })
        .await?;
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
        // The cache starts empty, so this transaction is the complete one: it rereads and chains
        // the whole history, hashes every active object and disposes of what nothing references.
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
        let observed = runtime.observed.clone();
        // Taken, not borrowed: a transaction that refuses leaves no cache behind, so the next one
        // reverifies everything rather than trusting a view assembled beside a refusal. Filtered,
        // because another entry point may have moved the observed head past what this view folded.
        let verified = runtime
            .verified
            .take()
            .filter(|verified| Some(&verified.manifest) == observed.as_ref());
        let inline = runtime.inline.clone();
        let permit = self.permit.clone();
        let mut tx =
            blocking(move || enter(&path, observed.as_ref(), verified, inline, permit)).await?;
        let outcome = work(&mut tx).await;
        let result = match outcome {
            Ok(result) => result,
            Err(refusal) => {
                // A refusal after the batch is on disk leaves objects no frame will ever
                // reference. Dispose of them here rather than leaving them for whatever writes to
                // this store next; a disposal that itself fails is reported over the refusal,
                // because it means the store is not writable.
                blocking(move || tx.discard_written()).await?;
                return Err(refusal);
            }
        };
        let (manifest, transactions, state, cacheable, content) = blocking(move || {
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
                #[cfg(test)]
                let group_pending = tx.pending.iter().any(|op| matches!(op, Op::Group { .. }));
                #[cfg(test)]
                if group_pending {
                    journal::checkpoint("group-precommit");
                }
                tx.journal
                    .append(serde_json::to_value(&tx.pending).map_err(backend)?)?;
                #[cfg(test)]
                if group_pending {
                    journal::checkpoint("group-postcommit");
                }
            }
            // Disposal belongs to the writer that changed what is referenced.
            let committed = tx.privacy || !tx.pending.is_empty();
            if committed {
                tx.clean_blobs()?;
            }
            // A privacy rewrite mints a new epoch over replaced bytes. Reverify it from scratch.
            let cacheable = !tx.privacy;
            let state = tx.state;
            let (manifest, transactions, content) = tx.journal.into_parts();
            Ok((manifest, transactions, state, cacheable, content))
        })
        .await?;
        runtime.observed = Some(manifest.clone());
        if cacheable {
            runtime.verified = Some(Verified {
                manifest,
                transactions,
                state,
                content,
            });
        }
        Ok(result)
    }
    /// Commit an append group together with the blobs it binds, under the group's one barrier.
    ///
    /// The blobs are written, synchronized and bound inside the group's own transaction, so a
    /// batch of any size costs the frame the group already commits and no barrier besides. A
    /// caller that instead calls [`EventStore::put_blob`] once per blob pays one barrier per
    /// blob, which is what this exists to remove; the bindings and the bytes are the same either
    /// way. A deduplicated retry binds nothing, because the original commit already bound it —
    /// and [`Transaction::verify_bound`] checks that rather than assuming it, because the group's
    /// identity does not cover the batch.
    ///
    /// [`AtomicEventStore::append_group_guarded_with_blobs`] is this under an admission guard.
    ///
    /// # Errors
    /// Refuses whatever the group and each blob refuse on their own paths, and publishes nothing
    /// — not the group and not a blob — when either refuses. Refuses a retry under a committed
    /// key whose batch the committed history does not already carry.
    pub async fn append_group_with_blobs(
        &self,
        group: &AppendGroup,
        blobs: &[(String, Vec<u8>)],
    ) -> Result<AppendGroupResult, EventLogError> {
        self.group(group, Arc::new(NoGuard), blobs).await
    }
    async fn group(
        &self,
        group: &AppendGroup,
        guard: Arc<dyn Guard>,
        blobs: &[(String, Vec<u8>)],
    ) -> Result<AppendGroupResult, EventLogError> {
        let digest = group.fingerprint()?;
        let group = group.clone();
        let blobs = blobs.to_vec();
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
                    // The group's fingerprint hashes the tenant, the members and the command meta,
                    // and deliberately not the batch — moving it would move every provider's
                    // digests. So the batch is checked against the history instead of against the
                    // key: a deduplicating `Ok` says the batch is durable, and it may only be
                    // returned when the committed frame really does bind every digest of it to
                    // these bytes. A retry holding anything else is not this request.
                    tx.verify_bound(&group.tenant, &blobs, &group.meta.idempotency_key)?;
                    return Ok(AppendGroupResult {
                        appends: ranges.iter().map(|r| tx.result(r, true)).collect(),
                        deduplicated: true,
                    });
                }
                guard
                    .check(&mut projection::View {
                        admission: true,
                        selected: None,
                        tx,
                        tenant: &group.tenant,
                    })
                    .await?;
                #[cfg(test)]
                journal::checkpoint("group-admitted");
                tx.bind_blobs(&group.tenant, &blobs).await?;
                #[cfg(test)]
                if !blobs.is_empty() {
                    journal::checkpoint("group-blobs-written");
                }
                let mut appends = Vec::new();
                let mut ranges = Vec::new();
                #[cfg(test)]
                let mut member = 0;
                for entry in &group.appends {
                    let result = tx
                        .append(&entry.stream, entry.expected, &entry.events, &group.meta)
                        .await?;
                    #[cfg(test)]
                    {
                        member += 1;
                        journal::checkpoint(&format!("group-member-{member}"));
                    }
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
                #[cfg(test)]
                journal::checkpoint("group-bookkeeping");
                Ok(AppendGroupResult {
                    appends,
                    deduplicated: false,
                })
            })
        })
        .await
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
                    selected: None,
                    tx: self,
                    tenant: stream.tenant(),
                }
                .validate(spec, stream.tenant())?;
            }
            for event in &written {
                let mut projection = projection::View {
                    admission: false,
                    selected: None,
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
        #[cfg(test)]
        cost::charge(&self.root, |cost| {
            cost.blobs_hashed += 1;
        });
        if hash(&bytes) != blob.hash {
            return Err(backend("referenced blob integrity check failed"));
        }
        Ok(Some(bytes))
    }
    /// Verify the committed history already binds every digest of a batch to exactly these bytes.
    ///
    /// What a deduplicating return means. [`AppendGroup::fingerprint`] identifies a group by its
    /// tenant, members and command meta and not by the batch it carries, so a second call under a
    /// committed key is recognized as a retry however the batch differs — and answering it `Ok`
    /// is a claim that the batch is durable which the key alone cannot support. This is that
    /// claim, checked: a digest bound to these bytes is the retry the mechanism exists to serve,
    /// a digest bound to different bytes is the same contradiction [`Transaction::bind_blobs`]
    /// refuses on the fresh path, and a digest the history does not carry at all means this is
    /// not the request that committed.
    ///
    /// # Errors
    /// [`EventLogError::Invalid`] for a digest bound to different content or an unusable digest,
    /// and [`EventLogError::IdempotencyMismatch`] for a digest no committed frame binds.
    fn verify_bound(
        &self,
        tenant: &TenantId,
        blobs: &[(String, Vec<u8>)],
        key: &str,
    ) -> Result<(), EventLogError> {
        for (digest, bytes) in blobs {
            validate_field("blob digest", digest)?;
            match self.blob(tenant, digest)? {
                Some(bound) if &bound == bytes => {}
                Some(_) => {
                    return Err(EventLogError::Invalid(
                        "blob digest already names different content".into(),
                    ));
                }
                None => {
                    return Err(EventLogError::IdempotencyMismatch {
                        key: key.to_owned(),
                    });
                }
            }
        }
        Ok(())
    }
    /// Write, synchronize and bind every blob of one batch inside this transaction.
    ///
    /// The batch takes **no barrier of its own**. Each object file is written and synchronized,
    /// the object directory is synchronized once for the whole batch, and the bindings are
    /// recorded as pending operations — so what publishes them is the single frame this
    /// transaction commits, in the ordering [`Journal::append`] already enforces: every object is
    /// durable, and its directory entry is durable, before the manifest that names the frame
    /// binding it. A crash anywhere before that manifest leaves object files no committed frame
    /// references, and [`Transaction::clean_blobs`] and the complete opener dispose of them.
    ///
    /// That is the whole difference from a blob per transaction: the barrier count is the number
    /// of frames, and one group is one frame however many blobs it binds.
    async fn bind_blobs(
        &mut self,
        tenant: &TenantId,
        blobs: &[(String, Vec<u8>)],
    ) -> Result<(), EventLogError> {
        let directory = self.root.join("blobs");
        let mut fresh: Vec<(Blob, String)> = Vec::new();
        let mut objects: Vec<(String, PathBuf, Vec<u8>)> = Vec::new();
        for (digest, bytes) in blobs {
            validate_field("blob digest", digest)?;
            // A digest repeated inside one batch binds once, and disagrees with itself exactly as
            // it would disagree with a binding the committed history already carries.
            if let Some((earlier, _)) = fresh.iter().find(|(_, name)| name == digest) {
                if earlier.hash != hash(bytes) {
                    return Err(EventLogError::Invalid(
                        "blob digest already names different content".into(),
                    ));
                }
                continue;
            }
            if let Some(old) = self.blob(tenant, digest)? {
                if &old != bytes {
                    return Err(EventLogError::Invalid(
                        "blob digest already names different content".into(),
                    ));
                }
                continue;
            }
            let blob = Blob {
                id: new_event_id(),
                hash: hash(bytes),
            };
            objects.push((blob.id.clone(), directory.join(&blob.id), bytes.clone()));
            fresh.push((blob, digest.clone()));
        }
        if fresh.is_empty() {
            return Ok(());
        }
        fs::create_dir_all(&directory).map_err(backend)?;
        self::directory(&directory)?;
        // Every object this call creates, recorded as it is created rather than before or after
        // the loop, so the disposal list is exactly what exists: a batch that fails halfway
        // leaves the objects it did create on the list, and an object the loop never created is
        // never on it. Disposal has to go by object identity and not by what `state` references,
        // because a transaction that refuses has already applied its own `Op::Blob`s to `state`
        // and those objects would read as live.
        let created: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let recorder = Arc::clone(&created);
        let outcome = blocking(move || {
            use std::io::Write;
            // Counted where the synchronizations happen, not derived from the batch size: a
            // counter computed from `objects.len()` reads the same whether the objects were
            // synchronized or not, which is the one thing it exists to say.
            let mut synchronized = 0_u64;
            for (id, path, bytes) in &objects {
                let mut file = fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .map_err(backend)?;
                recorder
                    .lock()
                    .map_err(|_| backend("blob object recorder poisoned"))?
                    .push(id.clone());
                file.write_all(bytes)
                    .and_then(|()| file.sync_all())
                    .map_err(backend)?;
                synchronized += 1;
            }
            fs::File::open(directory)
                .and_then(|f| f.sync_all())
                .map_err(backend)?;
            synchronized += 1;
            Ok::<_, EventLogError>(synchronized)
        })
        .await;
        self.written.extend(
            created
                .lock()
                .map_err(|_| backend("blob object recorder poisoned"))?
                .drain(..),
        );
        let synchronized = outcome?;
        #[cfg(test)]
        cost::charge(&self.root, |cost| {
            cost.object_syncs += synchronized;
        });
        #[cfg(not(test))]
        let _ = synchronized;
        for (blob, digest) in fresh {
            self.record(Op::Blob {
                tenant: tenant.clone(),
                digest,
                object: Some(blob),
            })?;
        }
        Ok(())
    }
    /// Remove the object files this transaction wrote, for a transaction that will not commit.
    ///
    /// The refusal path needs its own disposer. Before this provider bound blobs inside a group,
    /// an object was written in a transaction with nothing after it, so nothing could refuse
    /// between the write and the commit; now a member append, an inline projector or an
    /// optimistic-concurrency conflict can all refuse after the batch is on disk. The two
    /// disposers the design names — the next committed transaction and the complete opener — both
    /// still apply and both run later, so without this a refused batch of N objects is N files
    /// occupying the store until something else writes to it.
    ///
    /// Driven by object identity rather than by `state`, because a refused transaction's `state`
    /// already carries the rolled-back bindings and would report these very objects as live.
    fn discard_written(&self) -> Result<(), EventLogError> {
        if self.written.is_empty() {
            return Ok(());
        }
        let directory = self.root.join("blobs");
        if !directory.exists() {
            return Ok(());
        }
        self::directory(&directory)?;
        for object in &self.written {
            validate_object(object)?;
            let path = directory.join(object);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(backend(error)),
            }
        }
        fs::File::open(directory)
            .and_then(|f| f.sync_all())
            .map_err(backend)
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
                        selected: None,
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
/// Take the process lock and produce the transaction's journal and folded state.
///
/// With a verified view of `observed` in hand, [`Journal::resume`] compares `manifest.json` with
/// it and re-hashes the committed bytes behind it: identical history costs the lock, that
/// comparison and one raw pass over the committed prefix; an extended history costs the same pass
/// plus the frames past the observed length, chained from the observed digest, folded onto the
/// cached state, and the objects those frames bind. Anything else — no cached view, a pending
/// recovery intent, a new epoch, a shorter or unchained file, or a committed prefix that is no
/// longer the bytes this handle verified — falls to the complete opener, which rereads and
/// rechains the whole history, refuses one that does not extend what this handle observed, hashes
/// every active object and disposes of snapshots and objects nothing references.
///
/// The prefix hash is what makes the resumed path safe to *write* from: `Journal::append` seeks to
/// the committed length and writes, so a transaction that were handed append authority over a
/// prefix nobody re-read could commit a valid frame after a damaged one and leave a history no
/// opener accepts. Every resumed transaction passes that check before this function returns.
fn enter(
    root: &Path,
    observed: Option<&journal::Manifest>,
    verified: Option<Verified>,
    inline: Vec<Arc<dyn Projector>>,
    permit: AdmissionPermit,
) -> Result<Transaction, EventLogError> {
    if let (Some(observed), Some(verified)) = (observed, verified)
        && let Some(resumed) = Journal::resume(root, observed, &verified.content)?
    {
        let mut state = verified.state;
        let mut bound = Vec::new();
        for transaction in &resumed.fresh {
            bound.extend(state.fold(transaction)?);
        }
        let advanced = !resumed.fresh.is_empty();
        let tx = Transaction {
            journal: resumed.into_journal(root, verified.transactions),
            state,
            pending: Vec::new(),
            inline,
            root: root.to_owned(),
            privacy: false,
            permit,
            written: Vec::new(),
        };
        // Only the objects the new frames bind are new to this handle; the rest it already hashed.
        for (tenant, digest) in &bound {
            tx.blob(tenant, digest)?;
        }
        if advanced {
            // Another writer's frames can retire a generation this handle still has cached.
            tx.clear_snapshots()?;
        }
        return Ok(tx);
    }
    let journal = Journal::open_existing(root)?;
    if let Some(observed) = observed
        && !journal.extends(observed)?
    {
        return Err(backend(
            "file history diverged from this handle's observed history",
        ));
    }
    let tx = Transaction {
        state: State::replay(&journal.transactions)?,
        journal,
        pending: Vec::new(),
        inline,
        root: root.to_owned(),
        privacy: false,
        permit,
        written: Vec::new(),
    };
    tx.clear_snapshots()?;
    tx.clean_blobs()?;
    for (tenant, digest) in tx.state.blobs.keys() {
        tx.blob(&TenantId::new(tenant)?, digest)?;
    }
    Ok(tx)
}

/// One store root, resolved the same way for the ordinary opener and the read-only handle.
fn root_path(path: &Path) -> Result<PathBuf, EventLogError> {
    if path.is_absolute() {
        return Ok(path.to_owned());
    }
    Ok(std::env::current_dir().map_err(backend)?.join(path))
}

fn validate_object(id: &str) -> Result<(), EventLogError> {
    if id.len() != 36 || !id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        return Err(backend("invalid blob object name"));
    }
    Ok(())
}

fn directory(path: &Path) -> Result<(), EventLogError> {
    if !journal::physical_directory(path).map_err(backend)? {
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
        Box::pin(self.group(group, guard, &[]))
    }
    /// The batch takes the group's one barrier rather than one each, and the guard runs before a
    /// byte of it is written, so a refused guard publishes neither the group nor a blob.
    fn append_group_guarded_with_blobs<'a>(
        &'a self,
        group: &'a AppendGroup,
        guard: Arc<dyn Guard>,
        blobs: &'a [(String, Vec<u8>)],
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        Box::pin(self.group(group, guard, blobs))
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
                            selected: None,
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
            Box::pin(async move { tx.bind_blobs(&tenant, &[(digest, bytes)]).await })
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
                written: Vec::new(),
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
                                selected: None,
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
                    selected: None,
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
                    selected: None,
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
                    selected: None,
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

#[cfg(test)]
mod native_group_crash {
    use super::*;
    use eventlog_conformance::{TALLY, Tally, event, meta};
    use eventlog_core::StreamAppend;
    use std::process::Command;

    fn tenant() -> TenantId {
        TenantId::new("native-crash").unwrap()
    }

    fn stream(id: &str) -> StreamId {
        StreamId::new(tenant(), "item", id).unwrap()
    }

    fn group() -> AppendGroup {
        AppendGroup {
            tenant: tenant(),
            meta: meta("native-group", &serde_json::json!({})),
            appends: ["a", "z", "a"]
                .into_iter()
                .map(|id| StreamAppend {
                    stream: stream(id),
                    expected: Expected::Any,
                    events: vec![event("item.changed", 1)],
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn child() {
        let Ok(root) = std::env::var("EVENTLOG_FILE_GROUP_CHILD_ROOT") else {
            return;
        };
        let store = FileEventStore::open(root).await.unwrap();
        store.register_inline(Arc::new(Tally)).await.unwrap();
        store.append_group(&group()).await.unwrap();
    }

    #[tokio::test]
    async fn every_native_group_boundary_recovers_one_complete_outcome() {
        for (point, committed) in [
            ("group-member-1", false),
            ("group-member-2", false),
            ("group-member-3", false),
            ("group-bookkeeping", false),
            ("group-precommit", false),
            ("append-prepared", false),
            ("append-torn", false),
            ("append-synced", false),
            ("append-committed", true),
            ("group-postcommit", true),
        ] {
            let root = tempfile::tempdir().unwrap();
            let prepared = FileEventStore::open(root.path()).await.unwrap();
            prepared.register_inline(Arc::new(Tally)).await.unwrap();
            drop(prepared);
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "native_group_crash::child", "--nocapture"])
                .env("EVENTLOG_FILE_GROUP_CHILD_ROOT", root.path())
                .env("EVENTLOG_FILE_CRASH_AT", point)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            let store = FileEventStore::open(root.path()).await.unwrap();
            let before = store.read_feed(&tenant(), 0, 10).await.unwrap().events;
            assert_eq!(before.len(), if committed { 3 } else { 0 }, "{point}");
            for (id, count) in [("a", 2), ("z", 1)] {
                assert_eq!(
                    store.stream_version(&stream(id)).await.unwrap(),
                    committed.then_some(count),
                    "{point}"
                );
                let row = store
                    .projection_get(&TALLY, &tenant(), &format!("item/{id}"))
                    .await
                    .unwrap();
                assert_eq!(
                    row.and_then(|value| value["count"].as_u64()),
                    committed.then_some(count),
                    "{point}"
                );
            }

            store.register_inline(Arc::new(Tally)).await.unwrap();
            let retry = store
                .append_group(&group())
                .await
                .unwrap_or_else(|error| panic!("{point}: retry refused after reopen: {error}"));
            assert_eq!(retry.deduplicated, committed, "{point}");
            assert_eq!(retry.appends.len(), 3, "{point}");
            assert_eq!(
                retry
                    .appends
                    .iter()
                    .map(|append| (append.first_version, append.last_version))
                    .collect::<Vec<_>>(),
                [(1, 1), (1, 1), (2, 2)],
                "{point}: original repeated-stream ranges"
            );
            let received: Vec<_> = retry
                .appends
                .iter()
                .flat_map(|append| append.events.iter().cloned())
                .collect();
            assert_eq!(received.len(), 3, "{point}");
            if committed {
                assert_eq!(received, before, "{point}: original event coordinates");
            } else {
                assert_eq!(received[0].global_seq, 1, "{point}: fresh retry");
            }
            assert_eq!(
                store.read_feed(&tenant(), 0, 10).await.unwrap().events,
                received,
                "{point}: no duplicate or missing group member"
            );
            for (id, count) in [("a", 2), ("z", 1)] {
                let row = store
                    .projection_get(&TALLY, &tenant(), &format!("item/{id}"))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(row["count"].as_u64(), Some(count), "{point}");
            }
            drop(store);
            let reopened = FileEventStore::open(root.path()).await.unwrap();
            let second_retry = reopened.append_group(&group()).await.unwrap();
            assert!(
                second_retry.deduplicated,
                "{point}: retry after recovery reopen"
            );
            assert_eq!(second_retry.appends.len(), retry.appends.len(), "{point}");
            for (original, again) in retry.appends.iter().zip(&second_retry.appends) {
                assert_eq!(again.first_version, original.first_version, "{point}");
                assert_eq!(again.last_version, original.last_version, "{point}");
                assert_eq!(again.events, original.events, "{point}");
                assert!(again.deduplicated, "{point}");
            }
        }
    }
}

/// Cases for `story:grouped-blob-writes-take-one-durability-barrier`.
///
/// **What the crash cases here can and cannot prove.** They are process-death cases: the child
/// calls `std::process::exit(73)` at a named checkpoint, which does not drop the page cache, so a
/// write the operating system has accepted but not yet put on the device still survives the death
/// and is still read back by the parent. Nothing here can therefore detect a **dropped** `fsync` —
/// remove one and every case stays green. What they do prove is barrier **placement** and binding:
/// which writes have happened at a named point, what a reopen recovers from that state, and that
/// the group and its blobs are present together or absent together. Barrier *presence* needs a
/// power-loss harness, which this tree does not have.
#[cfg(test)]
mod grouped_blob_barrier {
    use super::*;
    use eventlog_conformance::{event, meta};
    use eventlog_core::{CaptureLimits, ConsistentTenantCapture, StreamAppend};
    use std::process::Command;

    fn tenant() -> TenantId {
        TenantId::new("grouped-blob").unwrap()
    }

    fn limits() -> CaptureLimits {
        CaptureLimits {
            max_events: 64,
            max_blobs: 64,
            max_projection_rows: 64,
            max_payload_bytes: 65_536,
        }
    }

    fn stream(id: &str) -> StreamId {
        StreamId::new(tenant(), "item", id).unwrap()
    }

    /// `count` blobs whose digests and contents are fixed, so two stores written by two paths
    /// bind the same names to the same bytes.
    fn blobs(count: usize) -> Vec<(String, Vec<u8>)> {
        (0..count)
            .map(|index| {
                (
                    format!("d{index:04}"),
                    format!("bytes-{index}").into_bytes(),
                )
            })
            .collect()
    }

    fn group(key: &str, count: usize) -> AppendGroup {
        AppendGroup {
            tenant: tenant(),
            meta: meta(key, &serde_json::json!({})),
            appends: (0..count)
                .map(|index| StreamAppend {
                    stream: stream(&format!("s{index}")),
                    expected: Expected::Any,
                    events: vec![event("item.changed", 1)],
                })
                .collect(),
        }
    }

    /// The acceptance statement: N blobs written inside one append group cost the group's one
    /// durability barrier, not one per blob.
    ///
    /// The comparison is against the same store doing the same work the way a caller has to do it
    /// today — `put_blob` per blob and then the group — because an absolute barrier count means
    /// nothing without the count it replaces.
    #[tokio::test]
    async fn blobs_inside_one_group_take_the_groups_single_barrier() {
        for count in [1_usize, 8, 32] {
            let separate = tempfile::tempdir().unwrap();
            let store = FileEventStore::open(separate.path()).await.unwrap();
            let before = cost::of(separate.path());
            for (digest, bytes) in blobs(count) {
                store.put_blob(&tenant(), &digest, &bytes).await.unwrap();
            }
            store.append_group(&group("separate", count)).await.unwrap();
            let apart = cost::of(separate.path()) - before;

            let together = tempfile::tempdir().unwrap();
            let store = FileEventStore::open(together.path()).await.unwrap();
            let before = cost::of(together.path());
            store
                .append_group_with_blobs(&group("together", count), &blobs(count))
                .await
                .unwrap();
            let joined = cost::of(together.path()) - before;

            println!("BARRIERS count={count} apart={apart:?} joined={joined:?}");
            assert_eq!(
                apart.durability_barriers,
                count as u64 + 1,
                "the cost this story exists to remove: one barrier per blob plus the group's"
            );
            assert_eq!(
                joined.durability_barriers, 1,
                "{count} blobs inside one group take the group's one barrier"
            );
            assert_eq!(
                apart.object_syncs,
                2 * count as u64,
                "a blob per transaction synchronizes the object directory once per blob"
            );
            assert_eq!(
                joined.object_syncs,
                count as u64 + 1,
                "the batch synchronizes each object once and their directory once for all of them"
            );
        }
    }

    /// Byte identity: a store written one blob per transaction and a store written through the
    /// group bind the same digests to the same bytes and bind nothing else.
    ///
    /// The comparison is the provider's own complete-content observation — every currently bound
    /// blob, once, in bytewise digest order, orphans included — because a per-digest read only
    /// proves that what was asked for is there, and the claim the migration rests on is that the
    /// faster path writes the same content and no other.
    #[tokio::test]
    async fn both_paths_bind_the_same_digests_to_the_same_bytes() {
        let count = 8;
        let apart = tempfile::tempdir().unwrap();
        let store = FileEventStore::open(apart.path()).await.unwrap();
        store.stream_identity(&tenant()).await.unwrap();
        for (digest, bytes) in blobs(count) {
            store.put_blob(&tenant(), &digest, &bytes).await.unwrap();
        }
        store.append_group(&group("identity", count)).await.unwrap();
        let one = store
            .capture_tenant(&tenant(), &[], limits())
            .await
            .unwrap()
            .blobs;

        let joined = tempfile::tempdir().unwrap();
        let store = FileEventStore::open(joined.path()).await.unwrap();
        store.stream_identity(&tenant()).await.unwrap();
        store
            .append_group_with_blobs(&group("identity", count), &blobs(count))
            .await
            .unwrap();
        let two = store
            .capture_tenant(&tenant(), &[], limits())
            .await
            .unwrap()
            .blobs;

        assert_eq!(
            one, two,
            "the same digests name the same bytes on both paths, and no others are bound"
        );
        assert_eq!(
            one.len(),
            count,
            "the fixture only means something when every blob is bound: {one:?}"
        );
        for (digest, bytes) in blobs(count) {
            assert_eq!(
                store.get_blob(&tenant(), &digest).await.unwrap(),
                Some(bytes),
                "{digest} reads back as itself through the ordinary port too"
            );
        }
    }

    /// The refusals `put_blob` makes are the refusals the batch makes, and a refused batch
    /// publishes nothing at all — not the group, and not the blobs that were valid.
    #[tokio::test]
    async fn a_refused_blob_refuses_the_whole_group() {
        let root = tempfile::tempdir().unwrap();
        let store = FileEventStore::open(root.path()).await.unwrap();
        store.put_blob(&tenant(), "d0000", b"first").await.unwrap();

        let conflict = store
            .append_group_with_blobs(
                &group("conflict", 2),
                &[
                    ("d0000".to_owned(), b"second".to_vec()),
                    ("d0001".to_owned(), b"fresh".to_vec()),
                ],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(conflict, EventLogError::Invalid(ref message)
                if message.contains("already names different content")),
            "conflicting bytes under a bound digest refuse as they do on the single path: {conflict:?}"
        );
        assert_eq!(
            store.get_blob(&tenant(), "d0000").await.unwrap(),
            Some(b"first".to_vec()),
            "the bound blob is untouched"
        );
        assert_eq!(
            store.get_blob(&tenant(), "d0001").await.unwrap(),
            None,
            "a refused batch binds none of its blobs"
        );
        assert_eq!(
            store.stream_version(&stream("s0")).await.unwrap(),
            None,
            "a refused batch appends none of its group"
        );

        let invalid = store
            .append_group_with_blobs(&group("invalid", 1), &[(String::new(), b"x".to_vec())])
            .await
            .unwrap_err();
        assert!(
            matches!(invalid, EventLogError::Invalid(_)),
            "an invalid digest refuses as it does on the single path: {invalid:?}"
        );
    }

    /// The workload a wall-clock profiler attaches to, in the shape `child` already uses: an
    /// ordinary case that does nothing until the environment names a workload, so the suite gains
    /// no ignored lane and `strace -c -w` still has one process doing one thing.
    ///
    /// `EVENTLOG_FILE_BLOB_MEASURE` is `apart` (a transaction per blob, then the group),
    /// `joined` (the blobs inside the group) or `guarded` (the blobs inside a guarded group, the
    /// shape an importer commits in); `EVENTLOG_FILE_BLOB_MEASURE_COUNT` is how many blobs and
    /// `EVENTLOG_FILE_BLOB_MEASURE_MEMBERS` how many streams the group appends to.
    /// Measure with `-w`: `fsync` waits on the device and burns no CPU, so bare `strace -c`
    /// reports it as free and is the wrong instrument for this question.
    #[tokio::test]
    async fn measure() {
        let Ok(path) = std::env::var("EVENTLOG_FILE_BLOB_MEASURE") else {
            return;
        };
        let count: usize = std::env::var("EVENTLOG_FILE_BLOB_MEASURE_COUNT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(64);
        let members: usize = std::env::var("EVENTLOG_FILE_BLOB_MEASURE_MEMBERS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1);
        let root = tempfile::tempdir().unwrap();
        let store = FileEventStore::open(root.path()).await.unwrap();
        match path.as_str() {
            "apart" => {
                for (digest, bytes) in blobs(count) {
                    store.put_blob(&tenant(), &digest, &bytes).await.unwrap();
                }
                store
                    .append_group_guarded(&group("measure", members), Arc::new(NoGuard))
                    .await
                    .unwrap();
            }
            "joined" => {
                store
                    .append_group_with_blobs(&group("measure", members), &blobs(count))
                    .await
                    .unwrap();
            }
            "guarded" => {
                store
                    .append_group_guarded_with_blobs(
                        &group("measure", members),
                        Arc::new(NoGuard),
                        &blobs(count),
                    )
                    .await
                    .unwrap();
            }
            other => panic!("unknown workload {other}"),
        }
        for (digest, bytes) in blobs(count) {
            assert_eq!(
                store.get_blob(&tenant(), &digest).await.unwrap(),
                Some(bytes),
                "the measured workload actually bound its blobs"
            );
        }
    }

    #[tokio::test]
    async fn child() {
        let Ok(root) = std::env::var("EVENTLOG_FILE_BLOB_GROUP_CHILD_ROOT") else {
            return;
        };
        let store = FileEventStore::open(root).await.unwrap();
        store
            .append_group_with_blobs(&group("crash", 3), &blobs(4))
            .await
            .unwrap();
    }

    /// Crash consistency for the joined path, at every boundary the joined path has.
    ///
    /// `group-blobs-written` is the boundary this story adds: the object files are on disk and
    /// synchronized, and the frame that binds them is not published. Nothing of the group may be
    /// observable there — not an event, and not a blob — and the unreferenced objects must not
    /// survive as bound content. `group-admitted` is its pair, above the same write, and the two
    /// together are what hold each checkpoint to the state it names; see the module's note on
    /// what a process-death case can and cannot prove.
    #[tokio::test]
    async fn every_grouped_blob_boundary_recovers_one_complete_outcome() {
        for (point, committed) in [
            ("group-admitted", false),
            ("group-blobs-written", false),
            ("group-member-1", false),
            ("group-bookkeeping", false),
            ("group-precommit", false),
            ("append-prepared", false),
            ("append-torn", false),
            ("append-synced", false),
            ("append-committed", true),
            ("group-postcommit", true),
        ] {
            let root = tempfile::tempdir().unwrap();
            let prepared = FileEventStore::open(root.path()).await.unwrap();
            drop(prepared);
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "grouped_blob_barrier::child", "--nocapture"])
                .env("EVENTLOG_FILE_BLOB_GROUP_CHILD_ROOT", root.path())
                .env("EVENTLOG_FILE_CRASH_AT", point)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            // Where each checkpoint is, stated as the state on disk at it rather than as the line
            // it sits on. `group-admitted` is above `bind_blobs` and `group-blobs-written` is
            // below it, so the batch's objects are absent at the first and written and
            // synchronized at the second. Without this pair either checkpoint may be moved to the
            // other side of the write it names and every other assertion here stays green. Read
            // before the store is reopened, because opening is what disposes of unreferenced
            // objects.
            let written = fs::read_dir(root.path().join("blobs")).map_or(0, Iterator::count);
            assert_eq!(
                written,
                if point == "group-admitted" { 0 } else { 4 },
                "{point}: the batch's objects are on disk from group-blobs-written onwards, \
                 and not before it"
            );

            let store = FileEventStore::open(root.path()).await.unwrap();
            let events = store.read_feed(&tenant(), 0, 10).await.unwrap().events;
            assert_eq!(
                events.len(),
                if committed { 3 } else { 0 },
                "{point}: the group is all present or all absent"
            );
            for (digest, bytes) in blobs(4) {
                assert_eq!(
                    store.get_blob(&tenant(), &digest).await.unwrap(),
                    committed.then_some(bytes),
                    "{point}: a blob is durable exactly when the group that binds it is"
                );
            }
            // An interrupted batch leaves object files no committed frame names. They are not
            // content: the store disposes of them rather than serving them.
            let objects = fs::read_dir(root.path().join("blobs")).map_or(0, Iterator::count);
            assert_eq!(
                objects,
                if committed { 4 } else { 0 },
                "{point}: unreferenced objects from an interrupted batch are disposed of"
            );

            let retry = store
                .append_group_with_blobs(&group("crash", 3), &blobs(4))
                .await
                .unwrap_or_else(|error| panic!("{point}: retry refused after reopen: {error}"));
            assert_eq!(retry.deduplicated, committed, "{point}");
            assert_eq!(retry.appends.len(), 3, "{point}");
            for (digest, bytes) in blobs(4) {
                assert_eq!(
                    store.get_blob(&tenant(), &digest).await.unwrap(),
                    Some(bytes),
                    "{point}: the retry binds every blob"
                );
            }
            assert_eq!(
                store
                    .read_feed(&tenant(), 0, 10)
                    .await
                    .unwrap()
                    .events
                    .len(),
                3,
                "{point}: no duplicate or missing group member"
            );
        }
    }
}
