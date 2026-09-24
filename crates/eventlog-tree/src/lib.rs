//! A file event store whose history merges under version control.
//!
//! The store is a directory of immutable files:
//!
//! ```text
//! <root>/store.json                                     {format, identity}, written once
//! <root>/tenants/<tenant>/identity.json                 the tenant's stream identity, written once
//! <root>/tenants/<tenant>/streams/<type>/<id>/<digest>.json    one event per file
//! <root>/tenants/<tenant>/groups/<kk>/<key-digest>.json        one command per file: the commit point
//! <root>/tenants/<tenant>/blobs/<kk>/<digest>           content-addressed bytes
//! <root>/.lock                                          one writer per checkout; not history
//! ```
//!
//! No file describes the whole store, so two branches that wrote different streams merge with no
//! textual conflict. Two branches that wrote one stream merge too: they added different files, and
//! the stream then has two heads. A forked stream still reads, in a deterministic order, but only
//! an append with [`Expected::Merge`] over exactly its heads extends it.
//!
//! Every read and every projection is served by an in-memory `SQLite` store that the files are
//! replayed into when the store opens, in causal order. A stream's versions are its events' places
//! in that order, so they are gapless as every other provider's are; a merge can renumber the side
//! that sorts second, which is why a file never stores its version.
//!
//! What this store does not promise, in [`Capabilities::BRANCHABLE`]: positions are valid only
//! while the store is open, claims are refused, and snapshots, catch-up cursors and admission
//! counters live only in memory.

mod fold;
mod history;
mod layout;
mod verify;

pub use verify::{Finding, verify};

use eventlog_core::{
    AppendGroup, AppendGroupResult, AppendResult, AtomicBlobEventStore, AtomicEventStore,
    BlobAppendGroup, BlobWrite, BoxFuture, BranchableEventStore, Capabilities, CaptureError,
    CaptureLimits, CatchUpProgress, Claim, ClaimedCommand, CommandMeta, ConsistentTenantCapture,
    EventLogError, EventStore, Expected, FeedPage, Guard, HeadSetDigest, InlineProjectionAdmin,
    InlineRebuildResult, NewEvent, NoGuard, ProjectionPage, ProjectionSpec, Projector,
    RecordedEvent, Snapshot, SnapshotGeneration, StreamAppend, StreamId, StreamSlice,
    TenantCapture, TenantId, redaction_tombstone,
};
use eventlog_sqlite::{RestoredEvent, SqliteEventStore};
use history::{Committed, EventRecord, GroupRecord, Kind, Member};
use layout::{
    FORMAT, backend, blob_path, canonical, corrupt, event_path, group_path, identity_path,
    sha256_hex, tenant_dir, write_atomic, write_once,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

type StreamKey = (String, String, String);

fn key_of(stream: &StreamId) -> StreamKey {
    (
        stream.tenant().as_str().to_owned(),
        stream.stream_type().to_owned(),
        stream.stream_id().to_owned(),
    )
}

/// What the store knows beyond what the engine holds.
struct State {
    engine: Arc<SqliteEventStore>,
    /// Each stream's head digests. More than one means the stream has forked.
    heads: BTreeMap<StreamKey, Vec<String>>,
    /// Each committed event's file record, by event id.
    records: BTreeMap<String, EventRecord>,
    /// Each committed event's digest, by event id, so a read does not hash its records again.
    digests: BTreeMap<String, String>,
    /// The group files this state was built from.
    group_files: BTreeSet<PathBuf>,
    /// Tenants whose identity file exists.
    identities: BTreeSet<String>,
    /// Registrations to repeat when the state is rebuilt from the files.
    inline: Vec<Arc<dyn Projector>>,
    catch_up: Vec<Arc<dyn Projector>>,
    /// Set when a write reached the engine but not the files. The engine then holds history the
    /// directory does not, and nothing more may be served from it.
    poisoned: bool,
    /// Whether this state was replayed from the files rather than read back from `.cache/`.
    replayed: bool,
}

/// An [`EventStore`] over a directory that version control can merge.
pub struct TreeEventStore {
    root: PathBuf,
    state: Mutex<State>,
}

impl TreeEventStore {
    /// Open the store at `root`, creating it when the directory holds none.
    ///
    /// # Errors
    /// Refuses a `store.json` of another format, and any file the replay cannot verify: see
    /// [`TreeEventStore::torn`] for the one kind of leftover an open tolerates.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, EventLogError> {
        Self::open_with_inline(root, Vec::new()).await
    }

    /// Whether the state this store serves was replayed from its files, rather than read back from
    /// the one `.cache/` kept for exactly these files. A cost probe; the answers are the same.
    pub async fn replayed(&self) -> bool {
        self.state.lock().await.replayed
    }

    /// The store's own identity, from `store.json`.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when `store.json` cannot be read.
    pub fn identity(&self) -> Result<String, EventLogError> {
        let bytes = fs::read(self.root.join("store.json")).map_err(backend)?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| corrupt("store.json"))?;
        value
            .get("identity")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| corrupt("store.json"))
    }

    /// Event files no group commits: writes interrupted before their commit point. They are never
    /// served; [`TreeEventStore::repair`] removes them.
    ///
    /// # Errors
    /// Refuses a store the replay cannot verify.
    pub fn torn(&self) -> Result<Vec<PathBuf>, EventLogError> {
        Ok(history::load(&self.root)?.torn)
    }

    /// Delete every torn event file, under the writer lock. The only operation that removes an
    /// event file: nothing any reader was ever served is removed.
    ///
    /// # Errors
    /// Refuses a store the replay cannot verify, and reports a file that cannot be removed.
    pub async fn repair(&self) -> Result<Vec<PathBuf>, EventLogError> {
        let _state = self.state.lock().await;
        let _lock = self.writer_lock()?;
        let torn = history::load(&self.root)?.torn;
        for path in &torn {
            fs::remove_file(path).map_err(backend)?;
        }
        Ok(torn)
    }

    fn writer_lock(&self) -> Result<fs::File, EventLogError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.root.join(".lock"))
            .map_err(backend)?;
        file.lock().map_err(backend)?;
        Ok(file)
    }

    async fn engine(&self) -> Result<Arc<SqliteEventStore>, EventLogError> {
        let state = self.state.lock().await;
        if state.poisoned {
            return Err(EventLogError::Closed);
        }
        Ok(Arc::clone(&state.engine))
    }

    /// Fill in each event's digest and parents from the files.
    async fn decorate(&self, events: &mut [RecordedEvent]) {
        let state = self.state.lock().await;
        for event in events {
            if let Some(record) = state.records.get(&event.event_id) {
                event.digest = state.digests.get(&event.event_id).cloned();
                event.parents.clone_from(&record.parents);
            }
        }
    }

    /// Open the store at `root` with `inline` projectors registered before its history is
    /// replayed, so the replay runs once. [`TreeEventStore::open`] followed by
    /// [`InlineProjectionAdmin::attach_inline_existing`] replays twice.
    ///
    /// # Errors
    /// As [`TreeEventStore::open`], and whatever a projector refuses during the replay.
    pub async fn open_with_inline(
        root: impl AsRef<Path>,
        inline: Vec<Arc<dyn Projector>>,
    ) -> Result<Self, EventLogError> {
        let root = root.as_ref().to_owned();
        prepare(&root)?;
        let state = build(&root, inline, Vec::new()).await?;
        Ok(Self {
            root,
            state: Mutex::new(state),
        })
    }

    /// Append through the engine and, when it wrote, commit the same facts to the files.
    ///
    /// `members` names each stream and what its caller expected; `call` receives what the engine
    /// is to expect instead, which differs only for a merge.
    async fn commit<'a, F>(
        &'a self,
        kind: Kind,
        tenant: &TenantId,
        members: &[(StreamId, Expected)],
        meta: &CommandMeta,
        blobs: &[(String, Vec<u8>)],
        call: F,
    ) -> Result<(Vec<AppendResult>, bool), EventLogError>
    where
        F: FnOnce(
            Arc<SqliteEventStore>,
            Vec<Expected>,
        ) -> BoxFuture<'a, Result<(Vec<AppendResult>, bool), EventLogError>>,
    {
        if meta.claim.is_some() {
            return Err(EventLogError::Unsupported {
                capability: "claims",
            });
        }
        let mut state = self.state.lock().await;
        if state.poisoned {
            return Err(EventLogError::Closed);
        }
        let _lock = self.writer_lock()?;
        refresh(&self.root, &mut state).await?;

        let mut parents_before = Vec::with_capacity(members.len());
        let mut engine_expected = Vec::with_capacity(members.len());
        for (stream, expected) in members {
            let heads = state
                .heads
                .get(&key_of(stream))
                .cloned()
                .unwrap_or_default();
            let passed = match expected {
                Expected::Merge(digest) if heads.len() > 1 => {
                    if *digest != HeadSetDigest::of(&heads) {
                        return Err(EventLogError::Forked {
                            stream: format!("{}/{}", stream.stream_type(), stream.stream_id()),
                            heads,
                        });
                    }
                    Expected::Any
                }
                Expected::Merge(_) => {
                    return Err(EventLogError::Invalid(
                        "a merge expectation names a stream that has not forked".into(),
                    ));
                }
                _ if heads.len() > 1 => {
                    return Err(EventLogError::Forked {
                        stream: format!("{}/{}", stream.stream_type(), stream.stream_id()),
                        heads,
                    });
                }
                other => *other,
            };
            engine_expected.push(passed);
            parents_before.push(heads);
        }

        let engine = Arc::clone(&state.engine);
        let (mut results, deduplicated) = call(engine, engine_expected.clone()).await?;
        if deduplicated {
            for result in &mut results {
                for event in &mut result.events {
                    if let Some(record) = state.records.get(&event.event_id) {
                        event.digest = state.digests.get(&event.event_id).cloned();
                        event.parents.clone_from(&record.parents);
                    }
                }
            }
            return Ok((results, true));
        }

        let recorded_at = results
            .iter()
            .flat_map(|result| result.events.iter())
            .map(|event| event.recorded_at)
            .max()
            .unwrap_or(meta.occurred_at);
        let mut group = GroupRecord {
            tenant: tenant.as_str().to_owned(),
            kind,
            meta: CommandMeta {
                claim: None,
                ..meta.clone()
            },
            members: members
                .iter()
                .zip(&engine_expected)
                .map(|((stream, _), expected)| Member {
                    stream_type: stream.stream_type().to_owned(),
                    stream_id: stream.stream_id().to_owned(),
                    expected: *expected,
                    events: Vec::new(),
                })
                .collect(),
            blobs: {
                let mut digests: Vec<String> =
                    blobs.iter().map(|(digest, _)| digest.clone()).collect();
                digests.sort();
                digests.dedup();
                digests
            },
            recorded_at,
        };
        let key = group.key_digest();
        let mut written = Vec::new();
        // Several members may append to one stream; each follows the one before it in the group,
        // so the heads are carried from member to member rather than read once before the group.
        let mut running: BTreeMap<StreamKey, Vec<String>> = BTreeMap::new();
        let mut offset = 0_u64;
        for (index, result) in results.iter_mut().enumerate() {
            let (stream, _) = &members[index];
            let mut parents = running
                .get(&key_of(stream))
                .cloned()
                .unwrap_or_else(|| parents_before[index].clone());
            for event in &mut result.events {
                let record = EventRecord {
                    tenant: tenant.as_str().to_owned(),
                    stream_type: stream.stream_type().to_owned(),
                    stream_id: stream.stream_id().to_owned(),
                    event_id: event.event_id.clone(),
                    name: event.name.clone(),
                    schema_version: event.schema_version,
                    recorded_at: event.recorded_at,
                    parents: parents.clone(),
                    group: key.clone(),
                    index: offset,
                    data_digest: sha256_hex(&canonical(&event.data)),
                    data: event.data.clone(),
                    redacted: None,
                };
                let digest = record.digest()?;
                event.digest = Some(digest.clone());
                event.parents.clone_from(&parents);
                group.members[index].events.push(digest.clone());
                parents = vec![digest];
                written.push(record);
                offset += 1;
            }
            running.insert(key_of(stream), parents);
        }

        let persisted = (|| {
            for (digest, bytes) in blobs {
                write_once(&blob_path(&self.root, tenant.as_str(), digest), bytes)?;
            }
            for record in &written {
                let path = event_path(
                    &self.root,
                    &record.tenant,
                    &record.stream_type,
                    &record.stream_id,
                    &record.digest()?,
                );
                write_once(&path, &canonical(&record.to_value()?))?;
            }
            // Written last: until this file exists the events above are torn, never served.
            let path = group_path(&self.root, tenant.as_str(), &key);
            write_once(&path, &canonical(&group.to_value()?))?;
            Ok::<PathBuf, EventLogError>(path)
        })();
        let path = match persisted {
            Ok(path) => path,
            Err(error) => {
                state.poisoned = true;
                return Err(error);
            }
        };
        state.group_files.insert(path);
        for (index, (stream, _)) in members.iter().enumerate() {
            if let Some(last) = group.members[index].events.last() {
                state.heads.insert(key_of(stream), vec![last.clone()]);
            }
        }
        for record in written {
            state
                .digests
                .insert(record.event_id.clone(), record.digest()?);
            state.records.insert(record.event_id.clone(), record);
        }
        Ok((results, false))
    }
}

/// Refuse a directory that holds another kind of store, and create `store.json` in one that
/// holds none.
fn prepare(root: &Path) -> Result<(), EventLogError> {
    // A file store's journal here would be a second history nobody replays.
    if root.join("manifest.json").exists() || root.join("events.jsonl").exists() {
        return Err(EventLogError::Invalid(
            "this directory holds an eventlog-file store, not a tree store".into(),
        ));
    }
    fs::create_dir_all(&root).map_err(backend)?;
    let manifest = root.join("store.json");
    match fs::read(&manifest) {
        Ok(bytes) => {
            let value: Value = serde_json::from_slice(&bytes).map_err(|_| corrupt("store.json"))?;
            if value.get("format").and_then(Value::as_str) != Some(FORMAT)
                || value.get("identity").and_then(Value::as_str).is_none()
            {
                return Err(corrupt("store.json is not an eventlog-tree store"));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let value = json!({ "format": FORMAT, "identity": eventlog_core::new_event_id() });
            write_once(&manifest, &canonical(&value))?;
        }
        Err(error) => return Err(backend(error)),
    }
    Ok(())
}

/// Rebuild the state when another writer has committed a group since this one was built.
async fn refresh(root: &Path, state: &mut State) -> Result<(), EventLogError> {
    if history::group_files(root)? == state.group_files {
        return Ok(());
    }
    let rebuilt = build(root, state.inline.clone(), state.catch_up.clone()).await?;
    *state = rebuilt;
    Ok(())
}

/// The state of the files under `root`: the one kept for exactly these files when there is one,
/// otherwise a replay, which is then kept if no file changed while it ran and none is too new to
/// trust by its stamp.
async fn build(
    root: &Path,
    inline: Vec<Arc<dyn Projector>>,
    catch_up: Vec<Arc<dyn Projector>>,
) -> Result<State, EventLogError> {
    let started = fold::now_ns();
    let before = fold::stamps(root);
    let names = |projectors: &[Arc<dyn Projector>]| -> Vec<&'static str> {
        projectors
            .iter()
            .map(|projector| projector.name())
            .collect()
    };
    let key = before
        .as_ref()
        .map(|stamps| fold::key(stamps, &names(&inline), &names(&catch_up)));
    if let Some(key) = &key
        && let Some((image, kept)) = fold::load(root, key)
        && let Ok(state) = from_kept(root, image, &kept, &inline, &catch_up).await
    {
        return Ok(state);
    }
    let state = replay_all(root, inline, catch_up).await?;
    if let (Some(before), Some(key)) = (before, key)
        && fold::settled(&before, started)
        && fold::stamps(root).as_ref() == Some(&before)
        && let Ok(image) = state.engine.image().await
    {
        fold::keep(root, &key, &image, kept_state(root, &state));
    }
    Ok(state)
}

/// What [`State`] holds beyond the engine, as [`from_kept`] reads it back.
fn kept_state(root: &Path, state: &State) -> Value {
    let heads: Vec<Value> = state
        .heads
        .iter()
        .map(|((tenant, stream_type, stream_id), heads)| {
            json!([tenant, stream_type, stream_id, heads])
        })
        .collect();
    let records: Vec<Value> = state
        .records
        .values()
        .filter_map(|record| record.to_value().ok())
        .collect();
    let group_files: Vec<String> = state
        .group_files
        .iter()
        .filter_map(|path| Some(path.strip_prefix(root).ok()?.to_string_lossy().into_owned()))
        .collect();
    json!({
        "heads": heads,
        "records": records,
        "group_files": group_files,
        "identities": state.identities,
    })
}

/// The state kept by [`build`], with its projectors registered again.
async fn from_kept(
    root: &Path,
    image: Vec<u8>,
    kept: &Value,
    inline: &[Arc<dyn Projector>],
    catch_up: &[Arc<dyn Projector>],
) -> Result<State, EventLogError> {
    let invalid = || corrupt("a kept state that does not read back");
    let engine = Arc::new(SqliteEventStore::from_image("tree", image).await?);
    for projector in catch_up {
        engine.create_projections(Arc::clone(projector)).await?;
    }
    for projector in inline {
        engine.register_inline(Arc::clone(projector)).await?;
    }
    let mut heads = BTreeMap::new();
    for entry in kept
        .get("heads")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?
    {
        let text = |index: usize| {
            entry
                .get(index)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(invalid)
        };
        let digests: Vec<String> =
            serde_json::from_value(entry.get(3).cloned().ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
        heads.insert((text(0)?, text(1)?, text(2)?), digests);
    }
    let mut records = BTreeMap::new();
    let mut digests = BTreeMap::new();
    for value in kept
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?
    {
        let (record, digest) = EventRecord::from_value(value)?;
        digests.insert(record.event_id.clone(), digest);
        records.insert(record.event_id.clone(), record);
    }
    let group_files = kept
        .get("group_files")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?
        .iter()
        .map(|path| {
            path.as_str()
                .map(|path| root.join(path))
                .ok_or_else(invalid)
        })
        .collect::<Result<BTreeSet<PathBuf>, _>>()?;
    let identities: BTreeSet<String> =
        serde_json::from_value(kept.get("identities").cloned().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
    Ok(State {
        engine,
        heads,
        records,
        digests,
        group_files,
        identities,
        inline: inline.to_vec(),
        catch_up: catch_up.to_vec(),
        poisoned: false,
        replayed: false,
    })
}

/// Replay the files under `root` into a fresh engine.
async fn replay_all(
    root: &Path,
    inline: Vec<Arc<dyn Projector>>,
    catch_up: Vec<Arc<dyn Projector>>,
) -> Result<State, EventLogError> {
    let loaded = history::load(root)?;
    let engine = Arc::new(SqliteEventStore::in_memory("tree").await?);
    for projector in &catch_up {
        engine.create_projections(Arc::clone(projector)).await?;
    }
    for projector in &inline {
        engine.register_inline(Arc::clone(projector)).await?;
    }
    let mut heads: BTreeMap<StreamKey, BTreeSet<String>> = BTreeMap::new();
    let mut records = BTreeMap::new();
    let mut digests = BTreeMap::new();
    let mut identities = BTreeSet::new();
    let mut redactions = Vec::new();
    for (tenant_name, tenant_history) in &loaded.tenants {
        let tenant = TenantId::new(tenant_name.clone())?;
        if let Some(identity) = &tenant_history.identity {
            engine.restore_stream_identity(&tenant, identity).await?;
            identities.insert(tenant_name.clone());
        }
        for (digest, bytes) in &tenant_history.blobs {
            engine.put_blob(&tenant, digest, bytes).await?;
        }
        for committed in &tenant_history.groups {
            let versions = replay(&engine, &tenant, committed).await?;
            for (record, version) in committed.events.iter().flatten().zip(versions) {
                let key = (
                    record.tenant.clone(),
                    record.stream_type.clone(),
                    record.stream_id.clone(),
                );
                let entry = heads.entry(key).or_default();
                for parent in &record.parents {
                    entry.remove(parent);
                }
                let digest = record.digest()?;
                entry.insert(digest.clone());
                if let Some(reason) = &record.redacted {
                    redactions.push((record.stream()?, version, reason.clone()));
                }
                digests.insert(record.event_id.clone(), digest);
                records.insert(record.event_id.clone(), record.clone());
            }
        }
    }
    for (stream, version, reason) in redactions {
        engine.redact(&stream, version, &reason).await?;
    }
    // A parent is always replayed before its child, so removing parents as their children arrive
    // leaves exactly the events no other event follows.
    Ok(State {
        engine,
        heads: heads
            .into_iter()
            .map(|(key, set)| (key, set.into_iter().collect()))
            .collect(),
        records,
        digests,
        group_files: loaded.group_files,
        identities,
        inline,
        catch_up,
        poisoned: false,
        replayed: true,
    })
}

/// Append one committed group to the engine as its writer did, keeping its identities and
/// instants. Returns each event's version, in the group's order.
async fn replay(
    engine: &SqliteEventStore,
    tenant: &TenantId,
    committed: &Committed,
) -> Result<Vec<u64>, EventLogError> {
    let restored = committed
        .events
        .iter()
        .flatten()
        .map(|record| RestoredEvent {
            event_id: record.event_id.clone(),
            recorded_at: record.recorded_at,
        })
        .collect();
    engine.restore_events(restored)?;
    let mut appends = Vec::with_capacity(committed.events.len());
    for (member, records) in committed.record.members.iter().zip(&committed.events) {
        appends.push(StreamAppend {
            stream: StreamId::new(
                tenant.clone(),
                member.stream_type.clone(),
                member.stream_id.clone(),
            )?,
            expected: member.expected,
            events: records
                .iter()
                .map(EventRecord::new_event)
                .collect::<Result<Vec<NewEvent>, _>>()?,
        });
    }
    let meta = &committed.record.meta;
    let results = match committed.record.kind {
        Kind::Append => {
            let append = &appends[0];
            vec![
                engine
                    .append(&append.stream, append.expected, &append.events, meta)
                    .await?,
            ]
        }
        Kind::Group => {
            let group = AppendGroup {
                tenant: tenant.clone(),
                appends,
                meta: meta.clone(),
            };
            let result = if committed.blobs.is_empty() {
                engine.append_group(&group).await?
            } else {
                engine
                    .append_group_with_blobs(&blob_group(group, &committed.blobs))
                    .await?
            };
            result.appends
        }
    };
    if engine.restored_pending()? != 0 || results.iter().any(|result| result.events.is_empty()) {
        return Err(corrupt(
            "a replayed group the engine did not append in full",
        ));
    }
    Ok(results
        .iter()
        .flat_map(|result| result.events.iter().map(|event| event.version))
        .collect())
}

/// The engine commits blob-bearing groups through its blob entry point, whose request fingerprint
/// covers the bytes; a replay goes through the same one so a retry still matches.
fn blob_group(group: AppendGroup, blobs: &[(String, Vec<u8>)]) -> BlobAppendGroup {
    BlobAppendGroup {
        group,
        blobs: blobs
            .iter()
            .map(|(digest, bytes)| BlobWrite {
                digest: digest.clone(),
                bytes: bytes.clone(),
            })
            .collect(),
    }
}

impl EventStore for TreeEventStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities::BRANCHABLE
    }

    fn append<'a>(
        &'a self,
        stream: &'a StreamId,
        expected: Expected,
        events: &'a [NewEvent],
        meta: &'a CommandMeta,
    ) -> BoxFuture<'a, Result<AppendResult, EventLogError>> {
        self.append_guarded(stream, expected, events, meta, Arc::new(NoGuard))
    }

    fn recorded_claim<'a>(
        &'a self,
        _tenant: &'a TenantId,
        _claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Option<ClaimedCommand>, EventLogError>> {
        Box::pin(async {
            Err(EventLogError::Unsupported {
                capability: "claims",
            })
        })
    }

    fn recorded_command<'a>(
        &'a self,
        stream: &'a StreamId,
        idempotency_key: &'a str,
        request_hash: &'a str,
    ) -> BoxFuture<'a, Result<Option<AppendResult>, EventLogError>> {
        Box::pin(async move {
            let engine = self.engine().await?;
            let mut found = engine
                .recorded_command(stream, idempotency_key, request_hash)
                .await?;
            if let Some(result) = &mut found {
                self.decorate(&mut result.events).await;
            }
            Ok(found)
        })
    }

    fn read_stream<'a>(
        &'a self,
        stream: &'a StreamId,
        after_version: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<StreamSlice, EventLogError>> {
        Box::pin(async move {
            let engine = self.engine().await?;
            let mut slice = engine.read_stream(stream, after_version, limit).await?;
            self.decorate(&mut slice.events).await;
            Ok(slice)
        })
    }

    fn stream_version<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<u64>, EventLogError>> {
        Box::pin(async move { self.engine().await?.stream_version(stream).await })
    }

    fn read_feed<'a>(
        &'a self,
        tenant: &'a TenantId,
        after_position: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<FeedPage, EventLogError>> {
        Box::pin(async move {
            let engine = self.engine().await?;
            let mut page = engine.read_feed(tenant, after_position, limit).await?;
            self.decorate(&mut page.events).await;
            Ok(page)
        })
    }

    fn redact<'a>(
        &'a self,
        stream: &'a StreamId,
        version: u64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<RecordedEvent, EventLogError>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Err(EventLogError::Closed);
            }
            let _lock = self.writer_lock()?;
            refresh(&self.root, &mut state).await?;
            let mut redacted = state.engine.redact(stream, version, reason).await?;
            let Some(record) = state.records.get_mut(&redacted.event_id) else {
                state.poisoned = true;
                return Err(corrupt("a redacted event with no file"));
            };
            // The body goes; the digest stays, because it covers the body only through
            // `data_digest`, so the chain through this event holds.
            record.data = redaction_tombstone(reason);
            record.redacted = Some(reason.to_owned());
            let record = record.clone();
            let written = record.digest().and_then(|digest| {
                let path = event_path(
                    &self.root,
                    &record.tenant,
                    &record.stream_type,
                    &record.stream_id,
                    &digest,
                );
                write_atomic(&path, &canonical(&record.to_value()?))
            });
            if let Err(error) = written {
                state.poisoned = true;
                return Err(error);
            }
            redacted.digest = record.digest().ok();
            redacted.parents.clone_from(&record.parents);
            Ok(redacted)
        })
    }

    fn save_snapshot<'a>(
        &'a self,
        stream: &'a StreamId,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move { self.engine().await?.save_snapshot(stream, snapshot).await })
    }

    fn snapshot_generation<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<SnapshotGeneration>, EventLogError>> {
        Box::pin(async move { self.engine().await?.snapshot_generation(stream).await })
    }

    fn save_snapshot_checked<'a>(
        &'a self,
        stream: &'a StreamId,
        snapshot: &'a Snapshot,
        generation: &'a SnapshotGeneration,
    ) -> BoxFuture<'a, Result<bool, EventLogError>> {
        Box::pin(async move {
            self.engine()
                .await?
                .save_snapshot_checked(stream, snapshot, generation)
                .await
        })
    }

    fn load_snapshot<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<Snapshot>, EventLogError>> {
        Box::pin(async move { self.engine().await?.load_snapshot(stream).await })
    }

    fn forget_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Err(EventLogError::Closed);
            }
            let _lock = self.writer_lock()?;
            state.engine.forget_tenant(tenant).await?;
            // Erasure is the one removal a log admits, besides redaction; it takes the tenant's
            // whole directory, so nothing of it is left to merge back.
            let directory = tenant_dir(&self.root, tenant.as_str());
            if let Err(error) = fs::remove_dir_all(&directory)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                state.poisoned = true;
                return Err(backend(error));
            }
            let name = tenant.as_str().to_owned();
            state.heads.retain(|(owner, _, _), _| owner != &name);
            state.records.retain(|_, record| record.tenant != name);
            let State {
                records, digests, ..
            } = &mut *state;
            digests.retain(|id, _| records.contains_key(id));
            state.identities.remove(&name);
            state.group_files = history::group_files(&self.root)?;
            Ok(())
        })
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
            // Malformed input is refused as such before any declared limit is.
            eventlog_core::validate_append(events, meta)?;
            let members = [(stream.clone(), expected)];
            let (mut results, _) = self
                .commit(
                    Kind::Append,
                    stream.tenant(),
                    &members,
                    meta,
                    &[],
                    move |engine, passed| {
                        Box::pin(async move {
                            let result = engine
                                .append_guarded(stream, passed[0], events, meta, guard)
                                .await?;
                            let deduplicated = result.deduplicated;
                            Ok((vec![result], deduplicated))
                        })
                    },
                )
                .await?;
            Ok(results.remove(0))
        })
    }

    fn create_projections(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            state
                .engine
                .create_projections(Arc::clone(&projector))
                .await?;
            state.catch_up.push(projector);
            Ok(())
        })
    }

    fn register_inline(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Err(EventLogError::Closed);
            }
            // Registration on an engine that has not served keeps the cheap path; once the replay
            // has appended history, the engine is rebuilt with the projector in place first.
            match state.engine.register_inline(Arc::clone(&projector)).await {
                Ok(()) => state.inline.push(projector),
                Err(EventLogError::Invalid(_)) if !state.records.is_empty() => {
                    let mut inline = state.inline.clone();
                    inline.push(projector);
                    *state = build(&self.root, inline, state.catch_up.clone()).await?;
                }
                Err(error) => return Err(error),
            }
            Ok(())
        })
    }

    fn is_inline<'a>(&'a self, name: &'a str) -> BoxFuture<'a, bool> {
        Box::pin(async move {
            let engine = Arc::clone(&self.state.lock().await.engine);
            engine.is_inline(name).await
        })
    }

    fn run_catch_up<'a>(
        &'a self,
        projector: Arc<dyn Projector>,
        tenant: &'a TenantId,
        batch: usize,
    ) -> BoxFuture<'a, Result<CatchUpProgress, EventLogError>> {
        Box::pin(async move {
            self.engine()
                .await?
                .run_catch_up(projector, tenant, batch)
                .await
        })
    }

    fn rebuild_projection<'a>(
        &'a self,
        projector: Arc<dyn Projector>,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<u64, EventLogError>> {
        Box::pin(async move {
            self.engine()
                .await?
                .rebuild_projection(projector, tenant)
                .await
        })
    }

    fn projection_get<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        Box::pin(async move {
            self.engine()
                .await?
                .projection_get(projection, tenant, key)
                .await
        })
    }

    fn projection_find<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        field: &'a str,
        value: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Value>, EventLogError>> {
        Box::pin(async move {
            self.engine()
                .await?
                .projection_find(projection, tenant, field, value, limit)
                .await
        })
    }

    fn projection_list<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        after_key: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(String, Value)>, EventLogError>> {
        Box::pin(async move {
            self.engine()
                .await?
                .projection_list(projection, tenant, after_key, limit)
                .await
        })
    }

    fn projection_page<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        prefix: Option<&'a str>,
        cursor: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<ProjectionPage, EventLogError>> {
        Box::pin(async move {
            self.engine()
                .await?
                .projection_page(projection, tenant, prefix, cursor, limit)
                .await
        })
    }

    fn stream_identity<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<String, EventLogError>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Err(EventLogError::Closed);
            }
            let identity = state.engine.stream_identity(tenant).await?;
            if !state.identities.contains(tenant.as_str()) {
                let _lock = self.writer_lock()?;
                let path = identity_path(&self.root, tenant.as_str());
                // Another writer may have named this tenant first; the replay after a merge would
                // then refuse two identities, so the first one on disk wins here too.
                if let Ok(bytes) = fs::read(&path) {
                    let value: Value =
                        serde_json::from_slice(&bytes).map_err(|_| corrupt("tenant identity"))?;
                    let stored = value
                        .get("stream_identity")
                        .and_then(Value::as_str)
                        .ok_or_else(|| corrupt("tenant identity"))?;
                    if stored != identity {
                        state.poisoned = true;
                        return Err(corrupt("another writer named this tenant's identity"));
                    }
                } else {
                    write_once(&path, &canonical(&json!({ "stream_identity": identity })))?;
                }
                state.identities.insert(tenant.as_str().to_owned());
            }
            Ok(identity)
        })
    }

    fn put_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Err(EventLogError::Closed);
            }
            let _lock = self.writer_lock()?;
            state.engine.put_blob(tenant, digest, bytes).await?;
            if let Err(error) = write_once(&blob_path(&self.root, tenant.as_str(), digest), bytes) {
                state.poisoned = true;
                return Err(error);
            }
            Ok(())
        })
    }

    fn get_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, EventLogError>> {
        Box::pin(async move { self.engine().await?.get_blob(tenant, digest).await })
    }

    fn delete_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Err(EventLogError::Closed);
            }
            let _lock = self.writer_lock()?;
            state.engine.delete_blob(tenant, digest).await?;
            let path = blob_path(&self.root, tenant.as_str(), digest);
            if let Err(error) = fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                state.poisoned = true;
                return Err(backend(error));
            }
            Ok(())
        })
    }
}

impl AtomicEventStore for TreeEventStore {
    fn append_group_guarded<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        self.group(group, admission, &[])
    }

    fn append_group_guarded_with_blobs<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
        blobs: &'a [(String, Vec<u8>)],
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        self.group(group, admission, blobs)
    }
}

impl TreeEventStore {
    fn group<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
        blobs: &'a [(String, Vec<u8>)],
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        Box::pin(async move {
            group.fingerprint()?;
            let members: Vec<(StreamId, Expected)> = group
                .appends
                .iter()
                .map(|append| (append.stream.clone(), append.expected))
                .collect();
            let (appends, deduplicated) = self
                .commit(
                    Kind::Group,
                    &group.tenant,
                    &members,
                    &group.meta,
                    blobs,
                    move |engine, passed| {
                        Box::pin(async move {
                            let mut rewritten = group.clone();
                            for (append, expected) in rewritten.appends.iter_mut().zip(passed) {
                                append.expected = expected;
                            }
                            let result = if blobs.is_empty() {
                                engine.append_group_guarded(&rewritten, admission).await?
                            } else {
                                engine
                                    .append_group_with_blobs_guarded(
                                        &blob_group(rewritten, blobs),
                                        admission,
                                    )
                                    .await?
                            };
                            Ok((result.appends, result.deduplicated))
                        })
                    },
                )
                .await?;
            Ok(AppendGroupResult {
                appends,
                deduplicated,
            })
        })
    }
}

impl BranchableEventStore for TreeEventStore {
    fn heads<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Vec<String>, EventLogError>> {
        Box::pin(async move {
            let state = self.state.lock().await;
            if state.poisoned {
                return Err(EventLogError::Closed);
            }
            Ok(state
                .heads
                .get(&key_of(stream))
                .cloned()
                .unwrap_or_default())
        })
    }
}

impl ConsistentTenantCapture for TreeEventStore {
    fn capture_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
        projections: &'a [ProjectionSpec],
        limits: CaptureLimits,
    ) -> BoxFuture<'a, Result<TenantCapture, CaptureError>> {
        Box::pin(async move {
            let engine = self.engine().await.map_err(CaptureError::from)?;
            let mut capture = engine.capture_tenant(tenant, projections, limits).await?;
            self.decorate(&mut capture.events).await;
            Ok(capture)
        })
    }
}

impl InlineProjectionAdmin for TreeEventStore {
    fn attach_inline_existing(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Err(EventLogError::Closed);
            }
            // The engine freezes inline registration once it has served, and the replay that
            // loaded this history already has. So the engine is rebuilt from the files with the
            // projector registered before the replay, which is what attaching to existing history
            // means: every row it would have written is written.
            let mut inline = state.inline.clone();
            inline.push(projector);
            *state = build(&self.root, inline, state.catch_up.clone()).await?;
            Ok(())
        })
    }

    fn rebuild_inline_projection<'a>(
        &'a self,
        projector_name: &'a str,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<InlineRebuildResult, EventLogError>> {
        Box::pin(async move {
            self.engine()
                .await?
                .rebuild_inline_projection(projector_name, tenant)
                .await
        })
    }
}
