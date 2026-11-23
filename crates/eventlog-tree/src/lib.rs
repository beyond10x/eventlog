//! A file event store whose history merges under version control.
//!
//! The store is a directory of immutable files:
//!
//! ```text
//! <root>/store.json                                     {format, identity}, written once
//! <root>/tenants/<tenant>/identity.json                 the tenant's stream identity, written once
//! <root>/tenants/<tenant>/streams/<type>/<id>/<digest>.json    one event per file (nested)
//! <root>/tenants/<tenant>/streams/<kk>/<hash>/<digest>.json    eventlog-tree/3: the same, hashed
//! <root>/tenants/<tenant>/groups/<kk>/<key-digest>.json        one command per file: the commit point
//! <root>/tenants/<tenant>/blobs/<kk>/<digest>           content-addressed bytes
//! <root>/tenants/<tenant>/split-blobs/<kk>/<digest>.json  eventlog-tree/2: a blob as a manifest
//! <root>/tenants/<tenant>/texts/<hh>/<sha256 hex>       eventlog-tree/2: one text, stored once
//! <root>/.lock                                          one writer per checkout; not history
//! ```
//!
//! `store.json` names the layout. `eventlog-tree/1` holds raw blob files only. `eventlog-tree/2`,
//! the layout a new store is created in, may keep a blob that is a JSON document as a manifest
//! whose long string literals are texts stored once by their SHA-256; a reader serves the exact
//! bytes either way. [`migrate`] moves an `eventlog-tree/1` store to `eventlog-tree/2`.
//!
//! `eventlog-tree/3` is `eventlog-tree/2` with each stream's directory named by a hash of its
//! key ([`TreeLayout::Hashed`]), so that every event path has one length whatever the stream. The
//! hash bounds the stream part only: the tenant segment and blob names are not bounded, so a Git
//! checkout on Windows without long paths fits only for a tenant whose escaped segment is at most
//! 36 bytes and `sha256:<64 hex>` blob digests ([`TreeLayout::Hashed`] gives the budget). A store
//! is created in it only when its owner asks, with
//! [`TreeEventStore::open_in`]; [`TreeEventStore::open`] creates `eventlog-tree/2` as every earlier
//! release did. Either way the layout is the store's: every open reads and writes the layout its
//! `store.json` names, and no migration moves a store from one stream layout to the other.
//! Eventlog 0.8.4 and earlier refuse to open an `eventlog-tree/3` store as not a tree store.
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
//! counters live only in memory. While a handle is open its positions hold: when it rebuilds its
//! engine, because another handle or a merge added history or content was removed, every event it
//! served keeps its position, an erased one leaves a gap, and history new to it is placed above
//! every position it assigned, so a saved feed cursor never passes an event it has not seen.

mod copy;
mod fold;
mod history;
#[cfg(feature = "kpi-counters")]
pub mod kpi;
mod layout;
mod migrate;
mod split;
mod verify;

pub use copy::{CopyReport, copy};
pub use layout::TreeLayout;
pub use migrate::{MigrationMode, MigrationReport, migrate};
pub use verify::{Finding, verify};

use eventlog_core::{
    AppendGroup, AppendGroupResult, AppendResult, AtomicBlobEventStore, AtomicEventStore,
    BlobAppendGroup, BlobWrite, BoxFuture, BranchableEventStore, Capabilities, CaptureError,
    CaptureLimits, CatchUpProgress, Claim, ClaimedCommand, CommandMeta, ConsistentTenantCapture,
    EventLogError, EventStore, Expected, FeedPage, Guard, HeadSetDigest, InlineProjectionAdmin,
    InlineRebuildResult, NewEvent, NoGuard, ProjectionPage, ProjectionSpec, Projector, Read,
    ReadResult, RecordedEvent, Snapshot, SnapshotGeneration, StreamAppend, StreamId, StreamSlice,
    TenantCapture, TenantId, redaction_tombstone,
};
use eventlog_sqlite::{EventOrigin, EventPositions, RestoredEvent, SqliteEventStore};
use history::{Committed, EventRecord, GroupRecord, Kind, Member};
use layout::{
    FORMAT, FORMAT_V3, backend, canonical, corrupt, event_path, group_path, identity_path,
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

/// The reason a replay or a [`copy`] redacts with when the file holds one this release refuses.
///
/// Releases up to 0.9.4 accepted free text as a redaction reason, and an event file keeps the
/// reason it was redacted with. A replay redacts again through
/// [`EventStore::redact`], which now takes only an owner-defined code, so such a reason is
/// replaced by this code rather than refusing to open or copy the store. The event file keeps
/// its bytes; what the store serves, and what a copy holds, is this code.
pub const LEGACY_REDACTION_REASON: &str = "legacy-reason";

/// `reason` when [`EventStore::redact`] accepts it, otherwise [`LEGACY_REDACTION_REASON`].
fn replayable_reason(reason: &str) -> &str {
    if eventlog_core::validate_redaction_reason(reason).is_ok() {
        reason
    } else {
        LEGACY_REDACTION_REASON
    }
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
    /// The privacy token published when this state was built: read before the files were, so a
    /// removal of content that ran while they were read leaves this state stale, never current.
    privacy: Option<String>,
    /// Set while a write is in the engine and not yet in the files, and left set when the caller
    /// stopped awaiting it in between: the engine may then hold what the files never committed,
    /// so the next operation rebuilds from the files before serving anything.
    unsettled: bool,
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

    /// Open the store at `root`, creating it in `layout` when the directory holds none.
    ///
    /// `layout` is used only to create a store: a store that exists keeps the layout its
    /// `store.json` names, and so does a directory that already holds tree files without a
    /// `store.json`. [`TreeEventStore::layout`] says which one a store has.
    ///
    /// # Errors
    /// As [`TreeEventStore::open`].
    pub async fn open_in(
        root: impl AsRef<Path>,
        layout: TreeLayout,
    ) -> Result<Self, EventLogError> {
        Self::open_in_with_inline(root, layout, Vec::new()).await
    }

    /// The layout this store's event files are in, from `store.json`.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when `store.json` cannot be read.
    pub fn layout(&self) -> Result<TreeLayout, EventLogError> {
        Ok(layout::store_format(&self.root)?.layout())
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
        let _lock = self.writer_lock().await?;
        let torn = history::load(&self.root)?.torn;
        for path in &torn {
            fs::remove_file(path).map_err(backend)?;
        }
        Ok(torn)
    }

    /// The writer lock, waited for on a blocking worker. Another handle holding it may be
    /// suspended on this very runtime, awaiting its engine; blocking the executor thread in the
    /// wait would keep that handle from ever resuming to release it.
    async fn writer_lock(&self) -> Result<fs::File, EventLogError> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || writer_lock(&root))
            .await
            .map_err(|_| {
                EventLogError::Backend("the writer lock's blocking worker panicked".into())
            })?
    }

    /// The engine, rebuilt first when another handle has removed content since it was built.
    async fn engine(&self) -> Result<Arc<SqliteEventStore>, EventLogError> {
        Ok(self.engine_at().await?.0)
    }

    /// The engine and the privacy token it is current for.
    async fn engine_at(&self) -> Result<(Arc<SqliteEventStore>, Option<String>), EventLogError> {
        let mut state = self.state.lock().await;
        self.current(&mut state).await?;
        Ok((Arc::clone(&state.engine), state.privacy.clone()))
    }

    /// Bring `state` up to the last removal of content any handle published: a redaction, an
    /// erasure or a blob deletion another handle made is in the files and not in this engine. The
    /// rebuild runs under the writer lock, so no removal is half done while it reads the files. A
    /// write this handle's caller abandoned between the engine and the files is rebuilt away too.
    async fn current(&self, state: &mut State) -> Result<(), EventLogError> {
        if state.poisoned {
            return Err(EventLogError::Closed);
        }
        if !state.unsettled && fold::privacy_token(&self.root)? == state.privacy {
            return Ok(());
        }
        let _lock = self.writer_lock().await?;
        refresh(&self.root, state).await
    }

    /// Serve `read` from an engine current for the last removal of content, and serve it again
    /// from a rebuilt one when a removal was published while it ran: what a read returns after a
    /// redaction or an erasure has returned never holds what it removed.
    async fn read<T, F, Fut>(&self, read: F) -> Result<T, EventLogError>
    where
        F: Fn(Arc<SqliteEventStore>) -> Fut,
        Fut: std::future::Future<Output = Result<T, EventLogError>>,
    {
        loop {
            let (engine, privacy) = self.engine_at().await?;
            let value = read(engine).await?;
            if fold::privacy_token(&self.root)? == privacy {
                return Ok(value);
            }
        }
    }

    /// After a removal of content, still under the writer lock: delete every kept state and
    /// publish a new privacy token, whatever the removal did, since one that failed half way has
    /// still changed the files and every other handle must rebuild from them. This handle stays
    /// marked unsettled after a failed removal, so it rebuilds from the files too.
    fn published<T>(
        &self,
        state: &mut State,
        removed: Result<T, EventLogError>,
    ) -> Result<T, EventLogError> {
        if removed.is_ok() {
            state.unsettled = false;
        }
        match fold::publish_removal(&self.root) {
            Ok(token) => {
                state.privacy = Some(token);
                removed
            }
            Err(error) => {
                // Other handles cannot be told, so this one stops serving rather than pretend
                // the removal is complete.
                state.poisoned = true;
                removed.and(Err(error))
            }
        }
    }

    /// Fill in each event's digest and parents from the files.
    async fn decorate(&self, events: &mut [RecordedEvent]) {
        decorate_from(&*self.state.lock().await, events);
    }

    /// Open the store at `root` with `inline` projectors registered before its history is
    /// replayed, so the replay runs once. [`TreeEventStore::open`] followed by
    /// [`EventStore::create_projections`] and [`InlineProjectionAdmin::attach_inline_existing`],
    /// which attaches only what the handle admitted, replays twice.
    ///
    /// # Errors
    /// As [`TreeEventStore::open`], and whatever a projector refuses during the replay.
    pub async fn open_with_inline(
        root: impl AsRef<Path>,
        inline: Vec<Arc<dyn Projector>>,
    ) -> Result<Self, EventLogError> {
        Self::open_in_with_inline(root, TreeLayout::Nested, inline).await
    }

    /// [`TreeEventStore::open_with_inline`], creating the store in `layout` when the directory
    /// holds none, as [`TreeEventStore::open_in`] does.
    ///
    /// # Errors
    /// As [`TreeEventStore::open_with_inline`].
    pub async fn open_in_with_inline(
        root: impl AsRef<Path>,
        layout: TreeLayout,
        inline: Vec<Arc<dyn Projector>>,
    ) -> Result<Self, EventLogError> {
        let root = root.as_ref().to_owned();
        prepare(&root, layout)?;
        let state = build(&root, inline, Vec::new(), Locked::NotHeld, None).await?;
        Ok(Self {
            root,
            state: Mutex::new(state),
        })
    }

    /// Append through the engine and, when it wrote, commit the same facts to the files.
    ///
    /// `members` names each stream and what its caller expected; `call` receives what the engine
    /// is to expect instead, which differs only for a merge, and whether the files already hold
    /// this command's receipt.
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
            bool,
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
        let _lock = self.writer_lock().await?;
        refresh(&self.root, &mut state).await?;

        // A command already recorded is answered from its receipt, whatever the heads are now: a
        // merge that committed has left one head where it found several, and a retry of it must
        // still return what it wrote. The engine checks the request against its receipt, with
        // each merge expectation passed as the `Any` it was written with; the merge's own head
        // set is checked against the parents its first event recorded, below.
        let stream = members
            .first()
            .map(|(stream, _)| (stream.stream_type(), stream.stream_id()));
        let receipt = group_path(
            &self.root,
            tenant.as_str(),
            &history::receipt_digest(kind, tenant.as_str(), stream, &meta.idempotency_key),
        );
        let recorded = state.group_files.contains(&receipt);

        let mut parents_before = Vec::with_capacity(members.len());
        let mut engine_expected = Vec::with_capacity(members.len());
        for (stream, expected) in members {
            let heads = state
                .heads
                .get(&key_of(stream))
                .cloned()
                .unwrap_or_default();
            let passed = match expected {
                Expected::Merge(_) if recorded => Expected::Any,
                other if recorded => *other,
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
        // From here until the files hold what the engine wrote, the engine may be ahead of them.
        // A caller that stops awaiting leaves the engine's worker to finish without this future;
        // the mark it leaves makes the next operation rebuild from the files instead of serving
        // a write that was never committed.
        state.unsettled = true;
        let (mut results, deduplicated) =
            match call(engine, engine_expected.clone(), recorded).await {
                Ok(answer) => answer,
                Err(error) => {
                    // The engine refused inside its own transaction and holds nothing new.
                    state.unsettled = false;
                    return Err(error);
                }
            };
        if deduplicated {
            state.unsettled = false;
            for result in &mut results {
                for event in &mut result.events {
                    if let Some(record) = state.records.get(&event.event_id) {
                        event.digest = state.digests.get(&event.event_id).cloned();
                        event.parents.clone_from(&record.parents);
                    }
                }
            }
            // A merge's head set is part of its request: the receipt answers only the merge over
            // the heads it actually joined.
            for ((_, expected), result) in members.iter().zip(&results) {
                if let Expected::Merge(digest) = expected {
                    let joined = result.events.first().map(|event| &event.parents);
                    if !joined.is_some_and(|parents| {
                        parents.len() > 1 && HeadSetDigest::of(parents) == *digest
                    }) {
                        return Err(EventLogError::IdempotencyMismatch {
                            key: meta.idempotency_key.clone(),
                        });
                    }
                }
            }
            return Ok((results, true));
        }
        if recorded {
            // The receipt exists but the engine wrote anew: it no longer matches the files.
            state.poisoned = true;
            return Err(corrupt(
                "a recorded command the engine did not answer from its receipt",
            ));
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
                parents = vec![digest.clone()];
                written.push((record, digest));
                offset += 1;
            }
            running.insert(key_of(stream), parents);
        }

        let persisted = (|| {
            let format = layout::store_format(&self.root)?;
            let splits = format.splits();
            for (digest, bytes) in blobs {
                split::write_blob(&self.root, tenant.as_str(), digest, bytes, splits)?;
            }
            for (record, digest) in &written {
                let path = event_path(
                    &self.root,
                    format,
                    &record.tenant,
                    &record.stream_type,
                    &record.stream_id,
                    digest,
                );
                write_once(&path, &canonical(&record.to_value_named(digest)?))?;
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
        for (record, digest) in written {
            state.digests.insert(record.event_id.clone(), digest);
            state.records.insert(record.event_id.clone(), record);
        }
        state.unsettled = false;
        Ok((results, false))
    }
}

/// Fill in each event's digest and parents from the files, as `state` knows them.
fn decorate_from(state: &State, events: &mut [RecordedEvent]) {
    for event in events {
        if let Some(record) = state.records.get(&event.event_id) {
            event.digest = state.digests.get(&event.event_id).cloned();
            event.parents.clone_from(&record.parents);
        }
    }
}

/// The one-writer-per-checkout lock, held until the returned file is dropped.
fn writer_lock(root: &Path) -> Result<fs::File, EventLogError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(".lock"))
        .map_err(backend)?;
    file.lock().map_err(backend)?;
    Ok(file)
}

/// The writer lock when no one holds it right now, without waiting; `None` otherwise.
fn try_writer_lock(root: &Path) -> Option<fs::File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(".lock"))
        .ok()?;
    file.try_lock().ok()?;
    Some(file)
}

/// Refuse a directory that holds another kind of store, and create `store.json` in one that
/// holds none: in `layout` for an empty directory, and nested for one that already holds tree
/// files, which every release before `eventlog-tree/3` wrote nested.
fn prepare(root: &Path, layout: TreeLayout) -> Result<(), EventLogError> {
    // A file store's journal here would be a second history nobody replays.
    if root.join("manifest.json").exists() || root.join("events.jsonl").exists() {
        return Err(EventLogError::Invalid(
            "this directory holds an eventlog-file store, not a tree store".into(),
        ));
    }
    fs::create_dir_all(root).map_err(backend)?;
    let manifest = root.join("store.json");
    match fs::read(&manifest) {
        Ok(_) => {
            layout::store_format(root)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let format = match layout {
                TreeLayout::Hashed if !root.join("tenants").exists() => FORMAT_V3,
                _ => FORMAT,
            };
            let identity = eventlog_core::new_event_id();
            let value = json!({ "format": format, "identity": identity });
            // No lock is held yet, so two handles may create one store at once. The first
            // `store.json` published is the store's; a later one never replaces it, since the
            // first handle may already have appended under its layout. The others honour it.
            // That needs hard links; without them `publish_once` falls back to a rename.
            if !layout::publish_once(&manifest, &canonical(&value), &identity)? {
                layout::store_format(root)?;
            }
        }
        Err(error) => return Err(backend(error)),
    }
    Ok(())
}

/// Rebuild the state when another writer has committed a group, or removed content, since this
/// one was built. The caller holds the writer lock.
async fn refresh(root: &Path, state: &mut State) -> Result<(), EventLogError> {
    if !state.unsettled
        && fold::privacy_token(root)? == state.privacy
        && history::group_files(root)? == state.group_files
    {
        return Ok(());
    }
    let rebuilt = build(
        root,
        state.inline.clone(),
        state.catch_up.clone(),
        Locked::Held,
        Some(state.engine.positions().await?),
    )
    .await?;
    *state = rebuilt;
    Ok(())
}

/// Whether the caller of [`build`] holds the writer lock.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Locked {
    Held,
    NotHeld,
}

/// The state of the files under `root`: the one kept for exactly these files when there is one,
/// otherwise a replay, which is then kept if no file changed while it ran and none is too new to
/// trust by its stamp.
///
/// A state is kept only under the writer lock, and only while the privacy token it was built under
/// is still the published one: a removal of content deletes every kept state under that lock, so
/// a replay that read the files before the removal can never put its image back afterwards.
///
/// `served` holds where every event stood in the engine an open handle is replacing. Positions are
/// valid for as long as the handle is open, and a reader may have saved any of them, so a rebuild
/// replays from the files with each of those events at its old position, and places every event
/// new to the handle above the highest position it ever assigned. A kept state was folded by
/// another replay, in that replay's order, so a rebuild never loads one.
async fn build(
    root: &Path,
    inline: Vec<Arc<dyn Projector>>,
    catch_up: Vec<Arc<dyn Projector>>,
    locked: Locked,
    served: Option<EventPositions>,
) -> Result<State, EventLogError> {
    let privacy = fold::privacy_token(root)?;
    let started = fold::now_ns();
    let before = fold::stamps(root);
    // A kept state stands for a fold through exactly these projectors only when each vouches for
    // its output with a fingerprint; without one, every open folds through it again.
    let fingerprints = |projectors: &[Arc<dyn Projector>]| -> Option<Vec<(&'static str, String)>> {
        projectors
            .iter()
            .map(|projector| Some((projector.name(), projector.fingerprint()?)))
            .collect()
    };
    let key = match (
        before.as_ref(),
        fingerprints(&inline),
        fingerprints(&catch_up),
    ) {
        (Some(stamps), Some(inline), Some(catch_up)) => Some(fold::key(stamps, &inline, &catch_up)),
        _ => None,
    };
    if served.is_none()
        && let Some(key) = &key
        && let Some((image, kept)) = fold::load(root, key)
        && let Ok(mut state) = from_kept(root, image, &kept, &inline, &catch_up).await
    {
        state.privacy = privacy;
        return Ok(state);
    }
    let mut state = replay_all(root, inline, catch_up, served.as_ref()).await?;
    state.privacy = privacy;
    if let (Some(before), Some(key)) = (before, key)
        && fold::settled(&before, started)
        && let Ok(image) = state.engine.image().await
    {
        // Without the lock already held, only a lock free right now is taken: keeping a state
        // saves a later replay, and is never worth waiting for.
        let _lock = match locked {
            Locked::Held => None,
            Locked::NotHeld => match try_writer_lock(root) {
                Some(lock) => Some(lock),
                None => return Ok(state),
            },
        };
        if fold::stamps(root).as_ref() == Some(&before)
            && fold::privacy_token(root).ok().as_ref() == Some(&state.privacy)
        {
            fold::keep(root, &key, &image, kept_state(root, &state));
        }
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
        privacy: None,
        unsettled: false,
    })
}

/// Replay the files under `root` into a fresh engine, keeping the positions in `served` (see
/// [`build`]).
async fn replay_all(
    root: &Path,
    inline: Vec<Arc<dyn Projector>>,
    catch_up: Vec<Arc<dyn Projector>>,
    served: Option<&EventPositions>,
) -> Result<State, EventLogError> {
    let loaded = history::load(root)?;
    // An inline projector is handed each event as the replay appends it, and a redacted event is
    // appended with its tombstone body and marked redacted only after every group is in: the
    // projector would take a redacted event for a live one. With any redaction in the history,
    // the inline projectors are therefore driven afterwards, from the restored events.
    let redacted = loaded
        .tenants
        .values()
        .flat_map(|tenant| &tenant.groups)
        .flat_map(|committed| committed.events.iter().flatten())
        .any(|record| record.redacted.is_some());
    let deferred = redacted && !inline.is_empty();
    let engine = Arc::new(SqliteEventStore::in_memory("tree").await?);
    for projector in &catch_up {
        engine.create_projections(Arc::clone(projector)).await?;
    }
    if !deferred {
        for projector in &inline {
            engine.register_inline(Arc::clone(projector)).await?;
        }
    }
    let mut heads: BTreeMap<StreamKey, BTreeSet<String>> = BTreeMap::new();
    let mut records = BTreeMap::new();
    let mut digests = BTreeMap::new();
    let mut identities = BTreeSet::new();
    let mut redactions = Vec::new();
    let mut order = Vec::new();
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
            order.push((tenant.clone(), committed));
        }
    }
    // Tenant by tenant, each in causal order, unless the handle has served positions. Then every
    // group it served goes first, in the order it held them, which was causal when they were
    // appended, and each event keeps its position; the groups new to it follow in causal order,
    // above the highest position it assigned. A stream's versions then still rise with positions.
    let mut next = served.map_or(0, |served| served.high_water);
    if let Some(served) = served {
        let first = |committed: &Committed| {
            committed
                .events
                .iter()
                .flatten()
                .filter_map(|record| served.by_event.get(&record.event_id).copied())
                .min()
        };
        // Stable, so the new groups keep their causal order behind the served ones.
        order.sort_by_key(|(_, committed)| first(committed).map_or((1, 0), |at| (0, at)));
    }
    for (tenant, committed) in order {
        let positions = served.map(|served| {
            committed
                .events
                .iter()
                .flatten()
                .map(|record| {
                    served
                        .by_event
                        .get(&record.event_id)
                        .copied()
                        .unwrap_or_else(|| {
                            next += 1;
                            next
                        })
                })
                .collect()
        });
        let versions = replay(&engine, &tenant, committed, None, positions).await?;
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
    for (stream, version, reason) in redactions {
        engine
            .redact(&stream, version, replayable_reason(&reason))
            .await?;
    }
    if served.is_some() {
        // An append after the rebuild lands above every position the handle ever assigned, also
        // one an event since erased held.
        engine.reserve_positions_through(next).await?;
    }
    let engine = if deferred {
        drive_inline_from_restored(&engine, &loaded, &inline, &catch_up).await?
    } else {
        engine
    };
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
        privacy: None,
        unsettled: false,
    })
}

/// An engine holding everything `replayed` holds, with `inline` registered and each of their
/// projections folded from the restored events, redaction marks included, tenant by tenant.
///
/// Registration closes once an engine has appended, so the replayed engine is carried over through
/// its image, as a kept state is ([`from_kept`]).
async fn drive_inline_from_restored(
    replayed: &SqliteEventStore,
    loaded: &history::Loaded,
    inline: &[Arc<dyn Projector>],
    catch_up: &[Arc<dyn Projector>],
) -> Result<Arc<SqliteEventStore>, EventLogError> {
    let engine = Arc::new(SqliteEventStore::from_image("tree", replayed.image().await?).await?);
    for projector in catch_up {
        engine.create_projections(Arc::clone(projector)).await?;
    }
    for projector in inline {
        engine.register_inline(Arc::clone(projector)).await?;
    }
    for tenant in loaded.tenants.keys() {
        let tenant = TenantId::new(tenant.clone())?;
        for projector in inline {
            engine
                .rebuild_inline_projection(projector.name(), &tenant)
                .await?;
        }
    }
    Ok(engine)
}

/// Append one committed group to the engine as its writer did, keeping its identities and
/// instants. Returns each event's version, in the group's order.
///
/// `origins` holds the tree's version of each event, by event id, when the engine is a copy's
/// target rather than the tree's own; each event then records where it stood in the tree.
/// `positions` holds the global position each event takes, in the group's order, when a rebuild
/// keeps the positions a handle served.
async fn replay(
    engine: &SqliteEventStore,
    tenant: &TenantId,
    committed: &Committed,
    origins: Option<&BTreeMap<String, u64>>,
    positions: Option<Vec<u64>>,
) -> Result<Vec<u64>, EventLogError> {
    let restored = committed
        .events
        .iter()
        .flatten()
        .map(|record| {
            let origin = match origins {
                None => None,
                Some(versions) => Some(EventOrigin {
                    version: *versions
                        .get(&record.event_id)
                        .ok_or_else(|| corrupt("a committed event the tree does not serve"))?,
                    digest: record.digest()?,
                    parents: record.parents.clone(),
                }),
            };
            Ok(RestoredEvent {
                event_id: record.event_id.clone(),
                recorded_at: record.recorded_at,
                origin,
            })
        })
        .collect::<Result<Vec<_>, EventLogError>>()?;
    match positions {
        None => engine.restore_events(restored)?,
        Some(positions) => engine.restore_events_at(restored, positions)?,
    }
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
/// covers the bytes; a replay goes through the same one so a retry still matches. The copy it
/// makes lives only for this one group's append.
fn blob_group(group: AppendGroup, blobs: &[(String, impl AsRef<[u8]>)]) -> BlobAppendGroup {
    BlobAppendGroup {
        group,
        blobs: blobs
            .iter()
            .map(|(digest, bytes)| BlobWrite {
                digest: digest.clone(),
                bytes: bytes.as_ref().to_vec(),
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
        Box::pin(self.read(move |engine| async move {
            let mut found = engine
                .recorded_command(stream, idempotency_key, request_hash)
                .await?;
            if let Some(result) = &mut found {
                self.decorate(&mut result.events).await;
            }
            Ok(found)
        }))
    }

    fn read_stream<'a>(
        &'a self,
        stream: &'a StreamId,
        after_version: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<StreamSlice, EventLogError>> {
        Box::pin(self.read(move |engine| async move {
            let mut slice = engine.read_stream(stream, after_version, limit).await?;
            self.decorate(&mut slice.events).await;
            Ok(slice)
        }))
    }

    fn stream_version<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<u64>, EventLogError>> {
        Box::pin(self.read(move |engine| async move { engine.stream_version(stream).await }))
    }

    fn read_feed<'a>(
        &'a self,
        tenant: &'a TenantId,
        after_position: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<FeedPage, EventLogError>> {
        Box::pin(self.read(move |engine| async move {
            let mut page = engine.read_feed(tenant, after_position, limit).await?;
            self.decorate(&mut page.events).await;
            Ok(page)
        }))
    }

    fn read_many<'a>(
        &'a self,
        reads: &'a [Read],
    ) -> BoxFuture<'a, Result<Vec<ReadResult>, EventLogError>> {
        Box::pin(async move {
            // One observation for the whole batch: the state stays locked from the first read to
            // the last, so no commit through this handle lands between two of them, and every
            // event's digest and parents come from the state that served its body.
            loop {
                let mut state = self.state.lock().await;
                self.current(&mut state).await?;
                let mut results = Vec::with_capacity(reads.len());
                for read in reads {
                    results.push(match read {
                        Read::Stream {
                            stream,
                            after_version,
                            limit,
                        } => {
                            let mut slice = state
                                .engine
                                .read_stream(stream, *after_version, *limit)
                                .await?;
                            decorate_from(&state, &mut slice.events);
                            ReadResult::Stream(slice)
                        }
                        Read::Blob { tenant, digest } => {
                            ReadResult::Blob(state.engine.get_blob(tenant, digest).await?)
                        }
                    });
                }
                // As `TreeEventStore::read`: a removal another handle published meanwhile is
                // served again from a rebuilt state.
                if fold::privacy_token(&self.root)? == state.privacy {
                    return Ok(results);
                }
            }
        })
    }

    fn redact<'a>(
        &'a self,
        stream: &'a StreamId,
        version: u64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<RecordedEvent, EventLogError>> {
        Box::pin(async move {
            eventlog_core::validate_redaction_reason(reason)?;
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Err(EventLogError::Closed);
            }
            let _lock = self.writer_lock().await?;
            refresh(&self.root, &mut state).await?;
            state.unsettled = true;
            let removed = async {
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
                    let format = layout::store_format(&self.root)?;
                    let path = event_path(
                        &self.root,
                        format,
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
            }
            .await;
            self.published(&mut state, removed)
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
        Box::pin(self.read(move |engine| async move { engine.load_snapshot(stream).await }))
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
            let _lock = self.writer_lock().await?;
            // Another writer's groups for other tenants are taken in first: the erasure below
            // drops only this tenant's from the groups this state has loaded, so a group it never
            // loaded can never count as loaded.
            refresh(&self.root, &mut state).await?;
            state.unsettled = true;
            let removed = async {
                state.engine.forget_tenant(tenant).await?;
                // Erasure is the one removal a log admits, besides redaction; it takes the
                // tenant's whole directory, so nothing of it is left to merge back. Every state
                // kept under `.cache/` is deleted with it, by `published` below.
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
                state
                    .group_files
                    .retain(|path| !path.starts_with(&directory));
                Ok(())
            }
            .await;
            self.published(&mut state, removed)
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
                    move |engine, passed, _| {
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
                    let served = state.engine.positions().await?;
                    *state = build(
                        &self.root,
                        inline,
                        state.catch_up.clone(),
                        Locked::NotHeld,
                        Some(served),
                    )
                    .await?;
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
        Box::pin(self.read(move |engine| async move {
            engine.projection_get(projection, tenant, key).await
        }))
    }

    fn projection_find<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        field: &'a str,
        value: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Value>, EventLogError>> {
        Box::pin(self.read(move |engine| async move {
            engine
                .projection_find(projection, tenant, field, value, limit)
                .await
        }))
    }

    fn projection_list<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        after_key: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(String, Value)>, EventLogError>> {
        Box::pin(self.read(move |engine| async move {
            engine
                .projection_list(projection, tenant, after_key, limit)
                .await
        }))
    }

    fn projection_page<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        prefix: Option<&'a str>,
        cursor: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<ProjectionPage, EventLogError>> {
        Box::pin(self.read(move |engine| async move {
            engine
                .projection_page(projection, tenant, prefix, cursor, limit)
                .await
        }))
    }

    fn stream_identity<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<String, EventLogError>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            // An identity another handle's erasure removed must not be handed out again.
            self.current(&mut state).await?;
            if state.identities.contains(tenant.as_str()) {
                return state.engine.stream_identity(tenant).await;
            }
            // Under the writer lock, before the engine makes one up: another handle may have named
            // this tenant since this one opened, and the first identity on disk is the tenant's
            // (the replay after a merge would refuse two).
            let _lock = self.writer_lock().await?;
            let path = identity_path(&self.root, tenant.as_str());
            let identity = match fs::read(&path) {
                Ok(bytes) => {
                    let value: Value =
                        serde_json::from_slice(&bytes).map_err(|_| corrupt("tenant identity"))?;
                    let stored = value
                        .get("stream_identity")
                        .and_then(Value::as_str)
                        .ok_or_else(|| corrupt("tenant identity"))?
                        .to_owned();
                    // Refused only when this handle's engine already holds another identity.
                    match state.engine.restore_stream_identity(tenant, &stored).await {
                        Ok(()) => {}
                        Err(EventLogError::Invalid(_)) => {
                            state.poisoned = true;
                            return Err(corrupt("another writer named this tenant's identity"));
                        }
                        Err(other) => return Err(other),
                    }
                    stored
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let identity = state.engine.stream_identity(tenant).await?;
                    write_once(&path, &canonical(&json!({ "stream_identity": identity })))?;
                    identity
                }
                Err(error) => return Err(backend(error)),
            };
            state.identities.insert(tenant.as_str().to_owned());
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
            let _lock = self.writer_lock().await?;
            // As in `commit`: a caller that stops awaiting leaves the engine ahead of the files.
            state.unsettled = true;
            if let Err(error) = state.engine.put_blob(tenant, digest, bytes).await {
                state.unsettled = false;
                return Err(error);
            }
            let written = layout::store_format(&self.root).and_then(|format| {
                split::write_blob(&self.root, tenant.as_str(), digest, bytes, format.splits())
            });
            if let Err(error) = written {
                state.poisoned = true;
                return Err(error);
            }
            state.unsettled = false;
            Ok(())
        })
    }

    fn get_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, EventLogError>> {
        Box::pin(self.read(move |engine| async move { engine.get_blob(tenant, digest).await }))
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
            // A key `put_blob` refuses names no blob, and its file name would be derived from
            // bytes no writer ever spelled: refuse it before anything is resolved or removed.
            eventlog_core::validate_field("digest", digest)?;
            let _lock = self.writer_lock().await?;
            refresh(&self.root, &mut state).await?;
            // A split blob takes with it every text no other blob of the tenant names, so a
            // deleted blob's content is gone from the tree and not only its manifest. Every
            // manifest is read before anything is removed: a fault refuses the whole deletion.
            let deletion = split::plan_delete(&self.root, tenant.as_str(), digest)?;
            state.unsettled = true;
            let removed = async {
                state.engine.delete_blob(tenant, digest).await?;
                if let Err(error) = deletion.apply() {
                    state.poisoned = true;
                    return Err(error);
                }
                Ok(())
            }
            .await;
            self.published(&mut state, removed)
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
                    move |engine, passed, recorded| {
                        Box::pin(async move {
                            let mut rewritten = group.clone();
                            for (append, expected) in rewritten.appends.iter_mut().zip(passed) {
                                append.expected = expected;
                            }
                            let result = if blobs.is_empty() {
                                engine.append_group_guarded(&rewritten, admission).await?
                            } else if recorded {
                                // A retry carrying a batch is admitted again before its receipt
                                // answers it, as `AtomicEventStore` promises: the blob entry point
                                // the commit went through answers from its receipt without asking
                                // the guard. The receipt it recorded is matched all the same.
                                engine
                                    .append_group_guarded_with_blobs(&rewritten, admission, blobs)
                                    .await?
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
            // As `TreeEventStore::read`, with the capture's own error type.
            loop {
                let (engine, privacy) = self.engine_at().await.map_err(CaptureError::from)?;
                let mut capture = engine.capture_tenant(tenant, projections, limits).await?;
                if fold::privacy_token(&self.root).map_err(CaptureError::from)? == privacy {
                    self.decorate(&mut capture.events).await;
                    return Ok(capture);
                }
            }
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
            // Attachment is to projections admitted already; registering the projector for the
            // replay below would create its tables, so a misspelled or never admitted projection
            // would report success. The files keep no projection structure: what this handle
            // admitted is what `create_projections` admitted on it. An engine holding exactly those
            // admissions judges the attachment by the rules SQLite attaches by.
            let admitted = SqliteEventStore::in_memory("tree").await?;
            for admission in &state.catch_up {
                admitted.create_projections(Arc::clone(admission)).await?;
            }
            admitted
                .attach_inline_existing(Arc::clone(&projector))
                .await?;
            // The engine freezes inline registration once it has served, and the replay that
            // loaded this history already has. So the engine is rebuilt from the files with the
            // projector registered before the replay, which is what attaching to existing history
            // means: every row it would have written is written.
            let mut inline = state.inline.clone();
            inline.push(projector);
            let served = state.engine.positions().await?;
            *state = build(
                &self.root,
                inline,
                state.catch_up.clone(),
                Locked::NotHeld,
                Some(served),
            )
            .await?;
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
