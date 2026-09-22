//! Durable JSONL framing. The manifest is the commit point, never a best-effort cache.
use eventlog_core::{CaptureError, CaptureMaterial, EventLogError, InspectionError, new_event_id};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const FORMAT: &str = "eventlog-file/1";
const ZERO: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[cfg(test)]
pub(crate) fn checkpoint(name: &str) {
    if name == "append-committed"
        && std::env::var_os("EVENTLOG_FILE_ATOMIC_BLOB_FAIL_CLEANUP").is_some()
    {
        let root = std::env::var_os("EVENTLOG_FILE_CRASH_ROOT").expect("test-owned store");
        fs::create_dir(Path::new(&root).join("blobs/00000000-0000-0000-0000-000000000000"))
            .expect("inject a native post-commit cleanup error");
    }
    if std::env::var("EVENTLOG_FILE_CRASH_AT").as_deref() == Ok(name) {
        std::process::exit(73);
    }
}
#[cfg(not(test))]
pub(crate) fn checkpoint(_: &str) {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    format: String,
    pub store: String,
    pub epoch: u64,
    pub sequence: u64,
    pub length: u64,
    pub digest: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    format: String,
    store: String,
    epoch: u64,
    sequence: u64,
    previous: String,
    transaction: Value,
    digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivacyIntent {
    before: Manifest,
    after: Manifest,
    replacement_digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendIntent {
    before: Manifest,
    after: Manifest,
    frame: String,
}

pub(crate) struct Journal {
    root: PathBuf,
    // Kept open for the entire read/decide/commit cycle. Each caller opens its own description.
    _lock: File,
    pub manifest: Manifest,
    pub transactions: Vec<Value>,
    content: Content,
}

/// A running SHA-256 over the committed bytes of `events.jsonl`, to the committed length.
///
/// A handle that resumes onto a head it already chained trusts committed bytes it is not decoding
/// again. This carries what those bytes were when it verified them, so [`Journal::resume`] can
/// re-read exactly them and hand a history whose committed prefix is no longer the one this handle
/// verified to the complete opener, which refuses it. Without it, damage inside the committed
/// prefix is invisible to every later transaction, including the one that appends onto it.
#[derive(Clone)]
pub(crate) struct Content(Sha256);

impl Content {
    fn empty() -> Self {
        Self(Sha256::new())
    }
    fn of(bytes: &[u8]) -> Self {
        let mut content = Self::empty();
        content.absorb(bytes);
        content
    }
    fn absorb(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
    fn digest(&self) -> String {
        format!("{:x}", self.0.clone().finalize())
    }
}

/// Retains the native lock while the caller folds and examines this observation.
pub(crate) struct InspectedJournal {
    _lock: File,
    pub transactions: Vec<Value>,
}

/// Source bytes count the manifest and journal, the only files decoded here.
pub(crate) fn inspect(root: &Path, source_bytes: u64) -> Result<InspectedJournal, InspectionError> {
    // The one predicate, not a second copy of the decision: an inspection that read a link as a
    // store is the same defect the write paths already refuse.
    if !physical_directory(root).map_err(inspection_io)? {
        return Err(InspectionError::UnsupportedSource);
    }
    let root_metadata = fs::symlink_metadata(root).map_err(inspection_io)?;
    let lock = inspection_file(&root.join("writer.lock"))?;
    lock.try_lock().map_err(|_| InspectionError::SourceBusy)?;
    for name in ["append.json", "privacy.json"] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(_) => return Err(InspectionError::RecoveryRequired),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(inspection_io(error)),
        }
    }
    let manifest_file = inspection_file(&root.join("manifest.json"))?;
    let events_file = inspection_file(&root.join("events.jsonl"))?;
    let manifest_len = manifest_file.metadata().map_err(inspection_io)?.len();
    let events_len = events_file.metadata().map_err(inspection_io)?.len();
    if manifest_len
        .checked_add(events_len)
        .ok_or(InspectionError::LimitExceeded)?
        > source_bytes
    {
        return Err(InspectionError::LimitExceeded);
    }
    let manifest: Manifest = serde_json::from_slice(&inspection_read(manifest_file, manifest_len)?)
        .map_err(|_| InspectionError::CorruptSource)?;
    if manifest.format != FORMAT {
        return Err(InspectionError::UnsupportedSource);
    }
    if manifest.store.is_empty() || manifest.length != events_len {
        return Err(InspectionError::CorruptSource);
    }
    let transactions = decode(&inspection_read(events_file, events_len)?, &manifest)
        .map_err(|_| InspectionError::CorruptSource)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let after = fs::symlink_metadata(root).map_err(|_| InspectionError::SourceChanged)?;
        let after_lock = fs::symlink_metadata(root.join("writer.lock"))
            .map_err(|_| InspectionError::SourceChanged)?;
        let locked = lock.metadata().map_err(inspection_io)?;
        if (after.dev(), after.ino()) != (root_metadata.dev(), root_metadata.ino())
            || (after_lock.dev(), after_lock.ino()) != (locked.dev(), locked.ino())
        {
            return Err(InspectionError::SourceChanged);
        }
    }
    Ok(InspectedJournal {
        _lock: lock,
        transactions,
    })
}

fn inspection_io(error: std::io::Error) -> InspectionError {
    let kind = error.kind();
    // This boundary consumes the original diagnostic; retained paths never enter errors.
    drop(error);
    if kind == std::io::ErrorKind::NotFound {
        InspectionError::MissingSource
    } else {
        InspectionError::SourceBusy
    }
}

fn inspection_file(path: &Path) -> Result<File, InspectionError> {
    if !fs::symlink_metadata(path).map_err(inspection_io)?.is_file() {
        return Err(InspectionError::UnsupportedSource);
    }
    File::open(path).map_err(inspection_io)
}

fn inspection_read(file: File, length: u64) -> Result<Vec<u8>, InspectionError> {
    let mut bytes = Vec::new();
    file.take(
        length
            .checked_add(1)
            .ok_or(InspectionError::LimitExceeded)?,
    )
    .read_to_end(&mut bytes)
    .map_err(inspection_io)?;
    if u64::try_from(bytes.len()).map_err(|_| InspectionError::LimitExceeded)? != length {
        return Err(InspectionError::SourceChanged);
    }
    Ok(bytes)
}

pub(crate) fn backend(error: impl std::fmt::Display) -> EventLogError {
    EventLogError::Backend(error.to_string())
}
fn corrupt() -> EventLogError {
    EventLogError::Backend("file journal integrity check failed; no history was repaired".into())
}
pub(crate) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn sync_dir(root: &Path) -> Result<(), EventLogError> {
    File::open(root)
        .and_then(|file| file.sync_all())
        .map_err(backend)
}
fn regular(path: &Path) -> Result<(), EventLogError> {
    if !fs::symlink_metadata(path).map_err(backend)?.is_file() {
        return Err(corrupt());
    }
    Ok(())
}
/// Whether `path` is a directory in this filesystem, rather than a link to one.
///
/// Every entry point onto a store root asks this and asks it *first*: a symlink in a path
/// component is followed by every check after it, so a root that is not a physical directory is
/// refused here or nowhere. One predicate and five callers, because five hand-written copies is
/// how they came to disagree — [`resume_strict`] shipped without one and read a store the other
/// four refuse.
///
/// The refusals stay different on purpose and belong to the callers: `corrupt()` for the two
/// writer paths, [`open_strict`]'s own string for the reader a consumer reads errors from,
/// `crate::directory`'s own message, and `None` for [`resume_strict`], which hands the decision
/// to `open_strict`. `symlink_metadata` does not follow a final-component link, which is the
/// whole question.
pub(crate) fn physical_directory(path: &Path) -> Result<bool, std::io::Error> {
    Ok(fs::symlink_metadata(path)?.is_dir())
}
fn read(path: &Path) -> Result<Vec<u8>, EventLogError> {
    regular(path)?;
    fs::read(path).map_err(backend)
}
fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), EventLogError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(backend)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(backend)
}
fn atomic_json(root: &Path, name: &str, value: &impl Serialize) -> Result<(), EventLogError> {
    let staging = root.join(format!(".write-{}", new_event_id()));
    write_synced(&staging, &serde_json::to_vec(value).map_err(backend)?)?;
    fs::rename(&staging, root.join(name)).map_err(backend)?;
    sync_dir(root)
}

impl Journal {
    pub fn open(root: &Path) -> Result<Self, EventLogError> {
        Self::open_with_creation(root, true)
    }

    /// Open a provisioned journal without minting any missing authority.
    pub fn open_existing(root: &Path) -> Result<Self, EventLogError> {
        Self::open_with_creation(root, false)
    }

    fn open_with_creation(root: &Path, create: bool) -> Result<Self, EventLogError> {
        if create {
            fs::create_dir_all(root).map_err(backend)?;
        }
        if !physical_directory(root).map_err(backend)? {
            return Err(corrupt());
        }
        let lock_path = root.join("writer.lock");
        if !create || fs::symlink_metadata(&lock_path).is_ok() {
            regular(&lock_path)?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(lock_path)
            .map_err(backend)?;
        lock.lock().map_err(backend)?;
        let manifest_path = root.join("manifest.json");
        if !manifest_path.exists() {
            if !create {
                return Err(corrupt());
            }
            // Never interpret an existing history without its commit authority as a new store.
            if root.join("events.jsonl").exists() || root.join("privacy.json").exists() {
                return Err(corrupt());
            }
            let manifest = Manifest {
                format: FORMAT.into(),
                store: new_event_id(),
                epoch: 0,
                sequence: 0,
                length: 0,
                digest: ZERO.into(),
            };
            write_synced(&root.join("events.jsonl"), b"")?;
            sync_dir(root)?;
            atomic_json(root, "manifest.json", &manifest)?;
            if let Some(parent) = root.parent() {
                sync_dir(parent)?;
            }
        }
        let mut manifest: Manifest =
            serde_json::from_slice(&read(&manifest_path)?).map_err(|_| corrupt())?;
        if manifest.format != FORMAT {
            return Err(corrupt());
        }
        if root.join("privacy.json").exists() && root.join("append.json").exists() {
            // No serialized writer can publish both. A mixed copy is not recovery authority.
            return Err(corrupt());
        }
        if root.join("privacy.json").exists() {
            manifest = recover_privacy(root, &manifest)?;
        }
        if root.join("append.json").exists() {
            recover_append(root, &manifest)?;
        }
        let events_path = root.join("events.jsonl");
        regular(&events_path)?;
        let mut events = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&events_path)
            .map_err(backend)?;
        if events.metadata().map_err(backend)?.len() < manifest.length {
            return Err(corrupt());
        }
        let mut committed = Vec::new();
        (&mut events)
            .take(manifest.length)
            .read_to_end(&mut committed)
            .map_err(backend)?;
        let transactions = decode(&committed, &manifest)?;
        #[cfg(test)]
        crate::cost::charge(root, |cost| {
            cost.frames_chained += transactions.len() as u64;
        });
        if events.metadata().map_err(backend)?.len() > manifest.length {
            return Err(corrupt());
        }
        let content = Content::of(&committed);
        sweep_unselected(root)?;
        sync_dir(root)?;
        Ok(Self {
            root: root.to_owned(),
            _lock: lock,
            manifest,
            transactions,
            content,
        })
    }

    pub fn append(&mut self, transaction: Value) -> Result<(), EventLogError> {
        let sequence = self.manifest.sequence.checked_add(1).ok_or_else(corrupt)?;
        let (line, digest) = encode(
            &self.manifest,
            sequence,
            &self.manifest.digest,
            transaction.clone(),
        )?;
        let mut next = self.manifest.clone();
        next.sequence = sequence;
        next.length = next
            .length
            .checked_add(line.len() as u64)
            .ok_or_else(corrupt)?;
        next.digest = digest;
        let intent = AppendIntent {
            before: self.manifest.clone(),
            after: next.clone(),
            frame: String::from_utf8(line.clone()).map_err(backend)?,
        };
        atomic_json(&self.root, "append.json", &intent)?;
        checkpoint("append-prepared");
        let mut file = OpenOptions::new()
            .write(true)
            .open(self.root.join("events.jsonl"))
            .map_err(backend)?;
        file.seek(SeekFrom::Start(self.manifest.length))
            .map_err(backend)?;
        let midpoint = line.len() / 2;
        file.write_all(&line[..midpoint]).map_err(backend)?;
        checkpoint("append-torn");
        file.write_all(&line[midpoint..])
            .and_then(|()| file.sync_all())
            .map_err(backend)?;
        // Failure after the rename can mean the commit exists. Do not encourage a new identity.
        checkpoint("append-synced");
        atomic_json(&self.root, "manifest.json", &next)
            .map_err(|_| EventLogError::UnknownCommit)?;
        checkpoint("append-committed");
        fs::remove_file(self.root.join("append.json"))
            .and_then(|()| File::open(&self.root)?.sync_all())
            .map_err(|_| EventLogError::UnknownCommit)?;
        #[cfg(test)]
        crate::cost::charge(&self.root, |cost| {
            cost.durability_barriers += 1;
        });
        self.manifest = next;
        self.transactions.push(transaction);
        self.content.absorb(&line);
        Ok(())
    }

    /// The hash of the committed bytes this journal decoded, for a test that resumes onto them.
    #[cfg(test)]
    pub fn content(&self) -> Content {
        self.content.clone()
    }

    pub fn extends(&self, observed: &Manifest) -> Result<bool, EventLogError> {
        extends_observed(&self.manifest, &self.transactions, observed)
    }

    /// Release the lock and keep what it protected: the manifest, the verified frames, and the
    /// hash of the committed bytes those frames were decoded from.
    pub fn into_parts(self) -> (Manifest, Vec<Value>, Content) {
        (self.manifest, self.transactions, self.content)
    }

    /// Take the lock and decide from `manifest.json` whether the committed history is exactly
    /// `observed` or extends it, reading and verifying only the frames past `observed.length`,
    /// chained from `observed.digest` to the new manifest digest.
    ///
    /// The committed bytes behind `observed` are re-read and hashed against `content` before any
    /// of them is trusted, so a frame damaged in place after this handle verified it is never
    /// served, folded onto, or appended after: it is handed to the complete opener, which refuses
    /// it before the caller can write.
    ///
    /// `None` hands the decision to the complete opener: a pending recovery intent, a missing
    /// manifest, a manifest that is not on the observed store and epoch or is shorter, a file
    /// whose length is not the committed length, a committed prefix that is not the bytes this
    /// handle verified, or a tail that does not chain. That path rereads and re-verifies
    /// everything and refuses with the same errors it always has.
    pub fn resume(
        root: &Path,
        observed: &Manifest,
        content: &Content,
    ) -> Result<Option<Resumed>, EventLogError> {
        if !physical_directory(root).map_err(backend)? {
            return Err(corrupt());
        }
        let lock_path = root.join("writer.lock");
        regular(&lock_path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .open(lock_path)
            .map_err(backend)?;
        lock.lock().map_err(backend)?;
        let Some((manifest, fresh, content)) = resumed_committed(root, observed, content)? else {
            return Ok(None);
        };
        if sweep_unselected(root)? {
            sync_dir(root)?;
        }
        Ok(Some(Resumed {
            lock,
            manifest,
            fresh,
            content,
        }))
    }

    /// Privacy is the only rewrite path. The caller supplies history with only erased data removed.
    pub fn privacy(&mut self, transactions: Vec<Value>) -> Result<(), EventLogError> {
        let mut next = self.manifest.clone();
        next.epoch = next.epoch.checked_add(1).ok_or_else(corrupt)?;
        next.sequence = 0;
        next.length = 0;
        next.digest = ZERO.into();
        let mut bytes = Vec::new();
        for transaction in &transactions {
            next.sequence += 1;
            let (line, digest) = encode(&next, next.sequence, &next.digest, transaction.clone())?;
            bytes.extend(line);
            next.digest = digest;
        }
        next.length = bytes.len() as u64;
        let replacement = self.root.join("privacy.next");
        if replacement.exists() {
            fs::remove_file(&replacement).map_err(backend)?;
        }
        write_synced(&replacement, &bytes)?;
        sync_dir(&self.root)?;
        let intent = PrivacyIntent {
            before: self.manifest.clone(),
            after: next,
            replacement_digest: hash(&bytes),
        };
        atomic_json(&self.root, "privacy.json", &intent)?;
        checkpoint("privacy-prepared");
        self.manifest = recover_privacy(&self.root, &self.manifest)
            .map_err(|_| EventLogError::UnknownCommit)?;
        #[cfg(test)]
        crate::cost::charge(&self.root, |cost| {
            cost.durability_barriers += 1;
        });
        self.transactions = transactions;
        self.content = Content::of(&bytes);
        Ok(())
    }
}

/// Decide, with the writers' lock already held, whether the committed history is still the one
/// behind `observed` — and what it gained since.
///
/// One walk, used by the writer's [`Journal::resume`] and by the reader's [`resume_strict`]: a
/// second copy of it would be a second chance to compare the wrong thing. It reads and decides and
/// nothing else. The sweep of unselected staging names belongs to the caller, because a writer
/// owes it and a reader owes none: a capture that removed a file would have changed the thing it
/// came to observe.
///
/// `None` hands the decision to the caller's complete opener: a pending recovery intent, a missing
/// manifest, a manifest that is not on the observed store and epoch or is shorter, a file whose
/// length is not the committed length, a committed prefix that is not the bytes this handle
/// verified, or a tail that does not chain.
fn resumed_committed(
    root: &Path,
    observed: &Manifest,
    content: &Content,
) -> Result<Option<(Manifest, Vec<Value>, Content)>, EventLogError> {
    // A reserved intent name is present when its directory entry exists, even when following that
    // entry reaches nothing. `Path::exists` follows links and reports a dangling one as absent, so
    // it is not the question being asked here; and an entry that cannot be inspected at all is a
    // doubt, which the complete opener resolves.
    if pending_intent_exists(root).unwrap_or(true) {
        return Ok(None);
    }
    let manifest_path = root.join("manifest.json");
    if !manifest_path.exists() {
        return Ok(None);
    }
    let manifest: Manifest =
        serde_json::from_slice(&read(&manifest_path)?).map_err(|_| corrupt())?;
    if manifest.format != FORMAT {
        return Err(corrupt());
    }
    if manifest.store != observed.store
        || manifest.epoch != observed.epoch
        || manifest.sequence < observed.sequence
        || manifest.length < observed.length
    {
        return Ok(None);
    }
    let events_path = root.join("events.jsonl");
    regular(&events_path)?;
    if fs::metadata(&events_path).map_err(backend)?.len() != manifest.length {
        return Ok(None);
    }
    // The committed prefix is re-read as raw bytes and hashed: cheaper than decoding and
    // rechaining it, and the only way a handle can tell that what it verified is still there.
    let mut events = File::open(&events_path).map_err(backend)?;
    let mut observed_content = Content::empty();
    let mut remaining = observed.length;
    let mut buffer = vec![0_u8; 64 * 1024];
    while remaining > 0 {
        let want = usize::try_from(remaining.min(buffer.len() as u64)).map_err(backend)?;
        let read = events.read(&mut buffer[..want]).map_err(backend)?;
        if read == 0 {
            return Ok(None);
        }
        observed_content.absorb(&buffer[..read]);
        remaining -= read as u64;
    }
    #[cfg(test)]
    crate::cost::charge(root, |cost| {
        cost.prefix_bytes_hashed += observed.length;
    });
    if observed_content.digest() != content.digest() {
        return Ok(None);
    }
    // Past the prefix: an unchanged head reads nothing more.
    let fresh = if manifest == *observed {
        Vec::new()
    } else {
        let mut tail = Vec::new();
        events.read_to_end(&mut tail).map_err(backend)?;
        match decode_chain(&tail, &manifest, observed.sequence, &observed.digest) {
            Ok(fresh) => {
                observed_content.absorb(&tail);
                fresh
            }
            Err(_) => return Ok(None),
        }
    };
    #[cfg(test)]
    crate::cost::charge(root, |cost| {
        cost.frames_chained += fresh.len() as u64;
    });
    Ok(Some((manifest, fresh, observed_content)))
}

/// Whether the committed history still contains the exact prefix a handle observed.
///
/// One implementation, used by the ordinary opener's per-handle guard and by the strict reader's:
/// a second copy of this walk is a second chance to compare the wrong thing.
pub(crate) fn extends_observed(
    manifest: &Manifest,
    transactions: &[Value],
    observed: &Manifest,
) -> Result<bool, EventLogError> {
    if observed.store != manifest.store
        || observed.epoch != manifest.epoch
        || observed.sequence > manifest.sequence
    {
        return Ok(false);
    }
    let mut previous = ZERO.to_owned();
    for (index, transaction) in transactions
        .iter()
        .take(usize::try_from(observed.sequence).map_err(backend)?)
        .enumerate()
    {
        previous = encode(manifest, index as u64 + 1, &previous, transaction.clone())?.1;
    }
    Ok(previous == observed.digest)
}

/// The lock, the committed manifest, the frames a handle has not verified yet, and the hash of
/// the committed bytes this resume re-read and validated.
pub(crate) struct Resumed {
    lock: File,
    pub manifest: Manifest,
    pub fresh: Vec<Value>,
    content: Content,
}

impl Resumed {
    /// Assemble the writer from the handle's verified frames followed by the fresh ones.
    pub fn into_journal(self, root: &Path, mut transactions: Vec<Value>) -> Journal {
        transactions.extend(self.fresh);
        Journal {
            root: root.to_owned(),
            _lock: self.lock,
            manifest: self.manifest,
            transactions,
            content: self.content,
        }
    }
}

/// Committed history a reader resumed onto, with the writers' lock held for as long as it lives.
///
/// The reader's counterpart to [`Resumed`]. It carries no root and has no `into_journal`: there is
/// no method here that could write, so there is none to forget to guard.
pub(crate) struct StrictResumed {
    // Kept open for the whole life of this value and released by its own drop: nothing reads it,
    // and there is deliberately no statement that releases it early. `Journal` holds the writers'
    // lock the same way and for the same reason.
    _lock: File,
    pub manifest: Manifest,
    pub fresh: Vec<Value>,
    content: Content,
}

impl StrictResumed {
    /// Run `read` with the lock still held, then release it and keep what it protected: the
    /// committed head, the frames past the head the handle observed, and the hash of the
    /// committed bytes this resume validated.
    ///
    /// The read is an argument rather than the caller's next statement on purpose. The rule is
    /// that the bytes a reader hands out are read under the writers' lock, and while the caller
    /// wrote "observe, then take the parts" that rule was two statements in one order and
    /// nothing else: swapping them read the content unlocked and left every case in this crate
    /// green (review 2, finding A3). Here the lock is `self`'s and `self` is alive until this
    /// function returns, so releasing it before the read is not something a caller can write.
    pub(crate) fn read_under_lock<T>(
        self,
        read: impl FnOnce() -> T,
    ) -> (T, Manifest, Vec<Value>, Content) {
        // No `drop` and no order to keep: `self` owns the lock until this function returns,
        // so `read` cannot run after it is released.
        let value = read();
        (value, self.manifest, self.fresh, self.content)
    }
}

/// Resume onto committed history without the authority to change any of it.
///
/// The read-only sibling of [`Journal::resume`], sharing its walk and adding nothing to it. Two
/// differences, both of them the difference between a writer and a reader: this removes no
/// staging name and synchronizes no directory, and it answers `None` rather than an error for
/// everything it cannot decide, because every refusal belongs to [`open_strict`], which reaches
/// them with the evidence left exactly where it was found.
pub(crate) fn resume_strict(
    root: &Path,
    observed: &Manifest,
    content: &Content,
) -> Option<StrictResumed> {
    // The member of this class that was missing, and an absence no mutation of present code
    // could reach: every check below follows a link in a path component, so a root that is not a
    // physical directory has to be refused before any of them. `None`, so `open_strict` makes
    // the refusal with the evidence where it was found, exactly as the rest of this function does.
    if !physical_directory(root).ok()? {
        return None;
    }
    let lock_path = root.join("writer.lock");
    regular(&lock_path).ok()?;
    // No create, no truncate: this opens the writers' own lock, it does not establish one.
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .truncate(false)
        .open(&lock_path)
        .ok()?;
    lock.lock().ok()?;
    let (manifest, fresh, content) = resumed_committed(root, observed, content).ok()??;
    Some(StrictResumed {
        _lock: lock,
        manifest,
        fresh,
        content,
    })
}

/// One committed history, read without the authority to change it.
///
/// The interprocess lock is held for as long as this value lives, exactly as an ordinary writer
/// holds it, so an observation cannot straddle somebody else's commit.
pub(crate) struct Strict {
    lock: File,
    pub manifest: Manifest,
    pub transactions: Vec<Value>,
    content: Content,
}

impl Strict {
    /// Run `read` with the lock still held, then release it and keep what it protected: the
    /// committed head, the verified frames, and the hash of the committed bytes those frames
    /// were decoded from.
    ///
    /// The reader's half of the same rule [`StrictResumed::read_under_lock`] states, on the path
    /// that rereads everything. Both paths hand blob objects to a caller and both read them here.
    pub(crate) fn read_under_lock<T>(
        self,
        read: impl FnOnce() -> T,
    ) -> (T, Manifest, Vec<Value>, Content) {
        // No `drop` and no order to keep: `self` owns the lock until this function returns,
        // so `read` cannot run after it is released.
        let value = read();
        (value, self.manifest, self.transactions, self.content)
    }

    /// Turn a fully validated strict observation into a writer without reopening or recovering it.
    pub(crate) fn into_journal(self, root: &Path) -> Journal {
        Journal {
            root: root.to_owned(),
            _lock: self.lock,
            manifest: self.manifest,
            transactions: self.transactions,
            content: self.content,
        }
    }
}

fn unavailable(reason: &str) -> CaptureError {
    CaptureError::Store(EventLogError::Backend(reason.to_owned()))
}

fn damaged() -> CaptureError {
    CaptureError::Corrupt {
        material: CaptureMaterial::Journal,
    }
}

/// Open an existing store for reading only.
///
/// [`Journal::open`] is the wrong entry point for an inspector, and not by a little: it creates a
/// store where there is none, runs append and privacy recovery, truncates a torn frame, deletes
/// unselected staging files and rewrites `manifest.json` — all before the caller's read closure is
/// ever invoked. This path does none of it. A missing root, lock or manifest is a storage refusal
/// rather than an invitation to make one; a pending durable intent is somebody else's recovery;
/// and damage is refused with the evidence left exactly where it was found.
pub(crate) fn open_strict(root: &Path) -> Result<Strict, CaptureError> {
    if !physical_directory(root).map_err(|_| unavailable("file store root is not present"))? {
        return Err(unavailable("file store root is not a physical directory"));
    }
    let lock_path = root.join("writer.lock");
    if !fs::symlink_metadata(&lock_path)
        .map_err(|_| unavailable("file store writer lock is not present"))?
        .is_file()
    {
        return Err(damaged());
    }
    // No create, no truncate: this opens the writers' own lock, it does not establish one.
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .truncate(false)
        .open(&lock_path)
        .map_err(|_| unavailable("file store writer lock cannot be opened"))?;
    lock.lock()
        .map_err(|_| unavailable("file store writer lock cannot be held"))?;
    // A pending intent is the authority of an ordinary opener. Reading past one would either
    // repair history without permission or hand back bytes an erasure has already claimed.
    if pending_intent_exists(root)? {
        return Err(CaptureError::RecoveryRequired);
    }
    let manifest_path = root.join("manifest.json");
    if !fs::symlink_metadata(&manifest_path)
        .map_err(|_| unavailable("file store manifest is not present"))?
        .is_file()
    {
        return Err(damaged());
    }
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(&manifest_path).map_err(|_| damaged())?)
            .map_err(|_| damaged())?;
    if manifest.format != FORMAT {
        return Err(damaged());
    }
    let events_path = root.join("events.jsonl");
    if !fs::symlink_metadata(&events_path)
        .map_err(|_| damaged())?
        .is_file()
    {
        return Err(damaged());
    }
    let committed = fs::read(&events_path).map_err(|_| damaged())?;
    // Surplus bytes are as much a refusal as missing ones: neither is history this manifest commits.
    if committed.len() as u64 != manifest.length {
        return Err(damaged());
    }
    let transactions = decode(&committed, &manifest).map_err(|_| damaged())?;
    #[cfg(test)]
    crate::cost::charge(root, |cost| {
        cost.frames_chained += transactions.len() as u64;
    });
    let content = Content::of(&committed);
    Ok(Strict {
        lock,
        manifest,
        transactions,
        content,
    })
}

/// Observe reserved intent names as directory entries, without following or changing them.
///
/// `Path::exists` follows links and treats every metadata error as absence. A strict reader may
/// read past only an actually absent name: a dangling link is still recovery authority, while an
/// inability to inspect either name is an operational refusal.
fn pending_intent_exists(root: &Path) -> Result<bool, CaptureError> {
    for name in ["append.json", "privacy.json"] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(unavailable("file store pending intent cannot be inspected")),
        }
    }
    Ok(false)
}

/// Remove reserved names that no durable intent ever selected, reporting whether any was there.
///
/// A complete open has always done this; a resumed handle does it too, because the files may hold
/// sensitive bytes and a handle that stays open across a failed rename is otherwise the one path
/// that never sweeps them. The caller syncs the directory when something was actually removed.
fn sweep_unselected(root: &Path) -> Result<bool, EventLogError> {
    let mut swept = false;
    for entry in fs::read_dir(root).map_err(backend)? {
        let entry = entry.map_err(backend)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "privacy.next"
            || name.strip_prefix(".write-").is_some_and(|id| {
                id.len() == 36 && id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
            })
        {
            regular(&entry.path())?;
            fs::remove_file(entry.path()).map_err(backend)?;
            swept = true;
        }
    }
    Ok(swept)
}

fn recover_append(root: &Path, current: &Manifest) -> Result<(), EventLogError> {
    let intent: AppendIntent =
        serde_json::from_slice(&read(&root.join("append.json"))?).map_err(|_| corrupt())?;
    if current != &intent.before && current != &intent.after {
        return Err(corrupt());
    }
    let mut bytes = read(&root.join("events.jsonl"))?;
    let boundary = usize::try_from(intent.before.length).map_err(backend)?;
    if bytes.len() < boundary {
        return Err(corrupt());
    }
    decode(&bytes[..boundary], &intent.before)?;
    let suffix = &bytes[boundary..];
    if !intent.frame.as_bytes().starts_with(suffix) {
        return Err(corrupt());
    }
    let mut planned = bytes[..boundary].to_vec();
    planned.extend(intent.frame.as_bytes());
    decode(&planned, &intent.after)?;
    if current == &intent.after {
        decode(&bytes, current)?;
    } else {
        bytes.truncate(boundary);
        let file = OpenOptions::new()
            .write(true)
            .open(root.join("events.jsonl"))
            .map_err(backend)?;
        file.set_len(intent.before.length)
            .and_then(|()| file.sync_all())
            .map_err(backend)?;
    }
    fs::remove_file(root.join("append.json")).map_err(backend)?;
    sync_dir(root)
}

fn encode(
    manifest: &Manifest,
    sequence: u64,
    previous: &str,
    transaction: Value,
) -> Result<(Vec<u8>, String), EventLogError> {
    let mut frame = Frame {
        format: FORMAT.into(),
        store: manifest.store.clone(),
        epoch: manifest.epoch,
        sequence,
        previous: previous.into(),
        transaction,
        digest: String::new(),
    };
    frame.digest = hash(&serde_json::to_vec(&frame).map_err(backend)?);
    let mut bytes = serde_json::to_vec(&frame).map_err(backend)?;
    bytes.push(b'\n');
    Ok((bytes, frame.digest))
}
fn decode(bytes: &[u8], manifest: &Manifest) -> Result<Vec<Value>, EventLogError> {
    if bytes.len() as u64 != manifest.length {
        return Err(corrupt());
    }
    decode_chain(bytes, manifest, 0, ZERO)
}
/// Verify the frames `sequence + 1..=manifest.sequence`, chained from `previous` to the manifest
/// digest. The complete history starts at zero; a resumed handle starts at its observed head.
fn decode_chain(
    bytes: &[u8],
    manifest: &Manifest,
    mut sequence: u64,
    previous: &str,
) -> Result<Vec<Value>, EventLogError> {
    if !bytes.is_empty() && bytes.last() != Some(&b'\n') {
        return Err(corrupt());
    }
    let mut previous = previous.to_owned();
    let mut transactions = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let mut frame: Frame = serde_json::from_slice(line).map_err(|_| corrupt())?;
        sequence = sequence.checked_add(1).ok_or_else(corrupt)?;
        if frame.format != FORMAT
            || frame.store != manifest.store
            || frame.epoch != manifest.epoch
            || frame.sequence != sequence
            || frame.previous != previous
        {
            return Err(corrupt());
        }
        let digest = std::mem::take(&mut frame.digest);
        if hash(&serde_json::to_vec(&frame).map_err(backend)?) != digest {
            return Err(corrupt());
        }
        previous = digest;
        transactions.push(frame.transaction);
    }
    if sequence != manifest.sequence || previous != manifest.digest {
        return Err(corrupt());
    }
    Ok(transactions)
}
fn recover_privacy(root: &Path, current: &Manifest) -> Result<Manifest, EventLogError> {
    let intent: PrivacyIntent =
        serde_json::from_slice(&read(&root.join("privacy.json"))?).map_err(|_| corrupt())?;
    if (current != &intent.before && current != &intent.after)
        || intent.before.store != intent.after.store
        || intent.after.epoch != intent.before.epoch.checked_add(1).ok_or_else(corrupt)?
    {
        return Err(corrupt());
    }
    let events = read(&root.join("events.jsonl"))?;
    if hash(&events) == intent.replacement_digest {
        decode(&events, &intent.after)?;
    } else {
        decode(&events, &intent.before)?;
        let replacement = read(&root.join("privacy.next"))?;
        if hash(&replacement) != intent.replacement_digest {
            return Err(corrupt());
        }
        decode(&replacement, &intent.after)?;
        fs::rename(root.join("privacy.next"), root.join("events.jsonl")).map_err(backend)?;
        sync_dir(root)?;
        checkpoint("privacy-renamed");
    }
    atomic_json(root, "manifest.json", &intent.after)?;
    checkpoint("privacy-committed");
    if root.join("privacy.next").exists() {
        fs::remove_file(root.join("privacy.next")).map_err(backend)?;
    }
    fs::remove_file(root.join("privacy.json")).map_err(backend)?;
    sync_dir(root)?;
    Ok(intent.after)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::process::Command;

    // Invoked only by the parent tests with an isolated directory and failpoint.
    #[test]
    fn crash_child() {
        let Ok(root) = std::env::var("EVENTLOG_FILE_CRASH_ROOT") else {
            return;
        };
        if std::env::var_os("EVENTLOG_FILE_ATOMIC_BLOB").is_some() {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let store = crate::FileEventStore::open(&root).await.unwrap();
                let request = eventlog_conformance::atomic_blob_request("crash", "one", "content");
                let result =
                    eventlog_core::AtomicBlobEventStore::append_group_with_blobs(&store, &request)
                        .await;
                if std::env::var_os("EVENTLOG_FILE_ATOMIC_BLOB_FAIL_CLEANUP").is_some() {
                    assert_eq!(result, Err(EventLogError::UnknownCommit));
                } else {
                    result.unwrap();
                }
            });
            if std::env::var_os("EVENTLOG_FILE_ATOMIC_BLOB_FAIL_CLEANUP").is_some() {
                return;
            }
            panic!("atomic blob failpoint was not reached");
        }
        if std::env::var_os("EVENTLOG_FILE_PROVIDER_PRIVACY").is_some() {
            use eventlog_core::EventStore;
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let store = crate::FileEventStore::open(&root).await.unwrap();
                store
                    .forget_tenant(&eventlog_core::TenantId::new("private-owner").unwrap())
                    .await
                    .unwrap();
            });
            panic!("provider failpoint was not reached");
        }
        let mut journal = Journal::open(Path::new(&root)).unwrap();
        if std::env::var("EVENTLOG_FILE_CRASH_AT")
            .unwrap()
            .starts_with("privacy")
        {
            journal.privacy(vec![json!({"value": "erased"})]).unwrap();
        } else {
            journal.append(json!({"value": "new"})).unwrap();
        }
        panic!("failpoint was not reached");
    }
    fn crash(root: &Path, point: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "journal::tests::crash_child", "--nocapture"])
            .env("EVENTLOG_FILE_CRASH_ROOT", root)
            .env("EVENTLOG_FILE_CRASH_AT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn atomic_blob_crash_boundaries_recover_only_complete_publication() {
        use eventlog_core::EventStore;
        for point in [
            "blob-staged",
            "append-prepared",
            "append-torn",
            "append-synced",
            "append-committed",
            "blob-cleaned",
        ] {
            let root = tempfile::tempdir().unwrap();
            drop(crate::FileEventStore::open(root.path()).await.unwrap());
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "journal::tests::crash_child", "--nocapture"])
                .env("EVENTLOG_FILE_CRASH_ROOT", root.path())
                .env("EVENTLOG_FILE_CRASH_AT", point)
                .env("EVENTLOG_FILE_ATOMIC_BLOB", "1")
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let request = eventlog_conformance::atomic_blob_request("crash", "one", "content");
            let store = crate::FileEventStore::open(root.path()).await.unwrap();
            let committed = matches!(point, "append-committed" | "blob-cleaned");
            let events = store
                .read_stream(&request.group.appends[0].stream, 0, 100)
                .await
                .unwrap()
                .events;
            assert_eq!(events.len(), usize::from(committed), "{point}");
            assert_eq!(
                store
                    .get_blob(&request.group.tenant, "content")
                    .await
                    .unwrap(),
                committed.then(|| request.blobs[0].bytes.clone()),
                "{point}"
            );
            assert_eq!(
                fs::read_dir(root.path().join("blobs")).unwrap().count(),
                usize::from(committed),
                "unbound crash staging reclaimed at {point}"
            );
            let resolved =
                eventlog_core::AtomicBlobEventStore::append_group_with_blobs(&store, &request)
                    .await
                    .unwrap();
            assert_eq!(resolved.deduplicated, committed);
            if committed {
                assert_eq!(resolved.appends[0].events, events);
            }
            assert_eq!(
                store
                    .stream_version(&request.group.appends[0].stream)
                    .await
                    .unwrap(),
                Some(1)
            );
        }
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn atomic_blob_postcommit_cleanup_failure_keeps_committed_content() {
        use eventlog_core::EventStore;
        let root = tempfile::tempdir().unwrap();
        drop(crate::FileEventStore::open(root.path()).await.unwrap());
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "journal::tests::crash_child", "--nocapture"])
            .env("EVENTLOG_FILE_CRASH_ROOT", root.path())
            .env("EVENTLOG_FILE_ATOMIC_BLOB", "1")
            .env("EVENTLOG_FILE_ATOMIC_BLOB_FAIL_CLEANUP", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        // Remove only the obstruction deliberately injected above, then recover normally.
        fs::remove_dir(
            root.path()
                .join("blobs/00000000-0000-0000-0000-000000000000"),
        )
        .unwrap();
        let store = crate::FileEventStore::open(root.path()).await.unwrap();
        let request = eventlog_conformance::atomic_blob_request("crash", "one", "content");
        let events = store
            .read_stream(&request.group.appends[0].stream, 0, 10)
            .await
            .unwrap()
            .events;
        assert_eq!(events.len(), 1);
        assert_eq!(
            store
                .get_blob(&request.group.tenant, "content")
                .await
                .unwrap(),
            Some(request.blobs[0].bytes.clone())
        );
        let result = eventlog_core::AtomicBlobEventStore::append_group_with_blobs(&store, &request)
            .await
            .unwrap();
        assert!(result.deduplicated);
        assert_eq!(result.appends[0].events, events);
    }

    #[test]
    fn process_death_at_each_append_boundary() {
        for point in [
            "append-prepared",
            "append-torn",
            "append-synced",
            "append-committed",
        ] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut journal = Journal::open(root.path()).unwrap();
                journal.append(json!({"value": "original"})).unwrap();
            }
            crash(root.path(), point);
            let journal = Journal::open(root.path()).unwrap();
            assert_eq!(journal.transactions[0], json!({"value":"original"}));
            assert_eq!(
                journal.transactions.len(),
                if point == "append-committed" { 2 } else { 1 },
                "{point}"
            );
            assert!(!root.path().join("append.json").exists());
            assert_eq!(
                fs::metadata(root.path().join("events.jsonl"))
                    .unwrap()
                    .len(),
                journal.manifest.length
            );
        }
    }
    #[test]
    fn process_death_at_each_privacy_boundary() {
        for point in ["privacy-prepared", "privacy-renamed", "privacy-committed"] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut journal = Journal::open(root.path()).unwrap();
                journal
                    .append(json!({"value": "sensitive-old-body"}))
                    .unwrap();
            }
            crash(root.path(), point);
            let journal = Journal::open(root.path()).unwrap();
            assert_eq!(journal.transactions, vec![json!({"value":"erased"})]);
            for entry in fs::read_dir(root.path()).unwrap() {
                let bytes = fs::read(entry.unwrap().path()).unwrap();
                assert!(!String::from_utf8_lossy(&bytes).contains("sensitive-old-body"));
            }
        }
    }
    #[test]
    fn committed_damage_and_unproven_suffix_are_never_repaired() {
        for mode in ["truncate", "alter", "suffix", "merge"] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut journal = Journal::open(root.path()).unwrap();
                journal.append(json!({"number": 17})).unwrap();
            }
            let path = root.path().join("events.jsonl");
            let mut bytes = fs::read(&path).unwrap();
            match mode {
                "truncate" => {
                    bytes.pop();
                }
                "alter" => {
                    let index = bytes.windows(2).position(|b| b == b"17").unwrap();
                    bytes[index] = b'2';
                }
                "suffix" => bytes.extend(b"{\"torn\":"),
                "merge" => bytes.extend(bytes.clone()),
                _ => unreachable!(),
            }
            fs::write(&path, &bytes).unwrap();
            assert!(Journal::open(root.path()).is_err(), "{mode}");
            assert_eq!(
                fs::read(&path).unwrap(),
                bytes,
                "refusal preserves evidence"
            );
        }
    }
    #[test]
    fn divergent_longer_history_does_not_extend_observed_head() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(root.path()).unwrap();
        journal.append(json!({"branch":"left"})).unwrap();
        let observed = journal.manifest.clone();
        // Independently encoded fork, with the same store identity and greater sequence.
        let mut fork = journal.manifest.clone();
        let (a, digest) = encode(&fork, 1, ZERO, json!({"branch":"right"})).unwrap();
        let (b, last) = encode(&fork, 2, &digest, json!({"next":true})).unwrap();
        let bytes = [a, b].concat();
        fork.sequence = 2;
        fork.length = bytes.len() as u64;
        fork.digest = last;
        fs::write(root.path().join("events.jsonl"), &bytes).unwrap();
        fs::write(
            root.path().join("manifest.json"),
            serde_json::to_vec(&fork).unwrap(),
        )
        .unwrap();
        drop(journal);
        let fork = Journal::open(root.path()).unwrap();
        assert!(!fork.extends(&observed).unwrap());
    }

    #[test]
    fn resume_reads_only_frames_past_the_observed_head() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(root.path()).unwrap();
        journal.append(json!({"value": 1})).unwrap();
        let observed = journal.manifest.clone();
        let observed_content = journal.content();
        drop(journal);
        let same = Journal::resume(root.path(), &observed, &observed_content)
            .unwrap()
            .unwrap();
        assert!(same.fresh.is_empty());
        assert_eq!(same.manifest, observed);
        drop(same);
        let mut journal = Journal::open(root.path()).unwrap();
        journal.append(json!({"value": 2})).unwrap();
        journal.append(json!({"value": 3})).unwrap();
        let head = journal.manifest.clone();
        let head_content = journal.content();
        drop(journal);
        let extended = Journal::resume(root.path(), &observed, &observed_content)
            .unwrap()
            .unwrap();
        assert_eq!(extended.fresh, [json!({"value": 2}), json!({"value": 3})]);
        let journal = extended.into_journal(root.path(), vec![json!({"value": 1})]);
        assert_eq!(journal.manifest, head);
        assert_eq!(journal.transactions.len(), 3);
        drop(journal);
        // Shorter than observed: the complete opener decides.
        let bytes = fs::read(root.path().join("events.jsonl")).unwrap();
        fs::write(
            root.path().join("events.jsonl"),
            &bytes[..usize::try_from(observed.length).unwrap()],
        )
        .unwrap();
        fs::write(
            root.path().join("manifest.json"),
            serde_json::to_vec(&observed).unwrap(),
        )
        .unwrap();
        assert!(
            Journal::resume(root.path(), &head, &head_content)
                .unwrap()
                .is_none()
        );
        // The observed prefix left exactly as this handle verified it, and one more frame that
        // does not chain from the observed digest: the prefix hash passes and the chain check is
        // what refuses.
        let mut unchained = head.clone();
        let (line, digest) = encode(&unchained, 4, ZERO, json!({"value": 11})).unwrap();
        let mut rebuilt = bytes.clone();
        rebuilt.extend(line);
        unchained.sequence = 4;
        unchained.length = rebuilt.len() as u64;
        unchained.digest = digest;
        fs::write(root.path().join("events.jsonl"), &rebuilt).unwrap();
        fs::write(
            root.path().join("manifest.json"),
            serde_json::to_vec(&unchained).unwrap(),
        )
        .unwrap();
        assert!(
            Journal::resume(root.path(), &head, &head_content)
                .unwrap()
                .is_none()
        );
        // A fork whose first three frames have the observed lengths and one frame more: the tail
        // starts exactly at the observed offset and must fail to chain from the observed digest.
        let mut fork = head.clone();
        let mut previous = ZERO.to_owned();
        let mut forked = Vec::new();
        for (sequence, value) in [(1, 7), (2, 8), (3, 9), (4, 10)] {
            let (line, digest) =
                encode(&fork, sequence, &previous, json!({"value": value})).unwrap();
            forked.extend(line);
            previous = digest;
        }
        fork.sequence = 4;
        fork.length = forked.len() as u64;
        fork.digest = previous;
        fs::write(root.path().join("events.jsonl"), &forked).unwrap();
        fs::write(
            root.path().join("manifest.json"),
            serde_json::to_vec(&fork).unwrap(),
        )
        .unwrap();
        assert!(
            Journal::resume(root.path(), &head, &head_content)
                .unwrap()
                .is_none()
        );
        // A privacy epoch: the complete opener decides, while the new head resumes as itself.
        let mut journal = Journal::open(root.path()).unwrap();
        journal.privacy(vec![json!({"value": "erased"})]).unwrap();
        let sanitized = journal.manifest.clone();
        let sanitized_content = journal.content();
        drop(journal);
        assert!(
            Journal::resume(root.path(), &fork, &Content::of(&forked))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            Journal::resume(root.path(), &sanitized, &sanitized_content)
                .unwrap()
                .unwrap()
                .manifest,
            sanitized
        );
    }

    #[test]
    fn a_resumed_handle_sweeps_unselected_staging_files() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(root.path()).unwrap();
        journal.append(json!({"value": 1})).unwrap();
        let observed = journal.manifest.clone();
        let content = journal.content();
        drop(journal);
        // Neither name was ever selected by a durable intent, and both may hold sensitive bytes.
        let staging = root.path().join(format!(".write-{}", new_event_id()));
        fs::write(&staging, b"unselected").unwrap();
        let replacement = root.path().join("privacy.next");
        fs::write(&replacement, b"unselected").unwrap();
        let resumed = Journal::resume(root.path(), &observed, &content)
            .unwrap()
            .unwrap();
        assert_eq!(resumed.manifest, observed);
        drop(resumed);
        assert!(
            !staging.exists(),
            "a resumed handle removes .write-<uuid> staging files, as a complete open does"
        );
        assert!(
            !replacement.exists(),
            "a resumed handle removes privacy.next, as a complete open does"
        );
    }

    #[test]
    fn mixed_recovery_intents_refuse_before_changing_authority() {
        let root = tempfile::tempdir().unwrap();
        drop(Journal::open(root.path()).unwrap());
        crash(root.path(), "append-committed");
        let stale_append = fs::read(root.path().join("append.json")).unwrap();
        drop(Journal::open(root.path()).unwrap());
        crash(root.path(), "privacy-prepared");
        fs::write(root.path().join("append.json"), stale_append).unwrap();
        let events = fs::read(root.path().join("events.jsonl")).unwrap();
        let manifest = fs::read(root.path().join("manifest.json")).unwrap();
        assert!(matches!(
            Journal::open(root.path()),
            Err(EventLogError::Backend(_))
        ));
        assert_eq!(fs::read(root.path().join("events.jsonl")).unwrap(), events);
        assert_eq!(
            fs::read(root.path().join("manifest.json")).unwrap(),
            manifest
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn privacy_crash_cleans_cached_bodies_and_blobs_on_open() {
        use eventlog_core::{EventStore, Expected, Snapshot, StreamId, TenantId};
        for point in ["privacy-prepared", "privacy-renamed", "privacy-committed"] {
            let root = tempfile::tempdir().unwrap();
            let owner = TenantId::new("private-owner").unwrap();
            let stream = StreamId::new(owner.clone(), "item", "one").unwrap();
            let store = crate::FileEventStore::open(root.path()).await.unwrap();
            store
                .append(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.created", 1)],
                    &eventlog_conformance::meta("first", &json!({})),
                )
                .await
                .unwrap();
            store
                .put_blob(&owner, "content", b"private-content-marker")
                .await
                .unwrap();
            let generation = store.snapshot_generation(&stream).await.unwrap().unwrap();
            store
                .save_snapshot_checked(
                    &stream,
                    &Snapshot {
                        version: 1,
                        state_schema_version: 1,
                        state: json!({"value":"private-content-marker"}),
                        recorded_at: time::OffsetDateTime::UNIX_EPOCH,
                    },
                    &generation,
                )
                .await
                .unwrap();
            drop(store);
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "journal::tests::crash_child", "--nocapture"])
                .env("EVENTLOG_FILE_CRASH_ROOT", root.path())
                .env("EVENTLOG_FILE_CRASH_AT", point)
                .env("EVENTLOG_FILE_PROVIDER_PRIVACY", "1")
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let store = crate::FileEventStore::open(root.path()).await.unwrap();
            assert_eq!(store.stream_version(&stream).await.unwrap(), None);
            assert_eq!(store.get_blob(&owner, "content").await.unwrap(), None);
            for path in [root.path().join(".cache"), root.path().join("blobs")] {
                assert_eq!(fs::read_dir(path).unwrap().count(), 0, "{point}");
            }
        }
    }
    /// One shape of damage, every entry point onto a store root, and each one's own refusal.
    ///
    /// A symlink in a path component is followed by every check after it, so a root that is not a
    /// physical directory is refused at the root or nowhere. That makes the rule a class with one
    /// member per entry point, and `resume_strict` shipped as the member that was *absent* — an
    /// omission no mutation of code that is present can reach, which is why both independent
    /// reviews had to find it by hand. The five now share one predicate,
    /// [`physical_directory`], and keep the five different refusals they already owed: the two
    /// writer paths answer `corrupt()`, the strict opener answers the exact string a consumer
    /// reads, the store directory answers its own message, and the resumed reader answers `None`
    /// so that [`open_strict`] makes the refusal with the evidence where it was found.
    ///
    /// A sixth entry point that hand-writes the check instead of calling the predicate is caught
    /// by `every_root_directory_decision_goes_through_one_predicate`; a sixth that omits it
    /// entirely is caught by neither, and adding a row here is what that costs.
    #[cfg(unix)]
    #[test]
    fn a_root_that_is_not_a_physical_directory_is_refused_at_every_entry_point() {
        let directory = tempfile::tempdir().unwrap();
        let real = directory.path().join("real");
        let link = directory.path().join("link");
        let (observed, content) = {
            let mut journal = Journal::open(&real).expect("writable store");
            journal
                .append(json!({"value": "committed"}))
                .expect("committed frame");
            (journal.manifest.clone(), journal.content.clone())
        };

        // The control: every entry point reads the same store happily by its real name.
        assert_eq!(Journal::open_existing(&real).err(), None);
        assert!(
            Journal::resume(&real, &observed, &content)
                .expect("readable")
                .is_some()
        );
        assert!(open_strict(&real).is_ok());
        assert!(resume_strict(&real, &observed, &content).is_some());
        assert_eq!(crate::directory(&real).err(), None);

        std::os::unix::fs::symlink(&real, &link).expect("a link where the root was");
        assert!(
            link.is_dir(),
            "the fixture only means something if every later check follows the link"
        );

        assert_eq!(
            Journal::open(&link).err(),
            Some(corrupt()),
            "the ordinary opener minted commit authority through a link"
        );
        assert_eq!(
            Journal::open_existing(&link).err(),
            Some(corrupt()),
            "the provisioned opener read a root that is not a physical directory"
        );
        assert_eq!(
            Journal::resume(&link, &observed, &content).err(),
            Some(corrupt()),
            "the writer resumed onto a root that is not a physical directory"
        );
        assert_eq!(
            open_strict(&link).err().map(|error| format!("{error:?}")),
            Some(format!(
                "{:?}",
                unavailable("file store root is not a physical directory")
            )),
            "the strict opener changed the refusal a consumer reads"
        );
        assert!(
            resume_strict(&link, &observed, &content).is_none(),
            "the resumed reader served a root the strict opener refuses by name"
        );
        assert_eq!(
            crate::directory(&link).err(),
            Some(backend("store directory is not a physical directory")),
            "the store directory check read a link as a directory"
        );
    }

    /// The root-is-a-physical-directory decision has exactly one implementation.
    ///
    /// The defect this round answers was an *omission*, and an omission is not machine-checkable
    /// from here. A second copy of the decision is, and a second copy is how the five entry
    /// points diverged in the first place: the crate already refuses one for the resume walk and
    /// for `extends_observed`, for the same stated reason — a second copy is a second chance to
    /// compare the wrong thing.
    ///
    /// The needles are assembled at run time so that this case is not a hit on itself.
    #[test]
    fn every_root_directory_decision_goes_through_one_predicate() {
        let asked = concat!("symlink_", "metadata");
        let decided = concat!(".is_", "dir()");
        let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut written_by_hand = Vec::new();
        for entry in fs::read_dir(&source_root).expect("readable source directory") {
            let file = entry.expect("readable entry").path();
            if file.extension().is_none_or(|kind| kind != "rs") {
                continue;
            }
            let source = fs::read_to_string(&file).expect("readable source file");
            let collapsed = source.split_whitespace().collect::<Vec<_>>().join(" ");
            // Sources carry multi-byte characters, so every window start is moved forward to a
            // character boundary rather than sliced blind.
            let window = |text: &str, from: usize, to: usize| {
                let start = (from..=to)
                    .find(|index| text.is_char_boundary(*index))
                    .unwrap_or(to);
                text[start..to].to_owned()
            };
            for (at, _) in collapsed.match_indices(decided) {
                let context = &window(&collapsed, at.saturating_sub(200), at);
                // A question that follows links is a different question; this one does not.
                if !context.contains(asked) {
                    continue;
                }
                if context.contains("fn physical_directory(") {
                    continue;
                }
                written_by_hand.push(format!(
                    "{}: ...{}{decided}",
                    file.file_name()
                        .unwrap_or(file.as_os_str())
                        .to_string_lossy(),
                    window(context, context.len().saturating_sub(70), context.len())
                ));
            }
        }
        assert!(
            written_by_hand.is_empty(),
            "the root-directory decision is written by hand outside physical_directory, which is \
             how the five entry points came to disagree:\n  {}",
            written_by_hand.join("\n  ")
        );
    }
}
