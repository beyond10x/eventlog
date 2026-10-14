//! Durable JSONL framing. The manifest is the commit point, never a best-effort cache.
use eventlog_core::{CaptureError, CaptureMaterial, EventLogError, InspectionError, new_event_id};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

const FORMAT: &str = "eventlog-file/1";
/// The segmented layout: sealed segments under `segments/`, then the active one in
/// `events.jsonl`. A store is written in it only when its owner asked for it at creation
/// ([`FileLayout::Segmented`]); [`FORMAT`] stays the default and is read by every release.
const SEGMENTED_FORMAT: &str = "eventlog-file/2";
/// The seal size [`FileLayout::segmented`] chooses: a resume inside the untrusted window compares
/// at most about this many bytes of the active segment, beside the young sealed ones.
const DEFAULT_SEAL_AT: u64 = 256 * 1024;
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

#[cfg(test)]
#[path = "adversary2_compare_once.rs"]
mod adversary2_compare_once;
#[cfg(test)]
#[path = "adversary_compare_once.rs"]
mod adversary_compare_once;

/// The on-disk layout a store is created with. A store that already exists keeps its own: no
/// release migrates one, and every release reads both.
///
/// `ess/file-layout/` models it (`eventlog.file_layout.Layout`, `SegmentedPolicy`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum FileLayout {
    /// `eventlog-file/1`: the whole committed history in `events.jsonl`. The default.
    #[default]
    Single,
    /// `eventlog-file/2`: sealed segments under `segments/`, then the active segment in
    /// `events.jsonl`. An append that finds the active segment holding at least `seal_at`
    /// committed bytes seals it first. A sealed segment is never written again, so once its
    /// inode stamp is trusted a resumed handle need not re-read it; a resume inside the
    /// untrusted window reads the active segment and the young sealed ones, not the whole history.
    /// `seal_at` must be at least 1 and is persisted in the store's manifest.
    Segmented {
        /// The committed size of the active segment at which the next append seals it.
        seal_at: u64,
    },
}

impl FileLayout {
    /// The segmented layout at the default seal size (256 KiB).
    #[must_use]
    pub const fn segmented() -> Self {
        Self::Segmented {
            seal_at: DEFAULT_SEAL_AT,
        }
    }
}

/// The manifest's segment record of an `eventlog-file/2` store (`eventlog.file_layout.Segments`):
/// the committed length of every sealed segment, in history order. The active segment holds the
/// rest of the manifest's committed length.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Segments {
    seal_at: u64,
    sealed: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    format: String,
    pub store: String,
    pub epoch: u64,
    pub sequence: u64,
    pub length: u64,
    pub digest: String,
    /// Present exactly on an `eventlog-file/2` manifest, so an `eventlog-file/1` manifest is
    /// written byte for byte as before, and a released reader refuses the new layout twice over:
    /// by its format and by this unknown field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    segments: Option<Segments>,
}

impl Manifest {
    /// Whether this manifest names a layout this release reads, and names it consistently: every
    /// sealed segment holds at least one byte and they fit inside the committed length.
    fn known(&self) -> bool {
        match (self.format.as_str(), &self.segments) {
            (FORMAT, None) => true,
            (SEGMENTED_FORMAT, Some(segments)) => {
                segments.seal_at >= 1
                    && segments.sealed.iter().all(|length| *length > 0)
                    && segments
                        .sealed
                        .iter()
                        .try_fold(0_u64, |sum, length| sum.checked_add(*length))
                        .is_some_and(|sum| sum <= self.length)
            }
            _ => false,
        }
    }

    /// The committed lengths of the sealed segments; none on `eventlog-file/1`.
    fn sealed(&self) -> &[u64] {
        self.segments
            .as_ref()
            .map_or(&[], |segments| segments.sealed.as_slice())
    }

    /// The committed length of `events.jsonl`: everything on `eventlog-file/1`.
    fn active_length(&self) -> u64 {
        self.length.saturating_sub(
            self.sealed()
                .iter()
                .fold(0, |sum, l| sum.saturating_add(*l)),
        )
    }

    fn seal_at(&self) -> Option<u64> {
        self.segments.as_ref().map(|segments| segments.seal_at)
    }
}

/// Where sealed segment `index` lives. Its name is its position, so the manifest's list is all
/// a reader needs to find every committed byte.
fn segment_path(root: &Path, index: usize) -> PathBuf {
    root.join("segments").join(format!("{index:010}.jsonl"))
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

/// An open `writer.lock` whose `flock` this value holds, released by this value's own drop.
///
/// Closing the descriptor is not a release. A `flock` belongs to the open file description, and
/// any `fork` in the process — `Command::spawn` from any thread — copies the descriptor into a
/// child that keeps the description, and so the lock, alive until it execs or exits. A lock
/// released by closing therefore outlives its holder by somebody else's fork window, which an
/// inspection's `try_lock` reports as `SourceBusy`. `LOCK_UN` releases the description's lock
/// whoever else still has it open, so the lock lives exactly as long as this value.
/// (story:strict-reader-lock-case-races-a-forked-crash-child)
struct WriterLock(File);

impl WriterLock {
    /// Wait for the lock on `file`, and hold it for as long as the returned value lives.
    fn hold(file: File) -> std::io::Result<Self> {
        file.lock()?;
        Ok(Self(file))
    }

    /// Take the lock on `file` only if nobody holds it now.
    fn try_hold(file: File) -> Result<Self, std::fs::TryLockError> {
        file.try_lock()?;
        Ok(Self(file))
    }
}

impl Drop for WriterLock {
    fn drop(&mut self) {
        // Nothing to report to: a failed unlock leaves the close below to release it.
        let _ = self.0.unlock();
    }
}

pub(crate) struct Journal {
    root: PathBuf,
    // Kept open for the entire read/decide/commit cycle. Each caller opens its own description.
    _lock: WriterLock,
    pub manifest: Manifest,
    pub transactions: Vec<Value>,
    content: Content,
}

/// The committed bytes of `events.jsonl` one handle verified, to the committed length.
///
/// A handle that resumes onto a head it already chained trusts committed bytes it is not decoding
/// again. This keeps those bytes as it verified them, so [`Journal::resume`] can re-read the
/// file's committed prefix, compare it with them byte for byte, and hand a history whose committed
/// prefix is no longer the one this handle verified to the complete opener, which refuses it.
/// Without it, damage inside the committed prefix is invisible to every later transaction,
/// including the one that appends onto it.
///
/// The bytes live in a [`Retained`] buffer and this is a view of its first `length` bytes, so a
/// clone costs a reference count and the writer's and the reader's views of one handle can hold
/// one copy between them ([`Content::adopt`]). Nothing here is persisted.
#[derive(Clone)]
pub(crate) struct Content {
    retained: Arc<Retained>,
    /// How many leading bytes of `retained` this view verified. Bytes past it, if any, were added
    /// by another view of the same buffer and are nothing this one vouches for.
    length: usize,
    /// The inode metadata `events.jsonl` had while these bytes were read from it, when it held
    /// still for the whole read and was last changed well before it began. While the file still
    /// carries exactly this stamp, its bytes are these bytes, and a resume need not read them.
    stamp: Option<Stamp>,
    /// The same, for each sealed segment of an `eventlog-file/2` history, in history order: the
    /// stamp each segment file carried while this view verified its bytes, when it was trusted.
    /// Empty on `eventlog-file/1`, and shorter than the manifest's list wherever a view keeps
    /// no stamp for a segment, which costs that segment a comparison and trusts nothing.
    sealed: Vec<Option<Stamp>>,
}

/// One append-only buffer of committed bytes, shared by the views of one handle.
///
/// Bytes are only ever added at the end; none is changed or removed while the buffer lives. That
/// is what lets two views share it: a view's bytes are the first `length` of these, and nothing
/// another view does can change them. A view that would continue the buffer differently from what
/// it already holds gets a buffer of its own instead ([`Content::absorb`]).
struct Retained(Mutex<Vec<u8>>);

impl Retained {
    fn bytes(&self) -> MutexGuard<'_, Vec<u8>> {
        // A panic while the lock was held cannot have left a byte changed: the only writer is
        // `extend_from_slice`, which reserves before it copies, so a capacity overflow panics
        // before a byte moves and an allocation failure aborts the process.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Content {
    /// A view of all of `bytes`, kept at their committed size. Every buffer starts here, so none
    /// starts with the slack of however it was read or built: a buffer grown by doubling would
    /// otherwise hold up to twice the committed bytes for as long as the handle keeps it. Only
    /// appends grow it afterwards ([`Content::absorb`]).
    fn of(mut bytes: Vec<u8>) -> Self {
        bytes.shrink_to_fit();
        Self {
            length: bytes.len(),
            retained: Arc::new(Retained(Mutex::new(bytes))),
            stamp: None,
            sealed: Vec::new(),
        }
    }

    /// Extend this view by `bytes`, which this handle has just verified follow it.
    ///
    /// When this view ends where the buffer does, the bytes are appended in place, so a handle's
    /// own append costs the frame it appends and not the history before it. When another view of
    /// the buffer already went further, its bytes are compared with these: a buffer that already
    /// continues with them is shared, and one that continues with anything else is left to that
    /// view while this one copies its own prefix into a buffer of its own.
    fn absorb(&mut self, bytes: &[u8]) {
        // Bytes appended after the stamp was taken are not what the stamp describes.
        self.stamp = None;
        let end = self.length + bytes.len();
        let own = {
            let mut retained = self.retained.bytes();
            let ahead = (retained.len() - self.length).min(bytes.len());
            if retained[self.length..self.length + ahead] == bytes[..ahead] {
                retained.extend_from_slice(&bytes[ahead..]);
                None
            } else {
                let mut own = Vec::with_capacity(end);
                own.extend_from_slice(&retained[..self.length]);
                own.extend_from_slice(bytes);
                Some(own)
            }
        };
        if let Some(own) = own {
            self.retained = Arc::new(Retained(Mutex::new(own)));
        }
        self.length = end;
    }

    fn length(&self) -> u64 {
        self.length as u64
    }

    fn stamped(mut self, stamp: Option<Stamp>) -> Self {
        self.stamp = stamp;
        self
    }

    /// Keep `sealed` as the stamps of the sealed segments these bytes were read from.
    fn sealed_stamped(mut self, sealed: Vec<Option<Stamp>>) -> Self {
        self.sealed = sealed;
        self
    }

    /// The active segment these bytes end in has just been sealed by an append: a new sealed
    /// segment, whose stamp is not trusted yet, and a new active one, which has none.
    fn seal(&mut self) {
        self.sealed.push(None);
        self.stamp = None;
    }

    /// Move this view onto `other`'s buffer when the bytes one of them verified begin the other's,
    /// and answer whether the two now share one buffer.
    ///
    /// Two views of one handle reach the same bytes by separate reads — a writer's complete reread
    /// and a capture's strict read each read the whole file — and this is where the second copy is
    /// dropped. A view shorter than `other` points at `other`'s buffer; a longer one continues
    /// that buffer with the bytes it has past `other`'s, in place when the buffer ends where
    /// `other` does. Only storage moves: this view still holds exactly the bytes it verified, its
    /// own length and its own stamp, which is why nothing is trusted that was not before. When the
    /// bytes differ, or the buffer already continues differently, this view keeps its own buffer
    /// and the answer is `false`; the caller then drops `other`, which is stale.
    ///
    /// It compares up to the shorter view's length, so callers run it inside their blocking file
    /// work, never on the async executor; [`Content::shares_bytes_with`] is the O(1) question for
    /// there.
    pub(crate) fn adopt(&mut self, other: &Self) -> bool {
        if self.shares_bytes_with(other) {
            return true;
        }
        let common = self.length.min(other.length);
        // A view of no bytes has nothing to save, and every other view shares only a buffer whose
        // bytes it verified itself. That keeps views of two epochs apart: every frame carries its
        // epoch, so the first frames already differ, and only an empty history — one an erasure
        // emptied — could otherwise end up keeping the buffer of the bytes it erased.
        if common == 0 {
            return false;
        }
        let same = {
            // Both buffers hold at least `common` bytes, so the order they are locked in is free:
            // address order, so that two calls can never wait on each other.
            let (low, high) = if Arc::as_ptr(&self.retained) < Arc::as_ptr(&other.retained) {
                (&self.retained, &other.retained)
            } else {
                (&other.retained, &self.retained)
            };
            let low = low.bytes();
            let high = high.bytes();
            low[..common] == high[..common]
        };
        if !same {
            return false;
        }
        // Only the bytes past `other`'s view are copied, and only to continue its buffer with them.
        let rest = self.retained.bytes()[common..self.length].to_vec();
        let mut adopted = Self {
            retained: Arc::clone(&other.retained),
            length: common,
            stamp: None,
            sealed: Vec::new(),
        };
        adopted.absorb(&rest);
        if !adopted.shares_bytes_with(other) {
            return false;
        }
        adopted.stamp = self.stamp;
        adopted.sealed = std::mem::take(&mut self.sealed);
        *self = adopted;
        true
    }

    /// Whether two views keep their bytes in one buffer. A pointer comparison: it reads no byte.
    pub(crate) fn shares_bytes_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.retained, &other.retained)
    }
}

/// Whether the next `verified.len()` bytes of `file` are, byte for byte, `verified`. Reading stops
/// at the first difference. The caller charges the comparison, once every file it compared has
/// matched.
fn same_bytes(file: &mut File, verified: &[u8]) -> Result<bool, EventLogError> {
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut compared = 0;
    while compared < verified.len() {
        let want = (verified.len() - compared).min(buffer.len());
        let read = file.read(&mut buffer[..want]).map_err(backend)?;
        if read == 0 || buffer[..read] != verified[compared..compared + read] {
            return Ok(false);
        }
        compared += read;
    }
    Ok(true)
}

/// The inode metadata the kernel moves whenever a file's bytes change through the filesystem.
///
/// Userspace cannot set `ctime`: `utimensat` sets `mtime` and moves `ctime` to now, a write or
/// truncate moves both, and a rename over the name moves the inode. So a file whose stamp is
/// unchanged still holds the bytes it held, with two exceptions this store does not defend
/// against: a write to the block device under the filesystem, and a shared mapping written after
/// its first fault of a dirty cycle. `docs/design/file-provider.md` records that envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stamp {
    dev: u64,
    ino: u64,
    len: u64,
    mtime_ns: i128,
    ctime_ns: i128,
}

/// How long after a change a stamp is still untrusted: one second of filesystem timestamp
/// granularity plus the coarse clock tick. A file changed inside the window can carry the same
/// timestamps as the change that follows it, so it is not stamped until the window has passed.
/// A handle's own writes get no exemption: a same-length edit microseconds after its commit can
/// land in the same tick (`tests/verify_once_review.rs`).
const UNTRUSTED_NS: i128 = 2_000_000_000;

impl Stamp {
    fn of(file: &File) -> Option<Self> {
        Self::from_metadata(&file.metadata().ok()?)
    }

    /// The stamp of a regular file from metadata already taken: an `lstat` of a name that
    /// [`regular`] admitted describes the same inode a later open of it would.
    #[cfg(unix)]
    #[expect(
        clippy::unnecessary_wraps,
        reason = "a platform without inode stamps answers None for the same call"
    )]
    fn from_metadata(metadata: &fs::Metadata) -> Option<Self> {
        use std::os::unix::fs::MetadataExt as _;
        Some(Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            len: metadata.len(),
            mtime_ns: i128::from(metadata.mtime()) * 1_000_000_000
                + i128::from(metadata.mtime_nsec()),
            ctime_ns: i128::from(metadata.ctime()) * 1_000_000_000
                + i128::from(metadata.ctime_nsec()),
        })
    }

    #[cfg(not(unix))]
    fn from_metadata(_metadata: &fs::Metadata) -> Option<Self> {
        None
    }
}

/// The wall clock, as the kernel stamps `ctime` from it.
fn now_ns() -> i128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| i128::try_from(elapsed.as_nanos()).unwrap_or(0))
}

/// The stamp to keep for bytes read between `before` and `after`, which began at `started`:
/// only when the file did not move during the read and had not moved for the window before it.
fn trusted(before: Option<Stamp>, after: Option<Stamp>, started: i128) -> Option<Stamp> {
    match (before, after) {
        (Some(before), Some(after))
            if before == after && before.ctime_ns < started - UNTRUSTED_NS =>
        {
            Some(before)
        }
        _ => None,
    }
}

/// Retains the native lock while the caller folds and examines this observation.
pub(crate) struct InspectedJournal {
    _lock: WriterLock,
    pub transactions: Vec<Value>,
}

/// Source bytes count the manifest and journal, the only files decoded here.
pub(crate) fn inspect(root: &Path, source_bytes: u64) -> Result<InspectedJournal, InspectionError> {
    #[cfg(test)]
    let _hashing = crate::cost::hashing_for(root);
    // The one predicate, not a second copy of the decision: an inspection that read a link as a
    // store is the same defect the write paths already refuse.
    if !physical_directory(root).map_err(inspection_io)? {
        return Err(InspectionError::UnsupportedSource);
    }
    let root_metadata = fs::symlink_metadata(root).map_err(inspection_io)?;
    let lock = WriterLock::try_hold(inspection_file(&root.join("writer.lock"))?)
        .map_err(|_| InspectionError::SourceBusy)?;
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
    if manifest.format != FORMAT && manifest.format != SEGMENTED_FORMAT {
        return Err(InspectionError::UnsupportedSource);
    }
    if !manifest.known() || manifest.store.is_empty() || manifest.active_length() != events_len {
        return Err(InspectionError::CorruptSource);
    }
    // Sealed segments are journal bytes too, and count against the same budget.
    if manifest_len
        .checked_add(manifest.length)
        .ok_or(InspectionError::LimitExceeded)?
        > source_bytes
    {
        return Err(InspectionError::LimitExceeded);
    }
    let mut committed = Vec::new();
    read_sealed(root, &manifest, &mut committed).map_err(|_| InspectionError::CorruptSource)?;
    if unexplained_segments(root, &manifest).map_err(|_| InspectionError::CorruptSource)? {
        return Err(InspectionError::CorruptSource);
    }
    let active = inspection_read(events_file, events_len)?;
    let committed = if committed.is_empty() {
        active
    } else {
        committed.extend(active);
        committed
    };
    let transactions = decode(&committed, &manifest).map_err(|_| InspectionError::CorruptSource)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let after = fs::symlink_metadata(root).map_err(|_| InspectionError::SourceChanged)?;
        let after_lock = fs::symlink_metadata(root.join("writer.lock"))
            .map_err(|_| InspectionError::SourceChanged)?;
        let locked = lock.0.metadata().map_err(inspection_io)?;
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
/// SHA-256 of `bytes`, as lowercase hex: every digest this crate computes goes through here.
///
/// In tests the bytes are charged as `journal_bytes_hashed` to the root whose journal operation
/// is running on this thread (`crate::cost::hashing_for`); hashing outside any journal operation,
/// as of blob content, charges nothing here.
pub(crate) fn hash(bytes: &[u8]) -> String {
    let digest = format!("{:x}", Sha256::digest(bytes));
    #[cfg(test)]
    crate::cost::charge_hashing(|cost| {
        cost.journal_bytes_hashed += bytes.len() as u64;
    });
    digest
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

/// Append every sealed segment `manifest` names to `into`, in history order, and return the stamp
/// each file earned while it was read ([`trusted`]). Nothing on `eventlog-file/1`.
///
/// Each segment must be a regular file of exactly its committed length ending at a frame
/// boundary, under a `segments` directory that is a physical directory: a missing, short, long or
/// linked segment refuses. A frame never spans two files, so a sealed segment that does not end
/// in a newline is not a segment any writer sealed.
fn read_sealed(
    root: &Path,
    manifest: &Manifest,
    into: &mut Vec<u8>,
) -> Result<Vec<Option<Stamp>>, EventLogError> {
    let sealed = manifest.sealed();
    if sealed.is_empty() {
        return Ok(Vec::new());
    }
    if !physical_directory(&root.join("segments")).map_err(backend)? {
        return Err(corrupt());
    }
    let mut stamps = Vec::with_capacity(sealed.len());
    for (index, length) in sealed.iter().enumerate() {
        let path = segment_path(root, index);
        regular(&path)?;
        let mut file = File::open(&path).map_err(backend)?;
        let started = now_ns();
        let before = Stamp::of(&file);
        let start = into.len();
        (&mut file)
            .take(length.checked_add(1).ok_or_else(corrupt)?)
            .read_to_end(into)
            .map_err(backend)?;
        if (into.len() - start) as u64 != *length || into.last() != Some(&b'\n') {
            return Err(corrupt());
        }
        stamps.push(trusted(before, Stamp::of(&file), started));
    }
    Ok(stamps)
}

/// Whether `segments/` holds an entry `manifest` does not name. Only an interrupted seal leaves
/// one, and only beside its durable intent, which recovery resolves first; any other is not
/// committed history, and a complete reader refuses it rather than guess what it is.
fn unexplained_segments(root: &Path, manifest: &Manifest) -> Result<bool, EventLogError> {
    if manifest.segments.is_none() {
        return Ok(false);
    }
    let entries = match fs::read_dir(root.join("segments")) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(!manifest.sealed().is_empty());
        }
        Err(error) => return Err(backend(error)),
    };
    let named = manifest.sealed().len();
    for entry in entries {
        let name = entry.map_err(backend)?.file_name();
        let explained = name
            .to_str()
            .and_then(|name| name.strip_suffix(".jsonl"))
            .filter(|index| index.len() == 10 && index.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|index| index.parse::<usize>().ok())
            .is_some_and(|index| index < named);
        if !explained {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Remove sealed segments `0..count` of a retired epoch, and synchronize their directory.
/// Absent ones are already gone: a recovery may be finishing what an interrupted run began.
fn remove_sealed(root: &Path, count: usize) -> Result<(), EventLogError> {
    if count == 0 {
        return Ok(());
    }
    for index in 0..count {
        match fs::remove_file(segment_path(root, index)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(backend(error)),
        }
    }
    sync_dir(&root.join("segments"))
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
        Self::open_with_creation(root, Some(FileLayout::Single))
    }

    /// Open a journal, creating a missing store in `layout`. An existing store keeps its own.
    pub fn open_with_layout(root: &Path, layout: FileLayout) -> Result<Self, EventLogError> {
        Self::open_with_creation(root, Some(layout))
    }

    /// Open a provisioned journal without minting any missing authority.
    pub fn open_existing(root: &Path) -> Result<Self, EventLogError> {
        Self::open_with_creation(root, None)
    }

    fn open_with_creation(root: &Path, layout: Option<FileLayout>) -> Result<Self, EventLogError> {
        let create = layout.is_some();
        #[cfg(test)]
        let _hashing = crate::cost::hashing_for(root);
        // A tree store keeps its history as one file per event under `store.json`. Opening one
        // here would write a journal beside it, which neither store could then read.
        if root.join("store.json").exists() {
            return Err(EventLogError::Invalid(
                "this directory holds an eventlog-tree store, not a file store".into(),
            ));
        }
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
        let lock = WriterLock::hold(lock).map_err(backend)?;
        let manifest_path = root.join("manifest.json");
        if !manifest_path.exists() {
            if !create {
                return Err(corrupt());
            }
            // Never interpret an existing history without its commit authority as a new store.
            if root.join("events.jsonl").exists() || root.join("privacy.json").exists() {
                return Err(corrupt());
            }
            let (format, segments) = match layout {
                Some(FileLayout::Segmented { seal_at }) if seal_at >= 1 => (
                    SEGMENTED_FORMAT,
                    Some(Segments {
                        seal_at,
                        sealed: Vec::new(),
                    }),
                ),
                Some(FileLayout::Segmented { .. }) => {
                    return Err(EventLogError::Invalid(
                        "a segmented file store seals at one byte or more".into(),
                    ));
                }
                _ => (FORMAT, None),
            };
            let manifest = Manifest {
                format: format.into(),
                store: new_event_id(),
                epoch: 0,
                sequence: 0,
                length: 0,
                digest: ZERO.into(),
                segments,
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
        if !manifest.known() {
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
        let active = manifest.active_length();
        if events.metadata().map_err(backend)?.len() < active {
            return Err(corrupt());
        }
        // Sized from the committed length: this vector becomes the handle's kept bytes.
        let mut committed = Vec::with_capacity(usize::try_from(manifest.length).map_err(backend)?);
        let sealed = read_sealed(root, &manifest, &mut committed)?;
        if unexplained_segments(root, &manifest)? {
            return Err(corrupt());
        }
        let started = now_ns();
        let before = Stamp::of(&events);
        (&mut events)
            .take(active)
            .read_to_end(&mut committed)
            .map_err(backend)?;
        let transactions = decode(&committed, &manifest)?;
        #[cfg(test)]
        crate::cost::charge(root, |cost| {
            cost.frames_chained += transactions.len() as u64;
        });
        if events.metadata().map_err(backend)?.len() > active {
            return Err(corrupt());
        }
        let content = Content::of(committed)
            .stamped(trusted(before, Stamp::of(&events), started))
            .sealed_stamped(sealed);
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
        #[cfg(test)]
        let _hashing = crate::cost::hashing_for(&self.root);
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
        // A segmented store whose active segment has reached its seal size seals it first: the
        // frame then opens a new active segment, and the sealed one is never written again.
        let active = self.manifest.active_length();
        let seal = self
            .manifest
            .seal_at()
            .is_some_and(|seal_at| active > 0 && active >= seal_at);
        if seal && let Some(segments) = next.segments.as_mut() {
            segments.sealed.push(active);
        }
        // A seal that would refuse refuses before its intent exists: recovery reads an entry at
        // the sealed name as the active segment the seal moved, so a refusal after the intent
        // would leave recovery a foreign file to move over `events.jsonl`.
        if seal {
            self.seal_target_free()?;
        }
        let intent = AppendIntent {
            before: self.manifest.clone(),
            after: next.clone(),
            frame: String::from_utf8(line.clone()).map_err(backend)?,
        };
        atomic_json(&self.root, "append.json", &intent)?;
        checkpoint("append-prepared");
        let events = self.root.join("events.jsonl");
        let mut file = if seal {
            self.seal_active(&events)?
        } else {
            let mut file = OpenOptions::new()
                .write(true)
                .open(&events)
                .map_err(backend)?;
            file.seek(SeekFrom::Start(active)).map_err(backend)?;
            file
        };
        let midpoint = line.len() / 2;
        file.write_all(&line[..midpoint]).map_err(backend)?;
        checkpoint("append-torn");
        file.write_all(&line[midpoint..])
            .and_then(|()| file.sync_all())
            .map_err(backend)?;
        if seal {
            // The new active segment's directory entry is durable before the manifest names it.
            sync_dir(&self.root)?;
        }
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
        if seal {
            self.content.seal();
        }
        self.content.absorb(&line);
        Ok(())
    }

    /// Whether the next seal can take its name: `Ok(true)` when `segments/` is a physical
    /// directory, `Ok(false)` when it does not exist yet. Anything else at `segments/`, or any
    /// entry already at the next sealed name, refuses: the manifest names every sealed segment, so
    /// one more is nothing a writer made, and a rename would silently replace it.
    fn seal_target_free(&self) -> Result<bool, EventLogError> {
        let exists = match physical_directory(&self.root.join("segments")) {
            Ok(true) => true,
            Ok(false) => return Err(corrupt()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(backend(error)),
        };
        if exists {
            let target = segment_path(&self.root, self.manifest.sealed().len());
            match fs::symlink_metadata(&target) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Ok(_) => return Err(corrupt()),
                Err(error) => return Err(backend(error)),
            }
        }
        Ok(exists)
    }

    /// Move the active segment to its sealed name and create an empty active segment, returning it
    /// open for the frame. Runs after `append.json` names the seal, so an interruption anywhere
    /// here is undone by [`recover_append`]: a sealed name the manifest does not list yet is moved
    /// back over `events.jsonl`.
    ///
    /// A name already taken refuses ([`Self::seal_target_free`], asked before the intent and
    /// again here).
    fn seal_active(&self, events: &Path) -> Result<File, EventLogError> {
        let directory = self.root.join("segments");
        if !self.seal_target_free()? {
            fs::create_dir(&directory).map_err(backend)?;
            sync_dir(&self.root)?;
        }
        let target = segment_path(&self.root, self.manifest.sealed().len());
        fs::rename(events, &target).map_err(backend)?;
        sync_dir(&directory)?;
        sync_dir(&self.root)?;
        checkpoint("append-sealed");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(events)
            .map_err(backend)
    }

    /// The committed bytes this journal decoded, for a test that resumes onto them.
    #[cfg(test)]
    pub fn content(&self) -> Content {
        self.content.clone()
    }

    /// Keep this journal's committed bytes in `reader`'s buffer when the two agree
    /// ([`Content::adopt`]); `false` when they do not, or there is no reader. Compares bytes:
    /// blocking work only.
    pub(crate) fn adopt(&mut self, reader: Option<&Content>) -> bool {
        reader.is_some_and(|reader| self.content.adopt(reader))
    }

    /// Whether this journal keeps its committed bytes in `content`'s buffer. Reads no byte.
    pub(crate) fn shares_bytes_with(&self, content: &Content) -> bool {
        self.content.shares_bytes_with(content)
    }

    pub fn extends(&self, observed: &Manifest) -> Result<bool, EventLogError> {
        #[cfg(test)]
        let _hashing = crate::cost::hashing_for(&self.root);
        extends_observed(&self.manifest, &self.transactions, observed)
    }

    /// Release the lock and keep what it protected: the manifest, the verified frames, and the
    /// committed bytes those frames were decoded from.
    pub fn into_parts(self) -> (Manifest, Vec<Value>, Content) {
        (self.manifest, self.transactions, self.content)
    }

    /// Take the lock and decide from `manifest.json` whether the committed history is exactly
    /// `observed` or extends it, reading and verifying only the frames past `observed.length`,
    /// chained from `observed.digest` to the new manifest digest.
    ///
    /// The committed bytes behind `observed` are re-read and compared with `content` before any
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
        #[cfg(test)]
        let _hashing = crate::cost::hashing_for(root);
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
        let lock = WriterLock::hold(lock).map_err(backend)?;
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
        #[cfg(test)]
        let _hashing = crate::cost::hashing_for(&self.root);
        let mut next = self.manifest.clone();
        next.epoch = next.epoch.checked_add(1).ok_or_else(corrupt)?;
        next.sequence = 0;
        next.length = 0;
        next.digest = ZERO.into();
        // The new epoch's history is one active segment; the retired epoch's sealed segments hold
        // the bytes this rewrite removes, and its recovery deletes them after the switch.
        if let Some(segments) = next.segments.as_mut() {
            segments.sealed.clear();
        }
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
        self.content = Content::of(bytes);
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
    if !manifest.known() {
        return Err(corrupt());
    }
    if manifest.format != observed.format
        || manifest.store != observed.store
        || manifest.epoch != observed.epoch
        || manifest.sequence < observed.sequence
        || manifest.length < observed.length
        || content.length() != observed.length
    {
        return Ok(None);
    }
    // The segments this handle verified are still sealed at the lengths it verified, and the one
    // it saw active has at most grown, whether or not it has been sealed since. Anything else is
    // not an extension of the observed history, and the complete opener decides it.
    let observed_sealed = observed.sealed();
    let sealed = manifest.sealed();
    let held = observed_sealed.len();
    if manifest.seal_at() != observed.seal_at()
        || sealed.len() < held
        || sealed[..held] != *observed_sealed
        || sealed
            .get(held)
            .is_some_and(|length| *length < observed.active_length())
    {
        return Ok(None);
    }
    if !sealed.is_empty() && !physical_directory(&root.join("segments")).map_err(backend)? {
        return Err(corrupt());
    }
    // A `segments/` entry the manifest does not name refuses the complete opener, the strict
    // reader and the inspector; a resume hands it to the complete opener rather than serve or
    // extend a store none of them accepts. One listing of `segments/`, on `eventlog-file/2` only.
    if unexplained_segments(root, &manifest)? {
        return Ok(None);
    }
    let events_path = root.join("events.jsonl");
    regular(&events_path)?;
    if fs::metadata(&events_path).map_err(backend)?.len() != manifest.active_length() {
        return Ok(None);
    }
    // Every file of the committed history, in order: each sealed segment, then the active one.
    // For each, the bytes of it this handle verified (all of an observed sealed segment, the
    // observed length of the segment it saw active, none of a newer one) are either vouched for
    // by an unchanged trusted stamp or re-read and compared with the bytes it kept; the bytes past
    // them are read into the tail, which must chain from the observed head. On `eventlog-file/1`
    // this is one file, `events.jsonl`, exactly as before.
    let retained = content.retained.bytes();
    let mut offset = 0_usize;
    let mut compared = 0_u64;
    let mut read_any = false;
    let mut tail = Vec::new();
    let mut stamps = Vec::with_capacity(sealed.len() + 1);
    for index in 0..=sealed.len() {
        let is_sealed = index < sealed.len();
        let (path, committed) = if is_sealed {
            (segment_path(root, index), sealed[index])
        } else {
            (events_path.clone(), manifest.active_length())
        };
        let (verified, kept) = match index.cmp(&held) {
            std::cmp::Ordering::Less => (
                observed_sealed[index],
                content.sealed.get(index).copied().flatten(),
            ),
            std::cmp::Ordering::Equal => (observed.active_length(), content.stamp),
            std::cmp::Ordering::Greater => (0, None),
        };
        let verified_len = usize::try_from(verified).map_err(backend)?;
        if is_sealed {
            let metadata = fs::symlink_metadata(&path).map_err(backend)?;
            if !metadata.is_file() {
                return Err(corrupt());
            }
            // A sealed segment that still carries the stamp taken when its bytes were verified
            // still holds them: a stamp is only kept once the file has held still for the whole
            // untrusted window, and its length is in it. Nothing of it needs re-reading.
            if kept.is_some() && kept == Stamp::from_metadata(&metadata) && verified == committed {
                stamps.push(kept);
                offset += verified_len;
                continue;
            }
        }
        let mut file = File::open(&path).map_err(backend)?;
        let started = now_ns();
        let before = Stamp::of(&file);
        // The same rule for the active segment, which `eventlog-file/1` has always applied to its
        // one file.
        if !is_sealed && kept.is_some() && kept == before && verified == committed {
            stamps.push(kept);
            offset += verified_len;
            continue;
        }
        read_any = true;
        // Otherwise the bytes this handle verified are re-read as raw bytes and compared with the
        // bytes it kept: cheaper than decoding and rechaining them, cheaper than hashing them,
        // and the only way a handle can tell that what it verified is still there. Any difference
        // — a byte, or a length the view does not have — is the same answer a different digest
        // was.
        if !same_bytes(&mut file, &retained[offset..offset + verified_len])? {
            return Ok(None);
        }
        offset += verified_len;
        compared += verified;
        let rest = committed - verified;
        let start = tail.len();
        (&mut file)
            .take(rest.checked_add(1).ok_or_else(corrupt)?)
            .read_to_end(&mut tail)
            .map_err(backend)?;
        if (tail.len() - start) as u64 != rest {
            return Ok(None);
        }
        // A frame never spans two files: a sealed segment ends at a frame boundary.
        if is_sealed && rest > 0 && tail.last() != Some(&b'\n') {
            return Ok(None);
        }
        // Every byte of this file has now been read and matched or read for the tail; stamp it
        // for the next resume.
        stamps.push(trusted(before, Stamp::of(&file), started));
    }
    drop(retained);
    #[cfg(not(test))]
    let _ = (compared, read_any);
    #[cfg(test)]
    crate::cost::charge(root, |cost| {
        if read_any {
            cost.prefix_bytes_compared += compared;
        } else {
            cost.resumes_trusted += 1;
        }
    });
    // Past the observed head: an unchanged head has an empty tail, which chains only when the
    // manifest is the observed one.
    let Ok(fresh) = decode_chain(&tail, &manifest, observed.sequence, &observed.digest) else {
        return Ok(None);
    };
    #[cfg(test)]
    crate::cost::charge(root, |cost| {
        cost.frames_chained += fresh.len() as u64;
    });
    // The bytes just matched are the view's own; carrying it costs a reference count.
    let mut resumed = content.clone();
    resumed.absorb(&tail);
    let active = stamps.pop().flatten();
    Ok(Some((
        manifest,
        fresh,
        resumed.stamped(active).sealed_stamped(stamps),
    )))
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

/// The lock, the committed manifest, the frames a handle has not verified yet, and the committed
/// bytes this resume re-read and validated.
pub(crate) struct Resumed {
    lock: WriterLock,
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
    _lock: WriterLock,
    pub manifest: Manifest,
    pub fresh: Vec<Value>,
    content: Content,
}

impl StrictResumed {
    /// Run `read` with the lock still held, then release it and keep what it protected: the
    /// committed head, the frames past the head the handle observed, and the committed bytes this
    /// resume validated.
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
    #[cfg(test)]
    let _hashing = crate::cost::hashing_for(root);
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
    let lock = WriterLock::hold(lock).ok()?;
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
    lock: WriterLock,
    pub manifest: Manifest,
    pub transactions: Vec<Value>,
    content: Content,
}

impl Strict {
    /// Run `read` with the lock still held, then release it and keep what it protected: the
    /// committed head, the verified frames, and the committed bytes those frames were decoded
    /// from.
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
    #[cfg(test)]
    let _hashing = crate::cost::hashing_for(root);
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
    let lock =
        WriterLock::hold(lock).map_err(|_| unavailable("file store writer lock cannot be held"))?;
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
    if !manifest.known() {
        return Err(damaged());
    }
    let events_path = root.join("events.jsonl");
    if !fs::symlink_metadata(&events_path)
        .map_err(|_| damaged())?
        .is_file()
    {
        return Err(damaged());
    }
    // Sealed segments first, each exactly its committed length; then the active segment.
    let mut sealed = Vec::new();
    read_sealed(root, &manifest, &mut sealed).map_err(|_| damaged())?;
    if unexplained_segments(root, &manifest).map_err(|_| damaged())? {
        return Err(damaged());
    }
    let active = fs::read(&events_path).map_err(|_| damaged())?;
    // Surplus bytes are as much a refusal as missing ones: neither is history this manifest commits.
    if active.len() as u64 != manifest.active_length() {
        return Err(damaged());
    }
    let committed = if sealed.is_empty() {
        active
    } else {
        sealed.extend(active);
        sealed
    };
    let transactions = decode(&committed, &manifest).map_err(|_| damaged())?;
    #[cfg(test)]
    crate::cost::charge(root, |cost| {
        cost.frames_chained += transactions.len() as u64;
    });
    let content = Content::of(committed);
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
    let (before, after) = (&intent.before, &intent.after);
    let held = before.sealed().len();
    // An append either leaves the sealed segments alone or seals exactly the active segment it
    // found: one more sealed segment, of exactly the active length before it.
    let sealing = after.sealed().len() == held + 1;
    if !before.known()
        || !after.known()
        || after.format != before.format
        || after.seal_at() != before.seal_at()
        || after.sealed()[..held.min(after.sealed().len())] != *before.sealed()
        || (sealing && after.sealed()[held] != before.active_length())
        || (!sealing && after.sealed().len() != held)
    {
        return Err(corrupt());
    }
    let events = root.join("events.jsonl");
    let mut prefix = Vec::new();
    read_sealed(root, before, &mut prefix)?;
    if sealing {
        let target = segment_path(root, held);
        let renamed = match fs::symlink_metadata(&target) {
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(backend(error)),
        };
        if current == before {
            // Uncommitted: put the active segment back where the manifest says it is. A sealing
            // append never writes into the segment it seals, so that is the whole of the undo,
            // and it replaces whatever part of the new active segment was written.
            //
            // Only the segment this append moved goes back: a regular file of exactly the active
            // length the intent recorded, whose bytes extend the sealed prefix into `before`.
            // Anything else at that name is not ours to move, and `events.jsonl` is left alone.
            if renamed {
                let moved = read(&target)?;
                if moved.len() as u64 != before.active_length() {
                    return Err(corrupt());
                }
                let mut committed = prefix.clone();
                committed.extend(&moved);
                decode(&committed, before)?;
                fs::rename(&target, &events).map_err(backend)?;
                sync_dir(&root.join("segments"))?;
                sync_dir(root)?;
            }
            let mut committed = prefix;
            committed.extend(read(&events)?);
            decode(&committed, before)?;
            let mut planned = committed;
            planned.extend(intent.frame.as_bytes());
            decode(&planned, after)?;
        } else {
            if !renamed {
                return Err(corrupt());
            }
            let mut committed = Vec::new();
            read_sealed(root, after, &mut committed)?;
            committed.extend(read(&events)?);
            decode(&committed, after)?;
        }
    } else {
        let mut bytes = read(&events)?;
        let boundary = usize::try_from(before.active_length()).map_err(backend)?;
        if bytes.len() < boundary {
            return Err(corrupt());
        }
        let mut committed = prefix;
        let sealed_length = committed.len();
        committed.extend(&bytes[..boundary]);
        decode(&committed, before)?;
        let suffix = &bytes[boundary..];
        if !intent.frame.as_bytes().starts_with(suffix) {
            return Err(corrupt());
        }
        let mut planned = committed;
        planned.extend(intent.frame.as_bytes());
        decode(&planned, after)?;
        if current == after {
            planned.truncate(sealed_length);
            planned.extend(&bytes);
            decode(&planned, current)?;
        } else {
            bytes.truncate(boundary);
            let file = OpenOptions::new()
                .write(true)
                .open(&events)
                .map_err(backend)?;
            file.set_len(before.active_length())
                .and_then(|()| file.sync_all())
                .map_err(backend)?;
        }
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
        format: manifest.format.clone(),
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
        if frame.format != manifest.format
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
        || !intent.before.known()
        || !intent.after.known()
        || intent.after.format != intent.before.format
        || intent.after.seal_at() != intent.before.seal_at()
        || !intent.after.sealed().is_empty()
    {
        return Err(corrupt());
    }
    let events = read(&root.join("events.jsonl"))?;
    if hash(&events) == intent.replacement_digest {
        decode(&events, &intent.after)?;
    } else {
        let mut committed = Vec::new();
        read_sealed(root, &intent.before, &mut committed)?;
        committed.extend(events);
        decode(&committed, &intent.before)?;
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
    // The retired epoch's sealed segments hold bytes the rewrite removed. They go before the
    // intent does, so an interruption here leaves the intent that finishes the removal.
    remove_sealed(root, intent.before.sealed().len())?;
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

    /// Every holder of the writers' lock releases it when it drops, not when the last copy of its
    /// descriptor closes.
    ///
    /// A `fork` anywhere in the process copies every descriptor into a child that keeps the open
    /// file description until it execs; a lock released by closing outlives its holder by that
    /// window, and a concurrent `try_lock` (an inspection's `SourceBusy`) sees it held. A
    /// duplicate made here stands in for that child with no timing in it: it shares the
    /// description exactly as a forked child does, and it outlives every holder below.
    /// (story:strict-reader-lock-case-races-a-forked-crash-child)
    #[test]
    #[expect(
        clippy::used_underscore_binding,
        reason = "the case duplicates the descriptor production code deliberately never reads"
    )]
    fn every_writer_lock_holder_releases_the_lock_when_it_drops() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let probe = || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(root.join("writer.lock"))
                .expect("existing writer lock")
        };
        let released = |holder: &str| {
            let probe = probe();
            probe.try_lock().unwrap_or_else(|_| {
                panic!("{holder}: the lock outlived its holder through a shared description")
            });
            probe.unlock().expect("released the probe");
        };

        let journal = Journal::open(root).expect("initialized store");
        let inherited = journal._lock.0.try_clone().expect("shared description");
        let (manifest, content) = (journal.manifest.clone(), journal.content());
        drop(journal);
        released("Journal");
        drop(inherited);

        let resumed = Journal::resume(root, &manifest, &content)
            .expect("resumable")
            .expect("same head");
        let inherited = resumed.lock.0.try_clone().expect("shared description");
        drop(resumed.into_journal(root, Vec::new()));
        released("Journal::resume");
        drop(inherited);

        let strict = open_strict(root).expect("strict read");
        let inherited = strict.lock.0.try_clone().expect("shared description");
        drop(strict);
        released("open_strict");
        drop(inherited);

        let strict = resume_strict(root, &manifest, &content).expect("same head");
        let inherited = strict._lock.0.try_clone().expect("shared description");
        drop(strict);
        released("resume_strict");
        drop(inherited);

        let inspected = inspect(root, u64::MAX).expect("inspectable");
        let inherited = inspected._lock.0.try_clone().expect("shared description");
        drop(inspected);
        released("inspect");
        drop(inherited);
    }

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
        assert_eq!(same.fresh, [] as [serde_json::Value; 0]);
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
        // does not chain from the observed digest: the prefix comparison passes and the chain check
        // is what refuses.
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
            Journal::resume(root.path(), &fork, &Content::of(forked))
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

    /// Every buffer a view starts from is held at its committed size, whichever way its bytes were
    /// read or built — a complete open, a privacy rewrite, a strict read — so what a handle keeps
    /// is the committed size of `events.jsonl` until its own appends grow it.
    #[test]
    fn every_new_buffer_starts_at_its_committed_size() {
        let root = tempfile::tempdir().unwrap();
        let held = |content: &Content| (content.retained.bytes().capacity(), content.length);
        {
            let mut journal = Journal::open(root.path()).unwrap();
            for index in 0..40 {
                journal
                    .append(json!({ "index": index, "pad": "x".repeat(3_000) }))
                    .unwrap();
            }
        }
        let mut journal = Journal::open(root.path()).unwrap();
        let (capacity, length) = held(&journal.content);
        assert_eq!(capacity, length, "a complete open keeps slack");
        journal.privacy(journal.transactions.clone()).unwrap();
        let (capacity, length) = held(&journal.content);
        assert_eq!(capacity, length, "a privacy rewrite keeps slack");
        drop(journal);
        let strict = open_strict(root.path()).unwrap();
        let (capacity, length) = held(&strict.content);
        assert_eq!(capacity, length, "a strict read keeps slack");
    }

    /// A journal's own appends extend the committed-bytes buffer in place while another view of
    /// the same handle holds it: no append copies the history before it.
    #[test]
    fn appends_extend_shared_committed_bytes_in_place() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(root.path()).unwrap();
        journal.append(json!({"value": 0})).unwrap();
        let other = journal.content();
        let held = journal.manifest.clone();
        for value in 1..=50 {
            journal.append(json!({ "value": value })).unwrap();
            assert!(
                journal.content.shares_bytes_with(&other),
                "append {value} copied the committed bytes"
            );
        }
        // The other view still holds exactly what it verified, and still resumes.
        assert_eq!(other.length(), held.length);
        drop(journal);
        let resumed = Journal::resume(root.path(), &held, &other)
            .unwrap()
            .unwrap();
        assert_eq!(resumed.fresh.len(), 50);
    }

    /// Two views of one buffer each keep exactly the bytes they verified. A view continued with
    /// the bytes the buffer already holds shares them; one continued with different bytes gets a
    /// buffer of its own and leaves the other's untouched. A view adopts another's buffer only
    /// where their bytes agree, and never as a view of no bytes.
    #[test]
    fn views_of_one_buffer_keep_their_own_bytes() {
        let mut first = Content::of(b"prefix|".to_vec());
        let mut second = first.clone();
        first.absorb(b"left|");
        second.absorb(b"le");
        assert!(
            second.shares_bytes_with(&first),
            "a matching continuation copied"
        );
        second.absorb(b"ft|more");
        assert!(
            second.shares_bytes_with(&first),
            "a longer continuation copied"
        );
        let mut third = Content::of(b"prefix|".to_vec());
        assert!(
            third.adopt(&first),
            "a shorter view whose bytes agree was not moved"
        );
        assert!(
            third.shares_bytes_with(&first),
            "equal bytes were not shared"
        );
        third.absorb(b"right|");
        assert!(
            !third.shares_bytes_with(&first),
            "a different continuation was shared"
        );
        let bytes = |content: &Content| content.retained.bytes()[..content.length].to_vec();
        assert_eq!(bytes(&first), b"prefix|left|");
        assert_eq!(bytes(&second), b"prefix|left|more");
        assert_eq!(bytes(&third), b"prefix|right|");
        let mut longer = Content::of(b"prefix|left|more|tail".to_vec());
        assert!(
            longer.adopt(&first),
            "a longer view whose bytes agree was not moved"
        );
        assert!(
            longer.shares_bytes_with(&first),
            "a longer view kept its own copy"
        );
        assert_eq!(bytes(&longer), b"prefix|left|more|tail");
        assert_eq!(bytes(&second), b"prefix|left|more");
        let mut diverging = Content::of(b"prefix|left|less".to_vec());
        assert!(
            !diverging.adopt(&first),
            "a view was moved onto a buffer that continues with other bytes"
        );
        assert_eq!(bytes(&diverging), b"prefix|left|less");
        let mut stale = Content::of(b"prefiX|".to_vec());
        assert!(!stale.adopt(&first), "different bytes were adopted");
        assert!(
            !stale.shares_bytes_with(&first),
            "different bytes were shared"
        );
        let mut emptied = Content::of(Vec::new());
        assert!(!emptied.adopt(&first), "a view of no bytes was moved");
        assert!(
            !emptied.shares_bytes_with(&first),
            "a view of no bytes kept another view's buffer alive"
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

    /// Every file under `root`, recursively, as one lossy string: what a privacy rewrite must have
    /// removed a body from, sealed segments included.
    fn every_byte_below(root: &Path) -> String {
        let mut all = String::new();
        for entry in fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                all.push_str(&every_byte_below(&path));
            } else {
                all.push_str(&String::from_utf8_lossy(&fs::read(path).unwrap()));
            }
        }
        all
    }

    const SEAL_EVERY_FRAME: FileLayout = FileLayout::Segmented { seal_at: 1 };

    /// The segmented layout's append boundaries, including the one a seal adds: the active
    /// segment moved to its sealed name and no new active segment yet. Every boundary before the
    /// manifest leaves the history as it was, sealed nothing, and appends again; the one after it
    /// leaves the sealed segment and the new frame.
    #[test]
    fn process_death_at_each_segmented_append_boundary() {
        for point in [
            "append-prepared",
            "append-sealed",
            "append-torn",
            "append-synced",
            "append-committed",
        ] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut journal = Journal::open_with_layout(root.path(), SEAL_EVERY_FRAME).unwrap();
                journal.append(json!({"value": "original"})).unwrap();
                assert_eq!(journal.manifest.sealed(), [0_u64; 0]);
            }
            // The child opens the existing store, so the store's own seal size makes it seal.
            crash(root.path(), point);
            let mut journal = Journal::open(root.path()).unwrap();
            let committed = point == "append-committed";
            assert_eq!(journal.transactions[0], json!({"value":"original"}));
            assert_eq!(
                journal.transactions.len(),
                if committed { 2 } else { 1 },
                "{point}"
            );
            assert_eq!(
                journal.manifest.sealed().len(),
                usize::from(committed),
                "{point}"
            );
            assert!(!root.path().join("append.json").exists(), "{point}");
            assert_eq!(
                fs::metadata(root.path().join("events.jsonl"))
                    .unwrap()
                    .len(),
                journal.manifest.active_length(),
                "{point}"
            );
            journal.append(json!({"value": "after"})).unwrap();
            drop(journal);
            assert_eq!(
                Journal::open(root.path()).unwrap().transactions.len(),
                if committed { 3 } else { 2 },
                "{point}"
            );
        }
    }

    /// A seal refused because its name is taken refuses before it writes an intent: nothing is
    /// left for recovery to resolve, and the handle still appends no frame onto the taken name.
    #[test]
    fn a_seal_refused_by_a_taken_name_writes_no_intent() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::open_with_layout(root.path(), SEAL_EVERY_FRAME).unwrap();
        journal.append(json!({"value": "original"})).unwrap();
        let active = fs::read(root.path().join("events.jsonl")).unwrap();
        fs::create_dir(root.path().join("segments")).unwrap();
        fs::write(segment_path(root.path(), 0), b"not a segment\n").unwrap();
        assert!(journal.append(json!({"value": "refused"})).is_err());
        assert!(
            !root.path().join("append.json").exists(),
            "the refused seal left an intent for recovery to resolve"
        );
        assert_eq!(fs::read(root.path().join("events.jsonl")).unwrap(), active);
    }

    /// Recovery of a seal interrupted before its rename moves back only the segment the seal
    /// moved. A file that appeared at the sealed name since, whether of another length or of the
    /// active length with other bytes, is refused, and `events.jsonl` is left as it was.
    #[test]
    fn a_recovered_seal_moves_back_only_the_segment_it_sealed() {
        for same_length in [false, true] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut journal = Journal::open_with_layout(root.path(), SEAL_EVERY_FRAME).unwrap();
                journal.append(json!({"value": "original"})).unwrap();
            }
            crash(root.path(), "append-prepared");
            assert!(root.path().join("append.json").exists());
            let active = fs::read(root.path().join("events.jsonl")).unwrap();
            let foreign = if same_length {
                let mut bytes = vec![b'x'; active.len() - 1];
                bytes.push(b'\n');
                bytes
            } else {
                b"not a segment\n".to_vec()
            };
            fs::create_dir_all(root.path().join("segments")).unwrap();
            fs::write(segment_path(root.path(), 0), &foreign).unwrap();
            assert!(
                Journal::open(root.path()).is_err(),
                "same length: {same_length}"
            );
            assert_eq!(
                fs::read(root.path().join("events.jsonl")).unwrap(),
                active,
                "same length: {same_length}: recovery moved a foreign file over the active segment"
            );
            assert_eq!(fs::read(segment_path(root.path(), 0)).unwrap(), foreign);
        }
    }

    /// A privacy rewrite of a segmented store removes the retired epoch's sealed segments, at
    /// every boundary it can be interrupted at, including after the manifest switched and before
    /// the segments were removed.
    #[test]
    fn process_death_at_each_segmented_privacy_boundary() {
        for point in ["privacy-prepared", "privacy-renamed", "privacy-committed"] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut journal = Journal::open_with_layout(root.path(), SEAL_EVERY_FRAME).unwrap();
                journal
                    .append(json!({"value": "sensitive-old-body"}))
                    .unwrap();
                journal.append(json!({"value": "later"})).unwrap();
                assert_eq!(journal.manifest.sealed().len(), 1);
            }
            assert!(every_byte_below(&root.path().join("segments")).contains("sensitive-old-body"));
            crash(root.path(), point);
            let journal = Journal::open(root.path()).unwrap();
            assert_eq!(journal.transactions, vec![json!({"value":"erased"})]);
            assert_eq!(journal.manifest.sealed(), [0_u64; 0], "{point}");
            assert!(
                !every_byte_below(root.path()).contains("sensitive-old-body"),
                "{point}: a retired sealed segment survived the rewrite"
            );
        }
    }

    /// A manifest names one of the two layouts this release reads, consistently, or nothing opens
    /// it: segment records on `eventlog-file/1`, none on `eventlog-file/2`, a zero seal size, an
    /// empty sealed segment, or sealed segments longer than the history.
    #[test]
    fn a_manifest_that_misnames_its_layout_is_refused() {
        type Change = fn(&mut Value);
        let cases: [(&str, Change); 6] = [
            ("segments on the single layout", |manifest| {
                manifest["segments"] = json!({"seal_at": 1, "sealed": []});
            }),
            ("no segments on the segmented layout", |manifest| {
                manifest["format"] = json!("eventlog-file/2");
            }),
            ("an unknown format", |manifest| {
                manifest["format"] = json!("eventlog-file/3");
            }),
            ("a zero seal size", |manifest| {
                manifest["format"] = json!("eventlog-file/2");
                manifest["segments"] = json!({"seal_at": 0, "sealed": []});
            }),
            ("an empty sealed segment", |manifest| {
                manifest["format"] = json!("eventlog-file/2");
                manifest["segments"] = json!({"seal_at": 1, "sealed": [0]});
            }),
            ("sealed segments past the history", |manifest| {
                manifest["format"] = json!("eventlog-file/2");
                manifest["segments"] = json!({"seal_at": 1, "sealed": [1_000_000]});
            }),
        ];
        for (name, change) in cases {
            let root = tempfile::tempdir().unwrap();
            drop(Journal::open(root.path()).unwrap());
            let path = root.path().join("manifest.json");
            let mut manifest: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            change(&mut manifest);
            fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
            assert!(Journal::open(root.path()).is_err(), "{name}");
            assert!(open_strict(root.path()).is_err(), "{name}");
            assert!(inspect(root.path(), u64::MAX).is_err(), "{name}");
        }
    }

    /// A segmented resume over another writer's seals: the frames past the observed head are
    /// chained across the new sealed segments; a sealed segment rewritten at its own length, or a
    /// fork that keeps the observed lengths, is handed to the complete opener.
    #[test]
    fn a_segmented_resume_reads_past_its_head_across_seals_and_refuses_a_rewritten_segment() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::open_with_layout(root.path(), SEAL_EVERY_FRAME).unwrap();
        journal.append(json!({"value": 1})).unwrap();
        journal.append(json!({"value": 2})).unwrap();
        let observed = journal.manifest.clone();
        let observed_content = journal.content();
        drop(journal);
        let mut other = Journal::open(root.path()).unwrap();
        other.append(json!({"value": 3})).unwrap();
        other.append(json!({"value": 4})).unwrap();
        let head = other.manifest.clone();
        let head_content = other.content();
        drop(other);
        assert_eq!(head.sealed().len(), 3);
        let extended = Journal::resume(root.path(), &observed, &observed_content)
            .unwrap()
            .unwrap();
        assert_eq!(extended.fresh, [json!({"value": 3}), json!({"value": 4})]);
        assert_eq!(extended.manifest, head);
        drop(extended);
        let strict = resume_strict(root.path(), &observed, &observed_content).unwrap();
        assert_eq!(strict.fresh.len(), 2);
        drop(strict);

        // The first sealed segment rewritten at its own length: the bytes this handle verified
        // are not there any more, whichever resume asks.
        let first = segment_path(root.path(), 0);
        let original = fs::read(&first).unwrap();
        let mut rewritten = original.clone();
        let at = rewritten.windows(1).position(|b| b == b"1").unwrap();
        rewritten[at] = b'7';
        fs::write(&first, &rewritten).unwrap();
        assert!(
            Journal::resume(root.path(), &head, &head_content)
                .unwrap()
                .is_none()
        );
        assert!(resume_strict(root.path(), &head, &head_content).is_none());
        assert!(Journal::open(root.path()).is_err());
        assert!(open_strict(root.path()).is_err());
        fs::write(&first, &original).unwrap();
        assert!(
            Journal::resume(root.path(), &head, &head_content)
                .unwrap()
                .is_some()
        );
    }

    /// The manifest's segment record is the ESS value `eventlog.file_layout.Segments`: its encoding
    /// has exactly the committed schema's properties, each of the declared type, and nothing else.
    #[test]
    fn the_segment_record_serializes_to_its_committed_ess_schema() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../../ess/file-layout-generated/schema/types/eventlog.file_layout.Segments.schema.json",
        );
        let schema: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(schema["$ref"], "#/$defs/eventlog.file_layout.Segments");
        let node = &schema["$defs"]["eventlog.file_layout.Segments"];
        assert_eq!(node["type"], "object");
        assert_eq!(node["additionalProperties"], false);
        let properties = node["properties"].as_object().unwrap();
        let mut declared: Vec<_> = properties.keys().cloned().collect();
        declared.sort();
        let mut required: Vec<_> = node["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|name| name.as_str().unwrap().to_owned())
            .collect();
        required.sort();
        assert_eq!(required, declared);
        assert_eq!(
            properties["seal_at"],
            json!({"type": "integer", "minimum": 1})
        );
        assert_eq!(
            properties["sealed"],
            json!({"type": "array", "items": {"type": "integer"}})
        );
        let encoded = serde_json::to_value(Segments {
            seal_at: 1,
            sealed: vec![3, 4],
        })
        .unwrap();
        let mut fields: Vec<_> = encoded.as_object().unwrap().keys().cloned().collect();
        fields.sort();
        assert_eq!(fields, declared);
        assert!(encoded["seal_at"].is_u64());
        assert!(
            encoded["sealed"]
                .as_array()
                .unwrap()
                .iter()
                .all(Value::is_u64)
        );
        // And it is what a segmented manifest carries, while a single-layout one carries none.
        let root = tempfile::tempdir().unwrap();
        let segmented = Journal::open_with_layout(root.path(), SEAL_EVERY_FRAME).unwrap();
        let written = serde_json::to_value(&segmented.manifest).unwrap();
        assert_eq!(written["format"], SEGMENTED_FORMAT);
        assert_eq!(written["segments"], json!({"seal_at": 1, "sealed": []}));
        drop(segmented);
        let single = tempfile::tempdir().unwrap();
        let written =
            serde_json::to_value(&Journal::open(single.path()).unwrap().manifest).unwrap();
        assert!(written.get("segments").is_none());
    }
}
