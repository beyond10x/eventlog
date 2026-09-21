//! What a handle actually verified, counted per store root. Test-only scaffolding.
//!
//! A resumed reader and a strict one read the same bytes of `events.jsonl`, so no damage a case
//! can do to a store tells them apart: what differs is the frames one of them decodes, chains,
//! re-encodes and folds, and the objects it hashes. Those are counted here, because a case that
//! asserts "this handle paid for what changed" has nothing else to assert against.
//!
//! The tally is keyed by store root. A process-wide counter would be measuring the whole suite:
//! these cases run in the same binary, at the same time, as every other case in the crate.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

/// The verification work charged to one store root since the process started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Cost {
    /// Committed frames decoded from JSON, chained to their predecessor and digest-checked.
    pub frames_chained: u64,
    /// Committed frames re-encoded to answer whether history still extends an observed head.
    pub frames_reencoded: u64,
    /// Committed transactions deserialized into operations and folded into state.
    pub frames_folded: u64,
    /// Bytes of an already verified committed prefix re-read and hashed without decoding it.
    pub prefix_bytes_hashed: u64,
    /// Blob objects read and hashed against the hash the committed history recorded for them.
    pub blobs_hashed: u64,
    /// Durability barriers published against this root: one per committed journal frame, and one
    /// per privacy epoch. This is the unit a write is charged in, not the `fsync` count — a
    /// barrier is the commit sequence `append.json` → frame → manifest → directory, and what a
    /// batched writer removes is the sequence, not the individual synchronizations inside it.
    pub durability_barriers: u64,
    /// Object files and object-directory entries synchronized, charged one per `sync_all` that
    /// actually ran and not derived from the size of the batch. Counted beside the barriers
    /// because a batch that took one barrier and still synchronized its directory once per blob
    /// would read as fixed against a metric that only counted commits — and counted at the call
    /// so that dropping a synchronization moves it, which a count computed from the batch would
    /// not. It is the number of object-level synchronizations, not the process's `fsync` total:
    /// the commit sequence's own synchronizations are charged as `durability_barriers`.
    pub object_syncs: u64,
}

impl std::ops::Sub for Cost {
    type Output = Self;
    fn sub(self, earlier: Self) -> Self {
        Self {
            frames_chained: self.frames_chained - earlier.frames_chained,
            frames_reencoded: self.frames_reencoded - earlier.frames_reencoded,
            frames_folded: self.frames_folded - earlier.frames_folded,
            prefix_bytes_hashed: self.prefix_bytes_hashed - earlier.prefix_bytes_hashed,
            blobs_hashed: self.blobs_hashed - earlier.blobs_hashed,
            durability_barriers: self.durability_barriers - earlier.durability_barriers,
            object_syncs: self.object_syncs - earlier.object_syncs,
        }
    }
}

fn table() -> &'static Mutex<HashMap<PathBuf, Cost>> {
    static TABLE: OnceLock<Mutex<HashMap<PathBuf, Cost>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Add to one root's tally.
pub(crate) fn charge(root: &Path, change: impl FnOnce(&mut Cost)) {
    let mut table = table().lock().expect("cost table");
    change(table.entry(root.to_owned()).or_default());
}

/// One root's tally so far. A case compares two of these; an absolute count means nothing.
pub(crate) fn of(root: &Path) -> Cost {
    table()
        .lock()
        .expect("cost table")
        .get(root)
        .copied()
        .unwrap_or_default()
}
