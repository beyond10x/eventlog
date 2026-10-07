//! What a handle actually verified, counted per store root. Test-only scaffolding.
//!
//! A resumed reader and a strict one read the same bytes of `events.jsonl`, so no damage a case
//! can do to a store tells them apart: what differs is the work. Counted here, one field each: the
//! frames a handle decodes and chains, re-encodes and folds; the committed prefix bytes a resume
//! re-reads and finds equal to the bytes it verified, and the resumes that trusted a stamp and
//! read none; the objects it hashes and finds to be their record; and the barriers and object
//! synchronizations a write takes. A case that asserts "this handle paid for what changed" has
//! nothing else to assert against.
//!
//! Every field is charged somewhere outside this file, and `every_counter_is_charged` holds that:
//! an assertion against a number nothing moves cannot fail, so it asserts nothing.
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
    /// Bytes of an already verified committed prefix re-read and found, byte for byte, to be the
    /// bytes the handle verified.
    ///
    /// Charged by the comparison, the rule `blobs_hashed` states below: a resume whose prefix
    /// differs adds nothing, and so does one that skips the comparison.
    pub prefix_bytes_compared: u64,
    /// Resumes that trusted an unchanged file stamp and read no prefix byte.
    pub resumes_trusted: u64,
    /// Blob objects read, hashed against the hash the committed history recorded for them, and
    /// found to be that content.
    ///
    /// Charged by the comparison rather than beside it (`crate::verified`), which is the same rule
    /// `object_syncs` below states and for the same reason: a charge written between the read and
    /// the comparison measures *read*, so deleting the comparison leaves every assertion about
    /// this number green while nothing is verified. A mismatch therefore adds nothing — as a
    /// failed `sync_all` adds no `object_syncs` — and that difference is what
    /// `capture::tests::a_binding_whose_content_is_not_the_record_charges_no_hashing` measures.
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
            prefix_bytes_compared: self.prefix_bytes_compared - earlier.prefix_bytes_compared,
            resumes_trusted: self.resumes_trusted - earlier.resumes_trusted,
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

/// Every counter is charged somewhere outside this file.
///
/// A field nothing charges reads zero forever, so `assert_eq!(spent.field, 0)` passes whatever the
/// code does. That happened once: the resume stopped hashing its prefix and `prefix_bytes_hashed`
/// stayed behind, asserted zero by three cases that could no longer fail. The fields are read from
/// this file's own declaration, so a new one is covered without touching this case.
#[test]
fn every_counter_is_charged() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let own = std::fs::read_to_string(source_root.join("cost.rs")).expect("readable cost.rs");
    let declaration = own
        .split("pub(crate) struct Cost {")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .expect("the Cost declaration");
    let fields: Vec<&str> = declaration
        .lines()
        .filter_map(|line| line.trim().strip_prefix("pub "))
        .filter_map(|line| line.split(':').next())
        .collect();
    assert!(
        fields.len() >= 8,
        "the declaration was not read: {fields:?}"
    );
    let mut elsewhere = String::new();
    for entry in std::fs::read_dir(&source_root).expect("readable source directory") {
        let file = entry.expect("readable entry").path();
        if file.extension().is_some_and(|kind| kind == "rs") && !file.ends_with("cost.rs") {
            elsewhere.push_str(&std::fs::read_to_string(&file).expect("readable source file"));
        }
    }
    let collapsed = elsewhere.split_whitespace().collect::<Vec<_>>().join(" ");
    let uncharged: Vec<&&str> = fields
        .iter()
        .filter(|field| !collapsed.contains(&format!("cost.{field} +=")))
        .collect();
    assert!(
        uncharged.is_empty(),
        "counters no code charges, so every assertion against them passes: {uncharged:?}"
    );
}
