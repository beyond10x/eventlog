//! What a handle actually verified, counted per store root. Test-only scaffolding.
//!
//! A resumed reader and a strict one read the same bytes of `events.jsonl`, so no damage a case
//! can do to a store tells them apart: what differs is the work. Counted here, one field each: the
//! frames a handle decodes and chains, re-encodes and folds; the committed prefix bytes a resume
//! re-reads and finds equal to the bytes it verified, and the resumes that trusted a stamp and
//! read none; the journal bytes it runs through SHA-256; the objects it hashes and finds to be
//! their record; and the barriers and object synchronizations a write takes. A case that asserts
//! "this handle paid for what changed" has nothing else to assert against.
//!
//! Every field is charged in code somewhere outside this file, and `every_counter_is_charged`
//! holds that: an assertion against a number nothing moves cannot fail, so it asserts nothing.
//!
//! The tally is keyed by store root. A process-wide counter would be measuring the whole suite:
//! these cases run in the same binary, at the same time, as every other case in the crate.

use std::{
    cell::RefCell,
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
    /// Bytes of journal content run through SHA-256 for this root: frames whose digest is encoded
    /// or checked, and a whole committed file or privacy replacement hashed by a rewrite or its
    /// recovery.
    ///
    /// Charged inside `journal::hash`, where the hashing happens, to the root whose journal
    /// operation is running on that thread ([`hashing_for`]). `hash` has no root of its own, and
    /// its other callers hash blob content, which is `blobs_hashed`: they run outside every
    /// journal operation, so nothing of theirs lands here.
    pub journal_bytes_hashed: u64,
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
            journal_bytes_hashed: self.journal_bytes_hashed - earlier.journal_bytes_hashed,
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

thread_local! {
    /// The store roots whose journal operations are running on this thread, innermost last.
    static HASHING: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
}

/// While this lives, journal bytes hashed on this thread are charged to its root.
pub(crate) struct Hashing(());

impl Drop for Hashing {
    fn drop(&mut self) {
        HASHING.with(|roots| {
            roots.borrow_mut().pop();
        });
    }
}

/// Enter one journal operation on `root`: what `journal::hash` computes until the returned value
/// is dropped is charged to `root`. Every journal entry point that can reach `hash` takes one,
/// because `hash` has no root of its own to charge.
pub(crate) fn hashing_for(root: &Path) -> Hashing {
    HASHING.with(|roots| roots.borrow_mut().push(root.to_owned()));
    Hashing(())
}

/// Add to the tally of the root whose journal operation is running on this thread, if any.
pub(crate) fn charge_hashing(change: impl FnOnce(&mut Cost)) {
    if let Some(root) = HASHING.with(|roots| roots.borrow().last().cloned()) {
        charge(&root, change);
    }
}

/// Every counter is charged in code somewhere outside this file.
///
/// A field nothing charges reads zero forever, so `assert_eq!(spent.field, 0)` passes whatever the
/// code does. That happened once: the resume stopped hashing its prefix and `prefix_bytes_hashed`
/// stayed behind, asserted zero by three cases that could no longer fail.
///
/// The fields are the ones `Cost`'s own `Debug` prints, whatever their visibility, so a new one is
/// covered without touching this case. A charge counts only where it is code: comments and string
/// and character literals are removed first ([`code_only`]), so a charge left in a comment is not
/// one.
#[test]
fn every_counter_is_charged() {
    let printed = format!("{:?}", Cost::default());
    let fields: Vec<&str> = printed
        .trim_start_matches("Cost {")
        .trim_end_matches('}')
        .split(',')
        .filter_map(|pair| pair.split(':').next())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect();
    assert!(fields.len() >= 9, "the fields were not read: {printed}");
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut code = String::new();
    for entry in std::fs::read_dir(&source_root).expect("readable source directory") {
        let file = entry.expect("readable entry").path();
        if file.extension().is_some_and(|kind| kind == "rs") && !file.ends_with("cost.rs") {
            code.push_str(&code_only(
                &std::fs::read_to_string(&file).expect("readable source file"),
            ));
            code.push('\n');
        }
    }
    let collapsed = code.split_whitespace().collect::<Vec<_>>().join(" ");
    let uncharged: Vec<&&str> = fields
        .iter()
        .filter(|field| !collapsed.contains(&format!("cost.{field} +=")))
        .collect();
    assert!(
        uncharged.is_empty(),
        "counters no code charges, so every assertion against them passes: {uncharged:?}"
    );
}

/// `source` with every comment and every string, raw string and character literal replaced by a
/// space, so that what is left to search is code.
#[cfg(test)]
fn code_only(source: &str) -> String {
    let chars: Vec<char> = source.chars().collect();
    let at = |index: usize| chars.get(index).copied();
    let identifier = |character: Option<char>| {
        character.is_some_and(|character| character.is_alphanumeric() || character == '_')
    };
    let mut code = String::with_capacity(source.len());
    let mut index = 0;
    while let Some(character) = at(index) {
        if character == '/' && at(index + 1) == Some('/') {
            while at(index).is_some_and(|character| character != '\n') {
                index += 1;
            }
        } else if character == '/' && at(index + 1) == Some('*') {
            let mut depth = 0;
            loop {
                match (at(index), at(index + 1)) {
                    (Some('/'), Some('*')) => {
                        depth += 1;
                        index += 2;
                    }
                    (Some('*'), Some('/')) => {
                        depth -= 1;
                        index += 2;
                        if depth == 0 {
                            break;
                        }
                    }
                    (Some(_), _) => index += 1,
                    (None, _) => break,
                }
            }
        } else if character == 'r'
            && !identifier(index.checked_sub(1).and_then(at))
            && matches!(at(index + 1), Some('"' | '#'))
        {
            let mut hashes = 0;
            let mut cursor = index + 1;
            while at(cursor) == Some('#') {
                hashes += 1;
                cursor += 1;
            }
            if at(cursor) == Some('"') {
                cursor += 1;
                while let Some(character) = at(cursor) {
                    cursor += 1;
                    if character == '"'
                        && (0..hashes).all(|offset| at(cursor + offset) == Some('#'))
                    {
                        cursor += hashes;
                        break;
                    }
                }
                index = cursor;
            } else {
                code.push(character);
                index += 1;
                continue;
            }
        } else if character == '"' {
            index += 1;
            while let Some(character) = at(index) {
                index += if character == '\\' { 2 } else { 1 };
                if character == '"' {
                    break;
                }
            }
        } else if character == '\'' && at(index + 1) == Some('\\') {
            index += 2;
            while at(index).is_some_and(|character| character != '\'') {
                index += 1;
            }
            index += 1;
        } else if character == '\'' && at(index + 2) == Some('\'') {
            index += 3;
        } else {
            code.push(character);
            index += 1;
            continue;
        }
        code.push(' ');
    }
    code
}

/// The scanner removes what it must and keeps what it must, including the shapes this crate's
/// sources contain: raw strings with hashes, byte strings, escaped quotes, character literals that
/// are quotes and lifetimes that are not literals.
#[test]
fn code_only_keeps_code_and_nothing_else() {
    let source = concat!(
        "a(); // cost.x += 1;\n",
        "/* cost.y += 1; /* nested */ still */ b();\n",
        "let s = \"cost.z += 1; \\\" still\"; c();\n",
        "let r = r#\"cost.w += \"1\"\"#; d();\n",
        "let q = '\"'; let e = '\\''; fn f<'a>(x: &'a str) {}\n",
        "let bytes = br\"cost.v\"; cost.u += 1;\n",
    );
    let code = code_only(source);
    for gone in [
        "cost.x", "cost.y", "nested", "cost.z", "still", "cost.w", "cost.v",
    ] {
        assert!(!code.contains(gone), "{gone} survived: {code}");
    }
    for kept in [
        "a();",
        "b();",
        "c();",
        "d();",
        "fn f<'a>(x: &'a str)",
        "cost.u += 1;",
    ] {
        assert!(code.contains(kept), "{kept} was removed: {code}");
    }
}
