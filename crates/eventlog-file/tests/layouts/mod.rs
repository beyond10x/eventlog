//! The on-disk layouts every refusal case runs against (decision-blocker:on-disk-layout-changes,
//! option A: conformance holds both layouts).
//!
//! `eventlog-file/1` keeps the whole committed history in `events.jsonl`. `eventlog-file/2` keeps
//! sealed segments under `segments/` and the active one in `events.jsonl`, so a damaged byte can
//! sit in either. A case parameterised here runs once per [`Variant`]: on the old layout, on the
//! new one with the damaged frame in the active segment, and on the new one with it sealed.
#![allow(
    dead_code,
    reason = "each test binary uses its own part of the fixture"
)]

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_file::{FileEventStore, FileLayout};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Variant {
    /// `eventlog-file/1`, exactly as every release before the segmented layout wrote it.
    Single,
    /// `eventlog-file/2`, with the case's frames where the case put them: the last is active.
    SegmentedActive,
    /// `eventlog-file/2`, with one more frame committed after the case's own, so every frame the
    /// case wrote is in a sealed segment.
    SegmentedSealed,
}

pub const VARIANTS: [Variant; 3] = [
    Variant::Single,
    Variant::SegmentedActive,
    Variant::SegmentedSealed,
];

/// The smallest seal size: an append seals whatever the active segment holds, so every frame but
/// the last is in a sealed segment.
pub const SEGMENTED: FileLayout = FileLayout::Segmented { seal_at: 1 };

impl Variant {
    pub fn layout(self) -> FileLayout {
        match self {
            Self::Single => FileLayout::Single,
            Self::SegmentedActive | Self::SegmentedSealed => SEGMENTED,
        }
    }

    /// Open (and, for a new directory, create) a store of this variant's layout.
    pub async fn open(self, root: &Path) -> FileEventStore {
        FileEventStore::open_with_layout(root, self.layout())
            .await
            .unwrap()
    }

    /// For [`Variant::SegmentedSealed`], commit one frame on an unrelated stream, which seals the
    /// segment holding every frame before it. Nothing for the other variants.
    pub async fn settle(self, store: &FileEventStore) {
        static SETTLED: AtomicU64 = AtomicU64::new(0);
        if self != Self::SegmentedSealed {
            return;
        }
        let index = SETTLED.fetch_add(1, Ordering::Relaxed);
        let stream = StreamId::new(
            TenantId::new("settle-owner").unwrap(),
            "settle",
            format!("s{index}"),
        )
        .unwrap();
        store
            .append(
                &stream,
                Expected::NoStream,
                &[event("settle.recorded", 0)],
                &meta(&format!("settle-{index}"), &json!({})),
            )
            .await
            .unwrap();
    }

    /// Whether this variant's damage must land in a sealed segment; checked against the file the
    /// damage actually hit, so a fixture that stops exercising what it claims fails loudly.
    pub fn damages_sealed(self) -> bool {
        self == Self::SegmentedSealed
    }
}

/// The committed history's files, in history order: sealed segments, then `events.jsonl`.
pub fn journal_files(root: &Path) -> Vec<PathBuf> {
    let mut sealed: Vec<PathBuf> = match fs::read_dir(root.join("segments")) {
        Ok(entries) => entries.map(|entry| entry.unwrap().path()).collect(),
        Err(_) => Vec::new(),
    };
    sealed.sort();
    sealed.push(root.join("events.jsonl"));
    sealed
}

/// The committed history as one byte string, whichever layout holds it.
pub fn history(root: &Path) -> Vec<u8> {
    journal_files(root)
        .iter()
        .flat_map(|path| fs::read(path).unwrap())
        .collect()
}

/// Edit the history as one byte string and write each file the edit changed back in place, at its
/// own length. Returns the files written.
pub fn rewrite_history(root: &Path, edit: impl FnOnce(&mut Vec<u8>)) -> Vec<PathBuf> {
    let files = journal_files(root);
    let parts: Vec<Vec<u8>> = files.iter().map(|path| fs::read(path).unwrap()).collect();
    let mut joined = parts.concat();
    let length = joined.len();
    edit(&mut joined);
    assert_eq!(joined.len(), length, "the edit must not change the length");
    let mut written = Vec::new();
    let mut offset = 0;
    for (path, part) in files.iter().zip(&parts) {
        let edited = &joined[offset..offset + part.len()];
        if edited != part.as_slice() {
            fs::write(path, edited).unwrap();
            written.push(path.clone());
        }
        offset += part.len();
    }
    written
}

/// Replace the first same-length run `from` by `to` inside the committed history, leaving
/// `manifest.json` byte-identical and every journal file exactly as long as it was. Returns the
/// one file the damage landed in.
pub fn damage_in_place(root: &Path, from: &str, to: &str) -> PathBuf {
    assert_eq!(
        from.len(),
        to.len(),
        "the damage must not change the length"
    );
    let written = rewrite_history(root, |bytes| {
        let at = bytes
            .windows(from.len())
            .position(|window| window == from.as_bytes())
            .unwrap_or_else(|| panic!("fixture byte run {from} is in the history"));
        bytes[at..at + to.len()].copy_from_slice(to.as_bytes());
    });
    assert_eq!(written.len(), 1, "the damage lands in one file");
    written.into_iter().next().unwrap()
}

/// Assert the damaged file is in a sealed segment exactly when the variant says so.
pub fn assert_damaged_where(variant: Variant, root: &Path, damaged: &Path) {
    assert_eq!(
        damaged.starts_with(root.join("segments")),
        variant.damages_sealed(),
        "{variant:?}: the damage landed in {}",
        damaged.display()
    );
}
