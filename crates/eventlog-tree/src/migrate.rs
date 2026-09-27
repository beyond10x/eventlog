//! Moving a store to `eventlog-tree/2`: every blob whose long text can be stored once is split
//! (`crate::split`), and nothing any reader is served changes.
//!
//! The migration is explicit and never runs on open. It holds the writer lock throughout, reads
//! and verifies the whole store before it writes, and afterwards reads the whole store again and
//! compares what it serves — every blob's bytes, every group and event — with what it served
//! before. Event, group and identity files are never touched, so every digest and parent stays as
//! it was.
//!
//! Crash order: `store.json` is switched first, because an `eventlog-tree/2` store whose blobs are
//! all raw is valid. Then each blob is converted on its own: its texts, then its manifest, which
//! is read back and compared with the blob's bytes before the raw file is removed. A migration
//! interrupted anywhere leaves a store that opens and serves the same bytes, and running it again
//! finishes it.

use crate::history::{self, Loaded};
use crate::layout::{FORMAT, StoreFormat, backend, blob_path, sha256_hex, store_format};
use crate::split;
use eventlog_core::EventLogError;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// What a [`migrate`] call does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationMode {
    /// Work out and report what an apply would write, and write nothing.
    DryRun,
    /// Write it, then verify that the store serves exactly what it served before.
    Apply,
}

/// What a [`migrate`] call found, and did or would do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationReport {
    /// The format `store.json` declared before.
    pub from_format: String,
    /// The format it declares after (or would, for a dry run).
    pub to_format: String,
    /// Blobs the store holds, in either form.
    pub blobs: usize,
    /// Blobs already held split before this call.
    pub already_split: usize,
    /// Raw blobs this call splits (or would).
    pub blobs_split: usize,
    /// Text files this call adds (or would): texts no blob held yet, each once.
    pub texts_added: usize,
    /// Bytes of every history file — `store.json` and everything under `tenants/` — before.
    pub bytes_before: u64,
    /// The same, after (or as it would be).
    pub bytes_after: u64,
    /// The largest history file before, relative to the root, with its length.
    pub largest_before: (PathBuf, u64),
    /// The largest history file after (or as it would be).
    pub largest_after: (PathBuf, u64),
    /// For an apply: the blobs read back after the migration, each byte for byte what the store
    /// served before it. `None` for a dry run.
    pub verified_blobs: Option<usize>,
}

impl MigrationReport {
    /// Whether an apply would change anything.
    #[must_use]
    pub fn pending(&self) -> bool {
        self.blobs_split > 0 || self.from_format != self.to_format
    }
}

/// Migrate the tree store at `root` to `eventlog-tree/2`, or report what that would do.
///
/// Idempotent: on a store already migrated, a dry run reports nothing pending and an apply
/// changes nothing. A raw blob in an `eventlog-tree/2` store — one merged from a branch that had
/// not migrated — is split like any other.
///
/// # Errors
/// Refuses a directory that is not a tree store, and a store the replay cannot verify, before
/// writing anything. After writing, returns an error if the store no longer serves exactly what
/// it served before; every file the migration wrote is then still in place for inspection.
pub fn migrate(root: &Path, mode: MigrationMode) -> Result<MigrationReport, EventLogError> {
    let format = store_format(root)?;
    let _lock = crate::writer_lock(root)?;
    let before = history::load(root)?;
    let served_before = served(&before);
    let mut sizes = file_sizes(root)?;
    let bytes_before = sizes.values().sum();
    let largest_before = largest(&sizes);

    let mut already_split = 0;
    let mut plan = Vec::new();
    for tenant in before.tenants.keys() {
        let splits: BTreeSet<String> = split::split_files(root, tenant)?
            .into_iter()
            .map(|(digest, _)| digest)
            .collect();
        already_split += splits.len();
        let blobs: BTreeMap<&str, &[u8]> = before.tenants[tenant]
            .blobs
            .iter()
            .map(|(digest, bytes)| (digest.as_str(), bytes.as_slice()))
            .collect();
        for (digest, _) in split::raw_files(root, tenant)? {
            if splits.contains(&digest) {
                continue;
            }
            let bytes = blobs
                .get(digest.as_str())
                .ok_or_else(|| crate::layout::corrupt("a blob the replay did not read"))?;
            if let Some(cut) = split::split(&digest, bytes) {
                plan.push((tenant.clone(), digest, cut));
            }
        }
    }

    // What the history files would be: each converted blob's raw file replaced by its manifest,
    // and each text no blob held yet added once.
    let mut texts_added = 0;
    for (tenant, digest, cut) in &plan {
        sizes.remove(&relative(root, &blob_path(root, tenant, digest)));
        sizes.insert(
            relative(root, &split::split_path(root, tenant, digest)),
            cut.manifest.len() as u64,
        );
        for (hex, text) in &cut.texts {
            let path = relative(root, &split::text_path(root, tenant, hex));
            if sizes.insert(path, text.len() as u64).is_none() {
                texts_added += 1;
            }
        }
    }
    if format == StoreFormat::V1 {
        let manifest = root.join("store.json");
        let grown = FORMAT.len() as u64 - StoreFormat::V1.tag().len() as u64;
        if let Some(size) = sizes.get_mut(&relative(root, &manifest)) {
            *size += grown;
        }
    }
    let mut report = MigrationReport {
        from_format: format.tag().to_owned(),
        to_format: FORMAT.to_owned(),
        blobs: served_before.blobs.len(),
        already_split,
        blobs_split: plan.len(),
        texts_added,
        bytes_before,
        bytes_after: sizes.values().sum(),
        largest_before,
        largest_after: largest(&sizes),
        verified_blobs: None,
    };
    if mode == MigrationMode::DryRun {
        return Ok(report);
    }

    if format == StoreFormat::V1 {
        split::set_format(root, FORMAT)?;
    }
    for (tenant, digest, cut) in &plan {
        let bytes = before.tenants[tenant]
            .blobs
            .iter()
            .find(|(held, _)| held == digest)
            .map(|(_, bytes)| bytes.as_slice())
            .ok_or_else(|| crate::layout::corrupt("a blob the replay did not read"))?;
        split::convert(root, tenant, digest, bytes, cut)?;
    }

    let after = history::load(root)?;
    let served_after = served(&after);
    if served_after != served_before {
        return Err(crate::layout::corrupt(
            "the migrated store does not serve what it served before",
        ));
    }
    let measured = file_sizes(root)?;
    report.bytes_after = measured.values().sum();
    report.largest_after = largest(&measured);
    report.verified_blobs = Some(served_after.blobs.len());
    Ok(report)
}

/// Everything a store serves, reduced to what a comparison needs: each blob's bytes by digest,
/// and each committed group's events by digest, per tenant.
#[derive(Debug, PartialEq, Eq)]
struct Served {
    blobs: BTreeMap<(String, String), String>,
    groups: Vec<(String, String, Vec<Vec<String>>)>,
    identities: BTreeMap<String, Option<String>>,
    torn: Vec<PathBuf>,
}

fn served(loaded: &Loaded) -> Served {
    let mut blobs = BTreeMap::new();
    let mut groups = Vec::new();
    let mut identities = BTreeMap::new();
    for (tenant, history) in &loaded.tenants {
        identities.insert(tenant.clone(), history.identity.clone());
        for (digest, bytes) in &history.blobs {
            blobs.insert((tenant.clone(), digest.clone()), sha256_hex(bytes));
        }
        for committed in &history.groups {
            let events = committed
                .events
                .iter()
                .map(|member| {
                    member
                        .iter()
                        .map(|event| event.digest().unwrap_or_default())
                        .collect()
                })
                .collect();
            groups.push((tenant.clone(), committed.record.key_digest(), events));
        }
    }
    Served {
        blobs,
        groups,
        identities,
        torn: loaded.torn.clone(),
    }
}

fn relative(root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(root).unwrap_or(path).to_owned()
}

/// The length of every history file: `store.json` and every file under `tenants/`.
fn file_sizes(root: &Path) -> Result<BTreeMap<PathBuf, u64>, EventLogError> {
    let mut sizes = BTreeMap::new();
    let manifest = root.join("store.json");
    sizes.insert(
        relative(root, &manifest),
        fs::metadata(&manifest).map_err(backend)?.len(),
    );
    let mut stack = vec![root.join("tenants")];
    while let Some(directory) = stack.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(backend(error)),
        };
        for entry in entries {
            let entry = entry.map_err(backend)?;
            let kind = entry.file_type().map_err(backend)?;
            if kind.is_dir() {
                stack.push(entry.path());
            } else if kind.is_file() && !entry.file_name().to_string_lossy().starts_with('.') {
                sizes.insert(
                    relative(root, &entry.path()),
                    entry.metadata().map_err(backend)?.len(),
                );
            }
        }
    }
    Ok(sizes)
}

fn largest(sizes: &BTreeMap<PathBuf, u64>) -> (PathBuf, u64) {
    sizes
        .iter()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
        .map(|(path, size)| (path.clone(), *size))
        .unwrap_or_default()
}
