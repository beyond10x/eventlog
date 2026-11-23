//! Moving a store to `eventlog-tree/2`: every blob whose long text can be stored once is split
//! (`crate::split`), and nothing any reader is served changes.
//!
//! The migration is explicit and never runs on open. It holds the writer lock throughout, reads
//! and verifies the whole store before it writes, and afterwards reads the whole store again and
//! compares what it serves — every blob's bytes, every group and event — with what it served
//! before. Event and identity files are never touched, and a group file only gains the request
//! fingerprint its receipt recorded (`eventlog-tree/group/2`), so every digest and parent stays as
//! it was.
//!
//! Crash order: `store.json` is switched first, because an `eventlog-tree/2` store whose blobs are
//! all raw is valid. Then each blob is converted on its own: its texts, then its manifest, which
//! is read back and compared with the blob's bytes before the raw file is removed. A migration
//! interrupted anywhere leaves a store that opens and serves the same bytes, and running it again
//! finishes it.

use crate::history::{self, GroupRecord, Kind, Loaded};
use crate::layout::{
    FORMAT, GROUP_FORMAT_V2, GROUPS_KEY, StoreFormat, backend, blob_path, canonical, corrupt,
    group_path, sha256_hex, store_format, store_manifest, write_atomic,
};
use crate::split;
use eventlog_core::EventLogError;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    /// Raw blobs this call splits (or would), counting a raw copy left beside a split form of the
    /// same blob, which it removes.
    pub blobs_split: usize,
    /// Text files this call adds (or would): texts no blob held yet, each once.
    pub texts_added: usize,
    /// Whether `store.json` declared `eventlog-tree/group/2` before: every group written to the
    /// store keeps its request fingerprint. A migration declares it.
    pub group_fingerprints_before: bool,
    /// Atomic groups whose file this call gives (or would give) the request fingerprint their
    /// receipt recorded, rewriting it from `eventlog-tree/group/1` to `eventlog-tree/group/2`.
    pub groups_fingerprinted: usize,
    /// Atomic groups left in `eventlog-tree/group/1`: an event of theirs was redacted, or a blob of
    /// theirs deleted, before any migration kept their fingerprint, and the files no longer hold
    /// the request it was computed from. After a reopen, the exact retry of such a group is
    /// refused with [`EventLogError::IdempotencyMismatch`].
    pub groups_without_fingerprint: usize,
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
        self.blobs_split > 0
            || self.from_format != self.to_format
            || !self.group_fingerprints_before
            || self.groups_fingerprinted > 0
    }
}

/// Migrate the tree store at `root` to `eventlog-tree/2`, or report what that would do.
///
/// Idempotent: on a store already migrated, a dry run reports nothing pending and an apply
/// changes nothing. A raw blob in an `eventlog-tree/2` store — one merged from a branch that had
/// not migrated — is split like any other. An `eventlog-tree/3` store already splits, so it keeps
/// its format and only has its raw blobs split; no migration changes a store's stream layout.
///
/// The migration also fences the store into `eventlog-tree/group/2`: `store.json` declares it, so
/// every atomic group written from then on keeps the request fingerprint its receipt recorded,
/// and every atomic group already committed whose content is intact gets that fingerprint
/// (computed from its files, as a replay computes it). A replay then restores each group's receipt
/// even after one of its events is redacted or one of its blobs deleted. A group whose content
/// was removed before the migration cannot be given its fingerprint back
/// ([`MigrationReport::groups_without_fingerprint`]). Eventlog 0.10.0 and earlier refuse a group
/// file in `eventlog-tree/group/2` as corrupt: migrate only once every binary that opens the store
/// reads it. A store created by this release declares it from the start.
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
    let mut plan: Vec<Planned> = Vec::new();
    for tenant in before.tenants.keys() {
        let splits: BTreeSet<String> = split::split_files(root, tenant)?
            .into_iter()
            .map(|(digest, _)| digest)
            .collect();
        already_split += splits.len();
        let blobs: BTreeMap<&str, &Arc<[u8]>> = before.tenants[tenant]
            .blobs
            .iter()
            .map(|(digest, bytes)| (digest.as_str(), bytes))
            .collect();
        for (digest, _) in split::raw_files(root, tenant)? {
            // The load read every blob, and refused one whose two forms disagree, so these are
            // the bytes the store serves under `digest`.
            let bytes = Arc::clone(
                blobs
                    .get(digest.as_str())
                    .ok_or_else(|| crate::layout::corrupt("a blob the replay did not read"))?,
            );
            // A raw file beside a split form that serves the same bytes — an interrupted
            // conversion, or a merge of a branch that had not migrated — is finished: the raw
            // copy goes. Planning it as already split would leave it there for good.
            let step = if splits.contains(&digest) {
                Step::DropRaw
            } else if let Some(cut) = split::split(&digest, &bytes) {
                Step::Split(cut)
            } else {
                continue;
            };
            plan.push(Planned {
                tenant: tenant.clone(),
                digest,
                bytes,
                step,
            });
        }
    }

    // What the history files would be: each converted blob's raw file replaced by its manifest,
    // and each text no blob held yet added once; a raw copy beside its split form just removed.
    let mut texts_added = 0;
    for planned in &plan {
        let (tenant, digest) = (&planned.tenant, &planned.digest);
        sizes.remove(&relative(root, &blob_path(root, tenant, digest)));
        let Step::Split(cut) = &planned.step else {
            continue;
        };
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
    // Every atomic group whose content is intact gets the fingerprint its receipt recorded, which
    // is the one a replay of it computes today. One whose event was redacted or whose blob was
    // deleted no longer holds the request its writer made, so its fingerprint is lost for good.
    let (_, group_fingerprints_before) = store_manifest(root)?;
    let mut backfill: Vec<(PathBuf, Vec<u8>, GroupRecord)> = Vec::new();
    let mut groups_without_fingerprint = 0;
    for history in before.tenants.values() {
        for committed in &history.groups {
            if committed.record.kind != Kind::Group || committed.record.fingerprint.is_some() {
                continue;
            }
            let Some(fingerprint) = committed.intact_fingerprint()? else {
                groups_without_fingerprint += 1;
                continue;
            };
            let mut record = committed.record.clone();
            record.fingerprint = Some(fingerprint);
            let path = group_path(root, &record.tenant, &record.key_digest());
            let bytes = canonical(&record.to_value()?);
            sizes.insert(relative(root, &path), bytes.len() as u64);
            backfill.push((path, bytes, record));
        }
    }

    // `store.json` moves to `eventlog-tree/2` from the first layout, and declares group files
    // that keep their fingerprint: from then on every group written keeps it, and Eventlog 0.10.0
    // and earlier refuse the store's new group files rather than misread them.
    let manifest_path = root.join("store.json");
    let manifest = if format == StoreFormat::V1 || !group_fingerprints_before {
        let bytes = fs::read(&manifest_path).map_err(backend)?;
        let mut value: Value = serde_json::from_slice(&bytes).map_err(|_| corrupt("store.json"))?;
        let map = value.as_object_mut().ok_or_else(|| corrupt("store.json"))?;
        if format == StoreFormat::V1 {
            map.insert("format".into(), Value::String(FORMAT.to_owned()));
        }
        map.insert(GROUPS_KEY.into(), Value::String(GROUP_FORMAT_V2.to_owned()));
        let bytes = canonical(&value);
        sizes.insert(relative(root, &manifest_path), bytes.len() as u64);
        Some(bytes)
    } else {
        None
    };
    let mut report = MigrationReport {
        from_format: format.tag().to_owned(),
        to_format: if format == StoreFormat::V1 {
            FORMAT
        } else {
            format.tag()
        }
        .to_owned(),
        blobs: served_before.blobs.len(),
        already_split,
        blobs_split: plan.len(),
        texts_added,
        group_fingerprints_before,
        groups_fingerprinted: backfill.len(),
        groups_without_fingerprint,
        bytes_before,
        bytes_after: sizes.values().sum(),
        largest_before,
        largest_after: largest(&sizes),
        verified_blobs: None,
    };
    if mode == MigrationMode::DryRun {
        return Ok(report);
    }

    // First, as the first layout's switch always was: a store declaring group files that keep
    // their fingerprint still reads every group file that does not.
    if let Some(bytes) = &manifest {
        write_atomic(&manifest_path, bytes)?;
    }
    for planned in &plan {
        let (tenant, digest, bytes) = (&planned.tenant, &planned.digest, &planned.bytes);
        match &planned.step {
            Step::Split(cut) => {
                split::convert(root, tenant, digest, bytes, cut)?;
            }
            Step::DropRaw => split::drop_raw(root, tenant, digest, bytes)?,
        }
    }
    // A group file is rewritten in place, as a redacted event file is: only the format tag and
    // the fingerprint change, so the group's name, its events and their digests stay as they were.
    for (path, bytes, _) in &backfill {
        write_atomic(path, bytes)?;
    }

    let after = history::load(root)?;
    let served_after = served(&after);
    if served_after != served_before {
        return Err(corrupt(
            "the migrated store does not serve what it served before",
        ));
    }
    let kept: BTreeMap<(&str, String), Option<&String>> = after
        .tenants
        .values()
        .flat_map(|history| &history.groups)
        .map(|committed| {
            (
                (
                    committed.record.tenant.as_str(),
                    committed.record.key_digest(),
                ),
                committed.record.fingerprint.as_ref(),
            )
        })
        .collect();
    for (_, _, record) in &backfill {
        if kept.get(&(record.tenant.as_str(), record.key_digest()))
            != Some(&record.fingerprint.as_ref())
        {
            return Err(corrupt(
                "a migrated group file does not read back with its fingerprint",
            ));
        }
    }
    let measured = file_sizes(root)?;
    report.bytes_after = measured.values().sum();
    report.largest_after = largest(&measured);
    report.verified_blobs = Some(served_after.blobs.len());
    Ok(report)
}

/// One raw blob file the migration replaces, with the bytes the load read for it, so applying the
/// plan looks nothing up again.
struct Planned {
    tenant: String,
    digest: String,
    bytes: Arc<[u8]>,
    step: Step,
}

enum Step {
    /// Write the split form, read it back, then remove the raw file.
    Split(split::Split),
    /// The split form is already there: read it back, then remove the raw file.
    DropRaw,
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
