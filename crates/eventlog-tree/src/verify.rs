//! The offline check a pull request runs: every rule a merged tree must satisfy, reported together.
//!
//! It never trusts a cache and always hashes. It reads files only, so it runs where no store can be
//! opened, such as on the checked-out tree of a pull request and its merge base.

use crate::history;
use crate::layout::{StoreFormat, canonical, json_files, subdirectories};
use crate::split;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// The rule of a finding that says the check itself is incomplete: a directory or file it had to
/// read could not be read, or the base is not the store the tree is. It names no defect of the
/// tree, and it is never a pass: the tree was not proved against what was asked.
const INCOMPLETE: &str = "incomplete";

/// One broken rule, with the file it was found in.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Finding {
    /// `V1` to `V5`, as the design names them, or `incomplete` when the check could not read a
    /// directory or file it had to, or the base is not the tree's store. An `incomplete` finding
    /// is never a pass.
    pub rule: &'static str,
    pub path: PathBuf,
    pub detail: String,
}

/// Check the tree at `root`: its layout (V1), torn writes (V3), digests and parents (V4) and
/// canonical bytes (V5). When `base` is given, also check that no committed file was deleted or
/// changed except by redaction (V2). The base must name the same store as `root`; a base that
/// names another store, names none, or holds a directory or file that cannot be read is an
/// `incomplete` finding, never a pass against a partial baseline.
///
/// An empty result is a valid tree. Findings are sorted, so two runs over one tree print the same.
#[must_use]
pub fn verify(root: &Path, base: Option<&Path>) -> Vec<Finding> {
    let mut findings = Vec::new();
    layout(root, &mut findings);
    canonical_bytes(root, &mut findings);
    match history::load(root) {
        Ok(loaded) => {
            for path in loaded.torn {
                findings.push(Finding {
                    rule: "V3",
                    path,
                    detail: "an event file no group commits".into(),
                });
            }
        }
        Err(error) => findings.push(Finding {
            rule: "V4",
            path: root.to_owned(),
            detail: error.to_string(),
        }),
    }
    if let Some(base) = base {
        immutability(base, root, &mut findings);
    }
    findings.sort();
    findings.dedup();
    findings
}

fn relative(root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(root).unwrap_or(path).to_owned()
}

fn incomplete(path: PathBuf, detail: String) -> Finding {
    Finding {
        rule: INCOMPLETE,
        path,
        detail,
    }
}

/// Every file under `root`, relative to it, except the writer lock and `.cache/`, which are not
/// history. The check never reads a cache: whoever can write one can write the files it names.
/// A directory that cannot be listed is an `incomplete` finding naming it, not an empty one.
fn files(root: &Path, findings: &mut Vec<Finding>) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(directory) = stack.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) => {
                findings.push(incomplete(
                    directory,
                    format!("a directory that cannot be listed: {error}"),
                ));
                continue;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    findings.push(incomplete(
                        directory.clone(),
                        format!("a directory that cannot be listed: {error}"),
                    ));
                    continue;
                }
            };
            let path = entry.path();
            if path.is_dir() {
                if path != root.join(".cache") {
                    stack.push(path);
                }
            } else {
                let name = relative(root, &path);
                if name != Path::new(".lock") {
                    found.push(name);
                }
            }
        }
    }
    found.sort();
    found
}

/// V1: only the files this store writes, each where it belongs. Split blobs and texts belong only
/// to an `eventlog-tree/2` store.
fn layout(root: &Path, findings: &mut Vec<Finding>) {
    let splits = crate::layout::store_format(root).is_ok_and(StoreFormat::splits);
    for path in files(root, findings) {
        let parts: Vec<String> = path
            .components()
            .map(|part| part.as_os_str().to_string_lossy().into_owned())
            .collect();
        let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
        // A reader looks a blob up only at the path its digest names, so a blob file anywhere
        // else is never served: the blob would be silently missing from the reopened store.
        if let ["tenants", tenant, kind @ ("blobs" | "split-blobs"), _, file] = parts.as_slice()
            && !file.starts_with('.')
            && !blob_at_its_path(root, &path, tenant, file, *kind == "split-blobs")
        {
            findings.push(Finding {
                rule: "V1",
                path,
                detail: "a blob file outside the path its digest names".into(),
            });
            continue;
        }
        let known = match parts.as_slice() {
            ["store.json"] | ["tenants", _, "identity.json"] => true,
            ["tenants", _, "streams", _, _, file] => is_json(file) && !file.starts_with('.'),
            ["tenants", _, "groups", shard, file] => {
                shard.len() == 2 && is_json(file) && file.starts_with(*shard)
            }
            ["tenants", _, "blobs", _, file] => !file.starts_with('.'),
            ["tenants", _, "split-blobs", _, file] => {
                splits && is_json(file) && !file.starts_with('.')
            }
            ["tenants", _, "texts", shard, file] => {
                splits
                    && shard.len() == 2
                    && file.starts_with(*shard)
                    && file.len() == 64
                    && file
                        .bytes()
                        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            }
            _ => false,
        };
        if !known {
            findings.push(Finding {
                rule: "V1",
                path,
                detail: "a file outside the tree store's layout".into(),
            });
        }
    }
}

/// Whether the blob file at `path` (relative to `root`), in the directory of tenant segment
/// `tenant` and named `file`, is where the store puts the blob its name spells.
fn blob_at_its_path(root: &Path, path: &Path, tenant: &str, file: &str, split: bool) -> bool {
    let Ok(tenant) = history::unescape(tenant) else {
        return false;
    };
    let stem = if split {
        let Some(stem) = file.strip_suffix(".json") else {
            return false;
        };
        stem
    } else {
        file
    };
    let Ok(digest) = history::unescape(stem) else {
        return false;
    };
    let expected = if split {
        split::split_path(root, &tenant, &digest)
    } else {
        crate::layout::blob_path(root, &tenant, &digest)
    };
    expected == root.join(path)
}

/// This crate names its files, always with a lower-case `.json`.
fn is_json(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|extension| extension == "json")
}

/// What `list` finds in `directory`; a directory that cannot be listed is an `incomplete`
/// finding. An absent directory lists as empty.
fn listed(
    directory: &Path,
    list: fn(&Path) -> Result<Vec<PathBuf>, eventlog_core::EventLogError>,
    findings: &mut Vec<Finding>,
) -> Vec<PathBuf> {
    list(directory).unwrap_or_else(|error| {
        findings.push(incomplete(
            directory.to_owned(),
            format!("a directory that cannot be listed: {error}"),
        ));
        Vec::new()
    })
}

/// V5: every JSON file is in its one canonical encoding.
fn canonical_bytes(root: &Path, findings: &mut Vec<Finding>) {
    let mut directories = vec![root.to_owned()];
    for tenant in listed(&root.join("tenants"), subdirectories, findings) {
        directories.push(tenant.clone());
        directories.extend(listed(&tenant.join("groups"), subdirectories, findings));
        directories.extend(listed(
            &tenant.join("split-blobs"),
            subdirectories,
            findings,
        ));
        for kind in listed(&tenant.join("streams"), subdirectories, findings) {
            directories.extend(listed(&kind, subdirectories, findings));
        }
    }
    for directory in directories {
        for path in listed(&directory, json_files, findings) {
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(error) => {
                    findings.push(incomplete(
                        path,
                        format!("a file that cannot be read: {error}"),
                    ));
                    continue;
                }
            };
            let canonical_form = serde_json::from_slice(&bytes)
                .ok()
                .map(|value: serde_json::Value| canonical(&value));
            if canonical_form.as_deref() != Some(bytes.as_slice()) {
                findings.push(Finding {
                    rule: "V5",
                    path: relative(root, &path),
                    detail: "bytes that are not the canonical encoding".into(),
                });
            }
        }
    }
}

/// V2: every file the base holds is still here with the same bytes. The exceptions change no
/// byte any reader is served:
///
/// - an event file whose body was redacted: its envelope, and so its digest, must be unchanged;
/// - a blob held in the other form: a raw file or a split blob of the base may be gone when the
///   tree still serves that digest with exactly the base's bytes (the `eventlog-tree/2`
///   migration);
/// - `store.json` moving from `eventlog-tree/1` to `eventlog-tree/2` with its identity unchanged.
///
/// The comparison is made only against a base that names the same store as `root`, and a base
/// directory or file that cannot be read is an `incomplete` finding: a baseline with a hole in it
/// cannot show that what the hole held is unchanged.
fn immutability(base: &Path, root: &Path, findings: &mut Vec<Finding>) {
    let base_identity = match store_identity(base) {
        Ok(identity) => identity,
        Err(why) => {
            findings.push(incomplete(
                base.join("store.json"),
                format!("the base names no tree store, so nothing was compared: {why}"),
            ));
            return;
        }
    };
    // A tree whose own `store.json` is gone or broken is compared all the same: the comparison
    // then reports that file as deleted or changed.
    if let Ok(identity) = store_identity(root)
        && identity != base_identity
    {
        findings.push(incomplete(
            base.join("store.json"),
            format!(
                "the base is another store (identity {base_identity:?}, the tree's is \
                 {identity:?}), so nothing was compared"
            ),
        ));
        return;
    }
    let mut before: BTreeMap<PathBuf, Vec<u8>> = BTreeMap::new();
    for path in files(base, findings) {
        match fs::read(base.join(&path)) {
            Ok(bytes) => {
                before.insert(path, bytes);
            }
            Err(error) => findings.push(incomplete(
                base.join(&path),
                format!("a file of the base that cannot be read: {error}"),
            )),
        }
    }
    for (path, old) in before {
        if let Some(same) = blob_served_alike(base, root, &path) {
            if !same {
                findings.push(Finding {
                    rule: "V2",
                    path,
                    detail: "a committed blob is no longer served with the same bytes".into(),
                });
            }
            continue;
        }
        let Ok(new) = fs::read(root.join(&path)) else {
            findings.push(Finding {
                rule: "V2",
                path,
                detail: "a committed file was deleted".into(),
            });
            continue;
        };
        if new == old {
            continue;
        }
        if path == Path::new("store.json") && is_format_upgrade(&old, &new) {
            continue;
        }
        if !is_redaction(&old, &new) {
            findings.push(Finding {
                rule: "V2",
                path,
                detail: "a committed file was changed other than by redaction".into(),
            });
        }
    }
}

/// For a base file that holds a blob — a raw file or a split blob — whether the tree at `root`
/// still serves that digest with the bytes the base served. `None` for any other file.
fn blob_served_alike(base: &Path, root: &Path, path: &Path) -> Option<bool> {
    let parts: Vec<String> = path
        .components()
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .collect();
    let (tenant, digest) = match parts
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["tenants", tenant, "blobs", _, file] => (*tenant, history::unescape(file).ok()?),
        ["tenants", tenant, "split-blobs", _, file] => (
            *tenant,
            history::unescape(file.strip_suffix(".json")?).ok()?,
        ),
        _ => return None,
    };
    let tenant = history::unescape(tenant).ok()?;
    let old = split::read_blob(base, &tenant, &digest).ok().flatten();
    let new = split::read_blob(root, &tenant, &digest).ok().flatten();
    Some(old.is_some() && old == new)
}

/// The identity the `store.json` under `root` declares.
fn store_identity(root: &Path) -> Result<String, String> {
    let bytes = fs::read(root.join("store.json")).map_err(|error| error.to_string())?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| "store.json is not JSON".to_owned())?;
    value
        .get("identity")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "store.json declares no identity".to_owned())
}

/// `store.json` changed only from `eventlog-tree/1` to `eventlog-tree/2`, keeping its identity.
fn is_format_upgrade(old: &[u8], new: &[u8]) -> bool {
    let (Ok(serde_json::Value::Object(mut old)), Ok(serde_json::Value::Object(mut new))) = (
        serde_json::from_slice::<serde_json::Value>(old),
        serde_json::from_slice::<serde_json::Value>(new),
    ) else {
        return false;
    };
    old.remove("format")
        .as_ref()
        .and_then(serde_json::Value::as_str)
        == Some(StoreFormat::V1.tag())
        && new
            .remove("format")
            .as_ref()
            .and_then(serde_json::Value::as_str)
            == Some(StoreFormat::V2.tag())
        && old == new
}

fn is_redaction(old: &[u8], new: &[u8]) -> bool {
    let (Ok(old), Ok(new)) = (
        serde_json::from_slice::<serde_json::Value>(old),
        serde_json::from_slice::<serde_json::Value>(new),
    ) else {
        return false;
    };
    let (Some(old), Some(new)) = (old.as_object(), new.as_object()) else {
        return false;
    };
    if old.contains_key("redacted") || !new.contains_key("redacted") {
        return false;
    }
    let Some(reason) = new
        .get("redacted")
        .and_then(|redaction| redaction.get("reason"))
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    let envelope_equal = old
        .iter()
        .filter(|(key, _)| key.as_str() != "data")
        .all(|(key, value)| new.get(key) == Some(value));
    let only_expected_keys = new
        .keys()
        .all(|key| old.contains_key(key) || key == "redacted");
    envelope_equal
        && only_expected_keys
        && new.get("data") == Some(&eventlog_core::redaction_tombstone(reason))
}
