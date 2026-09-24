//! The offline check a pull request runs: every rule a merged tree must satisfy, reported together.
//!
//! It never trusts a cache and always hashes. It reads files only, so it runs where no store can be
//! opened, such as on the checked-out tree of a pull request and its merge base.

use crate::history;
use crate::layout::{canonical, json_files, subdirectories};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// One broken rule, with the file it was found in.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Finding {
    /// `V1` to `V5`, as the design names them.
    pub rule: &'static str,
    pub path: PathBuf,
    pub detail: String,
}

/// Check the tree at `root`: its layout (V1), torn writes (V3), digests and parents (V4) and
/// canonical bytes (V5). When `base` names the same store as the merge base holds it, also check
/// that no committed file was deleted or changed except by redaction (V2).
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

/// Every file under `root`, relative to it, except the writer lock and `.cache/`, which are not
/// history. The check never reads a cache: whoever can write one can write the files it names.
fn files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(directory) = stack.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
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

/// V1: only the files this store writes, each where it belongs.
fn layout(root: &Path, findings: &mut Vec<Finding>) {
    for path in files(root) {
        let parts: Vec<String> = path
            .components()
            .map(|part| part.as_os_str().to_string_lossy().into_owned())
            .collect();
        let known = match parts
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["store.json"] | ["tenants", _, "identity.json"] => true,
            ["tenants", _, "streams", _, _, file] => is_json(file) && !file.starts_with('.'),
            ["tenants", _, "groups", shard, file] => {
                shard.len() == 2 && is_json(file) && file.starts_with(*shard)
            }
            ["tenants", _, "blobs", _, file] => !file.starts_with('.'),
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

/// This crate names its files, always with a lower-case `.json`.
fn is_json(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|extension| extension == "json")
}

/// V5: every JSON file is in its one canonical encoding.
fn canonical_bytes(root: &Path, findings: &mut Vec<Finding>) {
    let mut directories = vec![root.to_owned()];
    for tenant in subdirectories(&root.join("tenants")).unwrap_or_default() {
        directories.push(tenant.clone());
        for shard in subdirectories(&tenant.join("groups")).unwrap_or_default() {
            directories.push(shard);
        }
        for kind in subdirectories(&tenant.join("streams")).unwrap_or_default() {
            directories.extend(subdirectories(&kind).unwrap_or_default());
        }
    }
    for directory in directories {
        for path in json_files(&directory).unwrap_or_default() {
            let Ok(bytes) = fs::read(&path) else { continue };
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

/// V2: every file the base holds is still here with the same bytes. The one exception is an event
/// file whose body was redacted: its envelope, and so its digest, must be unchanged.
fn immutability(base: &Path, root: &Path, findings: &mut Vec<Finding>) {
    let mut before: BTreeMap<PathBuf, Vec<u8>> = BTreeMap::new();
    for path in files(base) {
        if let Ok(bytes) = fs::read(base.join(&path)) {
            before.insert(path, bytes);
        }
    }
    for (path, old) in before {
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
        if !is_redaction(&old, &new) {
            findings.push(Finding {
                rule: "V2",
                path,
                detail: "a committed file was changed other than by redaction".into(),
            });
        }
    }
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
