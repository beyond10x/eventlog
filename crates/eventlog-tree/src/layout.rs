//! Where each file lives, how its bytes are written, and how they are digested.
//!
//! Every tracked file is created once and never rewritten, except an event file under redaction.
//! That is what lets two branches of one store merge in Git without a textual conflict: they add
//! different files.

use eventlog_core::EventLogError;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

/// The format tag in `store.json`.
pub(crate) const FORMAT: &str = "eventlog-tree/1";
/// The format tag of an event file.
pub(crate) const EVENT_FORMAT: &str = "eventlog-tree/event/1";
/// The format tag of a group file.
pub(crate) const GROUP_FORMAT: &str = "eventlog-tree/group/1";

pub(crate) fn backend(error: impl std::fmt::Display) -> EventLogError {
    EventLogError::Backend(error.to_string())
}

pub(crate) fn corrupt(what: &str) -> EventLogError {
    EventLogError::Backend(format!("tree store is corrupt: {what}"))
}

/// One path segment for an identifier: bytes outside `[A-Za-z0-9._-]` become `%XX`, so any id is
/// a single, portable file name and two ids never share one.
pub(crate) fn segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    if out.starts_with('.') {
        // A leading dot would hide the entry and could spell `.` or `..`.
        out.replace_range(0..1, "%2E");
    }
    out
}

pub(crate) fn tenant_dir(root: &Path, tenant: &str) -> PathBuf {
    root.join("tenants").join(segment(tenant))
}

pub(crate) fn stream_dir(root: &Path, tenant: &str, stream_type: &str, stream_id: &str) -> PathBuf {
    tenant_dir(root, tenant)
        .join("streams")
        .join(segment(stream_type))
        .join(segment(stream_id))
}

pub(crate) fn event_path(
    root: &Path,
    tenant: &str,
    stream_type: &str,
    stream_id: &str,
    digest: &str,
) -> PathBuf {
    stream_dir(root, tenant, stream_type, stream_id).join(format!("{digest}.json"))
}

pub(crate) fn group_path(root: &Path, tenant: &str, key_digest: &str) -> PathBuf {
    tenant_dir(root, tenant)
        .join("groups")
        .join(&key_digest[..2])
        .join(format!("{key_digest}.json"))
}

pub(crate) fn blob_path(root: &Path, tenant: &str, digest: &str) -> PathBuf {
    let shard = if digest.len() >= 2 {
        &digest[..2]
    } else {
        "__"
    };
    tenant_dir(root, tenant)
        .join("blobs")
        .join(segment(shard))
        .join(segment(digest))
}

pub(crate) fn identity_path(root: &Path, tenant: &str) -> PathBuf {
    tenant_dir(root, tenant).join("identity.json")
}

/// JSON with object keys in byte order and no insignificant whitespace, so one value has exactly
/// one encoding whatever map order the caller's `serde_json` build keeps.
pub(crate) fn canonical(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            out.push(b'{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                out.extend(serde_json::to_vec(key).expect("a string always encodes"));
                out.push(b':');
                write_canonical(&map[key], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        scalar => out.extend(serde_json::to_vec(scalar).expect("a scalar always encodes")),
    }
}

/// Lower-case hexadecimal SHA-256.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Write `bytes` to `path` so that a reader sees either nothing or all of it: a temporary sibling,
/// synced, then renamed over the name. The directory is synced so the name survives a crash.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), EventLogError> {
    let directory = path
        .parent()
        .ok_or_else(|| corrupt("a file with no directory"))?;
    fs::create_dir_all(directory).map_err(backend)?;
    let name = path
        .file_name()
        .ok_or_else(|| corrupt("a file with no name"))?
        .to_string_lossy()
        .into_owned();
    let staging = directory.join(format!(".{name}.tmp"));
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&staging)
            .map_err(backend)?;
        file.write_all(bytes).map_err(backend)?;
        file.sync_all().map_err(backend)?;
    }
    fs::rename(&staging, path).map_err(backend)?;
    sync_dir(directory)
}

/// Create `path` with `bytes` unless it already exists; an existing file must hold the same bytes.
///
/// Event and group files are content-named and immutable, so a second write of one is either the
/// same fact again or a collision, never an update.
pub(crate) fn write_once(path: &Path, bytes: &[u8]) -> Result<(), EventLogError> {
    match fs::read(path) {
        Ok(existing) if existing == bytes => Ok(()),
        Ok(_) => Err(corrupt("an immutable file already holds different bytes")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => write_atomic(path, bytes),
        Err(error) => Err(backend(error)),
    }
}

pub(crate) fn sync_dir(directory: &Path) -> Result<(), EventLogError> {
    File::open(directory)
        .and_then(|handle| handle.sync_all())
        .map_err(backend)
}

/// Every regular `*.json` file directly under `directory`, sorted by name. Staging files, which
/// start with a dot, are not facts and are skipped.
pub(crate) fn json_files(directory: &Path) -> Result<Vec<PathBuf>, EventLogError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(backend(error)),
    };
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(backend)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        // Only this crate writes these names, always lower-case.
        if name.starts_with('.') || Path::new(&name).extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        if entry.file_type().map_err(backend)?.is_file() {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

/// Every directory directly under `directory`, sorted by name.
pub(crate) fn subdirectories(directory: &Path) -> Result<Vec<PathBuf>, EventLogError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(backend(error)),
    };
    let mut found = Vec::new();
    for entry in entries {
        let entry = entry.map_err(backend)?;
        if entry.file_type().map_err(backend)?.is_dir() {
            found.push(entry.path());
        }
    }
    found.sort();
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_segment_is_one_portable_name_and_never_collides() {
        assert_eq!(segment("story"), "story");
        assert_eq!(segment("a/b"), "a%2Fb");
        assert_eq!(segment(".."), "%2E.");
        assert_ne!(
            segment("a/b"),
            segment("a%2Fb"),
            "an escape and its text shared a name"
        );
    }

    /// The characters in `path`, the unit Windows' 260-character `MAX_PATH` counts in.
    fn length(path: &Path) -> usize {
        path.to_string_lossy().chars().count()
    }

    /// A Git checkout of a repository whose planning store is a tree adds each of these paths to
    /// the checkout prefix, so a layout change that moves one is a change to every consumer's
    /// Windows path budget (`docs/design/tree-layout-path-length-v0.1.md`). The inputs are the
    /// longest a planning store holds today: its root, its tenant, a stream type and an
    /// `sha256:`-prefixed stream id, and full SHA-256 digests.
    #[test]
    fn a_planning_store_writes_each_file_kind_at_its_recorded_longest_path_length() {
        let root = Path::new(".engineering/state");
        let tenant = "planning";
        let stream_type = "er.subject";
        let stream_id = format!("sha256:{}", "f".repeat(64));
        let digest = "f".repeat(64);
        let blob_digest = format!("sha256:{digest}");
        let measured = [
            ("identity", length(&identity_path(root, tenant))),
            (
                "event",
                length(&event_path(root, tenant, stream_type, &stream_id, &digest)),
            ),
            ("group", length(&group_path(root, tenant, &digest))),
            ("blob", length(&blob_path(root, tenant, &blob_digest))),
        ];
        assert_eq!(
            measured,
            [
                ("identity", 49),
                ("event", 198),
                ("group", 115),
                ("blob", 118)
            ],
            "a layout function changed a path length: update the budget table in \
             docs/design/tree-layout-path-length-v0.1.md with these values"
        );
    }

    #[test]
    fn canonical_json_sorts_keys_at_every_depth() {
        let one = json!({ "b": 1, "a": { "d": [1, { "z": 0, "y": 1 }], "c": "x" } });
        assert_eq!(
            String::from_utf8(canonical(&one)).unwrap(),
            r#"{"a":{"c":"x","d":[1,{"y":1,"z":0}]},"b":1}"#
        );
    }
}
