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

/// The format tag in `store.json` of a store every blob of which is a raw file.
pub(crate) const FORMAT_V1: &str = "eventlog-tree/1";
/// The format tag in `store.json` of a store that may also hold split blobs and texts
/// (`crate::split`). A new store is created in it unless its owner asks for
/// [`TreeLayout::Hashed`].
pub(crate) const FORMAT: &str = "eventlog-tree/2";
/// The format tag in `store.json` of a store that is `eventlog-tree/2` except that each stream's
/// directory is named by a fixed-length hash of its key ([`stream_dir`]). Written only by a store
/// created with [`TreeLayout::Hashed`].
pub(crate) const FORMAT_V3: &str = "eventlog-tree/3";

/// Where a new store puts each stream's event files. The layout is chosen once, when the store is
/// created, and recorded in its `store.json`; every later open keeps it, whatever it asks for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum TreeLayout {
    /// `streams/<type>/<id>/<digest>.json` (`eventlog-tree/1` and `eventlog-tree/2`): the layout
    /// every release before this one wrote, and the one a store is created in unless its owner
    /// asks otherwise. A path grows with its stream type and id.
    #[default]
    Nested,
    /// `streams/<kk>/<32 hex>/<digest>.json` (`eventlog-tree/3`): the directory is the first 32
    /// hex characters of the SHA-256 of the escaped `<type>/<id>`, fanned out by its first two.
    /// Every event path has one length whatever the stream, short enough for a Git checkout on
    /// Windows without long paths. Eventlog 0.8.4 and earlier refuse to open such a store.
    Hashed,
}

/// Which of the layouts a store's `store.json` declares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreFormat {
    /// `eventlog-tree/1`: raw blob files only.
    V1,
    /// `eventlog-tree/2`: raw blob files, split blobs and texts.
    V2,
    /// `eventlog-tree/3`: `eventlog-tree/2` with hashed stream directories.
    V3,
}

impl StoreFormat {
    /// The tag `store.json` spells this format with.
    pub(crate) fn tag(self) -> &'static str {
        match self {
            Self::V1 => FORMAT_V1,
            Self::V2 => FORMAT,
            Self::V3 => FORMAT_V3,
        }
    }

    /// Whether a blob may be kept split.
    pub(crate) fn splits(self) -> bool {
        matches!(self, Self::V2 | Self::V3)
    }

    /// Where this format puts each stream's event files.
    pub(crate) fn layout(self) -> TreeLayout {
        match self {
            Self::V1 | Self::V2 => TreeLayout::Nested,
            Self::V3 => TreeLayout::Hashed,
        }
    }
}

/// The format `store.json` under `root` declares.
///
/// # Errors
/// Refuses a `store.json` that is not a tree store's, including one of a format this reader does
/// not know, by name.
pub(crate) fn store_format(root: &Path) -> Result<StoreFormat, EventLogError> {
    let bytes = fs::read(root.join("store.json")).map_err(backend)?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| corrupt("store.json"))?;
    if value.get("identity").and_then(Value::as_str).is_none() {
        return Err(corrupt("store.json is not an eventlog-tree store"));
    }
    match value.get("format").and_then(Value::as_str) {
        Some(FORMAT_V1) => Ok(StoreFormat::V1),
        Some(FORMAT) => Ok(StoreFormat::V2),
        Some(FORMAT_V3) => Ok(StoreFormat::V3),
        _ => Err(corrupt("store.json is not an eventlog-tree store")),
    }
}
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

/// The directory of a stream's event files in a store of `format`.
///
/// Nested, it is `streams/<type>/<id>`, each escaped. Hashed, it is `streams/<kk>/<h>`, where `h`
/// is the first 32 hex characters of the SHA-256 of `<type>/<id>`, each escaped (escaping
/// encodes `/`, so the key is unambiguous), and `kk` its first two. The directory only groups a
/// stream's files: every event file names its own stream, and a reader refuses one that is not
/// in the directory this function gives for it.
pub(crate) fn stream_dir(
    root: &Path,
    format: StoreFormat,
    tenant: &str,
    stream_type: &str,
    stream_id: &str,
) -> PathBuf {
    let streams = tenant_dir(root, tenant).join("streams");
    match format.layout() {
        TreeLayout::Nested => streams.join(segment(stream_type)).join(segment(stream_id)),
        TreeLayout::Hashed => {
            let key = format!("{}/{}", segment(stream_type), segment(stream_id));
            let name = &sha256_hex(key.as_bytes())[..32];
            streams.join(&name[..2]).join(name)
        }
    }
}

pub(crate) fn event_path(
    root: &Path,
    format: StoreFormat,
    tenant: &str,
    stream_type: &str,
    stream_id: &str,
    digest: &str,
) -> PathBuf {
    stream_dir(root, format, tenant, stream_type, stream_id).join(format!("{digest}.json"))
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
                length(&event_path(
                    root,
                    StoreFormat::V2,
                    tenant,
                    stream_type,
                    &stream_id,
                    &digest,
                )),
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

    /// `<CARGO_HOME>\git\checkouts\<name>-<16 hex>\<7-char rev>\` on GitHub's hosted Windows
    /// runner, for Eventlog: the prefix every tracked path of a Git dependency is checked out under.
    const EVENTLOG_CHECKOUT_PREFIX: usize = 76;
    /// The same for Entity Runtime, the longest consumer name the design budgets for.
    const ENTITY_RUNTIME_CHECKOUT_PREFIX: usize = 82;
    /// Git without `core.longpaths` refuses a path of this many characters or more.
    const MAX_PATH: usize = 260;

    /// `eventlog-tree/3` names a stream's directory by a fixed-length hash of its key, so the
    /// longest event path a planning store can hold no longer grows with the stream type or id:
    /// it is computed here over ids that segment escaping makes long, and held under `MAX_PATH`
    /// after the checkout prefix (`docs/design/tree-layout-path-length-v0.1.md`).
    #[test]
    fn a_hashed_store_writes_every_event_under_max_path_after_the_checkout_prefix() {
        let root = Path::new(".engineering/state");
        let tenant = "planning";
        let digest = "f".repeat(64);
        let long_id = "/".repeat(200);
        let sha_id = format!("sha256:{}", "f".repeat(64));
        let streams = [
            ("er.subject", sha_id.as_str()),
            ("er.subject", "x"),
            ("a.stream.type.of.some.length", long_id.as_str()),
        ];
        let lengths: Vec<usize> = streams
            .iter()
            .map(|(stream_type, stream_id)| {
                length(&event_path(
                    root,
                    StoreFormat::V3,
                    tenant,
                    stream_type,
                    stream_id,
                    &digest,
                ))
            })
            .collect();
        let longest = *lengths.iter().max().unwrap();
        assert_eq!(
            lengths,
            vec![longest; streams.len()],
            "a hashed event path grew with its stream type or id"
        );
        assert_eq!(
            longest, 149,
            "the hashed event path length changed: update the budget table in \
             docs/design/tree-layout-path-length-v0.1.md"
        );
        assert!(
            EVENTLOG_CHECKOUT_PREFIX + longest < MAX_PATH,
            "Eventlog's checkout prefix plus {longest} reaches MAX_PATH"
        );
        assert!(
            ENTITY_RUNTIME_CHECKOUT_PREFIX + longest < MAX_PATH,
            "Entity Runtime's checkout prefix plus {longest} reaches MAX_PATH"
        );
        // The nested layout is unchanged by the hashed one.
        assert_eq!(
            length(&event_path(
                root,
                StoreFormat::V2,
                tenant,
                "er.subject",
                &sha_id,
                &digest
            )),
            198
        );
    }

    #[test]
    fn a_hashed_stream_directory_is_a_fan_out_and_32_hex_of_the_escaped_key() {
        let directory = stream_dir(Path::new("r"), StoreFormat::V3, "t", "item", "a/b");
        let hash = sha256_hex(b"item/a%2Fb");
        assert_eq!(
            directory,
            Path::new("r/tenants/t/streams")
                .join(&hash[..2])
                .join(&hash[..32])
        );
        assert_ne!(
            directory,
            stream_dir(Path::new("r"), StoreFormat::V3, "t", "item/a", "b"),
            "two stream keys shared a directory"
        );
        for format in [StoreFormat::V1, StoreFormat::V2] {
            assert_eq!(
                stream_dir(Path::new("r"), format, "t", "item", "a/b"),
                Path::new("r/tenants/t/streams/item/a%2Fb")
            );
        }
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
