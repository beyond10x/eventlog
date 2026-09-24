//! The replayed state of an unchanged tree, kept so that the next open skips the replay.
//!
//! Opening a tree replays every file into an in-memory engine, and the engine and every projector
//! over it hash what they are given. On a store nobody has changed that work is the same each
//! time, so the result is kept under `.cache/state/`, named by a digest of every history file's
//! stamp (`dev`, `ino`, length, `mtime`, `ctime`) together with the projectors registered and the
//! running executable. An open whose files all carry the same stamps loads it, and opens no event
//! file at all.
//!
//! What a stamp detects is set out in the file provider's design (Part B § B3): any write,
//! truncation or rename moves `ctime` or `ino`, and userspace cannot set `ctime`. A file whose
//! `ctime` is within [`UNTRUSTED_NS`] of the open is too new to be told apart from a same-length
//! rewrite in the same clock tick, so no state is kept while any file is that new. The next open
//! after that window replays once and keeps it.
//!
//! `.cache/` holds its own `.gitignore`, so a cache is never committed, and `verify` never reads it.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde_json::{Value, json};

use crate::layout::{canonical, sha256_hex};

/// Only a history file whose `ctime` is this far behind the open is trusted by its stamp.
pub(crate) const UNTRUSTED_NS: i128 = 2_000_000_000;

const FORMAT: &str = "eventlog-tree-fold/1";

/// Each history file's stamp, by its path relative to the root.
pub(crate) type Stamps = BTreeMap<String, [i128; 5]>;

/// The stamps of every file the history is made of: `store.json` and everything under `tenants/`.
/// `None` where stamps cannot be read, which only costs the replay.
pub(crate) fn stamps(root: &Path) -> Option<Stamps> {
    let mut found = Stamps::new();
    stamp_into(root, &root.join("store.json"), &mut found)?;
    let mut stack = vec![root.join("tenants")];
    while let Some(directory) = stack.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return None,
        };
        for entry in entries {
            let path = entry.ok()?.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                stamp_into(root, &path, &mut found)?;
            }
        }
    }
    Some(found)
}

#[cfg(unix)]
fn stamp_into(root: &Path, path: &Path, found: &mut Stamps) -> Option<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Some(()),
        Err(_) => return None,
    };
    let nanos = |seconds: i64, nanos: i64| i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    found.insert(
        path.strip_prefix(root).ok()?.to_string_lossy().into_owned(),
        [
            i128::from(metadata.dev()),
            i128::from(metadata.ino()),
            i128::from(metadata.size()),
            nanos(metadata.mtime(), metadata.mtime_nsec()),
            nanos(metadata.ctime(), metadata.ctime_nsec()),
        ],
    );
    Some(())
}

#[cfg(not(unix))]
fn stamp_into(_root: &Path, _path: &Path, _found: &mut Stamps) -> Option<()> {
    None
}

/// Whether every stamp is old enough to be trusted at `now_ns`.
pub(crate) fn settled(stamps: &Stamps, now_ns: i128) -> bool {
    stamps
        .values()
        .all(|stamp| stamp[4] < now_ns - UNTRUSTED_NS)
}

/// The current time, as `ctime` counts it.
pub(crate) fn now_ns() -> i128 {
    time::OffsetDateTime::now_utc().unix_timestamp_nanos()
}

/// The name a state is kept under: every stamp, every projector, and the program that folded it.
pub(crate) fn key(stamps: &Stamps, inline: &[&str], catch_up: &[&str]) -> String {
    // A projector has no version of its own, so the executable stands in for its code: a rebuilt
    // program replays once rather than trusting a projection an older one wrote.
    let program = std::env::current_exe()
        .ok()
        .and_then(|path| {
            let mut own = Stamps::new();
            let parent = path.parent()?.to_owned();
            stamp_into(&parent, &path, &mut own)?;
            Some(json!({ "path": path.to_string_lossy(), "stamp": own.values().next().map(|s| s.map(|v| v.to_string())) }))
        })
        .unwrap_or(Value::Null);
    let stamps: BTreeMap<&String, [String; 5]> = stamps
        .iter()
        .map(|(path, stamp)| (path, stamp.map(|value| value.to_string())))
        .collect();
    sha256_hex(&canonical(&json!({
        "format": FORMAT,
        "stamps": stamps,
        "inline": inline,
        "catch_up": catch_up,
        "program": program,
    })))
}

fn paths(root: &Path, key: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let directory = root.join(".cache").join("state");
    (
        directory.join(format!("{key}.sqlite")),
        directory.join(format!("{key}.json")),
    )
}

/// The engine image and the state beside it kept under `key`, when both are there.
pub(crate) fn load(root: &Path, key: &str) -> Option<(Vec<u8>, Value)> {
    let (image, state) = paths(root, key);
    let state: Value = serde_json::from_slice(&fs::read(state).ok()?).ok()?;
    if state.get("format").and_then(Value::as_str) != Some(FORMAT) {
        return None;
    }
    Some((fs::read(image).ok()?, state))
}

/// Keep `image` and `state` under `key`, replacing whatever was kept before. A failure keeps
/// nothing, which only costs the next open a replay.
pub(crate) fn keep(root: &Path, key: &str, image: &[u8], mut state: Value) {
    let cache = root.join(".cache");
    let directory = cache.join("state");
    if fs::create_dir_all(&directory).is_err() {
        return;
    }
    let ignore = cache.join(".gitignore");
    if !ignore.exists() && fs::write(&ignore, b"*\n").is_err() {
        return;
    }
    if let Some(map) = state.as_object_mut() {
        map.insert("format".into(), Value::String(FORMAT.into()));
    }
    let (image_path, state_path) = paths(root, key);
    let written = (|| {
        let temporary = directory.join(format!("{key}.{}.partial", std::process::id()));
        fs::write(&temporary, image)?;
        fs::rename(&temporary, &image_path)?;
        fs::write(&temporary, canonical(&state))?;
        fs::rename(&temporary, &state_path)
    })();
    if written.is_err() {
        let _ = fs::remove_file(&image_path);
        let _ = fs::remove_file(&state_path);
        return;
    }
    // Only the newest state is ever read again.
    if let Ok(entries) = fs::read_dir(&directory) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path != image_path && path != state_path {
                let _ = fs::remove_file(path);
            }
        }
    }
}
