//! The replayed state of an unchanged tree, kept so that the next open skips the replay.
//!
//! Opening a tree replays every file into an in-memory engine, and the engine and every projector
//! over it hash what they are given. On a store nobody has changed that work is the same each
//! time, so the result is kept under `.cache/state/`, named by a digest of every history file's
//! stamp (`dev`, `ino`, length, `mtime`, `ctime`) together with the projectors registered, each
//! with its fingerprint, and the running executable. An open whose files all carry the same stamps
//! loads it, and opens no event file at all. No state is kept or loaded while any registered
//! projector has no fingerprint ([`eventlog_core::Projector::fingerprint`]): a projector whose
//! configuration changed under one name and one executable would otherwise be served the rows its
//! earlier configuration wrote.
//!
//! What a stamp detects is set out in the file provider's design (Part B § B3): any write,
//! truncation or rename moves `ctime` or `ino`, and userspace cannot set `ctime`. A file whose
//! `ctime` is within [`UNTRUSTED_NS`] of the open is too new to be told apart from a same-length
//! rewrite in the same clock tick, so no state is kept while any file is that new. The next open
//! after that window replays once and keeps it.
//!
//! `.cache/` holds its own `.gitignore`, so a cache is never committed, and `verify` never reads it.
//!
//! A removal of content (a redaction, a tenant's erasure, a blob's deletion) changes a file in
//! place or removes it, which a kept state and another handle's engine both predate. So each such
//! removal, under the writer lock, deletes every kept state and publishes a new privacy token in
//! `.cache/privacy`. A state records the token it was built under; a handle whose token is no
//! longer the published one rebuilds before it serves, and a state is kept only while the token
//! it was built under is still the published one.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use eventlog_core::EventLogError;
use serde_json::{Value, json};

use crate::layout::{backend, canonical, sha256_hex, write_atomic};

fn privacy_path(root: &Path) -> PathBuf {
    root.join(".cache").join("privacy")
}

/// The privacy token last published under `root`, `None` when none ever was.
///
/// # Errors
/// Returns [`EventLogError::Backend`] when the token exists but cannot be read: a handle that
/// cannot tell whether it is current must not serve.
pub(crate) fn privacy_token(root: &Path) -> Result<Option<String>, EventLogError> {
    match fs::read(privacy_path(root)) {
        Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(backend(error)),
    }
}

/// Delete every kept state and publish a new privacy token, which it returns. Called under the
/// writer lock by every operation that removes content.
///
/// # Errors
/// Returns [`EventLogError::Backend`] when a kept state cannot be removed or the token cannot be
/// written: the removal is then not complete, and the caller must say so.
pub(crate) fn publish_removal(root: &Path) -> Result<String, EventLogError> {
    match fs::remove_dir_all(root.join(".cache").join("state")) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(backend(error)),
    }
    ignored_cache(root).map_err(backend)?;
    let token = eventlog_core::new_event_id();
    write_atomic(&privacy_path(root), token.as_bytes())?;
    Ok(token)
}

/// Create `.cache/` with the `.gitignore` that keeps it out of history.
fn ignored_cache(root: &Path) -> std::io::Result<()> {
    let cache = root.join(".cache");
    fs::create_dir_all(&cache)?;
    let ignore = cache.join(".gitignore");
    if !ignore.exists() {
        fs::write(&ignore, b"*\n")?;
    }
    Ok(())
}

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
            let entry = entry.ok()?;
            let path = entry.path();
            // The entry's own type comes with the directory listing, so a directory costs no
            // metadata request and a regular file only the one its stamp makes. A symbolic link is
            // followed into a directory it names, as before, and otherwise stamped as the link.
            let directory = match entry.file_type() {
                Ok(kind) if kind.is_symlink() => followed_is_dir(&path),
                Ok(kind) => kind.is_dir(),
                Err(_) => followed_is_dir(&path),
            };
            if directory {
                stack.push(path);
            } else {
                stamp_into(root, &path, &mut found)?;
            }
        }
    }
    Some(found)
}

#[cfg(test)]
thread_local! {
    /// How many metadata requests [`stamps`] made on this thread.
    static METADATA_REQUESTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Every metadata request [`stamps`] makes goes through here or [`own_metadata`].
fn followed_is_dir(path: &Path) -> bool {
    #[cfg(test)]
    METADATA_REQUESTS.with(|count| count.set(count.get() + 1));
    path.is_dir()
}

/// The metadata of `path` itself, a symbolic link as the link.
#[cfg(unix)]
fn own_metadata(path: &Path) -> std::io::Result<fs::Metadata> {
    #[cfg(test)]
    METADATA_REQUESTS.with(|count| count.set(count.get() + 1));
    fs::symlink_metadata(path)
}

#[cfg(unix)]
fn stamp_into(root: &Path, path: &Path, found: &mut Stamps) -> Option<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = match own_metadata(path) {
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

/// The name a state is kept under: every stamp, every projector with its fingerprint, and the
/// program that folded it.
pub(crate) fn key(
    stamps: &Stamps,
    inline: &[(&str, String)],
    catch_up: &[(&str, String)],
) -> String {
    // The executable also stands in for a projector's code: a rebuilt program replays once rather
    // than trusting a projection an older one wrote, whatever fingerprint it reports.
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
/// nothing, which only costs the next open a replay. The caller holds the writer lock, so no
/// removal of content runs while a state is written.
pub(crate) fn keep(root: &Path, key: &str, image: &[u8], mut state: Value) {
    let directory = root.join(".cache").join("state");
    if ignored_cache(root).is_err() || fs::create_dir_all(&directory).is_err() {
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// A warm open stamps every history file; review finding L70 counted two metadata requests
    /// per file (one to classify the path, one for its stamp) and one per directory. The
    /// directory listing already says what each entry is.
    #[test]
    fn stamps_request_metadata_once_per_file_and_never_for_a_directory() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join("store.json"), b"{}").unwrap();
        let mut files = 1;
        for (sub, count) in [
            ("streams/item/one", 3),
            ("streams/item/two", 2),
            ("groups/ab", 4),
        ] {
            let path = root.join("tenants/tenant-a").join(sub);
            fs::create_dir_all(&path).unwrap();
            for index in 0..count {
                fs::write(path.join(format!("{index}.json")), b"{}").unwrap();
                files += 1;
            }
        }
        METADATA_REQUESTS.with(|count| count.set(0));
        let stamped = stamps(root).unwrap();
        assert_eq!(stamped.len(), files);
        assert_eq!(
            METADATA_REQUESTS.with(std::cell::Cell::get),
            files,
            "one metadata request per file and none per directory"
        );
    }

    /// The stamp rules for a symbolic link stay as they were: a link to a directory is followed
    /// into it, any other link is stamped as the link itself, so retargeting it moves the stamp.
    #[test]
    fn a_link_to_a_directory_is_followed_and_any_other_link_is_stamped_as_the_link() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join("store.json"), b"{}").unwrap();
        let outside = root.join("outside");
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("event.json"), b"{\"a\":1}").unwrap();
        fs::write(outside.join("long-target-name.json"), b"{}").unwrap();
        let tenant = root.join("tenants/tenant-a");
        fs::create_dir_all(&tenant).unwrap();
        std::os::unix::fs::symlink(&outside, tenant.join("linked")).unwrap();
        std::os::unix::fs::symlink(outside.join("event.json"), tenant.join("file-link")).unwrap();
        let stamped = stamps(root).unwrap();
        assert!(stamped.contains_key("tenants/tenant-a/linked/event.json"));
        let link = stamped["tenants/tenant-a/file-link"];
        fs::remove_file(tenant.join("file-link")).unwrap();
        std::os::unix::fs::symlink(
            outside.join("long-target-name.json"),
            tenant.join("file-link"),
        )
        .unwrap();
        let moved = stamps(root).unwrap()["tenants/tenant-a/file-link"];
        assert_ne!(
            link[2], moved[2],
            "the link's own length is its target's name"
        );
    }
}
