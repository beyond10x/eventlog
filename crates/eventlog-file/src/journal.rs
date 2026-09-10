//! Durable JSONL framing. The manifest is the commit point, never a best-effort cache.
use eventlog_core::{EventLogError, new_event_id};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const FORMAT: &str = "eventlog-file/1";
const ZERO: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[cfg(test)]
fn checkpoint(name: &str) {
    if std::env::var("EVENTLOG_FILE_CRASH_AT").as_deref() == Ok(name) {
        std::process::exit(73);
    }
}
#[cfg(not(test))]
fn checkpoint(_: &str) {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    format: String,
    pub store: String,
    pub epoch: u64,
    pub sequence: u64,
    pub length: u64,
    pub digest: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    format: String,
    store: String,
    epoch: u64,
    sequence: u64,
    previous: String,
    transaction: Value,
    digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivacyIntent {
    before: Manifest,
    after: Manifest,
    replacement_digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendIntent {
    before: Manifest,
    after: Manifest,
    frame: String,
}

pub(crate) struct Journal {
    root: PathBuf,
    // Kept open for the entire read/decide/commit cycle. Each caller opens its own description.
    _lock: File,
    pub manifest: Manifest,
    pub transactions: Vec<Value>,
}

pub(crate) fn backend(error: impl std::fmt::Display) -> EventLogError {
    EventLogError::Backend(error.to_string())
}
fn corrupt() -> EventLogError {
    EventLogError::Backend("file journal integrity check failed; no history was repaired".into())
}
pub(crate) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn sync_dir(root: &Path) -> Result<(), EventLogError> {
    File::open(root)
        .and_then(|file| file.sync_all())
        .map_err(backend)
}
fn regular(path: &Path) -> Result<(), EventLogError> {
    if !fs::symlink_metadata(path).map_err(backend)?.is_file() {
        return Err(corrupt());
    }
    Ok(())
}
fn read(path: &Path) -> Result<Vec<u8>, EventLogError> {
    regular(path)?;
    fs::read(path).map_err(backend)
}
fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), EventLogError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(backend)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(backend)
}
fn atomic_json(root: &Path, name: &str, value: &impl Serialize) -> Result<(), EventLogError> {
    let staging = root.join(format!(".write-{}", new_event_id()));
    write_synced(&staging, &serde_json::to_vec(value).map_err(backend)?)?;
    fs::rename(&staging, root.join(name)).map_err(backend)?;
    sync_dir(root)
}

impl Journal {
    pub fn open(root: &Path) -> Result<Self, EventLogError> {
        fs::create_dir_all(root).map_err(backend)?;
        if !fs::symlink_metadata(root).map_err(backend)?.is_dir() {
            return Err(corrupt());
        }
        let lock_path = root.join("writer.lock");
        if fs::symlink_metadata(&lock_path).is_ok() {
            regular(&lock_path)?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(backend)?;
        lock.lock().map_err(backend)?;
        let manifest_path = root.join("manifest.json");
        if !manifest_path.exists() {
            // Never interpret an existing history without its commit authority as a new store.
            if root.join("events.jsonl").exists() || root.join("privacy.json").exists() {
                return Err(corrupt());
            }
            let manifest = Manifest {
                format: FORMAT.into(),
                store: new_event_id(),
                epoch: 0,
                sequence: 0,
                length: 0,
                digest: ZERO.into(),
            };
            write_synced(&root.join("events.jsonl"), b"")?;
            sync_dir(root)?;
            atomic_json(root, "manifest.json", &manifest)?;
            if let Some(parent) = root.parent() {
                sync_dir(parent)?;
            }
        }
        let mut manifest: Manifest =
            serde_json::from_slice(&read(&manifest_path)?).map_err(|_| corrupt())?;
        if manifest.format != FORMAT {
            return Err(corrupt());
        }
        if root.join("privacy.json").exists() && root.join("append.json").exists() {
            // No serialized writer can publish both. A mixed copy is not recovery authority.
            return Err(corrupt());
        }
        if root.join("privacy.json").exists() {
            manifest = recover_privacy(root, &manifest)?;
        }
        if root.join("append.json").exists() {
            recover_append(root, &manifest)?;
        }
        let events_path = root.join("events.jsonl");
        regular(&events_path)?;
        let mut events = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&events_path)
            .map_err(backend)?;
        if events.metadata().map_err(backend)?.len() < manifest.length {
            return Err(corrupt());
        }
        let mut committed = Vec::new();
        (&mut events)
            .take(manifest.length)
            .read_to_end(&mut committed)
            .map_err(backend)?;
        let transactions = decode(&committed, &manifest)?;
        if events.metadata().map_err(backend)?.len() > manifest.length {
            return Err(corrupt());
        }
        // These files were never selected by a durable intent. They may contain sensitive bytes.
        for entry in fs::read_dir(root).map_err(backend)? {
            let entry = entry.map_err(backend)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == "privacy.next"
                || name.strip_prefix(".write-").is_some_and(|id| {
                    id.len() == 36 && id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
                })
            {
                regular(&entry.path())?;
                fs::remove_file(entry.path()).map_err(backend)?;
            }
        }
        sync_dir(root)?;
        Ok(Self {
            root: root.to_owned(),
            _lock: lock,
            manifest,
            transactions,
        })
    }

    pub fn append(&mut self, transaction: Value) -> Result<(), EventLogError> {
        let sequence = self.manifest.sequence.checked_add(1).ok_or_else(corrupt)?;
        let (line, digest) = encode(
            &self.manifest,
            sequence,
            &self.manifest.digest,
            transaction.clone(),
        )?;
        let mut next = self.manifest.clone();
        next.sequence = sequence;
        next.length = next
            .length
            .checked_add(line.len() as u64)
            .ok_or_else(corrupt)?;
        next.digest = digest;
        let intent = AppendIntent {
            before: self.manifest.clone(),
            after: next.clone(),
            frame: String::from_utf8(line.clone()).map_err(backend)?,
        };
        atomic_json(&self.root, "append.json", &intent)?;
        checkpoint("append-prepared");
        let mut file = OpenOptions::new()
            .write(true)
            .open(self.root.join("events.jsonl"))
            .map_err(backend)?;
        file.seek(SeekFrom::Start(self.manifest.length))
            .map_err(backend)?;
        let midpoint = line.len() / 2;
        file.write_all(&line[..midpoint]).map_err(backend)?;
        checkpoint("append-torn");
        file.write_all(&line[midpoint..])
            .and_then(|()| file.sync_all())
            .map_err(backend)?;
        // Failure after the rename can mean the commit exists. Do not encourage a new identity.
        checkpoint("append-synced");
        atomic_json(&self.root, "manifest.json", &next)
            .map_err(|_| EventLogError::UnknownCommit)?;
        checkpoint("append-committed");
        fs::remove_file(self.root.join("append.json"))
            .and_then(|()| File::open(&self.root)?.sync_all())
            .map_err(|_| EventLogError::UnknownCommit)?;
        self.manifest = next;
        self.transactions.push(transaction);
        Ok(())
    }

    pub fn extends(&self, observed: &Manifest) -> Result<bool, EventLogError> {
        if observed.store != self.manifest.store
            || observed.epoch != self.manifest.epoch
            || observed.sequence > self.manifest.sequence
        {
            return Ok(false);
        }
        let mut previous = ZERO.to_owned();
        for (index, transaction) in self
            .transactions
            .iter()
            .take(usize::try_from(observed.sequence).map_err(backend)?)
            .enumerate()
        {
            previous = encode(
                &self.manifest,
                index as u64 + 1,
                &previous,
                transaction.clone(),
            )?
            .1;
        }
        Ok(previous == observed.digest)
    }

    /// Privacy is the only rewrite path. The caller supplies history with only erased data removed.
    pub fn privacy(&mut self, transactions: Vec<Value>) -> Result<(), EventLogError> {
        let mut next = self.manifest.clone();
        next.epoch = next.epoch.checked_add(1).ok_or_else(corrupt)?;
        next.sequence = 0;
        next.length = 0;
        next.digest = ZERO.into();
        let mut bytes = Vec::new();
        for transaction in &transactions {
            next.sequence += 1;
            let (line, digest) = encode(&next, next.sequence, &next.digest, transaction.clone())?;
            bytes.extend(line);
            next.digest = digest;
        }
        next.length = bytes.len() as u64;
        let replacement = self.root.join("privacy.next");
        if replacement.exists() {
            fs::remove_file(&replacement).map_err(backend)?;
        }
        write_synced(&replacement, &bytes)?;
        sync_dir(&self.root)?;
        let intent = PrivacyIntent {
            before: self.manifest.clone(),
            after: next,
            replacement_digest: hash(&bytes),
        };
        atomic_json(&self.root, "privacy.json", &intent)?;
        checkpoint("privacy-prepared");
        self.manifest = recover_privacy(&self.root, &self.manifest)
            .map_err(|_| EventLogError::UnknownCommit)?;
        self.transactions = transactions;
        Ok(())
    }
}

fn recover_append(root: &Path, current: &Manifest) -> Result<(), EventLogError> {
    let intent: AppendIntent =
        serde_json::from_slice(&read(&root.join("append.json"))?).map_err(|_| corrupt())?;
    if current != &intent.before && current != &intent.after {
        return Err(corrupt());
    }
    let mut bytes = read(&root.join("events.jsonl"))?;
    let boundary = usize::try_from(intent.before.length).map_err(backend)?;
    if bytes.len() < boundary {
        return Err(corrupt());
    }
    decode(&bytes[..boundary], &intent.before)?;
    let suffix = &bytes[boundary..];
    if !intent.frame.as_bytes().starts_with(suffix) {
        return Err(corrupt());
    }
    let mut planned = bytes[..boundary].to_vec();
    planned.extend(intent.frame.as_bytes());
    decode(&planned, &intent.after)?;
    if current == &intent.after {
        decode(&bytes, current)?;
    } else {
        bytes.truncate(boundary);
        let file = OpenOptions::new()
            .write(true)
            .open(root.join("events.jsonl"))
            .map_err(backend)?;
        file.set_len(intent.before.length)
            .and_then(|()| file.sync_all())
            .map_err(backend)?;
    }
    fs::remove_file(root.join("append.json")).map_err(backend)?;
    sync_dir(root)
}

fn encode(
    manifest: &Manifest,
    sequence: u64,
    previous: &str,
    transaction: Value,
) -> Result<(Vec<u8>, String), EventLogError> {
    let mut frame = Frame {
        format: FORMAT.into(),
        store: manifest.store.clone(),
        epoch: manifest.epoch,
        sequence,
        previous: previous.into(),
        transaction,
        digest: String::new(),
    };
    frame.digest = hash(&serde_json::to_vec(&frame).map_err(backend)?);
    let mut bytes = serde_json::to_vec(&frame).map_err(backend)?;
    bytes.push(b'\n');
    Ok((bytes, frame.digest))
}
fn decode(bytes: &[u8], manifest: &Manifest) -> Result<Vec<Value>, EventLogError> {
    if bytes.len() as u64 != manifest.length || (!bytes.is_empty() && bytes.last() != Some(&b'\n'))
    {
        return Err(corrupt());
    }
    let mut previous = ZERO.to_owned();
    let mut transactions = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let mut frame: Frame = serde_json::from_slice(line).map_err(|_| corrupt())?;
        if frame.format != FORMAT
            || frame.store != manifest.store
            || frame.epoch != manifest.epoch
            || frame.sequence != transactions.len() as u64 + 1
            || frame.previous != previous
        {
            return Err(corrupt());
        }
        let digest = std::mem::take(&mut frame.digest);
        if hash(&serde_json::to_vec(&frame).map_err(backend)?) != digest {
            return Err(corrupt());
        }
        previous = digest;
        transactions.push(frame.transaction);
    }
    if transactions.len() as u64 != manifest.sequence || previous != manifest.digest {
        return Err(corrupt());
    }
    Ok(transactions)
}
fn recover_privacy(root: &Path, current: &Manifest) -> Result<Manifest, EventLogError> {
    let intent: PrivacyIntent =
        serde_json::from_slice(&read(&root.join("privacy.json"))?).map_err(|_| corrupt())?;
    if (current != &intent.before && current != &intent.after)
        || intent.before.store != intent.after.store
        || intent.after.epoch != intent.before.epoch.checked_add(1).ok_or_else(corrupt)?
    {
        return Err(corrupt());
    }
    let events = read(&root.join("events.jsonl"))?;
    if hash(&events) == intent.replacement_digest {
        decode(&events, &intent.after)?;
    } else {
        decode(&events, &intent.before)?;
        let replacement = read(&root.join("privacy.next"))?;
        if hash(&replacement) != intent.replacement_digest {
            return Err(corrupt());
        }
        decode(&replacement, &intent.after)?;
        fs::rename(root.join("privacy.next"), root.join("events.jsonl")).map_err(backend)?;
        sync_dir(root)?;
        checkpoint("privacy-renamed");
    }
    atomic_json(root, "manifest.json", &intent.after)?;
    checkpoint("privacy-committed");
    if root.join("privacy.next").exists() {
        fs::remove_file(root.join("privacy.next")).map_err(backend)?;
    }
    fs::remove_file(root.join("privacy.json")).map_err(backend)?;
    sync_dir(root)?;
    Ok(intent.after)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::process::Command;

    // Invoked only by the parent tests with an isolated directory and failpoint.
    #[test]
    fn crash_child() {
        let Ok(root) = std::env::var("EVENTLOG_FILE_CRASH_ROOT") else {
            return;
        };
        if std::env::var_os("EVENTLOG_FILE_PROVIDER_PRIVACY").is_some() {
            use eventlog_core::EventStore;
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let store = crate::FileEventStore::open(&root).await.unwrap();
                store
                    .forget_tenant(&eventlog_core::TenantId::new("private-owner").unwrap())
                    .await
                    .unwrap();
            });
            panic!("provider failpoint was not reached");
        }
        let mut journal = Journal::open(Path::new(&root)).unwrap();
        if std::env::var("EVENTLOG_FILE_CRASH_AT")
            .unwrap()
            .starts_with("privacy")
        {
            journal.privacy(vec![json!({"value": "erased"})]).unwrap();
        } else {
            journal.append(json!({"value": "new"})).unwrap();
        }
        panic!("failpoint was not reached");
    }
    fn crash(root: &Path, point: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "journal::tests::crash_child", "--nocapture"])
            .env("EVENTLOG_FILE_CRASH_ROOT", root)
            .env("EVENTLOG_FILE_CRASH_AT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    #[test]
    fn process_death_at_each_append_boundary() {
        for point in [
            "append-prepared",
            "append-torn",
            "append-synced",
            "append-committed",
        ] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut journal = Journal::open(root.path()).unwrap();
                journal.append(json!({"value": "original"})).unwrap();
            }
            crash(root.path(), point);
            let journal = Journal::open(root.path()).unwrap();
            assert_eq!(journal.transactions[0], json!({"value":"original"}));
            assert_eq!(
                journal.transactions.len(),
                if point == "append-committed" { 2 } else { 1 },
                "{point}"
            );
            assert!(!root.path().join("append.json").exists());
            assert_eq!(
                fs::metadata(root.path().join("events.jsonl"))
                    .unwrap()
                    .len(),
                journal.manifest.length
            );
        }
    }
    #[test]
    fn process_death_at_each_privacy_boundary() {
        for point in ["privacy-prepared", "privacy-renamed", "privacy-committed"] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut journal = Journal::open(root.path()).unwrap();
                journal
                    .append(json!({"value": "sensitive-old-body"}))
                    .unwrap();
            }
            crash(root.path(), point);
            let journal = Journal::open(root.path()).unwrap();
            assert_eq!(journal.transactions, vec![json!({"value":"erased"})]);
            for entry in fs::read_dir(root.path()).unwrap() {
                let bytes = fs::read(entry.unwrap().path()).unwrap();
                assert!(!String::from_utf8_lossy(&bytes).contains("sensitive-old-body"));
            }
        }
    }
    #[test]
    fn committed_damage_and_unproven_suffix_are_never_repaired() {
        for mode in ["truncate", "alter", "suffix", "merge"] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut journal = Journal::open(root.path()).unwrap();
                journal.append(json!({"number": 17})).unwrap();
            }
            let path = root.path().join("events.jsonl");
            let mut bytes = fs::read(&path).unwrap();
            match mode {
                "truncate" => {
                    bytes.pop();
                }
                "alter" => {
                    let index = bytes.windows(2).position(|b| b == b"17").unwrap();
                    bytes[index] = b'2';
                }
                "suffix" => bytes.extend(b"{\"torn\":"),
                "merge" => bytes.extend(bytes.clone()),
                _ => unreachable!(),
            }
            fs::write(&path, &bytes).unwrap();
            assert!(Journal::open(root.path()).is_err(), "{mode}");
            assert_eq!(
                fs::read(&path).unwrap(),
                bytes,
                "refusal preserves evidence"
            );
        }
    }
    #[test]
    fn divergent_longer_history_does_not_extend_observed_head() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(root.path()).unwrap();
        journal.append(json!({"branch":"left"})).unwrap();
        let observed = journal.manifest.clone();
        // Independently encoded fork, with the same store identity and greater sequence.
        let mut fork = journal.manifest.clone();
        let (a, digest) = encode(&fork, 1, ZERO, json!({"branch":"right"})).unwrap();
        let (b, last) = encode(&fork, 2, &digest, json!({"next":true})).unwrap();
        let bytes = [a, b].concat();
        fork.sequence = 2;
        fork.length = bytes.len() as u64;
        fork.digest = last;
        fs::write(root.path().join("events.jsonl"), &bytes).unwrap();
        fs::write(
            root.path().join("manifest.json"),
            serde_json::to_vec(&fork).unwrap(),
        )
        .unwrap();
        drop(journal);
        let fork = Journal::open(root.path()).unwrap();
        assert!(!fork.extends(&observed).unwrap());
    }

    #[test]
    fn mixed_recovery_intents_refuse_before_changing_authority() {
        let root = tempfile::tempdir().unwrap();
        drop(Journal::open(root.path()).unwrap());
        crash(root.path(), "append-committed");
        let stale_append = fs::read(root.path().join("append.json")).unwrap();
        drop(Journal::open(root.path()).unwrap());
        crash(root.path(), "privacy-prepared");
        fs::write(root.path().join("append.json"), stale_append).unwrap();
        let events = fs::read(root.path().join("events.jsonl")).unwrap();
        let manifest = fs::read(root.path().join("manifest.json")).unwrap();
        assert!(matches!(
            Journal::open(root.path()),
            Err(EventLogError::Backend(_))
        ));
        assert_eq!(fs::read(root.path().join("events.jsonl")).unwrap(), events);
        assert_eq!(
            fs::read(root.path().join("manifest.json")).unwrap(),
            manifest
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn privacy_crash_cleans_cached_bodies_and_blobs_on_open() {
        use eventlog_core::{EventStore, Expected, Snapshot, StreamId, TenantId};
        for point in ["privacy-prepared", "privacy-renamed", "privacy-committed"] {
            let root = tempfile::tempdir().unwrap();
            let owner = TenantId::new("private-owner").unwrap();
            let stream = StreamId::new(owner.clone(), "item", "one").unwrap();
            let store = crate::FileEventStore::open(root.path()).await.unwrap();
            store
                .append(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.created", 1)],
                    &eventlog_conformance::meta("first", &json!({})),
                )
                .await
                .unwrap();
            store
                .put_blob(&owner, "content", b"private-content-marker")
                .await
                .unwrap();
            let generation = store.snapshot_generation(&stream).await.unwrap().unwrap();
            store
                .save_snapshot_checked(
                    &stream,
                    &Snapshot {
                        version: 1,
                        state_schema_version: 1,
                        state: json!({"value":"private-content-marker"}),
                        recorded_at: time::OffsetDateTime::UNIX_EPOCH,
                    },
                    &generation,
                )
                .await
                .unwrap();
            drop(store);
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "journal::tests::crash_child", "--nocapture"])
                .env("EVENTLOG_FILE_CRASH_ROOT", root.path())
                .env("EVENTLOG_FILE_CRASH_AT", point)
                .env("EVENTLOG_FILE_PROVIDER_PRIVACY", "1")
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let store = crate::FileEventStore::open(root.path()).await.unwrap();
            assert_eq!(store.stream_version(&stream).await.unwrap(), None);
            assert_eq!(store.get_blob(&owner, "content").await.unwrap(), None);
            for path in [root.path().join(".cache"), root.path().join("blobs")] {
                assert_eq!(fs::read_dir(path).unwrap().count(), 0, "{point}");
            }
        }
    }
}
