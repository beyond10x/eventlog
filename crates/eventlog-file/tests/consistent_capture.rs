//! Provider cases for the file store's consistent tenant capture.
//!
//! What is provable here and nowhere else: that an inspector changes nothing, that it queues on
//! the same interprocess lock every writer takes, and that a pending durable intent belongs to an
//! authorized opener rather than to a reader that happened to arrive first.

use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::symlink;

use eventlog_core::{
    CaptureError, CaptureLimits, ConsistentTenantCapture, EventLogError, EventStore, Expected,
    NewEvent, ProjectionSpec, StreamId, TenantId,
};
use eventlog_file::{FileEventStore, FileTenantCapture};
use serde_json::json;

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 256,
        max_blobs: 256,
        max_projection_rows: 256,
        max_payload_bytes: 1 << 20,
    }
}

/// Every entry below a root, with directories marked and file bytes read.
///
/// "Non-mutating" is a claim about stored files, so the check is stored files: their names, their
/// kinds and their exact bytes. Access-time bookkeeping and the advisory lock are not durable
/// facts and are not in here.
fn inventory(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    let mut found = BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).expect("readable directory") {
            let entry = entry.expect("readable entry");
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .expect("entry below the root")
                .to_owned();
            if entry.file_type().expect("entry kind").is_dir() {
                found.insert(relative, None);
                pending.push(path);
            } else {
                found.insert(relative, Some(fs::read(&path).expect("readable file")));
            }
        }
    }
    found
}

async fn populated(root: &Path) -> TenantId {
    let tenant = TenantId::new("file-capture-owner").expect("valid tenant");
    let store = FileEventStore::open(root).await.expect("opened store");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    store
        .append(
            &StreamId::new(tenant.clone(), "item", "one").expect("valid stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("one", &json!({})),
        )
        .await
        .expect("appended");
    store
        .put_blob(&tenant, "bound", b"bound-bytes")
        .await
        .expect("bound content");
    tenant
}

#[tokio::test(flavor = "multi_thread")]
async fn file_consistent_capture_contract() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let concrete = Arc::new(
        FileEventStore::open(directory.path())
            .await
            .expect("opened store"),
    );
    let store: Arc<dyn EventStore> = concrete.clone();
    eventlog_conformance::run_consistent_capture(&store, concrete.as_ref()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_handle_inspects_a_store_nobody_opened_for_writing() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let tenant = populated(directory.path()).await;
    // No FileEventStore::open here. The inspector is the first thing to touch the store.
    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");
    let captured = handle
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("complete observation");
    assert_eq!(captured.events.len(), 1);
    assert_eq!(captured.blobs.len(), 1);
    assert_eq!(captured.blobs[0].bytes, b"bound-bytes".to_vec());
}

#[tokio::test(flavor = "multi_thread")]
async fn opening_and_capturing_change_no_stored_entry_or_byte() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let tenant = populated(directory.path()).await;
    // Unselected staging bytes and an unbound object: exactly what an ordinary opener deletes.
    let staging = format!(".write-{}", eventlog_core::new_event_id());
    fs::write(directory.path().join(&staging), b"unselected").expect("staging file");
    fs::write(directory.path().join("privacy.next"), b"unselected").expect("staging file");
    fs::create_dir_all(directory.path().join("blobs")).expect("blob directory");
    fs::write(
        directory
            .path()
            .join("blobs")
            .join(eventlog_core::new_event_id()),
        b"unbound",
    )
    .expect("unbound object");

    let before = inventory(directory.path());
    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");
    assert_eq!(inventory(directory.path()), before, "open changed a file");
    handle
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("complete observation");
    assert_eq!(
        inventory(directory.path()),
        before,
        "capture changed a file"
    );
    let missing = TenantId::new("file-capture-absent").expect("valid tenant");
    assert_eq!(
        handle
            .capture_tenant(&missing, &[], limits())
            .await
            .expect_err("no identity"),
        CaptureError::TenantIdentityMissing
    );
    assert_eq!(
        inventory(directory.path()),
        before,
        "a refusal changed a file"
    );
    assert!(
        before.contains_key(Path::new(&staging)) && before.contains_key(Path::new("privacy.next")),
        "the fixture only means something while those files are there to be deleted"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pending_intent_refuses_without_recovery_cleanup_or_initialization() {
    for intents in [
        vec!["append.json"],
        vec!["privacy.json"],
        vec!["append.json", "privacy.json"],
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let _ = populated(directory.path()).await;
        for intent in &intents {
            fs::write(directory.path().join(intent), b"{\"pending\":true}")
                .expect("pending intent");
        }
        let before = inventory(directory.path());
        // Malformed and mixed intents alike: capture neither repairs them nor diagnoses them.
        for _ in 0..2 {
            assert!(
                matches!(
                    FileTenantCapture::open(directory.path()).await,
                    Err(CaptureError::RecoveryRequired)
                ),
                "{intents:?}: a pending intent is an authorized opener's work, not a reader's"
            );
            assert_eq!(inventory(directory.path()), before, "{intents:?}");
        }
    }
}

/// A reserved pending-intent name is present when the directory entry exists, even if following
/// that entry cannot reach a target. Both strict entry points share this rule and leave the entry
/// untouched for an authorized ordinary opener.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_dangling_pending_intent_entry_refuses_strict_open_and_capture() {
    for intent in ["append.json", "privacy.json"] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let tenant = populated(directory.path()).await;
        let handle = FileTenantCapture::open(directory.path())
            .await
            .expect("strict handle opened before the intent");
        let target = directory.path().join(format!("missing-{intent}-target"));
        let entry = directory.path().join(intent);
        symlink(&target, &entry).expect("dangling pending-intent entry");

        assert!(
            matches!(
                FileTenantCapture::open(directory.path()).await,
                Err(CaptureError::RecoveryRequired)
            ),
            "{intent}: strict open read past the reserved directory entry"
        );
        assert_eq!(
            handle.capture_tenant(&tenant, &[], limits()).await,
            Err(CaptureError::RecoveryRequired),
            "{intent}: capture read past the reserved directory entry"
        );
        assert!(
            fs::symlink_metadata(&entry)
                .expect("intent entry remains")
                .file_type()
                .is_symlink(),
            "{intent}: capture changed the pending entry"
        );
        assert!(
            !target.exists(),
            "{intent}: capture created the link target"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_store_files_refuse_and_create_nothing() {
    let directory = tempfile::tempdir().expect("temporary directory");

    let absent = directory.path().join("not-a-store");
    assert!(matches!(
        FileTenantCapture::open(&absent).await,
        Err(CaptureError::Store(EventLogError::Backend(_)))
    ));
    assert!(!absent.exists(), "a missing root was created");

    let bare = directory.path().join("bare");
    fs::create_dir_all(&bare).expect("empty directory");
    assert!(matches!(
        FileTenantCapture::open(&bare).await,
        Err(CaptureError::Store(EventLogError::Backend(_)))
    ));
    assert!(
        inventory(&bare).is_empty(),
        "an empty directory became a store"
    );

    let partial = directory.path().join("partial");
    let _ = populated(&partial).await;
    fs::remove_file(partial.join("manifest.json")).expect("removed commit authority");
    let before = inventory(&partial);
    assert!(matches!(
        FileTenantCapture::open(&partial).await,
        Err(CaptureError::Store(EventLogError::Backend(_)))
    ));
    assert_eq!(inventory(&partial), before, "a missing manifest was minted");
}

/// Hold the store's interprocess lock for a bounded interval, then exit.
///
/// Invoked only by the parent case below, with its own directory.
#[test]
fn writer_lock_holder() {
    let Ok(root) = std::env::var("EVENTLOG_FILE_CAPTURE_LOCK_ROOT") else {
        return;
    };
    let millis: u64 = std::env::var("EVENTLOG_FILE_CAPTURE_LOCK_MILLIS")
        .expect("hold interval")
        .parse()
        .expect("hold interval is a number");
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(Path::new(&root).join("writer.lock"))
        .expect("existing writer lock");
    lock.lock().expect("held the writer lock");
    println!("{HANDSHAKE}");
    std::io::stdout().flush().expect("flushed");
    std::thread::sleep(Duration::from_millis(millis));
}

/// The rendezvous token the holder prints once the interprocess lock is actually held.
///
/// It is matched as a token inside a line rather than as a whole line. The child is a libtest
/// binary, and libtest owns that stream too: whether its own `test <name> ... ` prefix lands on
/// the same line as the test's output is a formatting detail of the runner, not a fact about
/// locking. Exact line equality made the case depend on that detail, and it is what the
/// production runner's `--show-output --test-threads=1 --format=pretty` observed differently.
/// Nothing about the lock assertion below moves; only how the two processes say hello.
const HANDSHAKE: &str = "writer-lock-held";

#[tokio::test(flavor = "multi_thread")]
async fn capture_queues_behind_another_process_holding_the_writer_lock() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let tenant = populated(directory.path()).await;
    let hold = 800_u64;
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", "writer_lock_holder", "--nocapture"])
        .env("EVENTLOG_FILE_CAPTURE_LOCK_ROOT", directory.path())
        .env("EVENTLOG_FILE_CAPTURE_LOCK_MILLIS", hold.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawned lock holder");
    let mut output = BufReader::new(child.stdout.take().expect("piped stdout"));
    let mut line = String::new();
    let mut seen = String::new();
    loop {
        line.clear();
        let read = output.read_line(&mut line).expect("child output");
        if read == 0 {
            // End of stream without the rendezvous. Say what the holder actually produced and
            // how it ended, rather than asserting a cause nothing here observed.
            let mut failed = String::new();
            child
                .stderr
                .take()
                .expect("piped stderr")
                .read_to_string(&mut failed)
                .expect("child stderr");
            let status = child.wait().expect("lock holder finished");
            panic!(
                "the lock holder never signalled {HANDSHAKE}: status={status:?}, \
                 stdout={seen:?}, stderr={failed:?}"
            );
        }
        seen.push_str(&line);
        if line.contains(HANDSHAKE) {
            break;
        }
    }
    let started = Instant::now();
    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened once the holder released");
    let waited = started.elapsed();
    let status = child.wait().expect("lock holder finished");
    assert!(status.success(), "lock holder failed");
    eprintln!(
        "capture waited {}ms for a hold of {hold}ms",
        waited.as_millis()
    );
    assert!(
        waited >= Duration::from_millis(300),
        "capture did not queue on the interprocess lock; it waited {waited:?}"
    );
    assert_eq!(
        handle
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("complete observation")
            .events
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn another_process_rewriting_history_invalidates_an_observing_handle() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let survivor = populated(directory.path()).await;
    let victim = TenantId::new("file-capture-victim").expect("valid tenant");
    let store = FileEventStore::open(directory.path())
        .await
        .expect("opened store");
    store
        .append(
            &StreamId::new(victim.clone(), "item", "one").expect("valid stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 2)],
            &eventlog_conformance::meta("victim", &json!({})),
        )
        .await
        .expect("appended");

    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");
    handle
        .capture_tenant(&survivor, &[], limits())
        .await
        .expect("complete observation");
    // A privacy rewrite replaces the epoch, so the history this handle observed is gone.
    store.forget_tenant(&victim).await.expect("erased tenant");
    for _ in 0..2 {
        assert!(
            matches!(
                handle.capture_tenant(&survivor, &[], limits()).await,
                Err(CaptureError::Store(EventLogError::Backend(_)))
            ),
            "no refusal lets a handle reset its observation and retry silently"
        );
    }
    let reopened = FileTenantCapture::open(directory.path())
        .await
        .expect("explicitly observing the new epoch");
    assert_eq!(
        reopened
            .capture_tenant(&survivor, &[], limits())
            .await
            .expect("complete observation")
            .events
            .len(),
        1
    );
}

/// Only the two refusals the design names are answered ahead of the per-handle divergence guard.
///
/// "Missing-identity or redacted-history refusals may be returned from validated state before that
/// guard; a successful value must pass it." A crossed cap, an unavailable projection, corruption
/// and required recovery are none of those two, and each of them is a statement about a history
/// this handle no longer holds: it says what the caller's request would have met in an epoch that
/// has since been replaced. The guard's own rule is unchanged — no refusal lets the handle reset
/// its observation — so every answer below is the same on the second round as on the first.
#[tokio::test(flavor = "multi_thread")]
async fn only_missing_identity_and_redacted_history_answer_ahead_of_the_divergence_guard() {
    const UNDECLARED: ProjectionSpec = ProjectionSpec {
        name: "nobody_declared_this",
        indexed: &[],
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = FileEventStore::open(directory.path())
        .await
        .expect("opened store");
    let erased = TenantId::new("file-guard-erased").expect("valid tenant");
    let intact = TenantId::new("file-guard-intact").expect("valid tenant");
    let stranger = TenantId::new("file-guard-stranger").expect("valid tenant");
    for tenant in [&erased, &intact] {
        store
            .stream_identity(tenant)
            .await
            .expect("provisioned identity");
        store
            .append(
                &StreamId::new(tenant.clone(), "item", "one").expect("valid stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta(tenant.as_str(), &json!({})),
            )
            .await
            .expect("appended");
    }

    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");
    handle
        .capture_tenant(&intact, &[], limits())
        .await
        .expect("complete observation");
    // A redaction is a privacy rewrite: it replaces the epoch, so the history this handle observed
    // is gone and every answer below would be given against one it never read.
    store
        .redact(
            &StreamId::new(erased.clone(), "item", "one").expect("valid stream"),
            1,
            "erased",
        )
        .await
        .expect("redacted event");

    let zero = CaptureLimits {
        max_events: 0,
        max_blobs: 0,
        max_projection_rows: 0,
        max_payload_bytes: 0,
    };
    let none: &[ProjectionSpec] = &[];
    let undeclared: &[ProjectionSpec] = &[UNDECLARED];
    for round in 0..2 {
        assert_eq!(
            handle.capture_tenant(&erased, none, limits()).await,
            Err(CaptureError::RedactedHistory),
            "round {round}: redacted history is one of the two the design names"
        );
        assert_eq!(
            handle.capture_tenant(&stranger, none, limits()).await,
            Err(CaptureError::TenantIdentityMissing),
            "round {round}: a missing identity is the other"
        );
        for (request, caps, what) in [
            (none, zero, "a crossed cap"),
            (undeclared, limits(), "an unavailable projection"),
            (none, limits(), "a complete value"),
        ] {
            assert!(
                matches!(
                    handle.capture_tenant(&intact, request, caps).await,
                    Err(CaptureError::Store(EventLogError::Backend(_)))
                ),
                "round {round}: {what} is not an answer this handle may give from a history it \
                 never observed"
            );
        }
    }

    // Reopening is still the only way to observe the new epoch, and it observes all of it.
    let reopened = FileTenantCapture::open(directory.path())
        .await
        .expect("explicitly observing the new epoch");
    assert_eq!(
        reopened
            .capture_tenant(&intact, none, limits())
            .await
            .expect("complete observation")
            .events
            .len(),
        1
    );
    assert_eq!(
        reopened.capture_tenant(&erased, none, limits()).await,
        Err(CaptureError::RedactedHistory),
        "and the refusal a fresh handle gives is the one the stale handle gave"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_key_an_existing_writer_admits_round_trips_including_an_embedded_nul() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let concrete = Arc::new(
        FileEventStore::open(directory.path())
            .await
            .expect("opened store"),
    );
    let store: Arc<dyn EventStore> = concrete.clone();
    let tenant = TenantId::new("file-capture-keys").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    let runner = eventlog_core::CatchUpRunner::new(
        Arc::clone(&store),
        Arc::new(eventlog_conformance::CaptureLedger),
    )
    .await
    .expect("declared projections");
    let keys = ["", "\u{0}", "a\u{0}b", " \t ", "é\u{1f600}"];
    for (index, key) in keys.iter().enumerate() {
        let index = i64::try_from(index).expect("small index");
        store
            .append(
                &StreamId::new(tenant.clone(), "item", "one").expect("valid stream"),
                Expected::Any,
                &[
                    NewEvent::new("item.received", 1, json!({ "key": key, "value": index }))
                        .expect("valid event"),
                ],
                &eventlog_conformance::meta(&format!("key-{index}"), &json!({})),
            )
            .await
            .expect("appended");
    }
    eventlog_conformance::drain_at_least(&runner, &tenant, keys.len() as u64).await;
    let captured = concrete
        .capture_tenant(&tenant, &[eventlog_conformance::CAPTURE_SIDECAR], limits())
        .await
        .expect("complete observation");
    let mut expected: Vec<&str> = keys.to_vec();
    expected.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    assert_eq!(
        captured.projections[0]
            .rows
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        expected,
        "exact decoded bytes, in bytewise order, with no key grammar invented"
    );
}
