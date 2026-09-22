//! Independent verification (pass 2) of `story:file-capture-reuses-the-verified-view`.
//!
//! These cases are the reviewer's, not the author's. Each one asks a question the shipped suite
//! does not: whether the resumed reader reaches a refusal `open_strict` makes, whether it takes
//! the interprocess lock at all, and whether a view survives a path that moves the committed
//! bytes underneath it.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::symlink;

use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture, EventLogError,
    EventStore, Expected, StreamId, TenantId,
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

/// A store root that is no longer a physical directory is `open_strict`'s refusal, and a handle
/// that already has a view must reach it too.
///
/// `open_strict` asks `symlink_metadata(root).is_dir()` and refuses a symlinked root by name:
/// "file store root is not a physical directory". `resume_strict` asks nothing about the root; its
/// first question is `regular(root/writer.lock)`, and path resolution follows every component but
/// the last, so a root that has become a symlink answers it. The state is one an operator reaches
/// by moving a store and leaving a link where it was, while a reader holds a handle.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_store_root_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does() {
    let outer = tempfile::tempdir().expect("temporary directory");
    let root = outer.path().join("store");
    fs::create_dir(&root).expect("store directory");
    let tenant = populated(&root).await;

    let handle = FileTenantCapture::open(&root)
        .await
        .expect("opened read-only");
    handle
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("complete observation");

    // The store is moved and a link is left where it was. Every byte of it is unchanged.
    let moved = outer.path().join("moved");
    fs::rename(&root, &moved).expect("moved store");
    symlink(&moved, &root).expect("a link where the root was");

    let strict_refusal = FileTenantCapture::open(&root).await.err();
    assert!(
        matches!(
            &strict_refusal,
            Some(CaptureError::Store(EventLogError::Backend(reason)))
                if reason == "file store root is not a physical directory"
        ),
        "the fixture only means something while the strict opener refuses this root: \
         {strict_refusal:?}"
    );

    let resumed = handle.capture_tenant(&tenant, &[], limits()).await;
    assert_eq!(
        format!("{:?}", resumed.err()),
        format!("{strict_refusal:?}"),
        "a resumed capture observed a store root the strict opener refuses as not a physical \
         directory"
    );
}

/// A resumed capture takes the writers' own interprocess lock, as the strict reader does.
///
/// Nothing in the repository asks this of the resumed path. `the_strict_reader_holds_the_writer_lock_for_its_whole_life`
/// holds `open_strict` to it, and `capture_queues_behind_another_process_holding_the_writer_lock`
/// times `FileTenantCapture::open`, which is `open_strict` again — both use a handle that has no
/// view, so neither reaches `resume_strict`. `flock` conflicts between open file descriptions, so
/// a second description in this same process settles it: with the lock held elsewhere, a capture
/// that takes it cannot finish, and one that does not takes no notice.
#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_capture_takes_the_writers_lock() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let tenant = populated(directory.path()).await;
    let handle = Arc::new(
        FileTenantCapture::open(directory.path())
            .await
            .expect("opened read-only"),
    );
    // The first capture is the strict one; it is what leaves the view the second one resumes.
    handle
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("complete observation");

    let probe = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join("writer.lock"))
        .expect("existing writer lock");
    probe
        .try_lock()
        .expect("the control: nothing holds the lock yet");

    let resuming = Arc::clone(&handle);
    let owner = tenant.clone();
    let task = tokio::spawn(async move { resuming.capture_tenant(&owner, &[], limits()).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let queued = !task.is_finished();
    probe.unlock().expect("released the control");

    let observed = task
        .await
        .expect("the capture task joined")
        .expect("complete observation once the lock is free");
    assert_eq!(observed.events.len(), 1);
    assert!(
        queued,
        "a resumed capture read the committed history without queueing on the writers' lock"
    );
}

/// Committed history that is no longer a regular file is `open_strict`'s refusal, and the
/// resumed walk's `regular(&events_path)` is the only thing that reaches it on this path.
///
/// Nothing in the repository holds that line to anything: deleting it leaves all 90 cases green
/// (mutation A7), and what it lets through is a `events.jsonl` whose bytes are correct and whose
/// directory entry is a link — `fs::metadata` follows it and reports the target's length, so
/// every remaining question in the walk is answered by the target. The strict opener asks
/// `symlink_metadata(...).is_file()` and refuses. This case is the difference.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn committed_history_that_became_a_symlink_refuses_a_capture_as_the_strict_opener_does() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let elsewhere = tempfile::tempdir().expect("temporary directory");
    let tenant = populated(directory.path()).await;
    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");
    handle
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("complete observation");

    // The exact committed bytes, reached through a link instead of an entry.
    let events = directory.path().join("events.jsonl");
    let displaced = elsewhere.path().join("events.jsonl");
    fs::rename(&events, &displaced).expect("displaced history");
    symlink(&displaced, &events).expect("history entry that is not a regular file");

    let strict_refusal = FileTenantCapture::open(directory.path()).await.err();
    assert_eq!(
        strict_refusal,
        Some(CaptureError::Corrupt {
            material: CaptureMaterial::Journal
        }),
        "the fixture only means something while the strict opener refuses this entry"
    );
    assert_eq!(
        handle.capture_tenant(&tenant, &[], limits()).await.err(),
        strict_refusal,
        "a resumed capture read committed history through an entry the strict opener refuses"
    );
}

/// The child half of `a_foreign_process_append_is_observed_by_the_next_capture`.
///
/// Selected by name from the parent through this same test binary, exactly as
/// `writer_lock_holder` is in `consistent_capture.rs`. It is inert without its environment.
#[tokio::test(flavor = "multi_thread")]
async fn foreign_writer_child() {
    let Ok(root) = std::env::var("EVENTLOG_U6R2_WRITER_ROOT") else {
        return;
    };
    let name = std::env::var("EVENTLOG_U6R2_WRITER_TENANT").expect("the tenant to append for");
    let tenant = TenantId::new(name.as_str()).expect("valid tenant");
    let store = FileEventStore::open_existing(Path::new(&root))
        .await
        .expect("opened the store from another process");
    store
        .append(
            &StreamId::new(tenant.clone(), "item", "from-another-process").expect("valid stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 7)],
            &eventlog_conformance::meta("foreign", &json!({})),
        )
        .await
        .expect("appended");
    store
        .put_blob(&tenant, "foreign", b"foreign-bytes")
        .await
        .expect("bound content");
}

/// A frame another *process* committed between two captures is in the second one.
///
/// The unit's own case uses a second `FileEventStore` in this process, which shares nothing with
/// the reading handle but does share an address space and an allocator. A real second process is
/// the shape the brief names, and it is the one the CLI actually has.
#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_process_append_is_observed_by_the_next_capture() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let tenant = populated(directory.path()).await;
    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");
    let first = handle
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("complete observation");
    assert_eq!((first.events.len(), first.blobs.len()), (1, 1));

    // Captured, not inherited: a nested runner writing its own `test … ok` and `test result:`
    // lines into this one's stdout is a second suite to anything that reads test output, and the
    // production proof refuses the ambiguity rather than guessing which run it is looking at.
    let child = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", "foreign_writer_child"])
        .env("EVENTLOG_U6R2_WRITER_ROOT", directory.path())
        .env("EVENTLOG_U6R2_WRITER_TENANT", tenant.as_str())
        .output()
        .expect("spawned the foreign writer");
    assert!(
        child.status.success(),
        "the foreign writer failed: {:?}\n{}\n{}",
        child.status,
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );

    let second = handle
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("complete observation");
    assert_eq!(
        second.events.len(),
        2,
        "a capture served a history another process has already left behind"
    );
    assert!(
        second
            .blobs
            .iter()
            .any(|blob| blob.bytes == b"foreign-bytes".to_vec()),
        "content another process bound after the view was built is part of the next observation"
    );
}

/// A capture that fell back to the strict opener leaves nothing that makes the next one wrong.
///
/// Three questions in one fixture, because they are one question: after a refusal that was
/// reached through `open_strict`, does the handle still hold a view it should not, does it write
/// anything on the way, and does it observe the repaired history correctly afterwards?
#[tokio::test(flavor = "multi_thread")]
async fn a_capture_that_fell_back_to_the_strict_opener_leaves_the_next_one_correct() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let tenant = populated(directory.path()).await;
    // Unselected staging bytes: what an ordinary opener deletes and a reader must not.
    let staging = format!(".write-{}", eventlog_core::new_event_id());
    fs::write(directory.path().join(&staging), b"unselected").expect("staging file");
    fs::write(directory.path().join("privacy.next"), b"unselected").expect("staging file");

    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");
    let first = handle
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("complete observation");

    let events = directory.path().join("events.jsonl");
    let healthy = fs::read(&events).expect("committed history");
    let mut damaged = healthy.clone();
    let at = damaged
        .windows(b"item.received".len())
        .position(|window| window == b"item.received")
        .expect("a committed frame to damage");
    damaged[at] = b'j';
    fs::write(&events, &damaged).expect("damaged history");

    let before = inventory(directory.path());
    assert_eq!(
        handle.capture_tenant(&tenant, &[], limits()).await,
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Journal
        }),
        "a handle does not serve committed bytes it can no longer validate"
    );
    assert_eq!(
        inventory(directory.path()),
        before,
        "a capture that fell back to the strict opener changed a file"
    );
    assert!(
        before.contains_key(Path::new(&staging)) && before.contains_key(Path::new("privacy.next")),
        "the fixture only means something while those files are there to be deleted"
    );

    fs::write(&events, &healthy).expect("repaired history");
    let repaired = handle
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("the handle observes the history it can validate again");
    assert_eq!(
        repaired, first,
        "a handle that refused once serves a different observation of the same bytes afterwards"
    );
    assert_eq!(
        handle
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("complete observation"),
        first,
        "the view rebuilt after a fallback serves a different observation again"
    );
}

/// One writer handle's own transactions keep its reader view correct.
///
/// `Runtime.captured` is the slot this unit added beside `Runtime.verified`, and every case the
/// unit wrote reaches it either from a `FileTenantCapture` or from a `FileEventStore` with no
/// write between the captures. The interleaving a mixed workload actually has — capture, write on
/// the same handle, capture — is reached by nothing, and it is the one where the reader's view
/// and the writer's observed head are moved by two different pieces of code.
#[tokio::test(flavor = "multi_thread")]
async fn a_writer_handle_interleaving_transactions_and_captures_observes_each_one() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let tenant = TenantId::new("file-capture-mixed").expect("valid tenant");
    let victim = TenantId::new("file-capture-mixed-victim").expect("valid tenant");
    let store = FileEventStore::open(directory.path())
        .await
        .expect("opened store");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    store
        .append(
            &StreamId::new(victim.clone(), "item", "only").expect("valid stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 9)],
            &eventlog_conformance::meta("victim", &json!({})),
        )
        .await
        .expect("appended");

    for round in 1..=4_i64 {
        store
            .append(
                &StreamId::new(tenant.clone(), "item", format!("s{round}")).expect("valid stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", round)],
                &eventlog_conformance::meta(&format!("m{round}"), &json!({})),
            )
            .await
            .expect("appended");
        store
            .put_blob(
                &tenant,
                &format!("d{round}"),
                format!("bytes-{round}").as_bytes(),
            )
            .await
            .expect("bound content");
        let captured = store
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("complete observation");
        assert_eq!(
            (captured.events.len(), captured.blobs.len()),
            (
                usize::try_from(round).expect("small"),
                usize::try_from(round).expect("small")
            ),
            "round {round}: a capture on a writer handle served a view its own transaction left \
             behind"
        );
        assert!(
            captured
                .blobs
                .iter()
                .any(|blob| blob.bytes == format!("bytes-{round}").into_bytes()),
            "round {round}: the content this handle just bound is not in its own observation"
        );
    }

    // A privacy rewrite on the same handle mints a new epoch over replaced bytes: the reader's
    // view is of an epoch that is gone, and the handle that authored the rewrite is the one that
    // must observe the epoch it wrote.
    store.forget_tenant(&victim).await.expect("erased tenant");
    let after = store
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("the handle that wrote the new epoch observes it");
    assert_eq!(
        (after.events.len(), after.blobs.len()),
        (4, 4),
        "a capture after this handle's own privacy rewrite was answered from the old epoch"
    );
}
