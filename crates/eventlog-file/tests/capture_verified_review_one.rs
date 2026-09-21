//! Independent verification, pass 1, for `story:file-capture-reuses-the-verified-view`.
//!
//! The unit adds `journal::resume_strict`, a read-only sibling of the writer's `Journal::resume`,
//! and states one contract for it in its own doc comment: *every refusal belongs to
//! `open_strict`*. These cases check that claim against the code rather than against the claim.
//!
//! The question each case asks is the same one: with a handle that **already holds a view** — the
//! only state in which the resumed path is taken at all — does a damaged store reach the exact
//! refusal `open_strict` makes about that same damaged store?

#![cfg(unix)]

use std::{fs, os::unix::fs::symlink, path::Path};

use eventlog_core::{
    CaptureError, CaptureLimits, ConsistentTenantCapture, EventStore, Expected, ProjectionSpec,
    StreamId, TenantId,
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

const NONE: &[ProjectionSpec] = &[];

/// A store with one provisioned tenant, one committed event and one bound object.
async fn populated(root: &Path) -> TenantId {
    fs::create_dir_all(root).expect("store root");
    let tenant = TenantId::new("review-one-owner").expect("valid tenant");
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

/// A handle that has captured once, so the next capture takes the resumed path.
async fn resumed_handle(root: &Path, tenant: &TenantId) -> FileTenantCapture {
    let handle = FileTenantCapture::open(root)
        .await
        .expect("opened read-only");
    handle
        .capture_tenant(tenant, NONE, limits())
        .await
        .expect("the first capture builds the view the next one resumes onto");
    handle
}

fn root_becomes_a_symlink_to_the_same_directory(root: &Path) {
    let moved = root
        .parent()
        .expect("a store root below the temporary directory")
        .join("moved-store");
    fs::rename(root, &moved).expect("moved store root");
    symlink("moved-store", root).expect("root entry that is not a physical directory");
}

fn displace_behind_a_symlink(root: &Path, name: &str, moved: &str) {
    fs::rename(root.join(name), root.join(moved)).expect("displaced entry");
    symlink(moved, root.join(name)).expect("entry that is not a regular file");
}

/// Every shape `open_strict` refuses is refused identically through the resumed path.
///
/// Parity is asserted against `open_strict` itself rather than against a hand-written expectation:
/// for each damaged store the case asks a *fresh* `FileTenantCapture::open` — which is nothing but
/// `journal::open_strict` — what it says, and requires the resumed capture on the handle that
/// already has a view to say exactly the same thing. A shape where the strict opener refuses and
/// the resumed path does not is the refusal-parity break this unit's contract forbids.
#[tokio::test(flavor = "multi_thread")]
async fn every_shape_the_strict_opener_refuses_is_refused_through_the_resumed_path() {
    type Damage = fn(&Path);
    let shapes: [(&str, Damage); 10] = [
        (
            "the store root is no longer a physical directory",
            root_becomes_a_symlink_to_the_same_directory,
        ),
        ("the writer lock is gone", |root| {
            fs::remove_file(root.join("writer.lock")).expect("removed lock");
        }),
        ("the writer lock is not a regular file", |root| {
            displace_behind_a_symlink(root, "writer.lock", "displaced.lock");
        }),
        ("the manifest is gone", |root| {
            fs::remove_file(root.join("manifest.json")).expect("removed manifest");
        }),
        ("the manifest is not a regular file", |root| {
            displace_behind_a_symlink(root, "manifest.json", "displaced-manifest.json");
        }),
        (
            "the manifest commits a format this build does not",
            |root| {
                let manifest = fs::read_to_string(root.join("manifest.json")).expect("manifest");
                let other = manifest.replace("eventlog-file/1", "eventlog-file/9");
                assert_ne!(other, manifest, "the fixture rewrote the format field");
                fs::write(root.join("manifest.json"), other).expect("foreign format");
            },
        ),
        (
            "the committed history is shorter than the manifest",
            |root| {
                let events = root.join("events.jsonl");
                let bytes = fs::read(&events).expect("committed history");
                fs::write(&events, &bytes[..bytes.len() / 2]).expect("truncated history");
            },
        ),
        (
            "the committed history carries an unexplained tail",
            |root| {
                let events = root.join("events.jsonl");
                let mut bytes = fs::read(&events).expect("committed history");
                bytes.extend_from_slice(b"{\"unexplained\":true}\n");
                fs::write(&events, bytes).expect("history with a tail");
            },
        ),
        ("the committed history is not a regular file", |root| {
            displace_behind_a_symlink(root, "events.jsonl", "displaced-events.jsonl");
        }),
        ("a committed frame is damaged in place", |root| {
            let events = root.join("events.jsonl");
            let mut bytes = fs::read(&events).expect("committed history");
            let length = bytes.len();
            let at = bytes
                .windows(b"item.received".len())
                .position(|window| window == b"item.received")
                .expect("a committed frame to damage");
            bytes[at] = b'j';
            assert_eq!(bytes.len(), length, "the damage keeps the committed length");
            fs::write(&events, bytes).expect("damaged history");
        }),
    ];

    let mut broken = Vec::new();
    for (shape, damage) in shapes {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("store");
        let tenant = populated(&root).await;
        let handle = resumed_handle(&root, &tenant).await;

        damage(&root);

        // What `open_strict` says about this damaged store, asked of it directly.
        let strict = FileTenantCapture::open(&root).await.err();
        // What the resumed path says about the same damaged store, from a handle with a view.
        let resumed = handle.capture_tenant(&tenant, NONE, limits()).await.err();
        if strict != resumed {
            broken.push(format!(
                "{shape}\n     open_strict: {strict:?}\n     resumed:     {resumed:?}"
            ));
        }
    }

    assert!(
        broken.is_empty(),
        "a resumed capture did not reach the refusal the strict opener makes:\n  - {}",
        broken.join("\n  - ")
    );
}

/// A store root that is no longer a physical directory refuses a capture, view or no view.
///
/// `journal::open_strict` refuses it (`file store root is not a physical directory`),
/// `Journal::open_with_creation` refuses it and the writer's `Journal::resume` refuses it — three
/// enforcements of one invariant. `journal::resume_strict` is the fourth entry point onto the same
/// root and is the only one that does not ask.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_root_that_is_no_longer_a_physical_directory_refuses_a_resumed_capture() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let tenant = populated(&root).await;
    let handle = resumed_handle(&root, &tenant).await;

    root_becomes_a_symlink_to_the_same_directory(&root);

    // The control: the strict opener refuses this root, so the invariant is real and unchanged.
    let strict = FileTenantCapture::open(&root)
        .await
        .err()
        .expect("the strict opener refuses a root that is not a physical directory");
    assert!(
        matches!(&strict, CaptureError::Store(error) if
            error.to_string().contains("not a physical directory")),
        "the control is the root check and not some other refusal: {strict:?}"
    );

    assert_eq!(
        handle.capture_tenant(&tenant, NONE, limits()).await.err(),
        Some(strict),
        "a handle with a view read a store root the strict opener refuses to read"
    );
}

/// A moved privacy epoch and a foreign store identity both refuse the next capture.
///
/// Neither is a shape `open_strict` refuses — it has no observed head to compare against and
/// reads both stores happily — so the refusal has to come from the handle. The resumed reader
/// answers the divergence guard without asking it, which makes "and it still refuses these" a
/// claim that needs a case rather than a reading.
#[tokio::test(flavor = "multi_thread")]
async fn a_moved_epoch_or_a_foreign_store_identity_refuses_a_resumed_capture() {
    let diverged = CaptureError::Store(eventlog_core::EventLogError::Backend(
        "file history diverged from this handle's observed history".to_owned(),
    ));

    // A privacy rewrite of another tenant moves the epoch under an observing handle.
    {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("store");
        let observed = populated(&root).await;
        let other = TenantId::new("review-one-other").expect("valid tenant");
        {
            let writer = FileEventStore::open(&root).await.expect("opened store");
            writer
                .stream_identity(&other)
                .await
                .expect("provisioned identity");
            writer
                .append(
                    &StreamId::new(other.clone(), "item", "one").expect("valid stream"),
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", 3)],
                    &eventlog_conformance::meta("three", &json!({})),
                )
                .await
                .expect("appended");
        }
        let handle = resumed_handle(&root, &observed).await;
        {
            let writer = FileEventStore::open(&root).await.expect("opened store");
            writer
                .forget_tenant(&other)
                .await
                .expect("erased the other tenant");
        }
        assert_eq!(
            handle.capture_tenant(&observed, NONE, limits()).await.err(),
            Some(diverged.clone()),
            "a handle reused a view across a privacy epoch the file has moved past"
        );
    }

    // The committed files of an entirely different store, dropped in place.
    {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("store");
        let foreign = directory.path().join("foreign");
        let tenant = populated(&root).await;
        let same = populated(&foreign).await;
        assert_eq!(tenant, same, "both stores provision the same tenant");
        let handle = resumed_handle(&root, &tenant).await;

        for name in ["events.jsonl", "manifest.json"] {
            fs::copy(foreign.join(name), root.join(name)).expect("foreign committed file");
        }
        for entry in fs::read_dir(foreign.join("blobs")).expect("foreign blobs") {
            let path = entry.expect("readable entry").path();
            let name = path.file_name().expect("object name").to_owned();
            fs::copy(&path, root.join("blobs").join(name)).expect("foreign object");
        }
        assert!(
            FileTenantCapture::open(&root).await.is_ok(),
            "the fixture is a valid store, so only the handle can refuse it"
        );
        assert_eq!(
            handle.capture_tenant(&tenant, NONE, limits()).await.err(),
            Some(diverged),
            "a handle reused a view across a different store's committed history"
        );
    }
}

/// A foreign writer's bytes, landed by a plain file write, are observed by the next capture.
///
/// No Eventlog writer runs in this process between the two captures: the committed history and
/// the manifest are replaced with the exact bytes a writer committed, so the only thing that can
/// make the second capture see the second event is the resumed reader re-reading the file.
#[tokio::test(flavor = "multi_thread")]
async fn a_direct_file_write_between_two_captures_is_observed_by_the_second() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let tenant = populated(&root).await;
    let events = root.join("events.jsonl");
    let manifest = root.join("manifest.json");

    let head_one = (
        fs::read(&events).expect("committed history"),
        fs::read(&manifest).expect("manifest"),
    );
    {
        let writer = FileEventStore::open(&root).await.expect("opened store");
        writer
            .append(
                &StreamId::new(tenant.clone(), "item", "two").expect("valid stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 2)],
                &eventlog_conformance::meta("two", &json!({})),
            )
            .await
            .expect("appended");
        writer
            .put_blob(&tenant, "later", b"later-bytes")
            .await
            .expect("bound content");
    }
    let head_two = (
        fs::read(&events).expect("committed history"),
        fs::read(&manifest).expect("manifest"),
    );
    assert_ne!(head_one.0, head_two.0, "the fixture committed something");

    // Roll the store back to the first head with plain writes, then build a view onto it.
    fs::write(&events, &head_one.0).expect("history at the first head");
    fs::write(&manifest, &head_one.1).expect("manifest at the first head");
    let handle = FileTenantCapture::open(&root)
        .await
        .expect("opened read-only");
    let first = handle
        .capture_tenant(&tenant, NONE, limits())
        .await
        .expect("complete observation");
    assert_eq!(first.events.len(), 1, "the view is built at the first head");

    // The foreign write: bytes only, no writer in this process.
    fs::write(&events, &head_two.0).expect("history at the second head");
    fs::write(&manifest, &head_two.1).expect("manifest at the second head");

    let second = handle
        .capture_tenant(&tenant, NONE, limits())
        .await
        .expect("complete observation");
    assert_eq!(
        second.events.len(),
        2,
        "a capture served a history the file has already left behind"
    );
    assert!(
        second
            .blobs
            .iter()
            .any(|blob| blob.bytes == b"later-bytes".to_vec()),
        "content bound behind the view is part of the next observation"
    );
}

/// Every capture on the resumed path leaves unselected staging names exactly where they were.
///
/// `opening_and_capturing_change_no_stored_entry_or_byte` covers one resumed capture and that one
/// refuses. This walks the paths it does not: a resumed capture that **succeeds** on an unchanged
/// head, a resumed capture that succeeds after folding a tail, and a capture that fell back to the
/// strict opener because a refusal retired the view. A sweep on any of them would be a reader
/// deleting the thing it came to observe.
#[tokio::test(flavor = "multi_thread")]
async fn no_capture_on_any_path_removes_an_unselected_staging_name() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let tenant = populated(&root).await;
    let events = root.join("events.jsonl");
    let manifest = root.join("manifest.json");

    // The tail is committed by an ordinary writer first and landed later with a plain file write.
    // An ordinary writer sweeps these names, correctly and by design — running one beside them
    // would be measuring the writer, and it is the reader that is under review here.
    let head_one = (
        fs::read(&events).expect("committed history"),
        fs::read(&manifest).expect("manifest"),
    );
    {
        let writer = FileEventStore::open(&root).await.expect("opened store");
        writer
            .append(
                &StreamId::new(tenant.clone(), "item", "two").expect("valid stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 2)],
                &eventlog_conformance::meta("two", &json!({})),
            )
            .await
            .expect("appended");
    }
    let head_two = (
        fs::read(&events).expect("committed history"),
        fs::read(&manifest).expect("manifest"),
    );
    fs::write(&events, &head_one.0).expect("history at the first head");
    fs::write(&manifest, &head_one.1).expect("manifest at the first head");

    let staging = root.join(format!(".write-{}", eventlog_core::new_event_id()));
    let next = root.join("privacy.next");
    fs::write(&staging, b"unselected").expect("staging file");
    fs::write(&next, b"unselected").expect("staging file");

    let present = |at: &str| {
        assert_eq!(
            fs::read(&staging).ok(),
            Some(b"unselected".to_vec()),
            "{at}: the unselected staging file was removed or rewritten"
        );
        assert_eq!(
            fs::read(&next).ok(),
            Some(b"unselected".to_vec()),
            "{at}: privacy.next was removed or rewritten"
        );
    };

    let handle = FileTenantCapture::open(&root)
        .await
        .expect("opened read-only");
    present("after a strict open");
    handle
        .capture_tenant(&tenant, NONE, limits())
        .await
        .expect("complete observation");
    present("after the first capture, which had no view");
    handle
        .capture_tenant(&tenant, NONE, limits())
        .await
        .expect("complete observation");
    present("after a resumed capture that succeeded on an unchanged head");

    fs::write(&events, &head_two.0).expect("history at the second head");
    fs::write(&manifest, &head_two.1).expect("manifest at the second head");
    present("after a foreign writer's bytes landed beside them");
    handle
        .capture_tenant(&tenant, NONE, limits())
        .await
        .expect("complete observation");
    present("after a resumed capture that folded a tail");

    let missing = TenantId::new("review-one-absent").expect("valid tenant");
    assert_eq!(
        handle.capture_tenant(&missing, NONE, limits()).await.err(),
        Some(CaptureError::TenantIdentityMissing)
    );
    present("after a refusal retired the view");
    handle
        .capture_tenant(&tenant, NONE, limits())
        .await
        .expect("complete observation");
    present("after the capture that fell back to the strict opener");
}
