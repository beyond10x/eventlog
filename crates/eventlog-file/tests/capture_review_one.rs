//! Source-examination cases for the file store's consistent tenant capture.
//!
//! Added by the first source examination of `feat: capture complete tenant state consistently
//! across providers`. Each case drives the implementation from
//! `docs/design/consistent-tenant-capture.md`. Nothing here edits an implementation file or an
//! existing case.

use std::{fs, path::Path, sync::Arc};

use eventlog_conformance::{CAPTURE_LEDGER, CaptureLedger};
use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, CaptureResource, CatchUpRunner,
    ConsistentTenantCapture, EventStore, Expected, ProjectionCaptureRefusal, StreamId, TenantId,
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

/// One provisioned tenant, one event and one bound blob, written through the ordinary opener.
async fn populated(root: &Path, label: &str) -> TenantId {
    let tenant = TenantId::new(label).expect("valid tenant");
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
            &eventlog_conformance::meta("review-one", &json!({})),
        )
        .await
        .expect("appended");
    store
        .put_blob(&tenant, "bound", b"bound-bytes")
        .await
        .expect("bound content");
    tenant
}

/// The single blob object an ordinary opener left behind.
#[cfg(unix)]
fn only_object(root: &Path) -> std::path::PathBuf {
    let mut objects: Vec<std::path::PathBuf> = fs::read_dir(root.join("blobs"))
        .expect("blob directory")
        .map(|entry| entry.expect("readable entry").path())
        .collect();
    assert_eq!(
        objects.len(),
        1,
        "the fixture expects exactly one bound object"
    );
    objects.pop().expect("one object")
}

/// `ProjectionCaptureRefusal::Dirty` has no reachable producer.
///
/// The design says File "also refuses an existing dirty-view marker", and the only writer of that
/// marker is `redact`, which sets `redacted_at` on the same event in the same transaction. The
/// redacted-history condition is checked before any projection is admitted, so the tenant whose
/// marker exists is exactly the tenant whose capture is already refused. A rebuild clears the
/// marker and the refusal stands, so there is no window either.
#[tokio::test(flavor = "multi_thread")]
async fn the_dirty_projection_refusal_has_no_reachable_producer() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let concrete = Arc::new(
        FileEventStore::open(directory.path())
            .await
            .expect("opened store"),
    );
    let port: Arc<dyn EventStore> = concrete.clone();
    let tenant = TenantId::new("file-review-one-dirty").expect("valid tenant");
    port.stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    let runner = CatchUpRunner::new(Arc::clone(&port), Arc::new(CaptureLedger))
        .await
        .expect("declared capture projections");
    let stream = StreamId::new(tenant.clone(), "item", "one").expect("valid stream");
    port.append(
        &stream,
        Expected::NoStream,
        &[eventlog_conformance::event("item.received", 1)],
        &eventlog_conformance::meta("dirty-control", &json!({})),
    )
    .await
    .expect("appended");
    eventlog_conformance::drain_at_least(&runner, &tenant, 1).await;
    assert_eq!(
        concrete
            .capture_tenant(&tenant, &[CAPTURE_LEDGER], limits())
            .await
            .expect("the control: the projection is admitted before anything is marked")
            .projections[0]
            .rows
            .len(),
        1
    );

    // The only writer of the marker, and it sets `redacted_at` on the same event.
    port.redact(&stream, 1, "erased").await.expect("redacted");
    let dirty = CaptureError::ProjectionUnavailable {
        projection: CAPTURE_LEDGER.name.to_owned(),
        reason: ProjectionCaptureRefusal::Dirty,
    };
    let marked = concrete
        .capture_tenant(&tenant, &[CAPTURE_LEDGER], limits())
        .await
        .expect_err("no complete observation of this tenant exists");
    assert_ne!(
        marked, dirty,
        "the marker is set here, and the refusal that reports it is unreachable behind redaction"
    );
    assert_eq!(marked, CaptureError::RedactedHistory);

    // Rebuilding clears the marker. The refusal does not move, so no window exposes `Dirty`.
    port.rebuild_projection(Arc::new(CaptureLedger), &tenant)
        .await
        .expect("rebuilt from current history");
    let rebuilt = concrete
        .capture_tenant(&tenant, &[CAPTURE_LEDGER], limits())
        .await
        .expect_err("rebuilding a view does not restore erased history");
    assert_ne!(rebuilt, dirty);
    assert_eq!(rebuilt, CaptureError::RedactedHistory);
}

/// "Refuse non-regular required files and existing path violations."
///
/// A symlink standing in for a blob object has the right bytes at the end of it. Following it
/// would read content from outside the store and call it this tenant's binding.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_blob_object_that_is_not_a_regular_file_is_refused_without_being_followed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let elsewhere = tempfile::tempdir().expect("temporary directory outside the store");
    let tenant = populated(directory.path(), "file-review-one-object").await;

    let object = only_object(directory.path());
    let target = elsewhere.path().join("object");
    fs::copy(&object, &target).expect("identical bytes, somewhere else");
    fs::remove_file(&object).expect("removed the regular object");
    std::os::unix::fs::symlink(&target, &object).expect("symlinked object");
    assert!(
        fs::symlink_metadata(&object)
            .expect("the link is there")
            .is_symlink(),
        "the fixture only means something while the object is not a regular file"
    );

    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");
    assert_eq!(
        handle.capture_tenant(&tenant, &[], limits()).await,
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "a required file that is not a regular file is corruption, not content to follow"
    );
    assert!(
        fs::symlink_metadata(&object)
            .expect("the link survived")
            .is_symlink(),
        "the refusal repaired or replaced the object"
    );
    assert_eq!(
        fs::read(&target).expect("readable target"),
        b"bound-bytes".to_vec(),
        "the refusal wrote through the link"
    );
}

/// "surplus/truncated journal bytes are corruption", and neither is repaired by a reader.
#[tokio::test(flavor = "multi_thread")]
async fn surplus_or_truncated_committed_bytes_are_corruption_and_nothing_is_repaired() {
    for surplus in [true, false] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let _ = populated(directory.path(), "file-review-one-journal").await;
        let events = directory.path().join("events.jsonl");
        let committed = fs::read(&events).expect("committed history");
        assert!(
            committed.len() > 1,
            "the fixture only means something while there is history to damage"
        );
        let damaged = if surplus {
            let mut bytes = committed.clone();
            bytes.push(b'\n');
            bytes
        } else {
            committed[..committed.len() - 1].to_vec()
        };
        fs::write(&events, &damaged).expect("damaged history");
        let manifest = fs::read(directory.path().join("manifest.json")).expect("manifest");

        // Twice: a reader that repaired on the first call would answer on the second.
        for _ in 0..2 {
            assert!(
                matches!(
                    FileTenantCapture::open(directory.path()).await,
                    Err(CaptureError::Corrupt {
                        material: CaptureMaterial::Journal
                    })
                ),
                "surplus = {surplus}: committed bytes the manifest does not commit are corruption"
            );
            assert_eq!(
                fs::read(&events).expect("committed history"),
                damaged,
                "surplus = {surplus}: the reader truncated or rewrote history"
            );
            assert_eq!(
                fs::read(directory.path().join("manifest.json")).expect("manifest"),
                manifest,
                "surplus = {surplus}: the reader rewrote the commit point"
            );
        }
    }
}

/// "Zero is a real cap", and the refusal names a resource this tenant actually holds.
#[tokio::test(flavor = "multi_thread")]
async fn a_zero_cap_names_the_resource_the_tenant_actually_holds() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let tenant = populated(directory.path(), "file-review-one-caps").await;
    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");

    for (resource, caps) in [
        (
            CaptureResource::Events,
            CaptureLimits {
                max_events: 0,
                ..limits()
            },
        ),
        (
            CaptureResource::Blobs,
            CaptureLimits {
                max_blobs: 0,
                ..limits()
            },
        ),
        (
            CaptureResource::PayloadBytes,
            CaptureLimits {
                max_payload_bytes: 0,
                ..limits()
            },
        ),
    ] {
        assert_eq!(
            handle.capture_tenant(&tenant, &[], caps).await,
            Err(CaptureError::LimitExceeded { resource, limit: 0 }),
            "one event and one blob cross exactly these caps and no others"
        );
    }

    // Nothing fits in nothing: a provisioned tenant with no content satisfies every zero cap.
    let bare = TenantId::new("file-review-one-bare").expect("valid tenant");
    {
        let store = FileEventStore::open(directory.path())
            .await
            .expect("opened store");
        store
            .stream_identity(&bare)
            .await
            .expect("provisioned identity");
    }
    let reopened = FileTenantCapture::open(directory.path())
        .await
        .expect("opened read-only");
    let empty = reopened
        .capture_tenant(
            &bare,
            &[],
            CaptureLimits {
                max_events: 0,
                max_blobs: 0,
                max_projection_rows: 0,
                max_payload_bytes: 0,
            },
        )
        .await
        .expect("nothing fits in nothing");
    assert!(empty.events.is_empty() && empty.blobs.is_empty());
    assert!(empty.projections.is_empty());
}
