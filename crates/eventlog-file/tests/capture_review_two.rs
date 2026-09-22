#![cfg(unix)]

use std::{os::unix::fs::symlink, path::Path};

use eventlog_core::{CaptureError, CaptureLimits, ConsistentTenantCapture, EventStore, TenantId};
use eventlog_file::{FileEventStore, FileTenantCapture};

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 8,
        max_blobs: 8,
        max_projection_rows: 8,
        max_payload_bytes: 8_192,
    }
}

async fn initialized(root: &Path, tenant: &TenantId) {
    let store = FileEventStore::open(root).await.expect("initialized store");
    store
        .stream_identity(tenant)
        .await
        .expect("provisioned identity");
}

/// The strict opener's pending-intent rule is about a directory entry, not whether following that
/// entry reaches a target. A dangling link at the reserved append-intent name is still an existing
/// path violation and must never be read past as if no recovery authority existed.
#[tokio::test]
async fn opening_refuses_a_dangling_pending_intent() {
    let directory = tempfile::tempdir().expect("temporary store");
    let tenant = TenantId::new("capture-review-two-open").expect("valid tenant");
    initialized(directory.path(), &tenant).await;

    let target = directory.path().join("missing-append-intent-target");
    let intent = directory.path().join("append.json");
    symlink(&target, &intent).expect("dangling pending-intent entry");

    let Err(refusal) = FileTenantCapture::open(directory.path()).await else {
        panic!("a dangling pending-intent entry was read past");
    };
    assert_eq!(refusal, CaptureError::RecoveryRequired);
    assert!(
        std::fs::symlink_metadata(&intent)
            .expect("intent entry remains")
            .file_type()
            .is_symlink(),
        "strict open must leave the pending entry untouched"
    );
    assert!(
        !target.exists(),
        "strict open must not create the link target"
    );
}

/// Opening and capturing use the same strict path. An intent appearing after the handle opened is
/// therefore also a recovery refusal, including when its reserved name is a dangling link.
#[tokio::test]
async fn capture_refuses_a_dangling_pending_intent_created_after_open() {
    let directory = tempfile::tempdir().expect("temporary store");
    let tenant = TenantId::new("capture-review-two-capture").expect("valid tenant");
    initialized(directory.path(), &tenant).await;
    let handle = FileTenantCapture::open(directory.path())
        .await
        .expect("strict handle opened before the intent");

    let target = directory.path().join("missing-privacy-intent-target");
    let intent = directory.path().join("privacy.json");
    symlink(&target, &intent).expect("dangling pending-intent entry");

    assert!(
        matches!(
            handle.capture_tenant(&tenant, &[], limits()).await,
            Err(CaptureError::RecoveryRequired)
        ),
        "capture read past a dangling pending-intent entry"
    );
    assert!(
        std::fs::symlink_metadata(&intent)
            .expect("intent entry remains")
            .file_type()
            .is_symlink(),
        "capture must leave the pending entry untouched"
    );
    assert!(!target.exists(), "capture must not create the link target");
}
