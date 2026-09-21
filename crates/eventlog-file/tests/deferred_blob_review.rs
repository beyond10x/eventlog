//! Independent review cases for unit 12's deferred blob reader (review pass 1).
//!
//! Additions only. Nothing here changes, skips or weakens an existing case. Each case names the
//! sentence of the unit's *own* documentation it checks the implementation against, because the
//! unit wrote both halves and nothing in the suite compares them.

use std::{fs, os::unix::fs::PermissionsExt, path::Path, sync::Arc};

use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture, EventStore, Expected,
    ProjectionSpec, StreamId, TenantId,
};
use eventlog_file::{FileEventStore, FileTenantCapture};
use serde_json::json;

fn generous() -> CaptureLimits {
    CaptureLimits {
        max_events: 1024,
        max_blobs: 1024,
        max_projection_rows: 1024,
        max_payload_bytes: 1 << 20,
    }
}

/// A small authority: three events on two streams and three bindings of different sizes.
async fn populated(root: &Path) -> (FileEventStore, TenantId) {
    let tenant = TenantId::new("deferred-review-owner").expect("valid tenant");
    let store = FileEventStore::open(root).await.expect("opened store");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    for (index, stream) in ["one", "two", "one"].into_iter().enumerate() {
        store
            .append(
                &StreamId::new(tenant.clone(), "item", stream).expect("valid stream"),
                Expected::Any,
                &[eventlog_conformance::event(
                    "item.received",
                    i64::try_from(index).expect("a fixture index fits"),
                )],
                &eventlog_conformance::meta(&format!("m{index}"), &json!({})),
            )
            .await
            .expect("appended");
    }
    for (digest, bytes) in [
        ("alpha", b"".to_vec()),
        ("bravo", b"bb".to_vec()),
        ("charlie", b"thirteen-byte".to_vec()),
    ] {
        store
            .put_blob(&tenant, digest, &bytes)
            .await
            .expect("bound content");
    }
    (store, tenant)
}

/// The path of the object whose stored content is exactly `content`.
fn object_holding(root: &Path, content: &[u8]) -> std::path::PathBuf {
    for entry in fs::read_dir(root.join("blobs")).expect("object directory") {
        let path = entry.expect("object entry").path();
        if fs::read(&path).expect("object bytes") == content {
            return path;
        }
    }
    panic!("no stored object holds the fixture's content");
}

/// An object the reader cannot open is an operational failure, not stored damage.
///
/// The unit's own new port contract says so, in `crates/eventlog-core/src/capture.rs:66-73`:
///
/// > Returns [`CaptureError::Corrupt`] when this reader does not name that digest, when the
/// > stored object is gone or is not the shape the provider writes, or when its content no
/// > longer hashes to what the observation recorded — and [`CaptureError::Store`] for an
/// > operational failure.
///
/// and again on `DeferredBlob::bytes` at `capture.rs:113-119`. `CaptureError::Corrupt` is
/// documented as the integrity verdict — "stored capture material failed its integrity check",
/// and "The variant is the whole diagnostic" — so the distinction is the only thing a consumer
/// has to tell a store it must stop trusting from one it should retry against.
///
/// The fixture isolates exactly that: the object is still a regular file, still under its
/// admitted name, still the size the observation charged, and its bytes still hash to what the
/// record says. Only `open(2)` fails. That is the one line of `read_blob` the promise is about.
///
/// This window is the deferred reader's, not the eager capture's: the eager read happened inside
/// the strict read, under the writers' lock, microseconds after the object was stat'd. A
/// `DeferredBlob` is read whenever its holder gets round to it, which is where a permission
/// change, an exhausted descriptor table or an EIO actually lands.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_object_is_an_operational_failure_and_not_integrity_damage() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path();
    let (store, tenant) = populated(root).await;

    let observed = store
        .capture_tenant_deferred(&tenant, &[], generous())
        .await
        .expect("a deferred capture of a provisioned tenant");

    let object = object_holding(root, b"thirteen-byte");
    let original = fs::metadata(&object)
        .expect("object metadata")
        .permissions();
    fs::set_permissions(&object, fs::Permissions::from_mode(0o000))
        .expect("object made unopenable");
    assert!(
        fs::read(&object).is_err(),
        "precondition: this case needs an object the process cannot open; it can, so nothing \
         below is being tested"
    );
    assert!(
        fs::symlink_metadata(&object)
            .expect("the object is still there")
            .is_file(),
        "precondition: the object is still a regular file of the admitted name"
    );

    let refused = observed
        .blobs
        .iter()
        .find(|blob| blob.digest == "charlie")
        .expect("the binding is in the observation")
        .bytes();
    fs::set_permissions(&object, original).expect("permissions restored");

    let error = refused.expect_err("an object that cannot be opened has no bytes to hand out");
    assert!(
        matches!(error, CaptureError::Store(_)),
        "an operational read failure was reported as stored integrity damage: the port's own \
         contract promises CaptureError::Store for it and the provider answered {error:?}"
    );
}

/// Every cap is answered by the deferred capture exactly as by the eager one.
///
/// `ConsistentTenantCapture::capture_tenant_deferred` promises it, in the unit's own words at
/// `crates/eventlog-core/src/capture.rs:296-311`: "Every refusal, every cap and every ordering is
/// [`Self::capture_tenant`]'s", and "# Errors: Exactly what [`Self::capture_tenant`] returns,
/// except that stored content which fails its integrity check is refused by
/// [`DeferredBlob::bytes`] rather than here." Nothing damages content here, so the exception does
/// not apply and the two must agree on every point of every cap.
///
/// The suite measures this at three points of one cap. A cap is a boundary and boundaries are
/// where a lazy capture that stopped counting what it stopped reading would show.
#[tokio::test(flavor = "multi_thread")]
async fn a_deferred_capture_answers_every_cap_exactly_as_the_eager_one() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path();
    let (store, tenant) = populated(root).await;

    let whole = store
        .capture_tenant(&tenant, &[], generous())
        .await
        .expect("a complete observation");
    let payload: u64 = whole
        .events
        .iter()
        .map(|event| {
            serde_json::to_vec(&event.data)
                .expect("encodable body")
                .len() as u64
        })
        .sum::<u64>()
        + whole
            .blobs
            .iter()
            .map(|blob| blob.bytes.len() as u64)
            .sum::<u64>();

    let eager = FileTenantCapture::open(root).await.expect("reader handle");
    let deferred = FileTenantCapture::open(root).await.expect("reader handle");

    let mut grid = Vec::new();
    for events in 0..=4_u64 {
        grid.push(CaptureLimits {
            max_events: events,
            ..generous()
        });
    }
    for blobs in 0..=4_u64 {
        grid.push(CaptureLimits {
            max_blobs: blobs,
            ..generous()
        });
    }
    for payload_bytes in [0, 1, payload / 2, payload - 1, payload, payload + 1] {
        grid.push(CaptureLimits {
            max_payload_bytes: payload_bytes,
            ..generous()
        });
    }

    for limits in grid {
        let left = eager.capture_tenant(&tenant, &[], limits).await;
        let right = deferred.capture_tenant_deferred(&tenant, &[], limits).await;
        match (left, right) {
            (Ok(left), Ok(right)) => assert_eq!(
                right.load().expect("nothing was damaged"),
                left,
                "the deferred capture is not the eager one at {limits:?}"
            ),
            (Err(left), Err(right)) => assert_eq!(
                right, left,
                "the two captures refuse the same store differently at {limits:?}"
            ),
            (left, right) => panic!(
                "one capture refused and the other did not at {limits:?}: eager {:?}, deferred {}",
                left.map(|value| value.blobs.len()),
                right.map_or_else(
                    |error| format!("{error:?}"),
                    |value| value.blobs.len().to_string()
                )
            ),
        }
    }
}

/// A projection request is answered by the deferred capture exactly as by the eager one.
///
/// Same promise as above, and this is the half of it nothing in the suite reaches: every existing
/// call of `capture_tenant_deferred` — the conformance exercise, the provider's own unit case and
/// the measurement harness — passes an *empty* projection list, so `observed_projections`,
/// `ProjectionUnavailable` and the projection-row cap have never been exercised on this path.
#[tokio::test(flavor = "multi_thread")]
async fn a_deferred_capture_answers_a_projection_request_exactly_as_the_eager_one() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path();
    let store = FileEventStore::open(root).await.expect("opened store");
    store
        .register_inline(Arc::new(eventlog_conformance::CaptureLedger))
        .await
        .expect("declared capture projections");
    let tenant = TenantId::new("deferred-review-projected").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    for (index, key) in ["one", "two", "one"].into_iter().enumerate() {
        store
            .append(
                &StreamId::new(tenant.clone(), "item", key).expect("valid stream"),
                Expected::Any,
                &[eventlog_core::NewEvent::new(
                    "item.received",
                    1,
                    json!({ "key": key, "value": index }),
                )
                .expect("valid event")],
                &eventlog_conformance::meta(&format!("p{index}"), &json!({})),
            )
            .await
            .expect("appended");
    }
    store
        .put_blob(&tenant, "bound", b"bound-bytes")
        .await
        .expect("bound content");

    let requested = [
        eventlog_conformance::CAPTURE_SIDECAR,
        eventlog_conformance::CAPTURE_LEDGER,
    ];
    let undeclared = [ProjectionSpec {
        name: "nobody_declared_this",
        indexed: &[],
    }];

    for (request, what) in [
        (&requested[..], "two declared projections, in request order"),
        (&undeclared[..], "a projection no registry entry names"),
    ] {
        for rows in [0_u64, 1, 2, 1024] {
            let limits = CaptureLimits {
                max_projection_rows: rows,
                ..generous()
            };
            let left = store.capture_tenant(&tenant, request, limits).await;
            let right = store
                .capture_tenant_deferred(&tenant, request, limits)
                .await;
            match (left, right) {
                (Ok(left), Ok(right)) => assert_eq!(
                    right.load().expect("nothing was damaged"),
                    left,
                    "{what}: the deferred capture is not the eager one at {limits:?}"
                ),
                (Err(left), Err(right)) => assert_eq!(
                    right, left,
                    "{what}: the two captures refuse differently at {limits:?}"
                ),
                (left, right) => panic!(
                    "{what}: one capture refused and the other did not at {limits:?}: eager \
                     {:?}, deferred {}",
                    left.map(|value| value.projections.len()),
                    right.map_or_else(
                        |error| format!("{error:?}"),
                        |value| value.projections.len().to_string()
                    )
                ),
            }
        }
    }
}

/// What an ordinary, authorized delete does to an observation already taken.
///
/// A characterization case, pinning behaviour the unit documents but nothing measures: after
/// `delete_blob` — a legitimate port operation, not damage — the object is swept and the
/// binding's content comes back as `CaptureError::Corrupt { material: Blob }`, the same verdict
/// the exercise's deliberately damaged object produces. The eager capture handed the bytes over
/// and nothing later could take them back.
///
/// This case asserts today's behaviour so the claim in the review is reproducible. Whether
/// `Corrupt` is the right verdict for a delete is the finding, not this assertion.
#[tokio::test(flavor = "multi_thread")]
async fn an_ordinary_delete_after_the_observation_reads_back_as_integrity_damage() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path();
    let (store, tenant) = populated(root).await;

    let eager = store
        .capture_tenant(&tenant, &[], generous())
        .await
        .expect("a complete observation");
    let observed = store
        .capture_tenant_deferred(&tenant, &[], generous())
        .await
        .expect("a deferred capture of a provisioned tenant");
    assert_eq!(
        eager
            .blobs
            .iter()
            .find(|blob| blob.digest == "charlie")
            .expect("the binding is in the eager observation")
            .bytes,
        b"thirteen-byte".to_vec()
    );

    store
        .delete_blob(&tenant, "charlie")
        .await
        .expect("an authorized delete through the port");

    assert_eq!(
        observed
            .blobs
            .iter()
            .find(|blob| blob.digest == "charlie")
            .expect("the binding is still named by the observation")
            .bytes(),
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "an ordinary delete is reported with the verdict reserved for damaged stored material"
    );
}

/// Four ways of getting bytes out of a deferred binding without the recorded hash agreeing.
///
/// Brief item 1, asked as an attack rather than as a reading: the eager capture bought "this
/// crate hashes what it hands out" by reading inside the writers' lock. The deferred reader reads
/// outside it, whenever its holder gets round to it, so anything that can reach `blobs/` between
/// the observation and the read is in scope.
///
/// - **Substitution.** The object is replaced, byte for byte, with *another binding of the same
///   store* — content this provider itself wrote and still vouches for. A reader that checked
///   "some hash the store knows" rather than the hash recorded for *this* binding would serve it.
/// - **Truncation.** The object is emptied. A reader with an error arm that returned what it had
///   already read would hand back a short value.
/// - **Redirection.** The object is replaced by a symlink to another object. A reader that used
///   `metadata` rather than `symlink_metadata` would follow it.
/// - **Blocking.** The object is replaced by a FIFO nobody will ever write to. A reader that
///   opened before it checked the object's kind would never return — and this read holds no lock
///   and no deadline, so nothing above it would time out either. The case bounds it at five
///   seconds so a failure is a failure and not a hung lane.
///
/// Every one must be refused, and refused as `Corrupt`, because each is stored material that is
/// not the shape or not the content the observation bound.
#[tokio::test(flavor = "multi_thread")]
async fn no_substitution_for_a_bound_object_is_handed_out_as_its_content() {
    let corrupt = Err(CaptureError::Corrupt {
        material: CaptureMaterial::Blob,
    });

    for (what, replace) in [
        (
            "another binding of this same store, byte for byte",
            Box::new(|object: &Path, other: &Path| {
                let bytes = fs::read(other).expect("the sibling object");
                fs::write(object, bytes).expect("object substituted");
            }) as Box<dyn Fn(&Path, &Path)>,
        ),
        (
            "an empty file",
            Box::new(|object: &Path, _: &Path| {
                fs::write(object, b"").expect("object truncated");
            }),
        ),
        (
            "a symlink to another object",
            Box::new(|object: &Path, other: &Path| {
                fs::remove_file(object).expect("object removed");
                std::os::unix::fs::symlink(other, object).expect("object redirected");
            }),
        ),
        (
            "a FIFO nobody will write to",
            Box::new(|object: &Path, _: &Path| {
                fs::remove_file(object).expect("object removed");
                let status = std::process::Command::new("mkfifo")
                    .arg(object)
                    .status()
                    .expect("mkfifo runs");
                assert!(status.success(), "the object was replaced by a FIFO");
            }),
        ),
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path();
        let (store, tenant) = populated(root).await;
        let observed = store
            .capture_tenant_deferred(&tenant, &[], generous())
            .await
            .expect("a deferred capture of a provisioned tenant");

        let object = object_holding(root, b"thirteen-byte");
        let sibling = object_holding(root, b"bb");
        replace(&object, &sibling);

        let blob = observed
            .blobs
            .iter()
            .find(|blob| blob.digest == "charlie")
            .expect("the binding is in the observation")
            .clone();
        let answered = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking(move || blob.bytes()),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("the deferred read never returned after the object became {what}")
        })
        .expect("the read did not panic");

        assert_eq!(
            answered, corrupt,
            "the binding handed out {what} as the content it bound"
        );
    }
}
