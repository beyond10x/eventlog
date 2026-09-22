//! Independent adversary cases for unit 12's deferred blob reader (review pass 2).
//!
//! Additions only. Nothing here changes, skips or weakens an existing case. Every case drives the
//! implementation against a sentence the unit wrote about itself, because the unit wrote both
//! halves and nothing else in the suite compares them.
//!
//! Pass 1 attacked one side of the correction's error split — an object that cannot be opened.
//! These attack the other side and the middle: the kinds of filesystem failure that decide
//! `CaptureError` by `std::io::ErrorKind` alone, and the one refusal the deferred capture answers
//! differently from the eager capture that it promises to answer identically.

use std::{fs, os::unix::fs::PermissionsExt, path::Path};

use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture, EventStore, TenantId,
};
use eventlog_file::FileEventStore;

fn generous() -> CaptureLimits {
    CaptureLimits {
        max_events: 1024,
        max_blobs: 1024,
        max_projection_rows: 1024,
        max_payload_bytes: 1 << 20,
    }
}

const CONTENT: &[u8] = b"adversary-two: the content the observation bound";

/// A provisioned tenant with one binding, and nothing else to fail on.
async fn bound(root: &Path) -> (FileEventStore, TenantId) {
    let tenant = TenantId::new("adversary-two-owner").expect("valid tenant");
    let store = FileEventStore::open(root).await.expect("opened store");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    store
        .put_blob(&tenant, "only", CONTENT)
        .await
        .expect("bound content");
    (store, tenant)
}

/// The object this fixture's content is stored under.
fn object_holding(root: &Path, content: &[u8]) -> std::path::PathBuf {
    for entry in fs::read_dir(root.join("blobs")).expect("object directory") {
        let path = entry.expect("object entry").path();
        if fs::read(&path).expect("object bytes") == content {
            return path;
        }
    }
    panic!("no stored object holds the fixture's content");
}

/// An object that cannot be opened is deferred to the read, and the read is where it surfaces.
///
/// **The decision this case now encodes** (coordinator, 2026-09-22 02:44, recorded in WAVE.md as a
/// decision on a reviewer's assertion): a deferred capture **defers every content read**, and the
/// guarantee a consumer keeps is that **a content failure is not lost** — it surfaces on the read
/// that would have handed the content out, as `CaptureError::Store`.
///
/// This case was written the other way round, asserting that the two entry points must refuse
/// alike, and its name carried the fork: `..._or_the_contract_is_wrong`. The fork is closed and
/// the contract was the thing that was wrong. `capture_tenant_deferred`'s `# Errors` used to
/// except one class — "stored content which fails its integrity check" — from what it returns,
/// which said that an observation performing no read must somehow still produce every *other*
/// verdict of that read. It now excepts **every refusal a content read produces, whichever
/// refusal that is** (`crates/eventlog-core/src/capture.rs`).
///
/// The alternative was measured before it was rejected: making `bound_length` open every object
/// makes the two entry points agree here and costs about +12 ms on a 7,815-blob, 157.0 MB capture
/// — 61/63/82 ms becoming 78/72/75 ms, against a 500 ms acceptance. It was not rejected on cost.
/// It was rejected because a per-blob `open(2)` on the capture path is the operation this unit's
/// acceptance says is *gone*, and because openability established at capture time expires the
/// instant the capture returns, while the deferred read happens whenever its holder gets round to
/// it. The unapplied patch is kept in the wave's scratch.
///
/// So what is asserted is the whole of what the contract now promises, in three parts: the
/// observation is complete about **what** is bound, the failure is **not lost**, and the eager
/// path — which does the read — still refuses exactly as it did. The third is what makes the
/// first two a deferral rather than a silent loss.
#[tokio::test(flavor = "multi_thread")]
async fn an_unopenable_object_is_deferred_to_the_read_rather_than_lost() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path();
    let (store, tenant) = bound(root).await;

    // What the eager path binds, established while it can still read, so the comparison below is
    // against the eager path's own answer and not against this fixture's memory of itself.
    let expected: Vec<String> = store
        .capture_tenant(&tenant, &[], generous())
        .await
        .expect("a complete observation of an undamaged store")
        .blobs
        .into_iter()
        .map(|blob| blob.digest)
        .collect();

    let object = object_holding(root, CONTENT);
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

    let deferred = store
        .capture_tenant_deferred(&tenant, &[], generous())
        .await;
    let read = deferred.as_ref().ok().map(|capture| {
        capture
            .blobs
            .iter()
            .find(|blob| blob.digest == "only")
            .expect("the binding is in the observation")
            .bytes()
    });
    let eager = store.capture_tenant(&tenant, &[], generous()).await;
    fs::set_permissions(&object, original).expect("permissions restored");

    // 1. The observation is complete about what is bound, because deciding that needs no content.
    let deferred = deferred.expect("a deferred capture reads no content and so cannot fail on it");
    assert_eq!(
        deferred
            .blobs
            .iter()
            .map(|blob| blob.digest.clone())
            .collect::<Vec<_>>(),
        expected,
        "the deferred capture bound a different set of digests from the eager path's"
    );

    // 2. The failure is not lost: it is waiting on the read that would have handed the content out.
    let read = read.expect("the observation was taken").expect_err(
        "an object that cannot be opened has no bytes to hand out, on either entry point",
    );
    assert!(
        matches!(read, CaptureError::Store(_)),
        "the deferred read did not surface the operational failure the eager capture refuses \
         with; a deferred capture may only defer a content failure, never drop it: {read:?}"
    );

    // 3. And the eager path, which does perform the read, still refuses exactly as it did.
    let eager = eager.expect_err("the eager capture reads the object and cannot");
    assert!(
        matches!(eager, CaptureError::Store(_))
            && format!("{eager:?}").contains("PermissionDenied"),
        "the eager capture stopped refusing an unopenable object with the operational verdict, \
         which is what makes the deferred capture above a deferral and not a loss: {eager:?}"
    );
}

/// A store that is no longer where it was is not a store whose content failed a check.
///
/// `BoundBlobs::read` divides its refusals at `crates/eventlog-core/src/capture.rs:69`:
///
/// > Returns [`CaptureError::Corrupt`] when this reader does not name that digest, when the
/// > stored object is gone or is not the shape the provider writes, or when its content no longer
/// > hashes to what the observation recorded — and [`CaptureError::Store`] for an operational
/// > failure.
///
/// `material` (`crates/eventlog-file/src/capture.rs:624`) implements that division with one
/// predicate on `std::io::ErrorKind`, and its own doc states the rule: *"`NotFound` is the only
/// kind that says anything about the stored material."* That is true of one object and false of
/// the path above it, and the deferred reader is the first thing in this crate that can outlive
/// the store it reads — `BoundObjects` holds a `PathBuf` and no handle, no lock and no manifest,
/// and is read "whenever its holder gets round to it".
///
/// A store directory that has been moved, unmounted or swept answers `ENOENT` for every object in
/// it. `CaptureMaterial`'s own documentation is that the variant is the whole diagnostic, and
/// `Corrupt` is documented as "stored capture material failed its integrity check" — so a
/// consumer is told to stop trusting a store that is intact and merely absent, with nothing else
/// in the value to read. This is the operational failure pass 1 found, arriving through the arm
/// the correction kept.
///
/// Nothing is damaged here and no byte of the store is touched: the root is renamed and renamed
/// back, and the same binding hands out its exact content afterwards, which is what makes the
/// refusal in the middle an operational one.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_that_moved_is_an_operational_failure_and_not_integrity_damage() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    fs::create_dir(&root).expect("store root");
    let (store, tenant) = bound(&root).await;

    let observed = store
        .capture_tenant_deferred(&tenant, &[], generous())
        .await
        .expect("a deferred capture of a provisioned tenant");
    let binding = || {
        observed
            .blobs
            .iter()
            .find(|blob| blob.digest == "only")
            .expect("the binding is in the observation")
    };
    assert_eq!(
        binding().bytes().expect("an undamaged binding"),
        CONTENT,
        "precondition: the binding hands out its content while the store is where it was"
    );

    let moved = directory.path().join("moved");
    fs::rename(&root, &moved).expect("the store directory is moved, not damaged");
    assert!(
        !root.exists(),
        "precondition: the store is no longer at the path the reader holds"
    );
    let refused = binding().bytes();
    fs::rename(&moved, &root).expect("the store directory is put back");

    assert_eq!(
        binding().bytes().expect("the store was never damaged"),
        CONTENT,
        "precondition: every byte of this store survived the move, so the refusal above was about \
         reaching it and not about its content"
    );
    let error = refused.expect_err("a store that is not there has no bytes to hand out");
    assert!(
        matches!(error, CaptureError::Store(_)),
        "a store that had been moved away read back as stored integrity damage: \
         CaptureError::Corrupt is documented as \"stored capture material failed its integrity \
         check\" and CaptureMaterial's variant is the whole diagnostic, so a consumer cannot tell \
         this from a store it must stop trusting — material() decides on ErrorKind alone \
         (eventlog-file/src/capture.rs:624) and ENOENT on a path above the object is not the \
         object failing a check: {error:?}"
    );
}

/// Material that is definitively gone does not become retryable by failing with a second errno.
///
/// The other side of the same predicate. `material` answers `Corrupt` for `NotFound` and `Store`
/// — "this attempt failed", the refusal a consumer is meant to retry — for every other kind. An
/// object directory that is not a directory fails every read with `ENOTDIR` forever: the object
/// the committed record names is gone, which `BoundBlobs::read`'s contract
/// (`eventlog-core/src/capture.rs:69`) places squarely in `Corrupt`, and no number of retries
/// will find it.
///
/// Taken with the sibling case above, the pair is one finding: `std::io::ErrorKind` is not the
/// axis the contract divides on. The question the contract asks is whether the stored material is
/// gone or whether this attempt failed, and one errno answers both ways on each side.
#[tokio::test(flavor = "multi_thread")]
async fn an_object_directory_that_is_not_a_directory_is_material_that_is_gone() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path();
    let (store, tenant) = bound(root).await;

    let observed = store
        .capture_tenant_deferred(&tenant, &[], generous())
        .await
        .expect("a deferred capture of a provisioned tenant");
    let binding = || {
        observed
            .blobs
            .iter()
            .find(|blob| blob.digest == "only")
            .expect("the binding is in the observation")
    };

    let objects = root.join("blobs");
    let stashed = root.join("blobs-stashed");
    fs::rename(&objects, &stashed).expect("the object directory is set aside");
    fs::write(&objects, b"not a directory").expect("a regular file stands where it stood");
    let refused = binding().bytes();
    fs::remove_file(&objects).expect("the regular file is removed");
    fs::rename(&stashed, &objects).expect("the object directory is put back");

    let error = refused.expect_err("an object under a path component that is not a directory");
    assert_eq!(
        error,
        CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        },
        "an object that is permanently unreachable was reported as an operational failure, which \
         BoundBlobs::read documents as the refusal that is not about the stored material: every \
         read of every object here fails the same way forever, so there is nothing to retry — \
         material() decides on ErrorKind alone (eventlog-file/src/capture.rs:624): {error:?}"
    );
}

/// The refusal carries the variant and nothing that identifies the store.
///
/// `CaptureMaterial`'s documentation is that the variant is the whole diagnostic, and correction
/// round 1's commit message makes the same promise for the arm it added: *"The `ErrorKind` is
/// fieldless, so no path and no content escape."* This holds the new `Store` message to that: a
/// refusal a consumer may log must not carry the store's path, the object's id, the tenant or a
/// byte of the content.
#[tokio::test(flavor = "multi_thread")]
async fn an_operational_refusal_names_no_path_object_tenant_or_byte_of_content() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path();
    let (store, tenant) = bound(root).await;

    let observed = store
        .capture_tenant_deferred(&tenant, &[], generous())
        .await
        .expect("a deferred capture of a provisioned tenant");
    let object = object_holding(root, CONTENT);
    let id = object
        .file_name()
        .expect("an object file name")
        .to_string_lossy()
        .into_owned();
    let original = fs::metadata(&object)
        .expect("object metadata")
        .permissions();
    fs::set_permissions(&object, fs::Permissions::from_mode(0o000))
        .expect("object made unopenable");
    assert!(
        fs::read(&object).is_err(),
        "precondition: this case needs an object the process cannot open"
    );
    let refused = observed
        .blobs
        .iter()
        .find(|blob| blob.digest == "only")
        .expect("the binding is in the observation")
        .bytes();
    fs::set_permissions(&object, original).expect("permissions restored");

    let error = refused.expect_err("an object that cannot be opened has no bytes to hand out");
    let rendered = format!("{error}|{error:?}");
    for (what, secret) in [
        ("the store root", root.to_string_lossy().into_owned()),
        ("the object id", id),
        ("the tenant", tenant.as_str().to_owned()),
        ("the bound digest", "only".to_owned()),
        (
            "the content",
            String::from_utf8(CONTENT.to_vec()).expect("utf8 fixture"),
        ),
    ] {
        assert!(
            !rendered.contains(&secret),
            "the refusal names {what}: a capture refusal is the variant and nothing else, and \
             correction round 1 promised the ErrorKind is fieldless so no path and no content \
             escape: {rendered}"
        );
    }
}

/// A shape this provider does not write is refused by the capture that finds it, either capture.
///
/// `bound_length` (`crates/eventlog-file/src/capture.rs:565`) carries the half of the
/// complete-content check that does not need the content, and says so in its own words: *"an
/// admitted object name and a regular file"*. Nothing in the suite drives the regular-file half of
/// it: deleting
///
/// ```ignore
/// if !metadata.is_file() {
///     return Err(corrupt());
/// }
/// ```
///
/// from `bound_length` leaves the whole `eventlog-file` lane green — 136 cases, exit 0, measured
/// on a copy of this tree. The eager path's identical check in `read_blob` is driven by pass 1's
/// substitution cases, which damage the object *after* the observation and therefore never reach
/// `bound_length`; a capture taken while the object is already the wrong shape reaches nothing
/// else.
///
/// That is what this pins, and it pins it as a divergence rather than as an internal detail,
/// because the consequence of the unguarded arm is the contract at
/// `crates/eventlog-core/src/capture.rs:297`: "Every refusal, every cap and every ordering is
/// [`Self::capture_tenant`]'s". Without the check the deferred capture returns a complete
/// observation where the eager one refuses `Corrupt`, and charges the caller's payload cap the
/// length of a symbolic link rather than of any content.
///
/// The link's target holds exactly the bytes the record binds, so nothing but the shape is wrong:
/// a provider that admitted it would be admitting an object it did not write, on the strength of
/// content it had not read.
#[tokio::test(flavor = "multi_thread")]
async fn both_captures_refuse_an_object_that_is_not_the_shape_this_provider_writes() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path();
    let (store, tenant) = bound(root).await;

    let object = object_holding(root, CONTENT);
    let elsewhere = directory.path().join("content-by-another-name");
    fs::rename(&object, &elsewhere).expect("the content is moved aside");
    std::os::unix::fs::symlink(&elsewhere, &object).expect("a link stands where the object stood");
    assert_eq!(
        fs::read(&object).expect("the link resolves"),
        CONTENT,
        "precondition: the link's target is exactly the content the record binds, so the only \
         thing wrong with this object is its shape"
    );
    assert!(
        !fs::symlink_metadata(&object)
            .expect("the link is there")
            .is_file(),
        "precondition: the entry at the object's own name is not a regular file"
    );

    let eager = store.capture_tenant(&tenant, &[], generous()).await;
    let deferred = store
        .capture_tenant_deferred(&tenant, &[], generous())
        .await;

    assert_eq!(
        eager.err(),
        Some(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "precondition: the eager capture refuses a shape this provider does not write"
    );
    assert_eq!(
        deferred.map(|capture| capture.blobs.len()).err(),
        Some(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "the deferred capture admitted an object this provider never wrote, and charged the \
         caller's payload cap the length of a symbolic link: bound_length's regular-file check is \
         the only thing that refuses it and nothing else in the suite drives it"
    );
}

/// A reader refuses a digest it does not name, which is the first clause of its own contract.
///
/// `BoundBlobs::read` (`eventlog-core/src/capture.rs:69`) begins "Returns `CaptureError::Corrupt`
/// when this reader does not name that digest". `DeferredBlob::digest` is a public field, so a
/// consumer holding one can ask the observation's reader for a coordinate the observation never
/// bound; nothing in the suite asks. The answer must be a refusal and never another binding's
/// bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_refuses_a_digest_its_own_observation_never_bound() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path();
    let (store, tenant) = bound(root).await;
    store
        .put_blob(&tenant, "sibling", b"a second binding's content")
        .await
        .expect("bound content");

    let observed = store
        .capture_tenant_deferred(&tenant, &[], generous())
        .await
        .expect("a deferred capture of a provisioned tenant");
    let mut retargeted = observed
        .blobs
        .iter()
        .find(|blob| blob.digest == "only")
        .expect("the binding is in the observation")
        .clone();

    retargeted.digest = "never-bound".to_owned();
    assert_eq!(
        retargeted.bytes(),
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "a reader answered for a digest its observation never bound"
    );

    retargeted.digest = "sibling".to_owned();
    assert_eq!(
        retargeted.bytes().expect("the sibling is bound"),
        b"a second binding's content".to_vec(),
        "a reader asked for a coordinate it does name answers that coordinate's content"
    );
}
