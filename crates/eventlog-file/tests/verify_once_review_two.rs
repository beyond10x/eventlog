//! Independent verification pass 2 for `story:file-eventlog-verifies-once-per-open`.
//!
//! Every case here drives the implementation from a sentence the unit itself wrote — in
//! `docs/design/file-provider.md`, in the story's `## Outcome`, or in the `CHANGELOG` entry — and
//! the sentence is quoted where the case asserts it. Nothing else in the repository compares the
//! two, so if these do not, nobody does.

use eventlog_conformance::{AdminProjector, event, meta};
use eventlog_core::{
    CaptureLimits, ConsistentTenantCapture, EventLogError, EventStore, Expected,
    InlineProjectionAdmin, Projector, StreamId, TenantId,
};
use eventlog_file::FileEventStore;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, path::Path, sync::Arc};

fn tenant() -> TenantId {
    TenantId::new("durable-owner").unwrap()
}
fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}
fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 64,
        max_blobs: 64,
        max_projection_rows: 64,
        max_payload_bytes: 65_536,
    }
}

/// The two files that are "the history": the committed bytes and the commit point.
fn history(root: &Path) -> (Vec<u8>, Vec<u8>) {
    (
        fs::read(root.join("events.jsonl")).unwrap(),
        fs::read(root.join("manifest.json")).unwrap(),
    )
}

/// Copy one store directory onto another, files only, one level deep plus `blobs/`.
fn overlay(from: &Path, onto: &Path, names: &[&str]) {
    for name in names {
        fs::copy(from.join(name), onto.join(name)).unwrap();
    }
}

fn digest_of(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Replace one same-length run of bytes inside the committed journal, leaving `manifest.json`
/// byte-identical and `events.jsonl` exactly as long as the manifest says it is.
fn damage_in_place(root: &Path, from: &str, to: &str) {
    assert_eq!(
        from.len(),
        to.len(),
        "the damage must not change the length"
    );
    let path = root.join("events.jsonl");
    let before = fs::read(&path).unwrap();
    let text = String::from_utf8(before.clone()).unwrap();
    assert!(text.contains(from), "fixture byte run {from} is in a frame");
    let after = text.replacen(from, to, 1).into_bytes();
    assert_eq!(after.len(), before.len(), "same committed byte length");
    fs::write(&path, after).unwrap();
}

async fn seeded(root: &Path, marker: i64) -> FileEventStore {
    let store = FileEventStore::open(root).await.unwrap();
    store
        .append(
            &stream("a"),
            Expected::NoStream,
            &[event("item.created", marker)],
            &meta("seed", &json!({})),
        )
        .await
        .unwrap();
    store
}

// ---------------------------------------------------------------------------------------------
// Area 1 — what the prefix hash does not cover.
//
// `docs/design/file-provider.md`: "Anything else — a new epoch, a pending recovery intent, a
// shorter or unchained file, a byte length that disagrees with the manifest, a committed prefix
// that is not the bytes this handle verified, or a head this handle never folded — falls back to
// the complete reread, which refuses a history that does not extend the observed head exactly as
// before."
// ---------------------------------------------------------------------------------------------

/// A change entirely *past* `observed.length` that is not a valid appended frame. The prefix hash
/// cannot see it: only the byte-length comparison against `manifest.json` can. `file-provider.md`
/// line 119: "corruption and unexplained tails are preserved on refusal".
#[tokio::test(flavor = "multi_thread")]
async fn an_unexplained_tail_past_the_committed_length_is_refused_and_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 1).await;
    let events = directory.path().join("events.jsonl");
    let mut tail = fs::read(&events).unwrap();
    tail.extend_from_slice(b"{\"not\":\"a frame\"}\n");
    fs::write(&events, &tail).unwrap();
    let before = history(directory.path());

    let served = store.stream_version(&stream("a")).await;
    assert!(
        matches!(&served, Err(EventLogError::Backend(message)) if message.contains("integrity")),
        "an unexplained tail past the committed length is refused: {served:?}"
    );
    assert_eq!(
        history(directory.path()),
        before,
        "the refusal preserved the tail and the commit point"
    );
}

/// Truncation to exactly a frame boundary, with `manifest.json` left naming the longer history.
#[tokio::test(flavor = "multi_thread")]
async fn a_truncation_to_a_frame_boundary_is_refused_without_altering_the_history() {
    let directory = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 1).await;
    store
        .append(
            &stream("b"),
            Expected::NoStream,
            &[event("item.created", 2)],
            &meta("second", &json!({})),
        )
        .await
        .unwrap();
    let events = directory.path().join("events.jsonl");
    let committed = fs::read(&events).unwrap();
    let boundary = committed
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("a committed frame ends in a newline");
    assert!(
        boundary + 1 < committed.len(),
        "the fixture has more than one frame to truncate to"
    );
    fs::write(&events, &committed[..=boundary]).unwrap();
    let before = history(directory.path());

    let served = store.stream_version(&stream("a")).await;
    assert!(
        matches!(&served, Err(EventLogError::Backend(message)) if message.contains("integrity")),
        "missing committed bytes are refused: {served:?}"
    );
    assert_eq!(
        history(directory.path()),
        before,
        "the refusal did not repair or re-truncate anything"
    );
}

/// A consistent rollback: both files replaced by an older, internally valid state of the same
/// store. Nothing is damaged; it simply is not the head this handle observed.
#[tokio::test(flavor = "multi_thread")]
async fn a_consistent_rollback_to_an_earlier_head_does_not_extend_the_observed_head() {
    let directory = tempfile::tempdir().unwrap();
    let older = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 1).await;
    overlay(
        directory.path(),
        older.path(),
        &["events.jsonl", "manifest.json"],
    );
    store
        .append(
            &stream("b"),
            Expected::NoStream,
            &[event("item.created", 2)],
            &meta("second", &json!({})),
        )
        .await
        .unwrap();
    overlay(
        older.path(),
        directory.path(),
        &["events.jsonl", "manifest.json"],
    );
    let before = history(directory.path());

    let served = store.stream_version(&stream("a")).await;
    assert!(
        matches!(&served, Err(EventLogError::Backend(message)) if message.contains("diverged")),
        "a shorter history does not extend the observed head: {served:?}"
    );
    assert_eq!(
        history(directory.path()),
        before,
        "the refusal wrote nothing back"
    );
}

/// The shape the brief names: "a replacement of the whole file with a different but internally
/// consistent history whose prefix happens to hash the same length". The unit's own
/// `a_longer_fork_of_the_same_store_is_refused_by_an_open_handle` builds a *longer* fork; this one
/// is exactly as long, exactly as many frames, and differs only in content.
#[tokio::test(flavor = "multi_thread")]
async fn a_same_length_replacement_history_of_the_same_store_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let fork = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    overlay(
        directory.path(),
        fork.path(),
        &["events.jsonl", "manifest.json", "writer.lock"],
    );
    store.put_blob(&tenant(), "digest", b"left").await.unwrap();

    let other = FileEventStore::open(fork.path()).await.unwrap();
    other.put_blob(&tenant(), "digest", b"righ").await.unwrap();
    drop(other);
    let replacement = fs::read(fork.path().join("events.jsonl")).unwrap();
    assert_eq!(
        replacement.len(),
        fs::read(directory.path().join("events.jsonl"))
            .unwrap()
            .len(),
        "the fixture replacement has the same committed byte length"
    );
    overlay(
        fork.path(),
        directory.path(),
        &["events.jsonl", "manifest.json"],
    );
    for entry in fs::read_dir(fork.path().join("blobs")).unwrap() {
        let entry = entry.unwrap();
        fs::copy(
            entry.path(),
            directory.path().join("blobs").join(entry.file_name()),
        )
        .unwrap();
    }
    let before = history(directory.path());

    let served = store.stream_version(&stream("a")).await;
    assert!(
        matches!(&served, Err(EventLogError::Backend(message)) if message.contains("diverged")),
        "a same-length replacement history does not extend the observed head: {served:?}"
    );
    assert_eq!(
        history(directory.path()),
        before,
        "the refusal wrote nothing back"
    );
}

/// A privacy epoch committed by another handle on the same directory.
#[tokio::test(flavor = "multi_thread")]
async fn a_privacy_epoch_committed_by_another_handle_is_refused_without_altering_the_history() {
    let directory = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 7).await;
    let other = FileEventStore::open(directory.path()).await.unwrap();
    other.redact(&stream("a"), 1, "privacy").await.unwrap();
    drop(other);
    let before = history(directory.path());

    let served = store.stream_version(&stream("a")).await;
    assert!(
        matches!(&served, Err(EventLogError::Backend(message)) if message.contains("diverged")),
        "a new epoch under an open handle is refused: {served:?}"
    );
    assert_eq!(
        history(directory.path()),
        before,
        "the refusal did not touch the erased history"
    );
}

/// A manifest naming a different store identity, over the observed committed bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_manifest_naming_a_different_store_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let stranger = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 1).await;
    let elsewhere = seeded(stranger.path(), 1).await;
    drop(elsewhere);
    overlay(
        stranger.path(),
        directory.path(),
        &["events.jsonl", "manifest.json"],
    );
    let before = history(directory.path());

    let served = store.stream_version(&stream("a")).await;
    assert!(
        matches!(&served, Err(EventLogError::Backend(message)) if message.contains("diverged")),
        "another store's history is not this handle's history: {served:?}"
    );
    assert_eq!(
        history(directory.path()),
        before,
        "the refusal wrote nothing back"
    );
}

// ---------------------------------------------------------------------------------------------
// Area 2 — the running content hash has to stay correct across every path that advances the
// committed bytes. Each case runs one such path, then damages a committed frame in place and
// requires the refusal `docs/design/file-provider.md:16` promises: "damaged committed frames
// refuse without altering the history."
// ---------------------------------------------------------------------------------------------

async fn damage_after(root: &Path, store: &FileEventStore, from: &str, to: &str, path: &str) {
    // Half one of the requirement: the path must not have left a handle that refuses a healthy
    // store. This read also re-establishes the verified view, so the damage below is caught by
    // the resumed prefix hash rather than by a complete reopen.
    let healthy = store.stream_version(&stream("a")).await;
    assert!(
        healthy.is_ok(),
        "{path}: this path left the handle refusing a store nothing has touched: {healthy:?}"
    );

    damage_in_place(root, from, to);
    let before = history(root);
    let served = store.stream_version(&stream("a")).await;
    assert!(
        matches!(&served, Err(EventLogError::Backend(message)) if message.contains("integrity")),
        "{path}: damage in place after this path must refuse the next read: {served:?}"
    );
    let appended = store
        .append(
            &stream("z"),
            Expected::NoStream,
            &[event("item.created", 1)],
            &meta("after-damage", &json!({})),
        )
        .await;
    assert!(
        matches!(&appended, Err(EventLogError::Backend(message)) if message.contains("integrity")),
        "{path}: damage in place after this path must refuse the next append: {appended:?}"
    );
    assert_eq!(
        history(root),
        before,
        "{path}: the refused append altered the history"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_committed_prefix_guard_holds_after_an_inline_registration() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    // `register_inline` admits the projections itself, so this advances the committed bytes
    // through a path that is not `transaction()`.
    store
        .register_inline(Arc::new(AdminProjector::default()))
        .await
        .unwrap();
    store
        .append(
            &stream("a"),
            Expected::NoStream,
            &[event("item.created", 5150)],
            &meta("seed", &json!({})),
        )
        .await
        .unwrap();
    damage_after(directory.path(), &store, "5150", "5151", "register_inline").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_committed_prefix_guard_holds_after_an_inline_attach_and_rebuild() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    store
        .create_projections(Arc::new(AdminProjector::default()))
        .await
        .unwrap();
    let attached = Arc::new(AdminProjector::default());
    store
        .attach_inline_existing(attached.clone())
        .await
        .unwrap();
    store
        .append(
            &stream("a"),
            Expected::NoStream,
            &[event("item.created", 6160)],
            &meta("seed", &json!({})),
        )
        .await
        .unwrap();
    store
        .rebuild_inline_projection(attached.name(), &tenant())
        .await
        .unwrap();
    damage_after(directory.path(), &store, "6160", "6161", "rebuild").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_committed_prefix_guard_holds_after_a_capture() {
    let directory = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 7170).await;
    store.stream_identity(&tenant()).await.unwrap();
    store
        .capture_tenant(&tenant(), &[], limits())
        .await
        .unwrap();
    damage_after(directory.path(), &store, "7170", "7171", "capture_tenant").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_committed_prefix_guard_holds_after_a_privacy_rewrite() {
    let directory = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 8180).await;
    store
        .append(
            &stream("a"),
            Expected::Exact(1),
            &[event("item.created", 9190)],
            &meta("second", &json!({})),
        )
        .await
        .unwrap();
    store.redact(&stream("a"), 1, "privacy").await.unwrap();
    // The epoch-1 history this handle now owns is what the guard must be carrying.
    damage_after(directory.path(), &store, "9190", "9191", "redact").await;
}

/// The other half of the same requirement: none of those paths may leave a handle that refuses a
/// store nothing has touched. A false refusal is as much a defect as a missed one.
#[tokio::test(flavor = "multi_thread")]
async fn a_healthy_store_is_not_refused_after_any_path_that_advances_the_committed_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    store
        .create_projections(Arc::new(AdminProjector::default()))
        .await
        .unwrap();
    let attached = Arc::new(AdminProjector::default());
    store
        .attach_inline_existing(attached.clone())
        .await
        .unwrap();

    store
        .append(
            &stream("a"),
            Expected::NoStream,
            &[event("item.created", 11)],
            &meta("seed", &json!({})),
        )
        .await
        .unwrap();
    assert_eq!(store.stream_version(&stream("a")).await.unwrap(), Some(1));

    store.stream_identity(&tenant()).await.unwrap();
    store
        .capture_tenant(&tenant(), &[], limits())
        .await
        .unwrap();
    assert_eq!(
        store.stream_version(&stream("a")).await.unwrap(),
        Some(1),
        "a capture must not leave the handle refusing its own history"
    );

    store.put_blob(&tenant(), "d", b"bytes").await.unwrap();
    assert_eq!(
        store.get_blob(&tenant(), "d").await.unwrap(),
        Some(b"bytes".to_vec())
    );

    store
        .rebuild_inline_projection(attached.name(), &tenant())
        .await
        .unwrap();
    assert_eq!(
        store.stream_version(&stream("a")).await.unwrap(),
        Some(1),
        "an inline rebuild must not leave the handle refusing its own history"
    );

    store.redact(&stream("a"), 1, "privacy").await.unwrap();
    assert_eq!(
        store.stream_version(&stream("a")).await.unwrap(),
        Some(1),
        "a privacy rewrite this handle minted is still this handle's history"
    );
    assert_eq!(
        store.get_blob(&tenant(), "d").await.unwrap(),
        Some(b"bytes".to_vec()),
        "the epoch this handle minted still carries its bindings"
    );
}

// ---------------------------------------------------------------------------------------------
// Area 5 — the sweep now runs on `resume` as well as on `open`.
//
// `docs/design/file-provider.md`: "A resumed operation also removes the staging names no durable
// intent selected, as a complete open does." `journal.rs` calls those names "unselected". The
// question this case answers is the converse: `privacy.next` *is* selected once `privacy.json`
// names it, and a resuming handle must not sweep a replacement somebody's recovery still needs.
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_handle_does_not_sweep_a_replacement_a_durable_intent_selected() {
    let directory = tempfile::tempdir().unwrap();
    let twin = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 4242).await;
    store.stream_identity(&tenant()).await.unwrap();
    let before_manifest: Value =
        serde_json::from_slice(&fs::read(directory.path().join("manifest.json")).unwrap()).unwrap();

    // A byte-exact copy of this store, erased there, is a valid epoch-1 replacement for here: the
    // store identity travels with the copy.
    for name in ["events.jsonl", "manifest.json", "writer.lock"] {
        fs::copy(directory.path().join(name), twin.path().join(name)).unwrap();
    }
    let elsewhere = FileEventStore::open(twin.path()).await.unwrap();
    elsewhere.redact(&stream("a"), 1, "privacy").await.unwrap();
    drop(elsewhere);
    let replacement = fs::read(twin.path().join("events.jsonl")).unwrap();
    let after_manifest: Value =
        serde_json::from_slice(&fs::read(twin.path().join("manifest.json")).unwrap()).unwrap();

    // The crash state a durable privacy intent leaves: the replacement staged, the intent
    // published, the manifest still naming the history before it.
    fs::write(directory.path().join("privacy.next"), &replacement).unwrap();
    fs::write(
        directory.path().join("privacy.json"),
        serde_json::to_vec(&json!({
            "before": before_manifest,
            "after": after_manifest,
            "replacement_digest": digest_of(&replacement),
        }))
        .unwrap(),
    )
    .unwrap();

    // The open handle's next operation resumes first. It must hand this to the complete opener
    // with `privacy.next` still on disk, not sweep the bytes the intent selected.
    let served = store.stream_version(&stream("a")).await;
    assert!(
        served.is_err(),
        "a pending privacy intent under an open handle is not this handle's history: {served:?}"
    );
    assert!(
        !directory.path().join("privacy.json").exists(),
        "the complete opener recovered the published intent"
    );
    let recovered = FileEventStore::open(directory.path()).await;
    assert!(
        recovered.is_ok(),
        "the selected replacement was still there for recovery to use: {:?}",
        recovered.err()
    );
    assert_eq!(
        fs::read(directory.path().join("events.jsonl")).unwrap(),
        replacement,
        "recovery published the replacement the intent selected, not a swept-away file"
    );
}

// ---------------------------------------------------------------------------------------------
// Area 1, continued — the clauses of `Journal::resume`'s gate that no case in the repository
// exercises. Each of the four below fails when its own clause is deleted and the rest of the
// crate is left alone; `cargo test -p eventlog-file` stays green for all four without them
// (AGENTS.md invariant 5: "Apply the one-line mutation, watch it fail, revert").
// ---------------------------------------------------------------------------------------------

/// `manifest.json` moved to a new epoch over committed bytes that did not move. The prefix hash
/// cannot see this: the bytes are the ones the handle verified. Only `resume`'s epoch clause can.
#[tokio::test(flavor = "multi_thread")]
async fn a_manifest_epoch_moved_under_unchanged_committed_bytes_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 1).await;
    let committed = fs::read(directory.path().join("events.jsonl")).unwrap();
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(directory.path().join("manifest.json")).unwrap()).unwrap();
    manifest["epoch"] = json!(manifest["epoch"].as_u64().unwrap() + 1);
    fs::write(
        directory.path().join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();

    let served = store.stream_version(&stream("a")).await;
    assert!(
        served.is_err(),
        "an epoch this handle never observed is not its history: {served:?}"
    );
    assert_eq!(
        fs::read(directory.path().join("events.jsonl")).unwrap(),
        committed,
        "the refusal did not touch the committed bytes"
    );
}

/// The same shape on the store identity.
#[tokio::test(flavor = "multi_thread")]
async fn a_manifest_store_identity_moved_under_unchanged_committed_bytes_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 1).await;
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(directory.path().join("manifest.json")).unwrap()).unwrap();
    manifest["store"] = json!("00000000-0000-4000-8000-000000000000");
    fs::write(
        directory.path().join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();

    let served = store.stream_version(&stream("a")).await;
    assert!(
        served.is_err(),
        "another store's identity over this store's bytes is refused: {served:?}"
    );
}

/// The same shape on the physical format string.
#[tokio::test(flavor = "multi_thread")]
async fn a_manifest_format_moved_under_unchanged_committed_bytes_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 1).await;
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(directory.path().join("manifest.json")).unwrap()).unwrap();
    manifest["format"] = json!("eventlog-file/2");
    fs::write(
        directory.path().join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();

    let served = store.stream_version(&stream("a")).await;
    assert!(
        served.is_err(),
        "an unknown physical format is refused, resumed or not: {served:?}"
    );
}

/// `docs/design/file-provider.md`: a pending recovery intent is one of the states that "falls back
/// to the complete reread". A resumed handle that walked past one would overwrite somebody else's
/// recovery authority with its own the next time it appended.
#[tokio::test(flavor = "multi_thread")]
async fn a_pending_append_intent_is_never_resumed_past() {
    let directory = tempfile::tempdir().unwrap();
    let store = seeded(directory.path(), 1).await;
    fs::write(directory.path().join("append.json"), b"{}").unwrap();

    let served = store.stream_version(&stream("a")).await;
    assert!(
        served.is_err(),
        "a published append intent is the complete opener's business, not a resumed handle's: \
         {served:?}"
    );
    assert!(
        directory.path().join("append.json").exists(),
        "the refusal left the intent where it found it"
    );
}

// ---------------------------------------------------------------------------------------------
// Area 3 — the acceptance property, asserted rather than timed.
//
// story `## Outcome`: "It re-parses no frame it has already folded and opens no blob it does not
// touch." That has to survive the handle having folded somebody else's frames once already: the
// running hash it carries afterwards covers the prefix *and* the tail it just chained, or every
// later transaction silently falls back to the complete reread this unit exists to remove.
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_handle_that_folded_fresh_frames_still_resumes_instead_of_rereading_everything() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    store.put_blob(&tenant(), "d", b"bytes").await.unwrap();

    let other = FileEventStore::open(directory.path()).await.unwrap();
    other
        .append(
            &stream("a"),
            Expected::NoStream,
            &[event("item.created", 1)],
            &meta("second-writer", &json!({})),
        )
        .await
        .unwrap();
    drop(other);

    // The advanced resume: this handle chains and folds the frame it has not seen.
    assert_eq!(store.stream_version(&stream("a")).await.unwrap(), Some(1));

    // Now make a complete reread impossible to survive. A handle that is still resuming opens no
    // blob it does not touch, so it does not notice; one that fell back re-hashes every active
    // object and fails.
    fs::remove_dir_all(directory.path().join("blobs")).unwrap();
    assert_eq!(
        store.stream_version(&stream("a")).await.unwrap(),
        Some(1),
        "a handle that folded fresh frames must resume again, not reread the whole store"
    );
}
