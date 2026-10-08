//! Security review of `story:file-eventlog-rechecks-its-prefix-by-comparison`: its safety fact.
//!
//! The story rests on one fact, stated at level 2: the SHA-256 the unit removed existed only in
//! memory, and the persisted digests — every frame's `digest` and `previous`, the manifest's
//! `digest`, and a privacy intent's `replacement_digest` — are SHA-256 over other bytes, which the
//! unit does not touch. These cases recompute those digests here, independently of the crate, from
//! the bytes on disk, after every path the unit changed has run: the opener, the handle's own
//! appends (which now extend a shared buffer), a capture (which shares it), and a privacy rewrite
//! (which replaces it). A privacy intent is then written by hand with a `replacement_digest`
//! computed here, and the opener must complete it — and must refuse one whose digest is not
//! SHA-256 over the replacement bytes.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    CaptureLimits, ConsistentTenantCapture, EventLogError, EventStore, Expected, StreamId, TenantId,
};
use eventlog_file::FileEventStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

const ZERO: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// `eventlog-file/1`'s frame, field for field and in the same order, read from the format rather
/// than from the crate's private type.
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

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn tenant() -> TenantId {
    TenantId::new("persisted-owner").unwrap()
}

fn stream() -> StreamId {
    StreamId::new(tenant(), "item", "one").unwrap()
}

/// The tenant the captures observe. Not the redacted one: a capture of a tenant whose history was
/// redacted refuses by design (`CaptureError::RedactedHistory`).
fn observed() -> TenantId {
    TenantId::new("persisted-observed").unwrap()
}

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 64,
        max_blobs: 64,
        max_projection_rows: 64,
        max_payload_bytes: 65_536,
    }
}

fn manifest(root: &Path) -> Value {
    serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap()
}

/// Recompute the committed history's digests from its bytes and require the persisted ones to be
/// exactly them: each frame's digest is SHA-256 over the canonical frame with an empty digest, each
/// frame names the one before it, the committed bytes are the canonical frames, and the manifest
/// commits the last digest at the file's length. Returns the transactions, in order.
fn verified_chain(root: &Path, label: &str) -> Vec<Value> {
    let manifest = manifest(root);
    let bytes = fs::read(root.join("events.jsonl")).unwrap();
    assert_eq!(
        bytes.len() as u64,
        manifest["length"].as_u64().unwrap(),
        "{label}: the manifest commits the file's length"
    );
    let mut previous = ZERO.to_owned();
    let mut transactions = Vec::new();
    for (index, line) in bytes.split_inclusive(|byte| *byte == b'\n').enumerate() {
        let frame: Frame = serde_json::from_slice(line).unwrap();
        assert_eq!(
            frame.format, "eventlog-file/1",
            "{label}: frame {index} format"
        );
        assert_eq!(
            frame.store, manifest["store"],
            "{label}: frame {index} store"
        );
        assert_eq!(
            frame.epoch, manifest["epoch"],
            "{label}: frame {index} epoch"
        );
        assert_eq!(
            frame.sequence,
            index as u64 + 1,
            "{label}: frame {index} sequence"
        );
        assert_eq!(frame.previous, previous, "{label}: frame {index} chains");
        let mut bare = frame.clone();
        bare.digest = String::new();
        assert_eq!(
            sha256(&serde_json::to_vec(&bare).unwrap()),
            frame.digest,
            "{label}: frame {index}'s digest is SHA-256 over its canonical bytes"
        );
        let mut canonical = serde_json::to_vec(&frame).unwrap();
        canonical.push(b'\n');
        assert_eq!(
            canonical, line,
            "{label}: frame {index} is committed as its canonical bytes"
        );
        previous = frame.digest;
        transactions.push(frame.transaction);
    }
    assert_eq!(
        manifest["sequence"].as_u64().unwrap(),
        transactions.len() as u64,
        "{label}: the manifest commits every frame"
    );
    assert_eq!(
        manifest["digest"], previous,
        "{label}: the manifest commits the last frame's digest"
    );
    transactions
}

/// Encode `transactions` as a complete history of `store` at `epoch`, the way the format defines.
fn encoded(store: &str, epoch: u64, transactions: &[Value]) -> (Vec<u8>, String) {
    let mut bytes = Vec::new();
    let mut previous = ZERO.to_owned();
    for (index, transaction) in transactions.iter().enumerate() {
        let mut frame = Frame {
            format: "eventlog-file/1".into(),
            store: store.into(),
            epoch,
            sequence: index as u64 + 1,
            previous: previous.clone(),
            transaction: transaction.clone(),
            digest: String::new(),
        };
        frame.digest = sha256(&serde_json::to_vec(&frame).unwrap());
        bytes.extend(serde_json::to_vec(&frame).unwrap());
        bytes.push(b'\n');
        previous = frame.digest;
    }
    (bytes, previous)
}

async fn append(store: &FileEventStore, version: u64, value: i64) {
    store
        .append(
            &stream(),
            if version == 0 {
                Expected::NoStream
            } else {
                Expected::Exact(version)
            },
            &[event("item.changed", value)],
            &meta(&format!("persisted-{value}"), &json!({})),
        )
        .await
        .unwrap();
}

/// Every path the unit changed leaves frames whose persisted digests are SHA-256 over the canonical
/// frame, exactly as `eventlog-file/1` defines them.
#[tokio::test(flavor = "multi_thread")]
async fn persisted_frame_digests_are_sha256_over_canonical_frames_on_every_changed_path() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = FileEventStore::open(root).await.unwrap();
    store.stream_identity(&observed()).await.unwrap();
    append(&store, 0, 1).await;
    verified_chain(root, "after the first append");

    // A capture shares the writer's buffer; the appends after it extend that buffer in place.
    store
        .capture_tenant(&observed(), &[], limits())
        .await
        .unwrap();
    for version in 1..4 {
        append(&store, version, i64::try_from(version).unwrap() + 1).await;
        store.read_stream(&stream(), 0, 10).await.unwrap();
    }
    store
        .capture_tenant(&observed(), &[], limits())
        .await
        .unwrap();
    verified_chain(root, "after a capture and own appends");

    // A privacy rewrite replaces the history with a new epoch and the handle's bytes with new ones.
    let epoch = manifest(root)["epoch"].as_u64().unwrap();
    store.redact(&stream(), 2, "privacy").await.unwrap();
    assert_eq!(
        manifest(root)["epoch"].as_u64().unwrap(),
        epoch + 1,
        "the redaction rewrote the history into a new epoch"
    );
    verified_chain(root, "after a privacy rewrite");
    append(&store, 4, 5).await;
    store
        .capture_tenant(&observed(), &[], limits())
        .await
        .unwrap();
    verified_chain(root, "after appending to the rewritten epoch");
}

/// Write the durable state a privacy rewrite leaves between preparing its intent and committing
/// it: the replacement history in `privacy.next` and the intent binding it by `replacement_digest`.
fn prepare_privacy(root: &Path, replacement_digest: impl FnOnce(&[u8]) -> String) -> Vec<u8> {
    let transactions = verified_chain(root, "before the prepared rewrite");
    let before = manifest(root);
    let store = before["store"].as_str().unwrap().to_owned();
    let epoch = before["epoch"].as_u64().unwrap() + 1;
    let (bytes, digest) = encoded(&store, epoch, &transactions);
    let after = json!({
        "format": "eventlog-file/1",
        "store": store,
        "epoch": epoch,
        "sequence": transactions.len(),
        "length": bytes.len(),
        "digest": digest,
    });
    fs::write(root.join("privacy.next"), &bytes).unwrap();
    let intent = json!({
        "before": before,
        "after": after,
        "replacement_digest": replacement_digest(&bytes),
    });
    fs::write(
        root.join("privacy.json"),
        serde_json::to_vec(&intent).unwrap(),
    )
    .unwrap();
    bytes
}

/// A prepared privacy intent whose `replacement_digest` is SHA-256 over the raw replacement bytes,
/// computed here, is the one the opener completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_privacy_intent_is_bound_by_sha256_over_the_raw_replacement_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    {
        let store = FileEventStore::open(root).await.unwrap();
        for version in 0..3 {
            append(&store, version, i64::try_from(version).unwrap() + 10).await;
        }
    }
    let replacement = prepare_privacy(root, sha256);
    let store = FileEventStore::open(root)
        .await
        .expect("the opener completes an intent bound by SHA-256 over the replacement bytes");
    assert_eq!(
        fs::read(root.join("events.jsonl")).unwrap(),
        replacement,
        "the replacement became the committed history"
    );
    assert!(
        !root.join("privacy.json").exists(),
        "the intent was retired"
    );
    verified_chain(root, "after the opener completed the rewrite");
    let page = store.read_stream(&stream(), 0, 10).await.unwrap();
    assert_eq!(page.events.len(), 3, "the rewritten history still serves");
}

/// The same intent bound by any other digest is refused, and nothing is replaced.
#[tokio::test(flavor = "multi_thread")]
async fn a_privacy_intent_bound_by_another_digest_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    {
        let store = FileEventStore::open(root).await.unwrap();
        for version in 0..3 {
            append(&store, version, i64::try_from(version).unwrap() + 20).await;
        }
    }
    let committed = fs::read(root.join("events.jsonl")).unwrap();
    // SHA-256 over the replacement with its last byte left off: not the replacement's digest.
    prepare_privacy(root, |bytes| sha256(&bytes[..bytes.len() - 1]));
    let opened = FileEventStore::open(root).await.map(|_| ());
    assert!(
        matches!(&opened, Err(EventLogError::Backend(message)) if message.contains("integrity")),
        "an intent whose digest is not SHA-256 over the replacement was completed: {opened:?}"
    );
    assert_eq!(
        fs::read(root.join("events.jsonl")).unwrap(),
        committed,
        "a refused intent replaced the committed history"
    );
}
