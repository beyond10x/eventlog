//! Adversary cases for `EVENTLOG-BLOB-BATCH`: concurrent handles, lazy batch-table creation
//! under concurrent `open_existing` owners, and tamper shapes the unit's own cases leave out.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, EventLogError, EventStore, Expected, NoGuard, StreamAppend,
    StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use std::sync::Arc;

const PREFIX: &str = "adv";

fn tenant() -> TenantId {
    TenantId::new("adv-batch").unwrap()
}

fn group(key: &str, stream: &str) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: meta(key, &serde_json::json!({})),
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant(), "item", stream).unwrap(),
            expected: Expected::NoStream,
            events: vec![event("item.created", 1)],
        }],
    }
}

fn batch(tag: &str) -> Vec<(String, Vec<u8>)> {
    (0..4)
        .map(|index| {
            (
                format!("{tag}-{index}"),
                format!("{tag}-bytes-{index}").into_bytes(),
            )
        })
        .collect()
}

/// Two handles on one file race the same group and batch 16 times: exactly one call commits
/// each group, the other deduplicates, and every stream holds one event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_handles_racing_one_guarded_batch_commit_it_once() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("race.sqlite3");
    let path = path.to_str().unwrap().to_owned();
    let one = Arc::new(SqliteEventStore::open(&path, PREFIX).await.unwrap());
    let two = Arc::new(SqliteEventStore::open(&path, PREFIX).await.unwrap());
    for round in 0..16 {
        let key = format!("race-{round}");
        let blobs = batch(&key);
        let calls = [Arc::clone(&one), Arc::clone(&two)].map(|store| {
            let key = key.clone();
            let blobs = blobs.clone();
            tokio::spawn(async move {
                store
                    .append_group_guarded_with_blobs(&group(&key, &key), Arc::new(NoGuard), &blobs)
                    .await
            })
        });
        let mut fresh = 0;
        for call in calls {
            match call.await.unwrap() {
                Ok(result) if !result.deduplicated => fresh += 1,
                Ok(_) => {}
                Err(error) => panic!("{key}: a racing handle was refused: {error:?}"),
            }
        }
        assert_eq!(fresh, 1, "{key}: exactly one racing call commits");
        assert_eq!(
            one.stream_version(&StreamId::new(tenant(), "item", key.clone()).unwrap())
                .await
                .unwrap(),
            Some(1)
        );
        for (digest, bytes) in &blobs {
            assert_eq!(
                two.get_blob(&tenant(), digest).await.unwrap().as_ref(),
                Some(bytes)
            );
        }
    }
}

/// An owner provisioned before the batch table, opened by two `open_existing` handles that each
/// commit their first guarded batch concurrently: the lazy `CREATE TABLE` inside each
/// `BEGIN IMMEDIATE` must not refuse either, and each handle's retry must see the other's record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_existing_owners_create_the_batch_table_lazily_without_refusal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lazy.sqlite3");
    let path = path.to_str().unwrap().to_owned();
    drop(SqliteEventStore::open(&path, PREFIX).await.unwrap());
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(&format!("DROP TABLE {PREFIX}_group_batches"))
        .unwrap();
    let one = Arc::new(
        SqliteEventStore::open_existing(&path, PREFIX)
            .await
            .unwrap(),
    );
    let two = Arc::new(
        SqliteEventStore::open_existing(&path, PREFIX)
            .await
            .unwrap(),
    );
    let calls = [("lazy-a", Arc::clone(&one)), ("lazy-b", Arc::clone(&two))].map(|(key, store)| {
        tokio::spawn(async move {
            store
                .append_group_guarded_with_blobs(&group(key, key), Arc::new(NoGuard), &batch(key))
                .await
        })
    });
    for call in calls {
        let result = call
            .await
            .unwrap()
            .expect("lazy creation refused a concurrent owner");
        assert!(!result.deduplicated);
    }
    for (key, store) in [("lazy-a", &two), ("lazy-b", &one)] {
        assert!(
            store
                .append_group_guarded_with_blobs(&group(key, key), Arc::new(NoGuard), &batch(key))
                .await
                .expect("the other handle's record is visible")
                .deduplicated
        );
        let other = store
            .append_group_guarded_with_blobs(&group(key, key), Arc::new(NoGuard), &batch("other"))
            .await
            .expect_err("a different batch under a recorded key");
        assert!(
            matches!(other, EventLogError::IdempotencyMismatch { .. }),
            "{other:?}"
        );
    }
}

/// A verified row deleted and re-inserted by a second handle with other bytes under the same
/// digest and a consistent new hash reads the new bytes (the base validator accepts a consistent
/// row too); re-inserted with the *old* hash over new bytes it is refused on the first handle.
#[tokio::test]
async fn a_row_rebound_through_another_handle_is_validated_from_what_is_stored_now() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rebind.sqlite3");
    let path = path.to_str().unwrap().to_owned();
    let reader = SqliteEventStore::open(&path, PREFIX).await.unwrap();
    reader
        .append_group_guarded_with_blobs(&group("rebind", "rebind"), Arc::new(NoGuard), &batch("r"))
        .await
        .unwrap();
    for _ in 0..3 {
        assert_eq!(
            reader.get_blob(&tenant(), "r-0").await.unwrap(),
            Some(b"r-bytes-0".to_vec())
        );
    }
    let writer = SqliteEventStore::open(&path, PREFIX).await.unwrap();
    writer.delete_blob(&tenant(), "r-0").await.unwrap();
    writer
        .put_blob(&tenant(), "r-0", b"rebound!!")
        .await
        .unwrap();
    assert_eq!(
        reader.get_blob(&tenant(), "r-0").await.unwrap(),
        Some(b"rebound!!".to_vec()),
        "the first handle returns what is stored now, not what it remembered"
    );
    let old_hash = eventlog_core::blob_integrity_sha256(b"r-bytes-1");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            &format!(
                "UPDATE {PREFIX}_blobs SET bytes=?1, byte_count=9, integrity_sha256=?2 \
                 WHERE digest='r-1'"
            ),
            rusqlite::params![b"r-bytes-X".as_slice(), old_hash],
        )
        .unwrap();
    assert!(
        matches!(
            reader.get_blob(&tenant(), "r-1").await,
            Err(EventLogError::Backend(_))
        ),
        "new bytes under the remembered hash are refused"
    );
}
