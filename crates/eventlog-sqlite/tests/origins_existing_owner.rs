//! An owner provisioned before the `<prefix>_origins` table existed, reopened the way Entity
//! Runtime reopens an authority (`open_existing`, which never creates a table), as the target of a
//! copy: `RestoredEvent.origin` must land, or at least a refused append must change nothing.

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_sqlite::{EventOrigin, RestoredEvent, SqliteEventStore};
use rusqlite::Connection;
use serde_json::json;
use time::OffsetDateTime;

/// A file owner as eventlog 0.4.0 provisioned it: every table but the origin map.
async fn pre_origin_owner(directory: &tempfile::TempDir) -> String {
    let path = directory.path().join("authority.sqlite3");
    let name = path.to_str().unwrap().to_owned();
    drop(SqliteEventStore::open(&name, "owner").await.unwrap());
    Connection::open(&name)
        .unwrap()
        .execute_batch("DROP TABLE owner_origins")
        .unwrap();
    name
}

fn restored(id: &str, version: u64) -> RestoredEvent {
    RestoredEvent {
        event_id: id.to_owned(),
        recorded_at: OffsetDateTime::UNIX_EPOCH,
        origin: Some(EventOrigin {
            version,
            digest: format!("digest-{version}"),
            parents: Vec::new(),
        }),
    }
}

fn stream() -> StreamId {
    StreamId::new(TenantId::new("tenant-a").unwrap(), "item", "x").unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copied_event_appends_with_its_origin_into_an_owner_provisioned_before_origins() {
    let directory = tempfile::tempdir().unwrap();
    let name = pre_origin_owner(&directory).await;
    let store = SqliteEventStore::open_existing(&name, "owner")
        .await
        .expect("a 0.4.0 owner reopens");
    store.restore_events(vec![restored("copied-1", 1)]).unwrap();
    store
        .append(
            &stream(),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("key-1", &json!({})),
        )
        .await
        .expect("a copied event could not be appended into a pre-origin owner");
    assert_eq!(
        store
            .origins(&TenantId::new("tenant-a").unwrap())
            .await
            .unwrap()
            .len(),
        1,
        "the copied event's origin was not recorded"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_restored_append_leaves_the_restored_queue_as_it_was() {
    let directory = tempfile::tempdir().unwrap();
    let name = pre_origin_owner(&directory).await;
    let store = SqliteEventStore::open_existing(&name, "owner")
        .await
        .unwrap();
    store
        .restore_events(vec![restored("copied-1", 1), restored("copied-2", 2)])
        .unwrap();
    let outcome = store
        .append(
            &stream(),
            Expected::NoStream,
            &[event("item.received", 1), event("item.indexed", 2)],
            &meta("key-1", &json!({})),
        )
        .await;
    if outcome.is_ok() {
        return; // the origin table was found or made; nothing was refused.
    }
    assert_eq!(
        store.restored_pending().unwrap(),
        2,
        "a refused append consumed part of the restored queue ({outcome:?}); the next append \
         on this handle takes a copied event's id and origin"
    );
}
