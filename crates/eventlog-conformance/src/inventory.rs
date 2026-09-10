use eventlog_core::{EventLogError, EventStore, Expected, StreamId, TenantId};
use serde_json::json;

/// Proves scoped committed inventory, paging and erasure without using a feed cursor.
///
/// # Panics
/// Names the first inventory contract a provider breaks.
pub async fn run_stream_inventory(store: &dyn EventStore) {
    let tenant = TenantId::new("inventory-a").unwrap();
    let other = TenantId::new("inventory-b").unwrap();
    for (owner, kind, id) in [
        (&tenant, "item", "z"),
        (&tenant, "item", "a"),
        (&tenant, "item", "m"),
        (&tenant, "other", "hidden"),
        (&other, "item", "foreign"),
    ] {
        let stream = StreamId::new(owner.clone(), kind, id).unwrap();
        store
            .append(
                &stream,
                Expected::NoStream,
                &[crate::event("item.created", 1)],
                &crate::meta(id, &json!({})),
            )
            .await
            .unwrap();
    }
    let first = store.list_streams(&tenant, "item", None, 2).await.unwrap();
    assert_eq!(
        first.iter().map(StreamId::stream_id).collect::<Vec<_>>(),
        ["a", "m"],
        "inventory is scoped and ordered, not append order"
    );
    assert!(
        first
            .iter()
            .all(|stream| stream.tenant() == &tenant && stream.stream_type() == "item")
    );
    let last = store
        .list_streams(&tenant, "item", Some("m"), 2)
        .await
        .unwrap();
    assert_eq!(
        last.iter().map(StreamId::stream_id).collect::<Vec<_>>(),
        ["z"]
    );
    assert!(
        store
            .list_streams(&tenant, "item", Some("z"), 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_streams(&tenant, "missing", None, 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .list_streams(&tenant, "item", None, 0)
            .await
            .unwrap()
            .len(),
        1,
        "zero follows the shared bounded-read limit"
    );
    let stream = &first[0];
    store
        .append(
            stream,
            Expected::Exact(1),
            &[crate::event("item.changed", 2)],
            &crate::meta("next", &json!({})),
        )
        .await
        .unwrap();
    store.redact(stream, 1, "test-redaction").await.unwrap();
    assert_eq!(
        store
            .list_streams(&tenant, "item", None, 10)
            .await
            .unwrap()
            .len(),
        3,
        "versions and tombstones are not extra streams"
    );
    assert!(matches!(
        store.list_streams(&tenant, "", None, 2).await.unwrap_err(),
        EventLogError::Invalid(_)
    ));
    assert!(matches!(
        store
            .list_streams(&tenant, "item", Some(""), 2)
            .await
            .unwrap_err(),
        EventLogError::Invalid(_)
    ));
    store.forget_tenant(&tenant).await.unwrap();
    assert!(
        store
            .list_streams(&tenant, "item", None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .list_streams(&other, "item", None, 10)
            .await
            .unwrap()
            .len(),
        1,
        "erasure stays tenant-scoped"
    );
}
