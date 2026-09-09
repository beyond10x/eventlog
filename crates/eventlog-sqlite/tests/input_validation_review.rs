//! Constructor invariants must also hold at the public deserialization/append boundary.
use eventlog_core::{Claim, EventLogError, EventStore, Expected, NewEvent, StreamId, TenantId};
use eventlog_sqlite::SqliteEventStore;
use serde_json::json;

#[tokio::test]
async fn append_refuses_deserialized_events_that_bypass_constructor_checks() {
    let store = SqliteEventStore::in_memory("invalid_event").await.unwrap();
    let tenant = TenantId::new("tenant").unwrap();
    for (index, wire) in [
        json!({"name": "item.received", "schema_version": 1, "data": 7}),
        json!({"name": "", "schema_version": 1, "data": {}}),
        json!({"name": "bad\nname", "schema_version": 1, "data": {}}),
    ]
    .into_iter()
    .enumerate()
    {
        let event = serde_json::from_value::<NewEvent>(wire);
        let Ok(event) = event else { continue };
        let stream = StreamId::new(tenant.clone(), "item", format!("{index}")).unwrap();
        let result = store
            .append(
                &stream,
                Expected::NoStream,
                &[event],
                &eventlog_conformance::meta("command", &json!({})),
            )
            .await;
        assert!(
            matches!(result, Err(EventLogError::Invalid(_))),
            "constructor-invalid event was appended: {result:?}"
        );
        assert_eq!(store.stream_version(&stream).await.unwrap(), None);
    }
}

#[tokio::test]
async fn append_refuses_deserialized_streams_that_bypass_constructor_checks() {
    let store = SqliteEventStore::in_memory("invalid_stream").await.unwrap();
    for wire in [
        json!({"tenant": "", "stream_type": "item", "stream_id": "one"}),
        json!({"tenant": "tenant", "stream_type": "", "stream_id": "one"}),
        json!({"tenant": "tenant", "stream_type": "item", "stream_id": ""}),
    ] {
        let stream = serde_json::from_value::<StreamId>(wire);
        let Ok(stream) = stream else { continue };
        let result = store
            .append(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("command", &json!({})),
            )
            .await;
        assert!(
            matches!(result, Err(EventLogError::Invalid(_))),
            "constructor-invalid stream was appended: {result:?}"
        );
    }
}

#[tokio::test]
async fn append_refuses_claim_fields_that_bypass_constructor_checks() {
    let store = SqliteEventStore::in_memory("invalid_claim").await.unwrap();
    let tenant = TenantId::new("tenant").unwrap();
    for (index, claim) in [
        Claim {
            scope: String::new(),
            key: "key".to_owned(),
            digest: "digest".to_owned(),
        },
        Claim {
            scope: "scope".to_owned(),
            key: String::new(),
            digest: "digest".to_owned(),
        },
        Claim {
            scope: "scope".to_owned(),
            key: "key".to_owned(),
            digest: String::new(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let stream = StreamId::new(tenant.clone(), "item", format!("{index}")).unwrap();
        let mut meta = eventlog_conformance::meta("command", &json!({}));
        meta.claim = Some(claim);
        let result = store
            .append(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &meta,
            )
            .await;
        assert!(
            matches!(result, Err(EventLogError::Invalid(_))),
            "constructor-invalid claim was appended: {result:?}"
        );
        assert_eq!(store.stream_version(&stream).await.unwrap(), None);
    }
}

#[tokio::test]
async fn public_input_validation_is_atomic_on_sqlite() {
    let store: std::sync::Arc<dyn EventStore> =
        std::sync::Arc::new(SqliteEventStore::in_memory("input_atomic").await.unwrap());
    eventlog_conformance::run_public_input_validation(&store).await;
}
