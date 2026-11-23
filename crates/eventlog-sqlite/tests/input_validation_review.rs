//! Constructor invariants must also hold at the public deserialization/append boundary.
use eventlog_core::{
    BoxFuture, CaptureLimits, Claim, ConsistentTenantCapture, EventLogError, EventStore, Expected,
    NewEvent, ProjectionSpec, ProjectionStore, Projector, RecordedEvent, StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::Connection;
use serde_json::json;
use std::sync::Arc;

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

/// A projector over whatever declarations a host assembled, apart from any registration.
struct Declared {
    name: &'static str,
    specs: &'static [ProjectionSpec],
}

impl Projector for Declared {
    fn name(&self) -> &'static str {
        self.name
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        self.specs
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            store
                .upsert(
                    &self.specs[0],
                    &event.tenant,
                    &event.stream_id,
                    &json!({"kind": "book"}),
                )
                .await
        })
    }
}

fn invalid(result: Result<impl std::fmt::Debug, EventLogError>) -> String {
    match result {
        Err(EventLogError::Invalid(reason)) => reason,
        other => panic!("expected a refused declaration, got {other:?}"),
    }
}

const REPEATED: ProjectionSpec = ProjectionSpec {
    name: "by_kind",
    indexed: &["kind", "kind"],
};
const CORRECTED: ProjectionSpec = ProjectionSpec {
    name: "by_kind",
    indexed: &["kind"],
};
const TWO_FIELDS: ProjectionSpec = ProjectionSpec {
    name: "by_kind",
    indexed: &["kind", "shelf"],
};

#[tokio::test]
async fn registration_refuses_a_repeated_indexed_field_capture_cannot_read() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.sqlite3");
    let store = SqliteEventStore::open(path.to_str().unwrap(), "repeated")
        .await
        .unwrap();
    let expected = "indexed field kind is declared twice in projection by_kind";
    let inline = Arc::new(Declared {
        name: "by_kind",
        specs: std::slice::from_ref(&REPEATED),
    });
    assert_eq!(
        invalid(store.register_inline(inline.clone()).await),
        expected
    );
    assert_eq!(invalid(store.create_projections(inline).await), expected);
    let raw = Connection::open(&path).unwrap();
    let (registered, tables): (i64, i64) = raw
        .query_row(
            "SELECT (SELECT COUNT(*) FROM repeated_projection_registry),
                    (SELECT COUNT(*) FROM sqlite_master WHERE name = 'repeated_p_by_kind')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (registered, tables),
        (0, 0),
        "a refused declaration left a registry row or a table behind"
    );
}

#[tokio::test]
async fn a_persisted_repeated_declaration_names_its_repair_and_the_repair_captures() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.sqlite3");
    let tenant = TenantId::new("tenant").unwrap();
    {
        // What a release up to 0.9.4 left: a two-column table registered as ["kind","kind"].
        let store = SqliteEventStore::open(path.to_str().unwrap(), "repeated")
            .await
            .unwrap();
        store
            .create_projections(Arc::new(Declared {
                name: "by_kind",
                specs: std::slice::from_ref(&TWO_FIELDS),
            }))
            .await
            .unwrap();
    }
    let raw = Connection::open(&path).unwrap();
    raw.execute_batch(
        r#"UPDATE repeated_projection_registry SET indexed_fields = '["kind","kind"]'
           WHERE projection_name = 'by_kind'"#,
    )
    .unwrap();
    let corrected = || {
        Arc::new(Declared {
            name: "by_kind",
            specs: std::slice::from_ref(&CORRECTED),
        })
    };
    let store = SqliteEventStore::open(path.to_str().unwrap(), "repeated")
        .await
        .unwrap();
    assert_eq!(
        invalid(store.register_inline(corrected()).await),
        r#"projection by_kind is persisted with indexed fields ["kind","kind"], which repeat a field capture cannot read; drop table repeated_p_by_kind and its repeated_projection_registry row, then register the corrected declaration and rebuild the projection"#
    );
    drop(store);

    raw.execute_batch(
        "DROP TABLE repeated_p_by_kind;
         DELETE FROM repeated_projection_registry WHERE projection_name = 'by_kind';",
    )
    .unwrap();
    let store = SqliteEventStore::open(path.to_str().unwrap(), "repeated")
        .await
        .unwrap();
    store.stream_identity(&tenant).await.unwrap();
    store.register_inline(corrected()).await.unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    store
        .append(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("command", &json!({})),
        )
        .await
        .unwrap();
    let limits = CaptureLimits {
        max_events: 16,
        max_blobs: 16,
        max_projection_rows: 16,
        max_payload_bytes: 1 << 16,
    };
    let capture = store
        .capture_tenant(&tenant, std::slice::from_ref(&CORRECTED), limits)
        .await
        .expect("the repaired projection is captured");
    assert_eq!(
        capture.projections[0].rows,
        vec![("one".to_owned(), json!({"kind": "book"}))]
    );
}

