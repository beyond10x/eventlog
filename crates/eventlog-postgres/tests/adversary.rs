//! Pass-one regressions against the frozen production-proof candidate.
use eventlog_core::{
    BoxFuture, EventLogError, EventStore, Expected, Guard, ProjectionSpec, ProjectionStore,
    Projector, RecordedEvent, StreamId, TenantId,
};
use eventlog_postgres::PostgresEventStore;
use std::{sync::Arc, time::Duration};
use tokio_postgres::NoTls;

fn prefix(label: &str) -> String {
    let suffix: String = time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .to_string()
        .bytes()
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    format!("adv_{label}_{suffix}")
}

async fn sql() -> tokio_postgres::Client {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL");
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.expect("fixture");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

const ASSIGNMENT: ProjectionSpec = ProjectionSpec {
    name: "assignment",
    indexed: &[],
};
struct Assignment;
impl Projector for Assignment {
    fn name(&self) -> &'static str {
        "assignment"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&ASSIGNMENT)
    }
    fn apply<'a>(
        &'a self,
        _: &'a RecordedEvent,
        _: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Ok(()) })
    }
}
struct AssignOlderXid {
    tenant: TenantId,
    assigned: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}
impl Guard for AssignOlderXid {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            store
                .upsert(
                    &ASSIGNMENT,
                    &self.tenant,
                    "guard",
                    &serde_json::json!({"held":1}),
                )
                .await?;
            self.assigned.notify_one();
            self.resume.notified().await;
            Ok(())
        })
    }
}

#[tokio::test]
async fn unrelated_xmin_cannot_make_committed_positions_noncontiguous() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL");
        let prefix = prefix("xmin");
        let older = Arc::new(PostgresEventStore::connect(&url, &prefix).await.expect("older writer"));
        older.create_projections(Arc::new(Assignment)).await.expect("guard view");
        let newer = PostgresEventStore::connect(&url, &prefix).await.expect("independent newer writer");
        let tenant = TenantId::new("xmin-tenant").expect("tenant");
        let assigned = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let guard = Arc::new(AssignOlderXid { tenant: tenant.clone(), assigned: assigned.clone(), resume: resume.clone() });
        let writer = older.clone();
        let writer_tenant = tenant.clone();
        let first = tokio::spawn(async move {
            writer.append_guarded(&StreamId::new(writer_tenant, "item", "older").expect("stream"), Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)], &eventlog_conformance::meta("older", &serde_json::json!({})), guard).await
        });
        assigned.notified().await;
        // An ordinary transaction in another owner allocates an XID between the two append XIDs.
        // It never touches Eventlog's namespace or publication locks.
        let mut outsider = sql().await;
        let transaction = outsider.transaction().await.expect("unrelated transaction");
        let middle: String = transaction.query_one("SELECT pg_current_xact_id()::text", &[]).await.expect("middle XID").get(0);
        let lower = newer.append(&StreamId::new(tenant.clone(), "item", "newer").expect("stream"), Expected::NoStream,
            &[eventlog_conformance::event("item.received", 2)], &eventlog_conformance::meta("newer", &serde_json::json!({}))).await.expect("lower position committed");
        resume.notify_one();
        let higher = first.await.expect("older task").expect("higher position committed");
        assert!(lower.events[0].global_seq < higher.events[0].global_seq);
        let observer = sql().await;
        let rows = observer.query(&format!("SELECT global_seq,committed_xid::text FROM {prefix}_events ORDER BY global_seq"), &[]).await.expect("observed XID order");
        let positions: Vec<_> = rows.iter().map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1).parse::<u64>().expect("XID"))).collect();
        let middle = middle.parse::<u64>().expect("middle XID");
        assert!(positions[1].1 < middle && middle < positions[0].1, "fixture XID inversion: {positions:?}, middle={middle}");
        let early = newer.read_feed(&tenant, 0, 10).await.expect("feed while unrelated XID is open");
        // Always release the unrelated transaction before assertions, including the regression red.
        transaction.commit().await.expect("release unrelated xmin");
        let resumed = newer.read_feed(&tenant, early.next_position, 10).await.expect("resume committed cursor");
        let all = newer.read_feed(&tenant, 0, 10).await.expect("all committed facts");
        let delivered: Vec<_> = early.events.iter().chain(&resumed.events).map(|event| event.global_seq).collect();
        let expected: Vec<_> = all.events.iter().map(|event| event.global_seq).collect();
        eprintln!("positions_and_xids={positions:?}; unrelated_xid={middle}; early_cursor={}; early_count={}; resumed_count={}; all_count={}", early.next_position, early.events.len(), resumed.events.len(), all.events.len());
        assert_eq!(delivered, expected, "a committed lower position became unreachable from the returned feed cursor");
    }).await.expect("bounded ordering fixture");
}

#[tokio::test]
async fn schema_admission_refuses_inherited_event_children() {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL");
    let prefix = prefix("inh");
    let setup = PostgresEventStore::connect(&url, &prefix)
        .await
        .expect("initial schema");
    let tenant = TenantId::new("inheritance-tenant").expect("tenant");
    let stream = StreamId::new(tenant, "item", "one").expect("stream");
    setup
        .append(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("one", &serde_json::json!({})),
        )
        .await
        .expect("one original event");
    let sql = sql().await;
    sql.batch_execute(&format!("CREATE TABLE {prefix}_child () INHERITS ({prefix}_events); INSERT INTO {prefix}_child SELECT * FROM ONLY {prefix}_events")).await.expect("unsupported inheritance shape and duplicate child row");
    let reopened = PostgresEventStore::connect(&url, &prefix).await;
    if let Ok(ref store) = reopened {
        let read = store
            .read_stream(&stream, 0, 10)
            .await
            .expect("admitted inherited read");
        eprintln!(
            "admission=accepted; stream_event_count={}; duplicate_versions={:?}",
            read.events.len(),
            read.events
                .iter()
                .map(|event| event.version)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            read.events.len(),
            2,
            "the constructed inheritance changes the public reader"
        );
    }
    assert!(
        reopened.is_err(),
        "schema admission accepted a non-partition inheritance child that bypasses the events unique constraints"
    );
}
