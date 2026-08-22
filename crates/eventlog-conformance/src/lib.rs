#![forbid(unsafe_code)]

//! One body of assertions every backend must pass.
//!
//! This exercise is the definition of correct behaviour; the backends are implementation details
//! that agree with it. It exists because the only divergence ever found between Daemonloom store
//! backends was found by an exercise like this one and not by review.

use eventlog_core::{
    CommandMeta, EventLogError, EventStore, Expected, NewEvent, Snapshot, StreamId, TenantId,
    request_hash,
};
use serde_json::json;
use time::OffsetDateTime;

/// Build a command meta for the exercise.
///
/// # Panics
/// Panics when the fixture body cannot be hashed, which would mean `serde_json` is broken.
#[must_use]
pub fn meta(key: &str, body: &serde_json::Value) -> CommandMeta {
    CommandMeta {
        idempotency_key: key.to_owned(),
        request_hash: request_hash(body).expect("fixture body is hashable"),
        subject: "person-1".to_owned(),
        actor: "service-1".to_owned(),
        request_id: format!("request-{key}"),
        trace_id: format!("trace-{key}"),
        causation_id: None,
        causation_depth: 0,
        occurred_at: OffsetDateTime::UNIX_EPOCH,
    }
}

/// Build an event for the exercise.
///
/// # Panics
/// Panics when the fixture event is not constructible, which would mean the core validation
/// changed under this exercise.
#[must_use]
pub fn event(name: &str, value: i64) -> NewEvent {
    NewEvent::new(name, 1, json!({ "value": value })).expect("fixture event is valid")
}

/// Run every assertion against one backend.
///
/// # Panics
/// Panics with the failing assertion. This is a test exercise; a backend that does not pass it is
/// not usable and the panic names which promise it broke.
pub fn run(store: &dyn EventStore) {
    let tenant = TenantId::new("tenant-a").expect("valid tenant");
    let other = TenantId::new("tenant-b").expect("valid tenant");
    let stream = StreamId::new(tenant.clone(), "item", "item-1").expect("valid stream");
    let twin = StreamId::new(other.clone(), "item", "item-1").expect("valid stream");

    a_new_stream_starts_at_one(store, &stream);
    a_stream_reads_back_in_order(store, &stream);
    the_expected_version_is_enforced(store, &stream);
    a_repeated_command_is_not_a_second_write(store, &stream);
    a_reused_key_with_a_different_body_is_refused(store, &stream);
    a_command_that_decided_nothing_is_refused(store, &stream);
    the_same_stream_id_under_another_tenant_is_another_stream(store, &twin);
    the_feed_is_resumable(store, &tenant);
    the_feed_shows_one_tenant_only(store, &tenant, &other);
    a_snapshot_round_trips(store, &stream);
    bytes_live_outside_the_log_and_can_be_erased_alone(store, &tenant);
    a_stream_identity_is_stable(store, &tenant, &other);
    redaction_keeps_the_place_and_drops_the_snapshot(store, &stream);
    forgetting_a_tenant_leaves_nothing(store, &other, &twin);
}

fn a_new_stream_starts_at_one(store: &dyn EventStore, stream: &StreamId) {
    assert_eq!(
        store.stream_version(stream).expect("readable"),
        None,
        "a stream nobody wrote to has no version"
    );
    let body = json!({ "command": "create" });
    let result = store
        .append(
            stream,
            Expected::NoStream,
            &[event("item.received", 1), event("item.extracted", 2)],
            &meta("key-1", &body),
        )
        .expect("append to a new stream");
    assert_eq!(result.first_version, 1);
    assert_eq!(result.last_version, 2);
    assert!(!result.deduplicated);
    assert_eq!(result.events.len(), 2);
    assert_eq!(result.events[0].version, 1);
    assert_eq!(result.events[1].version, 2);
    assert_ne!(
        result.events[0].event_id, result.events[1].event_id,
        "two facts are not the same fact"
    );
    assert_eq!(store.stream_version(stream).expect("readable"), Some(2));
}

fn a_stream_reads_back_in_order(store: &dyn EventStore, stream: &StreamId) {
    let slice = store.read_stream(stream, 0, 10).expect("readable");
    assert_eq!(slice.events.len(), 2);
    assert_eq!(slice.events[0].name, "item.received");
    assert_eq!(slice.events[1].name, "item.extracted");
    assert_eq!(slice.events[0].data, json!({ "value": 1 }));
    assert!(slice.end_of_stream);
    assert_eq!(slice.next_version, 2);

    let page = store.read_stream(stream, 0, 1).expect("readable");
    assert_eq!(page.events.len(), 1);
    assert!(!page.end_of_stream, "a partial read is not the end");
    let rest = store
        .read_stream(stream, page.next_version, 10)
        .expect("readable");
    assert_eq!(rest.events.len(), 1);
    assert_eq!(rest.events[0].version, 2);
}

fn the_expected_version_is_enforced(store: &dyn EventStore, stream: &StreamId) {
    let body = json!({ "command": "stale" });
    let error = store
        .append(
            stream,
            Expected::Exact(1),
            &[event("item.indexed", 3)],
            &meta("key-stale", &body),
        )
        .expect_err("a stale expectation is refused");
    assert_eq!(
        error,
        EventLogError::Conflict {
            expected: 1,
            actual: 2
        },
        "a conflict says where the stream actually is"
    );

    let error = store
        .append(
            stream,
            Expected::NoStream,
            &[event("item.indexed", 3)],
            &meta("key-exists", &body),
        )
        .expect_err("a stream that exists is not a new stream");
    assert!(matches!(error, EventLogError::Conflict { .. }));

    store
        .append(
            stream,
            Expected::Exact(2),
            &[event("item.indexed", 3)],
            &meta("key-2", &body),
        )
        .expect("the current expectation is accepted");
    assert_eq!(store.stream_version(stream).expect("readable"), Some(3));
}

fn a_repeated_command_is_not_a_second_write(store: &dyn EventStore, stream: &StreamId) {
    let body = json!({ "command": "retry" });
    let first = store
        .append(
            stream,
            Expected::Exact(3),
            &[event("item.searchable", 4)],
            &meta("key-retry", &body),
        )
        .expect("first attempt");
    let second = store
        .append(
            stream,
            Expected::Exact(3),
            &[event("item.searchable", 4)],
            &meta("key-retry", &body),
        )
        .expect("the retry is answered, not written again");
    assert!(second.deduplicated, "a retry is reported as one");
    assert_eq!(first.first_version, second.first_version);
    assert_eq!(first.last_version, second.last_version);
    assert_eq!(
        first.events[0].event_id, second.events[0].event_id,
        "a retry returns the fact that was stored, not a new one"
    );
    assert_eq!(
        store.stream_version(stream).expect("readable"),
        Some(4),
        "a retry did not move the stream"
    );
}

fn a_reused_key_with_a_different_body_is_refused(store: &dyn EventStore, stream: &StreamId) {
    let changed = json!({ "command": "retry", "but": "different" });
    let error = store
        .append(
            stream,
            Expected::Exact(4),
            &[event("item.searchable", 9)],
            &meta("key-retry", &changed),
        )
        .expect_err("the same key with a different body is refused");
    assert!(
        matches!(error, EventLogError::IdempotencyMismatch { .. }),
        "a changed request must not be answered with the earlier result"
    );
}

fn a_command_that_decided_nothing_is_refused(store: &dyn EventStore, stream: &StreamId) {
    let body = json!({ "command": "empty" });
    let error = store
        .append(stream, Expected::Any, &[], &meta("key-empty", &body))
        .expect_err("an empty append is refused");
    assert!(matches!(error, EventLogError::Invalid(_)));
}

fn the_same_stream_id_under_another_tenant_is_another_stream(
    store: &dyn EventStore,
    twin: &StreamId,
) {
    assert_eq!(
        store.stream_version(twin).expect("readable"),
        None,
        "one tenant's history is not another tenant's"
    );
    let body = json!({ "command": "create" });
    let result = store
        .append(
            twin,
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("key-1", &body),
        )
        .expect("the same key under another tenant is a different command");
    assert_eq!(
        result.first_version, 1,
        "the twin starts at one, not where the first tenant's stream is"
    );
    assert!(!result.deduplicated);
}

fn the_feed_is_resumable(store: &dyn EventStore, tenant: &TenantId) {
    let all = store.read_feed(tenant, 0, 100).expect("readable");
    assert!(all.events.len() >= 4);
    assert!(!all.has_more);
    for pair in all.events.windows(2) {
        assert!(
            pair[0].global_seq < pair[1].global_seq,
            "the feed is in commit order"
        );
    }

    let first = store.read_feed(tenant, 0, 2).expect("readable");
    assert_eq!(first.events.len(), 2);
    assert!(first.has_more);
    let rest = store
        .read_feed(tenant, first.next_position, 100)
        .expect("readable");
    assert_eq!(
        first.events.len() + rest.events.len(),
        all.events.len(),
        "resuming from a cursor loses nothing and repeats nothing"
    );
}

fn the_feed_shows_one_tenant_only(store: &dyn EventStore, tenant: &TenantId, other: &TenantId) {
    let mine = store.read_feed(tenant, 0, 100).expect("readable");
    assert!(
        mine.events.iter().all(|event| event.tenant == *tenant),
        "a feed carries one tenant's facts and nobody else's"
    );
    let theirs = store.read_feed(other, 0, 100).expect("readable");
    assert_eq!(theirs.events.len(), 1);
}

fn a_stream_identity_is_stable(store: &dyn EventStore, tenant: &TenantId, other: &TenantId) {
    let first = store.stream_identity(tenant).expect("readable");
    let again = store.stream_identity(tenant).expect("readable");
    assert_eq!(
        first, again,
        "an identity a reader pins must not move under it"
    );
    assert!(!first.is_empty());
    assert_ne!(
        first,
        store.stream_identity(other).expect("readable"),
        "two tenants are two streams"
    );
}

fn bytes_live_outside_the_log_and_can_be_erased_alone(store: &dyn EventStore, tenant: &TenantId) {
    let digest = "sha256:0000000000000000000000000000000000000000000000000000000000000001";
    assert!(store.get_blob(tenant, digest).expect("readable").is_none());
    store
        .put_blob(tenant, digest, b"the uploaded bytes")
        .expect("writable");
    assert_eq!(
        store.get_blob(tenant, digest).expect("readable").as_deref(),
        Some(&b"the uploaded bytes"[..])
    );
    // Writing the same digest twice is one file, not two.
    store
        .put_blob(tenant, digest, b"the uploaded bytes")
        .expect("writable");
    assert_eq!(
        store.get_blob(tenant, digest).expect("readable").as_deref(),
        Some(&b"the uploaded bytes"[..])
    );
    store.delete_blob(tenant, digest).expect("deletable");
    assert!(
        store.get_blob(tenant, digest).expect("readable").is_none(),
        "bytes are erasable on their own, so erasing them leaves the fact that a file arrived"
    );
}

fn a_snapshot_round_trips(store: &dyn EventStore, stream: &StreamId) {
    assert!(store.load_snapshot(stream).expect("readable").is_none());
    let snapshot = Snapshot {
        version: 4,
        state_schema_version: 1,
        state: json!({ "state": "searchable" }),
        recorded_at: OffsetDateTime::UNIX_EPOCH,
    };
    store.save_snapshot(stream, &snapshot).expect("writable");
    let loaded = store
        .load_snapshot(stream)
        .expect("readable")
        .expect("a saved snapshot is there");
    assert_eq!(loaded.version, 4);
    assert_eq!(loaded.state, json!({ "state": "searchable" }));

    let newer = Snapshot {
        version: 4,
        state_schema_version: 2,
        state: json!({ "state": "newer" }),
        recorded_at: OffsetDateTime::UNIX_EPOCH,
    };
    store.save_snapshot(stream, &newer).expect("writable");
    let loaded = store
        .load_snapshot(stream)
        .expect("readable")
        .expect("still there");
    assert_eq!(
        loaded.state_schema_version, 2,
        "a stream keeps its latest fold, not a pile of them"
    );
}

fn redaction_keeps_the_place_and_drops_the_snapshot(store: &dyn EventStore, stream: &StreamId) {
    let before = store.read_stream(stream, 0, 100).expect("readable");
    let target = before.events[1].clone();
    assert!(!target.is_redacted());

    let redacted = store
        .redact(stream, target.version, "the person asked")
        .expect("redactable");
    assert_eq!(redacted.event_id, target.event_id, "the fact keeps its id");
    assert_eq!(redacted.version, target.version, "and its place");
    assert!(redacted.is_redacted());
    assert_eq!(redacted.data["redacted"], json!(true));
    assert_eq!(redacted.data["reason"], json!("the person asked"));

    assert!(
        store.load_snapshot(stream).expect("readable").is_none(),
        "a snapshot taken at or after a redacted version is a lie with a timestamp"
    );

    let after = store.read_stream(stream, 0, 100).expect("readable");
    assert_eq!(
        after.events.len(),
        before.events.len(),
        "redaction erases a body, not a fact"
    );

    let error = store
        .redact(stream, 9_999, "no such version")
        .expect_err("there is nothing there to redact");
    assert_eq!(error, EventLogError::NotFound);
}

fn forgetting_a_tenant_leaves_nothing(store: &dyn EventStore, other: &TenantId, twin: &StreamId) {
    store
        .save_snapshot(
            twin,
            &Snapshot {
                version: 1,
                state_schema_version: 1,
                state: json!({ "state": "received" }),
                recorded_at: OffsetDateTime::UNIX_EPOCH,
            },
        )
        .expect("writable");
    store.forget_tenant(other).expect("forgettable");
    assert_eq!(store.stream_version(twin).expect("readable"), None);
    assert!(store.load_snapshot(twin).expect("readable").is_none());
    assert!(
        store
            .read_feed(other, 0, 100)
            .expect("readable")
            .events
            .is_empty(),
        "nothing of the tenant remains in any table the kit owns"
    );

    let body = json!({ "command": "create" });
    let result = store
        .append(
            twin,
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("key-1", &body),
        )
        .expect("the idempotency record went with the tenant");
    assert!(!result.deduplicated);
}

/// Drain a catch-up runner until it has applied at least `expected` events, or give up.
///
/// A feed reader stops at the commit watermark, so an event stays invisible while *any* older
/// transaction anywhere in the database is still open — including one belonging to a different
/// owner in the same cluster. That is the price of never skipping, and it means a catch-up
/// projection is eventually consistent by design rather than by accident. A test that drained
/// once and asserted would be asserting on whatever else happened to be running.
///
/// # Panics
/// Panics when the events never became visible, which is a real failure rather than a slow one.
pub fn drain_at_least(
    runner: &eventlog_core::CatchUpRunner,
    tenant: &TenantId,
    expected: u64,
) -> u64 {
    let mut applied = 0;
    for _ in 0..200 {
        applied += runner.drain(tenant).expect("drained");
        if applied >= expected {
            return applied;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    panic!("the feed never delivered {expected} events; it delivered {applied}");
}

/// A read model that counts what happened to each stream, and totals per tenant.
///
/// Deliberately dull: what is being proved is the runner, the guard and the rebuild, not the
/// cleverness of the fold.
pub struct Tally;

/// The per-stream row this projector writes.
pub const TALLY: eventlog_core::ProjectionSpec = eventlog_core::ProjectionSpec {
    name: "tally",
    indexed: &["kind"],
};

impl eventlog_core::Projector for Tally {
    fn name(&self) -> &'static str {
        "tally"
    }

    fn projections(&self) -> &'static [eventlog_core::ProjectionSpec] {
        std::slice::from_ref(&TALLY)
    }

    fn apply(
        &self,
        event: &eventlog_core::RecordedEvent,
        store: &mut dyn eventlog_core::ProjectionStore,
    ) -> Result<(), EventLogError> {
        let key = format!("{}/{}", event.stream_type, event.stream_id);
        let current = store
            .get(&TALLY, &event.tenant, &key)?
            .and_then(|row| row.get("count").and_then(serde_json::Value::as_u64))
            .unwrap_or(0);
        store.upsert(
            &TALLY,
            &event.tenant,
            &key,
            &json!({
                "stream": event.stream_id,
                "kind": event.stream_type,
                "count": current + 1,
                "last": event.name,
            }),
        )
    }
}

/// Run every projection assertion against one backend.
///
/// # Panics
/// Panics with the failing assertion.
pub fn run_projections(store: &std::sync::Arc<dyn EventStore>) {
    use eventlog_core::{CatchUpRunner, Guard, ProjectionStore, Projector};

    let tenant = TenantId::new("tenant-p").expect("valid tenant");
    let stream = StreamId::new(tenant.clone(), "item", "item-1").expect("valid stream");
    let body = json!({ "command": "create" });

    // Catch-up first: the projection is behind until somebody runs it.
    let projector = std::sync::Arc::new(Tally);
    let runner = CatchUpRunner::new(
        std::sync::Arc::clone(store),
        std::sync::Arc::clone(&projector) as std::sync::Arc<dyn Projector>,
    )
    .expect("a catch-up runner");

    store
        .append(
            &stream,
            Expected::NoStream,
            &[event("item.received", 1), event("item.extracted", 2)],
            &meta("p-1", &body),
        )
        .expect("append");
    assert!(
        store
            .projection_get(&TALLY, &tenant, "item/item-1")
            .expect("readable")
            .is_none(),
        "a catch-up projection has not seen anything until it runs"
    );

    let applied = drain_at_least(&runner, &tenant, 2);
    assert_eq!(applied, 2);
    let row = store
        .projection_get(&TALLY, &tenant, "item/item-1")
        .expect("readable")
        .expect("the runner wrote the row");
    assert_eq!(row["count"], json!(2));
    assert_eq!(row["last"], json!("item.extracted"));

    // Resuming applies only what is new.
    store
        .append(
            &stream,
            Expected::Exact(2),
            &[event("item.indexed", 3)],
            &meta("p-2", &body),
        )
        .expect("append");
    assert_eq!(drain_at_least(&runner, &tenant, 1), 1, "no repeats");
    let row = store
        .projection_get(&TALLY, &tenant, "item/item-1")
        .expect("readable")
        .expect("there");
    assert_eq!(row["count"], json!(3));

    // A declared field is queryable; an undeclared one is refused rather than scanned.
    let found = store
        .projection_find(&TALLY, &tenant, "kind", "item", 10)
        .expect("findable");
    assert_eq!(found.len(), 1);
    assert!(
        store
            .projection_find(&TALLY, &tenant, "last", "item.indexed", 10)
            .is_err(),
        "a field nobody declared is not an index that appears in production"
    );

    // A guard may not read a read model that lags.
    let refused = store.append_guarded(
        &stream,
        Expected::Exact(3),
        &[event("item.searchable", 4)],
        &meta("p-guard", &body),
        &(|projections: &mut dyn ProjectionStore| {
            projections
                .get_for_update(&TALLY, &tenant, "item/item-1")
                .map(|_| ())
        }) as &dyn Guard,
    );
    assert!(
        matches!(refused, Err(EventLogError::Invalid(_))),
        "a guard over a catch-up projection is a limit enforced late, which is not a limit"
    );

    // A list is a page in key order, resumable by the last key returned.
    let page = store
        .projection_list(&TALLY, &tenant, None, 10)
        .expect("listable");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].0, "item/item-1");
    let after = store
        .projection_list(&TALLY, &tenant, Some(&page[0].0), 10)
        .expect("listable");
    assert!(
        after.is_empty(),
        "a cursor does not repeat the row it ended on"
    );

    // Erasing a tenant takes its read models with it.
    store.forget_tenant(&tenant).expect("forgettable");
    assert!(
        store
            .projection_get(&TALLY, &tenant, "item/item-1")
            .expect("readable")
            .is_none(),
        "a read model left behind after an erasure is the erased tenant, still readable"
    );
    store
        .append(
            &stream,
            Expected::NoStream,
            &[event("item.received", 1), event("item.extracted", 2)],
            &meta("p-1", &body),
        )
        .expect("append");
    drain_at_least(&runner, &tenant, 2);
    let row = store
        .projection_get(&TALLY, &tenant, "item/item-1")
        .expect("readable")
        .expect("the cursor went with the tenant, so the new history was read");
    assert_eq!(
        row["count"],
        json!(2),
        "the count restarted: the erased tenant's rows did not survive under the same key"
    );

    // Rebuilding from the log reproduces the same rows.
    let before = store
        .projection_get(&TALLY, &tenant, "item/item-1")
        .expect("readable")
        .expect("there");
    let replayed = store
        .rebuild_projection(projector.as_ref(), &tenant)
        .expect("rebuildable");
    assert!(replayed >= 2, "the rebuild replayed the log it has");
    let after = store
        .projection_get(&TALLY, &tenant, "item/item-1")
        .expect("readable")
        .expect("there");
    assert_eq!(before, after, "a dropped read model comes back identical");
}

/// Run the inline-projection and guard assertions against a freshly built store.
///
/// Inline registration has to happen before the first append, so this takes a store the caller
/// has not written to yet.
///
/// # Panics
/// Panics with the failing assertion.
pub fn run_inline_projections(store: &std::sync::Arc<dyn EventStore>) {
    use eventlog_core::{Guard, ProjectionStore, Projector};

    let tenant = TenantId::new("tenant-i").expect("valid tenant");
    let stream = StreamId::new(tenant.clone(), "item", "item-1").expect("valid stream");
    let body = json!({ "command": "create" });

    store
        .register_inline(std::sync::Arc::new(Tally) as std::sync::Arc<dyn Projector>)
        .expect("registered");
    assert!(store.is_inline("tally"));

    store
        .append(
            &stream,
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("i-1", &body),
        )
        .expect("append");
    let row = store
        .projection_get(&TALLY, &tenant, "item/item-1")
        .expect("readable")
        .expect("an inline projection is written in the same transaction");
    assert_eq!(row["count"], json!(1), "read-your-writes, with no runner");

    // A guard reads the inline row inside the append transaction and refuses at the limit.
    let limit = 2;
    let guard = move |projections: &mut dyn ProjectionStore| {
        let count = projections
            .get_for_update(&TALLY, &tenant, "item/item-1")?
            .and_then(|row| row.get("count").and_then(serde_json::Value::as_u64))
            .unwrap_or(0);
        if count >= limit {
            return Err(EventLogError::Invalid("this one is full".to_owned()));
        }
        Ok(())
    };

    store
        .append_guarded(
            &stream,
            Expected::Exact(1),
            &[event("item.extracted", 2)],
            &meta("i-2", &body),
            &guard as &dyn Guard,
        )
        .expect("under the limit");

    let refused = store.append_guarded(
        &stream,
        Expected::Exact(2),
        &[event("item.indexed", 3)],
        &meta("i-3", &body),
        &guard as &dyn Guard,
    );
    assert!(
        matches!(refused, Err(EventLogError::Invalid(_))),
        "the guard refused the write at the limit"
    );
    let tenant = TenantId::new("tenant-i").expect("valid tenant");
    let stream = StreamId::new(tenant.clone(), "item", "item-1").expect("valid stream");
    assert_eq!(
        store.stream_version(&stream).expect("readable"),
        Some(2),
        "a refused guard wrote nothing at all"
    );
}

/// One stored event body, exactly as some past version of the code wrote it.
///
/// An event type is permanent, so the bytes a module wrote in 2026 must still fold in 2030. These
/// are committed beside a module's contract vectors and never edited — editing one is editing
/// history, which is the thing this whole design exists to stop.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EventVector {
    pub name: String,
    pub schema_version: u32,
    pub data: serde_json::Value,
}

/// Fold a run of stored bodies through an aggregate, as a replay would.
///
/// # Errors
/// Returns [`EventLogError::Backend`] when a stored body cannot be read at its recorded schema
/// version, which means an upcaster is missing.
pub fn fold_vectors<A: eventlog_core::Aggregate>(
    id: &str,
    vectors: &[EventVector],
) -> Result<A, EventLogError> {
    let tenant = TenantId::new("tenant-vectors")?;
    let mut state = A::empty(id);
    for (offset, vector) in vectors.iter().enumerate() {
        let recorded = eventlog_core::RecordedEvent {
            global_seq: offset as u64 + 1,
            tenant: tenant.clone(),
            stream_type: A::TYPE.to_owned(),
            stream_id: id.to_owned(),
            version: offset as u64 + 1,
            event_id: eventlog_core::new_event_id(),
            name: vector.name.clone(),
            schema_version: vector.schema_version,
            occurred_at: OffsetDateTime::UNIX_EPOCH,
            recorded_at: OffsetDateTime::UNIX_EPOCH,
            subject: "person-1".to_owned(),
            actor: "service-1".to_owned(),
            request_id: "request-vectors".to_owned(),
            trace_id: "trace-vectors".to_owned(),
            causation_id: None,
            causation_depth: 0,
            redacted_at: None,
            data: vector.data.clone(),
        };
        let event = <A::Event as eventlog_core::DomainEvent>::from_data(
            &recorded.name,
            recorded.schema_version,
            &recorded.data,
        )?;
        state.apply(&eventlog_core::Applied::Happened {
            event: &event,
            recorded: &recorded,
        });
    }
    Ok(state)
}

/// Read a module's committed vectors from a directory of JSON files.
///
/// # Panics
/// Panics when the directory cannot be read or a file is not a vector. A module whose history
/// cannot be loaded has no evolution gate, and reporting that as an empty pass would be worse
/// than failing.
#[must_use]
pub fn load_vectors(directory: &std::path::Path) -> Vec<EventVector> {
    let mut paths: Vec<_> = std::fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("cannot read vectors in {}: {error}", directory.display()))
        .map(|entry| entry.expect("a readable directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    paths.sort();
    paths
        .iter()
        .map(|path| {
            let bytes = std::fs::read(path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|error| panic!("{} is not a vector: {error}", path.display()))
        })
        .collect()
}

/// Prove every committed vector still folds, and that the fold reaches the recorded state.
///
/// # Panics
/// Panics naming the vector that stopped folding.
pub fn assert_vectors_fold<A: eventlog_core::Aggregate + PartialEq + std::fmt::Debug>(
    id: &str,
    vectors: &[EventVector],
    expected: &A,
) {
    for (offset, vector) in vectors.iter().enumerate() {
        let prefix = &vectors[..=offset];
        if let Err(error) = fold_vectors::<A>(id, prefix) {
            panic!(
                "vector {} ({} v{}) no longer folds: {error}",
                offset, vector.name, vector.schema_version
            );
        }
    }
    let folded = fold_vectors::<A>(id, vectors).expect("every vector folds");
    assert_eq!(
        &folded, expected,
        "history still folds, but no longer to the state it produced"
    );
}

/// The field names and value kinds of a stored body, without its values.
///
/// A vector records one run's identifiers and timestamps, which differ every run and mean nothing.
/// What must not drift is the shape: a field that disappeared, was renamed, or changed type is
/// what stops old bytes reading.
#[must_use]
pub fn shape(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), shape(value)))
                .collect(),
        ),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(shape).collect())
        }
        serde_json::Value::String(_) => serde_json::Value::String("string".to_owned()),
        serde_json::Value::Number(_) => serde_json::Value::String("number".to_owned()),
        serde_json::Value::Bool(_) => serde_json::Value::String("boolean".to_owned()),
        serde_json::Value::Null => serde_json::Value::Null,
    }
}

/// Every event an owner can emit has a committed vector.
///
/// A new event without one would ship unrecorded, and the first time anybody needed to prove its
/// bytes still read would be the first time they could not.
///
/// # Panics
/// Panics naming the event that has no vector.
pub fn assert_every_event_has_a_vector(directory: &std::path::Path, declared: &[&str]) {
    let committed: Vec<String> = load_vectors(directory)
        .into_iter()
        .map(|vector| vector.name)
        .collect();
    for name in declared {
        assert!(
            committed.iter().any(|found| found == name),
            "no committed vector for {name}; a new event may not ship unrecorded"
        );
    }
}

/// Every committed vector still reads at the version it was written under.
///
/// This is where a missing upcaster surfaces: the old bytes are still out there and always will
/// be.
///
/// # Panics
/// Panics naming the vector that stopped reading.
pub fn assert_every_vector_decodes(
    directory: &std::path::Path,
    decode: impl Fn(&EventVector) -> Result<(), EventLogError>,
) {
    for vector in load_vectors(directory) {
        if let Err(error) = decode(&vector) {
            panic!(
                "{} v{} no longer decodes; an upcaster is missing: {error}",
                vector.name, vector.schema_version
            );
        }
    }
}

/// What the code writes today matches what is committed.
///
/// A payload that changed shape without a version bump fails here rather than in production,
/// months later, when the oldest rows are read.
///
/// # Panics
/// Panics naming the event whose shape or version moved.
pub fn assert_committed_matches_emitted(
    directory: &std::path::Path,
    emitted: &std::collections::BTreeMap<String, EventVector>,
) {
    let committed: std::collections::BTreeMap<String, EventVector> = load_vectors(directory)
        .into_iter()
        .map(|vector| (vector.name.clone(), vector))
        .collect();
    for (name, fresh) in emitted {
        let Some(stored) = committed.get(name) else {
            panic!("{name} is emitted but has no committed vector");
        };
        assert_eq!(
            fresh.schema_version, stored.schema_version,
            "{name} changed schema version without a new vector"
        );
        assert_eq!(
            shape(&fresh.data),
            shape(&stored.data),
            "{name} changed shape without a version bump; bump the version, write an upcaster, \
             and commit the new vector beside the old one"
        );
    }
}

/// Write the vectors an owner emits today, without ever overwriting one.
///
/// A new event version is a new file beside the old one, because the old bytes are still out
/// there.
///
/// # Panics
/// Panics when the directory cannot be created or a file cannot be written.
pub fn write_vectors(
    directory: &std::path::Path,
    emitted: &std::collections::BTreeMap<String, EventVector>,
) {
    std::fs::create_dir_all(directory).expect("a vectors directory");
    for (name, vector) in emitted {
        let path = directory.join(format!("{}-v{}.json", name, vector.schema_version));
        if path.exists() {
            continue;
        }
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(vector).expect("serialisable"),
        )
        .expect("written");
        println!("wrote {}", path.display());
    }
}

/// Collect one of every event a tenant's feed holds, keyed by name.
///
/// # Panics
/// Panics when the feed cannot be read.
#[must_use]
pub fn vectors_from_feed(
    store: &dyn EventStore,
    tenant: &TenantId,
) -> std::collections::BTreeMap<String, EventVector> {
    let mut found = std::collections::BTreeMap::new();
    let mut position = 0;
    loop {
        let page = store.read_feed(tenant, position, 200).expect("readable");
        if page.events.is_empty() {
            return found;
        }
        position = page.next_position;
        for event in page.events {
            found.entry(event.name.clone()).or_insert(EventVector {
                name: event.name,
                schema_version: event.schema_version,
                data: event.data,
            });
        }
    }
}

/// The paging rule, which five modules each got slightly different.
///
/// # Panics
/// Panics with the failing assertion.
pub fn run_paging(store: &std::sync::Arc<dyn EventStore>) {
    use eventlog_core::Projector;

    let tenant = TenantId::new("tenant-page").expect("valid tenant");
    store
        .register_inline(std::sync::Arc::new(Tally) as std::sync::Arc<dyn Projector>)
        .expect("registered");
    let body = json!({ "command": "create" });
    for index in 0..5 {
        let stream =
            StreamId::new(tenant.clone(), "item", format!("item-{index}")).expect("valid stream");
        store
            .append(
                &stream,
                Expected::NoStream,
                &[event("item.received", index)],
                &meta(&format!("page-{index}"), &body),
            )
            .expect("append");
    }
    // A second kind, so a prefix has something to exclude.
    let other = StreamId::new(tenant.clone(), "note", "note-1").expect("valid stream");
    store
        .append(
            &other,
            Expected::NoStream,
            &[event("note.written", 9)],
            &meta("page-note", &body),
        )
        .expect("append");

    let page = store
        .projection_page(&TALLY, &tenant, Some("item/"), None, 2)
        .expect("pageable");
    assert_eq!(page.rows.len(), 2);
    assert_eq!(
        page.next_cursor.as_deref(),
        Some(page.rows[1].0.as_str()),
        "the cursor is the last row returned, not the last row fetched"
    );

    let mut seen = vec![page.rows[0].0.clone(), page.rows[1].0.clone()];
    let mut cursor = page.next_cursor;
    while let Some(from) = cursor {
        let next = store
            .projection_page(&TALLY, &tenant, Some("item/"), Some(&from), 2)
            .expect("pageable");
        seen.extend(next.rows.iter().map(|(key, _)| key.clone()));
        cursor = next.next_cursor;
    }
    assert_eq!(seen.len(), 5, "paging loses nothing and repeats nothing");
    let mut sorted = seen.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 5);

    let exact = store
        .projection_page(&TALLY, &tenant, Some("item/"), None, 5)
        .expect("pageable");
    assert_eq!(exact.rows.len(), 5);
    assert!(
        exact.next_cursor.is_none(),
        "a page that reached the end carries no cursor; one that did would cost the caller a \
         round trip that returns nothing"
    );

    let everything = store
        .projection_page(&TALLY, &tenant, None, None, 100)
        .expect("pageable");
    assert_eq!(
        everything.rows.len(),
        6,
        "no prefix is every row this tenant has"
    );
    let bounded = store
        .projection_page(&TALLY, &tenant, Some("item/"), None, 100)
        .expect("pageable");
    assert_eq!(
        bounded.rows.len(),
        5,
        "a prefix bounds the page to one contiguous run of keys, and the note is not in it"
    );
}
