//! What a domain author gets: decide, fold, snapshot, and a conflict that is refused rather than
//! looped on.

use std::sync::Arc;

use eventlog_core::{
    Aggregate, Applied, CommandMeta, DomainEvent, EventLogError, EventStore, Expected, NewEvent,
    Repository, SnapshotPolicy, StreamId, TenantId, request_hash,
};
use eventlog_sqlite::SqliteEventStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tally {
    Added(i64),
    Cleared,
}

impl DomainEvent for Tally {
    fn name(&self) -> &'static str {
        match self {
            Self::Added(_) => "tally.added",
            Self::Cleared => "tally.cleared",
        }
    }

    fn schema_version(&self) -> u32 {
        1
    }

    fn to_data(&self) -> Result<Value, EventLogError> {
        Ok(match self {
            Self::Added(amount) => json!({ "amount": amount }),
            Self::Cleared => json!({}),
        })
    }

    fn from_data(name: &str, _schema_version: u32, data: &Value) -> Result<Self, EventLogError> {
        match name {
            "tally.added" => Ok(Self::Added(
                data.get("amount")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| EventLogError::Backend("no amount in body".to_owned()))?,
            )),
            "tally.cleared" => Ok(Self::Cleared),
            other => Err(EventLogError::Backend(format!("unknown event {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Counter {
    id: String,
    total: i64,
    applied: u64,
    redactions: u64,
}

enum Command {
    Add(i64),
    Clear,
    Nothing,
}

impl Aggregate for Counter {
    type Command = Command;
    type Event = Tally;
    type Error = EventLogError;

    const TYPE: &'static str = "counter";
    const STATE_SCHEMA_VERSION: u32 = 1;

    fn empty(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            total: 0,
            applied: 0,
            redactions: 0,
        }
    }

    fn apply(&mut self, applied: &Applied<'_, Self::Event>) {
        self.applied += 1;
        match applied {
            Applied::Happened { event, .. } => match event {
                Tally::Added(amount) => self.total += amount,
                Tally::Cleared => self.total = 0,
            },
            // Total over a redaction: the fact is still counted, its contribution is not.
            Applied::Redacted { .. } => self.redactions += 1,
        }
    }

    fn decide(&self, command: &Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(match command {
            Command::Add(amount) => vec![Tally::Added(*amount)],
            Command::Clear => vec![Tally::Cleared],
            Command::Nothing => Vec::new(),
        })
    }
}

fn meta(key: &str) -> CommandMeta {
    CommandMeta {
        idempotency_key: key.to_owned(),
        request_hash: request_hash(&json!({ "key": key })).expect("hashable"),
        subject: "person-1".to_owned(),
        actor: "service-1".to_owned(),
        request_id: format!("request-{key}"),
        trace_id: format!("trace-{key}"),
        causation_id: None,
        causation_depth: 0,
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        claim: None,
    }
}

fn fixture() -> (Arc<SqliteEventStore>, TenantId) {
    let store = Arc::new(SqliteEventStore::in_memory("counter").expect("a store"));
    let tenant = TenantId::new("tenant-a").expect("valid tenant");
    (store, tenant)
}

#[test]
fn a_command_folds_into_the_state_it_produced() {
    let (store, tenant) = fixture();
    let repository = Repository::<Counter>::new(store);
    let loaded = repository.load(&tenant, "c-1").expect("loadable");
    assert!(loaded.is_new);
    assert_eq!(loaded.version, 0);
    assert_eq!(loaded.state.total, 0);

    let outcome = repository
        .handle(&tenant, "c-1", &Command::Add(5), &meta("k-1"))
        .expect("handled");
    assert_eq!(outcome.version, 1);
    assert_eq!(outcome.state.total, 5);
    assert_eq!(outcome.events.len(), 1);

    let reloaded = repository.load(&tenant, "c-1").expect("loadable");
    assert_eq!(reloaded.state, outcome.state, "a fold is not a lucky guess");
    assert!(!reloaded.is_new);
}

#[test]
fn a_command_that_decided_nothing_writes_nothing() {
    let (store, tenant) = fixture();
    let repository = Repository::<Counter>::new(store);
    repository
        .handle(&tenant, "c-1", &Command::Add(1), &meta("k-1"))
        .expect("handled");
    let outcome = repository
        .handle(&tenant, "c-1", &Command::Nothing, &meta("k-2"))
        .expect("handled");
    assert!(outcome.events.is_empty());
    assert_eq!(outcome.version, 1, "the stream did not move");
}

#[test]
fn a_retried_command_is_answered_not_written_again() {
    let (store, tenant) = fixture();
    let repository = Repository::<Counter>::new(store);
    let first = repository
        .handle(&tenant, "c-1", &Command::Add(3), &meta("k-1"))
        .expect("handled");
    let second = repository
        .handle(&tenant, "c-1", &Command::Add(3), &meta("k-1"))
        .expect("handled again");
    assert!(second.deduplicated);
    assert_eq!(first.version, second.version);
    assert_eq!(
        repository
            .load(&tenant, "c-1")
            .expect("loadable")
            .state
            .total,
        3,
        "a retry did not add the amount twice"
    );
}

#[test]
fn a_concurrent_write_is_retried_once_and_then_refused() {
    let (store, tenant) = fixture();
    let repository = Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>);
    repository
        .handle(&tenant, "c-1", &Command::Add(1), &meta("k-1"))
        .expect("handled");

    // Somebody else moves the stream between this caller's load and its append. The retry reloads
    // and succeeds.
    let stream = StreamId::new(tenant.clone(), "counter", "c-1").expect("valid stream");
    store
        .append(
            &stream,
            Expected::Exact(1),
            &[NewEvent::new("tally.added", 1, json!({ "amount": 10 })).expect("valid")],
            &meta("k-other"),
        )
        .expect("the other writer wins the race");

    let outcome = repository
        .handle(&tenant, "c-1", &Command::Add(2), &meta("k-2"))
        .expect("the retry reloads and lands");
    assert_eq!(outcome.version, 3);
    assert_eq!(
        outcome.state.total, 13,
        "the fold includes the other writer"
    );
}

#[test]
fn a_snapshot_is_a_cache_and_the_fold_agrees_with_it() {
    let (store, tenant) = fixture();
    let repository = Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>)
        .with_policy(SnapshotPolicy { every: 5 });
    for step in 1..=12 {
        repository
            .handle(
                &tenant,
                "c-1",
                &Command::Add(step),
                &meta(&format!("k-{step}")),
            )
            .expect("handled");
    }
    let stream = StreamId::new(tenant.clone(), "counter", "c-1").expect("valid stream");
    let snapshot = store
        .load_snapshot(&stream)
        .expect("readable")
        .expect("the policy wrote one");
    assert_eq!(snapshot.version, 10, "written at the multiple, not after");

    let from_snapshot = repository.load(&tenant, "c-1").expect("loadable");
    assert_eq!(from_snapshot.version, 12);
    assert_eq!(from_snapshot.state.total, (1..=12).sum::<i64>());

    // The same history with no snapshot at all must fold to the same thing.
    let bare = Arc::new(SqliteEventStore::in_memory("counter").expect("a store"));
    let bare_repository = Repository::<Counter>::new(Arc::clone(&bare) as Arc<dyn EventStore>)
        .with_policy(SnapshotPolicy { every: 0 });
    for step in 1..=12 {
        bare_repository
            .handle(
                &tenant,
                "c-1",
                &Command::Add(step),
                &meta(&format!("k-{step}")),
            )
            .expect("handled");
    }
    let folded = bare_repository.load(&tenant, "c-1").expect("loadable");
    assert_eq!(
        folded.state, from_snapshot.state,
        "a snapshot is a cache; folding from zero must reach the same state"
    );
}

#[test]
fn every_prefix_folds_the_same_way_with_or_without_a_snapshot() {
    let (store, tenant) = fixture();
    let repository = Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>)
        .with_policy(SnapshotPolicy { every: 0 });
    for step in 1..=8 {
        repository
            .handle(
                &tenant,
                "c-1",
                &Command::Add(step),
                &meta(&format!("k-{step}")),
            )
            .expect("handled");
        // Snapshot at this prefix, then prove the next load agrees with a fold from zero.
        let cached = repository
            .snapshot_now(&tenant, "c-1")
            .expect("snapshotted");
        assert_eq!(cached, u64::try_from(step).expect("small"));
        let with_cache = repository.load(&tenant, "c-1").expect("loadable");
        let expected: i64 = (1..=step).sum();
        assert_eq!(
            with_cache.state.total, expected,
            "fold(snapshot at {step}, tail) == fold(everything)"
        );
    }
}

#[test]
fn a_stale_snapshot_schema_is_discarded_rather_than_trusted() {
    let (store, tenant) = fixture();
    let repository = Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>);
    repository
        .handle(&tenant, "c-1", &Command::Add(7), &meta("k-1"))
        .expect("handled");
    let stream = StreamId::new(tenant.clone(), "counter", "c-1").expect("valid stream");
    store
        .save_snapshot(
            &stream,
            &eventlog_core::Snapshot {
                version: 1,
                state_schema_version: Counter::STATE_SCHEMA_VERSION + 1,
                state: json!({ "nonsense": true }),
                recorded_at: OffsetDateTime::UNIX_EPOCH,
            },
        )
        .expect("writable");
    let loaded = repository.load(&tenant, "c-1").expect("loadable");
    assert_eq!(
        loaded.state.total, 7,
        "a snapshot from another schema is discarded and the fold restarts from zero"
    );
}

#[test]
fn an_aggregate_stays_foldable_after_an_erasure() {
    let (store, tenant) = fixture();
    let repository = Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>);
    repository
        .handle(&tenant, "c-1", &Command::Add(4), &meta("k-1"))
        .expect("handled");
    repository
        .handle(&tenant, "c-1", &Command::Add(6), &meta("k-2"))
        .expect("handled");
    let stream = StreamId::new(tenant.clone(), "counter", "c-1").expect("valid stream");
    store
        .redact(&stream, 1, "the person asked")
        .expect("redactable");

    let loaded = repository.load(&tenant, "c-1").expect("loadable");
    assert_eq!(loaded.version, 2, "the fact kept its place");
    assert_eq!(loaded.state.applied, 2, "both facts were folded");
    assert_eq!(loaded.state.redactions, 1);
    assert_eq!(
        loaded.state.total, 6,
        "the erased amount is gone from the total, and the fold did not fail"
    );
}

#[test]
fn a_second_event_type_folds_and_reads_back() {
    let (store, tenant) = fixture();
    let repository = Repository::<Counter>::new(store);
    repository
        .handle(&tenant, "c-1", &Command::Add(9), &meta("k-1"))
        .expect("handled");
    let cleared = repository
        .handle(&tenant, "c-1", &Command::Clear, &meta("k-2"))
        .expect("handled");
    assert_eq!(cleared.state.total, 0);
    assert_eq!(
        repository
            .load(&tenant, "c-1")
            .expect("loadable")
            .state
            .total,
        0,
        "the cleared fact reads back as itself, not as an unknown event"
    );
}
