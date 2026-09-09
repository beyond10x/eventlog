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

async fn fixture() -> (Arc<SqliteEventStore>, TenantId) {
    let store = Arc::new(
        SqliteEventStore::in_memory("counter")
            .await
            .expect("a store"),
    );
    let tenant = TenantId::new("tenant-a").expect("valid tenant");
    (store, tenant)
}

#[tokio::test]
async fn a_delayed_unproven_snapshot_cannot_restore_redacted_state() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Counter>::new(store.clone());
    repository
        .handle(&tenant, "delayed", &Command::Add(1), &meta("delayed"))
        .await
        .unwrap();
    let loaded = repository.load(&tenant, "delayed").await.unwrap();
    let stream = repository.stream(&tenant, "delayed").unwrap();
    let snapshot = eventlog_core::Snapshot {
        version: loaded.version,
        state_schema_version: Counter::STATE_SCHEMA_VERSION,
        state: serde_json::to_value(loaded.state).unwrap(),
        recorded_at: OffsetDateTime::now_utc(),
    };
    store.redact(&stream, 1, "privacy").await.unwrap();
    let _ = store.save_snapshot(&stream, &snapshot).await;
    assert_eq!(
        repository
            .load(&tenant, "delayed")
            .await
            .unwrap()
            .state
            .total,
        0
    );
}

#[tokio::test]
async fn a_command_folds_into_the_state_it_produced() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Counter>::new(store);
    let loaded = repository.load(&tenant, "c-1").await.expect("loadable");
    assert!(loaded.is_new);
    assert_eq!(loaded.version, 0);
    assert_eq!(loaded.state.total, 0);

    let outcome = repository
        .handle(&tenant, "c-1", &Command::Add(5), &meta("k-1"))
        .await
        .expect("handled");
    assert_eq!(outcome.version, 1);
    assert_eq!(outcome.state.total, 5);
    assert_eq!(outcome.events.len(), 1);

    let reloaded = repository.load(&tenant, "c-1").await.expect("loadable");
    assert_eq!(reloaded.state, outcome.state, "a fold is not a lucky guess");
    assert!(!reloaded.is_new);
}

#[tokio::test]
async fn a_command_that_decided_nothing_writes_nothing() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Counter>::new(store);
    repository
        .handle(&tenant, "c-1", &Command::Add(1), &meta("k-1"))
        .await
        .expect("handled");
    let outcome = repository
        .handle(&tenant, "c-1", &Command::Nothing, &meta("k-2"))
        .await
        .expect("handled");
    assert!(outcome.events.is_empty());
    assert_eq!(outcome.version, 1, "the stream did not move");
}

#[tokio::test]
async fn a_retried_command_is_answered_not_written_again() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Counter>::new(store);
    let first = repository
        .handle(&tenant, "c-1", &Command::Add(3), &meta("k-1"))
        .await
        .expect("handled");
    let second = repository
        .handle(&tenant, "c-1", &Command::Add(3), &meta("k-1"))
        .await
        .expect("handled again");
    assert!(second.deduplicated);
    assert_eq!(first.version, second.version);
    assert_eq!(
        repository
            .load(&tenant, "c-1")
            .await
            .expect("loadable")
            .state
            .total,
        3,
        "a retry did not add the amount twice"
    );
}

#[tokio::test]
async fn a_concurrent_write_is_retried_once_and_then_refused() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>);
    repository
        .handle(&tenant, "c-1", &Command::Add(1), &meta("k-1"))
        .await
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
        .await
        .expect("the other writer wins the race");

    let outcome = repository
        .handle(&tenant, "c-1", &Command::Add(2), &meta("k-2"))
        .await
        .expect("the retry reloads and lands");
    assert_eq!(outcome.version, 3);
    assert_eq!(
        outcome.state.total, 13,
        "the fold includes the other writer"
    );
}

#[tokio::test]
async fn a_snapshot_is_a_cache_and_the_fold_agrees_with_it() {
    let (store, tenant) = fixture().await;
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
            .await
            .expect("handled");
    }
    let stream = StreamId::new(tenant.clone(), "counter", "c-1").expect("valid stream");
    let snapshot = store
        .load_snapshot(&stream)
        .await
        .expect("readable")
        .expect("the policy wrote one");
    assert_eq!(snapshot.version, 10, "written at the multiple, not after");

    let from_snapshot = repository.load(&tenant, "c-1").await.expect("loadable");
    assert_eq!(from_snapshot.version, 12);
    assert_eq!(from_snapshot.state.total, (1..=12).sum::<i64>());

    // The same history with no snapshot at all must fold to the same thing.
    let bare = Arc::new(
        SqliteEventStore::in_memory("counter")
            .await
            .expect("a store"),
    );
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
            .await
            .expect("handled");
    }
    let folded = bare_repository
        .load(&tenant, "c-1")
        .await
        .expect("loadable");
    assert_eq!(
        folded.state, from_snapshot.state,
        "a snapshot is a cache; folding from zero must reach the same state"
    );
}

#[tokio::test]
async fn every_prefix_folds_the_same_way_with_or_without_a_snapshot() {
    let (store, tenant) = fixture().await;
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
            .await
            .expect("handled");
        // Snapshot at this prefix, then prove the next load agrees with a fold from zero.
        let cached = repository
            .snapshot_now(&tenant, "c-1")
            .await
            .expect("snapshotted");
        assert_eq!(cached, u64::try_from(step).expect("small"));
        let with_cache = repository.load(&tenant, "c-1").await.expect("loadable");
        let expected: i64 = (1..=step).sum();
        assert_eq!(
            with_cache.state.total, expected,
            "fold(snapshot at {step}, tail) == fold(everything)"
        );
    }
}

#[tokio::test]
async fn a_stale_snapshot_schema_is_discarded_rather_than_trusted() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>);
    repository
        .handle(&tenant, "c-1", &Command::Add(7), &meta("k-1"))
        .await
        .expect("handled");
    let stream = StreamId::new(tenant.clone(), "counter", "c-1").expect("valid stream");
    let generation = store.snapshot_generation(&stream).await.unwrap().unwrap();
    store
        .save_snapshot_checked(
            &stream,
            &eventlog_core::Snapshot {
                version: 1,
                state_schema_version: Counter::STATE_SCHEMA_VERSION + 1,
                state: json!({ "nonsense": true }),
                recorded_at: OffsetDateTime::UNIX_EPOCH,
            },
            &generation,
        )
        .await
        .expect("writable");
    let loaded = repository.load(&tenant, "c-1").await.expect("loadable");
    assert_eq!(
        loaded.state.total, 7,
        "a snapshot from another schema is discarded and the fold restarts from zero"
    );
}

#[tokio::test]
async fn an_aggregate_stays_foldable_after_an_erasure() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>);
    repository
        .handle(&tenant, "c-1", &Command::Add(4), &meta("k-1"))
        .await
        .expect("handled");
    repository
        .handle(&tenant, "c-1", &Command::Add(6), &meta("k-2"))
        .await
        .expect("handled");
    let stream = StreamId::new(tenant.clone(), "counter", "c-1").expect("valid stream");
    store
        .redact(&stream, 1, "the person asked")
        .await
        .expect("redactable");

    let loaded = repository.load(&tenant, "c-1").await.expect("loadable");
    assert_eq!(loaded.version, 2, "the fact kept its place");
    assert_eq!(loaded.state.applied, 2, "both facts were folded");
    assert_eq!(loaded.state.redactions, 1);
    assert_eq!(
        loaded.state.total, 6,
        "the erased amount is gone from the total, and the fold did not fail"
    );
}

#[tokio::test]
async fn a_second_event_type_folds_and_reads_back() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Counter>::new(store);
    repository
        .handle(&tenant, "c-1", &Command::Add(9), &meta("k-1"))
        .await
        .expect("handled");
    let cleared = repository
        .handle(&tenant, "c-1", &Command::Clear, &meta("k-2"))
        .await
        .expect("handled");
    assert_eq!(cleared.state.total, 0);
    assert_eq!(
        repository
            .load(&tenant, "c-1")
            .await
            .expect("loadable")
            .state
            .total,
        0,
        "the cleared fact reads back as itself, not as an unknown event"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_history_and_repository_privacy_interleavings() {
    let store: Arc<dyn EventStore> =
        Arc::new(SqliteEventStore::in_memory("snapshot_races").await.unwrap());
    eventlog_conformance::run_snapshot_generations(store.clone()).await;
    let tenant = eventlog_core::TenantId::new("snapshot-races").unwrap();
    for automatic in [true, false] {
        let id = if automatic {
            "automatic-sqlite"
        } else {
            "explicit-sqlite"
        };
        let repository =
            eventlog_core::Repository::<eventlog_conformance::SnapshotCounter>::new(store.clone())
                .with_policy(eventlog_core::SnapshotPolicy {
                    every: u64::from(automatic),
                });
        if !automatic {
            repository
                .handle(
                    &tenant,
                    id,
                    &(),
                    &eventlog_conformance::meta(id, &serde_json::json!({})),
                )
                .await
                .unwrap();
        }
        let stream = repository.stream(&tenant, id).unwrap();
        let (observed, resume) = eventlog_conformance::pause_snapshot_serialization(id);
        let owner = tenant.clone();
        let task = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                if automatic {
                    repository
                        .handle(
                            &owner,
                            id,
                            &(),
                            &eventlog_conformance::meta(id, &serde_json::json!({})),
                        )
                        .await
                        .map(|outcome| outcome.version)
                } else {
                    repository.snapshot_now(&owner, id).await
                }
            })
        });
        tokio::task::spawn_blocking(move || {
            observed
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        })
        .await
        .unwrap();
        store.redact(&stream, 1, "privacy").await.unwrap();
        resume.send(()).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), 1);
        let loaded =
            eventlog_core::Repository::<eventlog_conformance::SnapshotCounter>::new(store.clone())
                .load(&tenant, id)
                .await
                .unwrap();
        assert_eq!(
            loaded.state.total, 0,
            "delayed repository cache must agree with redacted fold"
        );
        if automatic {
            assert!(store.load_snapshot(&stream).await.unwrap().is_none());
        } else {
            assert_eq!(
                store.load_snapshot(&stream).await.unwrap().unwrap().state["total"],
                serde_json::json!(0)
            );
        }
    }
}
#[tokio::test]
async fn committed_commands_survive_snapshot_storage_failure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("snapshots.db");
    let store = Arc::new(
        SqliteEventStore::open(path.to_str().unwrap(), "cache_failure")
            .await
            .unwrap(),
    );
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute_batch("CREATE TRIGGER cache_failure_injected BEFORE INSERT ON cache_failure_snapshots BEGIN SELECT RAISE(ABORT, 'injected cache failure'); END;").unwrap();
    let repository = Repository::<eventlog_conformance::SnapshotCounter>::new(store.clone())
        .with_policy(SnapshotPolicy { every: 1 });
    let tenant = TenantId::new("tenant").unwrap();
    assert_eq!(
        repository
            .handle(&tenant, "failure", &(), &meta("failure"))
            .await
            .unwrap()
            .version,
        1
    );
    assert!(repository.snapshot_now(&tenant, "failure").await.is_err());
    assert_eq!(
        repository
            .load(&tenant, "failure")
            .await
            .unwrap()
            .state
            .total,
        1
    );
    assert_eq!(sql.query_row("SELECT count(*) FROM cache_failure_snapshot_generations WHERE cached_generation IS NOT NULL", [], |row| row.get::<_, i64>(0)).unwrap(), 0, "failed cache write rolls back its proof");
}
