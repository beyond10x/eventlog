//! What a domain author gets: decide, fold, snapshot, and a conflict that is refused rather than
//! looped on.

use std::sync::Arc;

use eventlog_core::{
    Aggregate, Applied, Claim, CommandMeta, DomainEvent, EventLogError, EventStore, Expected,
    NewEvent, Repository, SnapshotPolicy, StreamId, TenantId, request_hash,
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
    assert_eq!(outcome.events, [] as [eventlog_core::RecordedEvent; 0]);
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
        .redact(&stream, 1, "subject-request")
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

fn claimed(key: &str, claim_key: &str) -> CommandMeta {
    CommandMeta {
        claim: Some(Claim::new("caller-1", claim_key, "create-digest").expect("valid claim")),
        ..meta(key)
    }
}

#[tokio::test]
async fn a_claimed_retry_under_a_new_id_answers_with_the_recorded_stream() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>)
        .with_policy(SnapshotPolicy { every: 1 });
    let first = repository
        .handle(
            &tenant,
            "minted-1",
            &Command::Add(3),
            &claimed("create", "create"),
        )
        .await
        .expect("handled");

    // The caller lost the answer and retried; the owner minted a fresh id for the retry.
    let retry = repository
        .handle(
            &tenant,
            "minted-2",
            &Command::Add(3),
            &claimed("create", "create"),
        )
        .await
        .expect("answered");
    assert!(retry.deduplicated);
    assert_eq!(retry.events[0].stream_id, "minted-1");
    assert_eq!(
        retry.state, first.state,
        "the answer is the recorded stream's state, not its events folded into the retry id"
    );
    assert_eq!(retry.version, first.version);

    let retry_stream = repository.stream(&tenant, "minted-2").expect("valid");
    assert!(
        store
            .load_snapshot(&retry_stream)
            .await
            .expect("readable")
            .is_none(),
        "no cache is written under an id that holds no events"
    );
    let unwritten = repository
        .load(&tenant, "minted-2")
        .await
        .expect("loadable");
    assert!(unwritten.is_new, "the retry id still holds nothing");
    assert_eq!(unwritten.state.total, 0);
}

#[tokio::test]
async fn a_claim_recorded_for_another_aggregate_type_is_refused_not_folded() {
    let (store, tenant) = fixture().await;
    Repository::<Counter>::new(Arc::clone(&store) as Arc<dyn EventStore>)
        .handle(
            &tenant,
            "minted-1",
            &Command::Add(3),
            &claimed("create", "create"),
        )
        .await
        .expect("handled");
    let other = Repository::<eventlog_conformance::SnapshotCounter>::new(
        Arc::clone(&store) as Arc<dyn EventStore>
    );
    let error = other
        .handle(&tenant, "minted-2", &(), &claimed("create", "create"))
        .await
        .expect_err("a counter's events are not a snapshot counter's history");
    assert_eq!(
        error,
        EventLogError::IdempotencyMismatch {
            key: "create".to_owned()
        }
    );
}

/// An aggregate whose command decides `n` events, with a per-thread count of folded events so a
/// test can see whether a load started from a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Batch {
    total: u64,
}

thread_local! {
    static BATCH_FOLDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn batch_folds() -> u64 {
    BATCH_FOLDS.with(std::cell::Cell::get)
}

impl Aggregate for Batch {
    type Command = usize;
    type Event = eventlog_conformance::SnapshotIncrement;
    type Error = EventLogError;
    const TYPE: &'static str = "batch";
    const STATE_SCHEMA_VERSION: u32 = 1;
    fn empty(_: &str) -> Self {
        Self { total: 0 }
    }
    fn apply(&mut self, _: &Applied<'_, Self::Event>) {
        BATCH_FOLDS.with(|folds| folds.set(folds.get() + 1));
        self.total += 1;
    }
    fn decide(&self, n: &usize) -> Result<Vec<Self::Event>, Self::Error> {
        Ok((0..*n)
            .map(|_| eventlog_conformance::SnapshotIncrement)
            .collect())
    }
}

#[tokio::test]
async fn multi_event_commands_that_cross_the_interval_write_a_snapshot() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Batch>::new(Arc::clone(&store) as Arc<dyn EventStore>)
        .with_policy(SnapshotPolicy { every: 4 });
    // Revisions 1, 3, 5, 7, 9: no command ends on a multiple of 4.
    for (step, n) in [1_usize, 2, 2, 2, 2].into_iter().enumerate() {
        repository
            .handle(&tenant, "b-1", &n, &meta(&format!("batch-{step}")))
            .await
            .expect("handled");
    }
    let stream = repository.stream(&tenant, "b-1").expect("valid");
    let snapshot = store
        .load_snapshot(&stream)
        .await
        .expect("readable")
        .expect("crossing the interval wrote one");
    assert_eq!(snapshot.version, 9, "written at the committed revision");

    let before = batch_folds();
    let loaded = repository.load(&tenant, "b-1").await.expect("loadable");
    assert_eq!((loaded.version, loaded.state.total), (9, 9));
    assert_eq!(
        batch_folds() - before,
        0,
        "the load started from the snapshot"
    );
}

#[tokio::test]
async fn loading_an_absent_aggregate_writes_no_storage_rows() {
    let directory = tempfile::tempdir().expect("a directory");
    let path = directory.path().join("reads.db");
    let store = Arc::new(
        SqliteEventStore::open(path.to_str().expect("utf-8"), "reads")
            .await
            .expect("a store"),
    );
    let tenant = TenantId::new("tenant-a").expect("valid tenant");
    let repository = Repository::<Batch>::new(Arc::clone(&store) as Arc<dyn EventStore>);
    for i in 0..3 {
        let loaded = repository
            .load(&tenant, &format!("absent-{i}"))
            .await
            .expect("loadable");
        assert!(loaded.is_new);
    }
    let sql = rusqlite::Connection::open(&path).expect("readable");
    let generations: i64 = sql
        .query_row(
            "SELECT count(*) FROM reads_snapshot_generations",
            [],
            |row| row.get(0),
        )
        .expect("countable");
    assert_eq!(generations, 0, "a read leaves nothing behind");

    // A read still starts from a proven cache.
    repository
        .handle(&tenant, "present", &3, &meta("present"))
        .await
        .expect("handled");
    repository
        .snapshot_now(&tenant, "present")
        .await
        .expect("snapshotted");
    let before = batch_folds();
    let loaded = repository.load(&tenant, "present").await.expect("loadable");
    assert_eq!(loaded.state.total, 3);
    assert_eq!(
        batch_folds() - before,
        0,
        "the load started from the snapshot"
    );
}

#[tokio::test]
async fn a_delayed_older_snapshot_does_not_move_the_replay_point_back() {
    let (store, _) = fixture().await;
    eventlog_conformance::run_snapshot_revision_is_monotonic(store.as_ref()).await;
}

/// What a concurrent writer does between this aggregate's load and its append.
type Race = Box<dyn FnOnce() + Send>;
static RACE: std::sync::Mutex<Option<Race>> = std::sync::Mutex::new(None);

/// A counter whose `decide` runs the pending race once, so a test can commit other events after
/// the repository loaded the stream and before it appends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Raced {
    total: i64,
}

impl Aggregate for Raced {
    type Command = i64;
    type Event = Tally;
    type Error = EventLogError;
    const TYPE: &'static str = "raced";
    const STATE_SCHEMA_VERSION: u32 = 1;
    fn empty(_: &str) -> Self {
        Self { total: 0 }
    }
    fn apply(&mut self, applied: &Applied<'_, Self::Event>) {
        if let Applied::Happened {
            event: Tally::Added(amount),
            ..
        } = applied
        {
            self.total += amount;
        }
    }
    fn decide(&self, amount: &i64) -> Result<Vec<Self::Event>, Self::Error> {
        let race = RACE.lock().expect("unpoisoned").take();
        if let Some(race) = race {
            race();
        }
        Ok(vec![Tally::Added(*amount)])
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_same_stream_duplicate_after_a_concurrent_write_answers_the_whole_history() {
    let (store, tenant) = fixture().await;
    let repository = Repository::<Raced>::new(Arc::clone(&store) as Arc<dyn EventStore>)
        .with_policy(SnapshotPolicy { every: 3 });
    repository
        .handle(&tenant, "r-1", &1, &meta("r-first"))
        .await
        .expect("handled");

    // After this caller loads revision 1, another writer commits revision 2, and a retry of this
    // very command commits revision 3. The append then answers with the recorded revision 3.
    let stream = repository.stream(&tenant, "r-1").expect("valid");
    let writer = Arc::clone(&store);
    *RACE.lock().expect("unpoisoned") = Some(Box::new(move || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                writer
                    .append(
                        &stream,
                        Expected::Exact(1),
                        &[
                            NewEvent::new("tally.added", 1, json!({ "amount": 10 }))
                                .expect("valid"),
                        ],
                        &meta("r-other"),
                    )
                    .await
                    .expect("the other writer commits revision 2");
                writer
                    .append(
                        &stream,
                        Expected::Exact(2),
                        &[NewEvent::new("tally.added", 1, json!({ "amount": 5 })).expect("valid")],
                        &meta("r-retried"),
                    )
                    .await
                    .expect("the retry commits revision 3");
            });
        });
    }));
    let outcome = repository
        .handle(&tenant, "r-1", &5, &meta("r-retried"))
        .await
        .expect("answered");
    assert!(outcome.deduplicated);
    assert_eq!(outcome.version, 3);
    assert_eq!(
        outcome.state.total, 16,
        "revision 2 is part of the answer, not skipped by folding revision 3 onto revision 1"
    );
    assert_eq!(
        repository
            .load(&tenant, "r-1")
            .await
            .expect("loadable")
            .state
            .total,
        16,
        "no cache of the incomplete fold was written"
    );
}
