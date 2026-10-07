//! Durable capture continuity on a real SQLite file: a checkpoint a later process continues from.
//!
//! Design: `docs/design/durable-capture-continuity.md`, whose Acceptance section is this file's
//! list. Every step opens its own `SqliteEventStore`, so nothing proved here rests on the
//! in-process journal of the object that issued a checkpoint: a later step sees only the file.

use eventlog_core::{
    AppendGroup, AtomicBlobEventStore, AtomicEventStore, BlobAppendGroup, BlobWrite, BoxFuture,
    CaptureBudget, CaptureCheckpoint, CaptureError, CaptureLimits, CaptureResource, CaptureUsage,
    CapturedProjection, ConsistentTenantCapture, DurableCaptureCheckpoint, EventLogError,
    EventStore, Expected, InlineProjectionAdmin, NewEvent, NoGuard, ProjectionCaptureRefusal,
    ProjectionSpec, ProjectionStore, Projector, RecordedEvent, Snapshot, StreamAppend, StreamId,
    TenantCapture, TenantCaptureDelta, TenantCaptureUpdate, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

const PREFIX: &str = "durable";
const EVENTS: &str = "durable_events";
const BLOBS: &str = "durable_blobs";
const IDENTITY: &str = "durable_identity";
const ROW_TABLE: &str = "durable_p_durable_rows";
const LATE_TABLE: &str = "durable_p_durable_late";
const CONTINUITY: &str = "durable_capture_continuity";
const JOURNAL: &str = "durable_capture_journal";
const MIB: u64 = 1 << 20;

const ROWS: ProjectionSpec = ProjectionSpec {
    name: "durable_rows",
    indexed: &["kind"],
};
const LATE: ProjectionSpec = ProjectionSpec {
    name: "durable_late",
    indexed: &[],
};

fn tenant() -> TenantId {
    TenantId::new("durable-tenant").unwrap()
}

fn neighbour() -> TenantId {
    TenantId::new("neighbour-tenant").unwrap()
}

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 4096,
        max_blobs: 4096,
        max_projection_rows: 4096,
        max_payload_bytes: 256 * MIB,
    }
}

/// `{"key": k, "value": v}` sets row `k` to `v`, a present JSON null included; `{"key": k,
/// "remove": true}` deletes it.
struct Rows;

impl Projector for Rows {
    fn name(&self) -> &'static str {
        "durable_rows"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        &[ROWS]
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let Some(key) = event.data["key"].as_str() else {
                return Ok(());
            };
            if event.data.get("remove").is_some() {
                store.delete(&ROWS, &event.tenant, key).await
            } else {
                store
                    .upsert(&ROWS, &event.tenant, key, &event.data["value"])
                    .await
            }
        })
    }
}

/// A projection that exists only from the moment a test creates it.
struct Late;

impl Projector for Late {
    fn name(&self) -> &'static str {
        "durable_late"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        &[LATE]
    }

    fn apply<'a>(
        &'a self,
        _: &'a RecordedEvent,
        _: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Ok(()) })
    }
}

/// One database file, opened afresh for every step.
struct File {
    directory: tempfile::TempDir,
    path: PathBuf,
}

impl File {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite3");
        Self { directory, path }
    }

    fn name(&self) -> &str {
        self.path.to_str().unwrap()
    }

    /// A new store object, as a new process opens it.
    async fn open(&self) -> SqliteEventStore {
        let store = SqliteEventStore::open(self.name(), PREFIX).await.unwrap();
        store.register_inline(Arc::new(Rows)).await.unwrap();
        store
    }

    fn raw(&self) -> Connection {
        Connection::open(&self.path).unwrap()
    }

    /// A byte copy of the database file, taken while no store has it open.
    fn copy(&self, name: &str) -> PathBuf {
        let raw = self.raw();
        raw.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(raw);
        let copy = self.directory.path().join(name);
        std::fs::copy(&self.path, &copy).unwrap();
        copy
    }

    /// Put an older copy back in place of the file, as a restore from backup does.
    fn restore(&self, copy: &Path) {
        for suffix in ["-wal", "-shm"] {
            let mut side = self.path.as_os_str().to_owned();
            side.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(side));
        }
        std::fs::copy(copy, &self.path).unwrap();
    }

    /// Every schema object, without root pages, which differ between equal schemas.
    fn schema(&self) -> Vec<(String, String, String, Option<String>)> {
        let raw = self.raw();
        let mut statement = raw
            .prepare("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name")
            .unwrap();
        statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn schema_version(&self) -> i64 {
        self.raw()
            .query_row("PRAGMA schema_version", [], |row| row.get(0))
            .unwrap()
    }

    /// The continuity row and the newest journal position.
    fn mark(&self) -> (String, i64, String, i64) {
        let raw = self.raw();
        let (instance, epoch, token) = raw
            .query_row(
                &format!("SELECT instance, epoch, token FROM {CONTINUITY}"),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let position = raw
            .query_row(
                &format!("SELECT COALESCE(MAX(position), 0) FROM {JOURNAL}"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        (instance, epoch, token, position)
    }

    fn journal(&self) -> (i64, i64) {
        self.raw()
            .query_row(
                &format!("SELECT COUNT(*), COALESCE(SUM(bytes), 0) FROM {JOURNAL}"),
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn triggers_on(&self, table: &str) -> Vec<(String, String)> {
        let raw = self.raw();
        let mut statement = raw
            .prepare(
                "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?1
                 ORDER BY name",
            )
            .unwrap();
        statement
            .query_map([table], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
}

fn group(owner: &TenantId, key: &str, bodies: &[Value]) -> AppendGroup {
    AppendGroup {
        tenant: owner.clone(),
        meta: eventlog_conformance::meta(key, &json!({ "key": key })),
        appends: vec![StreamAppend {
            stream: StreamId::new(owner.clone(), "item", key).unwrap(),
            expected: Expected::Any,
            events: bodies
                .iter()
                .map(|body| NewEvent::new("item.changed", 1, body.clone()).unwrap())
                .collect(),
        }],
    }
}

fn changed(key: &str, row: &str, value: &Value) -> AppendGroup {
    group(&tenant(), key, &[json!({ "key": row, "value": value })])
}

/// A provisioned, seeded file on which durable continuity was enabled after the seed.
async fn provisioned() -> File {
    let file = File::new();
    let store = file.open().await;
    store.stream_identity(&tenant()).await.unwrap();
    store.stream_identity(&neighbour()).await.unwrap();
    store
        .append_group_guarded_with_blobs(
            &changed("seed", "a", &json!(1)),
            Arc::new(NoGuard),
            &[("seed".into(), b"seed bytes".to_vec())],
        )
        .await
        .unwrap();
    store
        .append_group(&changed(
            "seed-b",
            "b",
            &json!({ "kind": "nested", "n": [1, 2] }),
        ))
        .await
        .unwrap();
    store.enable_durable_continuity().await.unwrap();
    file
}

fn kind(update: &TenantCaptureUpdate) -> &'static str {
    match update {
        TenantCaptureUpdate::Complete { .. } => "Complete",
        TenantCaptureUpdate::Unchanged { .. } => "Unchanged",
        TenantCaptureUpdate::AppendDelta { .. } => "AppendDelta",
    }
}

async fn complete(
    store: &SqliteEventStore,
    projections: &[ProjectionSpec],
) -> (TenantCapture, CaptureCheckpoint) {
    match store
        .capture_tenant_since(&tenant(), projections, limits(), None)
        .await
        .unwrap()
    {
        TenantCaptureUpdate::Complete {
            capture,
            checkpoint: Some(checkpoint),
        } => (capture, checkpoint),
        other => panic!("a complete capture issues a checkpoint: {}", kind(&other)),
    }
}

async fn durable(
    store: &SqliteEventStore,
    projections: &[ProjectionSpec],
) -> (TenantCapture, DurableCaptureCheckpoint) {
    let (capture, checkpoint) = complete(store, projections).await;
    let durable = store
        .durable_checkpoint(&checkpoint)
        .expect("an enabled store offers the durable form of its checkpoint");
    (capture, durable)
}

/// Restore `durable` on `store` and continue from it.
async fn resume(
    store: &SqliteEventStore,
    durable: &DurableCaptureCheckpoint,
    projections: &[ProjectionSpec],
) -> TenantCaptureUpdate {
    let restored = store
        .restore_checkpoint(durable)
        .expect("the store restores a checkpoint its own file issued");
    store
        .capture_tenant_since(&tenant(), projections, limits(), Some(&restored))
        .await
        .unwrap()
}

/// What a complete capture of this value reports, by the shared accounting.
fn usage(capture: &TenantCapture) -> CaptureUsage {
    let mut budget = CaptureBudget::new(limits());
    for event in &capture.events {
        budget.admit_event(event).unwrap();
    }
    for blob in &capture.blobs {
        budget.admit_blob(blob.bytes.len() as u64).unwrap();
    }
    for projection in &capture.projections {
        for (_, body) in &projection.rows {
            budget.admit_projection_row(body).unwrap();
        }
    }
    budget.usage()
}

/// The complete capture a consumer holds after installing `delta` on `base`.
fn apply(mut base: TenantCapture, delta: &TenantCaptureDelta) -> TenantCapture {
    assert_eq!(base.tenant, delta.tenant);
    assert_eq!(base.stream_identity, delta.stream_identity);
    base.events.extend(delta.events.iter().cloned());
    base.blobs.extend(delta.blobs.iter().cloned());
    base.blobs
        .sort_by(|left, right| left.digest.as_bytes().cmp(right.digest.as_bytes()));
    let mut projections = Vec::new();
    for (held, changes) in base.projections.iter().zip(&delta.projections) {
        assert_eq!(held.specification, changes.specification);
        let mut rows: BTreeMap<String, Value> = held.rows.iter().cloned().collect();
        for row in &changes.rows {
            assert_eq!(rows.get(&row.key), row.before.as_ref(), "{}", row.key);
            match &row.after {
                Some(value) => rows.insert(row.key.clone(), value.clone()),
                None => rows.remove(&row.key),
            };
        }
        projections.push(CapturedProjection {
            specification: held.specification,
            rows: rows.into_iter().collect(),
        });
    }
    base.projections = projections;
    base
}

const SMUGGLE_EVENT: &str =
    "INSERT INTO durable_events (tenant_id, stream_type, stream_id, version,
         event_id, event_name, event_schema_version, occurred_at, recorded_at, subject, actor,
         request_id, trace_id, causation_id, causation_depth, data)
     SELECT tenant_id, stream_type, 'smuggled', 1, 'smuggled-event', event_name,
         event_schema_version, occurred_at, recorded_at, subject, actor, request_id, trace_id,
         causation_id, causation_depth, data
     FROM durable_events WHERE global_seq =
         (SELECT MIN(global_seq) FROM durable_events WHERE tenant_id = 'durable-tenant')";

/// One write through another connection.
type ForeignWrite = fn(&Connection);

/// One edit to decoded checkpoint JSON.
type Edit = fn(&mut Value);

/// Valid-looking writes through another connection: each leaves a store a complete capture
/// accepts, so only continuity can tell that something happened.
fn foreign_writes() -> Vec<(&'static str, ForeignWrite)> {
    vec![
        ("an event's data updated", |raw| {
            raw.execute_batch(
                "UPDATE durable_events SET data = '{\"key\":\"a\",\"value\":99}'
                 WHERE global_seq =
                     (SELECT MIN(global_seq) FROM durable_events WHERE tenant_id = 'durable-tenant')",
            )
            .unwrap();
        }),
        ("a blob's bytes updated", |raw| {
            let bytes = b"replacement bytes".as_slice();
            raw.execute(
                "UPDATE durable_blobs SET bytes = ?1, byte_count = ?2, integrity_sha256 = ?3
                 WHERE tenant_id = 'durable-tenant' AND digest = 'seed'",
                rusqlite::params![
                    bytes,
                    i64::try_from(bytes.len()).unwrap(),
                    eventlog_core::blob_integrity_sha256(bytes)
                ],
            )
            .unwrap();
        }),
        ("a blob deleted", |raw| {
            raw.execute_batch(
                "DELETE FROM durable_blobs WHERE tenant_id = 'durable-tenant' AND digest = 'seed'",
            )
            .unwrap();
        }),
        ("the stream identity updated", |raw| {
            raw.execute_batch(
                "UPDATE durable_identity SET stream_identity = 'replaced-identity'
                 WHERE tenant_id = 'durable-tenant'",
            )
            .unwrap();
        }),
        ("a projection row updated", |raw| {
            raw.execute_batch(
                "UPDATE durable_p_durable_rows SET body = '42'
                 WHERE tenant_id = 'durable-tenant' AND row_key = 'a'",
            )
            .unwrap();
        }),
        ("a projection row inserted", |raw| {
            raw.execute_batch(
                "INSERT INTO durable_p_durable_rows (tenant_id, row_key, body, idx_0)
                 VALUES ('durable-tenant', 'smuggled', '{\"kind\":\"smuggled\"}', 'smuggled')",
            )
            .unwrap();
        }),
        ("an event inserted by SQL", |raw| {
            raw.execute_batch(SMUGGLE_EVENT).unwrap();
        }),
    ]
}

#[tokio::test]
async fn a_restored_checkpoint_on_an_untouched_store_is_unchanged_with_complete_usage() {
    let file = provisioned().await;
    let (capture, saved) = {
        let store = file.open().await;
        let (capture, checkpoint) = complete(&store, &[ROWS]).await;
        assert_eq!(
            store.checkpoint_usage(&checkpoint),
            Some(usage(&capture)),
            "the issuing store binds the complete capture's usage"
        );
        let saved = store.durable_checkpoint(&checkpoint).expect("enabled");
        (capture, saved)
    };
    assert!(!capture.events.is_empty() && !capture.blobs.is_empty());

    let store = file.open().await;
    let restored = store
        .restore_checkpoint(&saved)
        .expect("its own file's bytes");
    assert_eq!(
        store.checkpoint_usage(&restored),
        Some(usage(&capture)),
        "a restored checkpoint binds the same usage"
    );
    let update = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&restored))
        .await
        .unwrap();
    let TenantCaptureUpdate::Unchanged { checkpoint } = update else {
        panic!("an untouched store gave {}", kind(&update));
    };
    assert_eq!(store.checkpoint_usage(&checkpoint), Some(usage(&capture)));
    let again = store
        .durable_checkpoint(&checkpoint)
        .expect("the returned checkpoint is again eligible for durable_checkpoint");
    drop(store);

    let store = file.open().await;
    let update = resume(&store, &again, &[ROWS]).await;
    assert!(
        matches!(update, TenantCaptureUpdate::Unchanged { .. }),
        "{}",
        kind(&update)
    );
}

#[tokio::test]
async fn groups_another_store_appended_continue_as_the_exact_in_process_delta() {
    let file = provisioned().await;
    let (base, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };

    let in_process = {
        let store = file.open().await;
        let (_, local) = complete(&store, &[ROWS]).await;
        store
            .append_group_guarded_with_blobs(
                &group(
                    &tenant(),
                    "one",
                    &[
                        json!({ "key": "a", "value": null }),
                        json!({ "key": "c", "value": 3 }),
                    ],
                ),
                Arc::new(NoGuard),
                &[
                    ("fresh".into(), b"fresh bytes".to_vec()),
                    ("seed".into(), b"seed bytes".to_vec()),
                ],
            )
            .await
            .unwrap();
        let two = group(
            &tenant(),
            "two",
            &[
                json!({ "key": "b", "remove": true }),
                json!({ "key": "a", "value": 5 }),
                json!({ "key": "e", "value": null }),
            ],
        );
        store.append_group(&two).await.unwrap();
        store
            .append_group_with_blobs_guarded(
                &BlobAppendGroup {
                    group: changed("three", "d", &json!({ "kind": "deep", "x": 1.25 })),
                    blobs: vec![BlobWrite {
                        digest: "atomic".into(),
                        bytes: b"atomic bytes".to_vec(),
                    }],
                },
                Arc::new(NoGuard),
            )
            .await
            .unwrap();
        assert!(
            store.append_group(&two).await.unwrap().deduplicated,
            "a retry adds nothing"
        );
        match store
            .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&local))
            .await
            .unwrap()
        {
            TenantCaptureUpdate::AppendDelta { delta, .. } => delta,
            other => panic!("in-process control: {}", kind(&other)),
        }
    };
    assert_eq!(in_process.events.len(), 6);

    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    let TenantCaptureUpdate::AppendDelta { checkpoint, delta } = update else {
        panic!("acknowledged groups gave {}", kind(&update));
    };
    assert_eq!(
        delta, in_process,
        "the durable delta is the in-process delta"
    );
    let full = store
        .capture_tenant(&tenant(), &[ROWS], limits())
        .await
        .unwrap();
    assert_eq!(apply(base, &delta), full);
    assert_eq!(delta.resulting_usage, usage(&full));
    assert_eq!(store.checkpoint_usage(&checkpoint), Some(usage(&full)));
    let next = store
        .durable_checkpoint(&checkpoint)
        .expect("an AppendDelta's checkpoint is again eligible");
    drop(store);

    {
        let store = file.open().await;
        store
            .append_group(&group(
                &neighbour(),
                "neighbour",
                &[json!({ "key": "n", "value": 1 })],
            ))
            .await
            .unwrap();
        store
            .append_group(&changed("four", "a", &json!(6)))
            .await
            .unwrap();
    }
    let store = file.open().await;
    let update = resume(&store, &next, &[ROWS]).await;
    let TenantCaptureUpdate::AppendDelta { delta, .. } = update else {
        panic!("another tenant's group in the chain gave {}", kind(&update));
    };
    assert_eq!(delta.events.len(), 1);
    assert!(delta.events.iter().all(|event| event.tenant == tenant()));
    let after = store
        .capture_tenant(&tenant(), &[ROWS], limits())
        .await
        .unwrap();
    assert_eq!(apply(full, &delta), after);
}

#[tokio::test]
async fn foreign_sql_writes_to_captured_material_give_complete_alone_and_before_a_group() {
    for (what, write) in foreign_writes() {
        for then_a_group in [false, true] {
            let file = provisioned().await;
            let (_, saved) = {
                let store = file.open().await;
                durable(&store, &[ROWS]).await
            };
            write(&file.raw());
            if then_a_group {
                let store = file.open().await;
                store
                    .append_group(&changed("after", "c", &json!(8)))
                    .await
                    .unwrap();
            }
            let store = file.open().await;
            let update = resume(&store, &saved, &[ROWS]).await;
            let TenantCaptureUpdate::Complete { capture, .. } = update else {
                panic!(
                    "{what}, then an acknowledged group: {then_a_group}: gave {}",
                    kind(&update)
                );
            };
            assert_eq!(
                capture,
                store
                    .capture_tenant(&tenant(), &[ROWS], limits())
                    .await
                    .unwrap(),
                "{what}"
            );
        }
    }
}

#[tokio::test]
async fn a_trigger_dropped_and_recreated_gives_complete() {
    for then_a_group in [false, true] {
        let file = provisioned().await;
        let (_, saved) = {
            let store = file.open().await;
            durable(&store, &[ROWS]).await
        };
        let raw = file.raw();
        let (name, sql): (String, String) = raw
            .query_row(
                "SELECT name, sql FROM sqlite_master
                 WHERE type = 'trigger' AND tbl_name = ?1 AND instr(sql, 'AFTER INSERT') > 0",
                [EVENTS],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        raw.execute_batch(&format!("DROP TRIGGER {name}")).unwrap();
        raw.execute_batch(SMUGGLE_EVENT).unwrap();
        raw.execute_batch(&sql).unwrap();
        drop(raw);
        assert!(
            file.triggers_on(EVENTS).contains(&(name, sql)),
            "the same trigger, by name and text, is back"
        );
        if then_a_group {
            let store = file.open().await;
            store
                .append_group(&changed("after", "c", &json!(8)))
                .await
                .unwrap();
        }
        let store = file.open().await;
        let update = resume(&store, &saved, &[ROWS]).await;
        let TenantCaptureUpdate::Complete { capture, .. } = update else {
            panic!(
                "a trigger dropped and recreated (then a group: {then_a_group}) gave {}",
                kind(&update)
            );
        };
        assert!(
            capture
                .events
                .iter()
                .any(|event| event.stream_id == "smuggled")
        );
    }
}

#[tokio::test]
async fn a_journal_pruned_past_the_checkpoint_gives_complete() {
    let file = provisioned().await;
    let (_, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    {
        let store = file.open().await;
        for index in 0..129 {
            store
                .append_group(&changed(&format!("group-{index}"), "a", &json!(index)))
                .await
                .unwrap();
        }
    }
    assert_eq!(
        file.journal().0,
        128,
        "the journal keeps the newest 128 groups"
    );
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    assert!(
        matches!(update, TenantCaptureUpdate::Complete { .. }),
        "a pruned entry left a gap, yet {}",
        kind(&update)
    );
}

#[tokio::test]
async fn the_journal_keeps_at_most_sixteen_mebibytes_and_never_an_oversized_entry() {
    let file = provisioned().await;
    let (_, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    let big = |index: usize| {
        let filler = "x".repeat(usize::try_from(7 * MIB).unwrap());
        changed(
            &format!("big-{index}"),
            &format!("big-{index}"),
            &json!(filler),
        )
    };
    {
        let store = file.open().await;
        for index in 0..3 {
            store.append_group(&big(index)).await.unwrap();
        }
    }
    let (count, bytes) = file.journal();
    assert_eq!(count, 2, "the oldest whole entry went");
    assert!(bytes <= i64::try_from(16 * MIB).unwrap(), "{bytes}");
    assert_eq!(file.mark().3, 3, "the newest entry stays");

    let (_, recent) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    {
        let store = file.open().await;
        let filler = "y".repeat(usize::try_from(17 * MIB).unwrap());
        store
            .append_group(&changed("oversized", "o", &json!(filler)))
            .await
            .unwrap();
    }
    assert_eq!(
        file.journal().0,
        2,
        "an entry over the bound is never written"
    );
    let store = file.open().await;
    for (what, checkpoint) in [("pruned", &saved), ("before the oversized group", &recent)] {
        let update = resume(&store, checkpoint, &[ROWS]).await;
        assert!(
            matches!(update, TenantCaptureUpdate::Complete { .. }),
            "{what}: {}",
            kind(&update)
        );
    }
}

#[tokio::test]
async fn an_older_copy_of_the_file_gives_complete() {
    let file = provisioned().await;
    let older = file.copy("older.sqlite3");
    {
        let store = file.open().await;
        for index in 0..3 {
            store
                .append_group(&changed(&format!("lost-{index}"), "a", &json!(index)))
                .await
                .unwrap();
        }
    }
    let (_, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    file.restore(&older);
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    assert!(
        matches!(update, TenantCaptureUpdate::Complete { .. }),
        "{}",
        kind(&update)
    );
}

#[tokio::test]
async fn an_older_copy_written_as_many_times_as_it_lost_gives_complete() {
    let file = provisioned().await;
    let older = file.copy("older.sqlite3");
    let lost = || async {
        let store = file.open().await;
        for index in 0..3 {
            store
                .append_group(&changed(&format!("lost-{index}"), "a", &json!(index)))
                .await
                .unwrap();
        }
    };
    lost().await;
    let (_, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    let newer = (file.mark(), file.schema_version());
    file.restore(&older);
    lost().await;
    let rewritten = (file.mark(), file.schema_version());
    assert_eq!(
        (&rewritten.0.0, rewritten.0.1, rewritten.0.3, rewritten.1),
        (&newer.0.0, newer.0.1, newer.0.3, newer.1),
        "control: instance, epoch, journal position and schema version all coincide"
    );
    assert_ne!(rewritten.0.2, newer.0.2, "only the random token differs");
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    assert!(
        matches!(update, TenantCaptureUpdate::Complete { .. }),
        "an older copy written as often as it lost gave {}",
        kind(&update)
    );
}

#[tokio::test]
async fn a_foreign_write_between_the_observation_and_the_durable_call_gives_complete() {
    let file = provisioned().await;
    let store = file.open().await;
    let (_, checkpoint) = complete(&store, &[ROWS]).await;
    file.raw()
        .execute_batch(
            "UPDATE durable_events SET data = '{\"key\":\"a\",\"value\":77}'
             WHERE global_seq =
                 (SELECT MIN(global_seq) FROM durable_events WHERE tenant_id = 'durable-tenant')",
        )
        .unwrap();
    let saved = store
        .durable_checkpoint(&checkpoint)
        .expect("the checkpoint was issued on an enabled store");
    drop(store);
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    let TenantCaptureUpdate::Complete { capture, .. } = update else {
        panic!(
            "a write after the observation was absorbed: {}",
            kind(&update)
        );
    };
    assert_eq!(capture.events[0].data, json!({ "key": "a", "value": 77 }));
}

#[tokio::test]
async fn restore_gives_none_for_bytes_this_store_cannot_continue_from() {
    let file = provisioned().await;
    let (_, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    let bytes = saved.as_bytes().to_vec();
    let decoded: Value = serde_json::from_slice(&bytes).unwrap();
    let mut cases: Vec<(&str, Vec<u8>)> = vec![
        ("empty", Vec::new()),
        ("truncated by half", bytes[..bytes.len() / 2].to_vec()),
        ("truncated by one byte", bytes[..bytes.len() - 1].to_vec()),
        ("not JSON", vec![0, 159, 146, 150]),
    ];
    let edits: [(&str, Edit); 9] = [
        ("a foreign provider's format", |value| {
            value["format"] = json!("eventlog-postgres/durable-capture-checkpoint");
        }),
        ("a newer version", |value| value["version"] = json!(2)),
        ("another prefix", |value| value["prefix"] = json!("other")),
        ("another store instance", |value| {
            value["store_instance"] = json!("0123456789abcdef0123456789abcdef");
        }),
        ("an extra property", |value| value["extra"] = json!(1)),
        ("a missing property", |value| {
            value.as_object_mut().unwrap().remove("mark");
        }),
        ("a negative position", |value| value["position"] = json!(-1)),
        ("a token that is not 32 hex digits", |value| {
            value["mark"]["token"] = json!("not-a-token");
        }),
        ("an invalid tenant", |value| {
            value["scope"]["capture"]["tenant_id"] = json!("");
        }),
    ];
    for (what, edit) in edits {
        let mut edited = decoded.clone();
        edit(&mut edited);
        cases.push((what, serde_json::to_vec(&edited).unwrap()));
    }
    let other = provisioned().await;
    let (_, elsewhere) = {
        let store = other.open().await;
        durable(&store, &[ROWS]).await
    };
    cases.push(("another file's checkpoint", elsewhere.as_bytes().to_vec()));

    let store = file.open().await;
    assert!(
        store.restore_checkpoint(&saved).is_some(),
        "control: its own bytes restore"
    );
    for (what, case) in cases {
        assert!(
            store
                .restore_checkpoint(&DurableCaptureCheckpoint::from_bytes(case))
                .is_none(),
            "{what} restored"
        );
    }
}

#[tokio::test]
async fn a_store_never_enabled_offers_no_durable_checkpoint() {
    let enabled = provisioned().await;
    let (_, saved) = {
        let store = enabled.open().await;
        durable(&store, &[ROWS]).await
    };
    let file = File::new();
    let store = file.open().await;
    store.stream_identity(&tenant()).await.unwrap();
    store
        .append_group(&changed("one", "a", &json!(1)))
        .await
        .unwrap();
    let (capture, checkpoint) = complete(&store, &[ROWS]).await;
    assert_eq!(store.durable_checkpoint(&checkpoint), None);
    assert_eq!(
        store.checkpoint_usage(&checkpoint),
        Some(usage(&capture)),
        "in-process continuity is unaffected"
    );
    assert!(store.restore_checkpoint(&saved).is_none());
}

#[tokio::test]
async fn enabling_twice_changes_nothing() {
    let file = provisioned().await;
    let before = (file.schema(), file.schema_version(), file.mark());
    {
        let store = file.open().await;
        store.enable_durable_continuity().await.unwrap();
    }
    assert_eq!(
        (file.schema(), file.schema_version(), file.mark()),
        before,
        "a second enable changed the schema, the instance or the mark"
    );
}

#[tokio::test]
async fn enabling_refuses_a_store_carrying_a_foreign_trigger() {
    for trigger in [
        "CREATE TRIGGER audit AFTER INSERT ON durable_snapshots BEGIN SELECT 1; END",
        "CREATE TRIGGER durable_events_capture_insert AFTER INSERT ON durable_events
         BEGIN SELECT 1; END",
    ] {
        let file = File::new();
        drop(file.open().await);
        file.raw().execute_batch(trigger).unwrap();
        let before = file.schema();
        let store = file.open().await;
        let refused = store.enable_durable_continuity().await;
        assert!(
            matches!(refused, Err(EventLogError::Invalid(_))),
            "{trigger}: {refused:?}"
        );
        drop(store);
        assert_eq!(
            file.schema(),
            before,
            "a refused enable left nothing behind"
        );
    }
}

#[tokio::test]
async fn disabling_restores_the_never_enabled_schema_which_opens_and_attaches() {
    let never = File::new();
    let enabled = File::new();
    for file in [&never, &enabled] {
        let store = file.open().await;
        store.stream_identity(&tenant()).await.unwrap();
        store
            .append_group(&changed("one", "a", &json!(1)))
            .await
            .unwrap();
    }
    {
        let store = enabled.open().await;
        store.enable_durable_continuity().await.unwrap();
        store
            .append_group(&changed("two", "a", &json!(2)))
            .await
            .unwrap();
    }
    assert_eq!(
        enabled.triggers_on(EVENTS).len(),
        3,
        "control: it was enabled"
    );
    {
        let store = enabled.open().await;
        store.disable_durable_continuity().await.unwrap();
        store.disable_durable_continuity().await.unwrap();
    }
    assert_eq!(
        enabled.schema(),
        never.schema(),
        "disable leaves exactly the schema of a store that was never enabled"
    );
    assert!(
        enabled
            .schema()
            .iter()
            .all(|(kind, name, _, _)| kind != "trigger" && !name.contains("capture_")),
        "no provider trigger and no continuity table remain"
    );
    let store = SqliteEventStore::open_existing(enabled.name(), PREFIX)
        .await
        .unwrap();
    store.attach_inline_existing(Arc::new(Rows)).await.unwrap();
    let (_, checkpoint) = complete(&store, &[ROWS]).await;
    assert_eq!(store.durable_checkpoint(&checkpoint), None);
}

#[tokio::test]
async fn a_projection_created_after_enable_carries_the_provider_triggers() {
    let file = provisioned().await;
    {
        let store = file.open().await;
        store.create_projections(Arc::new(Late)).await.unwrap();
    }
    assert_eq!(
        file.triggers_on(LATE_TABLE)
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<BTreeSet<_>>(),
        file.triggers_on(ROW_TABLE)
            .iter()
            .map(|(name, _)| name.replace("durable_rows", "durable_late"))
            .collect(),
        "the new table carries the same three provider triggers"
    );
    let (_, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS, LATE]).await
    };
    file.raw()
        .execute_batch(
            "INSERT INTO durable_p_durable_late (tenant_id, row_key, body)
             VALUES ('durable-tenant', 'smuggled', '{}')",
        )
        .unwrap();
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS, LATE]).await;
    let TenantCaptureUpdate::Complete { capture, .. } = update else {
        panic!(
            "a foreign edit to the new projection gave {}",
            kind(&update)
        );
    };
    assert_eq!(capture.projections[1].rows.len(), 1);
}

/// A provisioned file with a projection table created after enable without the provider's
/// triggers, as a writer that bypasses the provider would create it.
async fn with_trigger_less_projection() -> File {
    let file = provisioned().await;
    file.raw()
        .execute_batch(
            "CREATE TABLE durable_p_durable_late (tenant_id TEXT NOT NULL, row_key TEXT NOT NULL,
                 body TEXT NOT NULL, PRIMARY KEY (tenant_id, row_key));
             INSERT INTO durable_projection_registry (projection_name, indexed_fields)
             VALUES ('durable_late', '[]');",
        )
        .unwrap();
    file
}

#[tokio::test]
async fn a_checkpoint_taken_while_a_projection_lacks_provider_triggers_has_no_durable_form() {
    let file = with_trigger_less_projection().await;
    let store = file.open().await;
    let (_, checkpoint) = complete(&store, &[ROWS, LATE]).await;
    assert_eq!(
        store.durable_checkpoint(&checkpoint),
        None,
        "a checkpoint taken while a requested table carries no provider trigger"
    );
}

#[tokio::test]
async fn a_scope_edited_to_name_a_projection_without_provider_triggers_gives_complete() {
    let file = with_trigger_less_projection().await;
    // The bytes are the consumer's: a scope edited to name that table must not continue.
    let (_, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    let mut edited: Value = serde_json::from_slice(saved.as_bytes()).unwrap();
    edited["scope"]["projections"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "name": "durable_late", "indexed_fields": [] }));
    let forged = DurableCaptureCheckpoint::from_bytes(serde_json::to_vec(&edited).unwrap());
    file.raw()
        .execute_batch(
            "INSERT INTO durable_p_durable_late (tenant_id, row_key, body)
             VALUES ('durable-tenant', 'smuggled', '{}')",
        )
        .unwrap();
    let store = file.open().await;
    let update = resume(&store, &forged, &[ROWS, LATE]).await;
    let TenantCaptureUpdate::Complete { capture, .. } = update else {
        panic!(
            "an edit to a table without provider triggers gave {}",
            kind(&update)
        );
    };
    assert_eq!(capture.projections[1].rows.len(), 1);
}

/// The provider's own snapshot write: a generation, then a checked save.
async fn snapshot(store: &SqliteEventStore) {
    let stream = StreamId::new(tenant(), "item", "seed").unwrap();
    let generation = store.snapshot_generation(&stream).await.unwrap().unwrap();
    let snapshot = Snapshot {
        version: 1,
        state_schema_version: 1,
        state: json!({ "count": 1 }),
        recorded_at: time::OffsetDateTime::now_utc(),
    };
    assert!(
        store
            .save_snapshot_checked(&stream, &snapshot, &generation)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn snapshot_writes_keep_durable_continuity() {
    let file = provisioned().await;
    let (_, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    snapshot(&file.open().await).await;
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    assert!(
        matches!(update, TenantCaptureUpdate::Unchanged { .. }),
        "durable continuity across the provider's own snapshot write: {}",
        kind(&update)
    );
}

/// On a store without durable continuity, so nothing but the in-process journal can answer.
#[tokio::test]
async fn snapshot_writes_keep_in_process_continuity() {
    let file = File::new();
    let store = file.open().await;
    store.stream_identity(&tenant()).await.unwrap();
    store
        .append_group(&changed("seed", "a", &json!(1)))
        .await
        .unwrap();
    let (_, local) = complete(&store, &[ROWS]).await;
    assert_eq!(
        store.durable_checkpoint(&local),
        None,
        "control: no durable path"
    );
    snapshot(&store).await;
    let update = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&local))
        .await
        .unwrap();
    let TenantCaptureUpdate::Unchanged { checkpoint } = update else {
        panic!(
            "in-process continuity across the provider's own snapshot write: {}",
            kind(&update)
        );
    };
    store
        .append_group(&changed("after", "a", &json!(2)))
        .await
        .unwrap();
    let update = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&checkpoint))
        .await
        .unwrap();
    assert!(
        matches!(update, TenantCaptureUpdate::AppendDelta { .. }),
        "the checkpoint returned across a snapshot write continues: {}",
        kind(&update)
    );
}

#[tokio::test]
async fn redaction_and_tenant_erasure_leave_no_journal_entry() {
    let file = provisioned().await;
    let journaled = |round: &'static str| {
        let file = &file;
        async move {
            let store = file.open().await;
            store
                .append_group(&changed(&format!("{round}-1"), "a", &json!("a row value")))
                .await
                .unwrap();
            store
                .append_group(&changed(&format!("{round}-2"), "a", &json!("another")))
                .await
                .unwrap();
            assert_eq!(file.journal().0, 2, "control: {round} wrote entries");
        }
    };
    journaled("first").await;
    {
        let store = file.open().await;
        store
            .redact(
                &StreamId::new(tenant(), "item", "seed").unwrap(),
                1,
                "erasure request",
            )
            .await
            .unwrap();
    }
    assert_eq!(file.journal().0, 0, "redaction left journal entries");
    journaled("second").await;
    {
        let store = file.open().await;
        store.forget_tenant(&tenant()).await.unwrap();
    }
    assert_eq!(file.journal().0, 0, "tenant erasure left journal entries");
}

#[tokio::test]
async fn standalone_append_put_blob_and_delete_blob_are_not_journaled() {
    for operation in ["append", "put_blob", "delete_blob"] {
        let file = provisioned().await;
        let (_, saved) = {
            let store = file.open().await;
            durable(&store, &[ROWS]).await
        };
        let before = file.journal();
        {
            let store = file.open().await;
            match operation {
                "append" => {
                    let item = StreamId::new(tenant(), "item", "standalone").unwrap();
                    store
                        .append(
                            &item,
                            Expected::Any,
                            &[
                                NewEvent::new("item.changed", 1, json!({ "key": "s", "value": 1 }))
                                    .unwrap(),
                            ],
                            &eventlog_conformance::meta("standalone", &json!({})),
                        )
                        .await
                        .unwrap();
                }
                "put_blob" => store
                    .put_blob(&tenant(), "standalone", b"standalone bytes")
                    .await
                    .unwrap(),
                _ => store.delete_blob(&tenant(), "seed").await.unwrap(),
            }
        }
        assert_eq!(file.journal(), before, "{operation} wrote a journal entry");
        let store = file.open().await;
        let update = resume(&store, &saved, &[ROWS]).await;
        assert!(
            matches!(update, TenantCaptureUpdate::Complete { .. }),
            "{operation}: {}",
            kind(&update)
        );
    }
}

/// Replace the provider's `AFTER INSERT` trigger on `table` with one of the same name whose text
/// differs: an admission check that compared names alone would admit it.
fn alter_trigger(file: &File, table: &str) {
    let raw = file.raw();
    let (name, sql): (String, String) = raw
        .query_row(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'trigger' AND tbl_name = ?1 AND instr(sql, 'AFTER INSERT') > 0",
            [table],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let altered = sql.replace("epoch + 1", "epoch + 2");
    assert_ne!(altered, sql, "control: the text changed");
    raw.execute_batch(&format!("DROP TRIGGER {name}; {altered}"))
        .unwrap();
}

#[tokio::test]
async fn the_trigger_checks_admit_exactly_the_provider_triggers() {
    // Every provider trigger in place: open, open_existing, attach and checkpoint issuance admit.
    let file = provisioned().await;
    for table in [EVENTS, BLOBS, IDENTITY, ROW_TABLE] {
        assert_eq!(file.triggers_on(table).len(), 3, "{table}");
    }
    {
        let existing = SqliteEventStore::open_existing(file.name(), PREFIX)
            .await
            .unwrap();
        existing
            .attach_inline_existing(Arc::new(Rows))
            .await
            .unwrap();
        complete(&existing, &[ROWS]).await;
    }

    // The blob table at open.
    let file = provisioned().await;
    alter_trigger(&file, BLOBS);
    assert!(SqliteEventStore::open(file.name(), PREFIX).await.is_err());
    assert!(
        SqliteEventStore::open_existing(file.name(), PREFIX)
            .await
            .is_err()
    );

    // A projection table at attach and at capture.
    let file = provisioned().await;
    alter_trigger(&file, ROW_TABLE);
    let existing = SqliteEventStore::open_existing(file.name(), PREFIX)
        .await
        .unwrap();
    assert!(
        existing
            .attach_inline_existing(Arc::new(Rows))
            .await
            .is_err()
    );
    assert!(matches!(
        existing.capture_tenant(&tenant(), &[ROWS], limits()).await,
        Err(CaptureError::ProjectionUnavailable {
            reason: ProjectionCaptureRefusal::PhysicalShapeMismatch,
            ..
        })
    ));
    drop(existing);

    // In-process continuity eligibility: still capturable, no checkpoint.
    let file = provisioned().await;
    alter_trigger(&file, IDENTITY);
    let store = file.open().await;
    assert!(matches!(
        store
            .capture_tenant_since(&tenant(), &[ROWS], limits(), None)
            .await
            .unwrap(),
        TenantCaptureUpdate::Complete {
            checkpoint: None,
            ..
        }
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn strict_inspection_admits_exactly_the_provider_triggers() {
    use eventlog_core::{InspectHistory, InspectionError};
    use eventlog_sqlite::SqliteHistoryInspector;
    for altered in [None, Some(EVENTS), Some(IDENTITY)] {
        let file = provisioned().await;
        if let Some(table) = altered {
            alter_trigger(&file, table);
        }
        file.raw()
            .execute_batch("PRAGMA journal_mode=DELETE")
            .unwrap();
        let inspected = SqliteHistoryInspector::new(&file.path, PREFIX)
            .inspect_history(&tenant(), eventlog_conformance::INSPECTION_LIMITS)
            .await;
        match altered {
            None => assert_eq!(inspected.unwrap().events.len(), 2),
            Some(table) => assert_eq!(
                inspected.map(|history| history.events.len()),
                Err(InspectionError::UnsupportedSource),
                "{table}"
            ),
        }
    }
}

#[tokio::test]
async fn request_limits_apply_to_the_whole_observation_a_restored_checkpoint_reaches() {
    let file = provisioned().await;
    let exact = {
        let store = file.open().await;
        let full = store
            .capture_tenant(&tenant(), &[ROWS], limits())
            .await
            .unwrap();
        CaptureLimits {
            max_events: u64::try_from(full.events.len()).unwrap(),
            ..limits()
        }
    };
    let saved = {
        let store = file.open().await;
        let update = store
            .capture_tenant_since(&tenant(), &[ROWS], exact, None)
            .await
            .unwrap();
        let TenantCaptureUpdate::Complete {
            checkpoint: Some(checkpoint),
            ..
        } = update
        else {
            panic!(
                "a capture at the exact cap issues a checkpoint: {}",
                kind(&update)
            );
        };
        store.durable_checkpoint(&checkpoint).expect("enabled")
    };
    {
        // A new row and one event: the delta alone fits every cap, the whole observation not.
        let store = file.open().await;
        store
            .append_group(&changed("over", "fresh", &json!(2)))
            .await
            .unwrap();
    }
    let store = file.open().await;
    let restored = store.restore_checkpoint(&saved).expect("its own bytes");
    assert!(
        matches!(
            store
                .capture_tenant_since(&tenant(), &[ROWS], exact, Some(&restored))
                .await,
            Err(CaptureError::LimitExceeded {
                resource: CaptureResource::Events,
                ..
            })
        ),
        "the cap applies to the base and the delta together, not to the delta alone"
    );
    let update = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&restored))
        .await
        .unwrap();
    assert!(
        matches!(update, TenantCaptureUpdate::Complete { .. }),
        "other limits are another scope: {}",
        kind(&update)
    );
}

#[tokio::test]
async fn owners_sharing_one_file_enable_and_continue_independently() {
    let file = provisioned().await;
    let neighbour = || async {
        let store = SqliteEventStore::open(file.name(), "neighbour")
            .await
            .unwrap();
        store.register_inline(Arc::new(Rows)).await.unwrap();
        store
    };
    {
        let other = neighbour().await;
        other.stream_identity(&tenant()).await.unwrap();
        other
            .append_group(&changed("other-seed", "a", &json!(1)))
            .await
            .unwrap();
        other
            .enable_durable_continuity()
            .await
            .expect("another owner's provider triggers are not foreign to this one");
    }
    let (_, mine) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    let (_, theirs) = {
        let other = neighbour().await;
        durable(&other, &[ROWS]).await
    };
    {
        let other = neighbour().await;
        other
            .append_group(&changed("other-next", "a", &json!(2)))
            .await
            .unwrap();
    }
    let store = file.open().await;
    let update = resume(&store, &mine, &[ROWS]).await;
    assert!(
        matches!(update, TenantCaptureUpdate::Unchanged { .. }),
        "another owner's group is none of this owner's material: {}",
        kind(&update)
    );
    assert!(
        store.restore_checkpoint(&theirs).is_none(),
        "another owner's bytes"
    );
    drop(store);
    let other = neighbour().await;
    let update = resume(&other, &theirs, &[ROWS]).await;
    let TenantCaptureUpdate::AppendDelta { delta, .. } = update else {
        panic!("the neighbour's own group gave {}", kind(&update));
    };
    assert_eq!(delta.events.len(), 1);
}

/// Resolve a committed schema by its ESS type name.
fn committed_schema(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../ess/capture-generated/schema/types")
        .join(format!("{name}.schema.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// The JSON Schema subset the ESS generator emits for these types, checked strictly: a keyword
/// this checker does not know is a refusal rather than a constraint silently skipped.
fn conforms(root: &Value, schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    const KNOWN: [&str; 10] = [
        "title",
        "x-ess-name",
        "x-ess-kind",
        "x-ess-invariants",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "minimum",
    ];
    let node = schema
        .as_object()
        .ok_or_else(|| format!("{path}: schema node is not an object"))?;
    if let Some(reference) = node.get("$ref") {
        let name = reference
            .as_str()
            .and_then(|reference| reference.strip_prefix("#/$defs/"))
            .ok_or_else(|| format!("{path}: unsupported $ref"))?;
        let target = root["$defs"]
            .get(name)
            .ok_or_else(|| format!("{path}: unresolved $ref {name}"))?;
        return conforms(root, target, value, path);
    }
    if node.is_empty() {
        return Ok(());
    }
    if let Some(unknown) = node.keys().find(|key| !KNOWN.contains(&key.as_str())) {
        return Err(format!("{path}: unchecked schema keyword {unknown}"));
    }
    match node.get("type").and_then(Value::as_str) {
        Some("object") => {
            let object = value
                .as_object()
                .ok_or_else(|| format!("{path}: not an object"))?;
            if node.get("additionalProperties") != Some(&Value::Bool(false)) {
                return Err(format!("{path}: schema admits additional properties"));
            }
            let properties = node["properties"]
                .as_object()
                .ok_or_else(|| format!("{path}: no properties"))?;
            for key in object.keys() {
                if !properties.contains_key(key) {
                    return Err(format!("{path}: extra property {key}"));
                }
            }
            for required in node["required"].as_array().into_iter().flatten() {
                let required = required.as_str().unwrap();
                if !object.contains_key(required) {
                    return Err(format!("{path}: missing required {required}"));
                }
            }
            for (key, member) in object {
                conforms(root, &properties[key], member, &format!("{path}.{key}"))?;
            }
            Ok(())
        }
        Some("array") => {
            let items = value
                .as_array()
                .ok_or_else(|| format!("{path}: not an array"))?;
            for (index, item) in items.iter().enumerate() {
                conforms(root, &node["items"], item, &format!("{path}[{index}]"))?;
            }
            Ok(())
        }
        Some("string") if value.is_string() => Ok(()),
        Some("integer") if value.is_i64() || value.is_u64() => {
            let minimum = match node.get("minimum") {
                None => None,
                Some(minimum) => Some(
                    minimum
                        .as_i64()
                        .ok_or_else(|| format!("{path}: unsupported minimum"))?,
                ),
            };
            // A value above i64::MAX is a u64 and above every minimum this subset can state.
            match (minimum, value.as_i64()) {
                (Some(minimum), Some(held)) if held < minimum => {
                    Err(format!("{path}: {held} is below the minimum {minimum}"))
                }
                _ => Ok(()),
            }
        }
        Some(expected) => Err(format!("{path}: not a {expected}: {value}")),
        None => Err(format!("{path}: schema node has no type")),
    }
}

#[tokio::test]
async fn the_durable_encodings_satisfy_the_committed_generated_schema() {
    let file = provisioned().await;
    let (_, saved) = {
        let store = file.open().await;
        durable(&store, &[ROWS]).await
    };
    let checkpoint: Value = serde_json::from_slice(saved.as_bytes()).unwrap();
    let schema = committed_schema("eventlog.capture.SqliteDurableCheckpoint");
    conforms(&schema, &schema, &checkpoint, "$").unwrap();
    let declared: BTreeSet<&String> =
        schema["$defs"]["eventlog.capture.SqliteDurableCheckpoint"]["properties"]
            .as_object()
            .unwrap()
            .keys()
            .collect();
    assert_eq!(
        checkpoint
            .as_object()
            .unwrap()
            .keys()
            .collect::<BTreeSet<_>>(),
        declared,
        "the encoding carries exactly the declared properties"
    );
    assert_eq!(
        checkpoint["format"],
        json!("eventlog-sqlite/durable-capture-checkpoint")
    );
    assert_eq!(checkpoint["version"], json!(1));

    // The checker refuses what it exists to refuse.
    let edits: [(&str, Edit); 5] = [
        ("an extra property", |value| value["extra"] = json!(1)),
        ("a nested extra property", |value| {
            value["scope"]["capture"]["extra"] = json!(1);
        }),
        ("a missing property", |value| {
            value["mark"].as_object_mut().unwrap().remove("token");
        }),
        ("a string where an integer belongs", |value| {
            value["scope"]["limits"]["max_events"] = json!("1");
        }),
        ("an integer below its minimum", |value| {
            value["version"] = json!(0);
        }),
    ];
    for (what, edit) in edits {
        let mut edited = checkpoint.clone();
        edit(&mut edited);
        assert!(
            conforms(&schema, &schema, &edited, "$").is_err(),
            "the checker admitted {what}"
        );
    }

    {
        let store = file.open().await;
        store
            .append_group_guarded_with_blobs(
                &group(
                    &tenant(),
                    "entry",
                    &[
                        json!({ "key": "b", "remove": true }),
                        json!({ "key": "n", "value": null }),
                        json!({ "key": "a", "value": { "kind": "k", "x": [1, 2] } }),
                    ],
                ),
                Arc::new(NoGuard),
                &[("entry".into(), b"entry bytes".to_vec())],
            )
            .await
            .unwrap();
    }
    let schema = committed_schema("eventlog.capture.DurableJournalEntry");
    let entries: Vec<String> = {
        let raw = file.raw();
        let mut statement = raw
            .prepare(&format!("SELECT entry FROM {JOURNAL} ORDER BY position"))
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert_eq!(entries.len(), 1);
    let entry: Value = serde_json::from_str(&entries[0]).unwrap();
    conforms(&schema, &schema, &entry, "$").unwrap();
    let rows: BTreeMap<&str, &Value> = entry["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| (row["key"].as_str().unwrap(), row))
        .collect();
    assert!(
        rows["b"].get("after").is_none(),
        "a deleted row has no after"
    );
    assert_eq!(
        rows["n"].get("after"),
        Some(&Value::Null),
        "a present null is a row"
    );
    assert!(rows["n"].get("before").is_none(), "a new row has no before");
    assert_eq!(entry["blobs"], json!(["entry"]));
}
