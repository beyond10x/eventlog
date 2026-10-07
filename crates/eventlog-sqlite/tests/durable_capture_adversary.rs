//! Adversarial cases against durable capture continuity on a real SQLite file.
//!
//! Each case drives the provider against `docs/design/durable-capture-continuity.md` from a
//! situation `durable_capture.rs` does not build. Every step opens its own store, as a later
//! process would, unless the case is about one handle's own state. A case that holds stays here
//! as a guard; a case that fails names the contract sentence it breaks.

use eventlog_core::{
    AdmissionPermit, AdmissionScope, AppendGroup, AtomicEventStore, BoxFuture, CaptureBudget,
    CaptureCheckpoint, CaptureError, CaptureLimits, CaptureUsage, CapturedProjection,
    ConsistentTenantCapture, DurableCaptureCheckpoint, EventLogError, EventStore, Expected, Guard,
    NewEvent, NoGuard, ProjectionCaptureRefusal, ProjectionSpec, ProjectionStore, Projector,
    RecordedEvent, Reservation, StreamAppend, StreamId, TenantCapture, TenantCaptureDelta,
    TenantCaptureUpdate, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

const PREFIX: &str = "adv";

const ROWS: ProjectionSpec = ProjectionSpec {
    name: "adv_rows",
    indexed: &["kind"],
};
/// The same physical table as [`ROWS`] under another declaration.
const ROWS_UNINDEXED: ProjectionSpec = ProjectionSpec {
    name: "adv_rows",
    indexed: &[],
};
const LATE_OK: ProjectionSpec = ProjectionSpec {
    name: "late_ok",
    indexed: &[],
};
const LATE_BAD: ProjectionSpec = ProjectionSpec {
    name: "Late-Bad",
    indexed: &[],
};
const LATE_ONE: ProjectionSpec = ProjectionSpec {
    name: "late_one",
    indexed: &[],
};
const LATE_TWO: ProjectionSpec = ProjectionSpec {
    name: "late_two",
    indexed: &[],
};
const INVALID_LATER: [ProjectionSpec; 2] = [LATE_OK, LATE_BAD];
const MISMATCHED_LATER: [ProjectionSpec; 2] = [LATE_ONE, LATE_TWO];

fn tenant() -> TenantId {
    TenantId::new("adv-tenant").unwrap()
}

fn neighbour() -> TenantId {
    TenantId::new("adv-neighbour").unwrap()
}

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 4096,
        max_blobs: 4096,
        max_projection_rows: 4096,
        max_payload_bytes: 1 << 28,
    }
}

/// `{"key": k, "value": v}` sets row `k` to `v`; `{"key": k, "remove": true}` deletes it.
struct Rows;

impl Projector for Rows {
    fn name(&self) -> &'static str {
        "adv_rows"
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

/// A projector whose declarations are named by a static list.
struct Declares(&'static [ProjectionSpec]);

impl Projector for Declares {
    fn name(&self) -> &'static str {
        "adv_declares"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        self.0
    }

    fn apply<'a>(
        &'a self,
        _: &'a RecordedEvent,
        _: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Ok(()) })
    }
}

/// An admission guard that reserves a counter and writes one row of `spec` in the group's
/// transaction, as a trusted host's guard may.
struct Writer {
    permit: AdmissionPermit,
    spec: ProjectionSpec,
}

impl Guard for Writer {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            store
                .reserve(
                    &self.permit,
                    &[Reservation {
                        scope: AdmissionScope::Tenant {
                            tenant: tenant(),
                            key: "quota".into(),
                        },
                        delta: 1,
                        ceiling: 1000,
                    }],
                )
                .await?;
            store
                .upsert(
                    &self.spec,
                    &tenant(),
                    "guarded",
                    &json!({ "kind": "guard" }),
                )
                .await
        })
    }
}

/// One database file, opened afresh for every step.
struct File {
    _directory: tempfile::TempDir,
    path: PathBuf,
}

impl File {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite3");
        Self {
            _directory: directory,
            path,
        }
    }

    fn name(&self) -> &str {
        self.path.to_str().unwrap()
    }

    /// A store of `prefix` with no projector registered.
    async fn bare(&self, prefix: &str) -> SqliteEventStore {
        SqliteEventStore::open(self.name(), prefix).await.unwrap()
    }

    async fn open_as(&self, prefix: &str) -> SqliteEventStore {
        let store = self.bare(prefix).await;
        store.register_inline(Arc::new(Rows)).await.unwrap();
        store
    }

    async fn open(&self) -> SqliteEventStore {
        self.open_as(PREFIX).await
    }

    fn raw(&self) -> Connection {
        Connection::open(&self.path).unwrap()
    }

    fn triggers_on(&self, table: &str) -> i64 {
        self.raw()
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?1",
                [table],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn has_table(&self, table: &str) -> bool {
        self.raw()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                [table],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn schema_version(&self) -> i64 {
        self.raw()
            .query_row("PRAGMA schema_version", [], |row| row.get(0))
            .unwrap()
    }

    /// The continuity row's mark of `adv`.
    fn mark(&self) -> (i64, String) {
        self.raw()
            .query_row(
                "SELECT epoch, token FROM adv_capture_continuity",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    /// The newest journal entry's `from` and `to` marks of `adv`.
    fn last_entry(&self) -> ((i64, String), (i64, String)) {
        self.raw()
            .query_row(
                "SELECT from_epoch, from_token, to_epoch, to_token FROM adv_capture_journal
                 ORDER BY position DESC LIMIT 1",
                [],
                |row| Ok(((row.get(0)?, row.get(1)?), (row.get(2)?, row.get(3)?))),
            )
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

/// A seeded file of `prefix` on which durable continuity was enabled after the seed.
async fn provisioned_as(file: &File, prefix: &str) {
    let store = file.open_as(prefix).await;
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
        .append_group(&changed("seed-b", "b", &json!({ "kind": "nested" })))
        .await
        .unwrap();
    store.enable_durable_continuity().await.unwrap();
}

async fn provisioned() -> File {
    let file = File::new();
    provisioned_as(&file, PREFIX).await;
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

/// The in-process delta `store` reports from `local` after it appended `then` itself.
async fn in_process_after(
    store: &SqliteEventStore,
    local: &CaptureCheckpoint,
    then: &AppendGroup,
) -> (TenantCaptureDelta, CaptureCheckpoint) {
    store.append_group(then).await.unwrap();
    match store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(local))
        .await
        .unwrap()
    {
        TenantCaptureUpdate::AppendDelta { delta, checkpoint } => (delta, checkpoint),
        other => panic!("in-process control: {}", kind(&other)),
    }
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

/// The complete capture a consumer holds after installing `delta` on `base`; `None` when a
/// row's `before` is not the value `base` holds for it.
fn apply(mut base: TenantCapture, delta: &TenantCaptureDelta) -> Option<TenantCapture> {
    base.events.extend(delta.events.iter().cloned());
    base.blobs.extend(delta.blobs.iter().cloned());
    base.blobs
        .sort_by(|left, right| left.digest.as_bytes().cmp(right.digest.as_bytes()));
    let mut projections = Vec::new();
    for (held, changes) in base.projections.iter().zip(&delta.projections) {
        let mut rows: BTreeMap<String, Value> = held.rows.iter().cloned().collect();
        for row in &changes.rows {
            if rows.get(&row.key) != row.before.as_ref() {
                return None;
            }
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
    Some(base)
}

fn row<'a>(capture: &'a TenantCapture, key: &str) -> &'a Value {
    &capture.projections[0]
        .rows
        .iter()
        .find(|(held, _)| held == key)
        .unwrap()
        .1
}

/// What a JSON number becomes after the store writes it as text and reads the text back,
/// exactly as a projection body, an event body and a journal entry are written and read.
fn reread(value: f64) -> f64 {
    let text = Value::from(value).to_string();
    serde_json::from_str::<Value>(&text)
        .unwrap()
        .as_f64()
        .unwrap()
}

fn same(left: f64, right: f64) -> bool {
    left.to_bits() == right.to_bits()
}

fn text_len(value: f64) -> usize {
    Value::from(value).to_string().len()
}

/// The first value of a fixed xorshift sequence over `f64` bit patterns, kept to normal values of
/// moderate magnitude, that satisfies `wanted`. Deterministic: the same value on every run.
fn find(what: &str, wanted: impl Fn(f64) -> bool) -> f64 {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..5_000_000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let value = f64::from_bits(state);
        if value.is_normal() && value.abs() > 1e-100 && value.abs() < 1e100 && wanted(value) {
            return value;
        }
    }
    panic!("no candidate value {what}");
}

// --- The JSON text the journal adds between a projection row and the delta ---------------------

#[tokio::test]
async fn a_row_value_the_journal_rereads_continues_with_the_before_the_base_holds() {
    // A value whose stored text reads back as y, and y's own text reads back as another value.
    let x = find("whose reread value drifts again", |x| {
        let stored = reread(x);
        !same(reread(stored), stored)
    });
    let file = provisioned().await;
    file.open()
        .await
        .append_group(&changed("float-seed", "f", &json!(x)))
        .await
        .unwrap();
    let (base, saved) = durable(&file.open().await, &[ROWS]).await;
    let held = row(&base, "f").clone();
    assert!(
        same(held.as_f64().unwrap(), reread(x)),
        "control: the base holds the reread value"
    );
    let in_process = {
        let store = file.open().await;
        let (_, local) = complete(&store, &[ROWS]).await;
        in_process_after(&store, &local, &changed("float-next", "f", &json!(1)))
            .await
            .0
    };
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    // A complete capture is always allowed; only the content of an AppendDelta is under test.
    let TenantCaptureUpdate::AppendDelta { delta, .. } = update else {
        return;
    };
    let change = delta.projections[0]
        .rows
        .iter()
        .find(|change| change.key == "f")
        .unwrap();
    assert_eq!(
        change.before.as_ref(),
        Some(&held),
        "the durable delta's before is not the value the base holds (x = {x:e}, bits {:#x})",
        x.to_bits()
    );
    assert_eq!(
        delta, in_process,
        "the durable delta is the in-process delta"
    );
}

#[tokio::test]
async fn the_durable_delta_is_the_in_process_delta_for_a_float_the_store_rewrites() {
    let x = find("that the store rewrites", |x| !same(reread(x), x));
    let file = provisioned().await;
    let (_, saved) = durable(&file.open().await, &[ROWS]).await;
    let in_process = {
        let store = file.open().await;
        let (_, local) = complete(&store, &[ROWS]).await;
        in_process_after(&store, &local, &changed("float", "g", &json!(x)))
            .await
            .0
    };
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    let TenantCaptureUpdate::AppendDelta { delta, .. } = update else {
        panic!("an acknowledged group gave {}", kind(&update));
    };
    assert_eq!(
        delta,
        in_process,
        "design § Contract: AppendDelta carries exactly the delta the in-process path returns \
         (x = {x:e}, bits {:#x})",
        x.to_bits()
    );
}

/// On a store without durable continuity: only the in-process journal answers, and its delta is
/// the reference the durable contract names.
#[tokio::test]
async fn an_in_process_delta_carrying_a_float_installs_to_the_complete_capture() {
    let x = find("that the store rewrites", |x| !same(reread(x), x));
    let file = File::new();
    let store = file.open().await;
    store.stream_identity(&tenant()).await.unwrap();
    store
        .append_group(&changed("seed", "a", &json!(1)))
        .await
        .unwrap();
    let (base, local) = complete(&store, &[ROWS]).await;
    let (delta, _) = in_process_after(&store, &local, &changed("float", "g", &json!(x))).await;
    let full = store
        .capture_tenant(&tenant(), &[ROWS], limits())
        .await
        .unwrap();
    assert_eq!(
        apply(base, &delta),
        Some(full.clone()),
        "base + in-process delta is not the complete capture (x = {x:e}, bits {:#x})",
        x.to_bits()
    );
    assert_eq!(delta.resulting_usage, usage(&full));
}

#[tokio::test]
async fn a_durable_checkpoint_issued_by_an_in_process_delta_binds_the_complete_usage() {
    let x = find("whose reread text has another length", |x| {
        text_len(x) != text_len(reread(x))
    });
    let file = provisioned().await;
    let saved = {
        let store = file.open().await;
        let (_, local) = complete(&store, &[ROWS]).await;
        let (_, checkpoint) =
            in_process_after(&store, &local, &changed("float", "g", &json!(x))).await;
        store
            .durable_checkpoint(&checkpoint)
            .expect("an AppendDelta's checkpoint is eligible")
    };
    let store = file.open().await;
    let restored = store.restore_checkpoint(&saved).expect("its own bytes");
    let full = store
        .capture_tenant(&tenant(), &[ROWS], limits())
        .await
        .unwrap();
    assert_eq!(
        store.checkpoint_usage(&restored),
        Some(usage(&full)),
        "design § Contract: checkpoint_usage is the counts a complete capture of that \
         observation reports (x = {x:e}, bits {:#x})",
        x.to_bits()
    );
}

// --- What the durable path no longer checks -----------------------------------------------------

#[tokio::test]
async fn a_foreign_edit_to_the_projection_registry_does_not_continue_as_unchanged() {
    let file = provisioned().await;
    let (_, saved) = durable(&file.open().await, &[ROWS]).await;
    file.raw()
        .execute_batch("DELETE FROM adv_projection_registry WHERE projection_name = 'adv_rows'")
        .unwrap();
    let store = file.bare(PREFIX).await;
    assert!(
        matches!(
            store.capture_tenant(&tenant(), &[ROWS], limits()).await,
            Err(CaptureError::ProjectionUnavailable {
                reason: ProjectionCaptureRefusal::Undeclared,
                ..
            })
        ),
        "control: a complete capture of this request is refused"
    );
    let restored = store.restore_checkpoint(&saved).expect("its own bytes");
    let update = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&restored))
        .await;
    assert!(
        !matches!(update, Ok(TenantCaptureUpdate::Unchanged { .. })),
        "a request a complete capture refuses continued as Unchanged"
    );
}

#[tokio::test]
async fn a_handle_opened_before_another_process_enabled_restores_this_stores_bytes() {
    let file = File::new();
    let early = file.open().await;
    early.stream_identity(&tenant()).await.unwrap();
    early
        .append_group(&changed("seed", "a", &json!(1)))
        .await
        .unwrap();
    file.open().await.enable_durable_continuity().await.unwrap();
    let (_, saved) = durable(&file.open().await, &[ROWS]).await;
    assert!(
        early.restore_checkpoint(&saved).is_some(),
        "design § Contract: restore refuses other-store-instance bytes, not this store's own"
    );
}

// --- Attacks that are expected to hold ----------------------------------------------------------

#[tokio::test]
async fn owners_a_and_a_p_in_one_file_keep_their_triggers_and_continuity_apart() {
    let file = File::new();
    provisioned_as(&file, "a").await;
    provisioned_as(&file, "a_p").await;
    for table in ["a_events", "a_p_adv_rows", "a_p_events", "a_p_p_adv_rows"] {
        assert_eq!(file.triggers_on(table), 3, "{table}");
    }
    let (_, mine) = durable(&file.open_as("a").await, &[ROWS]).await;
    let (_, theirs) = durable(&file.open_as("a_p").await, &[ROWS]).await;
    file.open_as("a_p")
        .await
        .append_group(&changed("a-p-next", "a", &json!(2)))
        .await
        .unwrap();
    let store = file.open_as("a").await;
    let update = resume(&store, &mine, &[ROWS]).await;
    assert!(
        matches!(update, TenantCaptureUpdate::Unchanged { .. }),
        "owner a_p's group is not owner a's material: {}",
        kind(&update)
    );
    store
        .append_group(&changed("a-next", "a", &json!(3)))
        .await
        .unwrap();
    drop(store);
    let other = file.open_as("a_p").await;
    let update = resume(&other, &theirs, &[ROWS]).await;
    let TenantCaptureUpdate::AppendDelta { delta, .. } = update else {
        panic!("owner a's group in owner a_p's chain: {}", kind(&update));
    };
    assert_eq!(delta.events.len(), 1, "only owner a_p's own group");
    drop(other);
    file.open_as("a")
        .await
        .disable_durable_continuity()
        .await
        .unwrap();
    for (table, expected) in [
        ("a_events", 0),
        ("a_p_adv_rows", 0),
        ("a_p_events", 3),
        ("a_p_p_adv_rows", 3),
    ] {
        assert_eq!(
            file.triggers_on(table),
            expected,
            "after a disabled: {table}"
        );
    }
    // Owner a's DDL moved the file's schema version, so owner a_p's older checkpoint may be
    // complete; a fresh one continues through owner a's trigger-less writes.
    let (_, fresh) = durable(&file.open_as("a_p").await, &[ROWS]).await;
    file.open_as("a")
        .await
        .append_group(&changed("a-after", "a", &json!(4)))
        .await
        .unwrap();
    let update = resume(&file.open_as("a_p").await, &fresh, &[ROWS]).await;
    assert!(
        matches!(update, TenantCaptureUpdate::Unchanged { .. }),
        "owner a's write after its disable is not owner a_p's material: {}",
        kind(&update)
    );
}

#[tokio::test]
async fn a_guard_that_writes_a_requested_row_and_reserves_continues_as_the_exact_delta() {
    let file = provisioned().await;
    let (base, saved) = durable(&file.open().await, &[ROWS]).await;
    let in_process = {
        let store = file.open().await;
        let (_, local) = complete(&store, &[ROWS]).await;
        let guard = Writer {
            permit: store.admission_permit(),
            spec: ROWS,
        };
        store
            .append_group_guarded(&changed("guarded", "a", &json!(5)), Arc::new(guard))
            .await
            .unwrap();
        match store
            .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&local))
            .await
            .unwrap()
        {
            TenantCaptureUpdate::AppendDelta { delta, .. } => delta,
            other => panic!("in-process control: {}", kind(&other)),
        }
    };
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    let TenantCaptureUpdate::AppendDelta { delta, .. } = update else {
        panic!("a guarded group gave {}", kind(&update));
    };
    assert_eq!(delta, in_process);
    let full = store
        .capture_tenant(&tenant(), &[ROWS], limits())
        .await
        .unwrap();
    assert!(
        full.projections[0]
            .rows
            .iter()
            .any(|(key, _)| key == "guarded")
    );
    assert_eq!(apply(base, &delta), Some(full.clone()));
    assert_eq!(delta.resulting_usage, usage(&full));
}

#[tokio::test]
async fn a_guard_writing_the_requested_table_under_another_declaration_never_continues() {
    let file = provisioned().await;
    let (_, saved) = durable(&file.open().await, &[ROWS]).await;
    {
        let store = file.open().await;
        let guard = Writer {
            permit: store.admission_permit(),
            spec: ROWS_UNINDEXED,
        };
        store
            .append_group_guarded(&changed("redeclared", "a", &json!(6)), Arc::new(guard))
            .await
            .unwrap();
    }
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    let TenantCaptureUpdate::Complete { capture, .. } = update else {
        panic!("a write under another declaration gave {}", kind(&update));
    };
    assert!(
        capture.projections[0]
            .rows
            .iter()
            .any(|(key, _)| key == "guarded")
    );
}

#[tokio::test]
async fn a_checkpoint_taken_before_a_redaction_never_continues() {
    let file = provisioned().await;
    let (_, saved) = durable(&file.open().await, &[ROWS]).await;
    file.open()
        .await
        .redact(
            &StreamId::new(tenant(), "item", "seed").unwrap(),
            1,
            "erasure request",
        )
        .await
        .unwrap();
    let store = file.open().await;
    let restored = store.restore_checkpoint(&saved).expect("its own bytes");
    let update = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&restored))
        .await;
    assert!(
        matches!(update, Err(CaptureError::RedactedHistory)),
        "{:?}",
        update.as_ref().map(kind)
    );
}

#[tokio::test]
async fn a_tenant_erased_and_provisioned_again_never_continues() {
    let file = provisioned().await;
    let (base, saved) = durable(&file.open().await, &[ROWS]).await;
    {
        let store = file.open().await;
        store.forget_tenant(&tenant()).await.unwrap();
        store.stream_identity(&tenant()).await.unwrap();
        store
            .append_group(&changed("again", "a", &json!(1)))
            .await
            .unwrap();
    }
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    let TenantCaptureUpdate::Complete { capture, .. } = update else {
        panic!("an erased and re-provisioned tenant gave {}", kind(&update));
    };
    assert_ne!(capture.stream_identity, base.stream_identity);
}

#[tokio::test]
async fn a_blob_deleted_and_bound_again_by_a_group_never_continues() {
    let file = provisioned().await;
    let (_, saved) = durable(&file.open().await, &[ROWS]).await;
    {
        let store = file.open().await;
        store.delete_blob(&tenant(), "seed").await.unwrap();
        store
            .append_group_guarded_with_blobs(
                &changed("rebind", "a", &json!(3)),
                Arc::new(NoGuard),
                &[("seed".into(), b"other bytes".to_vec())],
            )
            .await
            .unwrap();
    }
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    let TenantCaptureUpdate::Complete { capture, .. } = update else {
        panic!("a blob deleted and bound again gave {}", kind(&update));
    };
    let seed = capture.blobs.iter().find(|blob| blob.digest == "seed");
    assert_eq!(
        seed.map(|blob| blob.bytes.as_slice()),
        Some(&b"other bytes"[..])
    );
}

#[tokio::test]
async fn integers_at_the_edges_and_present_nulls_continue_as_the_exact_delta() {
    let file = provisioned().await;
    let (base, saved) = durable(&file.open().await, &[ROWS]).await;
    let bodies = [
        json!({ "key": "max", "value": u64::MAX }),
        json!({ "key": "min", "value": i64::MIN }),
        json!({ "key": "null", "value": null }),
        json!({ "key": "deep", "value": { "kind": null, "list": [null, { "x": null }] } }),
        json!({ "key": "half", "value": 0.5 }),
        json!({ "key": "negative-zero", "value": -0.0 }),
        json!({ "key": "a", "value": "\u{0}\u{1F600}" }),
    ];
    let in_process = {
        let store = file.open().await;
        let (_, local) = complete(&store, &[ROWS]).await;
        in_process_after(&store, &local, &group(&tenant(), "edges", &bodies))
            .await
            .0
    };
    let store = file.open().await;
    let update = resume(&store, &saved, &[ROWS]).await;
    let TenantCaptureUpdate::AppendDelta { delta, .. } = update else {
        panic!("edge values gave {}", kind(&update));
    };
    assert_eq!(delta, in_process);
    let full = store
        .capture_tenant(&tenant(), &[ROWS], limits())
        .await
        .unwrap();
    assert_eq!(apply(base, &delta), Some(full.clone()));
    assert_eq!(delta.resulting_usage, usage(&full));
}

#[tokio::test]
async fn create_projections_with_a_failing_later_spec_creates_nothing() {
    let file = provisioned().await;
    file.raw()
        .execute_batch(
            "INSERT INTO adv_projection_registry (projection_name, indexed_fields)
             VALUES ('late_two', '[\"x\"]')",
        )
        .unwrap();
    let version = file.schema_version();
    for (what, specs, first) in [
        ("an invalid later name", &INVALID_LATER, "adv_p_late_ok"),
        (
            "a later registry mismatch",
            &MISMATCHED_LATER,
            "adv_p_late_one",
        ),
    ] {
        let store = file.open().await;
        assert!(
            store
                .create_projections(Arc::new(Declares(specs)))
                .await
                .is_err(),
            "{what}: control"
        );
        drop(store);
        assert!(!file.has_table(first), "{what}: the first table stayed");
        assert_eq!(file.schema_version(), version, "{what}: DDL stayed");
    }
}

#[tokio::test]
async fn a_handle_holding_a_retired_instance_never_continues() {
    let file = provisioned().await;
    let early = file.open().await;
    let (_, old) = durable(&early, &[ROWS]).await;
    {
        let store = file.open().await;
        store.disable_durable_continuity().await.unwrap();
        store.enable_durable_continuity().await.unwrap();
    }
    let Some(restored) = early.restore_checkpoint(&old) else {
        return;
    };
    let update = early
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&restored))
        .await
        .unwrap();
    assert!(
        matches!(update, TenantCaptureUpdate::Complete { .. }),
        "a retired instance continued: {}",
        kind(&update)
    );
}

#[tokio::test]
async fn an_in_process_checkpoint_across_another_tenants_group_continues_exactly() {
    let file = provisioned().await;
    let store = file.open().await;
    let (base, local) = complete(&store, &[ROWS]).await;
    store
        .append_group(&group(
            &neighbour(),
            "n-1",
            &[json!({ "key": "n", "value": 1 })],
        ))
        .await
        .unwrap();
    let (delta, _) = in_process_after(&store, &local, &changed("t-1", "a", &json!(7))).await;
    assert_eq!(delta.events.len(), 1);
    let full = store
        .capture_tenant(&tenant(), &[ROWS], limits())
        .await
        .unwrap();
    assert_eq!(apply(base, &delta), Some(full.clone()));
    assert_eq!(delta.resulting_usage, usage(&full));
}

#[tokio::test]
async fn the_journal_entry_spans_exactly_the_marks_around_its_group() {
    let file = provisioned().await;
    let before = file.mark();
    let mut two = changed("two-members", "a", &json!(9));
    two.appends.push(StreamAppend {
        stream: StreamId::new(tenant(), "item", "second").unwrap(),
        expected: Expected::Any,
        events: vec![NewEvent::new("item.changed", 1, json!({ "key": "c", "value": 1 })).unwrap()],
    });
    file.open()
        .await
        .append_group_guarded_with_blobs(
            &two,
            Arc::new(NoGuard),
            &[("fresh".into(), b"fresh bytes".to_vec())],
        )
        .await
        .unwrap();
    let after = file.mark();
    assert_ne!(before, after, "control: the group moved the mark");
    assert_eq!(file.last_entry(), (before, after));
}
