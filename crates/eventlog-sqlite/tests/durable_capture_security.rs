//! Security conformance for durable capture continuity, checked against
//! `docs/design/durable-capture-continuity.md` and the kit invariants in `AGENTS.md`.
//!
//! Privacy: redaction and tenant erasure remove every journal copy of projection row values, in
//! their own transaction, and nothing copies redacted material back. Additive DDL: an owner that
//! never enabled durable continuity never gains its objects. Trigger admission: every check
//! admits exactly the provider's own triggers. Caller-supplied checkpoint bytes restore to nothing
//! or continue as a complete capture. Every step opens its own `SqliteEventStore`.

use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, CaptureError, CaptureLimits, ConsistentTenantCapture,
    DurableCaptureCheckpoint, EventLogError, EventStore, Expected, InlineProjectionAdmin, NewEvent,
    NoGuard, ProjectionSpec, ProjectionStore, Projector, RecordedEvent, Snapshot, StreamAppend,
    StreamId, TenantCaptureUpdate, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const PREFIX: &str = "secure";
const EVENTS: &str = "secure_events";
const BLOBS: &str = "secure_blobs";
const IDENTITY: &str = "secure_identity";
const ROW_TABLE: &str = "secure_p_secure_rows";
const CONTINUITY: &str = "secure_capture_continuity";
const JOURNAL: &str = "secure_capture_journal";

/// A value only the event that is about to be redacted ever carried.
const REDACTED: &str = "redacted-material-7f3a";
/// A field of an event body no projection copies.
const PAYLOAD: &str = "event-payload-marker-91c2";
/// Bytes of a bound blob.
const BLOB_BYTES: &[u8] = b"blob-bytes-marker-5d08";

const ROWS: ProjectionSpec = ProjectionSpec {
    name: "secure_rows",
    indexed: &["kind"],
};
const LATE: ProjectionSpec = ProjectionSpec {
    name: "secure_late",
    indexed: &[],
};

fn tenant() -> TenantId {
    TenantId::new("secure-tenant").unwrap()
}

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 4096,
        max_blobs: 4096,
        max_projection_rows: 4096,
        max_payload_bytes: 1 << 28,
    }
}

/// `{"key": k, "value": v}` sets row `k` to `v`; every other member of the body is not projected.
struct Rows;

impl Projector for Rows {
    fn name(&self) -> &'static str {
        "secure_rows"
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
            store
                .upsert(&ROWS, &event.tenant, key, &event.data["value"])
                .await
        })
    }
}

/// A projection that exists only from the moment a test creates it.
struct Late;

impl Projector for Late {
    fn name(&self) -> &'static str {
        "secure_late"
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

/// A projector whose declared projection name is not an identifier.
struct Crafted;

impl Projector for Crafted {
    fn name(&self) -> &'static str {
        "crafted"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        &[ProjectionSpec {
            name: "rows ON secure_events BEGIN DELETE FROM secure_events; END --",
            indexed: &[],
        }]
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

    async fn open(&self) -> SqliteEventStore {
        let store = SqliteEventStore::open(self.name(), PREFIX).await.unwrap();
        store.register_inline(Arc::new(Rows)).await.unwrap();
        store
    }

    fn raw(&self) -> Connection {
        Connection::open(&self.path).unwrap()
    }

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

    /// Every text a query's first column returns.
    fn texts(&self, sql: &str) -> Vec<String> {
        let raw = self.raw();
        let mut statement = raw.prepare(sql).unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn count(&self, sql: &str) -> i64 {
        self.raw().query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn journal(&self) -> Vec<String> {
        self.texts(&format!("SELECT entry FROM {JOURNAL} ORDER BY position"))
    }

    fn mark(&self) -> (String, i64, String) {
        self.raw()
            .query_row(
                &format!("SELECT instance, epoch, token FROM {CONTINUITY}"),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }
}

fn group(key: &str, bodies: &[Value]) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: eventlog_conformance::meta(key, &json!({ "key": key })),
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant(), "item", key).unwrap(),
            expected: Expected::Any,
            events: bodies
                .iter()
                .map(|body| NewEvent::new("item.changed", 1, body.clone()).unwrap())
                .collect(),
        }],
    }
}

fn changed(key: &str, row: &str, value: &Value) -> AppendGroup {
    group(key, &[json!({ "key": row, "value": value })])
}

/// A provisioned, seeded file with durable continuity enabled after the seed.
async fn enabled() -> File {
    let file = File::new();
    let store = file.open().await;
    store.stream_identity(&tenant()).await.unwrap();
    store
        .append_group_guarded_with_blobs(
            &changed("seed", "a", &json!(1)),
            Arc::new(NoGuard),
            &[("seed".into(), b"seed bytes".to_vec())],
        )
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

async fn durable(store: &SqliteEventStore) -> DurableCaptureCheckpoint {
    let update = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), None)
        .await
        .unwrap();
    let TenantCaptureUpdate::Complete {
        checkpoint: Some(checkpoint),
        ..
    } = update
    else {
        panic!("a complete capture issues a checkpoint: {}", kind(&update));
    };
    store
        .durable_checkpoint(&checkpoint)
        .expect("an enabled store offers the durable form of its checkpoint")
}

/// Restore `bytes` on a fresh handle and capture from them: `None` when they do not restore.
async fn continue_from(
    file: &File,
    bytes: Vec<u8>,
) -> Option<Result<TenantCaptureUpdate, CaptureError>> {
    let store = file.open().await;
    let restored = store.restore_checkpoint(&DurableCaptureCheckpoint::from_bytes(bytes))?;
    Some(
        store
            .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&restored))
            .await,
    )
}

// ---------------------------------------------------------------------------------------------
// Invariant 8 and the safety envelope: no copy of redacted or erased material survives.
// ---------------------------------------------------------------------------------------------

/// The journal is the one durable object that copies projection row values. Redaction leaves a
/// projection row derived from the redacted event in place (`consistent-tenant-capture.md`); an
/// ordinary later group that replaces that row must not carry the redacted value into a journal
/// entry, where it would outlive the row and the redaction both.
#[tokio::test]
async fn a_group_after_redaction_copies_no_redacted_material_into_the_journal() {
    let file = enabled().await;
    {
        let store = file.open().await;
        store
            .append_group(&changed("secret", "a", &json!(REDACTED)))
            .await
            .unwrap();
    }
    assert!(
        file.journal().iter().any(|entry| entry.contains(REDACTED)),
        "control: the group's entry holds the projected value"
    );
    {
        let store = file.open().await;
        store
            .redact(
                &StreamId::new(tenant(), "item", "secret").unwrap(),
                1,
                "erasure request",
            )
            .await
            .unwrap();
    }
    assert!(
        file.journal().is_empty(),
        "control: redaction cleared the journal"
    );
    {
        let store = file.open().await;
        store
            .append_group(&changed("later", "a", &json!("fresh")))
            .await
            .unwrap();
    }
    assert_eq!(
        file.count(&format!(
            "SELECT COUNT(*) FROM {EVENTS} WHERE instr(data, '{REDACTED}') > 0"
        )),
        0,
        "control: the event no longer holds it"
    );
    assert_eq!(
        file.texts(&format!(
            "SELECT body FROM {ROW_TABLE} WHERE tenant_id = 'secure-tenant' AND row_key = 'a'"
        )),
        vec!["\"fresh\"".to_owned()],
        "control: the projection row no longer holds it"
    );
    let leaked = file
        .journal()
        .iter()
        .filter(|entry| entry.contains(REDACTED))
        .count();
    assert_eq!(
        leaked, 0,
        "after redaction the redacted value lives on in {leaked} journal entry, the only copy left \
         in the store"
    );
}

/// Redaction and erasure clear the journal inside their own transaction: when either fails, the
/// whole operation rolls back and leaves the events, the journal and the mark as they were.
#[tokio::test]
async fn a_failed_redaction_or_erasure_leaves_journal_and_data_consistent() {
    let file = enabled().await;
    {
        let store = file.open().await;
        store
            .append_group(&changed("one", "a", &json!("first")))
            .await
            .unwrap();
        store
            .append_group(&changed("two", "a", &json!("second")))
            .await
            .unwrap();
    }
    let events = || {
        file.texts(&format!(
            "SELECT data || '|' || COALESCE(redacted_at, '') FROM {EVENTS} ORDER BY global_seq"
        ))
    };
    let before = (events(), file.journal(), file.mark());
    assert_eq!(before.1.len(), 2, "control: two entries");
    file.raw()
        .execute_batch(&format!(
            "CREATE TRIGGER block_journal_delete BEFORE DELETE ON {JOURNAL}
             BEGIN SELECT RAISE(ABORT, 'journal delete refused'); END"
        ))
        .unwrap();
    {
        let store = file.open().await;
        let redacted = store
            .redact(
                &StreamId::new(tenant(), "item", "one").unwrap(),
                1,
                "erasure request",
            )
            .await;
        assert!(
            redacted.is_err(),
            "control: the redaction failed at the journal"
        );
        let erased = store.forget_tenant(&tenant()).await;
        assert!(
            erased.is_err(),
            "control: the erasure failed at the journal"
        );
    }
    assert_eq!(
        (events(), file.journal(), file.mark()),
        before,
        "a failed redaction or erasure changed events, journal or mark"
    );
    file.raw()
        .execute_batch("DROP TRIGGER block_journal_delete")
        .unwrap();
    let store = file.open().await;
    store
        .redact(
            &StreamId::new(tenant(), "item", "one").unwrap(),
            1,
            "erasure request",
        )
        .await
        .unwrap();
    assert!(
        file.journal().is_empty(),
        "control: the retried redaction cleared it"
    );
}

/// Journal entries reference events by position and blobs by digest. A durable checkpoint holds
/// opaque coordinates only: tenant id, stream identity, projection names, counts, marks.
#[tokio::test]
async fn journal_and_checkpoint_bytes_hold_no_event_payload_and_no_blob_bytes() {
    let file = enabled().await;
    {
        let store = file.open().await;
        store
            .append_group_guarded_with_blobs(
                &group(
                    "payload",
                    &[json!({ "key": "a", "value": "row-value", "note": PAYLOAD })],
                ),
                Arc::new(NoGuard),
                &[("marker".into(), BLOB_BYTES.to_vec())],
            )
            .await
            .unwrap();
    }
    let blob_text = String::from_utf8(BLOB_BYTES.to_vec()).unwrap();
    let entries = file.journal();
    assert_eq!(entries.len(), 1, "control: the group wrote one entry");
    assert!(entries[0].contains("row-value"), "control: rows are copied");
    assert!(
        !entries[0].contains(PAYLOAD),
        "an event payload entered the journal"
    );
    assert!(
        !entries[0].contains(&blob_text),
        "blob bytes entered the journal"
    );

    let store = file.open().await;
    let bytes = durable(&store).await.as_bytes().to_vec();
    let text = String::from_utf8(bytes.clone()).unwrap();
    for (what, marker) in [
        ("an event payload", PAYLOAD),
        ("blob bytes", blob_text.as_str()),
        ("a row value", "row-value"),
    ] {
        assert!(
            !text.contains(marker),
            "{what} entered the durable checkpoint"
        );
    }
    let decoded: Value = serde_json::from_slice(&bytes).unwrap();
    let identity = store
        .stored_stream_identity(&tenant())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        decoded["scope"]["capture"],
        json!({ "tenant_id": "secure-tenant", "stream_identity": identity }),
        "the checkpoint's only tenant coordinates are the opaque ids"
    );
    assert_eq!(
        decoded["scope"]["projections"],
        json!([{ "name": "secure_rows", "indexed_fields": ["kind"] }])
    );
    let mut keys: Vec<&str> = decoded
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "format",
            "mark",
            "position",
            "prefix",
            "schema_version",
            "scope",
            "store_instance",
            "usage",
            "version"
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// Invariant 6: additive DDL; enabling is explicit, disabling restores the never-enabled schema.
// ---------------------------------------------------------------------------------------------

fn continuity_objects(file: &File) -> Vec<String> {
    file.schema()
        .into_iter()
        .filter(|(kind, name, _, _)| kind == "trigger" || name.contains("capture_"))
        .map(|(kind, name, _, _)| format!("{kind} {name}"))
        .collect()
}

/// Provisioning, `open_existing`, attach, every write path and every read path of an owner that
/// never enabled durable continuity create none of its tables or triggers, beside an owner in
/// the same file that did.
#[tokio::test]
async fn no_ordinary_path_creates_a_continuity_object_for_an_owner_that_never_enabled() {
    let file = File::new();
    {
        let store = file.open().await;
        store.stream_identity(&tenant()).await.unwrap();
        store
            .append_group_guarded_with_blobs(
                &changed("one", "a", &json!(1)),
                Arc::new(NoGuard),
                &[("one".into(), b"one".to_vec())],
            )
            .await
            .unwrap();
        let update = store
            .capture_tenant_since(&tenant(), &[ROWS], limits(), None)
            .await
            .unwrap();
        let TenantCaptureUpdate::Complete {
            checkpoint: Some(checkpoint),
            ..
        } = update
        else {
            panic!("control: a checkpoint");
        };
        assert_eq!(store.durable_checkpoint(&checkpoint), None);
        let stream = StreamId::new(tenant(), "item", "one").unwrap();
        let generation = store.snapshot_generation(&stream).await.unwrap().unwrap();
        store
            .save_snapshot_checked(
                &stream,
                &Snapshot {
                    version: 1,
                    state_schema_version: 1,
                    state: json!({}),
                    recorded_at: time::OffsetDateTime::now_utc(),
                },
                &generation,
            )
            .await
            .unwrap();
        store.redact(&stream, 1, "erasure request").await.unwrap();
    }
    file.open()
        .await
        .register_inline(Arc::new(Late))
        .await
        .unwrap();
    assert!(
        continuity_objects(&file).is_empty(),
        "{:?}",
        continuity_objects(&file)
    );
    {
        let existing = SqliteEventStore::open_existing(file.name(), PREFIX)
            .await
            .unwrap();
        existing
            .attach_inline_existing(Arc::new(Rows))
            .await
            .unwrap();
        existing.forget_tenant(&tenant()).await.unwrap();
    }
    assert!(
        continuity_objects(&file).is_empty(),
        "{:?}",
        continuity_objects(&file)
    );

    // Another owner in the same file enables; this owner still gains nothing on any path.
    {
        let neighbour = SqliteEventStore::open(file.name(), "neighbour")
            .await
            .unwrap();
        neighbour.register_inline(Arc::new(Rows)).await.unwrap();
        neighbour.enable_durable_continuity().await.unwrap();
    }
    {
        let store = file.open().await;
        store
            .append_group(&changed("two", "a", &json!(2)))
            .await
            .unwrap();
        let existing = SqliteEventStore::open_existing(file.name(), PREFIX)
            .await
            .unwrap();
        existing
            .attach_inline_existing(Arc::new(Rows))
            .await
            .unwrap();
    }
    let mine: Vec<String> = continuity_objects(&file)
        .into_iter()
        .filter(|object| !object.contains("neighbour"))
        .collect();
    assert!(mine.is_empty(), "{mine:?}");
}

/// A projection created after enable carries the provider triggers; disable removes them too, and
/// the store is then exactly a never-enabled store holding the same projections.
#[tokio::test]
async fn disabling_after_a_late_projection_restores_the_never_enabled_schema() {
    let never = File::new();
    let switched = File::new();
    for file in [&never, &switched] {
        let store = file.open().await;
        store.stream_identity(&tenant()).await.unwrap();
        store
            .append_group(&changed("one", "a", &json!(1)))
            .await
            .unwrap();
    }
    switched
        .open()
        .await
        .enable_durable_continuity()
        .await
        .unwrap();
    for file in [&never, &switched] {
        file.open()
            .await
            .register_inline(Arc::new(Late))
            .await
            .unwrap();
    }
    assert_eq!(
        switched
            .texts("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'secure_p_secure_late'")
            .len(),
        3,
        "control: the late projection was covered"
    );
    switched
        .open()
        .await
        .disable_durable_continuity()
        .await
        .unwrap();
    assert_eq!(switched.schema(), never.schema());
}

// ---------------------------------------------------------------------------------------------
// Trigger admission: exactly the provider's own triggers, by name and text.
// ---------------------------------------------------------------------------------------------

type TextEdit = fn(&str) -> String;

/// Near misses of a provider trigger, each kept under the provider trigger's own name.
fn near_misses() -> Vec<(&'static str, TextEdit)> {
    vec![
        ("BEFORE timing", |sql| {
            sql.replacen("AFTER INSERT", "BEFORE INSERT", 1)
        }),
        ("a WHEN clause", |sql| {
            sql.replacen(" BEGIN ", " WHEN 1 BEGIN ", 1)
        }),
        ("an extra statement", |sql| {
            sql.replacen("; END", "; DELETE FROM secure_capture_journal; END", 1)
        }),
        ("another owner's continuity row", |sql| {
            sql.replace("secure_capture_continuity", "neighbour_capture_continuity")
        }),
        ("a lower-case keyword", |sql| {
            sql.replacen("AFTER INSERT", "after INSERT", 1)
        }),
    ]
}

/// Replace the provider's `AFTER INSERT` trigger on `table` with `edit` of its text, same name.
fn near_miss(file: &File, table: &str, edit: TextEdit) {
    let raw = file.raw();
    let (name, sql): (String, String) = raw
        .query_row(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'trigger' AND tbl_name = ?1 AND instr(sql, 'AFTER INSERT') > 0",
            [table],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let edited = edit(&sql);
    raw.execute_batch(&format!("DROP TRIGGER {name}; {edited}"))
        .unwrap();
    let stored: String = raw
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
            [&name],
            |row| row.get(0),
        )
        .unwrap();
    assert_ne!(
        stored, sql,
        "control: the stored text differs from the provider's"
    );
}

async fn issues_a_checkpoint(file: &File) -> bool {
    let store = file.open().await;
    matches!(
        store
            .capture_tenant_since(&tenant(), &[ROWS], limits(), None)
            .await
            .unwrap(),
        TenantCaptureUpdate::Complete {
            checkpoint: Some(_),
            ..
        }
    )
}

#[cfg(target_os = "linux")]
async fn inspection_admits(file: &File) -> bool {
    use eventlog_core::InspectHistory;
    use eventlog_sqlite::SqliteHistoryInspector;
    file.raw()
        .execute_batch("PRAGMA journal_mode=DELETE")
        .unwrap();
    SqliteHistoryInspector::new(&file.path, PREFIX)
        .inspect_history(&tenant(), eventlog_conformance::INSPECTION_LIMITS)
        .await
        .is_ok()
}

#[cfg(not(target_os = "linux"))]
async fn inspection_admits(_: &File) -> bool {
    std::future::ready(false).await
}

/// Every admission check refuses a trigger whose name is the provider's and whose text is not.
#[tokio::test]
async fn near_miss_provider_triggers_are_refused_by_every_admission_check() {
    for (what, edit) in near_misses() {
        let file = enabled().await;
        near_miss(&file, BLOBS, edit);
        assert!(
            SqliteEventStore::open_existing(file.name(), PREFIX)
                .await
                .is_err(),
            "{what} on the blob table: admitted at open"
        );

        let file = enabled().await;
        near_miss(&file, ROW_TABLE, edit);
        let existing = SqliteEventStore::open_existing(file.name(), PREFIX)
            .await
            .unwrap();
        assert!(
            existing
                .attach_inline_existing(Arc::new(Rows))
                .await
                .is_err(),
            "{what} on a projection table: admitted at attach"
        );
        drop(existing);

        let file = enabled().await;
        near_miss(&file, IDENTITY, edit);
        assert!(
            !issues_a_checkpoint(&file).await,
            "{what} on the identity table: continuity eligibility admitted it"
        );
        assert!(
            matches!(
                file.open().await.enable_durable_continuity().await,
                Err(EventLogError::Invalid(_))
            ),
            "{what}: a second enable admitted it"
        );

        if cfg!(target_os = "linux") {
            let file = enabled().await;
            near_miss(&file, EVENTS, edit);
            assert!(
                !inspection_admits(&file).await,
                "{what} on the events table: admitted by strict inspection"
            );
        }
    }
}

/// SQLite resolves table names without regard to case, and records a trigger's `tbl_name` as the
/// statement spelled it. A foreign trigger that names a captured table in upper case fires on
/// every write to it, and is still "every other trigger" the checks must refuse.
#[tokio::test]
async fn a_foreign_trigger_spelling_a_captured_table_in_upper_case_is_refused() {
    let hidden = |table: &str| {
        format!(
            "CREATE TRIGGER hidden_writer AFTER INSERT ON {}
             BEGIN UPDATE {table} SET rowid = rowid WHERE rowid = NEW.rowid; END",
            table.to_ascii_uppercase()
        )
    };
    let mut admitted = Vec::new();

    let file = enabled().await;
    file.raw().execute_batch(&hidden(BLOBS)).unwrap();
    assert_eq!(
        file.texts("SELECT tbl_name FROM sqlite_master WHERE name = 'hidden_writer'"),
        vec!["SECURE_BLOBS".to_owned()],
        "control: SQLite keeps the spelling"
    );
    // `issues_a_checkpoint` and `File::open` unwrap `SqliteEventStore::open`. A refused open
    // admits nothing: eligibility and enable need an opened handle, so they are checked on one
    // only when open admitted the trigger, and otherwise count as not admitted.
    if SqliteEventStore::open(file.name(), PREFIX).await.is_ok() {
        admitted.push("the blob table check at open");
        if issues_a_checkpoint(&file).await {
            admitted.push("continuity eligibility");
        }
        if file.open().await.enable_durable_continuity().await.is_ok() {
            admitted.push("enable");
        }
    }
    if SqliteEventStore::open_existing(file.name(), PREFIX)
        .await
        .is_ok()
    {
        admitted.push("the blob table check at open_existing");
    }

    let file = enabled().await;
    file.raw().execute_batch(&hidden(ROW_TABLE)).unwrap();
    let existing = SqliteEventStore::open_existing(file.name(), PREFIX)
        .await
        .unwrap();
    if existing
        .attach_inline_existing(Arc::new(Rows))
        .await
        .is_ok()
    {
        admitted.push("the projection table check at attach");
    }
    if existing
        .capture_tenant(&tenant(), &[ROWS], limits())
        .await
        .is_ok()
    {
        admitted.push("the projection table check at capture");
    }
    drop(existing);

    if cfg!(target_os = "linux") {
        let file = enabled().await;
        file.raw().execute_batch(&hidden(EVENTS)).unwrap();
        if inspection_admits(&file).await {
            admitted.push("strict inspection of the events table");
        }
    }
    assert!(
        admitted.is_empty(),
        "a foreign trigger on a captured table, spelled in upper case, was admitted by: {}",
        admitted.join(", ")
    );
}

/// A trigger the provider did not install ends continuity wherever it writes from: a temporary
/// trigger on another connection, invisible to the provider's connection, and a main-schema
/// trigger on another owner's table that writes this owner's rows.
#[tokio::test]
async fn writes_from_foreign_and_temporary_triggers_end_continuity() {
    let file = enabled().await;
    let saved = durable(&file.open().await).await.as_bytes().to_vec();
    {
        let raw = file.raw();
        raw.execute_batch(
            "CREATE TEMP TRIGGER copy_in AFTER INSERT ON main.secure_projection_registry
             BEGIN INSERT INTO secure_p_secure_rows (tenant_id, row_key, body, idx_0)
                   VALUES ('secure-tenant', 'smuggled', '1', NULL); END;
             INSERT INTO secure_projection_registry (projection_name, indexed_fields)
             VALUES ('unrelated', '[]');",
        )
        .unwrap();
    }
    assert_eq!(
        file.count(&format!(
            "SELECT COUNT(*) FROM {ROW_TABLE} WHERE row_key = 'smuggled'"
        )),
        1,
        "control: the temporary trigger wrote a captured row"
    );
    let update = continue_from(&file, saved).await.unwrap().unwrap();
    assert!(
        matches!(update, TenantCaptureUpdate::Complete { .. }),
        "a row a temporary trigger wrote: {}",
        kind(&update)
    );

    let file = enabled().await;
    {
        let neighbour = SqliteEventStore::open(file.name(), "neighbour")
            .await
            .unwrap();
        neighbour.register_inline(Arc::new(Rows)).await.unwrap();
        neighbour.stream_identity(&tenant()).await.unwrap();
    }
    let saved = durable(&file.open().await).await.as_bytes().to_vec();
    file.raw()
        .execute_batch(
            "CREATE TRIGGER neighbour_events_capture_insert AFTER INSERT ON neighbour_events
             BEGIN INSERT INTO secure_p_secure_rows (tenant_id, row_key, body, idx_0)
                   VALUES ('secure-tenant', 'across', '1', NULL); END",
        )
        .unwrap();
    {
        let neighbour = SqliteEventStore::open(file.name(), "neighbour")
            .await
            .unwrap();
        neighbour.register_inline(Arc::new(Rows)).await.unwrap();
        neighbour
            .append_group(&changed("across", "z", &json!(1)))
            .await
            .unwrap();
    }
    assert!(
        !issues_a_checkpoint(&file).await,
        "the cross-owner writer was admitted"
    );
    let update = continue_from(&file, saved).await.unwrap().unwrap();
    assert!(
        matches!(update, TenantCaptureUpdate::Complete { .. }),
        "a row another owner's trigger wrote: {}",
        kind(&update)
    );
}

// ---------------------------------------------------------------------------------------------
// Names in trigger text: a prefix or projection name cannot reshape it.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn names_that_could_reshape_trigger_text_are_refused_before_any_ddl() {
    let file = File::new();
    for prefix in ["secure; DROP TABLE x", "Secure", "secure x", "secure\"", ""] {
        assert!(
            SqliteEventStore::open(file.name(), prefix).await.is_err(),
            "prefix {prefix:?} admitted"
        );
    }
    let store = file.open().await;
    store.stream_identity(&tenant()).await.unwrap();
    assert!(
        store.register_inline(Arc::new(Crafted)).await.is_err(),
        "a projection name that is not an identifier was admitted"
    );
    // A registry row a foreign writer added, naming a table with SQL in it.
    file.raw()
        .execute_batch(
            "INSERT INTO secure_projection_registry (projection_name, indexed_fields)
             VALUES ('x BEGIN DELETE FROM secure_events; END', '[]')",
        )
        .unwrap();
    let before = file.schema();
    assert!(
        matches!(
            store.enable_durable_continuity().await,
            Err(EventLogError::Invalid(_))
        ),
        "enable admitted a registered name that is not an identifier"
    );
    assert_eq!(
        file.schema(),
        before,
        "a refused enable left nothing behind"
    );
    drop(store);

    let file = enabled().await;
    let saved: Value =
        serde_json::from_slice(durable(&file.open().await).await.as_bytes()).unwrap();
    for name in [
        "SECURE_ROWS",
        "secure_rows ON x",
        "secure_rows\u{0}",
        "1rows",
    ] {
        let mut edited = saved.clone();
        edited["scope"]["projections"][0]["name"] = json!(name);
        assert!(
            file.open()
                .await
                .restore_checkpoint(&DurableCaptureCheckpoint::from_bytes(
                    serde_json::to_vec(&edited).unwrap()
                ))
                .is_none(),
            "projection name {name:?} restored"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Caller-supplied checkpoint bytes.
// ---------------------------------------------------------------------------------------------

/// One edit to decoded checkpoint JSON.
type ValueEdit = fn(&mut Value);

fn nested(depth: usize) -> Vec<u8> {
    let mut bytes = vec![b'['; depth];
    bytes.extend(std::iter::repeat_n(b']', depth));
    bytes
}

/// Hostile encodings restore to nothing, or restore and continue as a complete capture of an
/// untouched store whose genuine checkpoint is `Unchanged`. None panics.
#[tokio::test]
async fn hostile_checkpoint_bytes_restore_to_nothing_or_continue_as_complete() {
    let file = enabled().await;
    let genuine = durable(&file.open().await).await.as_bytes().to_vec();
    let text = String::from_utf8(genuine.clone()).unwrap();
    let decoded: Value = serde_json::from_slice(&genuine).unwrap();
    let control = continue_from(&file, genuine.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(control, TenantCaptureUpdate::Unchanged { .. }),
        "control: {}",
        kind(&control)
    );

    let mut cases: Vec<(String, Vec<u8>)> = vec![
        ("deep array nesting".into(), nested(200_000)),
        ("deep object nesting".into(), {
            let mut bytes = b"{\"scope\":".repeat(100_000);
            bytes.extend(b"1");
            bytes.extend(std::iter::repeat_n(b'}', 100_000));
            bytes
        }),
        (
            "deep nesting inside a projection's indexed fields".into(),
            text.replacen(
                "\"indexed_fields\":[\"kind\"]",
                &format!(
                    "\"indexed_fields\":{}",
                    String::from_utf8(nested(100_000)).unwrap()
                ),
                1,
            )
            .into_bytes(),
        ),
        (
            "a duplicate top-level key".into(),
            text.replacen('{', "{\"position\":0,", 1).into_bytes(),
        ),
        (
            "a duplicate mark epoch".into(),
            text.replacen("\"mark\":{", "\"mark\":{\"epoch\":0,", 1)
                .into_bytes(),
        ),
        (
            "a byte-order mark".into(),
            [b"\xEF\xBB\xBF".as_slice(), &genuine].concat(),
        ),
        (
            "trailing bytes".into(),
            [genuine.as_slice(), b" {}"].concat(),
        ),
        (
            "a fractional version".into(),
            text.replacen("\"version\":1", "\"version\":1.0", 1)
                .into_bytes(),
        ),
    ];
    // Numbers `serde_json::Value` cannot hold, written into the text in place of epoch zero.
    let mut zero = decoded.clone();
    zero["mark"]["epoch"] = json!(0);
    let zero = serde_json::to_string(&zero).unwrap();
    for (what, number) in [
        ("an epoch past u64", "18446744073709551616"),
        ("an epoch of 1e400", "1e400"),
        ("an epoch with two hundred digits", &"9".repeat(200)),
    ] {
        cases.push((
            what.to_owned(),
            zero.replacen("\"epoch\":0", &format!("\"epoch\":{number}"), 1)
                .into_bytes(),
        ));
    }
    let scalar_edits: Vec<(&str, ValueEdit)> = vec![
        ("the next epoch", |value| {
            let epoch = value["mark"]["epoch"].as_u64().unwrap();
            value["mark"]["epoch"] = json!(epoch + 1);
        }),
        ("another valid token", |value| {
            value["mark"]["token"] = json!("00000000000000000000000000000000");
        }),
        ("the largest epoch", |value| {
            value["mark"]["epoch"] = json!(u64::MAX);
        }),
        ("the next position", |value| {
            let position = value["position"].as_u64().unwrap();
            value["position"] = json!(position + 1);
        }),
        ("the largest position", |value| {
            value["position"] = json!(u64::MAX);
        }),
        ("the smallest schema version", |value| {
            value["schema_version"] = json!(i64::MIN);
        }),
        ("the largest schema version", |value| {
            value["schema_version"] = json!(i64::MAX);
        }),
        ("a hundred thousand projections", |value| {
            let many: Vec<Value> = (0..100_000)
                .map(|n| json!({ "name": format!("p{n}"), "indexed_fields": [] }))
                .collect();
            value["scope"]["projections"] = Value::Array(many);
        }),
        ("a hundred thousand copies of one projection", |value| {
            let one = value["scope"]["projections"][0].clone();
            value["scope"]["projections"] = Value::Array(vec![one; 100_000]);
        }),
        ("a stream identity of a mebibyte", |value| {
            value["scope"]["capture"]["stream_identity"] = json!("i".repeat(1 << 20));
        }),
    ];
    for (what, edit) in scalar_edits {
        let mut value = decoded.clone();
        edit(&mut value);
        cases.push((what.to_owned(), serde_json::to_vec(&value).unwrap()));
    }

    for (what, bytes) in cases {
        match continue_from(&file, bytes).await {
            None | Some(Ok(TenantCaptureUpdate::Complete { .. })) => {}
            Some(Ok(other)) => panic!("{what}: restored and continued as {}", kind(&other)),
            Some(Err(error)) => panic!("{what}: restored and refused: {error:?}"),
        }
    }
}

/// The usage a restored checkpoint carries is caller-supplied. A complete capture of the same
/// observation is within the request's limits, so "every reported limit must actually have been
/// exceeded" (`consistent-tenant-capture.md`) leaves no room for a limit refusal here.
#[tokio::test]
async fn a_restored_checkpoint_with_an_altered_usage_gives_complete_not_a_limit_refusal() {
    let file = enabled().await;
    let genuine = durable(&file.open().await).await.as_bytes().to_vec();
    file.open()
        .await
        .append_group(&changed("next", "a", &json!(2)))
        .await
        .unwrap();
    let control = continue_from(&file, genuine.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(control, TenantCaptureUpdate::AppendDelta { .. }),
        "control: {}",
        kind(&control)
    );
    let mut value: Value = serde_json::from_slice(&genuine).unwrap();
    value["usage"]["events"] = json!(limits().max_events);
    let altered = serde_json::to_vec(&value).unwrap();
    let held = file.count(&format!("SELECT COUNT(*) FROM {EVENTS}"));
    match continue_from(&file, altered).await {
        None | Some(Ok(TenantCaptureUpdate::Complete { .. })) => {}
        Some(Ok(other)) => panic!("an altered usage continued as {}", kind(&other)),
        Some(Err(error)) => panic!(
            "an altered usage turned a capture of {held} events under a cap of {} into: {error:?}",
            limits().max_events
        ),
    }
}

/// The stream identity a restored checkpoint carries is caller-supplied. The delta reports the
/// tenant's stream identity, and `Unchanged` says the observation that named it is current, so
/// neither may rest on an identity the store never held.
#[tokio::test]
async fn a_restored_checkpoint_naming_another_stream_identity_never_reports_it() {
    let file = enabled().await;
    let genuine = durable(&file.open().await).await.as_bytes().to_vec();
    let stored = file
        .open()
        .await
        .stored_stream_identity(&tenant())
        .await
        .unwrap()
        .unwrap();
    let mut value: Value = serde_json::from_slice(&genuine).unwrap();
    value["scope"]["capture"]["stream_identity"] = json!("forged-identity");
    let forged = serde_json::to_vec(&value).unwrap();
    let mut wrong = Vec::new();

    match continue_from(&file, forged.clone()).await {
        None | Some(Ok(TenantCaptureUpdate::Complete { .. })) => {}
        Some(other) => wrong.push(format!(
            "untouched store: {}",
            other.map_or_else(
                |error| format!("{error:?}"),
                |update| kind(&update).to_owned()
            )
        )),
    }

    file.open()
        .await
        .append_group(&changed("next", "a", &json!(2)))
        .await
        .unwrap();
    match continue_from(&file, forged).await {
        Some(Ok(TenantCaptureUpdate::AppendDelta { delta, .. })) => {
            if delta.stream_identity != stored {
                wrong.push(format!(
                    "after a group: an AppendDelta reporting stream identity {:?}; the store \
                     holds {stored:?}",
                    delta.stream_identity
                ));
            }
        }
        None | Some(Ok(TenantCaptureUpdate::Complete { .. })) => {}
        Some(other) => wrong.push(format!(
            "after a group: {}",
            other.map_or_else(
                |error| format!("{error:?}"),
                |update| kind(&update).to_owned()
            )
        )),
    }
    assert!(
        wrong.is_empty(),
        "a checkpoint edited to name another stream identity continued: {}",
        wrong.join("; ")
    );
}

// ---------------------------------------------------------------------------------------------
// Debug and error text.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn debug_and_error_text_carry_no_checkpoint_bytes_tokens_or_row_values() {
    let file = enabled().await;
    {
        let store = file.open().await;
        store
            .append_group(&changed("value", "a", &json!(PAYLOAD)))
            .await
            .unwrap();
    }
    let store = file.open().await;
    let saved = durable(&store).await;
    let (instance, _, token) = file.mark();
    let mut seen = vec![format!("{saved:?}")];
    let restored = store.restore_checkpoint(&saved).unwrap();
    seen.push(format!("{restored:?}"));
    let update = store
        .capture_tenant_since(&tenant(), &[ROWS], limits(), Some(&restored))
        .await
        .unwrap();
    assert!(matches!(update, TenantCaptureUpdate::Unchanged { .. }));
    seen.push(format!("{update:?}"));
    drop(store);

    file.raw()
        .execute_batch("CREATE TRIGGER audit AFTER INSERT ON secure_snapshots BEGIN SELECT 1; END")
        .unwrap();
    let refused = file
        .open()
        .await
        .enable_durable_continuity()
        .await
        .unwrap_err();
    seen.push(format!("{refused:?}"));
    seen.push(refused.to_string());

    let mut value: Value = serde_json::from_slice(saved.as_bytes()).unwrap();
    value["usage"]["events"] = json!(u64::MAX);
    file.raw().execute_batch("DROP TRIGGER audit").unwrap();
    file.open()
        .await
        .append_group(&changed("more", "a", &json!(PAYLOAD)))
        .await
        .unwrap();
    if let Some(Err(error)) = continue_from(&file, serde_json::to_vec(&value).unwrap()).await {
        seen.push(format!("{error:?}"));
        seen.push(error.to_string());
    }

    let saved_text = String::from_utf8(saved.as_bytes().to_vec()).unwrap();
    for text in &seen {
        for (what, secret) in [
            ("the store instance", instance.as_str()),
            ("the mark token", token.as_str()),
            ("a row value", PAYLOAD),
            ("the checkpoint bytes", saved_text.as_str()),
        ] {
            assert!(!text.contains(secret), "{what} in {text}");
        }
    }
}
