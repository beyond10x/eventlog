//! The deterministic counters: work done for a fixed fixture, by provider.
//!
//! Each scenario names the review finding whose fix moves it. Counters read through `/proc`,
//! SQL or the PostgreSQL wire need nothing from the provider; those read from a provider's `kpi`
//! module exist only with the `counters` feature ([`hooks`]), and are skipped without it.
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use eventlog_conformance::{SnapshotIncrement, Tally, event, meta};
use eventlog_core::{
    Aggregate, Applied, AtomicBlobEventStore, BlobAppendGroup, BlobWrite, EventLogError,
    EventStore, Expected, InlineProjectionAdmin, LegacyBlobMigration, Projector, Repository,
    TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    Measured, Outcome, ensure,
    fixture::{self, Place, Provider, ROWS, SEEDED_ROWS, Store},
    hooks, pg,
    proc_io::Snapshot,
};

pub async fn measure(scratch: &Path, url: Option<&str>, measured: &mut Measured) -> Outcome<()> {
    if Snapshot::take().is_none() {
        for provider in ["file", "sqlite", "tree"] {
            for id in [
                "open.bytes_read",
                "append.bytes_written",
                "read_stream.bytes_read",
            ] {
                measured.skip(
                    &format!("{provider}.{id}"),
                    "/proc/self/io is unavailable on this platform",
                );
            }
        }
        measured.skip(
            "file.read_feed.bytes_read",
            "/proc/self/io is unavailable on this platform",
        );
    }
    core_folds(measured).await?;
    file(scratch, measured).await?;
    sqlite(scratch, measured).await?;
    tree(scratch, measured).await?;
    for provider in [Provider::Sqlite, Provider::File, Provider::Tree] {
        projection_page(&Place::new(provider, scratch, None)?, measured).await?;
    }
    if let Some(url) = url {
        postgres(scratch, url, measured).await?;
    } else {
        measured.skip(
            "postgres.",
            "EVENTLOG_TEST_POSTGRES_URL is not set; PostgreSQL KPIs were not measured",
        );
    }
    Ok(())
}

fn put_read(measured: &mut Measured, id: &str, since: Option<Snapshot>) {
    if let Some(io) = since.and_then(|snapshot| snapshot.since()) {
        measured.put_count(id, io.read);
    }
}

fn put_written(measured: &mut Measured, id: &str, since: Option<Snapshot>) {
    if let Some(io) = since.and_then(|snapshot| snapshot.since()) {
        measured.put_count(id, io.written);
    }
}

fn put_hook(measured: &mut Measured, id: &str, value: Option<u64>) {
    match value {
        Some(value) => measured.put_count(id, value),
        None => measured.skip(id, hooks::ABSENT),
    }
}

/// A store of 128 events in one append, reopened (an ordinary open of unchanged data), then one
/// read of the stream. The review's open and feed fixtures are this store.
async fn reopened_128(
    provider: Provider,
    scratch: &Path,
    measured: &mut Measured,
) -> Outcome<Store> {
    let name = provider.name();
    let place = Place::new(provider, scratch, None)?;
    let tenant = fixture::tenant()?;
    let stream = fixture::stream(&tenant, "item", "first")?;
    let store = place.open().await?;
    fixture::seed(store.events(), &stream, 128, "seed").await?;
    store.close().await;

    let before = Snapshot::take();
    let store = place.open().await?;
    put_read(measured, &format!("{name}.open.bytes_read"), before);

    let before = Snapshot::take();
    let slice = store.events().read_stream(&stream, 0, 1000).await?;
    put_read(measured, &format!("{name}.read_stream.bytes_read"), before);
    ensure(
        slice.events.len() == 128,
        "read_stream returned the seeded stream",
    )?;
    Ok(store)
}

/// One single-event append to a fresh stream of a store that holds 128 events.
async fn append_written(provider: Provider, store: &Store, measured: &mut Measured) -> Outcome<()> {
    let other = fixture::stream(&fixture::tenant()?, "item", "second")?;
    let before = Snapshot::take();
    fixture::append_one(store.events(), &other, "one").await?;
    put_written(
        measured,
        &format!("{}.append.bytes_written", provider.name()),
        before,
    );
    Ok(())
}

static FOLDS: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize, Deserialize)]
struct Counter {
    n: u64,
}

impl Aggregate for Counter {
    type Command = usize;
    type Event = SnapshotIncrement;
    type Error = EventLogError;
    const TYPE: &'static str = "kpi_counter";
    const STATE_SCHEMA_VERSION: u32 = 1;

    fn empty(_: &str) -> Self {
        Self { n: 0 }
    }

    fn apply(&mut self, _: &Applied<'_, Self::Event>) {
        self.n += 1;
        FOLDS.fetch_add(1, Ordering::Relaxed);
    }

    fn decide(&self, count: &usize) -> Result<Vec<Self::Event>, Self::Error> {
        Ok((0..*count).map(|_| SnapshotIncrement).collect())
    }
}

/// Review P9: one one-event command, then 150 two-event commands, under the default snapshot
/// policy. Every threshold is crossed at an odd version, so no snapshot is taken.
async fn core_folds(measured: &mut Measured) -> Outcome<()> {
    let store = Arc::new(SqliteEventStore::in_memory("kpi").await?);
    let repository = Repository::<Counter>::new(store);
    let tenant = fixture::tenant()?;
    FOLDS.store(0, Ordering::Relaxed);
    for index in 0..151_usize {
        let count = if index == 0 { 1 } else { 2 };
        let key = format!("command-{index}");
        repository
            .handle(
                &tenant,
                "one",
                &count,
                &meta(&key, &json!({ "count": count })),
            )
            .await?;
    }
    measured.put_count(
        "core.fold_calls.151_commands",
        FOLDS.load(Ordering::Relaxed),
    );
    Ok(())
}

/// Review P5: 100 loads of streams that do not exist.
async fn absent_reads(place: &Place) -> Outcome<()> {
    let repository = Repository::<Counter>::new(place.open_shared().await?);
    let tenant = fixture::tenant()?;
    for index in 0..100 {
        let loaded = repository.load(&tenant, &format!("absent-{index}")).await?;
        ensure(loaded.is_new, "an absent stream loads as new")?;
    }
    Ok(())
}

async fn file(scratch: &Path, measured: &mut Measured) -> Outcome<()> {
    let tenant = fixture::tenant()?;
    // P8 is the open in `reopened_128`; P13 pages the same 128 events eight at a time.
    let store = reopened_128(Provider::File, scratch, measured).await?;
    let before = Snapshot::take();
    let page = store.events().read_feed(&tenant, 64, 8).await?;
    put_read(measured, "file.read_feed.bytes_read", before);
    ensure(page.events.len() == 8, "the feed page is full")?;
    hooks::file();
    let mut cursor = 0;
    let mut events = 0;
    loop {
        let page = store.events().read_feed(&tenant, cursor, 8).await?;
        events += page.events.len();
        cursor = page.next_position;
        if !page.has_more {
            break;
        }
    }
    ensure(events == 128, "the feed returned every event")?;
    put_hook(
        measured,
        "file.feed.map_visits",
        hooks::file().map(|counters| counters.feed_visits),
    );
    append_written(Provider::File, &store, measured).await?;
    store.close().await;

    // P7: a drained catch-up projector polled 20 more times.
    let place = Place::new(Provider::File, scratch, None)?;
    let store = place.open().await?;
    let stream = fixture::stream(&tenant, "item", "first")?;
    fixture::seed(store.events(), &stream, 8, "seed").await?;
    store.events().create_projections(Arc::new(Tally)).await?;
    while store
        .events()
        .run_catch_up(Arc::new(Tally), &tenant, 100)
        .await?
        .applied
        > 0
    {}
    let journal = place.file_root().join("events.jsonl");
    let before = std::fs::metadata(&journal)?.len();
    let mut applied = 0;
    for _ in 0..20 {
        applied += store
            .events()
            .run_catch_up(Arc::new(Tally), &tenant, 100)
            .await?
            .applied;
    }
    ensure(applied == 0, "an idle poll applies nothing")?;
    measured.put_count(
        "file.idle_poll.journal_bytes",
        std::fs::metadata(&journal)?.len().saturating_sub(before),
    );
    store.close().await;

    // L07: 64 stored blobs, then one append that binds none of them.
    let place = Place::new(Provider::File, scratch, None)?;
    let store = place.open().await?;
    for index in 0..64_u64 {
        store
            .events()
            .put_blob(&tenant, &format!("blob-{index}"), &index.to_le_bytes())
            .await?;
    }
    hooks::file();
    fixture::append_one(store.events(), &stream, "event-only").await?;
    let counters = hooks::file();
    put_hook(
        measured,
        "file.event_append.blob_dir_entries",
        counters.map(|counters| counters.blob_dir_entries),
    );
    put_hook(
        measured,
        "file.event_append.blob_dir_syncs",
        counters.map(|counters| counters.blob_dir_syncs),
    );
    store.close().await;
    Ok(())
}

async fn sqlite(scratch: &Path, measured: &mut Measured) -> Outcome<()> {
    let store = reopened_128(Provider::Sqlite, scratch, measured).await?;
    append_written(Provider::Sqlite, &store, measured).await?;
    store.close().await;
    let tenant = fixture::tenant()?;

    // P5
    let place = Place::new(Provider::Sqlite, scratch, None)?;
    absent_reads(&place).await?;
    let connection = rusqlite::Connection::open(place.sqlite_path())?;
    let rows: i64 =
        connection.query_row("SELECT COUNT(*) FROM kpi_snapshot_generations", [], |row| {
            row.get(0)
        })?;
    measured.put_count(
        "sqlite.absent_reads.generation_rows",
        u64::try_from(rows).unwrap_or(0),
    );
    drop(connection);

    // L08
    let place = Place::new(Provider::Sqlite, scratch, None)?;
    let store = fixture::projected(&place).await?;
    hooks::sqlite();
    let found = store
        .events()
        .projection_find(&ROWS, &tenant, "kind", "item", 10)
        .await?;
    put_hook(
        measured,
        "sqlite.projection_find.vm_steps",
        hooks::sqlite().map(|counters| counters.vm_steps),
    );
    ensure(found.len() == 10, "projection_find returned the ten items")?;
    store.close().await;

    // P10: 1,024 bindings in the predecessor blob table, migrated on open.
    let place = Place::new(Provider::Sqlite, scratch, None)?;
    let path = place.sqlite_path();
    {
        let mut connection = rusqlite::Connection::open(&path)?;
        connection.execute_batch(
            "CREATE TABLE owner_blobs (tenant_id TEXT NOT NULL,digest TEXT NOT NULL,\
             bytes BLOB NOT NULL,byte_count INTEGER NOT NULL,recorded_at TEXT NOT NULL,\
             PRIMARY KEY (tenant_id,digest));",
        )?;
        let transaction = connection.transaction()?;
        for index in 0..1024 {
            transaction.execute(
                "INSERT INTO owner_blobs VALUES('owner',?1,x'616263',3,'1970-01-01T00:00:00Z')",
                [format!("item-{index:06}")],
            )?;
        }
        transaction.commit()?;
    }
    hooks::sqlite();
    let (store, report) = SqliteEventStore::open_with_blob_migration(
        &path.to_string_lossy(),
        "owner",
        LegacyBlobMigration::TrustObservedBytes,
    )
    .await?;
    put_hook(
        measured,
        "sqlite.legacy_migration.vm_steps",
        hooks::sqlite().map(|counters| counters.vm_steps),
    );
    ensure(
        report.trusted_legacy_rows == 1024,
        "the migration trusted every legacy binding",
    )?;
    drop(store);

    // L16: an inline rebuild over 2,048 events, more than two read pages of 1,000.
    let place = Place::new(Provider::Sqlite, scratch, None)?;
    let store = SqliteEventStore::open(&place.sqlite_path().to_string_lossy(), "kpi").await?;
    let projector: Arc<dyn Projector> = Arc::new(Tally);
    store.create_projections(Arc::clone(&projector)).await?;
    store.register_inline(projector).await?;
    let stream = fixture::stream(&tenant, "item", "rebuilt")?;
    fixture::seed(&store, &stream, 1024, "first").await?;
    fixture::seed(&store, &stream, 1024, "second").await?;
    hooks::sqlite();
    let rebuilt = store.rebuild_inline_projection("tally", &tenant).await?;
    ensure(rebuilt.applied == 2048, "the rebuild applied every event")?;
    put_hook(
        measured,
        "sqlite.inline_rebuild.retained_events",
        hooks::sqlite().map(|counters| counters.rebuild_retained_events),
    );
    Ok(())
}

async fn tree(scratch: &Path, measured: &mut Measured) -> Outcome<()> {
    let store = reopened_128(Provider::Tree, scratch, measured).await?;
    append_written(Provider::Tree, &store, measured).await?;
    store.close().await;
    let tenant = fixture::tenant()?;

    // L17, L20: 64 earlier groups, then one more single-event append.
    let place = Place::new(Provider::Tree, scratch, None)?;
    let store = place.open().await?;
    let stream = fixture::stream(&tenant, "item", "first")?;
    for index in 0..64 {
        increment(store.events(), &stream, &format!("append-{index}")).await?;
    }
    hooks::tree();
    increment(store.events(), &stream, "last").await?;
    let counters = hooks::tree();
    put_hook(
        measured,
        "tree.append.group_files_listed",
        counters.map(|counters| counters.group_files_listed),
    );
    put_hook(
        measured,
        "tree.append.event_digests",
        counters.map(|counters| counters.event_digests),
    );
    store.close().await;

    // L08 through the Tree's SQLite engine.
    let place = Place::new(Provider::Tree, scratch, None)?;
    let store = fixture::projected(&place).await?;
    hooks::sqlite();
    let found = store
        .events()
        .projection_find(&ROWS, &tenant, "kind", "item", 10)
        .await?;
    put_hook(
        measured,
        "tree.projection_find.vm_steps",
        hooks::sqlite().map(|counters| counters.vm_steps),
    );
    ensure(found.len() == 10, "projection_find returned the ten items")?;
    store.close().await;
    Ok(())
}

/// One more event on `stream`, whatever its head: on Tree, one more group file.
async fn increment(
    store: &dyn EventStore,
    stream: &eventlog_core::StreamId,
    key: &str,
) -> Outcome<()> {
    store
        .append(
            stream,
            Expected::Any,
            &[event("increment", 1)],
            &meta(key, &json!({ "key": key })),
        )
        .await?;
    Ok(())
}

/// Review L05: a full page of 1,000 over 1,001 rows must say there is more.
async fn projection_page(place: &Place, measured: &mut Measured) -> Outcome<()> {
    let store = fixture::projected(place).await?;
    let tenant = fixture::tenant()?;
    let page = store
        .events()
        .projection_page(&ROWS, &tenant, None, None, 1000)
        .await?;
    ensure(page.rows.len() == 1000, "the page is full")?;
    let last = store
        .events()
        .projection_get(&ROWS, &tenant, &format!("key-{:05}", SEEDED_ROWS - 1))
        .await?;
    ensure(last.is_some(), "row 1,001 exists")?;
    measured.put_count(
        &format!("{}.projection_page.cursor_present", place.provider.name()),
        u64::from(page.next_cursor.is_some()),
    );
    store.discard().await;
    Ok(())
}

/// The library's feed statement (`eventlog-postgres` `read_feed` over `watermarked_source`),
/// copied so `EXPLAIN` can see it. A change to that statement changes this copy in the same
/// commit, or the KPI measures a statement nobody runs.
fn feed_statement(prefix: &str) -> String {
    const COLUMNS: &str = "global_seq, tenant_id, stream_type, stream_id, version, event_id, \
         event_name, event_schema_version, occurred_at, recorded_at, subject, actor, request_id, \
         trace_id, causation_id, causation_depth, redacted_at, data";
    const WATERMARK: &str = "committed_xid < pg_snapshot_xmin(pg_current_snapshot())";
    format!(
        "SELECT {COLUMNS} FROM {prefix}_events CROSS JOIN \
         (SELECT MIN(global_seq) AS first_unsettled FROM {prefix}_events \
          WHERE NOT ({WATERMARK}) AND global_seq > \
          (SELECT COALESCE(MAX(settled_through), 0) FROM {prefix}_settlement_floor)) \
         AS publication_prefix \
         WHERE tenant_id = $1 AND global_seq > $2 \
           AND (first_unsettled IS NULL OR global_seq < first_unsettled) \
         ORDER BY global_seq LIMIT $3"
    )
}

/// The library's capture preflight (`eventlog-postgres` `capture.rs`), copied for `EXPLAIN`
/// under the same rule as [`feed_statement`].
fn preflight_statement(prefix: &str) -> String {
    format!(
        "SELECT (SELECT COUNT(*) FROM {prefix}_events WHERE tenant_id = $1),
                (SELECT COUNT(*) FROM {prefix}_blobs WHERE tenant_id = $1),
                (SELECT COALESCE(SUM(octet_length(bytes)),0)::bigint FROM {prefix}_blobs
                 WHERE tenant_id = $1),
                (SELECT COUNT(*) FROM {prefix}_blobs WHERE tenant_id = $1
                 AND byte_count IS DISTINCT FROM octet_length(bytes)::bigint)"
    )
}

async fn postgres(scratch: &Path, url: &str, measured: &mut Measured) -> Outcome<()> {
    let client = pg::client(url).await?;
    let proxy = pg::Proxy::start(url).await?;
    let tenant = fixture::tenant()?;

    // L19: admission of an existing schema on open. The local open runs the same per-table
    // shape comparison as a hosted open.
    let place = Place::new(Provider::Postgres, scratch, Some(url))?;
    place.open().await?.close().await;
    proxy.take();
    let store = place.open_at(Some(&proxy.url)).await?;
    measured.put_count("postgres.open.round_trips", proxy.take());

    // One single-event append, then P1: a guarded group carrying four blobs.
    let one = fixture::stream(&tenant, "item", "one")?;
    fixture::append_one(store.events(), &one, "one").await?;
    measured.put_count("postgres.append.round_trips", proxy.take());
    let (group, blobs) = fixture::blob_group(&tenant, "guarded", 1, 4, 1024)?;
    let before = pg::next_xid(&client).await?;
    proxy.take();
    fixture::guarded_blob_group(store.atomic(), &group, &blobs).await?;
    let round_trips = proxy.take();
    let after = pg::next_xid(&client).await?;
    measured.put_count(
        "postgres.blob_group.write_transactions",
        after.saturating_sub(before + 1),
    );
    measured.put_count("postgres.blob_group.round_trips", round_trips);
    store.discard().await;

    // P5
    let place = Place::new(Provider::Postgres, scratch, Some(url))?;
    absent_reads(&place).await?;
    let rows = pg::count_rows(&client, &format!("{}_snapshot_generations", place.prefix)).await?;
    measured.put_count("postgres.absent_reads.generation_rows", rows);
    place.open().await?.discard().await;

    // L05
    projection_page(
        &Place::new(Provider::Postgres, scratch, Some(url))?,
        measured,
    )
    .await?;

    // L15: 32,768 settled events over eight tenants, then one feed row past the last but one.
    let place = Place::new(Provider::Postgres, scratch, Some(url))?;
    let store = place.open().await?;
    for round in 0..4 {
        for owner in 0..8 {
            let owner_tenant = TenantId::new(format!("tenant-{owner}"))?;
            let stream = fixture::stream(&owner_tenant, "item", "subject")?;
            fixture::seed(
                store.events(),
                &stream,
                1024,
                &format!("seed-{round}-{owner}"),
            )
            .await?;
        }
    }
    let events = format!("{}_events", place.prefix);
    client.batch_execute(&format!("ANALYZE {events}")).await?;
    let last: i64 = client
        .query_one(
            &format!("SELECT MAX(global_seq) FROM {events} WHERE tenant_id = 'tenant-0'"),
            &[],
        )
        .await?
        .get(0);
    let plan = pg::explain(
        &client,
        &feed_statement(&place.prefix),
        &[&"tenant-0", &(last - 1), &2_i64],
    )
    .await?;
    measured.put_count("postgres.feed_one_row.buffers", plan.buffers);
    measured.put_count("postgres.feed_one_row.rows_filtered", plan.rows_filtered);
    store.discard().await;

    // L18: 8,192 blob bindings for one tenant, then the capture preflight.
    let place = Place::new(Provider::Postgres, scratch, Some(url))?;
    let store = place.open().await?;
    let Store::Postgres(postgres) = &store else {
        return Err(crate::Failure(
            "a PostgreSQL place opened another provider".into(),
        ));
    };
    for batch in 0..8 {
        let (group, blobs) = fixture::blob_group(&tenant, &format!("bulk-{batch}"), 1, 1024, 16)?;
        let request = BlobAppendGroup {
            group,
            blobs: blobs
                .into_iter()
                .map(|(digest, bytes)| BlobWrite { digest, bytes })
                .collect(),
        };
        postgres.append_group_with_blobs(&request).await?;
    }
    client
        .batch_execute(&format!("ANALYZE {}_blobs", place.prefix))
        .await?;
    let plan = pg::explain(
        &client,
        &preflight_statement(&place.prefix),
        &[&tenant.as_str()],
    )
    .await?;
    measured.put_count("postgres.capture_preflight.buffers", plan.buffers);
    store.discard().await;
    Ok(())
}
