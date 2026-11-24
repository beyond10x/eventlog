//! The skewed-tenant fixture: one large tenant and many small ones, `rare` rare table-wide.
//!
//! A custom plan sees the value `rare` and reads the digest index: a handful of tuples. A generic
//! plan cannot see the value, takes the tenant's primary-key walk, and reads the whole tenant.
//! A prepared `projection_find` is safe only while it never goes generic, and no other KPI sees
//! the difference, so the fixture proves on every run that it can: the conditions E1 and E2 are
//! read from `EXPLAIN (ANALYZE, BUFFERS)` of a prepared copy of the statement and a condition that
//! does not hold is an error of the run, not a regression verdict. E3 (does `auto` flip on this
//! fixture) is only recorded.
//!
//! The rows go in by direct SQL on the indexed value column, so the same runner seeds the table
//! of 0.10.0 (which has no digest column) and of head, and the table stays below 30,000 rows so
//! that `ANALYZE` reads every row and the statistics do not vary between runs.
use std::{path::Path, sync::Arc, time::Instant};

use eventlog_core::{
    BoxFuture, EventLogError, ProjectionSpec, ProjectionStore, Projector, RecordedEvent, TenantId,
};
use serde_json::Value;

use crate::{
    Failure, Measured, Outcome, count, ensure,
    fixture::{Place, Provider, Store},
    pg,
};

pub const SKEWED: ProjectionSpec = ProjectionSpec {
    name: "kpi_skewed",
    indexed: &["kind"],
};

/// Rows of the large tenant; its last [`RARE_ROWS`] keys hold `rare`, the rest `open` and
/// `closed` alternately.
pub const BIG_ROWS: u64 = 25_000;
pub const SMALL_TENANTS: u64 = 63;
pub const SMALL_ROWS: u64 = 75;
pub const RARE_ROWS: u64 = 10;
/// `ANALYZE` reads every row of a table up to 300 x the default statistics target of 100.
pub const MAX_ROWS: u64 = 30_000;
const BIG_TENANT: &str = "big";
const RARE: &str = "rare";
/// Finds per timing round, and per counter window.
pub const FINDS: u64 = 16;
const LIMIT: usize = 10;

/// The conditions E1 and E2, as tuple counts: E1 at most this many under a custom plan; E2 at
/// least this many times E1 under a generic plan.
const E1_MAX_TUPLES: u64 = 50;
const E2_MIN_FACTOR: u64 = 100;

struct Nothing;

impl Projector for Nothing {
    fn name(&self) -> &'static str {
        "kpi_skewed"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&SKEWED)
    }

    fn apply<'a>(
        &'a self,
        _event: &'a RecordedEvent,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A store holding the seeded table, and the table's name.
struct Seeded {
    store: Store,
    table: String,
}

/// The projection table's name: a copy of `projection_table`
/// (`crates/eventlog-postgres/src/lib.rs:2007`). A drift fails the run at its first statement.
fn table_name(place: &Place) -> String {
    format!("{}_p_{}", place.prefix, SKEWED.name)
}

/// Create the projection through the store, seed it by direct SQL and `VACUUM (ANALYZE)` it. The
/// store and the setup session carry `application` as their `application_name`.
async fn seed(place: &Place, url: &str, application: &str) -> Outcome<Seeded> {
    let store = place
        .open_at(Some(&pg::with_application(url, application)))
        .await?;
    store.events().create_projections(Arc::new(Nothing)).await?;
    let table = table_name(place);
    let client = pg::client(&pg::with_application(url, application)).await?;
    let kind = format!(
        "CASE WHEN g > {} THEN '{RARE}' WHEN g % 2 = 0 THEN 'open' ELSE 'closed' END",
        BIG_ROWS - RARE_ROWS
    );
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
             SELECT '{BIG_TENANT}', 'key-' || lpad(g::text, 6, '0'),
                    jsonb_build_object('kind', {kind}, 'n', g), {kind}
             FROM generate_series(1, {BIG_ROWS}) AS g"
        ))
        .await?;
    let kind = "CASE WHEN g % 2 = 0 THEN 'open' ELSE 'closed' END";
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
             SELECT 'small-' || lpad(t::text, 2, '0'), 'key-' || lpad(g::text, 6, '0'),
                    jsonb_build_object('kind', {kind}, 'n', g), {kind}
             FROM generate_series(1, {SMALL_TENANTS}) AS t, generate_series(1, {SMALL_ROWS}) AS g"
        ))
        .await?;
    client
        .batch_execute(&format!("VACUUM (ANALYZE) {table}"))
        .await?;
    let rows = pg::count_rows(&client, &table).await?;
    let wanted = BIG_ROWS + SMALL_TENANTS * SMALL_ROWS;
    ensure(
        wanted <= MAX_ROWS,
        format!("{wanted} rows are more than ANALYZE reads in full ({MAX_ROWS})"),
    )?;
    ensure(
        rows == wanted,
        format!("the skewed fixture holds {rows} rows, not {wanted}"),
    )?;
    Ok(Seeded { store, table })
}

/// One find of the timed kind: `rare`, limit 10, in the large tenant, through the store.
async fn find(store: &Store, tenant: &TenantId) -> Outcome<()> {
    let found = store
        .events()
        .projection_find(&SKEWED, tenant, "kind", RARE, LIMIT)
        .await?;
    ensure(
        u64::try_from(found.len()).is_ok_and(|rows| rows == RARE_ROWS),
        "the skewed find returned the rare rows",
    )
}

/// The timing scenario: [`FINDS`] finds of `rare` in the large tenant, in microseconds.
pub async fn time_finds(scratch: &Path, url: &str) -> Outcome<Vec<f64>> {
    let place = Place::new(Provider::Postgres, scratch, Some(url))?;
    let seeded = seed(&place, url, "kpi_skewed_timing").await?;
    let tenant = TenantId::new(BIG_TENANT)?;
    let mut samples = Vec::new();
    for _ in 0..FINDS {
        let start = Instant::now();
        find(&seeded.store, &tenant).await?;
        samples.push(start.elapsed().as_secs_f64() * 1e6);
    }
    seeded.store.discard().await;
    Ok(samples)
}

/// Tuples read from the table and its indexes by a store that opens, makes `finds` finds and
/// closes: the statistics before and after, each read once from a session of its own after every
/// backend of the store (and of the step before) has exited and so flushed its counts.
async fn window(
    place: &Place,
    url: &str,
    observer: &tokio_postgres::Client,
    table: &str,
    finds: u64,
    application: &str,
) -> Outcome<u64> {
    let before = pg::tuples_read(url, table).await?;
    let store = place
        .open_at(Some(&pg::with_application(url, application)))
        .await?;
    let tenant = TenantId::new(BIG_TENANT)?;
    for _ in 0..finds {
        find(&store, &tenant).await?;
    }
    store.close().await;
    pg::await_backends_gone(observer, application).await?;
    let after = pg::tuples_read(url, table).await?;
    Ok(after.saturating_sub(before))
}

/// The counter `postgres.projection_find_skewed.tuples_read`, and E1, E2 and E3.
///
/// Procedure: seed and close the setup store; wait until its backends are gone; read the
/// statistics; open a store and close it again with no find (what opening reads is not a find's);
/// open a store, run [`FINDS`] finds, close it; the counter is the finds' extra reads over the
/// idle window, per find. The plan conditions are read last, on a session of their own, so that
/// their reads never fall in a window.
pub async fn counters(scratch: &Path, url: &str, measured: &mut Measured) -> Outcome<()> {
    let place = Place::new(Provider::Postgres, scratch, Some(url))?;
    let observer = pg::client(url).await?;
    let setup = format!("{}_setup", place.prefix);
    let seeded = seed(&place, url, &setup).await?;
    seeded.store.close().await;
    pg::await_backends_gone(&observer, &setup).await?;
    let idle = window(
        &place,
        url,
        &observer,
        &seeded.table,
        0,
        &format!("{}_idle", place.prefix),
    )
    .await?;
    let busy = window(
        &place,
        url,
        &observer,
        &seeded.table,
        FINDS,
        &format!("{}_busy", place.prefix),
    )
    .await?;
    let finds = busy.saturating_sub(idle);
    println!(
        "eventlog-kpi: skewed fixture: {} rows, {finds} tuples read by {FINDS} finds \
         ({idle} read by opening and closing a store with none)",
        BIG_ROWS + SMALL_TENANTS * SMALL_ROWS
    );
    measured.put(
        "postgres.projection_find_skewed.tuples_read",
        count(finds) / count(FINDS),
    );
    conditions(&place, url, &seeded.table).await?;
    place.open().await?.discard().await;
    Ok(())
}

/// The library's find statement for the large tenant (`eventlog-postgres` `projection_find` over
/// `schema::projection_find_filter`), copied so `EXPLAIN` can see it as a prepared statement. A
/// change to that statement changes this copy in the same commit, or the conditions are read from
/// a statement nobody runs. The counter above measures the real path, not this copy.
fn find_statement(prefix: &str, table: &str) -> String {
    format!(
        "SELECT body, idx_0 FROM {table}
         WHERE tenant_id = $1 AND idx_0_digest = hashtextextended($2::text, (0)::bigint)
           AND NOT EXISTS (SELECT FROM {prefix}_projection_registry
                           WHERE projection_name = $4 AND indexed_fields <> $5)
         ORDER BY row_key LIMIT $3"
    )
}

fn execute_call(name: &str) -> String {
    format!(
        "EXECUTE {name}('{BIG_TENANT}', '{RARE}', {LIMIT}, '{}', '[\"kind\"]')",
        SKEWED.name
    )
}

const PARAMETER_TYPES: &str = "text, text, bigint, text, jsonb";

/// One scan of the fixture table in a plan: how it ran and how many tuples it read.
#[derive(Debug, PartialEq)]
pub struct Scan {
    pub node: String,
    pub index: Option<String>,
    pub estimated: u64,
    pub actual: u64,
    /// Rows the scan returned plus the rows its filters and rechecks removed, over every loop.
    pub tuples: u64,
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "plan row counts are small non-negative whole numbers"
)]
fn whole(value: f64) -> u64 {
    let rounded = value.round();
    if rounded.is_finite() && rounded > 0.0 {
        rounded as u64
    } else {
        0
    }
}

fn number(node: &Value, key: &str) -> f64 {
    node.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

/// Every scan of `table` in an `EXPLAIN (ANALYZE, FORMAT JSON)` document, outermost first.
pub fn table_scans(document: &Value, table: &str) -> Vec<Scan> {
    fn walk(node: &Value, table: &str, scans: &mut Vec<Scan>) {
        if node.get("Relation Name").and_then(Value::as_str) == Some(table) {
            let loops = number(node, "Actual Loops").max(1.0);
            let read = number(node, "Actual Rows")
                + number(node, "Rows Removed by Filter")
                + number(node, "Rows Removed by Index Recheck");
            scans.push(Scan {
                node: node
                    .get("Node Type")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_owned(),
                index: node
                    .get("Index Name")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                estimated: whole(number(node, "Plan Rows")),
                actual: whole(number(node, "Actual Rows") * loops),
                tuples: whole(read * loops),
            });
        }
        for child in node
            .get("Plans")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            walk(child, table, scans);
        }
    }
    let mut scans = Vec::new();
    if let Some(root) = document.get(0).and_then(|entry| entry.get("Plan")) {
        walk(root, table, &mut scans);
    }
    scans
}

fn tuples(scans: &[Scan]) -> u64 {
    scans.iter().map(|scan| scan.tuples).sum()
}

/// E1 and E2 over the scans of the two plans; the tuples each read, or what did not hold.
///
/// E1: under a custom plan, an index scan on the digest index (`{table}_idx_0`) and at most
/// [`E1_MAX_TUPLES`] tuples read. E2: under a generic plan, an index scan on the primary key and
/// at least [`E2_MIN_FACTOR`] times E1's tuples.
pub fn judge(table: &str, custom: &[Scan], generic: &[Scan]) -> Result<(u64, u64), String> {
    let digest_index = format!("{table}_idx_0");
    let on_digest = custom.iter().any(|scan| {
        scan.node == "Index Scan" && scan.index.as_deref() == Some(digest_index.as_str())
    });
    if !on_digest {
        return Err(format!(
            "E1 does not hold: the custom plan is not an index scan on {digest_index}: {custom:?}"
        ));
    }
    let first = tuples(custom);
    if first > E1_MAX_TUPLES {
        return Err(format!(
            "E1 does not hold: the custom plan read {first} tuples, more than {E1_MAX_TUPLES}: \
             {custom:?}"
        ));
    }
    let walks = generic.iter().any(|scan| {
        scan.node == "Index Scan" && scan.index.as_deref().is_some_and(|i| i.ends_with("_pkey"))
    });
    if !walks {
        return Err(format!(
            "E2 does not hold: the generic plan is not a primary-key walk: {generic:?}"
        ));
    }
    let second = tuples(generic);
    if second < E2_MIN_FACTOR * first.max(1) {
        return Err(format!(
            "E2 does not hold: the generic plan read {second} tuples, fewer than \
             {E2_MIN_FACTOR} x {first}: {generic:?}"
        ));
    }
    Ok((first, second))
}

fn describe(scans: &[Scan], blocks: u64) -> String {
    let scans: Vec<String> = scans
        .iter()
        .map(|scan| {
            format!(
                "{} on {}, estimated {} rows, actual {} rows, {} tuples read",
                scan.node,
                scan.index.as_deref().unwrap_or("(no index)"),
                scan.estimated,
                scan.actual,
                scan.tuples
            )
        })
        .collect();
    format!("{}; {blocks} blocks", scans.join("; "))
}

/// E1 and E2 from `EXPLAIN (ANALYZE, BUFFERS)` of a prepared copy of the find statement under
/// `force_custom_plan` and `force_generic_plan`, each printed; E3 recorded: six executions under
/// `auto` in one session, and whether the sixth was generic.
async fn conditions(place: &Place, url: &str, table: &str) -> Outcome<()> {
    let client = pg::client(url).await?;
    client
        .batch_execute(&format!(
            "PREPARE kpi_find ({PARAMETER_TYPES}) AS {}",
            find_statement(&place.prefix, table)
        ))
        .await?;
    let call = execute_call("kpi_find");
    let mut plans = Vec::new();
    for mode in ["force_custom_plan", "force_generic_plan"] {
        client
            .batch_execute(&format!("SET plan_cache_mode = {mode}"))
            .await?;
        let document = pg::explain_document(&client, &call, &[]).await?;
        let blocks = document
            .get(0)
            .and_then(|entry| entry.get("Plan"))
            .map_or(0, pg::blocks);
        plans.push((table_scans(&document, table), blocks));
    }
    let (custom, custom_blocks) = &plans[0];
    let (generic, generic_blocks) = &plans[1];
    println!(
        "eventlog-kpi: skewed fixture E1, custom plan: {}",
        describe(custom, *custom_blocks)
    );
    println!(
        "eventlog-kpi: skewed fixture E2, forced generic plan: {}",
        describe(generic, *generic_blocks)
    );
    let (first, second) = judge(table, custom, generic).map_err(Failure)?;
    println!(
        "eventlog-kpi: skewed fixture: E1 holds ({first} tuples read), E2 holds ({second} tuples \
         read, {} x E1)",
        second / first.max(1)
    );

    client.batch_execute("SET plan_cache_mode = auto").await?;
    client
        .batch_execute(&format!(
            "PREPARE kpi_auto ({PARAMETER_TYPES}) AS {}",
            find_statement(&place.prefix, table)
        ))
        .await?;
    let call = execute_call("kpi_auto");
    for _ in 0..6 {
        client.simple_query(&call).await?;
    }
    let row = client
        .query_one(
            "SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = 'kpi_auto'",
            &[],
        )
        .await?;
    let (generic_plans, custom_plans): (i64, i64) = (row.get(0), row.get(1));
    println!(
        "eventlog-kpi: skewed fixture E3 (recorded only): after 6 executions under \
         plan_cache_mode = auto, {generic_plans} generic and {custom_plans} custom plans; \
         auto {} on this fixture",
        if generic_plans > 0 {
            "flipped to a generic plan"
        } else {
            "did not flip"
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const TABLE: &str = "kpi_a_p_kpi_skewed";

    fn document(node: &str, index: &str, estimated: u64, actual: u64, removed: u64) -> Value {
        json!([{ "Plan": {
            "Node Type": "Limit", "Plan Rows": 10, "Actual Rows": 10, "Actual Loops": 1,
            "Shared Hit Blocks": 4, "Shared Read Blocks": 0,
            "Plans": [
                { "Node Type": "Result", "Plans": [
                    { "Node Type": node, "Relation Name": TABLE, "Index Name": index,
                      "Plan Rows": estimated, "Actual Rows": actual, "Actual Loops": 1,
                      "Rows Removed by Filter": removed },
                    { "Node Type": "Seq Scan", "Relation Name": "kpi_a_projection_registry",
                      "Plan Rows": 1, "Actual Rows": 1, "Actual Loops": 1,
                      "Rows Removed by Filter": 0 }
                ] }
            ]
        } }])
    }

    fn custom() -> Vec<Scan> {
        table_scans(
            &document("Index Scan", &format!("{TABLE}_idx_0"), 8, 10, 0),
            TABLE,
        )
    }

    fn generic() -> Vec<Scan> {
        table_scans(
            &document("Index Scan", &format!("{TABLE}_pkey"), 155, 10, 24_990),
            TABLE,
        )
    }

    #[test]
    fn the_scans_of_the_fixture_table_are_read_from_the_plan_and_no_other_table_counts() {
        let scans = custom();
        assert_eq!(scans.len(), 1, "the registry's scan is not the fixture's");
        assert_eq!(scans[0].node, "Index Scan");
        assert_eq!(scans[0].estimated, 8);
        assert_eq!(scans[0].actual, 10);
        assert_eq!(scans[0].tuples, 10);
        assert_eq!(
            generic()[0].tuples,
            25_000,
            "rows returned plus rows filtered"
        );
    }

    #[test]
    fn loops_multiply_the_tuples_a_scan_read() {
        let document = json!([{ "Plan": {
            "Node Type": "Index Scan", "Relation Name": TABLE, "Index Name": "i",
            "Plan Rows": 1, "Actual Rows": 3, "Actual Loops": 4, "Rows Removed by Filter": 2 } }]);
        let scans = table_scans(&document, TABLE);
        assert_eq!(scans[0].tuples, 20);
        assert_eq!(scans[0].actual, 12);
    }

    #[test]
    fn a_fixture_that_shows_both_shapes_holds_e1_and_e2() {
        assert_eq!(judge(TABLE, &custom(), &generic()), Ok((10, 25_000)));
    }

    #[test]
    fn e1_fails_when_the_custom_plan_walks_or_reads_too_much() {
        let walking = table_scans(
            &document("Index Scan", &format!("{TABLE}_pkey"), 530, 10, 24_990),
            TABLE,
        );
        assert!(
            judge(TABLE, &walking, &generic())
                .unwrap_err()
                .starts_with("E1")
        );
        let sequential = table_scans(&document("Seq Scan", "", 8, 10, 0), TABLE);
        assert!(
            judge(TABLE, &sequential, &generic())
                .unwrap_err()
                .starts_with("E1")
        );
        let wide = table_scans(
            &document("Index Scan", &format!("{TABLE}_idx_0"), 8, 10, 41),
            TABLE,
        );
        assert!(
            judge(TABLE, &wide, &generic())
                .unwrap_err()
                .starts_with("E1")
        );
    }

    #[test]
    fn e2_fails_when_the_generic_plan_does_not_walk_far_more_than_the_custom_one() {
        let same = custom();
        assert!(
            judge(TABLE, &custom(), &same)
                .unwrap_err()
                .starts_with("E2")
        );
        let short_walk = table_scans(
            &document("Index Scan", &format!("{TABLE}_pkey"), 155, 10, 900),
            TABLE,
        );
        assert!(
            judge(TABLE, &custom(), &short_walk)
                .unwrap_err()
                .starts_with("E2"),
            "910 tuples is not 100 x 10"
        );
    }

    #[test]
    fn the_fixture_stays_below_the_rows_analyze_reads_in_full() {
        let rows = std::hint::black_box(BIG_ROWS) + SMALL_TENANTS * SMALL_ROWS;
        assert!(rows <= MAX_ROWS, "{rows} rows");
        assert!(std::hint::black_box(RARE_ROWS) <= BIG_ROWS);
        assert_eq!(
            u64::try_from(LIMIT).unwrap(),
            RARE_ROWS,
            "the find asks for exactly the rare rows"
        );
        assert!(
            std::hint::black_box(BIG_ROWS) > 5 * SMALL_TENANTS * SMALL_ROWS,
            "one tenant holds most of the table"
        );
    }
}
