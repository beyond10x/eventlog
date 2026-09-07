//! Read-only laboratory observation. This module does not select or admit a workload.
//!
//! The caller binds a disposable PostgreSQL server and its cgroup before starting.
//! Every sample carries actual elapsed time and errors: a failed observation is never zero.
//! Activity ages and sampled occupancy are not completed transaction/lock durations.
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use tokio_postgres::{Client, Config, NoTls};

pub struct MetricsConfig {
    pub postgres: Config,
    pub cgroup: PathBuf,
    pub output: PathBuf,
    pub stop_file: PathBuf,
    pub maximum_duration: Duration,
    pub interval: Duration,
    pub query_timeout: Duration,
}

struct Observer {
    client: Client,
    driver: tokio::task::JoinHandle<()>,
}

impl Observer {
    async fn close(mut self) {
        self.driver.abort();
        let _ = (&mut self.driver).await;
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

fn number(path: &Path) -> Result<u64, String> {
    fs::read_to_string(path)
        .map_err(|error| error.kind().to_string())?
        .trim()
        .parse()
        .map_err(|_| "invalid unsigned counter".to_owned())
}

fn counters(input: &str) -> Result<BTreeMap<String, u64>, String> {
    let mut result = BTreeMap::new();
    for line in input.lines() {
        let mut parts = line.split_whitespace();
        let key = parts.next().ok_or("missing counter name")?;
        let value = parts.next().ok_or("missing counter value")?;
        if parts.next().is_some() || result.contains_key(key) {
            return Err("ambiguous cgroup counter".to_owned());
        }
        result.insert(
            key.to_owned(),
            value.parse().map_err(|_| "invalid counter value")?,
        );
    }
    Ok(result)
}

fn read_counters(path: &Path) -> Result<BTreeMap<String, u64>, String> {
    counters(&fs::read_to_string(path).map_err(|error| error.kind().to_string())?)
}

fn read_io(path: &Path) -> Result<BTreeMap<String, BTreeMap<String, u64>>, String> {
    let input = fs::read_to_string(path).map_err(|error| error.kind().to_string())?;
    let mut devices = BTreeMap::new();
    for line in input.lines() {
        let mut fields = line.split_whitespace();
        let device = fields.next().ok_or("missing IO device")?;
        let mut values = BTreeMap::new();
        for field in fields {
            let (key, value) = field.split_once('=').ok_or("invalid IO counter")?;
            if values
                .insert(
                    key.to_owned(),
                    value.parse().map_err(|_| "invalid IO value")?,
                )
                .is_some()
            {
                return Err("duplicate IO counter".to_owned());
            }
        }
        if devices.insert(device.to_owned(), values).is_some() {
            return Err("duplicate IO device".to_owned());
        }
    }
    Ok(devices)
}

fn sample_cgroup(root: &Path) -> Result<Value, String> {
    Ok(json!({
        "cpu": read_counters(&root.join("cpu.stat"))?,
        "memory_current_bytes": number(&root.join("memory.current"))?,
        "memory_events": read_counters(&root.join("memory.events"))?,
        "io_by_device": read_io(&root.join("io.stat"))?,
    }))
}

async fn connect(config: &Config, timeout: Duration) -> Result<Observer, &'static str> {
    let mut configured = config.clone();
    configured.application_name("eventlog-proof-metrics");
    let (client, connection) = tokio::time::timeout(timeout, configured.connect(NoTls))
        .await
        .map_err(|_| "observer_connect_timeout")?
        .map_err(|_| "observer_connect_failed")?;
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(Observer { client, driver })
}

async fn sample_postgres(client: &Client, timeout: Duration) -> Result<Value, &'static str> {
    // No query text, addresses, usernames, application names, database names or row values.
    // The activity population is all client backends on this isolated server except this probe.
    // max_query_age of lock waiters is an upper bound on lock wait, not the wait itself.
    let query = r"
        SELECT jsonb_build_object(
          'observed_at', clock_timestamp(),
          'connections', count(*),
          'active', count(*) FILTER (WHERE state = 'active'),
          'idle', count(*) FILTER (WHERE state = 'idle'),
          'idle_in_transaction', count(*) FILTER (WHERE state LIKE 'idle in transaction%'),
          'lock_waiters', count(*) FILTER (WHERE wait_event_type = 'Lock'),
          'transaction_age_max_ms', COALESCE(max(extract(epoch FROM clock_timestamp() - xact_start) * 1000), 0),
          'lock_waiter_query_age_upper_bound_max_ms', COALESCE(max(extract(epoch FROM clock_timestamp() - query_start) * 1000) FILTER (WHERE wait_event_type = 'Lock'), 0)
        ) FROM pg_stat_activity
        WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
    ";
    let row = tokio::time::timeout(timeout, client.query_one(query, &[]))
        .await
        .map_err(|_| "observer_query_timeout")?
        .map_err(|_| "observer_query_failed")?;
    row.try_get(0).map_err(|_| "observer_result_decode_failed")
}

async fn statement_snapshot(config: &Config, timeout: Duration) -> Value {
    let observer = match connect(config, timeout).await {
        Ok(observer) => observer,
        Err(error) => return json!({"status": "unavailable", "error": error}),
    };
    // PostgreSQL17/pg_stat_statements1.11 schema, verified against the assigned test server.
    // Classify on the server; the retained result contains no SQL text or statement identifiers.
    // These are cumulative statement timings, never whole-transaction wall-clock timings.
    let query = r"
      WITH classified AS (
        SELECT CASE
          WHEN query ILIKE '%pg_advisory_%lock%' THEN 'advisory_lock'
          WHEN lower(query) ~ '^begin|^start transaction' THEN 'begin'
          WHEN lower(query) ~ '^commit' THEN 'commit'
          WHEN lower(query) ~ '^rollback' THEN 'rollback'
          WHEN lower(query) ~ '^insert' THEN 'insert'
          WHEN lower(query) ~ '^update' THEN 'update'
          WHEN lower(query) ~ '^delete' THEN 'delete'
          WHEN lower(query) ~ '^select' THEN 'read'
          ELSE 'other'
        END AS category, calls, rows, total_exec_time,
        shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,
        temp_blks_read, temp_blks_written, shared_blk_read_time, shared_blk_write_time
        FROM pg_stat_statements
        WHERE query NOT ILIKE '%pg_stat_statements%'
          AND query NOT ILIKE '%pg_stat_activity%'
      ), grouped AS (
        SELECT category, sum(calls) AS calls, sum(rows) AS rows,
          sum(total_exec_time) AS statement_execution_ms,
          sum(shared_blks_hit) AS shared_blocks_hit,
          sum(shared_blks_read) AS shared_blocks_read,
          sum(shared_blks_dirtied) AS shared_blocks_dirtied,
          sum(shared_blks_written) AS shared_blocks_written,
          sum(temp_blks_read) AS temp_blocks_read,
          sum(temp_blks_written) AS temp_blocks_written,
          sum(shared_blk_read_time) AS shared_block_read_ms,
          sum(shared_blk_write_time) AS shared_block_write_ms
        FROM classified GROUP BY category
      ) SELECT jsonb_build_object(
        'categories', COALESCE((SELECT jsonb_agg(to_jsonb(g) ORDER BY category) FROM grouped g), '[]'::jsonb),
        'stats_reset', (SELECT stats_reset FROM pg_stat_statements_info),
        'deallocations', (SELECT dealloc FROM pg_stat_statements_info)
      )
    ";
    let value = match tokio::time::timeout(timeout, observer.client.query_one(query, &[])).await {
        Ok(Ok(row)) => match row.try_get::<_, Value>(0) {
            Ok(value) => json!({"status": "observed", "value": value}),
            Err(_) => json!({"status": "unavailable", "error": "statement_snapshot_decode_failed"}),
        },
        Ok(Err(_)) => json!({"status": "unavailable", "error": "statement_snapshot_failed"}),
        Err(_) => json!({"status": "unavailable", "error": "statement_snapshot_timeout"}),
    };
    observer.close().await;
    value
}

fn write_record(output: &mut BufWriter<File>, value: &Value) -> io::Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    output.write_all(b"\n")?;
    output.flush()
}

fn utc_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn delta_counter(first: &Value, last: &Value, path: &[&str]) -> Value {
    let get = |value: &Value| {
        path.iter()
            .try_fold(value, |current, key| current.get(*key))
            .and_then(Value::as_u64)
    };
    match (get(first), get(last)) {
        (Some(before), Some(after)) => after.checked_sub(before).map_or_else(
            || json!({"status": "counter_reset", "before": before, "after": after}),
            |delta| json!({"status": "observed", "delta": delta}),
        ),
        _ => json!({"status": "unavailable"}),
    }
}

/// Collect raw observations until the caller's stop file appears or the time limit expires.
///
/// # Errors
/// Refuses a pre-existing output or stop file, zero/unbounded sampling intervals, and IO errors.
/// PostgreSQL/cgroup observation failures are retained in the JSONL and summary instead of
/// failing the observer silently. The workload admission runner must inspect these failures.
pub async fn collect(config: MetricsConfig) -> io::Result<Value> {
    if config.interval < Duration::from_millis(10)
        || config.interval > Duration::from_secs(10)
        || config.maximum_duration.is_zero()
        || config.query_timeout.is_zero()
        || config.stop_file.exists()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid metrics interval, duration or existing stop file",
        ));
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&config.output)?;
    let mut output = BufWriter::new(file);
    let start = Instant::now();
    let started_unix_ms = utc_unix_ms();
    let statements_before = statement_snapshot(&config.postgres, config.query_timeout).await;
    write_record(
        &mut output,
        &json!({
            "kind": "start", "format": "eventlog.lab-metrics/1",
            "started_unix_ms": started_unix_ms,
            "interval_ms": config.interval.as_millis(),
            "query_timeout_ms": config.query_timeout.as_millis(),
            "maximum_duration_ms": config.maximum_duration.as_millis(),
            "cpu_max": fs::read_to_string(config.cgroup.join("cpu.max")).ok().map(|value| value.trim().to_owned()),
            "memory_max": fs::read_to_string(config.cgroup.join("memory.max")).ok().map(|value| value.trim().to_owned()),
            "observer_connections": 1,
            "scope": "disposable server cgroup and all other client backends; no query or domain data",
            "limits": "sampled occupancy/ages only; no completed transaction/lock-duration claim; totals include server background work"
        }),
    )?;

    let mut observer = None;
    let mut first_cgroup = None;
    let mut last_cgroup = None;
    let mut samples = 0_u64;
    let mut cgroup_failures = 0_u64;
    let mut postgres_failures = 0_u64;
    let mut connections_max = 0_u64;
    let mut memory_max = 0_u64;
    let mut lock_waiters_max = 0_u64;
    let mut previous_tick = None;
    let mut maximum_gap_ms = 0_u128;
    let mut occupancy_lock_backend_ms = 0_u128;
    let mut previous_lock_waiters = None;
    let mut previous_pg_success = false;
    loop {
        let tick = start.elapsed();
        let gap_ms = previous_tick.map(|previous| tick.saturating_sub(previous).as_millis());
        if let Some(gap) = gap_ms {
            maximum_gap_ms = maximum_gap_ms.max(gap);
        }
        previous_tick = Some(tick);
        let cgroup = match sample_cgroup(&config.cgroup) {
            Ok(value) => {
                memory_max = memory_max.max(value["memory_current_bytes"].as_u64().unwrap_or(0));
                if first_cgroup.is_none() {
                    first_cgroup = Some(value.clone());
                }
                last_cgroup = Some(value.clone());
                json!({"status": "observed", "value": value})
            }
            Err(error) => {
                cgroup_failures += 1;
                json!({"status": "unavailable", "error": error})
            }
        };
        let connect_error = if observer.is_none() {
            match connect(&config.postgres, config.query_timeout).await {
                Ok(connected) => {
                    observer = Some(connected);
                    None
                }
                Err(error) => Some(error),
            }
        } else {
            None
        };
        let observation = match &observer {
            Some(connected) => sample_postgres(&connected.client, config.query_timeout).await,
            None => Err(connect_error.unwrap_or("observer_unavailable")),
        };
        let postgres = match observation {
            Ok(value) => {
                let connections = value["connections"].as_u64().unwrap_or(0);
                let waiters = value["lock_waiters"].as_u64().unwrap_or(0);
                // Only integrate adjacent successful samples; missing intervals stay missing.
                if previous_pg_success
                    && let (Some(previous), Some(gap)) = (previous_lock_waiters, gap_ms)
                {
                    occupancy_lock_backend_ms += u128::from(previous) * gap;
                }
                connections_max = connections_max.max(connections);
                lock_waiters_max = lock_waiters_max.max(waiters);
                previous_lock_waiters = Some(waiters);
                previous_pg_success = true;
                json!({"status": "observed", "value": value})
            }
            Err(error) => {
                postgres_failures += 1;
                previous_lock_waiters = None;
                previous_pg_success = false;
                if let Some(disconnected) = observer.take() {
                    disconnected.close().await;
                }
                json!({"status": "unavailable", "error": error})
            }
        };
        samples += 1;
        write_record(
            &mut output,
            &json!({
                "kind": "sample", "sample": samples,
                "elapsed_ms": tick.as_millis(), "unix_ms": utc_unix_ms(),
                "collection_duration_ms": start.elapsed().saturating_sub(tick).as_millis(),
                "cgroup": cgroup, "postgres": postgres
            }),
        )?;
        if config.stop_file.exists() || start.elapsed() >= config.maximum_duration {
            break;
        }
        // Do not burst missed samples to catch up: preserve actual observation cadence.
        let remainder = config
            .interval
            .saturating_sub(start.elapsed().saturating_sub(tick));
        tokio::time::sleep(remainder).await;
    }
    if let Some(connected) = observer.take() {
        connected.close().await;
    }
    let statements_after = statement_snapshot(&config.postgres, config.query_timeout).await;
    let first = first_cgroup.unwrap_or(Value::Null);
    let last = last_cgroup.unwrap_or(Value::Null);
    let summary = json!({
        "kind": "summary", "elapsed_ms": start.elapsed().as_millis(),
        "ended_unix_ms": utc_unix_ms(), "samples": samples,
        "stop_reason": if config.stop_file.exists() { "caller_stop" } else { "maximum_duration" },
        "cgroup_failures": cgroup_failures, "postgres_failures": postgres_failures,
        "sample_gap_max_ms": maximum_gap_ms,
        "connections_max_excluding_observer": connections_max,
        "observer_connections_additional": 1,
        "memory_current_sampled_max_bytes": memory_max,
        "lock_waiters_sampled_max": lock_waiters_max,
        "lock_wait_backend_ms_left_sampled_estimate": occupancy_lock_backend_ms,
        "cpu_usage_usec_delta": delta_counter(&first, &last, &["cpu", "usage_usec"]),
        "cpu_throttled_usec_delta": delta_counter(&first, &last, &["cpu", "throttled_usec"]),
        "cgroup_first": first, "cgroup_last": last,
        "statements_before": statements_before, "statements_after": statements_after,
        "acceptance": "observations only; caller checks missing samples, counter resets and workload budgets"
    });
    write_record(&mut output, &summary)?;
    Ok(summary)
}
