//! The large atomic group: one `append_group` of thousands of single-event streams.
//!
//! The group's statements run once per `STATEMENT_BATCH` items. A prepared statement under
//! `plan_cache_mode = auto` can switch to a generic plan at its sixth execution, so a group of at
//! most five batches never reaches that execution and a KPI over it would be green by
//! construction. The size here is above five batches, the counters job asserts that the group
//! really executes its chunked statement at least six times (read from the counting proxy), and
//! the group has to commit inside the store's transaction timeout.
use std::{
    path::Path,
    time::{Duration, Instant},
};

use crate::{
    Failure, Outcome, ensure,
    fixture::{self, Place, Provider},
    pg,
};

/// A copy of `STATEMENT_BATCH`, private to `eventlog-postgres`
/// (`crates/eventlog-postgres/src/lib.rs:73`). A unit test reads that line and fails when this
/// copy drifts from it.
pub const STATEMENT_BATCH: usize = 1_000;

/// Single-event streams in the group: more than five statement batches, so the chunked
/// statement runs seven times.
pub const GROUP_STREAMS: usize = 6_500;

/// The fewest executions of the group's chunked statement the scenario accepts.
pub const MIN_EXECUTIONS: u64 = 6;

/// A copy of the store's default transaction timeout, `PoolOptions::default()`
/// (`crates/eventlog-postgres/src/pool.rs:230`). A unit test reads that line and fails when this
/// copy drifts from it.
pub const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(10);

/// How many times a group of `streams` items executes one of its chunked statements.
fn chunked_executions(streams: usize) -> u64 {
    u64::try_from(streams.div_ceil(STATEMENT_BATCH)).unwrap_or(u64::MAX)
}

/// Groups per timing round; the round's value is the median.
const TIMED_GROUPS: usize = 3;

/// Append one group of [`GROUP_STREAMS`] streams under `key` and return how long it took. A group
/// that did not fit the transaction timeout fails in the store; one that came close is refused
/// here too, so the scenario never measures a group at the edge of its limit.
async fn append(store: &fixture::Store, key: &str) -> Outcome<Duration> {
    let tenant = fixture::tenant()?;
    let (group, _) = fixture::blob_group(&tenant, key, GROUP_STREAMS, 0, 0)?;
    let start = Instant::now();
    store.atomic().append_group(&group).await?;
    let elapsed = start.elapsed();
    ensure(
        elapsed < TRANSACTION_TIMEOUT,
        format!(
            "a group of {GROUP_STREAMS} streams took {elapsed:?}, not inside the \
             {TRANSACTION_TIMEOUT:?} transaction timeout"
        ),
    )?;
    Ok(elapsed)
}

/// The timing scenario: [`TIMED_GROUPS`] groups, in microseconds.
pub async fn time_groups(scratch: &Path, url: &str) -> Outcome<Vec<f64>> {
    let place = Place::new(Provider::Postgres, scratch, Some(url))?;
    let store = place.open().await?;
    let mut samples = Vec::new();
    for index in 0..TIMED_GROUPS {
        let elapsed = append(&store, &format!("large-{index}")).await?;
        samples.push(elapsed.as_secs_f64() * 1e6);
    }
    store.discard().await;
    Ok(samples)
}

/// The counters job's assertion: the group executes its chunked statement at least
/// [`MIN_EXECUTIONS`] times, read from the counting proxy. Printed in the job log.
pub async fn assert_chunked(scratch: &Path, url: &str) -> Outcome<()> {
    let place = Place::new(Provider::Postgres, scratch, Some(url))?;
    let proxy = pg::Proxy::start(url).await?;
    let store = place.open_at(Some(&proxy.url)).await?;
    proxy.take_busiest_statement();
    let elapsed = append(&store, "large-counted").await?;
    let executions = proxy.take_busiest_statement();
    store.discard().await;
    println!(
        "eventlog-kpi: atomic_group_large: {GROUP_STREAMS} streams in {elapsed:?}, the chunked \
         statement executed {executions} times ({} expected, at least {MIN_EXECUTIONS} required)",
        chunked_executions(GROUP_STREAMS)
    );
    if executions < MIN_EXECUTIONS {
        return Err(Failure(format!(
            "the group of {GROUP_STREAMS} streams executed its chunked statement {executions} \
             times, fewer than {MIN_EXECUTIONS}: it would not reach a prepared statement's sixth \
             execution"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(path: &str) -> String {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        std::fs::read_to_string(root.join(path)).unwrap_or_else(|error| panic!("{path}: {error}"))
    }

    #[test]
    fn the_group_is_above_five_statement_batches_so_the_sixth_execution_is_reached() {
        assert!(
            GROUP_STREAMS.checked_sub(6_000).is_some(),
            "at least 6,000 single-event streams"
        );
        assert!(
            GROUP_STREAMS
                .checked_sub(5 * STATEMENT_BATCH)
                .is_some_and(|above| above > 0),
            "a group of at most 5 x STATEMENT_BATCH never reaches a sixth execution"
        );
        assert!(chunked_executions(GROUP_STREAMS) >= MIN_EXECUTIONS);
    }

    #[test]
    fn the_copy_of_statement_batch_is_the_one_in_eventlog_postgres() {
        let library = source("crates/eventlog-postgres/src/lib.rs");
        let line = library
            .lines()
            .find(|line| line.starts_with("const STATEMENT_BATCH: usize ="))
            .expect("crates/eventlog-postgres/src/lib.rs declares STATEMENT_BATCH");
        let value: usize = line
            .split('=')
            .nth(1)
            .expect("a value")
            .trim()
            .trim_end_matches(';')
            .replace('_', "")
            .parse()
            .expect("a number");
        assert_eq!(value, STATEMENT_BATCH, "copied from lib.rs:73: {line}");
    }

    #[test]
    fn the_copy_of_the_transaction_timeout_is_the_one_in_eventlog_postgres() {
        let pool = source("crates/eventlog-postgres/src/pool.rs");
        let wanted = format!(
            "transaction_timeout: Duration::from_secs({}),",
            TRANSACTION_TIMEOUT.as_secs()
        );
        assert!(
            pool.contains(&wanted),
            "pool.rs:230 no longer says `{wanted}`: update TRANSACTION_TIMEOUT"
        );
    }
}
