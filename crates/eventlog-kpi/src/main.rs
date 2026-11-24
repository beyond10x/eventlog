//! The KPI gate of this repository: what the kit costs, measured the same way on every change.
//!
//! Two kinds of KPI, defined in `kpi/kpis.json`:
//!
//! - **Counters** are deterministic work counts on a fixed fixture: bytes read and written, SQLite
//!   VM steps, fold calls, map visits, rows created, allocation bytes, PostgreSQL round trips and
//!   buffers. `check` compares them with `kpi/baseline.json` under a tight tolerance and fails on a
//!   regression. Some need a counter inside a provider; those exist only with this crate's
//!   non-default `counters` feature (the providers' `kpi-counters`), so no product build carries
//!   them.
//! - **Timings** (latency p50/p95, throughput) are never compared with a number from another
//!   machine. `ab` builds two revisions on the runner it is on, runs them interleaved for several
//!   rounds and compares medians. `record` writes the local numbers to the baseline file for the
//!   report only.
//!
//! PostgreSQL scenarios run only when `EVENTLOG_TEST_POSTGRES_URL` names a disposable server, and
//! say so when skipped.

#![allow(
    clippy::missing_errors_doc,
    reason = "a binary: its functions are not an API, and each error is printed where it stops the run"
)]

mod ab;
mod alloc;
mod compare;
mod counter;
mod definition;
mod environment;
mod fixture;
mod hooks;
mod large_group;
mod pg;
mod proc_io;
mod scope;
mod skewed;
mod timing;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::{Parser, Subcommand};

/// Every failure of the runner itself, as opposed to a KPI verdict.
#[derive(Debug)]
pub struct Failure(pub String);

impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

macro_rules! failure_from {
    ($($source:ty),* $(,)?) => {
        $(impl From<$source> for Failure {
            fn from(error: $source) -> Self {
                Self(error.to_string())
            }
        })*
    };
}
failure_from!(
    eventlog_core::EventLogError,
    std::io::Error,
    rusqlite::Error,
    tokio_postgres::Error,
    serde_json::Error,
);

pub type Outcome<T> = Result<T, Failure>;

/// Fail with a message unless `condition` holds: a fixture that did not build is not a number.
pub fn ensure(condition: bool, message: impl Into<String>) -> Outcome<()> {
    if condition {
        Ok(())
    } else {
        Err(Failure(message.into()))
    }
}

/// What one run measured, by KPI id, and what it could not measure and why.
#[derive(Default)]
pub struct Measured {
    pub values: BTreeMap<String, f64>,
    pub skipped: BTreeMap<String, String>,
}

impl Measured {
    pub fn put(&mut self, id: &str, value: f64) {
        self.values.insert(id.to_owned(), value);
    }

    pub fn put_count(&mut self, id: &str, value: u64) {
        self.put(id, count(value));
    }

    pub fn skip(&mut self, prefix: &str, reason: &str) {
        self.skipped.insert(prefix.to_owned(), reason.to_owned());
    }

    /// Why `id` was not measured, when a skipped prefix covers it.
    pub fn skip_reason(&self, id: &str) -> Option<&str> {
        self.skipped
            .iter()
            .find(|(prefix, _)| id.starts_with(prefix.as_str()))
            .map(|(_, reason)| reason.as_str())
    }
}

/// A count as the number every KPI is stored as. Counts here stay far below 2^53.
#[allow(clippy::cast_precision_loss)]
pub fn count(value: u64) -> f64 {
    value as f64
}

const POSTGRES_URL: &str = "EVENTLOG_TEST_POSTGRES_URL";

fn postgres_url() -> Option<String> {
    std::env::var(POSTGRES_URL)
        .ok()
        .filter(|url| !url.trim().is_empty())
}

#[derive(Parser)]
#[command(about = "Eventlog KPI gate: counters against a baseline, timings A/B")]
struct Cli {
    /// Repository root holding `kpi/`; defaults to the checkout this binary was built from.
    #[arg(long, global = true)]
    root: Option<PathBuf>,
    /// Scratch directory for fixture stores and A/B builds; defaults to `$CARGO_TARGET_DIR/kpi-work`,
    /// else `<root>/target/kpi-work`. Everything under it is removed when a run ends.
    #[arg(long, global = true)]
    work: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Measure every counter and the local timings, and write `kpi/baseline.json`.
    Record {
        /// Timing rounds in this process; the baseline keeps the median of the rounds.
        #[arg(long, default_value_t = 3)]
        rounds: usize,
        /// Measure the counters only and rewrite no timing: the baseline's timings, their round
        /// count and its environment stay as recorded. A counter still within its tolerance of
        /// the measurement keeps its value. This is what the CI `record` dispatch runs.
        #[arg(long)]
        counters: bool,
    },
    /// Measure every counter and compare it with `kpi/baseline.json`; non-zero on regression.
    Check,
    /// Measure the timings once per round and print them (`--json` for `ab`).
    Timings {
        #[arg(long, default_value_t = 1)]
        rounds: usize,
        #[arg(long)]
        json: bool,
    },
    /// Build two revisions and compare their timings, interleaved on this machine.
    Ab {
        /// The revision measured against; defaults to the merge base with `origin/main`.
        #[arg(long)]
        base: Option<String>,
        #[arg(long, default_value = "HEAD")]
        head: String,
        /// Interleaved rounds per side; defaults to the definitions file's value.
        #[arg(long)]
        rounds: Option<usize>,
    },
    /// Say whether a change between two revisions needs the KPI lane (`run`) or is prose (`skip`).
    CiScope {
        #[arg(long)]
        base: String,
        #[arg(long)]
        head: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let root = cli.root.clone().unwrap_or_else(|| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap_or_else(|_| PathBuf::from("."))
    });
    let work = cli.work.clone().unwrap_or_else(|| {
        std::env::var_os("CARGO_TARGET_DIR")
            .map_or_else(|| root.join("target"), PathBuf::from)
            .join("kpi-work")
    });
    let result = match cli.command {
        Command::Record { rounds, counters } => {
            if counters {
                record_counters(&root, &work)
            } else {
                record(&root, &work, rounds)
            }
        }
        Command::Check => check(&root, &work),
        Command::Timings { rounds, json } => print_timings(&work, rounds, json),
        Command::Ab { base, head, rounds } => ab::run(&root, &work, base, &head, rounds),
        Command::CiScope { base, head } => scope::run(&root, &base, &head),
    };
    match result {
        Ok(code) => code,
        Err(failure) => {
            eprintln!("eventlog-kpi: {failure}");
            ExitCode::FAILURE
        }
    }
}

fn definitions_path(root: &Path) -> PathBuf {
    root.join("kpi/kpis.json")
}

fn baseline_path(root: &Path) -> PathBuf {
    root.join("kpi/baseline.json")
}

fn scratch(work: &Path) -> Outcome<tempfile::TempDir> {
    std::fs::create_dir_all(work)?;
    Ok(tempfile::Builder::new().prefix("kpi-").tempdir_in(work)?)
}

fn runtime() -> Outcome<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

/// Every counter: allocation scenarios first, before a runtime exists to allocate beside them.
fn measure_counters(work: &Path) -> Outcome<Measured> {
    let mut measured = Measured::default();
    alloc::measure(&mut measured)?;
    let directory = scratch(work)?;
    let url = postgres_url();
    runtime()?.block_on(counter::measure(
        directory.path(),
        url.as_deref(),
        &mut measured,
    ))?;
    Ok(measured)
}

fn measure_timings(work: &Path, rounds: usize) -> Outcome<timing::Rounds> {
    let directory = scratch(work)?;
    let url = postgres_url();
    let runtime = runtime()?;
    let mut all = timing::Rounds::default();
    for _ in 0..rounds.max(1) {
        let round = runtime.block_on(timing::measure(directory.path(), url.as_deref()))?;
        all.add(round);
    }
    Ok(all)
}

fn record(root: &Path, work: &Path, rounds: usize) -> Outcome<ExitCode> {
    let definitions = definition::Definitions::load(&definitions_path(root))?;
    let counters = measure_counters(work)?;
    let timings = measure_timings(work, rounds)?;
    definitions.require_known(counters.values.keys(), definition::Kind::Counter)?;
    definitions.require_known(timings.ids(), definition::Kind::Timing)?;
    let previous = definition::Baseline::load(&baseline_path(root)).ok();
    let baseline = definition::Baseline::new(
        root,
        &definitions,
        &counters,
        &timings,
        rounds,
        previous.as_ref(),
    );
    baseline.write(&baseline_path(root))?;
    compare::print_record(&definitions, &baseline, &counters);
    println!("eventlog-kpi: wrote {}", baseline_path(root).display());
    Ok(ExitCode::SUCCESS)
}

fn record_counters(root: &Path, work: &Path) -> Outcome<ExitCode> {
    let definitions = definition::Definitions::load(&definitions_path(root))?;
    let previous = definition::Baseline::load(&baseline_path(root))?;
    ensure(
        postgres_url().is_some(),
        "record --counters needs EVENTLOG_TEST_POSTGRES_URL: a baseline written without the \
         PostgreSQL counters would keep stale values for them",
    )?;
    let counters = measure_counters(work)?;
    definitions.require_known(counters.values.keys(), definition::Kind::Counter)?;
    let baseline = definition::Baseline::with_counters(root, &definitions, &counters, &previous);
    baseline.write(&baseline_path(root))?;
    compare::print_record(&definitions, &baseline, &counters);
    println!(
        "eventlog-kpi: wrote {} (counters only; timings untouched)",
        baseline_path(root).display()
    );
    Ok(ExitCode::SUCCESS)
}

fn check(root: &Path, work: &Path) -> Outcome<ExitCode> {
    let definitions = definition::Definitions::load(&definitions_path(root))?;
    let baseline = definition::Baseline::load(&baseline_path(root))?;
    let counters = measure_counters(work)?;
    definitions.require_known(counters.values.keys(), definition::Kind::Counter)?;
    let report = compare::check(&definitions, &baseline, &counters);
    report.print();
    Ok(if report.passed() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

fn print_timings(work: &Path, rounds: usize, json: bool) -> Outcome<ExitCode> {
    let timings = measure_timings(work, rounds)?;
    if json {
        println!("{}", serde_json::to_string(&timings.summary())?);
    } else {
        for (id, value) in timings.summary() {
            println!("{id:<44} p50 {:>12.1}  p95 {:>12.1}", value.p50, value.p95);
        }
        for (prefix, reason) in &timings.skipped {
            println!("{prefix:<44} skipped: {reason}");
        }
    }
    Ok(ExitCode::SUCCESS)
}
