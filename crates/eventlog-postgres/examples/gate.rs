//! Repository gate, with shell retained only as a compatibility entry point.
#[path = "gate/lanes.rs"]
mod lanes;
use clap::Parser;
use lanes::Lane;
use std::process::{Command, ExitCode};
use std::time::SystemTime;
#[derive(Parser)]
struct Args {
    #[arg(long, conflicts_with = "slow")]
    production_proof: bool,
    /// Run only the KPI lane: every counter against `kpi/baseline.json` (AGENTS.md § The KPI gate).
    #[arg(long, conflicts_with_all = ["production_proof", "slow"])]
    kpi: bool,
    /// Run only the slow lane: the wall-clock budgets written for a release build, in one.
    #[arg(long)]
    slow: bool,
}
fn main() -> ExitCode {
    let args = Args::parse();
    let lane = if args.kpi {
        Lane::Kpi
    } else if args.slow {
        Lane::Slow
    } else if args.production_proof {
        Lane::ProductionProof
    } else {
        Lane::Default
    };
    for arguments in lanes::steps(lane) {
        println!("gate: cargo {}", arguments.join(" "));
        let started = SystemTime::now();
        let status = Command::new("cargo").args(&arguments).status();
        if arguments.first() == Some(&"nextest") {
            print_slowest(started);
        }
        match status {
            Ok(status) if status.success() => {}
            Ok(status) => {
                eprintln!("gate: refused with exit {:?}", status.code());
                return ExitCode::FAILURE;
            }
            Err(error) => {
                eprintln!("gate: cannot start cargo: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
    println!("gate: green");
    ExitCode::SUCCESS
}

/// The `JUnit` report `.config/nextest.toml` has nextest write, relative to the workspace root.
const REPORT: &str = "target/nextest/default/junit.xml";

/// Name the ten slowest tests of the run that just wrote [`REPORT`], slowest first, so a test that
/// grows towards the 60 s bound is seen before it reaches it. A report older than `started` is an
/// earlier run's, and is not passed off as this one's.
fn print_slowest(started: SystemTime) {
    let written = std::fs::metadata(REPORT).and_then(|report| report.modified());
    if !written.as_ref().is_ok_and(|written| *written >= started) {
        eprintln!("gate: this run wrote no test report at {REPORT}");
        return;
    }
    match std::fs::read_to_string(REPORT) {
        Ok(report) => {
            println!("gate: the ten slowest tests");
            for (seconds, test) in slowest(&report, 10) {
                println!("gate: {seconds:>8.3} s  {test}");
            }
        }
        Err(error) => eprintln!("gate: no test report at {REPORT}: {error}"),
    }
}

/// The `count` longest `<testcase>` entries of a `JUnit` report, as seconds and `binary test`.
fn slowest(report: &str, count: usize) -> Vec<(f64, String)> {
    let mut cases: Vec<(f64, String)> = report
        .split("<testcase ")
        .skip(1)
        .filter_map(|case| {
            let tag = case.split('>').next()?;
            let seconds = attribute(tag, "time")?.parse().ok()?;
            let binary = attribute(tag, "classname")?;
            let name = attribute(tag, "name")?;
            Some((seconds, format!("{binary} {name}")))
        })
        .collect();
    cases.sort_by(|left, right| right.0.total_cmp(&left.0));
    cases.truncate(count);
    cases
}

/// The value of attribute `name` in one tag's attribute list.
fn attribute<'a>(tag: &'a str, name: &str) -> Option<&'a str> {
    let key = format!("{name}=\"");
    let (at, _) = tag
        .match_indices(&key)
        .find(|(at, _)| *at == 0 || tag[..*at].ends_with(' '))?;
    let value = &tag[at + key.len()..];
    value.find('"').map(|end| &value[..end])
}
