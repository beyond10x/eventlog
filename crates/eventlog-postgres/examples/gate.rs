//! Repository gate, with shell retained only as a compatibility entry point.
use clap::Parser;
use std::process::{Command, ExitCode};
use std::time::SystemTime;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    production_proof: bool,
    /// Run only the KPI lane: every counter against `kpi/baseline.json` (AGENTS.md § The KPI gate).
    #[arg(long, conflicts_with = "production_proof")]
    kpi: bool,
}
fn main() -> ExitCode {
    let args = Args::parse();
    let steps = if args.kpi {
        vec![vec![
            "run",
            "--release",
            "--locked",
            "-p",
            "eventlog-kpi",
            "--features",
            "counters",
            "--",
            "check",
        ]]
    } else {
        let tests = if args.production_proof {
            vec![
                "run",
                "--locked",
                "-p",
                "eventlog-postgres",
                "--example",
                "production-proof",
            ]
        } else {
            // `.config/nextest.toml` ends a test at 60 s and the run at 120 s (the folder's test
            // budget); the step then names its slowest tests.
            vec!["nextest", "run", "--workspace", "--locked"]
        };
        vec![
            tests,
            vec!["fmt", "--all", "--check"],
            vec![
                "clippy",
                "--workspace",
                "--all-targets",
                "--locked",
                "--",
                "-D",
                "warnings",
            ],
            // The providers' KPI counters compile only with this feature; lint them too.
            vec![
                "clippy",
                "-p",
                "eventlog-kpi",
                "--all-targets",
                "--locked",
                "--features",
                "counters",
                "--",
                "-D",
                "warnings",
            ],
        ]
    };
    for arguments in steps {
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
