//! Observer smoke and standalone collection entry point; requires the explicit disposable cgroup.
#[path = "observation/metrics_collector.rs"]
mod metrics_collector;
use clap::Parser;
use std::{path::PathBuf, time::Duration};
#[derive(Parser)]
struct Args {
    cgroup: PathBuf,
    output: PathBuf,
    stop: PathBuf,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let summary = metrics_collector::collect(metrics_collector::MetricsConfig {
        postgres: std::env::var("EVENTLOG_TEST_POSTGRES_URL")?.parse()?,
        cgroup: args.cgroup,
        output: args.output,
        stop_file: args.stop,
        maximum_duration: Duration::from_secs(2),
        interval: Duration::from_millis(20),
        query_timeout: Duration::from_secs(1),
    })
    .await?;
    println!("{summary}");
    if summary["postgres_failures"] != 0
        || summary["cgroup_failures"] != 0
        || summary["statements_before"]["status"] != "observed"
        || summary["statements_after"]["status"] != "observed"
    {
        return Err("observer prerequisite unavailable".into());
    }
    Ok(())
}
