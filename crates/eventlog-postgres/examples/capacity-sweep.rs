//! Comparative laboratory supervisor. The identical worker source links each adapter separately.
#[path = "observation/metrics_collector.rs"]
mod metrics_collector;
use clap::Parser;
use serde_json::{Value, json};
use std::{fs, io, path::Path, process::Command, time::Duration};

fn worker(
    binary: &Path,
    prefix: &str,
    index: usize,
    lanes: usize,
    samples: usize,
    mode: &str,
    output: &Path,
) -> io::Result<Value> {
    let result = Command::new(binary)
        .args([
            prefix,
            &index.to_string(),
            &lanes.to_string(),
            &samples.to_string(),
            mode,
        ])
        .output()?;
    fs::write(output.with_extension("stdout"), &result.stdout)?;
    fs::write(output.with_extension("stderr"), &result.stderr)?;
    if !result.status.success() {
        return Err(io::Error::other(format!(
            "worker failed: {:?}",
            result.status.code()
        )));
    }
    serde_json::from_slice(&result.stdout).map_err(io::Error::other)
}
fn quantile(values: &[u64], percentile: usize) -> u64 {
    values[(values.len() - 1) * percentile / 100]
}

#[derive(Parser)]
struct Args {
    baseline: std::path::PathBuf,
    candidate: std::path::PathBuf,
    output: std::path::PathBuf,
    cgroup: std::path::PathBuf,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let profile: Value = serde_json::from_str(include_str!("observation/laboratory-profile.json"))?;
    let profile_name = profile["profile"]
        .as_str()
        .ok_or("missing laboratory profile name")?;
    let output = args.output.as_path();
    fs::create_dir(output)?;
    let cgroup = args.cgroup.as_path();
    let postgres =
        std::env::var("EVENTLOG_TEST_POSTGRES_URL")?.parse::<tokio_postgres::Config>()?;
    let mut configurations = Vec::new();
    let generation: String = time::OffsetDateTime::now_utc()
        .unix_timestamp()
        .to_string()
        .chars()
        .map(|value| {
            char::from(
                b'a' + u8::try_from(value.to_digit(10).expect("decimal timestamp")).expect("digit"),
            )
        })
        .collect();
    for (adapter, binary) in [
        ("baseline", args.baseline.as_path()),
        ("candidate", args.candidate.as_path()),
    ] {
        for aggregate in [1, 8, 32] {
            for mode in ["uniform", "hot"] {
                let label = format!("{adapter}-{aggregate}-{mode}");
                let prefix = format!(
                    "lab{generation}_{}_{}_{mode}",
                    if adapter == "baseline" { "b" } else { "c" },
                    match aggregate {
                        1 => "one",
                        8 => "eight",
                        _ => "many",
                    }
                );
                worker(
                    binary,
                    &prefix,
                    0,
                    1,
                    0,
                    mode,
                    &output.join(format!("{label}-init")),
                )?;
                let stop = output.join(format!("{label}.stop"));
                let observer = tokio::spawn(metrics_collector::collect(
                    metrics_collector::MetricsConfig {
                        postgres: postgres.clone(),
                        cgroup: cgroup.to_path_buf(),
                        output: output.join(format!("{label}-metrics.jsonl")),
                        stop_file: stop.clone(),
                        maximum_duration: Duration::from_secs(120),
                        interval: Duration::from_millis(20),
                        query_timeout: Duration::from_secs(2),
                    },
                ));
                tokio::time::sleep(Duration::from_millis(150)).await;
                let workers = std::thread::scope(|scope| {
                    let mut results = Vec::new();
                    if aggregate == 1 {
                        for index in 0..2 {
                            results.push(worker(
                                binary,
                                &prefix,
                                index,
                                1,
                                128,
                                mode,
                                &output.join(format!("{label}-worker{index}")),
                            ));
                        }
                    } else {
                        let handles: Vec<_> = (0..2)
                            .map(|index| {
                                let path = output.join(format!("{label}-worker{index}"));
                                let prefix = &prefix;
                                scope.spawn(move || {
                                    worker(binary, prefix, index, aggregate / 2, 128, mode, &path)
                                })
                            })
                            .collect();
                        for handle in handles {
                            results.push(
                                handle
                                    .join()
                                    .map_err(|_| io::Error::other("worker supervisor panicked"))
                                    .and_then(|value| value),
                            );
                        }
                    }
                    results.into_iter().collect::<Result<Vec<_>, _>>()
                });
                fs::write(stop, b"complete\n")?;
                let metrics = observer.await??;
                let workers = workers?;
                let mut latencies: Vec<_> = workers
                    .iter()
                    .flat_map(|value| value["latencies_us"].as_array().expect("worker latencies"))
                    .map(|value| value.as_u64().expect("latency"))
                    .collect();
                latencies.sort_unstable();
                let total = |field: &str| {
                    workers
                        .iter()
                        .map(|value| value[field].as_u64().expect("worker total"))
                        .sum::<u64>()
                };
                let maximum = |field: &str| {
                    workers
                        .iter()
                        .map(|value| value[field].as_u64().expect("worker maximum"))
                        .max()
                        .expect("workers")
                };
                let elapsed_us = if aggregate == 1 {
                    total("elapsed_us")
                } else {
                    let first = workers
                        .iter()
                        .map(|value| value["steady_started_unix_ns"].as_i64().expect("timestamp"))
                        .min()
                        .expect("workers");
                    let last = workers
                        .iter()
                        .map(|value| {
                            i128::from(value["steady_started_unix_ns"].as_i64().expect("timestamp"))
                                + i128::from(value["elapsed_us"].as_u64().expect("elapsed")) * 1000
                        })
                        .max()
                        .expect("workers");
                    u64::try_from((last - i128::from(first)) / 1000)?
                };
                let valid = total("correctness_violations") == 0
                    && total("refused") == 0
                    && total("conflicts") == 0
                    && total("deduplicated") == 0
                    && quantile(&latencies, 99) <= 2_000_000
                    && maximum("max_us") <= 2_000_000
                    && maximum("safe_feed_delay_us") <= 10_000_000
                    && maximum("projection_lag_us") <= 10_000_000
                    && metrics["postgres_failures"] == 0
                    && metrics["cgroup_failures"] == 0
                    && metrics["connections_max_excluding_observer"]
                        .as_u64()
                        .is_some_and(|value| value <= 8)
                    && metrics["statements_before"]["status"] == "observed"
                    && metrics["statements_after"]["status"] == "observed"
                    && metrics["statements_before"]["value"]["stats_reset"]
                        == metrics["statements_after"]["value"]["stats_reset"];
                let valid = valid
                    && metrics["cpu_usage_usec_delta"]["status"] == "observed"
                    && metrics["memory_current_sampled_max_bytes"]
                        .as_u64()
                        .is_some_and(|value| value <= 1_073_741_824)
                    && metrics["statements_before"]["value"]["deallocations"]
                        == metrics["statements_after"]["value"]["deallocations"]
                    && metrics["cgroup_first"]["memory_events"]["oom"]
                        == metrics["cgroup_last"]["memory_events"]["oom"]
                    && metrics["cgroup_first"]["memory_events"]["max"]
                        == metrics["cgroup_last"]["memory_events"]["max"];
                let receipt = json!({"adapter":adapter,"mode":mode,"aggregate_concurrency":aggregate,"workers":workers,"metrics":metrics,"succeeded":total("succeeded"),"elapsed_us":elapsed_us,"p50_us":quantile(&latencies,50),"p95_us":quantile(&latencies,95),"p99_us":quantile(&latencies,99),"valid_observed_envelope":valid,"limits":"End-to-end append/read bounds are conservative queue and transaction duration upper bounds. Server statement timings and sampled transaction ages are separately labeled; no exact internal queue histogram. Two processes run sequentially at aggregate concurrency one. Metrics include warmup and replay; worker latency excludes both."});
                println!(
                    "{}",
                    json!({"configuration":label,"valid_observed_envelope":valid,"samples":latencies.len(),"p99_us":quantile(&latencies,99)})
                );
                fs::write(
                    output.join(format!("{label}.json")),
                    serde_json::to_vec_pretty(&receipt)?,
                )?;
                configurations.push(receipt);
            }
        }
    }
    let valid = configurations
        .iter()
        .all(|value| value["valid_observed_envelope"] == true);
    fs::write(
        output.join("comparison.json"),
        serde_json::to_vec_pretty(
            &json!({"profile":profile_name,"configurations":configurations,"steady_envelope_valid":valid,"production_capacity_admitted":false,"recovery_requirement":"separate restart/replay receipt required"}),
        )?,
    )?;
    if !valid {
        return Err("one or more measured configurations violated the laboratory envelope".into());
    }
    Ok(())
}
