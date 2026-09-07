//! Destructive only to the explicitly supplied disposable test container; never a production URL.
use clap::Parser;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};
#[derive(Parser)]
struct Args {
    baseline: PathBuf,
    candidate: PathBuf,
    #[arg(long)]
    container: String,
    output: PathBuf,
}
fn worker(
    binary: &Path,
    prefix: &str,
    phase: &str,
    manifest: &Path,
    url: &str,
    log: &Path,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new(binary)
        .args([prefix, phase, manifest.to_str().ok_or("manifest path")?])
        .env("EVENTLOG_TEST_POSTGRES_URL", url)
        .output()?;
    fs::write(log.with_extension("stdout"), &output.stdout)?;
    fs::write(log.with_extension("stderr"), &output.stderr)?;
    if !output.status.success() {
        return Err(format!("recovery worker failed {:?}", output.status.code()).into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    fs::create_dir(&args.output)?;
    let mut url = std::env::var("EVENTLOG_TEST_POSTGRES_URL")?;
    let mut receipts = Vec::new();
    for (adapter, binary) in [("baseline", &args.baseline), ("candidate", &args.candidate)] {
        let generation: String = time::OffsetDateTime::now_utc()
            .unix_timestamp()
            .to_string()
            .chars()
            .map(|digit| {
                char::from(b'a' + u8::try_from(digit.to_digit(10).expect("digit")).expect("digit"))
            })
            .collect();
        let prefix = format!(
            "recovery_{generation}_{}",
            if adapter == "baseline" { "b" } else { "c" }
        );
        let manifest = args.output.join(format!("{adapter}-events.json"));
        let before = worker(
            binary,
            &prefix,
            "prepare",
            &manifest,
            &url,
            &args.output.join(format!("{adapter}-prepare")),
        )?;
        let started = Instant::now();
        let restarted = Command::new("docker")
            .args(["restart", "--time", "1", &args.container])
            .output()?;
        fs::write(
            args.output.join(format!("{adapter}-restart.stdout")),
            &restarted.stdout,
        )?;
        fs::write(
            args.output.join(format!("{adapter}-restart.stderr")),
            &restarted.stderr,
        )?;
        if !restarted.status.success() {
            return Err("disposable restart refused".into());
        }
        let inspected = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{(index (index .NetworkSettings.Ports \"5432/tcp\") 0).HostPort}}",
                &args.container,
            ])
            .output()?;
        if !inspected.status.success() {
            return Err("test port discovery failed".into());
        }
        let port = String::from_utf8(inspected.stdout)?.trim().parse::<u16>()?;
        url = format!("postgresql://postgres@127.0.0.1:{port}/postgres");
        loop {
            if let Ok(Ok((client, connection))) = tokio::time::timeout(
                Duration::from_secs(1),
                tokio_postgres::connect(&url, tokio_postgres::NoTls),
            )
            .await
            {
                let driver = tokio::spawn(connection);
                drop(client);
                driver.await??;
                break;
            }
            if started.elapsed() > Duration::from_secs(15) {
                return Err("restart exceeded recovery objective".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let after = worker(
            binary,
            &prefix,
            "recover",
            &manifest,
            &url,
            &args.output.join(format!("{adapter}-recover")),
        )?;
        let elapsed = started.elapsed().as_millis();
        let valid = elapsed <= 15_000 && after["correctness_violations"] == 0;
        let receipt = serde_json::json!({"adapter":adapter,"before":before,"after":after,"restart_to_replayed_ms":elapsed,"observed_local_port":port,"valid":valid,"failure_mode":"disposable server restart between worker processes; retained exact events, retry receipt and partial projection replay"});
        println!("{receipt}");
        receipts.push(receipt);
        if !valid {
            return Err("recovery receipt refused".into());
        }
    }
    fs::write(
        args.output.join("recovery.json"),
        serde_json::to_vec_pretty(&receipts)?,
    )?;
    Ok(())
}
