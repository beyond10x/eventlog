//! Required real-backend runner. Missing prerequisites and selected-zero lanes are refusals.
#[path = "production-proof/admission.rs"]
mod admission;
#[path = "production-proof/required.rs"]
mod required;
use clap::Parser;
use serde_json::json;
#[derive(Parser)]
struct Args {}
use sha2::{Digest, Sha256};
use std::{
    fmt::Write as _,
    process::{Command, ExitCode},
    time::Instant,
};

fn main() -> ExitCode {
    let _ = Args::parse();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("production proof refused: {error}");
            ExitCode::FAILURE
        }
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    for required in [
        "EVENTLOG_TEST_POSTGRES_URL",
        "EVENTLOG_TEST_POSTGRES_CA",
        "EVENTLOG_TEST_HOSTED_POSTGRES_URL",
    ] {
        if std::env::var(required)
            .ok()
            .is_none_or(|value| value.trim().is_empty())
        {
            return Err(format!("required fixture {required} is absent").into());
        }
    }
    // Nested Cargo builds may replace the example executable; retain this run's bytes first.
    let binary = std::fs::read(std::env::current_exe()?)?;
    let started = time::OffsetDateTime::now_utc();
    let elapsed = Instant::now();
    let metadata = Command::new("cargo")
        .args(["metadata", "--no-deps", "--locked", "--format-version", "1"])
        .output()?;
    if !metadata.status.success() {
        return Err("Cargo workspace metadata failed".into());
    }
    let build = Command::new("cargo")
        .args([
            "test",
            "--workspace",
            "--locked",
            "--no-run",
            "--message-format=json",
        ])
        .output()?;
    let mut raw = format!(
        "cargo metadata --no-deps --locked --format-version 1\n{}\n{}\ncargo test --workspace --locked --no-run --message-format=json\n{}\n{}\n",
        String::from_utf8(metadata.stdout.clone())?,
        String::from_utf8(metadata.stderr)?,
        String::from_utf8(build.stdout.clone())?,
        String::from_utf8(build.stderr.clone())?
    );
    preserve_raw(&raw)?;
    eprint!("{}", String::from_utf8(build.stderr)?);
    if !build.status.success() {
        return Err("Cargo workspace test build failed".into());
    }
    let targets = admission::select_artifacts(
        &String::from_utf8(metadata.stdout)?,
        &String::from_utf8(build.stdout)?,
    )?;
    let mut executions = Vec::new();
    for selected in targets {
        let command = format!(
            "{} {} {}: {} --show-output --test-threads=1 --format=pretty",
            selected.target.package,
            selected.target.kind,
            selected.target.name,
            selected.executable.display()
        );
        let mut process = Command::new(&selected.executable);
        selected.apply_execution_context(&mut process);
        let output = process
            .args(["--show-output", "--test-threads=1", "--format=pretty"])
            .env("EVENTLOG_REQUIRE_POSTGRES", "1")
            .env("RUST_TEST_THREADS", "1")
            .output()?;
        executions.push(record_execution(
            selected.target,
            &format!("cwd {}: {command}", selected.working_directory.display()),
            output,
            &mut raw,
        )?);
    }
    // Cargo's workspace documentation selection is a separate authority; no test artifact represents it.
    let docs = Command::new("cargo")
        .args([
            "test",
            "--workspace",
            "--locked",
            "--doc",
            "--",
            "--show-output",
            "--test-threads=1",
            "--format=pretty",
        ])
        .env("EVENTLOG_REQUIRE_POSTGRES", "1")
        .env("RUST_TEST_THREADS", "1")
        .output()?;
    executions.push(record_execution(
        admission::Target {
            package: "workspace".into(),
            kind: "doc".into(),
            name: "workspace".into(),
        },
        "cargo test --workspace --locked --doc -- --show-output --test-threads=1 --format=pretty",
        docs,
        &mut raw,
    )?);
    let assessment = admission::assess(&executions, &required::required_cases());
    let admission::Assessment {
        passed,
        failed,
        ignored,
        summaries,
        missing,
        valid,
    } = assessment;
    let revision = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()?;
    let server = tokio::runtime::Runtime::new()?.block_on(async {
        let configuration: tokio_postgres::Config =
            std::env::var("EVENTLOG_TEST_POSTGRES_URL")?.parse()?;
        let (client, connection) = configuration.connect(tokio_postgres::NoTls).await?;
        let driver = tokio::spawn(connection);
        let row = client
            .query_one(
                "SELECT current_setting('server_version'), current_setting('server_version_num')",
                &[],
            )
            .await?;
        let server = json!({"version":row.get::<_,String>(0),"version_num":row.get::<_,String>(1)});
        drop(client);
        driver.await??;
        Ok::<_, Box<dyn std::error::Error>>(server)
    })?;
    let report = json!({"format":"eventlog-production-proof/1","backend_server":server,"schema_setup":"test-owned exact prefixes and hosted_owner schema; additive checksum admission exercised","default_pool":{"connections":4,"waiters":32,"acquisition_ms":2000,"transaction_ms":10000},"source_revision":String::from_utf8(revision.stdout)?.trim(),"source_dirty":!dirty.stdout.is_empty(),"binary_sha256":format!("{:x}",Sha256::digest(binary)),"started_at":started.to_string(),"finished_at":time::OffsetDateTime::now_utc().to_string(),"duration_ms":elapsed.elapsed().as_millis(),"passed":passed,"failed":failed,"skipped":ignored,"missing_required_cases":missing,"runner_summaries":summaries,"conformance_valid":valid,"capacity_admitted":false,"capacity_requirement":"comparative laboratory artifact is separately required; this runner does not manufacture capacity evidence","owner_fixture_handoff":["SDK current authority and exact realm/service bindings","SDK original and generated stream/feed/cursor/view/effect vectors"]});
    if let Ok(path) = std::env::var("EVENTLOG_PROOF_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    println!("{report}");
    if !valid {
        return Err("required backend suite did not execute completely and pass".into());
    }
    Ok(())
}

fn preserve_raw(raw: &str) -> Result<(), std::io::Error> {
    if let Ok(path) = std::env::var("EVENTLOG_PROOF_RAW") {
        std::fs::write(path, raw)?;
    }
    Ok(())
}
fn record_execution(
    target: admission::Target,
    command: &str,
    output: std::process::Output,
    raw: &mut String,
) -> Result<admission::Execution, Box<dyn std::error::Error>> {
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    write!(
        raw,
        "\nexecution: {command}\nexit: {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}\n",
        output.status.code()
    )?;
    preserve_raw(raw)?;
    println!("execution: {command}; exit: {:?}", output.status.code());
    print!("{stdout}");
    eprint!("{stderr}");
    Ok(admission::Execution {
        target,
        success: output.status.success(),
        stdout,
        stderr,
    })
}
