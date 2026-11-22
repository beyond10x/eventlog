//! Timing A/B: build the base and the head revision on this machine, run their timings
//! interleaved (base, head, head, base, ...) for several rounds, and compare medians.
//!
//! Both sides run the runner of the current checkout, built against each revision's library
//! sources: the same scenarios measure both, and a base older than this runner works too. Each
//! revision is exported fresh with `git archive` under `<work>/ab-<pid>/` (file times set to now) and
//! built in a target directory of its own: two checkouts of one workspace map their crates to the
//! same artifacts, so a shared target directory could hand one side the other side's crates.
//! `<work>/ab-<pid>/` is removed when the comparison ends.
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{Command, ExitCode, Stdio},
};

use crate::{
    Failure, Outcome,
    compare::{Verdict, judge_ratio},
    definition::{Definitions, Kind},
    timing::{Summary, median},
};

fn git(root: &Path, arguments: &[&str]) -> Outcome<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(Failure(format!(
            "git {}: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

fn copy_tree(from: &Path, to: &Path) -> Outcome<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// Export `sha`, put this checkout's runner into it, build it, and return the binary's copy.
fn build(root: &Path, directory: &Path, sha: &str, label: &str) -> Outcome<PathBuf> {
    let short = &sha[..sha.len().min(12)];
    let source = directory.join(format!("src-{short}"));
    if source.exists() {
        std::fs::remove_dir_all(&source)?;
    }
    {
        std::fs::create_dir_all(&source)?;
        let mut archive = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["archive", "--format=tar", sha])
            .stdout(Stdio::piped())
            .spawn()?;
        let stream = archive
            .stdout
            .take()
            .ok_or_else(|| Failure("git archive has no output".into()))?;
        let unpacked = Command::new("tar")
            .arg("-x")
            .arg("-m")
            .arg("-C")
            .arg(&source)
            .stdin(stream)
            .status()?;
        let archived = archive.wait()?;
        if !archived.success() || !unpacked.success() {
            return Err(Failure(format!("cannot export {sha}")));
        }
    }
    let runner = source.join("crates/eventlog-kpi");
    if runner.exists() {
        std::fs::remove_dir_all(&runner)?;
    }
    copy_tree(&root.join("crates/eventlog-kpi"), &runner)?;
    // Timings need no provider hook, and an older provider has no `kpi-counters` feature.
    let manifest = runner.join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)?;
    let text: Vec<&str> = text
        .lines()
        .filter(|line| !line.contains("/kpi-counters"))
        .collect();
    std::fs::write(&manifest, text.join("\n") + "\n")?;
    let workspace = source.join("Cargo.toml");
    let text = std::fs::read_to_string(&workspace)?;
    if !text.contains("\"crates/eventlog-kpi\"") {
        std::fs::write(
            &workspace,
            text.replacen(
                "members = [",
                "members = [\n    \"crates/eventlog-kpi\",",
                1,
            ),
        )?;
    }
    println!("eventlog-kpi ab: building {label} {short}");
    let target = directory.join(format!("target-{label}"));
    let status = Command::new("cargo")
        .args(["build", "--release", "-p", "eventlog-kpi"])
        .env("CARGO_TARGET_DIR", &target)
        .current_dir(&source)
        .status()?;
    if !status.success() {
        return Err(Failure(format!("the {label} build of {short} failed")));
    }
    let bin = directory.join("bin");
    std::fs::create_dir_all(&bin)?;
    let copy = bin.join(format!("eventlog-kpi-{label}-{short}"));
    std::fs::copy(target.join("release/eventlog-kpi"), &copy)?;
    Ok(copy)
}

fn timings(binary: &Path, work: &Path) -> Outcome<BTreeMap<String, Summary>> {
    let output = Command::new(binary)
        .arg("--work")
        .arg(work)
        .args(["timings", "--json"])
        .stderr(Stdio::inherit())
        .output()?;
    if !output.status.success() {
        return Err(Failure(format!("{} timings failed", binary.display())));
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn spread(values: &[f64]) -> f64 {
    let middle = median(values);
    let low = values.iter().copied().fold(f64::INFINITY, f64::min);
    let high = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if middle > 0.0 {
        (high - low) / middle * 100.0
    } else {
        0.0
    }
}

pub fn run(
    root: &Path,
    work: &Path,
    base: Option<String>,
    head: &str,
    rounds: Option<usize>,
) -> Outcome<ExitCode> {
    let definitions = Definitions::load(&crate::definitions_path(root))?;
    let base = match base {
        Some(base) => base,
        None => git(root, &["merge-base", "origin/main", "HEAD"])?,
    };
    let base_sha = git(root, &["rev-parse", &format!("{base}^{{commit}}")])?;
    let head_sha = git(root, &["rev-parse", &format!("{head}^{{commit}}")])?;
    let directory = work.join(format!("ab-{}", std::process::id()));
    let base_binary = build(root, &directory, &base_sha, "base")?;
    let head_binary = build(root, &directory, &head_sha, "head")?;
    let rounds = rounds.unwrap_or(definitions.ab.rounds).max(1);
    let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!(
        "eventlog-kpi ab: base {} head {}, {rounds} interleaved rounds, load {}",
        &base_sha[..12],
        &head_sha[..12],
        load.split_whitespace()
            .take(3)
            .collect::<Vec<_>>()
            .join(" ")
    );
    let mut sides: [BTreeMap<String, Vec<f64>>; 2] = [BTreeMap::new(), BTreeMap::new()];
    for round in 0..rounds {
        let order = if round % 2 == 0 { [0, 1] } else { [1, 0] };
        for side in order {
            let binary = if side == 0 {
                &base_binary
            } else {
                &head_binary
            };
            for (id, summary) in timings(binary, work)? {
                sides[side].entry(id).or_default().push(summary.p50);
            }
        }
    }
    if let Err(error) = std::fs::remove_dir_all(&directory) {
        eprintln!(
            "eventlog-kpi ab: cannot remove {}: {error}",
            directory.display()
        );
    }
    println!(
        "{:<40} {:>12} {:>12} {:>7} {:>7} {:>9} {:>9}  verdict",
        "KPI", "base p50", "head p50", "ratio", "limit", "spread b%", "spread h%"
    );
    let mut failed = false;
    for kpi in definitions.of_kind(Kind::Timing) {
        let (Some(base_values), Some(head_values)) = (sides[0].get(&kpi.id), sides[1].get(&kpi.id))
        else {
            println!("{:<40} skipped: not measured on both sides", kpi.id);
            continue;
        };
        let limit = definitions.ab_ratio(kpi);
        let (ratio, verdict) = judge_ratio(
            kpi.direction,
            median(base_values),
            median(head_values),
            limit,
        );
        failed |= verdict.fails();
        println!(
            "{:<40} {:>12.1} {:>12.1} {:>7.3} {:>7.2} {:>9.1} {:>9.1}  {}",
            kpi.id,
            median(base_values),
            median(head_values),
            ratio,
            limit,
            spread(base_values),
            spread(head_values),
            match verdict {
                Verdict::Regressed => "REGRESSED",
                Verdict::Improved => "improved",
                _ => "ok",
            }
        );
    }
    Ok(if failed {
        println!("eventlog-kpi ab: a timing regressed beyond its limit");
        ExitCode::FAILURE
    } else {
        println!("eventlog-kpi ab: no timing regressed beyond its limit");
        ExitCode::SUCCESS
    })
}
