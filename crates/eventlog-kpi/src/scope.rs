//! Whether a change needs the KPI lane: a change of planning or prose only does not.
use std::{io::Write, path::Path, process::Command, process::ExitCode};

use crate::{Failure, Outcome};

/// The one path list: planning records and Markdown prose. Everything else can move a KPI.
pub fn is_prose(path: &str) -> bool {
    path.starts_with(".engineering/")
        || Path::new(path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("md"))
}

/// An empty change list runs the lane: nothing proven is not the same as nothing changed.
pub fn needs_kpi<'a>(paths: impl IntoIterator<Item = &'a str>) -> bool {
    let mut paths = paths.into_iter().peekable();
    paths.peek().is_none() || paths.any(|path| !is_prose(path))
}

pub fn run(root: &Path, base: &str, head: &str) -> Outcome<ExitCode> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["diff", "--name-only", base, head])
        .output()?;
    if !output.status.success() {
        return Err(Failure(format!(
            "git diff {base} {head}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let listing = String::from_utf8_lossy(&output.stdout);
    let run = needs_kpi(listing.lines().filter(|line| !line.is_empty()));
    println!("{}", if run { "run" } else { "skip" });
    if let Ok(path) = std::env::var("GITHUB_OUTPUT") {
        let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
        writeln!(file, "run={run}")?;
    }
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::needs_kpi;

    #[test]
    fn planning_and_prose_only_changes_skip_the_lane() {
        assert!(!needs_kpi([
            ".engineering/planning/stories/kpi.md",
            ".engineering/planning/index.yaml",
            "AGENTS.md",
            "docs/design/file-provider.md",
        ]));
    }

    #[test]
    fn any_code_manifest_or_kpi_file_runs_the_lane() {
        assert!(needs_kpi(["AGENTS.md", "crates/eventlog-file/src/lib.rs"]));
        assert!(needs_kpi(["kpi/baseline.json"]));
        assert!(needs_kpi(["Cargo.lock"]));
        assert!(
            needs_kpi(std::iter::empty::<&str>()),
            "an empty diff proves nothing"
        );
    }
}
