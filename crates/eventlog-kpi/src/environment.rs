//! Where a baseline was recorded: enough to read a timing with, and no host name.
use std::{path::Path, process::Command};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Environment {
    pub cpu: String,
    pub cpus: usize,
    pub kernel: String,
    /// `/proc/loadavg`'s 1, 5 and 15 minute averages when the baseline was written.
    pub load_average: String,
    pub rustc: String,
    pub profile: String,
    /// Whether the PostgreSQL scenarios ran (a disposable local server).
    pub postgres: bool,
}

impl Environment {
    pub fn observe(postgres: bool) -> Self {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let cpu = cpuinfo
            .lines()
            .find_map(|line| line.strip_prefix("model name"))
            .and_then(|rest| rest.split_once(':'))
            .map_or_else(|| "unknown".to_owned(), |(_, name)| name.trim().to_owned());
        let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
        Self {
            cpu,
            cpus: std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
            kernel: std::fs::read_to_string("/proc/sys/kernel/osrelease")
                .map_or_else(|_| "unknown".to_owned(), |text| text.trim().to_owned()),
            load_average: load
                .split_whitespace()
                .take(3)
                .collect::<Vec<_>>()
                .join(" "),
            rustc: output("rustc", &["-V"]).unwrap_or_else(|| "unknown".to_owned()),
            profile: if cfg!(debug_assertions) {
                "debug".to_owned()
            } else {
                "release".to_owned()
            },
            postgres,
        }
    }
}

fn output(program: &str, arguments: &[&str]) -> Option<String> {
    let output = Command::new(program).args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// The checked-out commit, marked when the tree has changes outside `kpi/`.
pub fn revision(root: &Path) -> String {
    let root = root.to_string_lossy();
    let head =
        output("git", &["-C", &root, "rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    let changed = output(
        "git",
        &["-C", &root, "status", "--porcelain", "--", ".", ":!kpi"],
    )
    .is_some_and(|status| !status.is_empty());
    if changed {
        format!("{head}+local-changes")
    } else {
        head
    }
}

pub fn now() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown".to_owned())
}
