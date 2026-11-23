//! The gate's lanes as the Cargo arguments each one runs, in a module of its own so that
//! `tests/production_admission.rs` reads the same lists the gate runs.

/// The lane the gate was asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    /// Tests under nextest, formatter, linter.
    Default,
    /// The required real-backend runner in place of the nextest step, then formatter and linter.
    ProductionProof,
    /// Every KPI counter against `kpi/baseline.json`.
    Kpi,
    /// The wall-clock budgets written for a release build, in a release build.
    Slow,
}

/// The integration-test targets that assert a wall-clock budget for a release build, as
/// `(package, target)`. Each is declared `test = false` in its manifest, so no debug test step
/// selects it (`cargo nextest run --workspace` and `cargo test --workspace` both skip it), and
/// only [`Lane::Slow`] names it.
pub const RELEASE_TIMING: [(&str, &str); 1] = [("eventlog-postgres", "batch_timing")];

/// The Cargo invocations of `lane`, in the order the gate runs them.
pub fn steps(lane: Lane) -> Vec<Vec<&'static str>> {
    match lane {
        Lane::Kpi => vec![vec![
            "run",
            "--release",
            "--locked",
            "-p",
            "eventlog-kpi",
            "--features",
            "counters",
            "--",
            "check",
        ]],
        // Plain `cargo test`, so the lane needs no nextest where it runs (the KPI CI job); the
        // case bounds itself through the store's own deadline and its asserted budgets.
        Lane::Slow => RELEASE_TIMING
            .iter()
            .map(|(package, target)| {
                vec![
                    "test",
                    "--release",
                    "--locked",
                    "-p",
                    package,
                    "--test",
                    target,
                    "--",
                    "--nocapture",
                ]
            })
            .collect(),
        Lane::Default | Lane::ProductionProof => {
            let tests = if lane == Lane::ProductionProof {
                vec![
                    "run",
                    "--locked",
                    "-p",
                    "eventlog-postgres",
                    "--example",
                    "production-proof",
                ]
            } else {
                // `.config/nextest.toml` ends a test at 60 s and the run at 120 s (the folder's
                // test budget); the step then names its slowest tests.
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
                // A `test = false` target is outside `--all-targets`; lint the slow lane's too.
                vec![
                    "clippy",
                    "-p",
                    RELEASE_TIMING[0].0,
                    "--test",
                    RELEASE_TIMING[0].1,
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
        }
    }
}
