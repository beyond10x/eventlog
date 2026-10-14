//! Regenerate every committed ESS output under `ess/*-generated/` and refuse any difference.
//!
//! Each output directory is an `ess` managed output (`.ess-output/state.json`). A schema
//! projection is compared by `ess generate --check`. A generated Rust library has no `--check`,
//! so it is regenerated into a byte copy of the committed directory and the two trees are
//! compared file by file: a hand edit makes `ess` refuse to regenerate, and a model change makes
//! the bytes differ. A `*-generated` directory this table does not name is a refusal too, so a
//! new output cannot go unchecked.
//!
//! Run from the repository root with the `ess` release CI pins on PATH:
//! `cargo run --locked -p eventlog-postgres --example ess-drift`.
use clap::Parser;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

#[derive(Parser)]
struct Args {
    /// The `ess` executable.
    #[arg(long, default_value = "ess")]
    ess: PathBuf,
    /// The repository root.
    #[arg(long, default_value = ".")]
    repository: PathBuf,
}

/// One generated projection of one specification into one committed output directory.
enum Projection {
    /// `ess generate --kind schema`.
    Schema,
    /// `ess generate types --target rust --all-types` under this package name.
    RustTypes { package: &'static str },
}

/// Every committed output: the directory under `ess/`, its specification and its projections.
const OUTPUTS: [(&str, &str, &[Projection]); 2] = [
    ("pool-generated", "pool", &[Projection::Schema]),
    (
        "capture-generated",
        "capture",
        &[
            Projection::Schema,
            Projection::RustTypes {
                package: "eventlog-capture-types",
            },
        ],
    ),
];

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => {
            println!("ess-drift: every generated output is current");
            ExitCode::SUCCESS
        }
        Err(problems) => {
            for problem in problems {
                eprintln!("ess-drift: {problem}");
            }
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<(), Vec<String>> {
    let ess = args.repository.join("ess");
    let mut problems = Vec::new();
    for specification in specifications(&ess).map_err(|error| vec![error])? {
        if let Err(problem) = ess_cli(
            args,
            &[
                "specify".as_ref(),
                "validate".as_ref(),
                "--path".as_ref(),
                specification.as_os_str(),
            ],
        ) {
            problems.push(problem);
        }
    }
    let committed = generated_directories(&ess).map_err(|error| vec![error])?;
    for name in &committed {
        if !OUTPUTS.iter().any(|(known, _, _)| known == name) {
            problems.push(format!(
                "ess/{name} is not drift-checked: name it in OUTPUTS"
            ));
        }
    }
    for (output, specification, projections) in OUTPUTS {
        if !committed.iter().any(|name| name == output) {
            problems.push(format!("ess/{output} is missing"));
            continue;
        }
        let specification = ess.join(specification);
        let output = ess.join(output);
        for projection in projections {
            if let Err(problem) = check(args, &specification, &output, projection) {
                problems.push(problem);
            }
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}

/// Every specification directory under `ess/`: one holding a `system.yaml`.
fn specifications(ess: &Path) -> Result<Vec<PathBuf>, String> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(ess).map_err(|error| format!("{}: {error}", ess.display()))? {
        let path = entry
            .map_err(|error| format!("{}: {error}", ess.display()))?
            .path();
        if path.join("system.yaml").is_file() {
            found.push(path);
        }
    }
    found.sort();
    Ok(found)
}

fn generated_directories(ess: &Path) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(ess).map_err(|error| format!("{}: {error}", ess.display()))? {
        let entry = entry.map_err(|error| format!("{}: {error}", ess.display()))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with("-generated") && entry.path().is_dir() {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

fn check(
    args: &Args,
    specification: &Path,
    output: &Path,
    projection: &Projection,
) -> Result<(), String> {
    match projection {
        Projection::Schema => ess_cli(
            args,
            &[
                "generate".as_ref(),
                "--path".as_ref(),
                specification.as_os_str(),
                "--kind".as_ref(),
                "schema".as_ref(),
                "--out".as_ref(),
                output.as_os_str(),
                "--check".as_ref(),
            ],
        ),
        Projection::RustTypes { package } => {
            let copy =
                tempfile::tempdir().map_err(|error| format!("temporary directory: {error}"))?;
            let regenerated = copy.path().join("output");
            copy_tree(output, &regenerated)?;
            ess_cli(
                args,
                &[
                    "generate".as_ref(),
                    "types".as_ref(),
                    "--path".as_ref(),
                    specification.as_os_str(),
                    "--all-types".as_ref(),
                    "--target".as_ref(),
                    "rust".as_ref(),
                    "--package".as_ref(),
                    package.as_ref(),
                    "--out".as_ref(),
                    regenerated.as_os_str(),
                ],
            )
            .map_err(|problem| format!("{}: {problem}", output.display()))?;
            let before = files(output)?;
            let after = files(&regenerated)?;
            let mut drifted: Vec<String> = before
                .keys()
                .chain(after.keys())
                .filter(|path| before.get(*path) != after.get(*path))
                .map(|path| path.display().to_string())
                .collect();
            drifted.sort();
            drifted.dedup();
            if drifted.is_empty() {
                println!("ess-drift: {} Rust types current", output.display());
                Ok(())
            } else {
                Err(format!(
                    "{} differs from regenerated Rust types: {}",
                    output.display(),
                    drifted.join(", ")
                ))
            }
        }
    }
}

fn ess_cli(args: &Args, arguments: &[&std::ffi::OsStr]) -> Result<(), String> {
    let status = Command::new(&args.ess)
        .args(arguments)
        .status()
        .map_err(|error| format!("cannot start {}: {error}", args.ess.display()))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "`ess {}` refused with exit {:?}",
            arguments
                .iter()
                .map(|argument| argument.to_string_lossy())
                .collect::<Vec<_>>()
                .join(" "),
            status.code()
        ))
    }
}

fn copy_tree(from: &Path, to: &Path) -> Result<(), String> {
    std::fs::create_dir_all(to).map_err(|error| format!("{}: {error}", to.display()))?;
    for entry in std::fs::read_dir(from).map_err(|error| format!("{}: {error}", from.display()))? {
        let entry = entry.map_err(|error| format!("{}: {error}", from.display()))?;
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)
                .map_err(|error| format!("{}: {error}", entry.path().display()))?;
        }
    }
    Ok(())
}

/// Every file under `root` by its relative path, with its bytes.
fn files(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>, String> {
    fn walk(
        root: &Path,
        directory: &Path,
        found: &mut BTreeMap<PathBuf, Vec<u8>>,
    ) -> Result<(), String> {
        for entry in std::fs::read_dir(directory)
            .map_err(|error| format!("{}: {error}", directory.display()))?
        {
            let path = entry
                .map_err(|error| format!("{}: {error}", directory.display()))?
                .path();
            if path.is_dir() {
                walk(root, &path, found)?;
            } else {
                let bytes =
                    std::fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?;
                found.insert(path.strip_prefix(root).unwrap().to_path_buf(), bytes);
            }
        }
        Ok(())
    }
    let mut found = BTreeMap::new();
    walk(root, root, &mut found)?;
    Ok(found)
}
