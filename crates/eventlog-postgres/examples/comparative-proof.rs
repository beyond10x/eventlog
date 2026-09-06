//! Required comparative/restart proof with an exact immutable adapter baseline.
use clap::Parser;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};
fn files(
    directory: &Path,
    root: &Path,
    found: &mut BTreeSet<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            return Err("proof source must not contain an unbound symlink".into());
        }
        if kind.is_dir() {
            if entry.file_name() != "target" && entry.file_name() != ".git" {
                files(&path, root, found)?;
            }
        } else if kind.is_file() {
            found.insert(path.strip_prefix(root)?.to_path_buf());
        }
    }
    Ok(())
}
fn source(
    root: &Path,
    snapshot: Option<&Path>,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let listing = Command::new("git")
        .current_dir(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .output()?;
    if !listing.status.success() {
        return Err("source enumeration failed".into());
    }
    let mut paths = BTreeSet::new();
    for bytes in listing
        .stdout
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
    {
        paths.insert(PathBuf::from(std::str::from_utf8(bytes)?));
    }
    files(&root.join("crates"), root, &mut paths)?;
    if root.join(".cargo").exists() {
        files(&root.join(".cargo"), root, &mut paths)?;
    }
    let mut manifest = BTreeMap::new();
    for relative in paths {
        let path = root.join(&relative);
        if fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err("unbound source symlink".into());
        }
        let bytes = fs::read(&path)?;
        if let Some(snapshot) = snapshot {
            let target = snapshot.join(&relative);
            fs::create_dir_all(target.parent().ok_or("snapshot parent")?)?;
            fs::write(target, &bytes)?;
        }
        manifest.insert(
            relative
                .to_str()
                .ok_or("source path is not UTF-8")?
                .to_owned(),
            format!("{:x}", Sha256::digest(bytes)),
        );
    }
    Ok(manifest)
}
const BASE: &str = "20e00c1eeab5d67bd5f749bbbd871b1fbfe7f796";
#[derive(Parser)]
struct Args {
    #[arg(long)]
    baseline: PathBuf,
    #[arg(long)]
    container: String,
    #[arg(long)]
    output: PathBuf,
}
fn text(command: &mut Command) -> Result<String, Box<dyn std::error::Error>> {
    let result = command.output()?;
    if !result.status.success() {
        return Err(format!(
            "proof prerequisite command failed: {:?}",
            result.status.code()
        )
        .into());
    }
    Ok(String::from_utf8(result.stdout)?.trim().to_owned())
}
fn logged(command: &mut Command, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let output = command.output()?;
    fs::write(path.with_extension("stdout"), &output.stdout)?;
    fs::write(path.with_extension("stderr"), &output.stderr)?;
    if !output.status.success() {
        return Err(format!(
            "proof lane failed with {:?}; raw record {}",
            output.status.code(),
            path.display()
        )
        .into());
    }
    Ok(())
}
fn digest(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}
fn verify_original(baseline: &Path) -> Result<String, Box<dyn std::error::Error>> {
    if text(
        Command::new("git")
            .current_dir(baseline)
            .args(["rev-parse", "HEAD"]),
    )? != BASE
    {
        return Err("baseline is not the declared immutable adapter revision".into());
    }
    let paths = [
        "crates/eventlog-postgres/src",
        "crates/eventlog-core",
        "crates/eventlog-sqlite",
        "crates/eventlog-conformance",
        "Cargo.toml",
    ];
    let mut diff = Command::new("git");
    diff.current_dir(baseline)
        .args(["diff", "--exit-code", BASE, "--"])
        .args(paths);
    text(&mut diff)?;
    let runtime = [
        "crates/eventlog-postgres",
        "crates/eventlog-core",
        "crates/eventlog-sqlite",
        "crates/eventlog-conformance",
    ];
    let expected = text(
        Command::new("git")
            .current_dir(baseline)
            .args(["ls-tree", "-r", "--name-only", BASE, "--"])
            .args(runtime),
    )?
    .lines()
    .map(PathBuf::from)
    .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for directory in runtime {
        files(&baseline.join(directory), baseline, &mut actual)?;
    }
    for harness in ["capacity.rs", "recovery.rs"] {
        actual.remove(&PathBuf::from(format!(
            "crates/eventlog-postgres/examples/{harness}"
        )));
    }
    if actual != expected {
        return Err("untracked or missing original runtime input refuses baseline identity".into());
    }
    let extras = text(Command::new("git").current_dir(baseline).args([
        "ls-files",
        "--others",
        "--exclude-standard",
    ]))?;
    if extras.lines().any(|path| {
        !matches!(
            path,
            "crates/eventlog-postgres/examples/capacity.rs"
                | "crates/eventlog-postgres/examples/recovery.rs"
        )
    }) {
        return Err("unbound original input outside the admitted harness".into());
    }
    let mut tree = Command::new("git");
    tree.current_dir(baseline)
        .args(["ls-tree", "-r", BASE, "--"])
        .args(paths);
    text(&mut tree)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if std::env::var_os("CARGO_TARGET_DIR").is_some() {
        return Err("proof requires separate in-tree Cargo targets".into());
    }
    let started = Instant::now();
    let compiler = text(Command::new("rustc").arg("--version"))?;
    let candidate = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let baseline = args.baseline.canonicalize()?;
    if baseline == candidate {
        return Err("original and candidate require distinct build trees".into());
    }
    let original_tree = verify_original(&baseline)?;
    fs::create_dir(&args.output)?;
    let output = args.output.canonicalize()?;
    if output.starts_with(&candidate) || output.starts_with(&baseline) {
        return Err("proof output must be outside both source trees".into());
    }
    let candidate_source = source(&candidate, Some(&output.join("source/candidate")))?;
    fs::write(
        output.join("candidate-source-manifest.json"),
        serde_json::to_vec_pretty(&candidate_source)?,
    )?;
    fs::write(output.join("original-source-tree.txt"), &original_tree)?;
    fs::write(
        output.join("declared-profile.json"),
        include_str!("observation/laboratory-profile.json"),
    )?;
    let harness = Path::new("crates/eventlog-postgres/examples");
    fs::create_dir_all(baseline.join(harness))?;
    for name in ["capacity.rs", "recovery.rs"] {
        fs::copy(
            candidate.join(harness).join(name),
            baseline.join(harness).join(name),
        )?;
    }
    let manifest = baseline.join("crates/eventlog-postgres/Cargo.toml");
    let original_manifest = text(Command::new("git").current_dir(&baseline).args([
        "show",
        &format!("{BASE}:crates/eventlog-postgres/Cargo.toml"),
    ]))?;
    let modified = original_manifest.replace(
        "[dev-dependencies]",
        "[dev-dependencies]\nclap = { version = \"=4.6.6\", features = [\"derive\"] }",
    );
    fs::write(&manifest, format!("{modified}\n"))?;
    let mut baseline_build_source = source(&baseline, None)?;
    baseline_build_source.remove("Cargo.lock");
    // Only the indispensable harness dependency is resolved; original runtime source is checked again.
    logged(
        Command::new("cargo").current_dir(&baseline).args([
            "build",
            "--offline",
            "-p",
            "eventlog-postgres",
            "--example",
            "capacity",
            "--example",
            "recovery",
        ]),
        &output.join("baseline-build"),
    )?;
    let baseline_source = source(&baseline, Some(&output.join("source/baseline")))?;
    let mut baseline_after_build = baseline_source.clone();
    baseline_after_build.remove("Cargo.lock");
    if baseline_build_source != baseline_after_build {
        return Err("baseline source changed during harness build".into());
    }
    fs::write(
        output.join("baseline-source-manifest.json"),
        serde_json::to_vec_pretty(&baseline_source)?,
    )?;
    if verify_original(&baseline)? != original_tree {
        return Err("original backend source changed during harness build".into());
    }
    logged(
        Command::new("cargo").current_dir(&candidate).args([
            "build",
            "--locked",
            "-p",
            "eventlog-postgres",
            "--examples",
        ]),
        &output.join("candidate-build"),
    )?;
    let candidate_examples = candidate.join("target/debug/examples");
    if source(&candidate, None)? != candidate_source {
        return Err("candidate source changed during build".into());
    }
    let baseline_examples = baseline.join("target/debug/examples");
    let binaries = output.join("binaries");
    fs::create_dir(&binaries)?;
    for name in ["capacity", "recovery"] {
        fs::copy(
            baseline_examples.join(name),
            binaries.join(format!("baseline-{name}")),
        )?;
        fs::copy(
            candidate_examples.join(name),
            binaries.join(format!("candidate-{name}")),
        )?;
    }
    let revision = text(
        Command::new("git")
            .current_dir(&candidate)
            .args(["rev-parse", "HEAD"]),
    )?;
    let dirty = text(
        Command::new("git")
            .current_dir(&candidate)
            .args(["status", "--porcelain"]),
    )?;
    let delta = Command::new("git")
        .current_dir(&candidate)
        .args(["diff", "--binary"])
        .output()?;
    fs::write(output.join("candidate-tracked.diff"), delta.stdout)?;
    let baseline_delta = Command::new("git")
        .current_dir(&baseline)
        .args(["diff", "--binary"])
        .output()?;
    fs::write(
        output.join("baseline-harness-dependency.diff"),
        baseline_delta.stdout,
    )?;
    let identity = json!({"baseline_revision":BASE,"baseline_runtime_source_unchanged":true,"original_tree_sha256":format!("{:x}",Sha256::digest(original_tree.as_bytes())),"candidate_source_manifest_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&candidate_source)?)),"candidate_source_file_count":candidate_source.len(),"baseline_source_manifest_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&baseline_source)?)),"baseline_source_file_count":baseline_source.len(),"candidate_revision":revision,"candidate_dirty":!dirty.is_empty(),"compiler":compiler,"capacity_harness_sha256":digest(&candidate.join(harness).join("capacity.rs"))?,"recovery_harness_sha256":digest(&candidate.join(harness).join("recovery.rs"))?,"baseline_capacity_binary_sha256":digest(&baseline_examples.join("capacity"))?,"candidate_capacity_binary_sha256":digest(&candidate_examples.join("capacity"))?,"baseline_recovery_binary_sha256":digest(&baseline_examples.join("recovery"))?,"candidate_recovery_binary_sha256":digest(&candidate_examples.join("recovery"))?});
    fs::write(
        output.join("identity.json"),
        serde_json::to_vec_pretty(&identity)?,
    )?;
    let resources: serde_json::Value =
        serde_json::from_str(&text(Command::new("docker").args([
            "inspect",
            "--format",
            "{{json .HostConfig}}",
            &args.container,
        ]))?)?;
    if resources["NanoCpus"] != 2_000_000_000_u64
        || resources["Memory"] != 1_073_741_824_u64
        || resources["PidsLimit"] != 256
    {
        return Err("test server does not match predeclared CPU/memory/process limits".into());
    }
    fs::write(
        output.join("container-limits.json"),
        serde_json::to_vec_pretty(
            &json!({"nano_cpus":resources["NanoCpus"],"memory_bytes":resources["Memory"],"pids_limit":resources["PidsLimit"]}),
        )?,
    )?;
    let pid = text(Command::new("docker").args([
        "inspect",
        "--format",
        "{{.State.Pid}}",
        &args.container,
    ]))?
    .parse::<u32>()?;
    let groups = fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
    let group = groups
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or("cgroup v2 observation required")?;
    if group.split('/').any(|part| part == "..") {
        return Err("noncanonical cgroup path".into());
    }
    let cgroup = PathBuf::from("/sys/fs/cgroup").join(group.trim_start_matches('/'));
    let sweep = output.join("capacity");
    logged(
        Command::new(candidate_examples.join("capacity-sweep"))
            .arg(binaries.join("baseline-capacity"))
            .arg(binaries.join("candidate-capacity"))
            .arg(&sweep)
            .arg(&cgroup),
        &output.join("capacity-run"),
    )?;
    let recovery = output.join("restart");
    logged(
        Command::new(candidate_examples.join("recovery-sweep"))
            .arg(binaries.join("baseline-recovery"))
            .arg(binaries.join("candidate-recovery"))
            .arg("--container")
            .arg(&args.container)
            .arg(&recovery),
        &output.join("restart-run"),
    )?;
    let capacity: serde_json::Value =
        serde_json::from_slice(&fs::read(sweep.join("comparison.json"))?)?;
    let recovered: serde_json::Value =
        serde_json::from_slice(&fs::read(recovery.join("recovery.json"))?)?;
    if source(&candidate, None)? != candidate_source
        || source(&baseline, None)? != baseline_source
        || verify_original(&baseline)? != original_tree
        || text(Command::new("rustc").arg("--version"))? != compiler
    {
        return Err("source changed during comparative run".into());
    }
    let valid = capacity["steady_envelope_valid"] == true
        && capacity["configurations"]
            .as_array()
            .is_some_and(|values| values.len() == 12)
        && recovered.as_array().is_some_and(|values| {
            values.len() == 2 && values.iter().all(|value| value["valid"] == true)
        });
    let report = json!({"format":"eventlog-comparative-proof/1","identity":identity,"laboratory_valid":valid,"production_capacity_admitted":false,"configuration_count":12,"restart_count":2,"elapsed_ms":started.elapsed().as_millis(),"capacity":capacity,"recovery":recovered});
    fs::write(
        output.join("proof.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!(
        "{}",
        json!({"laboratory_valid":valid,"configuration_count":12,"restart_count":2,"elapsed_ms":started.elapsed().as_millis()})
    );
    if !valid {
        return Err("comparative/recovery admission refused".into());
    }
    Ok(())
}
