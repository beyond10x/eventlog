//! Exact Cargo execution attribution and conservative libtest admission.
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    path::PathBuf,
    process::Command,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Target {
    pub package: String,
    pub kind: String,
    pub name: String,
}
#[derive(Clone, Copy, Debug)]
pub struct RequiredCase {
    pub package: &'static str,
    pub kind: &'static str,
    pub target: &'static str,
    pub name: &'static str,
}
impl RequiredCase {
    fn belongs_to(self, target: &Target) -> bool {
        self.package == target.package && self.kind == target.kind && self.target == target.name
    }
    fn diagnostic(self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.package, self.kind, self.target, self.name
        )
    }
}
#[derive(Clone, Debug)]
pub struct Execution {
    pub target: Target,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}
#[derive(Debug)]
pub struct SelectedTarget {
    pub target: Target,
    pub executable: PathBuf,
    pub working_directory: PathBuf,
    pub package_environment: BTreeMap<&'static str, OsString>,
}
impl SelectedTarget {
    pub fn apply_execution_context(&self, command: &mut Command) {
        command.current_dir(&self.working_directory);
        for (name, _) in std::env::vars_os() {
            if name == "CARGO_MANIFEST_DIR"
                || name == "CARGO_MANIFEST_PATH"
                || name
                    .to_str()
                    .is_some_and(|name| name.starts_with("CARGO_PKG_"))
            {
                command.env_remove(name);
            }
        }
        command.envs(&self.package_environment);
    }
}
#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_members: BTreeSet<String>,
}
#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    version: String,
    authors: Vec<String>,
    description: Option<String>,
    homepage: Option<String>,
    repository: Option<String>,
    license: Option<String>,
    license_file: Option<PathBuf>,
    readme: Option<PathBuf>,
    rust_version: Option<String>,
    manifest_path: PathBuf,
}
#[derive(Clone)]
struct PackageContext {
    name: String,
    working_directory: PathBuf,
    environment: BTreeMap<&'static str, OsString>,
}
#[derive(Deserialize)]
struct Artifact {
    package_id: String,
    target: ArtifactTarget,
    profile: Profile,
    executable: Option<PathBuf>,
}
#[derive(Deserialize)]
struct ArtifactTarget {
    kind: Vec<String>,
    name: String,
}
#[derive(Deserialize)]
struct Profile {
    test: bool,
}

fn package_context(package: Package) -> Result<(String, PackageContext), String> {
    if !package.manifest_path.is_absolute()
        || package.manifest_path.file_name() != Some(OsStr::new("Cargo.toml"))
    {
        return Err("package manifest path must be an absolute Cargo.toml path".into());
    }
    let working_directory = package
        .manifest_path
        .parent()
        .ok_or("package manifest has no working directory")?
        .to_path_buf();
    let version_without_build = package
        .version
        .split_once('+')
        .map_or(package.version.as_str(), |(version, _)| version);
    let (release, prerelease) = version_without_build
        .split_once('-')
        .map_or((version_without_build, ""), |(release, prerelease)| {
            (release, prerelease)
        });
    let mut release = release.split('.');
    let (Some(major), Some(minor), Some(patch), None) = (
        release.next(),
        release.next(),
        release.next(),
        release.next(),
    ) else {
        return Err("package version must contain major, minor, and patch components".into());
    };
    if [major, minor, patch]
        .iter()
        .any(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err("package version components must be numeric".into());
    }

    let mut environment = BTreeMap::new();
    environment.insert(
        "CARGO_MANIFEST_DIR",
        working_directory.clone().into_os_string(),
    );
    environment.insert(
        "CARGO_MANIFEST_PATH",
        package.manifest_path.into_os_string(),
    );
    environment.insert("CARGO_PKG_AUTHORS", package.authors.join(":").into());
    environment.insert(
        "CARGO_PKG_DESCRIPTION",
        package.description.unwrap_or_default().into(),
    );
    environment.insert(
        "CARGO_PKG_HOMEPAGE",
        package.homepage.unwrap_or_default().into(),
    );
    environment.insert(
        "CARGO_PKG_LICENSE",
        package.license.unwrap_or_default().into(),
    );
    environment.insert(
        "CARGO_PKG_LICENSE_FILE",
        package.license_file.unwrap_or_default().into_os_string(),
    );
    environment.insert("CARGO_PKG_NAME", package.name.clone().into());
    environment.insert(
        "CARGO_PKG_README",
        package.readme.unwrap_or_default().into_os_string(),
    );
    environment.insert(
        "CARGO_PKG_REPOSITORY",
        package.repository.unwrap_or_default().into(),
    );
    environment.insert(
        "CARGO_PKG_RUST_VERSION",
        package.rust_version.unwrap_or_default().into(),
    );
    environment.insert("CARGO_PKG_VERSION", package.version.clone().into());
    environment.insert("CARGO_PKG_VERSION_MAJOR", major.into());
    environment.insert("CARGO_PKG_VERSION_MINOR", minor.into());
    environment.insert("CARGO_PKG_VERSION_PATCH", patch.into());
    environment.insert("CARGO_PKG_VERSION_PRE", prerelease.into());
    Ok((
        package.id,
        PackageContext {
            name: package.name,
            working_directory,
            environment,
        },
    ))
}

/// Identity comes only from the exact workspace Cargo selected and its test artifacts.
pub fn select_artifacts(metadata: &str, artifacts: &str) -> Result<Vec<SelectedTarget>, String> {
    let metadata: Metadata = serde_json::from_str(metadata).map_err(|error| error.to_string())?;
    let mut packages = BTreeMap::new();
    for package in metadata
        .packages
        .into_iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
    {
        let (id, context) = package_context(package)?;
        if packages.insert(id, context).is_some() {
            return Err("duplicate package metadata identity".into());
        }
    }
    let mut selected = BTreeMap::new();
    let mut executables = BTreeSet::new();
    let mut finished = false;
    for line in artifacts.lines() {
        if finished {
            return Err("artifact stream continues after build-finished".into());
        }
        let value: serde_json::Value =
            serde_json::from_str(line).map_err(|error| error.to_string())?;
        match value["reason"].as_str() {
            Some("compiler-artifact") => {
                let artifact: Artifact =
                    serde_json::from_value(value).map_err(|error| error.to_string())?;
                if !artifact.profile.test {
                    continue;
                }
                let Some(executable) = artifact.executable else {
                    continue;
                };
                let context = packages
                    .get(&artifact.package_id)
                    .ok_or("test artifact outside selected workspace")?;
                let [kind] = artifact.target.kind.as_slice() else {
                    return Err("ambiguous target kind".into());
                };
                let target = Target {
                    package: context.name.clone(),
                    kind: kind.clone(),
                    name: artifact.target.name,
                };
                if !executables.insert(executable.clone()) || selected.contains_key(&target) {
                    return Err("duplicate or ambiguous test artifact".into());
                }
                selected.insert(
                    target.clone(),
                    SelectedTarget {
                        target,
                        executable,
                        working_directory: context.working_directory.clone(),
                        package_environment: context.environment.clone(),
                    },
                );
            }
            Some("build-finished") if value["success"].as_bool() == Some(true) => finished = true,
            Some("build-finished") => return Err("workspace test build failed".into()),
            Some("compiler-message" | "build-script-executed") => {}
            _ => return Err("unrecognized Cargo artifact record".into()),
        }
    }
    if !finished || selected.is_empty() {
        return Err("missing completed workspace test artifacts".into());
    }
    Ok(selected.into_values().collect())
}

#[derive(Default, Debug)]
struct Suite {
    selected: usize,
    cases: BTreeMap<String, String>,
    passed: usize,
    failed: usize,
    ignored: usize,
    summary: String,
}
fn summary_count(part: &str, suffix: &str) -> Result<usize, String> {
    part.trim()
        .strip_suffix(suffix)
        .ok_or("malformed runner count")?
        .trim()
        .parse()
        .map_err(|_| "invalid runner count".into())
}
fn finish_suite(mut suite: Suite, line: &str) -> Result<Suite, String> {
    let body = line
        .strip_prefix("test result: ok. ")
        .or_else(|| line.strip_prefix("test result: FAILED. "))
        .ok_or("unknown runner result")?;
    let parts: Vec<_> = body.split(';').collect();
    if parts.len() != 6 || !parts[5].trim().starts_with("finished in ") {
        return Err("truncated runner summary".into());
    }
    let duration = parts[5]
        .trim()
        .strip_prefix("finished in ")
        .and_then(|value| value.strip_suffix('s'))
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0);
    if duration.is_none() {
        return Err("invalid or truncated runner duration".into());
    }
    suite.passed = summary_count(parts[0], "passed")?;
    suite.failed = summary_count(parts[1], "failed")?;
    suite.ignored = summary_count(parts[2], "ignored")?;
    let measured = summary_count(parts[3], "measured")?;
    let filtered = summary_count(parts[4], "filtered out")?;
    if filtered != 0
        || measured != 0
        || suite.selected != suite.cases.len()
        || suite.selected != suite.passed + suite.failed + suite.ignored
        || suite.passed
            != suite
                .cases
                .values()
                .filter(|value| value.as_str() == "ok")
                .count()
        || suite.failed
            != suite
                .cases
                .values()
                .filter(|value| value.as_str() == "FAILED")
                .count()
        || suite.ignored
            != suite
                .cases
                .values()
                .filter(|value| value.starts_with("ignored"))
                .count()
        || (suite.failed == 0) != line.starts_with("test result: ok.")
    {
        return Err("inconsistent runner result or narrowed selection".into());
    }
    line.clone_into(&mut suite.summary);
    Ok(suite)
}
fn parse_suites(stdout: &str) -> Result<Vec<Suite>, String> {
    let mut suites = Vec::new();
    let mut current: Option<Suite> = None;
    let mut captured = false;
    for line in stdout.lines().filter(|line| !line.is_empty()) {
        if let Some(count) = line.strip_prefix("running ") {
            if current.is_some() {
                return Err("nested or truncated runner output".into());
            }
            let count = count
                .strip_suffix(" tests")
                .or_else(|| count.strip_suffix(" test"))
                .ok_or("malformed selected count")?;
            current = Some(Suite {
                selected: count.parse().map_err(|_| "invalid selected count")?,
                ..Suite::default()
            });
            captured = false;
        } else if line.starts_with("test result:") {
            suites.push(finish_suite(
                current.take().ok_or("summary without selected execution")?,
                line,
            )?);
        } else if line == "successes:" || line == "failures:" {
            if current.is_none() {
                return Err("captured output without selected execution".into());
            }
            captured = true;
        } else if !captured {
            let suite = current
                .as_mut()
                .ok_or("output outside selected execution")?;
            let (name, result) = line
                .strip_prefix("test ")
                .and_then(|body| body.rsplit_once(" ... "))
                .ok_or("malformed case result")?;
            if name.is_empty()
                || !(matches!(result, "ok" | "FAILED") || result.starts_with("ignored"))
                || suite
                    .cases
                    .insert(name.to_owned(), result.to_owned())
                    .is_some()
            {
                return Err("duplicate or ambiguous case result".into());
            }
        } else if current.is_none() {
            return Err("trailing runner output".into());
        }
    }
    if current.is_some() || suites.is_empty() {
        return Err("missing or truncated runner summary".into());
    }
    Ok(suites)
}
#[derive(Debug)]
pub struct Assessment {
    pub passed: usize,
    pub failed: usize,
    pub ignored: usize,
    pub summaries: Vec<String>,
    pub missing: Vec<String>,
    pub valid: bool,
}
/// Admit only complete passing cases in one unambiguous execution of their exact target.
pub fn assess(executions: &[Execution], required: &[RequiredCase]) -> Assessment {
    let mut result = Assessment {
        passed: 0,
        failed: 0,
        ignored: 0,
        summaries: Vec::new(),
        missing: Vec::new(),
        valid: !executions.is_empty(),
    };
    let mut seen = BTreeSet::new();
    let mut passing = Vec::new();
    for execution in executions {
        let unique = seen.insert(&execution.target);
        let parsed = parse_suites(&execution.stdout);
        let Ok(suites) = parsed else {
            result.valid = false;
            continue;
        };
        let complete = unique
            && execution.success
            && !execution.stdout.contains("skipped:")
            && !execution.stderr.contains("skipped:")
            && (execution.target.kind == "doc" || suites.len() == 1)
            && suites
                .iter()
                .all(|suite| suite.failed == 0 && suite.ignored == 0);
        result.valid &= complete;
        for suite in suites {
            result.passed += suite.passed;
            result.failed += suite.failed;
            result.ignored += suite.ignored;
            result.summaries.push(suite.summary);
            if complete {
                passing.push((&execution.target, suite.cases));
            }
        }
    }
    let mut roster = BTreeSet::new();
    for case in required {
        let diagnostic = case.diagnostic();
        result.valid &= roster.insert(diagnostic.clone());
        let attributed = executions
            .iter()
            .filter(|execution| case.belongs_to(&execution.target))
            .count()
            == 1;
        if !attributed
            || !passing.iter().any(|(target, cases)| {
                case.belongs_to(target) && cases.get(case.name).is_some_and(|status| status == "ok")
            })
        {
            result.missing.push(diagnostic);
        }
    }
    result.valid &= result.passed > 0 && result.missing.is_empty();
    result
}
#[cfg(test)]
pub fn missing_cases(executions: &[Execution], required: &[RequiredCase]) -> Vec<String> {
    assess(executions, required).missing
}
