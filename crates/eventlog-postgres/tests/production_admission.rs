#[path = "../examples/production-proof/admission.rs"]
mod admission;
use admission::{Execution, RequiredCase, Target, missing_cases};
const SHARED: &str = "ordered_groups_commit_and_rollback_as_one_unit";
const REQUIRED: [RequiredCase; 2] = [
    RequiredCase {
        package: "eventlog-sqlite",
        kind: "test",
        target: "atomic_groups",
        name: SHARED,
    },
    RequiredCase {
        package: "eventlog-postgres",
        kind: "test",
        target: "atomic_groups",
        name: SHARED,
    },
];
fn passed(package: &str, target: &str) -> Execution {
    Execution {
        target: Target {
            package: package.into(),
            kind: "test".into(),
            name: target.into(),
        },
        success: true,
        stdout: format!(
            "\nrunning 1 test\ntest {SHARED} ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n"
        ),
        stderr: String::new(),
    }
}
#[test]
fn a_passing_sqlite_peer_cannot_mask_the_missing_postgres_target() {
    assert_eq!(
        missing_cases(&[passed("eventlog-sqlite", "atomic_groups")], &REQUIRED),
        [format!("eventlog-postgres/test/atomic_groups/{SHARED}")]
    );
}
#[path = "../examples/production-proof/required.rs"]
mod required;
use admission::{assess, select_artifacts};
use std::fmt::Write as _;
#[test]
fn a_passing_postgres_peer_cannot_mask_the_missing_sqlite_target() {
    assert_eq!(
        missing_cases(&[passed("eventlog-postgres", "atomic_groups")], &REQUIRED),
        [format!("eventlog-sqlite/test/atomic_groups/{SHARED}")]
    );
}
#[test]
fn success_in_an_unrelated_target_or_kind_cannot_fill_the_roster() {
    for (kind, target) in [("test", "conformance"), ("lib", "atomic_groups")] {
        let mut wrong = passed("eventlog-postgres", target);
        wrong.target.kind = kind.into();
        assert_eq!(
            missing_cases(
                &[passed("eventlog-sqlite", "atomic_groups"), wrong],
                &REQUIRED
            ),
            [format!("eventlog-postgres/test/atomic_groups/{SHARED}")]
        );
    }
}
#[test]
fn selected_zero_ignored_and_failed_results_refuse_required_cases() {
    for (status, selected, summary) in [
        ("", 0, "ok. 0 passed; 0 failed; 0 ignored"),
        ("ignored", 1, "ok. 0 passed; 0 failed; 1 ignored"),
        ("FAILED", 1, "FAILED. 0 passed; 1 failed; 0 ignored"),
    ] {
        let mut execution = passed("eventlog-postgres", "atomic_groups");
        execution.stdout = format!(
            "running {selected} tests\n{}\ntest result: {summary}; 0 measured; 0 filtered out; finished in 0.00s\n",
            if status.is_empty() {
                String::new()
            } else {
                format!("test {SHARED} ... {status}\n")
            }
        );
        let assessment = assess(&[execution], &REQUIRED[1..]);
        assert!(!assessment.valid, "{status}");
        assert_eq!(
            assessment.missing,
            [format!("eventlog-postgres/test/atomic_groups/{SHARED}")]
        );
    }
}
#[test]
fn process_and_explicit_skip_failures_refuse_otherwise_successful_results() {
    // Both streams belong to this invocation; neither is associated by global ordering.
    for failure in 0..3 {
        let mut execution = passed("eventlog-postgres", "atomic_groups");
        match failure {
            0 => execution.success = false,
            1 => execution.stderr = "skipped: fixture absent".into(),
            _ => execution.stdout = execution.stdout.replace(
                "\ntest result:",
                "\nsuccesses:\n\n---- case stdout ----\nskipped: fixture absent\n\ntest result:",
            ),
        }
        assert!(!assess(&[execution], &REQUIRED[1..]).valid);
    }
}
#[test]
fn malformed_truncated_duplicate_and_inconsistent_output_refuses() {
    let complete = passed("eventlog-postgres", "atomic_groups");
    for stdout in [
        format!("test {SHARED} ... ok\n"),
        complete.stdout.replace("1 passed", "2 passed"),
        complete.stdout.replace("finished in 0.00s", "finished in "),
        complete
            .stdout
            .replace("finished in 0.00s", "finished in NaNs"),
        complete.stdout.replace("0 filtered out", "1 filtered out"),
        complete.stdout.replace(" ... ok", " ... unknown"),
        complete
            .stdout
            .replace("test result: ok.", "test result: FAILED."),
        complete.stdout.replace(
            "\ntest result:",
            &format!("\ntest {SHARED} ... ok\ntest result:"),
        ),
        complete.stdout.replace("; finished in 0.00s", ""),
        format!("{}trailing garbage\n", complete.stdout),
        format!("{}{}", complete.stdout, complete.stdout),
    ] {
        let mut execution = complete.clone();
        execution.stdout = stdout;
        let result = assess(&[execution], &REQUIRED[1..]);
        assert!(!result.valid);
        assert_eq!(result.missing.len(), 1);
    }
}
#[test]
fn duplicate_execution_and_duplicate_roster_are_ambiguous() {
    let execution = passed("eventlog-postgres", "atomic_groups");
    let result = assess(&[execution.clone(), execution.clone()], &REQUIRED[1..]);
    assert!(!result.valid);
    assert_eq!(result.missing.len(), 1);
    assert!(!assess(&[execution], &[REQUIRED[1], REQUIRED[1]]).valid);
}
#[test]
fn complete_roster_passes_with_exact_provider_and_target_attribution() {
    let roster = required::required_cases();
    let mut grouped = std::collections::BTreeMap::<(String, String, String), Vec<&str>>::new();
    for case in &roster {
        grouped
            .entry((case.package.into(), case.kind.into(), case.target.into()))
            .or_default()
            .push(case.name);
    }
    let executions: Vec<_> = grouped.into_iter().map(|((package, kind, name), names)| Execution {
        target: Target { package, kind, name }, success: true, stderr: String::new(),
        stdout: format!("running {} tests\n{}\ntest result: ok. {} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n",
            names.len(), names.iter().fold(String::new(), |mut output, name| { writeln!(output, "test {name} ... ok").unwrap(); output }), names.len()),
    }).collect();
    let result = assess(&executions, &roster);
    assert!(result.valid, "{result:?}");
    assert_eq!(result.passed, roster.len());
    assert!(roster.iter().any(|case| case.package == "eventlog-sqlite"
        && case.name == "scope_reservations_are_atomic_and_confined_in_memory_and_file"));
    // Every original shared SQL case now appears under both owning targets.
    for name in [
        SHARED,
        "concurrent_groups_preserve_order_without_partial_commits",
        "snapshot_history_and_repository_privacy_interleavings",
        "committed_commands_survive_snapshot_storage_failure",
        "inline_failure_preserves_all_atomic_state_and_callback_authority",
        "rebuild_preserves_other_tenants_and_previous_view_on_failure",
    ] {
        for package in ["eventlog-sqlite", "eventlog-postgres"] {
            assert_eq!(
                roster
                    .iter()
                    .filter(|case| case.package == package && case.name == name)
                    .count(),
                1
            );
        }
    }
}
fn package_metadata(id: &str, name: &str, manifest_path: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "name": name,
        "version": "0.2.1",
        "authors": [],
        "description": null,
        "homepage": null,
        "repository": null,
        "license": null,
        "license_file": null,
        "readme": null,
        "rust_version": null,
        "manifest_path": manifest_path,
    })
}
fn metadata() -> String {
    serde_json::json!({"packages":[package_metadata("opaque-postgres-id", "eventlog-postgres", "/workspace/crates/eventlog-postgres/Cargo.toml"), package_metadata("opaque-sqlite-id", "eventlog-sqlite", "/workspace/crates/eventlog-sqlite/Cargo.toml")],
        "workspace_members":["opaque-postgres-id", "opaque-sqlite-id"]}).to_string()
}
fn artifact(package: &str, kind: &str, target: &str, executable: &str) -> String {
    serde_json::json!({"reason":"compiler-artifact", "package_id":package,
        "target":{"kind":[kind],"name":target}, "profile":{"test":true}, "executable":executable})
    .to_string()
}
#[test]
fn exact_cargo_artifact_identity_does_not_depend_on_executable_path_substrings() {
    let artifacts = format!(
        "{}\n{}\n{{\"reason\":\"build-finished\",\"success\":true}}\n",
        artifact(
            "opaque-postgres-id",
            "test",
            "atomic_groups",
            "/opaque/sqlite-looks-like-a-peer"
        ),
        artifact(
            "opaque-sqlite-id",
            "test",
            "atomic_groups",
            "/opaque/postgres-looks-like-a-peer"
        )
    );
    let selected = select_artifacts(&metadata(), &artifacts).unwrap();
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].target.package, "eventlog-postgres");
    assert_eq!(selected[0].target.kind, "test");
    assert_eq!(selected[0].target.name, "atomic_groups");
    assert_eq!(
        selected[0].executable.to_str(),
        Some("/opaque/sqlite-looks-like-a-peer")
    );
    assert_eq!(
        selected[0].working_directory.to_str(),
        Some("/workspace/crates/eventlog-postgres")
    );
    assert_eq!(selected[0].package_environment.len(), 16);
    for name in [
        "CARGO_PKG_AUTHORS",
        "CARGO_PKG_DESCRIPTION",
        "CARGO_PKG_HOMEPAGE",
        "CARGO_PKG_LICENSE",
        "CARGO_PKG_LICENSE_FILE",
        "CARGO_PKG_README",
        "CARGO_PKG_REPOSITORY",
        "CARGO_PKG_RUST_VERSION",
        "CARGO_PKG_VERSION_PRE",
    ] {
        assert_eq!(
            selected[0]
                .package_environment
                .get(name)
                .and_then(|value| value.to_str()),
            Some(""),
            "{name}"
        );
    }
}

#[test]
fn complete_cargo_package_environment_comes_from_exact_metadata() {
    let package = serde_json::json!({
        "id": "opaque-peer-id",
        "name": "peer-package",
        "version": "12.34.56-alpha.7+build.9",
        "authors": ["Ada Example <ada@example.invalid>", "Grace Example"],
        "description": "peer description",
        "homepage": "https://example.invalid/home",
        "repository": "https://example.invalid/repository",
        "license": null,
        "license_file": "LICENSE.custom",
        "readme": "README.custom",
        "rust_version": "1.91",
        "manifest_path": "/workspace/peer/Cargo.toml",
    });
    let metadata = serde_json::json!({
        "packages": [package],
        "workspace_members": ["opaque-peer-id"],
    })
    .to_string();
    let artifacts = format!(
        "{}\n{{\"reason\":\"build-finished\",\"success\":true}}\n",
        artifact("opaque-peer-id", "test", "environment", "/opaque/peer")
    );
    let selected = select_artifacts(&metadata, &artifacts).unwrap();
    let mut command = std::process::Command::new("/opaque/peer");
    selected[0].apply_execution_context(&mut command);
    assert_eq!(
        command.get_current_dir(),
        Some(std::path::Path::new("/workspace/peer"))
    );
    let environment = &selected[0].package_environment;
    assert_eq!(environment.len(), 16);
    for (name, value) in [
        ("CARGO_MANIFEST_DIR", "/workspace/peer"),
        ("CARGO_MANIFEST_PATH", "/workspace/peer/Cargo.toml"),
        (
            "CARGO_PKG_AUTHORS",
            "Ada Example <ada@example.invalid>:Grace Example",
        ),
        ("CARGO_PKG_DESCRIPTION", "peer description"),
        ("CARGO_PKG_HOMEPAGE", "https://example.invalid/home"),
        ("CARGO_PKG_LICENSE", ""),
        ("CARGO_PKG_LICENSE_FILE", "LICENSE.custom"),
        ("CARGO_PKG_NAME", "peer-package"),
        ("CARGO_PKG_README", "README.custom"),
        ("CARGO_PKG_REPOSITORY", "https://example.invalid/repository"),
        ("CARGO_PKG_RUST_VERSION", "1.91"),
        ("CARGO_PKG_VERSION", "12.34.56-alpha.7+build.9"),
        ("CARGO_PKG_VERSION_MAJOR", "12"),
        ("CARGO_PKG_VERSION_MINOR", "34"),
        ("CARGO_PKG_VERSION_PATCH", "56"),
        ("CARGO_PKG_VERSION_PRE", "alpha.7"),
    ] {
        assert_eq!(
            environment.get(name).and_then(|value| value.to_str()),
            Some(value),
            "{name}"
        );
    }
}
#[test]
fn absent_truncated_failed_unknown_and_duplicate_cargo_artifacts_refuse() {
    let one = artifact(
        "opaque-postgres-id",
        "test",
        "atomic_groups",
        "/opaque/executable",
    );
    let finished = "{\"reason\":\"build-finished\",\"success\":true}";
    for artifacts in [
        String::new(),
        one.clone(),
        finished.into(),
        format!("{one}\n{{\"reason\":\"build-finished\",\"success\":false}}"),
        format!("{one}\n{one}\n{finished}"),
        format!(
            "{}\n{finished}",
            one.replace("opaque-postgres-id", "unknown-id")
        ),
        format!("{one}\n{finished}\n{one}"),
        format!("{one}\n{{truncated"),
        format!(
            "{}\n{finished}",
            one.replace("[\"test\"]", "[\"test\",\"lib\"]")
        ),
        format!(
            "{one}\n{}\n{finished}",
            artifact(
                "opaque-sqlite-id",
                "test",
                "atomic_groups",
                "/opaque/executable"
            )
        ),
    ] {
        assert!(
            select_artifacts(&metadata(), &artifacts).is_err(),
            "{artifacts}"
        );
    }
}
#[test]
fn workspace_doc_and_empty_nonrequired_targets_retain_coverage() {
    let empty = "running 0 tests\n\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n";
    let mut doc = passed("workspace", "workspace");
    doc.target.kind = "doc".into();
    doc.stdout = format!("{empty}{}", doc.stdout);
    let mut library = passed("nonrequired", "library");
    library.target.kind = "lib".into();
    library.stdout = empty.into();
    let result = assess(
        &[passed("eventlog-postgres", "atomic_groups"), doc, library],
        &REQUIRED[1..],
    );
    assert!(result.valid, "{result:?}");
    assert_eq!(result.summaries.len(), 4);
}

#[test]
fn an_exact_selected_target_still_requires_the_fully_qualified_case_name() {
    for wrong_name in [
        "other_case",
        "nested::ordered_groups_commit_and_rollback_as_one_unit",
        "ordered_groups_commit_and_rollback_as_one_unit_suffix",
    ] {
        let mut execution = passed("eventlog-postgres", "atomic_groups");
        execution.stdout = execution.stdout.replace(SHARED, wrong_name);
        let result = assess(&[execution], &REQUIRED[1..]);
        assert!(!result.valid);
        assert_eq!(result.missing.len(), 1);
    }
}

#[test]
fn missing_or_malformed_package_execution_context_refuses_artifact_selection() {
    let artifacts = format!(
        "{}\n{{\"reason\":\"build-finished\",\"success\":true}}\n",
        artifact(
            "opaque-postgres-id",
            "test",
            "atomic_groups",
            "/opaque/executable"
        )
    );
    let mut missing_manifest = package_metadata(
        "opaque-postgres-id",
        "eventlog-postgres",
        "/workspace/Cargo.toml",
    );
    missing_manifest
        .as_object_mut()
        .unwrap()
        .remove("manifest_path");
    let mut malformed_version = package_metadata(
        "opaque-postgres-id",
        "eventlog-postgres",
        "/workspace/Cargo.toml",
    );
    malformed_version["version"] = "1.2".into();
    for package in [
        missing_manifest,
        package_metadata("opaque-postgres-id", "eventlog-postgres", "Cargo.toml"),
        package_metadata(
            "opaque-postgres-id",
            "eventlog-postgres",
            "crates/eventlog-postgres/Cargo.toml",
        ),
        package_metadata(
            "opaque-postgres-id",
            "eventlog-postgres",
            "/workspace/crates/eventlog-postgres/Other.toml",
        ),
        malformed_version,
    ] {
        let metadata = serde_json::json!({
            "packages": [package],
            "workspace_members": ["opaque-postgres-id"],
        })
        .to_string();
        assert!(
            select_artifacts(&metadata, &artifacts).is_err(),
            "{metadata}"
        );
    }
    let duplicate = serde_json::json!({
        "packages": [
            package_metadata("opaque-postgres-id", "eventlog-postgres", "/workspace/first/Cargo.toml"),
            package_metadata("opaque-postgres-id", "eventlog-postgres", "/workspace/second/Cargo.toml"),
        ],
        "workspace_members": ["opaque-postgres-id"],
    })
    .to_string();
    assert!(select_artifacts(&duplicate, &artifacts).is_err());
}
