//! Cargo supplies package-specific runtime environment to each test target. The production proof
//! must preserve it when the PostgreSQL example invokes this peer package's artifact directly.

#[test]
fn production_runner_preserves_cargo_package_runtime_environment() {
    let expected_directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("canonical package root");
    let actual_directory = std::env::current_dir()
        .expect("test working directory")
        .canonicalize()
        .expect("canonical test working directory");
    assert_eq!(
        actual_directory, expected_directory,
        "the direct runner must first preserve Cargo's package working directory"
    );

    assert_eq!(
        std::env::var("CARGO_MANIFEST_DIR").as_deref(),
        Ok(env!("CARGO_MANIFEST_DIR")),
        "the direct runner inherited another package's CARGO_MANIFEST_DIR"
    );
    // Cargo and the production proof's runner set CARGO_MANIFEST_PATH; nextest, which runs the
    // gate's test step and marks every test process with `NEXTEST=1`, does not set it at all.
    let manifest_path = std::env::var("CARGO_MANIFEST_PATH");
    let under_nextest = std::env::var("NEXTEST").as_deref() == Ok("1");
    assert!(
        manifest_path.as_deref() == Ok(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
            || (under_nextest && manifest_path == Err(std::env::VarError::NotPresent)),
        "the direct runner inherited another package's CARGO_MANIFEST_PATH: {manifest_path:?}"
    );
    assert_eq!(
        std::env::var("CARGO_PKG_NAME").as_deref(),
        Ok(env!("CARGO_PKG_NAME")),
        "the direct runner inherited another package's CARGO_PKG_NAME"
    );
}
