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
    assert_eq!(
        std::env::var("CARGO_MANIFEST_PATH").as_deref(),
        Ok(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")),
        "the direct runner inherited another package's CARGO_MANIFEST_PATH"
    );
    assert_eq!(
        std::env::var("CARGO_PKG_NAME").as_deref(),
        Ok(env!("CARGO_PKG_NAME")),
        "the direct runner inherited another package's CARGO_PKG_NAME"
    );
}
