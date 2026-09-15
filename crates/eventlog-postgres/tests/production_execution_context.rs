//! Cargo runs package tests from the package root. The production proof must preserve that
//! execution context when it invokes Cargo-selected test artifacts directly.

#[test]
fn production_runner_preserves_cargo_package_working_directory() {
    let actual = std::env::current_dir()
        .expect("test working directory")
        .canonicalize()
        .expect("canonical test working directory");
    let expected = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("canonical package root");

    assert_eq!(
        actual, expected,
        "direct artifact execution changed Cargo's package working directory"
    );
}
