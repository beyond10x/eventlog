//! README names the release a reader should pin, and every name it gives that release is the
//! workspace version.
//!
//! Adversary pass 1 on story:file-eventlog-indexes-streams found README moved to 0.8.0 while
//! still saying that `website/docs/releases.md` summarizes those release notes; that page ended
//! at 0.3.0. The decided state names only the version, the tag, the GitHub release notes and
//! `CHANGELOG.md`. The class behind it is a README version left behind by a release, so this case
//! ties every version README states to `[workspace.package] version` in `Cargo.toml`: a release
//! that bumps the workspace and forgets README turns it red.

use std::{fs, path::Path};

fn repository(path: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path),
    )
    .unwrap()
}

/// `version` under `[workspace.package]`, read line by line: no TOML parser is a dependency here.
fn workspace_version(manifest: &str) -> String {
    let mut section = "";
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            section = line;
        } else if section == "[workspace.package]"
            && let Some((key, value)) = line.split_once('=')
            && key.trim() == "version"
        {
            return value.trim().trim_matches('"').to_owned();
        }
    }
    panic!("Cargo.toml has no [workspace.package] version");
}

/// What README puts between `before` and `after`, required to occur exactly once.
fn named<'a>(readme: &'a str, before: &str, after: char) -> &'a str {
    assert_eq!(
        readme.matches(before).count(),
        1,
        "README.md names {before:?} exactly once"
    );
    let start = readme.find(before).unwrap() + before.len();
    let rest = &readme[start..];
    &rest[..rest.find(after).unwrap()]
}

/// Every three-part dotted number in `text`, such as `0.8.0`; `1.91` and `17.6` are not one.
fn releases(text: &str) -> Vec<&str> {
    text.split(|c: char| !(c.is_ascii_digit() || c == '.'))
        .map(|token| token.trim_matches('.'))
        .filter(|token| {
            let parts: Vec<_> = token.split('.').collect();
            parts.len() == 3
                && parts
                    .iter()
                    .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
        })
        .collect()
}

#[test]
fn every_version_the_readme_names_is_the_workspace_version() {
    let readme = repository("README.md");
    let version = workspace_version(&repository("Cargo.toml"));

    for (what, before, after) in [
        ("the bold version", "Version **", '*'),
        ("the pinned tag", "Pin the bare Git tag `", '`'),
        (
            "the release-notes tag",
            "https://github.com/beyond10x/eventlog/releases/tag/",
            ')',
        ),
        ("the checkout tag", "From a checkout of tag `", '`'),
    ] {
        assert_eq!(
            named(&readme, before, after),
            version,
            "README.md {what} is the workspace version"
        );
    }
    // A version named anywhere else in README is held to the same rule, so a new mention is not
    // a place for the next release to forget.
    for found in releases(&readme) {
        assert_eq!(found, version, "README.md names release {found}");
    }
    // README no longer says a highlights page covers the release; if it says so again, the page
    // has to have that release.
    if readme.contains("website/docs/releases.md") {
        let highlights = repository("website/docs/releases.md");
        assert!(
            highlights
                .lines()
                .any(|line| line.starts_with(&format!("## {version} "))),
            "README.md links website/docs/releases.md, which has no {version} section"
        );
    }
}
