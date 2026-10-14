//! README names the release a reader should pin, and every name it gives that release is the
//! workspace version.
//!
//! Adversary pass 1 on story:file-eventlog-indexes-streams found README moved to 0.8.0 while
//! still saying that `website/docs/releases.md` summarizes those release notes; that page ended
//! at 0.3.0. The decided state names only the version, the tag, the GitHub release notes and
//! `CHANGELOG.md`. The class behind it is a README version left behind by a release, so this case
//! ties every version README states to `[workspace.package] version` in `Cargo.toml`: a release
//! that bumps the workspace and forgets README turns it red.
//!
//! story:docs-name-the-current-release found the same class in the public guide: `intro.md` said
//! the current release was 0.3.0 and `quickstart.md` pinned tag 0.3.0 while the workspace was at
//! 0.8.0, and `releases.md` stopped at 0.3.0. The second case below holds every guide page to the
//! workspace version and the highlights page to `CHANGELOG.md`.

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

/// What `page` puts between each `before` and the next `after`; `before` must occur at least once.
fn every_named<'a>(page: &'a str, path: &str, before: &str, after: char) -> Vec<&'a str> {
    let found: Vec<_> = page
        .match_indices(before)
        .map(|(at, _)| {
            let rest = &page[at + before.len()..];
            &rest[..rest.find(after).unwrap()]
        })
        .collect();
    assert!(!found.is_empty(), "{path} names {before:?}");
    found
}

/// `0.10.0` sorts after `0.9.0`.
fn ordinal(version: &str) -> Vec<u64> {
    version
        .split('.')
        .map(|part| part.parse().unwrap())
        .collect()
}

/// `2026-09-22` as the highlights page writes it, `September 22, 2026`.
fn long_date(iso: &str) -> String {
    const MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    let [year, month, day]: [&str; 3] = iso.split('-').collect::<Vec<_>>().try_into().unwrap();
    let month = MONTHS[month.parse::<usize>().unwrap() - 1];
    let day: u32 = day.parse().unwrap();
    format!("{month} {day}, {year}")
}

/// Every `## <version> — <date>` heading in `text`, in order.
fn release_headings(text: &str) -> Vec<(&str, &str)> {
    text.lines()
        .filter_map(|line| line.strip_prefix("## ")?.split_once(" — "))
        .filter(|(version, _)| releases(version) == [*version])
        .collect()
}

#[test]
fn every_guide_page_names_the_workspace_version_and_the_highlights_cover_every_release() {
    let version = workspace_version(&repository("Cargo.toml"));

    // The pages that say which release is current or which tag to pin name the workspace version
    // there.
    let intro = repository("website/docs/intro.md");
    for (path, page, before, after) in [
        ("intro.md", &intro, "The current release is [", ']'),
        (
            "quickstart.md",
            &repository("website/docs/quickstart.md"),
            "git clone --branch ",
            ' ',
        ),
        (
            "quickstart.md",
            &repository("website/docs/quickstart.md"),
            "tag = \"",
            '"',
        ),
        (
            "quickstart.md",
            &repository("website/docs/quickstart.md"),
            "https://github.com/beyond10x/eventlog/blob/",
            '/',
        ),
    ] {
        for found in every_named(page, path, before, after) {
            assert_eq!(
                found, version,
                "website/docs/{path} names {found} after {before:?}, not the workspace version"
            );
        }
    }

    // Any other version a guide page names is a reader's current release left behind: history
    // belongs on the highlights page and in CHANGELOG.md, not here.
    for path in [
        "intro.md",
        "quickstart.md",
        "guarantees.md",
        "operations.md",
        "providers.md",
    ] {
        let page = repository(&format!("website/docs/{path}"));
        for found in releases(&page) {
            assert_eq!(found, version, "website/docs/{path} names release {found}");
        }
    }

    // The highlights page opens with the workspace version, newest first, and has a section per
    // CHANGELOG.md release from 0.4.0 on, under the date CHANGELOG.md gives it.
    let highlights = repository("website/docs/releases.md");
    let sections = release_headings(&highlights);
    assert_eq!(
        sections.first().map(|(found, _)| *found),
        Some(version.as_str()),
        "website/docs/releases.md opens with the workspace version"
    );
    assert!(
        sections
            .windows(2)
            .all(|pair| ordinal(pair[0].0) > ordinal(pair[1].0)),
        "website/docs/releases.md lists releases newest first, once each: {sections:?}"
    );
    let changelog = repository("CHANGELOG.md");
    for (release, iso) in release_headings(&changelog) {
        if ordinal(release) < ordinal("0.4.0") {
            continue;
        }
        let date = long_date(iso);
        assert!(
            sections.contains(&(release, date.as_str())),
            "website/docs/releases.md has no \"## {release} — {date}\" section"
        );
    }

    // A section's own links name its own release, so a section copied forward is not left
    // pointing at the release it was copied from.
    for (index, (release, _)) in sections.iter().enumerate() {
        let start = highlights.find(&format!("## {release} — ")).unwrap();
        let end = sections
            .get(index + 1)
            .map_or(highlights.len(), |(next, _)| {
                highlights.find(&format!("## {next} — ")).unwrap()
            });
        let section = &highlights[start..end];
        for before in [
            "https://github.com/beyond10x/eventlog/releases/tag/",
            "https://github.com/beyond10x/eventlog/blob/",
        ] {
            for (at, _) in section.match_indices(before) {
                let rest = &section[at + before.len()..];
                let linked = &rest[..rest.find([')', '/']).unwrap()];
                assert_eq!(
                    linked, *release,
                    "website/docs/releases.md's {release} section links {before}{linked}"
                );
            }
        }
        assert!(
            section.contains(&format!(
                "https://github.com/beyond10x/eventlog/releases/tag/{release})"
            )),
            "website/docs/releases.md's {release} section links its release notes"
        );
    }
}
