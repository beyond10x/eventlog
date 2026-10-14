//! The capture values are generated from `ess/capture/` into `ess/capture-generated/`, and nothing
//! in this workspace writes a second codec for them by hand.
//!
//! Design: `docs/design/durable-capture-continuity.md` § *Typed home*.

use std::path::{Path, PathBuf};

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The last segment of every type `ess/capture/domains/capture.yaml` declares.
fn capture_type_names() -> Vec<String> {
    let model =
        std::fs::read_to_string(workspace().join("ess/capture/domains/capture.yaml")).unwrap();
    let names: Vec<String> = model
        .lines()
        .filter_map(|line| line.trim().strip_prefix("- name: eventlog.capture."))
        .map(str::to_owned)
        .collect();
    assert!(
        names.len() >= 11,
        "the capture model declares at least the eleven durable-continuity types: {names:?}"
    );
    names
}

fn rust_sources(directory: &Path, found: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_sources(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

/// `struct Name` or `enum Name` declared on `line`, by any visibility.
fn declared(line: &str) -> Option<&str> {
    let mut words = line.split_whitespace();
    let mut word = words.next()?;
    if word.starts_with("pub") {
        word = words.next()?;
    }
    if word != "struct" && word != "enum" {
        return None;
    }
    let name = words.next()?;
    Some(
        name.split(|character: char| !(character.is_alphanumeric() || character == '_'))
            .next()
            .unwrap_or(name),
    )
}

/// Whether the attributes directly above line `index` derive a serde codec.
fn derives_serde(lines: &[&str], index: usize) -> bool {
    lines[..index]
        .iter()
        .rev()
        .map(|line| line.trim())
        .take_while(|line| line.starts_with("#[") || line.starts_with("//"))
        .any(|line| line.contains("Serialize") || line.contains("Deserialize"))
}

/// The journal row types the generator cannot carry yet: their `before` and `after` row values
/// must keep the row's stored JSON text, and the Rust target maps `Json` to `serde_json::Value`,
/// which re-parses a float. They stay hand-written until the target can map `Json` to raw text
/// (story:generate-journal-row-types-from-ess).
const AWAITING_RAW_JSON: [&str; 2] = ["JournaledRowChange", "DurableJournalEntry"];

/// A Rust type named after a capture-domain type and carrying a serde codec is a hand-written
/// duplicate of the generated one. The domain refinements in `eventlog-core`, which hold `u64`
/// counts and carry no codec, are not.
#[test]
fn no_workspace_crate_hand_writes_a_codec_for_a_capture_type() {
    let names = capture_type_names();
    let mut sources = Vec::new();
    rust_sources(&workspace().join("crates"), &mut sources);
    assert_ne!(sources.len(), 0, "no Rust sources under crates/");
    let mut duplicates = Vec::new();
    let mut awaiting = Vec::new();
    for source in sources {
        let text = std::fs::read_to_string(&source).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if let Some(name) = declared(line)
                && names.iter().any(|known| known == name)
                && derives_serde(&lines, index)
            {
                if AWAITING_RAW_JSON.contains(&name) {
                    awaiting.push(name.to_owned());
                } else {
                    duplicates.push(format!("{}:{}: {name}", source.display(), index + 1));
                }
            }
        }
    }
    assert!(
        duplicates.is_empty(),
        "hand-written codecs for generated capture types:\n{}",
        duplicates.join("\n")
    );
    awaiting.sort();
    let mut expected = AWAITING_RAW_JSON.map(str::to_owned).to_vec();
    expected.sort();
    assert_eq!(
        awaiting, expected,
        "the hand-written journal row types changed; generate them, or name the change here"
    );
}

/// The generated crate puts `serde_json/arbitrary_precision` behind its default `exact-numbers`
/// feature; with it on, Cargo would unify it into every crate here and `serde_json::to_vec`
/// would keep `1.50` as written, changing every stored request hash.
#[test]
fn serde_json_number_handling_is_the_default() {
    let body: serde_json::Value = serde_json::from_str(r#"{"a":1.50,"b":1e2}"#).unwrap();
    assert_eq!(
        serde_json::to_string(&body).unwrap(),
        r#"{"a":1.5,"b":100.0}"#
    );
}
