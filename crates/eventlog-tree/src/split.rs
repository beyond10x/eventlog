//! Split blobs: a blob's long text stored once, by its SHA-256, and the blob kept as a manifest
//! that points at it (`docs/design/tree-text-blobs-v0.1.md`).
//!
//! A blob is opaque bytes under an opaque digest. When those bytes are a JSON document, the long
//! string literals in it are what makes it large, and the same literal is often held by many
//! blobs: a body in a record, again in its revision, again in an import capture. An
//! `eventlog-tree/2` store may therefore keep such a blob as a *split blob* instead of its bytes:
//!
//! ```text
//! <tenant>/split-blobs/<kk>/<digest>.json   {blob, format, length, parts, sha256}
//! <tenant>/texts/<hh>/<64 hex>              the bytes of one text, named by their SHA-256
//! ```
//!
//! `parts` is a list whose expansion is exactly the blob's bytes: a JSON string is those bytes
//! literally, `{"text": "sha256:<hex>"}` is the text's bytes, and `{"hex": [parts]}` is the
//! lower-case hexadecimal spelling of the expansion of `parts`. The last form is what lets a
//! literal that spells bytes in hexadecimal keep them once, as bytes, and — when those bytes are a
//! JSON document themselves — keep that document's own long literals as texts too.
//!
//! Nothing here knows what a blob means. The cut is made at JSON string literals because that is
//! where a document's text is, and a reader never interprets a part: it concatenates, and it
//! refuses an expansion whose length or SHA-256 is not the one the manifest recorded.

use crate::layout::{
    backend, blob_path, canonical, corrupt, segment, sha256_hex, tenant_dir, write_atomic,
    write_once,
};
use eventlog_core::EventLogError;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// The format tag of a split-blob manifest.
pub(crate) const SPLIT_FORMAT: &str = "eventlog-tree/split/1";

/// The shortest literal content, in bytes, that is cut out as a text. Shorter literals stay in
/// the manifest: a file per short string would cost more in file count than it saves in bytes.
pub(crate) const MIN_TEXT: usize = 1024;

/// How deep hexadecimal literals that spell JSON are opened. The documents seen so far nest one
/// level (a record carried as hexadecimal inside an anchor); the bound keeps a hostile blob from
/// making the writer recurse without end.
const MAX_DEPTH: usize = 4;

/// Where the manifest of the split blob `digest` lives.
pub(crate) fn split_path(root: &Path, tenant: &str, digest: &str) -> PathBuf {
    let raw = blob_path(root, tenant, digest);
    let shard = raw
        .parent()
        .and_then(Path::file_name)
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    tenant_dir(root, tenant)
        .join("split-blobs")
        .join(shard)
        .join(format!("{}.json", segment(digest)))
}

/// Where the text whose SHA-256 is `hex` lives.
pub(crate) fn text_path(root: &Path, tenant: &str, hex: &str) -> PathBuf {
    tenant_dir(root, tenant)
        .join("texts")
        .join(&hex[..2])
        .join(hex)
}

/// The text a reference names, as its 64 lower-case hexadecimal digits.
fn text_hex(reference: &str) -> Option<&str> {
    let hex = reference.strip_prefix("sha256:")?;
    (hex.len() == 64
        && hex
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')))
    .then_some(hex)
}

/// A blob cut into a manifest and the texts it points at.
pub(crate) struct Split {
    /// The manifest's canonical bytes.
    pub manifest: Vec<u8>,
    /// Each text the manifest names, by its SHA-256 hex, once.
    pub texts: BTreeMap<String, Vec<u8>>,
}

/// Cut `bytes`, the blob `digest`, into a manifest and texts. `None` when the bytes are not a
/// JSON document or hold no literal long enough to cut, so the blob is better kept as it is.
pub(crate) fn split(digest: &str, bytes: &[u8]) -> Option<Split> {
    if bytes.len() < MIN_TEXT {
        return None;
    }
    let mut texts = BTreeMap::new();
    let parts = parts_of(bytes, 0, &mut texts)?;
    let manifest = canonical(&json!({
        "blob": digest,
        "format": SPLIT_FORMAT,
        "length": bytes.len(),
        "parts": parts,
        "sha256": format!("sha256:{}", sha256_hex(bytes)),
    }));
    Some(Split { manifest, texts })
}

/// The parts `bytes` expands from, or `None` when it is not JSON or nothing in it was cut.
fn parts_of(
    bytes: &[u8],
    depth: usize,
    texts: &mut BTreeMap<String, Vec<u8>>,
) -> Option<Vec<Value>> {
    if depth >= MAX_DEPTH {
        return None;
    }
    if !is_json_sequence(bytes) {
        return line_parts(bytes, depth, texts);
    }
    let mut parts = Parts::default();
    let mut cut = false;
    let mut kept = 0;
    for (start, end) in literals(bytes) {
        let content = &bytes[start..end];
        if content.len() < MIN_TEXT {
            continue;
        }
        cut = true;
        let hex_start = end - hex_suffix(content);
        if end - hex_start >= MIN_TEXT {
            parts.literal(&bytes[kept..hex_start]);
            let decoded = unhex(&bytes[hex_start..end]);
            let nested =
                parts_of(&decoded, depth + 1, texts).unwrap_or_else(|| vec![text(decoded, texts)]);
            parts.push(json!({ "hex": nested }));
        } else {
            parts.literal(&bytes[kept..start]);
            parts.push(text(content.to_vec(), texts));
        }
        kept = end;
    }
    parts.literal(&bytes[kept..]);
    cut.then_some(parts.0)
}

/// The parts of UTF-8 text that is not JSON as a whole but holds lines that are: a JSON Lines
/// journal with one damaged line. Each line that is JSON is cut as a document; every other line
/// is kept as it is, as a text when it is long. `None` unless some JSON line was cut, so ordinary
/// prose is not scattered into a text per paragraph.
fn line_parts(
    bytes: &[u8],
    depth: usize,
    texts: &mut BTreeMap<String, Vec<u8>>,
) -> Option<Vec<Value>> {
    if std::str::from_utf8(bytes).is_err() || !bytes.contains(&b'\n') {
        return None;
    }
    let mut parts = Parts::default();
    let mut cut = false;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let body = line.strip_suffix(b"\n").unwrap_or(line);
        let opened = if is_json_sequence(body) {
            parts_of(body, depth, texts)
        } else {
            None
        };
        match opened {
            Some(inner) => {
                cut = true;
                for part in inner {
                    match part {
                        Value::String(literal) => parts.literal(literal.as_bytes()),
                        other => parts.push(other),
                    }
                }
                parts.literal(&line[body.len()..]);
            }
            None if line.len() >= MIN_TEXT => parts.push(text(line.to_vec(), texts)),
            None => parts.literal(line),
        }
    }
    cut.then_some(parts.0)
}

/// Whether `bytes` are one or more JSON values separated by whitespace — a document, or JSON
/// Lines. Outside a string literal such bytes hold only JSON punctuation, scalars and whitespace,
/// which is what lets [`literals`] find every literal by its quotes.
fn is_json_sequence(bytes: &[u8]) -> bool {
    let mut values = serde_json::Deserializer::from_slice(bytes).into_iter::<Value>();
    let mut seen = false;
    loop {
        match values.next() {
            None => return seen,
            Some(Ok(_)) => seen = true,
            Some(Err(_)) => return false,
        }
    }
}

fn text(bytes: Vec<u8>, texts: &mut BTreeMap<String, Vec<u8>>) -> Value {
    let hex = sha256_hex(&bytes);
    let reference = json!({ "text": format!("sha256:{hex}") });
    texts.entry(hex).or_insert(bytes);
    reference
}

/// A part list that merges adjacent literals, so one expansion has one manifest.
#[derive(Default)]
struct Parts(Vec<Value>);

impl Parts {
    fn literal(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        // Every cut is at an ASCII byte of a valid UTF-8 document, so each piece is UTF-8 too.
        let piece = std::str::from_utf8(bytes).expect("a cut at ASCII keeps UTF-8");
        if let Some(Value::String(last)) = self.0.last_mut() {
            last.push_str(piece);
        } else {
            self.0.push(Value::String(piece.to_owned()));
        }
    }

    fn push(&mut self, part: Value) {
        self.0.push(part);
    }
}

/// The content span of every string literal in a valid JSON sequence, quotes excluded.
fn literals(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'"' {
            let start = index + 1;
            let mut cursor = start;
            while bytes[cursor] != b'"' {
                cursor += if bytes[cursor] == b'\\' { 2 } else { 1 };
            }
            spans.push((start, cursor));
            index = cursor + 1;
        } else {
            index += 1;
        }
    }
    spans
}

/// The length of the longest even-length suffix of `content` made of lower-case hex digits.
fn hex_suffix(content: &[u8]) -> usize {
    let run = content
        .iter()
        .rev()
        .take_while(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        .count();
    run - run % 2
}

fn unhex(hex: &[u8]) -> Vec<u8> {
    let digit = |byte: u8| match byte {
        b'0'..=b'9' => byte - b'0',
        _ => byte - b'a' + 10,
    };
    hex.as_chunks::<2>()
        .0
        .iter()
        .map(|pair| (digit(pair[0]) << 4) | digit(pair[1]))
        .collect()
}

fn push_hex(out: &mut Vec<u8>, bytes: &[u8]) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)]);
        out.push(DIGITS[usize::from(byte & 0x0f)]);
    }
}

/// The blob bytes the manifest at `path`, for `digest`, expands to.
///
/// # Errors
/// Refuses a manifest that is not canonical, names another blob, names a text that is missing, or
/// expands to bytes of another length or SHA-256.
pub(crate) fn expand_file(
    root: &Path,
    tenant: &str,
    digest: &str,
    path: &Path,
) -> Result<Vec<u8>, EventLogError> {
    let bytes = fs::read(path).map_err(backend)?;
    let manifest: Value =
        serde_json::from_slice(&bytes).map_err(|_| corrupt("a split blob that is not JSON"))?;
    if canonical(&manifest) != bytes {
        return Err(corrupt("a split blob that is not canonical"));
    }
    let map = manifest
        .as_object()
        .filter(|map| map.get("format").and_then(Value::as_str) == Some(SPLIT_FORMAT))
        .ok_or_else(|| corrupt("a split blob of an unknown format"))?;
    if map.len() != 5 || map.get("blob").and_then(Value::as_str) != Some(digest) {
        return Err(corrupt("a split blob whose name does not match it"));
    }
    let parts = map
        .get("parts")
        .and_then(Value::as_array)
        .ok_or_else(|| corrupt("a split blob without parts"))?;
    let mut out = Vec::new();
    expand(root, tenant, parts, &mut out)?;
    if map.get("length").and_then(Value::as_u64) != Some(out.len() as u64)
        || map.get("sha256").and_then(Value::as_str)
            != Some(format!("sha256:{}", sha256_hex(&out)).as_str())
    {
        return Err(corrupt("a split blob that does not expand to its digest"));
    }
    Ok(out)
}

fn expand(
    root: &Path,
    tenant: &str,
    parts: &[Value],
    out: &mut Vec<u8>,
) -> Result<(), EventLogError> {
    for part in parts {
        match part {
            Value::String(literal) => out.extend_from_slice(literal.as_bytes()),
            Value::Object(map) if map.len() == 1 => {
                if let Some(reference) = map.get("text").and_then(Value::as_str) {
                    let hex = text_hex(reference)
                        .ok_or_else(|| corrupt("a split blob naming a text badly"))?;
                    let text = fs::read(text_path(root, tenant, hex))
                        .map_err(|_| corrupt("a split blob whose text is missing"))?;
                    out.extend_from_slice(&text);
                } else if let Some(nested) = map.get("hex").and_then(Value::as_array) {
                    let mut inner = Vec::new();
                    expand(root, tenant, nested, &mut inner)?;
                    push_hex(out, &inner);
                } else {
                    return Err(corrupt("a split blob with an unknown part"));
                }
            }
            _ => return Err(corrupt("a split blob with an unknown part")),
        }
    }
    Ok(())
}

/// Every text a manifest's parts name.
fn referenced(parts: &[Value], into: &mut BTreeSet<String>) {
    for part in parts {
        if let Value::Object(map) = part {
            if let Some(hex) = map.get("text").and_then(Value::as_str).and_then(text_hex) {
                into.insert(hex.to_owned());
            }
            if let Some(nested) = map.get("hex").and_then(Value::as_array) {
                referenced(nested, into);
            }
        }
    }
}

fn manifest_parts(path: &Path) -> Vec<Value> {
    fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Map<String, Value>>(&bytes).ok())
        .and_then(|mut map| map.remove("parts"))
        .and_then(|parts| match parts {
            Value::Array(parts) => Some(parts),
            _ => None,
        })
        .unwrap_or_default()
}

/// The bytes the tree holds for blob `digest` of `tenant`, in whichever form it holds them.
///
/// # Errors
/// Refuses a split blob that does not expand to its digest, and a blob held in both forms whose
/// two forms disagree.
pub(crate) fn read_blob(
    root: &Path,
    tenant: &str,
    digest: &str,
) -> Result<Option<Vec<u8>>, EventLogError> {
    let raw = match fs::read(blob_path(root, tenant, digest)) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(backend(error)),
    };
    let manifest = split_path(root, tenant, digest);
    if !manifest.exists() {
        return Ok(raw);
    }
    let expanded = expand_file(root, tenant, digest, &manifest)?;
    match raw {
        Some(raw) if raw != expanded => Err(corrupt("a blob whose two forms disagree")),
        _ => Ok(Some(expanded)),
    }
}

/// Store blob `digest` of `tenant`: split when `split_allowed` and the bytes cut, as they are
/// otherwise. A blob the tree already holds, in either form, must hold the same bytes.
///
/// Texts are written before the manifest that names them, so a reader never finds a manifest
/// whose text is not there yet. A text another blob already wrote is left as it is.
pub(crate) fn write_blob(
    root: &Path,
    tenant: &str,
    digest: &str,
    bytes: &[u8],
    split_allowed: bool,
) -> Result<(), EventLogError> {
    let raw = blob_path(root, tenant, digest);
    if raw.exists() || !split_allowed {
        return write_once(&raw, bytes);
    }
    let manifest = split_path(root, tenant, digest);
    if manifest.exists() {
        return match read_blob(root, tenant, digest)? {
            Some(held) if held == bytes => Ok(()),
            _ => Err(corrupt("an immutable file already holds different bytes")),
        };
    }
    match split(digest, bytes) {
        Some(cut) => {
            for (hex, text) in &cut.texts {
                write_once(&text_path(root, tenant, hex), text)?;
            }
            write_once(&manifest, &cut.manifest)
        }
        None => write_once(&raw, bytes),
    }
}

/// Remove blob `digest` of `tenant` in both forms, and every text only it named.
pub(crate) fn delete_blob(root: &Path, tenant: &str, digest: &str) -> Result<(), EventLogError> {
    remove_if_present(&blob_path(root, tenant, digest))?;
    let manifest = split_path(root, tenant, digest);
    if !manifest.exists() {
        return Ok(());
    }
    let mut named = BTreeSet::new();
    referenced(&manifest_parts(&manifest), &mut named);
    remove_if_present(&manifest)?;
    for other in split_files(root, tenant)? {
        let mut still = BTreeSet::new();
        referenced(&manifest_parts(&other.1), &mut still);
        named.retain(|hex| !still.contains(hex));
    }
    for hex in named {
        remove_if_present(&text_path(root, tenant, &hex))?;
    }
    Ok(())
}

fn remove_if_present(path: &Path) -> Result<(), EventLogError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(backend(error)),
    }
}

fn files_under(directory: &Path) -> Result<Vec<(String, PathBuf)>, EventLogError> {
    let mut found = Vec::new();
    for shard in crate::layout::subdirectories(directory)? {
        for entry in fs::read_dir(&shard).map_err(backend)? {
            let entry = entry.map_err(backend)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with('.') && entry.file_type().map_err(backend)?.is_file() {
                found.push((name, entry.path()));
            }
        }
    }
    found.sort();
    Ok(found)
}

/// Every split-blob manifest of `tenant`, with the digest its file name spells.
pub(crate) fn split_files(
    root: &Path,
    tenant: &str,
) -> Result<Vec<(String, PathBuf)>, EventLogError> {
    let mut found = Vec::new();
    for (name, path) in files_under(&tenant_dir(root, tenant).join("split-blobs"))? {
        let Some(stem) = name.strip_suffix(".json") else {
            return Err(corrupt("a split blob file"));
        };
        found.push((crate::history::unescape(stem)?, path));
    }
    Ok(found)
}

/// Every raw blob file of `tenant`, with the digest its file name spells.
pub(crate) fn raw_files(
    root: &Path,
    tenant: &str,
) -> Result<Vec<(String, PathBuf)>, EventLogError> {
    files_under(&tenant_dir(root, tenant).join("blobs"))?
        .into_iter()
        .map(|(name, path)| Ok((crate::history::unescape(&name)?, path)))
        .collect()
}

/// Every blob of `tenant` in either form, by digest, with its bytes.
///
/// # Errors
/// As [`read_blob`], for each.
pub(crate) fn tenant_blobs(
    root: &Path,
    tenant: &str,
) -> Result<BTreeMap<String, Vec<u8>>, EventLogError> {
    let mut digests: BTreeSet<String> = raw_files(root, tenant)?
        .into_iter()
        .map(|(digest, _)| digest)
        .collect();
    digests.extend(
        split_files(root, tenant)?
            .into_iter()
            .map(|(digest, _)| digest),
    );
    let mut blobs = BTreeMap::new();
    for digest in digests {
        if let Some(bytes) = read_blob(root, tenant, &digest)? {
            blobs.insert(digest, bytes);
        }
    }
    Ok(blobs)
}

/// Whether `tenant` holds any file only an `eventlog-tree/2` store may hold.
pub(crate) fn holds_split_files(root: &Path, tenant: &str) -> bool {
    let directory = tenant_dir(root, tenant);
    directory.join("split-blobs").exists() || directory.join("texts").exists()
}

/// Replace the raw file of blob `digest` by its split form, and read it back before the raw file
/// goes. Returns the texts it wrote that were not there before, with their lengths.
pub(crate) fn convert(
    root: &Path,
    tenant: &str,
    digest: &str,
    bytes: &[u8],
    cut: &Split,
) -> Result<Vec<(String, u64)>, EventLogError> {
    let mut written = Vec::new();
    for (hex, text) in &cut.texts {
        let path = text_path(root, tenant, hex);
        if !path.exists() {
            written.push((hex.clone(), text.len() as u64));
        }
        write_once(&path, text)?;
    }
    let manifest = split_path(root, tenant, digest);
    write_once(&manifest, &cut.manifest)?;
    if expand_file(root, tenant, digest, &manifest)? != bytes {
        return Err(corrupt("a split blob that does not read back"));
    }
    remove_if_present(&blob_path(root, tenant, digest))?;
    if let Some(directory) = blob_path(root, tenant, digest).parent() {
        crate::layout::sync_dir(directory)?;
    }
    Ok(written)
}

/// Rewrite `store.json` with `format`, keeping its identity.
pub(crate) fn set_format(root: &Path, format: &str) -> Result<(), EventLogError> {
    let path = root.join("store.json");
    let bytes = fs::read(&path).map_err(backend)?;
    let mut value: Value = serde_json::from_slice(&bytes).map_err(|_| corrupt("store.json"))?;
    value
        .as_object_mut()
        .ok_or_else(|| corrupt("store.json"))?
        .insert("format".into(), Value::String(format.to_owned()));
    write_atomic(&path, &canonical(&value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand_in_memory(parts: &[Value], texts: &BTreeMap<String, Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        for part in parts {
            match part {
                Value::String(literal) => out.extend_from_slice(literal.as_bytes()),
                Value::Object(map) => {
                    if let Some(reference) = map.get("text").and_then(Value::as_str) {
                        out.extend_from_slice(&texts[text_hex(reference).unwrap()]);
                    } else {
                        let inner = expand_in_memory(map["hex"].as_array().unwrap(), texts);
                        push_hex(&mut out, &inner);
                    }
                }
                _ => panic!("unknown part"),
            }
        }
        out
    }

    fn round_trip(bytes: &[u8]) -> Split {
        let cut = split("sha256:blob", bytes).expect("the document cuts");
        let manifest: Value = serde_json::from_slice(&cut.manifest).unwrap();
        assert_eq!(
            expand_in_memory(manifest["parts"].as_array().unwrap(), &cut.texts),
            bytes,
            "the manifest does not expand to the blob"
        );
        cut
    }

    #[test]
    fn a_long_literal_repeated_in_one_blob_is_one_text() {
        let body = "a line of prose\\n".repeat(200);
        let bytes = format!(r#"{{"body":"{body}","document":{{"body":"{body}"}},"n":1}}"#);
        let cut = round_trip(bytes.as_bytes());
        assert_eq!(cut.texts.len(), 1, "the repeated body was stored twice");
        assert_eq!(cut.texts.values().next().unwrap(), body.as_bytes());
    }

    #[test]
    fn a_hexadecimal_literal_keeps_its_bytes_and_opens_json_it_spells() {
        let body = "x".repeat(3000);
        let inner = format!(r#"{{"body":"{body}","id":"r"}}"#);
        let mut hex = Vec::new();
        push_hex(&mut hex, inner.as_bytes());
        let hex = String::from_utf8(hex).unwrap();
        let bytes = format!(r#"["tag",{{"body":"{body}","exact":"hex:{hex}"}}]"#);
        let cut = round_trip(bytes.as_bytes());
        assert_eq!(
            cut.texts.len(),
            1,
            "the body inside the hexadecimal record was not the same text as the body beside it"
        );
        let manifest: Value = serde_json::from_slice(&cut.manifest).unwrap();
        assert!(
            manifest.to_string().contains("hex:"),
            "the prefix before the hexadecimal run was lost"
        );
    }

    #[test]
    fn hexadecimal_that_is_not_json_is_one_text_of_its_bytes() {
        let file = b"---\ntitle: a file\n---\n".repeat(100);
        let mut hex = Vec::new();
        push_hex(&mut hex, &file);
        let bytes = format!(r#"{{"bytes":"hex:{}"}}"#, String::from_utf8(hex).unwrap());
        let cut = round_trip(bytes.as_bytes());
        assert_eq!(cut.texts.values().next().unwrap(), &file);
    }

    #[test]
    fn an_odd_hexadecimal_run_and_escapes_round_trip() {
        let odd = format!("q{}", "abc".repeat(700));
        let escaped = "say \\\"hi\\\" \\\\ ".repeat(100);
        let bytes = format!(r#"{{"a":"{odd}","b":"{escaped}","c":"short"}}"#);
        round_trip(bytes.as_bytes());
    }

    #[test]
    fn json_lines_spelled_in_hexadecimal_share_their_texts_with_the_document() {
        let body = "a paragraph of a story\\n".repeat(100);
        let lines = format!("{{\"body\":\"{body}\"}}\n{{\"body\":\"{body}\",\"n\":2}}\n");
        let mut hex = Vec::new();
        push_hex(&mut hex, lines.as_bytes());
        let bytes = format!(
            r#"{{"body":"{body}","journal":"hex:{}"}}"#,
            String::from_utf8(hex).unwrap()
        );
        let cut = round_trip(bytes.as_bytes());
        assert_eq!(
            cut.texts.len(),
            1,
            "a body in a JSON Lines journal was stored again"
        );
    }

    #[test]
    fn a_damaged_line_does_not_keep_the_other_lines_of_a_journal_whole() {
        let body = "a paragraph of a story\\n".repeat(100);
        let damaged = format!("{{\"body\":\"{body} \\\"unclosed\"\" }}\n");
        let lines = format!("{{\"body\":\"{body}\"}}\n{damaged}{{\"n\":1,\"body\":\"{body}\"}}");
        let mut hex = Vec::new();
        push_hex(&mut hex, lines.as_bytes());
        let bytes = format!(
            r#"{{"body":"{body}","journal":"hex:{}"}}"#,
            String::from_utf8(hex).unwrap()
        );
        let cut = round_trip(bytes.as_bytes());
        assert_eq!(
            cut.texts.len(),
            2,
            "expected the shared body once and the damaged line as its own text"
        );
    }

    #[test]
    fn a_blob_that_is_not_json_or_has_no_long_literal_is_not_split() {
        assert!(split("d", &vec![b'x'; 5000]).is_none());
        let many_short = format!("[{}\"x\"]", "\"short\",".repeat(1000));
        assert!(split("d", many_short.as_bytes()).is_none());
        assert!(split("d", br#"{"a":"b"}"#).is_none());
    }
}
