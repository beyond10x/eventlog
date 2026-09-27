# Tree store text stored once, v0.1

Status: **accepted** (operator direction, 2026-09-27). Serves O2. Story: `story:tree-store-keeps-text-once`.

## The problem

A tree store keeps every blob as one file of its exact bytes (`eventlog-tree/1`,
`crates/eventlog-tree/src/layout.rs` `blob_path`). Entity Runtime keeps its records and import
anchors as blobs, and AEP's planning entities put their text into those documents. The same text
is therefore written many times, and some of it twice over as hexadecimal:

Measured on a copy of the ESS planning store (`tenants/planning`, ESS commit `b89c160c`), with
`find -printf %s` over the files and `jq` over the six largest blobs:

| What | Files | Bytes |
|---|---:|---:|
| blob files | 9,682 | 172,422,227 |
| of which `er.eventlog.import-anchor/1` | 8,062 | 159,171,920 |
| largest blob: an import capture holding the whole planning tree, each file hex-encoded | 1 | 75,332,948 |
| second largest: a legacy record carried as hexadecimal | 1 | 10,308,803 |

A `review-result` anchor holds its 2.5 MB body twice (`fields.body` and `fields.document.fields.body`),
the legacy record holds it again inside its hexadecimal, and the capture holds it again inside a
markdown file and inside a JSON Lines journal. Two blobs exceed the 8 MiB per-blob limit of the
Gates scanner, so a push of the store is refused.

## Decision

A new store layout, `eventlog-tree/2`, may keep a blob as a **split blob**: a manifest whose long
string literals are **texts**, each stored once per tenant under its SHA-256.

```text
<tenant>/blobs/<kk>/<digest>                 a blob's bytes, as in eventlog-tree/1
<tenant>/split-blobs/<kk>/<digest>.json      a blob as a manifest (kk as blob_path)
<tenant>/texts/<hh>/<64 hex>                 one text's bytes; hh is the first two hex digits
```

The manifest is canonical JSON:

```json
{"blob":"<digest>","format":"eventlog-tree/split/1","length":123,"parts":[...],"sha256":"sha256:<hex>"}
```

`parts` expand, concatenated, to exactly the blob's bytes:

| Part | Expands to |
|---|---|
| a JSON string | its UTF-8 bytes |
| `{"text":"sha256:<hex>"}` | the bytes of that text |
| `{"hex":[parts]}` | the lower-case hexadecimal spelling of the expansion of `parts` |

A reader concatenates and refuses a manifest that is not canonical, names another blob, names a
missing text, or expands to another `length` or `sha256`. It never interprets a part.

### Where the writer cuts

Eventlog stays free of domain types (AGENTS.md invariant 1): the writer knows JSON, not what a
document means.

1. Bytes that are one or more JSON values separated by whitespace (a document, or JSON Lines) are
   scanned for string literals. A literal whose escaped content is at least 1,024 bytes
   (`MIN_TEXT`) is cut out. Shorter literals stay in the manifest.
2. A cut literal that ends in a run of at least 1,024 lower-case hex digits keeps any prefix
   (such as `hex:`) in the manifest and becomes `{"hex":[...]}` of the bytes the run spells. Those
   bytes are opened again by rule 1, up to four levels deep; bytes that do not open are one text.
3. UTF-8 text that is not JSON as a whole but has JSON lines — a JSON Lines journal with one
   damaged line — is cut line by line: JSON lines by rule 1, other lines of 1,024 bytes or more as
   one text each. Text with no JSON line is one text, so prose is not scattered per paragraph.
4. Any other literal is one text of its escaped content.

A blob that is not JSON, or holds no literal to cut, stays a raw file. The same literal, escaped the
same way, is the same text wherever it appears, which is what makes a body stored once across the
record, its revision, the legacy record inside the hexadecimal and the journal line inside the
capture.

### Compatibility

- **Every `eventlog-tree/1` store reads exactly as before.** Its `store.json` still names it, a
  writer on it still writes raw files, and nothing is migrated on open.
- **Digests and verification are unchanged.** Blob digests are the caller's opaque keys; the reader
  serves the same bytes under the same digest, so Entity Runtime's framed-key checks and every event
  that names a blob are untouched. Event, group and identity files are never rewritten.
- An `eventlog-tree/2` store reads raw blob files too, so a raw blob merged in from a branch that
  had not migrated is served. A blob held in both forms must agree, or the store is refused.
- An `eventlog-tree/1` store holding split files is refused by name: a first-layout reader would
  serve the manifest as the blob. A reader older than 0.6.0 refuses an `eventlog-tree/2`
  `store.json` by name, as it refuses every unknown format.
- A new store is created as `eventlog-tree/2`.
- Deleting a blob removes every text no other blob of the tenant names, so a deleted blob's content
  leaves the tree. Tenant erasure removes the tenant directory, texts included.

### Migration

`eventlog_tree::migrate(root, MigrationMode::{DryRun, Apply})`, explicit and never on open:

1. Take the writer lock; read and verify the whole store; plan every raw blob that would split.
2. A dry run reports blobs, blobs to split, texts to add, history bytes and the largest history
   file before and after, and writes nothing.
3. An apply switches `store.json` first (a v2 store whose blobs are all raw is valid), then per
   blob writes its texts, then its manifest, reads the manifest back and compares the bytes, and
   only then removes the raw file. A crash anywhere leaves a store that opens and serves the same
   bytes, and running the migration again finishes it.
4. It reloads the store and compares what it serves — every blob's bytes by digest, every group and
   event digest, identities, torn files — with what it served before, and reports the count.

It is idempotent: on a migrated store nothing is pending and an apply writes nothing.

`verify`'s V2 rule accepts the migration commit: a raw file or split blob of the base may be gone
when the head serves that digest with the base's bytes, and `store.json` may change from
`eventlog-tree/1` to `eventlog-tree/2` with its identity unchanged. V1 admits `split-blobs/` and
`texts/` only in an `eventlog-tree/2` store; V5 checks manifests are canonical.

On the ESS store copy above the migration measured (`migrate` report, dry run and apply agreeing):

| | Before | After |
|---|---:|---:|
| history bytes (`store.json` and `tenants/`) | 180,651,308 | 77,406,062 |
| largest history file | 75,332,948 | 5,153,913 |
| blobs split / texts written | | 3,827 / 4,901 |

The largest remaining file is one line of a legacy JSON Lines journal that is not valid JSON (an
unescaped quote inside a string), kept whole as a text. Every file is under the 8 MiB scan limit.

## The path-length proposal

`tree-layout-path-length-v0.1.md` proposes hashed stream directories under a new tag it calls
`eventlog-tree/2`. That proposal is still open and needs an Atlas ADR; this change takes the tag
because it ships first. The stream-directory change becomes `eventlog-tree/3`, and its migration
is a second step of the same `migrate` entry point. The two do not interact: this change touches
blob files only and adds no event path; the longest new path is a split blob at 129 characters
for the planning root, below the 198-character event path the proposal measures.

## Relying parties and order

| Order | Party | What moves |
|---|---|---|
| 1 | Eventlog | `eventlog-tree` reads both layouts, writes v2 for new stores, ships `migrate` (0.6.0) |
| 2 | Entity Runtime | pins that Eventlog revision; no code change, since it reads blobs through the port |
| 3 | AEP | pins both; ships `aep plan store migrate texts` over `eventlog_tree::migrate` |
| 4 | each repository with a tree store | runs the migration in its own commit once its tooling reads v2 |

A repository must not migrate before every tool that opens its store is at step 3, because an
older reader refuses a v2 store.
