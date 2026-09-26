# Tree-store path length on Windows checkouts, v0.1

Status: **proposed**. Serves O2. Story: `story:tree-store-checks-out-on-windows`.

This document changes no bytes. Every option below changes the on-disk layout that the planning stores of
several repositories hold, so it is a coordinated migration and needs an Atlas ADR before any of it is
built.

## The problem

When Git has `core.longpaths` off, it refuses to check out a path of 260 characters or more (`MAX_PATH`).
Cargo checks out a Git dependency's whole tree at a revision, planning store included, under

```text
<CARGO_HOME>\git\checkouts\<name>-<16 hex>\<7-char rev>\
```

`<CARGO_HOME>` is 27 characters on GitHub's hosted Windows runner, so that prefix is `68 + len(name)` characters. For Eventlog it is 76, for Entity Runtime 82. An in-repository
path therefore fits only if it is at most `259 - 68 - len(name)` characters: 183 for Eventlog, 177 for
Entity Runtime, 188 for a three-letter name.

Entity Runtime 0.24.0's Windows release build failed on exactly this. 0.24.1 turns on `core.longpaths`
in CI to get past it.

## Measured budget, current layout (`eventlog-tree/1`)

The inputs are the longest a planning store holds today: root `.engineering/state`, tenant `planning`,
stream type `er.subject`, stream id `sha256:<64 hex>`, and full SHA-256 digests. The first four rows are
computed from the layout functions by
`layout::tests::a_planning_store_writes_each_file_kind_at_its_recorded_longest_path_length`. If a layout
change moves any of them, that test fails and this table must be updated.

| File kind | Layout function | Path shape | Length | + 76 (Eventlog) | + 82 (Entity Runtime) |
|---|---|---|---:|---:|---:|
| event | `event_path` | `tenants/<t>/streams/<type>/<seg(id)>/<64>.json` | **198** | 274 ✗ | 280 ✗ |
| blob | `blob_path` | `tenants/<t>/blobs/sh/sha256%3A<64>` | 118 | 194 | 200 |
| group | `group_path` | `tenants/<t>/groups/<kk>/<64>.json` | 115 | 191 | 197 |
| identity | `identity_path` | `tenants/<t>/identity.json` | 49 | 125 | 131 |
| store manifest | `root.join("store.json")` in `lib.rs` (no layout function, so not in the test) | `store.json` | 29 | 105 | 111 |
| event staging (never tracked) | `write_atomic` | `.<64>.json.tmp` next to the event | 203 | n/a | n/a |

The root-relative part of the event path is 179 characters. The root prefix and the checkout prefix
stack on top of it:

- AEP's fixture `crates/edge/aep-cli/tests/fixtures/tree-rendered-by-0-58-0/` adds 59 characters and
  reaches 257 before any checkout prefix.
- The event path grows with the stream id. `segment` escapes every byte outside `[A-Za-z0-9._-]` to three
  characters, so no stream id length is safe in general. The current layout has no maximum; it has a
  longest *observed* path.

Only the event kind is over budget. Blobs and groups already use a fixed two-character fan-out and full
digests, and they fit.

Staging files are written only on the writer's machine and never checked out. Rust's standard library
passes long absolute paths to Windows in verbatim form, so the staging row is believed not to be
affected. That has not been measured on Windows.

## Options

Every option changes the event path only. Blob, group and identity paths stay as they are.

| | Event path shape | Max event length | + 76 | + 82 | Bounded for any id? |
|---|---|---:|---:|---:|---|
| today | `streams/<type>/<seg(id)>/<64>.json` | 198 | 274 ✗ | 280 ✗ | no |
| A. drop the `sha256%3A` from the id segment | `streams/<type>/<64>/<64>.json` | 189 | 265 ✗ | 271 ✗ | no |
| B. shorten the event file name to a 32-hex digest prefix | `streams/<type>/<seg(id)>/<32>.json` | 166 | 242 | 248 | no |
| C. hash the stream key to a fixed name (recommended) | `streams/<kk>/<32 hex>/<64>.json` | **149** | 225 | 231 | **yes** |
| C′. C with a 64-hex stream name | `streams/<kk>/<64>/<64>.json` | 181 | 257 | 263 ✗ | yes |

### A. Drop the percent-encoded digest prefix from the directory name

It saves 9 characters, and that is not enough on its own: 265 is still over the limit. It also gives
the layout knowledge of one caller's id convention (`sha256:`), which breaks AGENTS.md invariant 1. A
generic version, a shorter escape for `:`, saves 2 characters. Rejected.

### B. Name each event file by a truncated digest

Keep the stream directory. Name the file by the first 32 hex characters of its digest and keep the full
digest inside the record.

- **Read compatibility.** A reader can accept both: a 64-character stem is v1 and a 32-character stem is
  v2, and it compares the stem with the recomputed digest either way. Parent references hold full
  digests, so they are unaffected.
- **Cost.** Small. `event_path`, the stem check in `history.rs`, and `verify`'s name rules change.
- **Why it is not enough.** It fits both prefixes today, with 17 and 11 characters to spare, but the
  length still grows with the stream id. A stream type or id 12 characters longer breaks it again, so
  the bound the story's acceptance asks for cannot be stated independently of callers.

### C. Hash the stream key to a fixed-length directory (recommended)

The stream directory becomes `streams/<kk>/<h>`, where `h` is the first 32 hex characters of
`sha256(seg(type) + "/" + seg(id))`, and `kk` is `h[..2]`, the same fan-out blobs and groups already
use. `segment` escapes `/`, so this key is unambiguous.

- **Length.** 149 for every tenant `planning` store, whatever the stream type or id: 34 characters of
  slack against Eventlog's prefix and 28 against Entity Runtime's. A 64-hex name (C′) is 181, which
  still breaks Entity Runtime's prefix by 4. That is why the name is truncated. A 128-bit name gives no
  practical collision risk, and a collision would be detected anyway (next point).
- **Read compatibility.** Every event record already carries `tenant`, `stream_type` and `stream_id`.
  `history.rs` already refuses "an event file outside its stream's directory" by recomputing
  `stream_dir` from the record. So a reader can serve both layouts by choosing the recomputation from
  `store.json`'s format tag: `eventlog-tree/1` keeps today's function, and `eventlog-tree/2` uses the
  hashed one. A store whose files do not match its own tag is refused by name (`verify` V1, and the
  replay's existing path check). Accepting both shapes inside one store is possible but not
  recommended, because it makes "which path is this event's" ambiguous.
- **Migration mechanics.** Event bytes do not contain their own path, so a migration only renames
  files and rewrites `store.json`'s format tag. Digests, parents, groups and blobs are unchanged. Git
  records the change as one commit of 100%-similarity renames. The old paths stay in history:
  - A checkout of any revision before the migration still fails on Windows without `core.longpaths`.
  - A checkout of any revision after it succeeds.
  - Consumers are fixed by depending on a post-migration revision, not by the migration alone.
- **Cost.**
  - Code: `stream_dir`, the directory walk in `history.rs` (`streams/<kk>/<h>/` instead of
    `streams/<type>/<id>/`), `verify`'s layout rules, and a migration entry point.
  - One migration commit in every repository that carries a tree store (AEP, ESS, Entity Runtime,
    Eventlog, SDK, and any others the ADR enumerates).
  - Stream directories stop being readable by a person browsing the tree. The type and id stay in
    every event file.
  - `verify` V2 ("a file the base holds is gone") would flag every event against a pre-migration base.
    It has to compare by digest across a format change, or treat the migration commit as a named
    exception.

### D. Keep the layout and have consumers set `core.longpaths`

This is the status quo since Entity Runtime 0.24.1. It costs nothing here, but every Windows consumer
of every repository that carries a tree store must know to set it. The story's acceptance explicitly
asks for that step to become removable, so D does not close the story. I don't know whether publishing
to a registry, which packages only crate directories, is an option; every workspace here has
`publish = false`.

## Recommendation

Adopt **C**:

- Add a hashed, fixed-length stream directory under a new format tag `eventlog-tree/2`.
- Keep a reader for `eventlog-tree/1` indefinitely. Old revisions and AEP's `tree-rendered-by-0-58-0`
  fixture are v1 by design.
- Migrate with an explicit rename-only command, never implicitly on open.
- Replace the characterization test with a bound: the event, group and blob paths under a
  `.engineering/state` root, plus a stated checkout prefix, are under 260. C makes that bound
  independent of caller ids.

It is the only option here that produces a bound a check can hold for every caller, and its migration
leaves every digest in place.

## What the Atlas ADR must decide

1. **The format.** `eventlog-tree/2` and the exact stream-directory function: the hash input, the
   truncation length and the fan-out.
2. **Read policy.** Whether v1 is read forever or until a named release, and whether a store that mixes
   layouts is refused, as recommended here, or normalized.
3. **Checkout budget.** The prefix the bound assumes (`68 + len(name)` on GitHub's Windows runner), the
   longest repository name it must admit, and the deepest store root it must admit. AEP's fixture
   directory is 59 characters.
4. **Cutover order.** Eventlog releases a v2-capable reader first. Every tree-store repository then
   depends on that reader before any repository migrates, because a v1-only reader cannot open a
   migrated store. The migration then happens in each repository in turn. Open branches that write the
   store must merge before their repository migrates, or be rebased by the migration command, because
   an unmigrated branch merged into a migrated store adds v1 paths that the store refuses.
5. **`verify` V2 across the migration commit.** Whether it compares by digest or treats the commit as a
   named exception.
6. **Retiring the workaround.** When Entity Runtime's `core.longpaths` step is removed: only once every
   Git dependency it pins is at a post-migration revision.
