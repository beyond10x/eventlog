---
format: aep.planning-md/3
id: story:docs-name-the-current-release
kind: story
status: draft
title: The public docs name the current release and summarize every release
summary: website pages still name 0.3.0 as current; releases.md stops at 0.3.0
owner: eventlog
relations:
- informed_by: review-result:adversary-file-eventlog-indexes-streams-pass-1
revision: 1
---
## Outcome

The public documentation names the current release where it says which release is current or
which tag to pin, and the release highlights page covers every release up to it.

## Why

Found while fixing `README.md` for https://github.com/beyond10x/eventlog/issues/42 (index unit,
correction 1 at `609ab0a4`). The workspace version is 0.8.0 (`Cargo.toml` `[workspace.package]`)
and these still name 0.3.0, read at that commit:

- `website/docs/intro.md:21` says the current release is 0.3.0;
- `website/docs/quickstart.md:14,31,32,46` pin 0.3.0;
- `website/docs/releases.md` has sections up to 0.3.0 only;
- `website/docs/operations.md:52`, `guarantees.md:28,35`, `providers.md:41-44` make statements
  about 0.3.0 that may be outdated (not checked).

`crates/eventlog-file/tests/adversary_stream_index_docs.rs` already holds `README.md` to the
workspace version.

## Acceptance

- The pages above name 0.8.0 (or the release current when this lands) where they state the
  current release or the tag to pin; statements scoped to 0.3.0 are kept only where they still
  describe 0.3.0 and say so.
- `website/docs/releases.md` has a section per release from 0.4.0 on, each taken from
  `CHANGELOG.md`.
- The docs case extends to these pages and goes red when one names another version as current.
