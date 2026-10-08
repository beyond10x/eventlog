---
format: aep.planning-md/3
id: story:tree-store-keeps-text-once
kind: story
status: implemented
title: A tree store keeps each long text once
relations:
- serves: vision:O2
revision: 5
transitions:
- {from: "draft", to: "proposed", at: "2026-10-08T11:00:38Z", actor: "human:timo", revision: 3}
- {from: "proposed", to: "active", at: "2026-10-08T11:00:38Z", actor: "human:timo", revision: 4}
- {from: "active", to: "implemented", at: "2026-10-08T12:24:45Z", actor: "human:timo", revision: 5, decided_on: {"recorded":{"test_result":1}}}
---
# A tree store keeps each long text once

## Outcome

A tree store keeps a blob's long text once, under its SHA-256, and the blob as a manifest that
points at it (`eventlog-tree/2`). Every `eventlog-tree/1` store reads exactly as before, and an
explicit, idempotent migration with a dry run and a read-back verification moves a store to the
new layout without changing a byte any reader is served.

## Why

On a copy of the ESS planning store (ESS commit `b89c160c`) 9,682 blob files hold 172,422,227
bytes; 159,171,920 of them are import anchors. The largest blob is 75,332,948 bytes: the whole
captured planning tree, each file hex-encoded. A `review-result` body of 2.5 MB is held twice in
its anchor, again in a legacy record's hexadecimal, and again in the capture. Two blobs exceed the
8 MiB per-blob limit of the Gates scanner, so a push of the store is refused. The operator's
direction, 2026-09-27: store the text once and point to it by its hash; every existing store must
still read exactly as before; ship a migrate command.

## Acceptance

- `a_new_store_keeps_a_body_two_blobs_share_as_one_text_and_serves_the_exact_bytes`
- `a_v1_store_reads_and_writes_raw_blobs_as_before`
- `a_dry_run_writes_nothing_and_an_apply_serves_every_blob_byte_for_byte`, including idempotence
  and `verify` against the pre-migration base
- `a_changed_text_is_refused_on_open_and_by_verify`
- `deleting_a_blob_removes_the_texts_only_it_named`
- On a copy of the ESS planning store every history file is under 8 MiB after the migration.

## Design

`docs/design/tree-text-blobs-v0.1.md`.
