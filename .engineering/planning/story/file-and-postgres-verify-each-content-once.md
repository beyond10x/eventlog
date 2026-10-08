---
format: aep.planning-md/3
id: story:file-and-postgres-verify-each-content-once
kind: story
status: implemented
title: File and PostgreSQL verify each blob's content once per handle
relations:
- serves: vision:O2
scope:
- confidence: inferred
  path: CHANGELOG.md
- confidence: inferred
  path: crates/eventlog-core/src/blob_integrity.rs
- confidence: cited
  path: crates/eventlog-file/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: cited
  path: crates/eventlog-sqlite/src/verified.rs
- confidence: inferred
  path: docs/design/file-provider.md
- confidence: inferred
  path: docs/design/sql-blob-read-integrity.md
- confidence: cited
  path: ess/blob-integrity/domains/blobs.yaml
revision: 6
transitions:
- {from: "draft", to: "proposed", at: "2026-10-08T09:04:31Z", actor: "human:timo", revision: 4}
- {from: "proposed", to: "active", at: "2026-10-08T09:04:31Z", actor: "human:timo", revision: 5}
- {from: "active", to: "implemented", at: "2026-10-08T10:04:02Z", actor: "human:timo", revision: 6, decided_on: {"recorded":{"test_result":1,"review_outcome":2}}}
---
## Outcome

A projector or guard that reads the same blob once per member of a batch, through
`ProjectionStore::get_blob` or `EventStore::get_blob`, pays one SHA-256 over that content per
handle on the File and PostgreSQL providers, as SQLite already does. Every read still refuses
content whose bytes, length, edition or recorded hash changed after it was verified.

## Why

An Entity Runtime 0.30.2 adversary finding: a recorded batch of M members reads its batch blob
once per member, and on File (`crates/eventlog-file/src/lib.rs:696-712` at 0.8.1, `Transaction::blob`)
and PostgreSQL (`crates/eventlog-postgres/src/lib.rs:1376-1407`, `PostgresProjections::get_blob`,
and `:1003`) each read hashes the whole blob, so the batch costs O(M^2) in SHA-256. SQLite skips
the hash for content its handle already verified (`crates/eventlog-sqlite/src/verified.rs`,
commit 340145f9). Entity Runtime holds the target as the ignored test
`crates/entity-eventlog/tests/batch_cost_file.rs` (at most 2.5x CPU per doubling of members,
128/256/512 members, File provider).

## Specification

`ess/blob-integrity/domains/blobs.yaml` declares `eventlog.blobs.VerifiedContent`: content a
handle verified, keyed by its integrity SHA-256, with its byte count. Validated with ess 0.56.0.

## Acceptance

- File and PostgreSQL remember, per handle, each `(integrity_sha256, exact bytes)` they verified
  or hashed at write time, under the same rules as SQLite's `VerifiedBlobs`: a read skips SHA-256
  only when the stored hash names remembered content and the bytes just read equal it in full;
  memory is bounded by the same budget and cleared on blob deletion, tenant erasure and a failed
  blob-binding write through that handle.
- The provider-neutral part of `VerifiedBlobs` lives in one place used by all three providers,
  not three copies; no crate or module named `common`, `shared`, `utils`, `misc` or `helpers`.
- Red first: a test per provider reads one blob N times through `ProjectionStore::get_blob` inside
  one guarded append and asserts one SHA-256 computation, not N; it fails at main and passes after.
- Tamper tests: content changed on disk (File) or in the row (PostgreSQL) after a verified read
  is refused on the next read through the same handle; each refusal fails with the byte
  comparison stubbed to `true` (invariant 5: mutation applied, watched, reverted).
- `docs/design/file-provider.md` and `docs/design/sql-blob-read-integrity.md` state what is
  remembered and for how long; `CHANGELOG.md` has an Unreleased entry.
- `cargo clippy -p <crate> --all-targets --locked -- -D warnings` and `cargo test -p <crate>
  --locked` pass for eventlog-core, eventlog-file, eventlog-sqlite and eventlog-postgres
  (PostgreSQL tests against `EVENTLOG_TEST_POSTGRES_URL` where it is set; a skipped backend is
  reported as not proved).
