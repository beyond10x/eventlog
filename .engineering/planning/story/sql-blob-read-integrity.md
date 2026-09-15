---
format: aep.planning-md/1
id: story:sql-blob-read-integrity
kind: story
status: active
title: Verify SQL blob integrity and explicitly import legacy bindings
relations:
- serves: vision:O2
- depends_on: story:blob-binding-conflicts
- depends_on: story:provider-qualified-production-proof
scope:
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-conformance/src/lib.rs
- confidence: inferred
  path: crates/eventlog-core/src/blob_integrity.rs
- confidence: cited
  path: crates/eventlog-core/src/lib.rs
- confidence: cited
  path: crates/eventlog-core/src/projection.rs
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof/required.rs
- confidence: cited
  path: crates/eventlog-postgres/src/atomic_group.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/src/pool.rs
- confidence: cited
  path: crates/eventlog-postgres/src/schema.rs
- confidence: cited
  path: crates/eventlog-postgres/tests/conformance.rs
- confidence: cited
  path: crates/eventlog-sqlite/src/atomic_group.rs
- confidence: cited
  path: crates/eventlog-sqlite/src/lib.rs
- confidence: cited
  path: crates/eventlog-sqlite/tests/conformance.rs
- confidence: cited
  path: docs/design/sql-blob-read-integrity.md
- confidence: cited
  path: ess/blob-integrity/domains/blobs.yaml
- confidence: cited
  path: ess/blob-integrity/system.yaml
revision: 37
---
## Outcome

Every SQLite/PostgreSQL blob read, transactional callback read and put readback verifies stored
length and computed SHA256 before returning bytes or acknowledging a binding. Legacy populated
stores require an explicit, atomic trust-of-observed-bytes migration; no old history is invented.

## Authority and typed home

Approved ESS evolution revision1, plan ess-evolution-20260915, SHA256
7579145c3de5a1c6f8088fd7fb804d29dac8903ec505048f3ce595c45023b787. Astra owns the selected design
under the approved delegation. Binding design: docs/design/sql-blob-read-integrity.md, independently
approved by review-result:sql-blob-integrity-design-pass-2 after the four pass1 findings were corrected
and their design-only outcomes recorded. Generic BlobIntegrity coordinates live in ess/blob-integrity/;
the minimal model validated and compiled successfully before this story was created.
Public digest identities and immutable binding semantics remain governed by blob-binding-parity.md.
Implementation scope is independently derived with cited/inferred paths. Scope and design approval
do not qualify code; behavioral red, production proof and independent implementation review remain.

## Acceptance

- Prove the existing unchecked-byte behavior with real SQLite/PostgreSQL cases before correction,
  including a same-length mutation after reopen and all six provider/read boundary combinations.
- Add the exact integrity_sha256/integrity_v1 columns and constrained edition in the design without
  rewriting old payload/count/identity fields or changing eventlog-file/1. Old five-column INSERTs
  fail after upgrade; actual old-reader behavior and its fenced-use limitation are demonstrated.
- Fallibly validate count, version, exact lowercase hash encoding and computed bytes everywhere.
  Corrupt persisted bindings return Backend without payload disclosure or panic; valid conflicting
  puts retain Invalid, identical retries succeed, and tenant/deletion/rebind/concurrency controls hold.
- Callback corruption poisons the enclosing transaction even when caught. Guarded single/group
  appends, inline views, reservations, receipts, catch-up and rebuild leave no partial effects or
  cursor advancement. Rebuild in each provider reads blobs from the original owner while writing
  shadow projections; preserve tenant authority and prove present-blob success and corruption
  failure. Preserve all existing cancellation and reservation semantics.
- Exactly admit old/current SQLite blob shape and retain all PostgreSQL schema/security/projection
  checks. Pin actual old edition checksums before changing base DDL. Refuse partial/altered shapes,
  inconsistent ledger editions and malformed rows before persistent migration changes.
- Preserve current API signatures as RefusePopulated wrappers; expose the closed explicit
  TrustObservedBytes provider-edge choice and exact finite report described by the design.
  Hosted application open cannot migrate or choose trust. Every successful populated import
  checks every old row and backfills/validates/changes ledger in one transaction under the required
  locks, with bounded memory and exact row-count/byte preservation. No success precedes commit ack.
  The sole new UPDATE is integrity-metadata backfill within that fenced migration; runtime puts
  still never overwrite a binding, and predecessor payload/identity/count/time fields stay intact.
  Preserve the exact acknowledged report and typed cleanup cause in BlobMigrationCompleted if
  temporary-pool shutdown fails; existing wrappers propagate it. BlobMigrationCommitUnknown carries
  no report; primary migration failure takes precedence over secondary cleanup failure. A retry's
  false/0 report describes that retry only, never the earlier import or its count.
- Test fresh/current/empty/populated predecessor cases, propagated and caught corruption, invalid
  rows, concurrent migration and failure at migration stages. Retain actual causal mutations of
  hash/count/callback/writer/old-edition guards and restore them before final checks.
- Add every decisive SQLite/PostgreSQL case to the provider-qualified production roster. Pass
  affected suites, fmt, strict workspace all-target Clippy, independent review and the actual
  bash scripts/gate.sh --production-proof with PostgreSQL17.6 and hosted TLS actually selected.

## Scope

Derived 2026-09-15 by `story-scoper`. Every line is **cited** (read from the story, selected design, or accepted tree) or **inferred** (a placement that does not yet exist).

- **Shared integrity implementation:** `crates/eventlog-core/src/blob_integrity.rs` — **inferred**; the selected design prefers one shared Rust validator and this new module is the narrow placement for fallible count/version/lowercase-SHA256/byte validation plus `LegacyBlobMigration` and `BlobMigrationReport`.
- **Public contract and errors:** `crates/eventlog-core/src/lib.rs:630` — **cited**; add/re-export the migration enum/report and add `BlobMigrationCommitUnknown` and recursive `BlobMigrationCompleted { report, cleanup }` to `EventLogError`; existing blob methods are at lines 972–1007.
- **Transactional callback contract:** `crates/eventlog-core/src/projection.rs:51` — **cited**; `ProjectionStore::get_blob` is the callback boundary whose corruption result must poison the enclosing provider transaction even when caught.
- **Shared conformance ownership:** `crates/eventlog-conformance/src/lib.rs:315` — **cited**; extend the existing blob binding, inline failure, reservation, catch-up, and rebuild helpers rather than creating provider-independent behavior twice.
- **SQLite implementation:** `crates/eventlog-sqlite/src/lib.rs:54` — **cited**; owns `open_with_blob_migration`, refusal-default wrappers, exact old/current blob admission, current DDL, ordinary/put/callback reads, transaction poison, catch-up, rebuild, and separate owner-blob/projection-target coordinates.
- **SQLite group transaction:** `crates/eventlog-sqlite/src/atomic_group.rs:18` — **cited**; owns guarded group callbacks, nested single appends, group receipts, and the shared transaction-failure marker.
- **SQLite real-backend evidence:** `crates/eventlog-sqlite/tests/conformance.rs:11` — **cited**; the existing file-backed target has raw `rusqlite` access and already owns blob races, callback atomicity, and rebuild isolation, so it can cover the three SQLite read boundaries, exact DDL/admission, migration, reopen, fencing, and rollback cases.
- **PostgreSQL implementation:** `crates/eventlog-postgres/src/lib.rs:61` — **cited**; owns `local_with_blob_migration`, `migrate_with_blob_migration`, refusal-default wrappers, acknowledged-report/cleanup precedence, ordinary/put/callback reads, catch-up, rebuild, and owner/shadow coordinates.
- **PostgreSQL group transaction:** `crates/eventlog-postgres/src/atomic_group.rs:10` — **cited**; owns single/group guard and inline callback execution, command/group receipts, reservations, commits, rollbacks, and the shared corruption marker.
- **PostgreSQL DDL and admission:** `crates/eventlog-postgres/src/schema.rs:19` — **cited**; owns predecessor/current blob DDL, old-shape comparison before mutation, additive migration and locks, stable bounded import, final-shape verification, historical ledger checksums, current checksum, old-reader refusal, and migration commit acknowledgement.
- **PostgreSQL cleanup-failure seam:** `crates/eventlog-postgres/src/pool.rs:432` — **cited**; the existing bounded shutdown/quarantine boundary must supply the controlled post-commit cleanup failure without weakening production pool behavior.
- **PostgreSQL real-backend evidence:** `crates/eventlog-postgres/tests/conformance.rs:678` — **cited**; this target already owns raw database access, schema admission, migration, blob concurrency, callback atomicity, rebuild, cancellation, and unknown-commit exercises.
- **Production roster:** `crates/eventlog-postgres/examples/production-proof/required.rs:8` — **cited**; retain every existing case and add every new SQLite/PostgreSQL conformance and PostgreSQL library case by exact package/kind/target/name.
- **SQLite required case identities:** `eventlog-sqlite/test/conformance/{sqlite_blob_integrity_covers_all_read_boundaries_and_binding_controls,sqlite_callback_blob_corruption_poison_rolls_back_every_owner,sqlite_blob_migration_is_exact_explicit_atomic_and_fenced}` — **inferred**; these three proposed cases cover all required SQLite proof without a new target.
- **PostgreSQL required case identities:** `eventlog-postgres/test/conformance/{postgres_blob_integrity_covers_all_read_boundaries_and_binding_controls,postgres_callback_blob_corruption_poison_rolls_back_every_owner,postgres_blob_migration_is_exact_explicit_atomic_and_fenced,postgres_blob_migration_unknown_commit_has_no_report}` — **inferred**; these proposed cases cover all required PostgreSQL real-backend proof.
- **PostgreSQL library case identities:** `eventlog-postgres/lib/eventlog_postgres/{schema::blob_integrity_tests::historical_editions_and_old_reader_are_frozen,blob_migration_tests::acknowledged_report_survives_cleanup_failure}` — **inferred**; these proposed cases own checksum goldens, exact old-reader policy, acknowledged report preservation, primary-error precedence, and retry false/0 behavior.
- **Adopter-visible record:** `CHANGELOG.md:6` — **cited**; record verified SQL reads and the explicit legacy import boundary without release claims.
- **Selected design:** `docs/design/sql-blob-read-integrity.md:1` — **cited**; root-owned and already present; it fixes the exact DDL, trust policy, callback poison, rebuild coordinates, unknown-commit behavior, and cleanup-report semantics.
- **ESS system home:** `ess/blob-integrity/system.yaml:1` — **cited**; root-owned and already present.
- **ESS typed coordinates:** `ess/blob-integrity/domains/blobs.yaml:1` — **cited**; root-owned and already present.
- **Confidence:** high — **cited**; every existing source/test owner and all six byte-returning paths were read at accepted HEAD `18322cbe19f0068e9b3fab84d874d6abea181f3e`; only the new module, private failure hook, and future test names remain inferred.
- **Would collide with:** any unit touching core blob/error contracts, either SQL provider’s blob/append/callback/rebuild code, PostgreSQL schema or pool shutdown, provider conformance targets, or the production roster — **cited**.

## Limits

Stored checksums do not authenticate content against authority able to rewrite bytes and checksum
together. Explicit legacy trust establishes an observed boundary and does not prove older history.
No real store cutover, deployment, AEP migration, ER adapter or consumer release occurs in this unit.
Whole-group crash/unknown/restart recovery, frozen file vectors and all consumer migrations remain
required under the full initiative. A green narrow suite does not complete those obligations.
