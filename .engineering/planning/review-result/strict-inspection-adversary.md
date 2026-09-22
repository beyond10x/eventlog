---
format: aep.planning-md/1
id: review-result:strict-inspection-adversary
kind: review-result
status: active
title: Independent strict inspection review
relations:
- reviews: story:strict-read-only-history-inspection
revision: 1
---
unit: strict-read-only-history-inspection, fd8a96b plus stable inspection implementation working tree
verdict: CONFIRMED — three blocker findings; correction required
cases: executed 94→102, red 4
origin: introduced 0 / pre-existing 0 / undecided 3
wrote-outside-worktree: assigned scratch and target directories; exact private path manifest retained beside this report
needs-coordinator: route three inspection-specific corrections, retain the failing tests, and run the wider production gate after repair

```text
git --no-pager diff --stat
 Cargo.lock                                         | 19 +++++
 crates/eventlog-conformance/src/lib.rs              |  2 +
 crates/eventlog-core/src/lib.rs                     |  2 +
 crates/eventlog-file/src/journal.rs                 | 96 +++++++++++++++++++++-
 crates/eventlog-file/src/lib.rs                     |  2 +
 .../eventlog-postgres/examples/production-proof.rs  | 18 ++++
 crates/eventlog-sqlite/Cargo.toml                   |  3 +
 crates/eventlog-sqlite/src/lib.rs                   |  2 +
 8 files changed, 143 insertions(+), 1 deletion(-)

git diff --no-index --stat /dev/null crates/eventlog-file/tests/adversary_strict_inspection.rs
 .../tests/adversary_strict_inspection.rs | 192 +++++++++++++++++++++
 1 file changed, 192 insertions(+)

git diff --no-index --stat /dev/null crates/eventlog-sqlite/tests/adversary_strict_inspection.rs
 .../tests/adversary_strict_inspection.rs | 316 +++++++++++++++++++++
 1 file changed, 316 insertions(+)
```

The whole-tree diff above is not this adversary's diff: the assigned checkout was already dirty with the stable implementation. Parent also owns disjoint future blob planning and specification work. The only repository writes made in this pass are the two new, untracked test files shown by the two no-index comparisons: 508 lines, eight top-level cases. No implementation, existing test, planning, specification, or repository documentation file was edited by this reviewer. The charter's literal whole-tree-clean proof cannot hold in this explicitly shared dirty checkout; the ownership-qualified comparison above is the review boundary, not a claim that the production diff is mine.

All paths below use public-safe aliases. Exact local paths and all retained outputs are listed in the private `outside-paths.txt` beside this report. The reviewed source predates the corrections requested here; no source owner was active during the executions.

## 1. Added cases and first behavioral executions

All cargo commands used `CARGO_TARGET_DIR=<cache>/b10x-target/ekr-eventlog-inspection-20260922`, `CARGO_BUILD_JOBS=2`, and `TMPDIR=<cache>/ekr-completion-20260922/eventlog-inspection/adversary`. Main-disk free space remained above the assigned 10 GB floor, last measured 22 GB. Fixtures are temporary synthetic histories written by the actual published providers and then, only where the case requires corruption, deliberately mutated. There was no live operator-store access.

| New case | Assertion and result |
|---|---|
| File: adversary_file_duplicate_event_identity_is_corruption | Three distinct events at global positions 1, 3, 4 may not carry the same event identity. RED: accepted. |
| File: adversary_file_unknown_recorded_envelope_field_is_not_discarded | A nested unsupported event-envelope field must not disappear during inspection. RED: discarded and accepted. |
| File: adversary_file_inadmissible_recorded_envelope_is_corruption | Independently mutate event_id, name, actor to empty and data to an array; all four must refuse. RED: all four accepted. |
| File: adversary_file_empty_identity_refuses_and_zero_result_caps_allow_absence | Actual stored empty identity refuses; absent tenant is empty even with zero result caps; source preserved. GREEN. |
| SQLite: adversary_sqlite_inadmissible_recorded_envelope_is_corruption | Same four inadmissible envelope variants through real SQLite rows. RED: all four accepted. |
| SQLite: adversary_sqlite_empty_identity_and_exact_source_cap | Exact source-byte bound succeeds, one byte below refuses, absent tenant with zero result caps succeeds, stored empty identity refuses; source preserved. GREEN. |
| SQLite: adversary_sqlite_success_preserves_preexisting_reader_lock | A subprocess exclusive writer is refused before and after successful public inspection while an ordinary same-process reader holds its transaction. GREEN. |
| SQLite: adversary_sqlite_public_descriptor_bound_keeps_prior_writer_lock | In a fresh process, 64 public inspections succeed; hard-link reuse succeeds; the 65th distinct inode refuses before opening another descriptor and preserves an existing same-process writer lock. GREEN. |

Each failure case was executed alone before the package suite. The File fixture rebuilds the real journal's frame digests and manifest, so these failures are semantic, not bad-checksum failures. Each mutation counts changed records and every preservation assertion compares source bytes and directory entries. SQLite first uses a fully closed writer and explicitly prepares a cold rollback-journal fixture; it does not relabel an ordinary WAL source as supported.

Exact single-case command form:

```sh
cargo test --locked -p <provider-package> --test adversary_strict_inspection <case-name> -- --exact --nocapture
```

The following are the retained first behavioral reds, with only private path prefixes substituted. Each command executed one case and exited 101. The SQLite excerpt includes the final four-mutant version; its earlier three-mutant behavioral red is also retained as sqlite-envelope-red.log.

`cargo test --locked -p eventlog-file --test adversary_strict_inspection adversary_file_duplicate_event_identity_is_corruption -- --exact --nocapture`

```text
   Compiling eventlog-file v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-file)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.44s
     Running tests/adversary_strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary_strict_inspection-a271ad57f4475742)

running 1 test

thread 'adversary_file_duplicate_event_identity_is_corruption' (375657) panicked at crates/eventlog-file/tests/adversary_strict_inspection.rs:91:5:
assertion `left == right` failed
  left: Ok(HistoryInspection { tenant: TenantId("inspection-tenant"), stream_identity: None, events: [RecordedEvent { global_seq: 1, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "first", version: 1, event_id: "01a0c72a-a830-7ce1-bf5e-4b5af9267c61", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:30:56.68832563 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-0", trace_id: "trace-inspection-0", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(0)} }, RecordedEvent { global_seq: 3, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "second", version: 1, event_id: "01a0c72a-a830-7ce1-bf5e-4b5af9267c61", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:30:56.726500372 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-2", trace_id: "trace-inspection-2", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(2)} }, RecordedEvent { global_seq: 4, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "first", version: 2, event_id: "01a0c72a-a830-7ce1-bf5e-4b5af9267c61", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:30:56.745156071 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-3", trace_id: "trace-inspection-3", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(3)} }] })
 right: Err(CorruptSource)
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test adversary_file_duplicate_event_identity_is_corruption ... FAILED

failures:

failures:
    adversary_file_duplicate_event_identity_is_corruption

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.09s

error: test failed, to rerun pass `-p eventlog-file --test adversary_strict_inspection`
```

`cargo test --locked -p eventlog-file --test adversary_strict_inspection adversary_file_unknown_recorded_envelope_field_is_not_discarded -- --exact --nocapture`

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.04s
     Running tests/adversary_strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary_strict_inspection-a271ad57f4475742)

running 1 test

thread 'adversary_file_unknown_recorded_envelope_field_is_not_discarded' (377094) panicked at crates/eventlog-file/tests/adversary_strict_inspection.rs:116:5:
assertion `left == right` failed
  left: Ok(HistoryInspection { tenant: TenantId("inspection-tenant"), stream_identity: None, events: [RecordedEvent { global_seq: 1, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "first", version: 1, event_id: "01a0c72a-cd77-7443-ae7e-e09e5f43f61c", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:31:06.231686703 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-0", trace_id: "trace-inspection-0", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(0)} }, RecordedEvent { global_seq: 3, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "second", version: 1, event_id: "01a0c72a-cda5-7a50-ad6d-45edc12c772d", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:31:06.277368186 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-2", trace_id: "trace-inspection-2", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(2)} }, RecordedEvent { global_seq: 4, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "first", version: 2, event_id: "01a0c72a-cdba-7791-b95a-758610eddaf4", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:31:06.298826192 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-3", trace_id: "trace-inspection-3", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(3)} }] })
 right: Err(CorruptSource)
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test adversary_file_unknown_recorded_envelope_field_is_not_discarded ... FAILED

failures:

failures:
    adversary_file_unknown_recorded_envelope_field_is_not_discarded

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.11s

error: test failed, to rerun pass `-p eventlog-file --test adversary_strict_inspection`
```

`cargo test --locked -p eventlog-file --test adversary_strict_inspection adversary_file_inadmissible_recorded_envelope_is_corruption -- --exact --nocapture`

```text
   Compiling eventlog-file v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-file)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.25s
     Running tests/adversary_strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary_strict_inspection-a271ad57f4475742)

running 1 test

thread 'adversary_file_inadmissible_recorded_envelope_is_corruption' (383122) panicked at crates/eventlog-file/tests/adversary_strict_inspection.rs:188:5:
invalid recorded envelopes were not refused: ["event_id: accepted=true", "name: accepted=true", "actor: accepted=true", "data: accepted=true"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test adversary_file_inadmissible_recorded_envelope_is_corruption ... FAILED

failures:

failures:
    adversary_file_inadmissible_recorded_envelope_is_corruption

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.34s

error: test failed, to rerun pass `-p eventlog-file --test adversary_strict_inspection`
```

`cargo test --locked -p eventlog-sqlite --test adversary_strict_inspection adversary_sqlite_inadmissible_recorded_envelope_is_corruption -- --exact --nocapture`

```text
   Compiling eventlog-sqlite v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-sqlite)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.26s
     Running tests/adversary_strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary_strict_inspection-843029f592bb4282)

running 1 test

thread 'adversary_sqlite_inadmissible_recorded_envelope_is_corruption' (383989) panicked at crates/eventlog-sqlite/tests/adversary_strict_inspection.rs:83:5:
invalid recorded envelopes were not refused: ["event id: accepted=true", "event name: accepted=true", "actor: accepted=true", "body: accepted=true"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test adversary_sqlite_inadmissible_recorded_envelope_is_corruption ... FAILED

failures:

failures:
    adversary_sqlite_inadmissible_recorded_envelope_is_corruption

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.34s

error: test failed, to rerun pass `-p eventlog-sqlite --test adversary_strict_inspection`
```

Two harness corrections are explicitly excluded from the findings. The first duplicate-ID compilation needed an Option<String> annotation (file-duplicate-first.log); no case executed in that attempt. The initial SQLite lock controls opened and closed the database through the test's own inventory routine while a POSIX lock was held, thereby releasing that process's lock. Inventories now run before the native transaction starts and after it ends; required subprocess writer checks remain both before and after inspection. Their corrected individual runs pass. Initial and corrected logs remain available. A final lint correction replaced two equivalent String clones in the new File fixture helper; final targeted clippy passes.

## 2. Package suite, after the cases existed

Baseline 94 is the implementor's reported and retained four-package run, not a preemptive adversary suite run. The final package run executes 102 top-level cases: 98 pass, 4 fail, zero ignored and zero filtered in the selected lane. All 94 pre-existing cases pass. Child-process executions are asserted by their parent cases and are not counted as additional top-level cases.

Command: `cargo test --locked -p eventlog-core -p eventlog-conformance -p eventlog-file -p eventlog-sqlite --no-fail-fast`

Exit: 101. Verbatim runner output, private prefixes substituted:

```text
   Compiling eventlog-file v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-file)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.47s
     Running unittests src/lib.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/eventlog_conformance-4c622f71bc4c5a4c)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/eventlog_core-50b45bd4d9def61d)

running 13 tests
test tests::a_command_with_no_events_is_refused ... ok
test tests::an_event_body_must_be_an_object ... ok
test admission::tests::coordinates_preserve_boundaries_and_grants_cannot_be_reconstructed ... ok
test tests::a_stream_cannot_be_named_without_a_tenant ... ok
test tests::an_identity_that_names_a_person_is_refused ... ok
test tests::depth_beyond_the_limit_is_refused ... ok
test tests::effect_evidence_and_boundary_inventory_are_machine_checked ... ok
test tests::effect_metadata_refuses_personal_text_in_every_identifier_and_code ... ok
test tests::effect_inventory_requires_unique_opaque_names_and_complete_coverage ... ok
test tests::effect_metadata_bounds_cover_required_optional_and_outcome_fields ... ok
test tests::the_same_body_hashes_the_same_way ... ok
test tests::effect_stages_preserve_wire_shape_and_explicit_coverage ... ok
test tests::input_validation_preserves_valid_wire_and_exact_field_limits ... ok

test result: ok. 13 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/eventlog_file-ed9c21f042a5b927)

running 7 tests
test journal::tests::crash_child ... ok
test journal::tests::divergent_longer_history_does_not_extend_observed_head ... ok
test journal::tests::mixed_recovery_intents_refuse_before_changing_authority ... ok
test journal::tests::committed_damage_and_unproven_suffix_are_never_repaired ... ok
test journal::tests::process_death_at_each_privacy_boundary ... ok
test journal::tests::process_death_at_each_append_boundary ... ok
test journal::tests::privacy_crash_cleans_cached_bodies_and_blobs_on_open ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.62s

     Running tests/adversary_strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary_strict_inspection-f100e414764d74ff)

running 4 tests
test adversary_file_empty_identity_refuses_and_zero_result_caps_allow_absence ... ok
test adversary_file_unknown_recorded_envelope_field_is_not_discarded ... FAILED
test adversary_file_duplicate_event_identity_is_corruption ... FAILED
test adversary_file_inadmissible_recorded_envelope_is_corruption ... FAILED

failures:

---- adversary_file_unknown_recorded_envelope_field_is_not_discarded stdout ----

thread 'adversary_file_unknown_recorded_envelope_field_is_not_discarded' (388738) panicked at crates/eventlog-file/tests/adversary_strict_inspection.rs:116:5:
assertion `left == right` failed
  left: Ok(HistoryInspection { tenant: TenantId("inspection-tenant"), stream_identity: None, events: [RecordedEvent { global_seq: 1, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "first", version: 1, event_id: "01a0c72d-b53c-7741-b609-d64caf8d4bca", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:34:16.636122537 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-0", trace_id: "trace-inspection-0", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(0)} }, RecordedEvent { global_seq: 3, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "second", version: 1, event_id: "01a0c72d-b582-7360-b3a8-536da08e4985", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:34:16.706906821 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-2", trace_id: "trace-inspection-2", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(2)} }, RecordedEvent { global_seq: 4, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "first", version: 2, event_id: "01a0c72d-b5ac-7583-8888-88e13a91c88c", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:34:16.748920914 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-3", trace_id: "trace-inspection-3", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(3)} }] })
 right: Err(CorruptSource)
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- adversary_file_duplicate_event_identity_is_corruption stdout ----

thread 'adversary_file_duplicate_event_identity_is_corruption' (388735) panicked at crates/eventlog-file/tests/adversary_strict_inspection.rs:91:5:
assertion `left == right` failed
  left: Ok(HistoryInspection { tenant: TenantId("inspection-tenant"), stream_identity: None, events: [RecordedEvent { global_seq: 1, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "first", version: 1, event_id: "01a0c72d-b53c-7741-b609-d624c000955f", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:34:16.63607727 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-0", trace_id: "trace-inspection-0", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(0)} }, RecordedEvent { global_seq: 3, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "second", version: 1, event_id: "01a0c72d-b53c-7741-b609-d624c000955f", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:34:16.706490119 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-2", trace_id: "trace-inspection-2", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(2)} }, RecordedEvent { global_seq: 4, tenant: TenantId("inspection-tenant"), stream_type: "inspection", stream_id: "first", version: 2, event_id: "01a0c72d-b53c-7741-b609-d624c000955f", name: "Inspected", schema_version: 7, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-22 3:34:16.748843862 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-inspection-3", trace_id: "trace-inspection-3", causation_id: Some("inspection-cause"), causation_depth: 1, redacted_at: None, data: Object {"ordinal": Number(3)} }] })
 right: Err(CorruptSource)

---- adversary_file_inadmissible_recorded_envelope_is_corruption stdout ----

thread 'adversary_file_inadmissible_recorded_envelope_is_corruption' (388737) panicked at crates/eventlog-file/tests/adversary_strict_inspection.rs:188:5:
invalid recorded envelopes were not refused: ["event_id: accepted=true", "name: accepted=true", "actor: accepted=true", "data: accepted=true"]


failures:
    adversary_file_duplicate_event_identity_is_corruption
    adversary_file_inadmissible_recorded_envelope_is_corruption
    adversary_file_unknown_recorded_envelope_field_is_not_discarded

test result: FAILED. 1 passed; 3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.46s

error: test failed, to rerun pass `-p eventlog-file --test adversary_strict_inspection`
     Running tests/conformance.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/conformance-fd829e3c95f2a393)

running 11 tests
test file_claims_contract ... ok
test file_inline_contract ... ok
test file_scopes_contract ... ok
test file_callback_failure_contract ... ok
test file_rebuild_contract ... ok
test file_paging_contract ... ok
test file_projections_contract ... ok
test file_groups_contract ... ok
test file_public_inputs_contract ... ok
test file_storage_contract ... ok
test file_snapshots_contract ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.06s

     Running tests/durability.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/durability-5260062a3e574278)

running 7 tests
test process_writer ... ok
test conflicting_groups_commit_exactly_one_complete_request ... ok
test reopen_preserves_receipts_and_cache_deletion_preserves_authority ... ok
test blob_damage_and_missing_manifest_refuse_reopen ... ok
test redaction_fences_projection_reads_and_new_writes_until_complete_rebuild ... ok
test privacy_removes_active_bytes_and_never_reuses_feed_positions ... ok
test independent_processes_serialize_groups_and_duplicate_keys ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.48s

     Running tests/strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/strict_inspection-42cae3c2aa1fe523)

running 6 tests
test file_inspection_native_lock_is_nonblocking ... ok
test file_inspection_missing_sources_never_create ... ok
test file_inspection_recovery_and_corruption_preserve_source ... ok
test file_inspection_history_preserves_source ... ok
test file_inspection_identity_redaction_and_unknown_format ... ok
test file_inspection_concurrent_append_is_one_observation ... ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.32s

     Running unittests src/lib.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/eventlog_sqlite-21c1b5ae672c7bb8)

running 7 tests
test inspection::linux::tests::inspection_ofd_blocks_native_writer_process ... ok
test inspection::linux::tests::inspection_concurrent_call_cannot_unlock_another_observation ... ok
test inspection::linux::tests::inspection_ofd_blocks_native_writers_in_process ... ok
test inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock ... ok
test inspection::linux::tests::inspection_refusal_preserves_existing_process_reader_lock ... ok
test inspection::linux::tests::inspection_ofd_detects_replaced_source ... ok
test inspection::linux::tests::inspection_descriptor_exhaustion_never_opens_or_closes_source ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.73s

     Running tests/adversary.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary-b843d9a5c895ed07)

running 1 test
test legacy_projection_rows_are_erased_without_reregistering_retired_projectors ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s

     Running tests/adversary_strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary_strict_inspection-843029f592bb4282)

running 4 tests
test adversary_sqlite_empty_identity_and_exact_source_cap ... ok
test adversary_sqlite_inadmissible_recorded_envelope_is_corruption ... FAILED
test adversary_sqlite_success_preserves_preexisting_reader_lock ... ok
test adversary_sqlite_public_descriptor_bound_keeps_prior_writer_lock ... ok

failures:

---- adversary_sqlite_inadmissible_recorded_envelope_is_corruption stdout ----

thread 'adversary_sqlite_inadmissible_recorded_envelope_is_corruption' (389537) panicked at crates/eventlog-sqlite/tests/adversary_strict_inspection.rs:83:5:
invalid recorded envelopes were not refused: ["event id: accepted=true", "event name: accepted=true", "actor: accepted=true", "body: accepted=true"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    adversary_sqlite_inadmissible_recorded_envelope_is_corruption

test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 6.27s

error: test failed, to rerun pass `-p eventlog-sqlite --test adversary_strict_inspection`
     Running tests/atomic_groups.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/atomic_groups-800e8bdfb5adc91e)

running 3 tests
test ordered_groups_commit_and_rollback_as_one_unit ... ok
test group_retry_survives_database_reopen ... ok
test concurrent_groups_preserve_order_without_partial_commits ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.95s

     Running tests/conformance.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/conformance-dabec4c1848fb15b)

running 11 tests
test the_claim_rule_holds_in_memory ... ok
test the_inline_projection_exercise_passes_in_memory ... ok
test the_paging_rule_holds_in_memory ... ok
test the_projection_exercise_passes_in_memory ... ok
test rebuild_preserves_other_tenants_and_previous_view_on_failure ... ok
test the_shared_exercise_passes_in_memory ... ok
test scope_reservations_are_atomic_and_confined_in_memory_and_file ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test inline_failure_preserves_all_atomic_state_and_callback_authority ... ok
test the_shared_exercise_passes_on_a_file ... ok
test two_owners_share_a_database_without_sharing_a_table ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.34s

     Running tests/evolution.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/evolution-0e9014dc71a2f0a4)

running 3 tests
test a_body_written_under_an_older_version_still_folds ... ok
test a_version_with_no_upcaster_is_named_rather_than_guessed ... ok
test a_committed_vector_still_folds_when_loaded_from_disk ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/input_validation_review.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/input_validation_review-ebb535db6bb38ed0)

running 4 tests
test append_refuses_deserialized_streams_that_bypass_constructor_checks ... ok
test append_refuses_deserialized_events_that_bypass_constructor_checks ... ok
test append_refuses_claim_fields_that_bypass_constructor_checks ... ok
test public_input_validation_is_atomic_on_sqlite ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

     Running tests/legacy_namespaces.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/legacy_namespaces-984ef597a659f4d9)

running 2 tests
test ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure ... ok
test legacy_erasure_matches_literal_underscores_and_preserves_other_owners ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.26s

     Running tests/repository.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/repository-1e1459dfe611e57b)

running 12 tests
test a_command_that_decided_nothing_writes_nothing ... ok
test a_concurrent_write_is_retried_once_and_then_refused ... ok
test a_delayed_unproven_snapshot_cannot_restore_redacted_state ... ok
test a_retried_command_is_answered_not_written_again ... ok
test a_stale_snapshot_schema_is_discarded_rather_than_trusted ... ok
test an_aggregate_stays_foldable_after_an_erasure ... ok
test a_command_folds_into_the_state_it_produced ... ok
test a_second_event_type_folds_and_reads_back ... ok
test every_prefix_folds_the_same_way_with_or_without_a_snapshot ... ok
test snapshot_history_and_repository_privacy_interleavings ... ok
test a_snapshot_is_a_cache_and_the_fold_agrees_with_it ... ok
test committed_commands_survive_snapshot_storage_failure ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s

     Running tests/runtime_context.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/runtime_context-9b7b04c1adfa30b4)

running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

     Running tests/strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/strict_inspection-7f2c7cb478588204)

running 5 tests
test sqlite_inspection_missing_sources_never_create ... ok
test sqlite_inspection_identity_corruption_schema_and_uri ... ok
test sqlite_inspection_history_preserves_source ... ok
test sqlite_inspection_native_writer_refuses_without_changes ... ok
test sqlite_inspection_wal_and_journal_refusals_preserve_source ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.51s

   Doc-tests eventlog_conformance

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests eventlog_core

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests eventlog_file

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests eventlog_sqlite

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

error: 2 targets failed:
    `-p eventlog-file --test adversary_strict_inspection`
    `-p eventlog-sqlite --test adversary_strict_inspection`

```

After the fixture-only clone lint correction:
- `cargo test --locked -p eventlog-file -p eventlog-sqlite --test adversary_strict_inspection --no-fail-fast`: exit 101; eight executed, four pass, four behavioral reds unchanged (adversary-final.log).
- `cargo clippy --locked -p eventlog-file -p eventlog-sqlite --test adversary_strict_inspection -- -D warnings`: exit 0 (clippy-final.log).
- `rustfmt --edition 2024 --check crates/eventlog-file/tests/adversary_strict_inspection.rs crates/eventlog-sqlite/tests/adversary_strict_inspection.rs`: exit 0.
- `git diff --check`: exit 0.

This is the bounded four-package lane, not PostgreSQL deployment validation and not the repository's full production gate.

## 3. Findings and exact repair contract

| ID | Location | Verdict / origin | What was measured | What reaches it |
|---|---|---|---|---|
| F1 | crates/eventlog-file/src/inspection.rs:52 | CONFIRMED / undecided | File inspection accepts distinct persisted events with a repeated event identity. Single-case exit 101. | Public FileHistoryInspector on a checksummed but semantically corrupted published-format journal, an input class the strict inspection contract promises to refuse. |
| F2 | crates/eventlog-file/src/journal.rs:111 | CONFIRMED / undecided | File inspection silently discards an unsupported field inside a persisted RecordedEvent envelope. Single-case exit 101. | Public FileHistoryInspector delegates to the native decoder; Frame and Op are strict, but their nested RecordedEvent deserialization ignores unknown fields. |
| F3 | crates/eventlog-sqlite/src/inspection.rs:423 | CONFIRMED / undecided | Both inspectors accept recorded envelopes with empty event identity, event name or actor, and non-object data. Each provider's single-case exit is 101 and aggregates all four mutations. | Public strict inspection of semantically corrupted but physically valid sources; SQLite reuses read_event, File reuses State::replay, and neither path reapplies complete recorded-envelope admission. |

All three are blockers for the promised fail-closed inspection unit. These are runtime refusals missing from the public API's declared corrupt/unsupported-source boundary; the claim is not that ordinary provider writers emit the constructed corruptions. The corruption fixture is the documented inspection use case itself. No judgment-only findings were added.

Origin is undecided for every finding: no assigned base worktree was available and the new inspection API does not exist on the published base. The old helper's permissiveness alone does not establish a measured pre-existing defect for this newly promised strict API. No tree was switched or stashed.

F1 repair: return CorruptSource when distinct selected-tenant records repeat event_id. Do not deduplicate and return a partial history. Preserve valid global-sequence gaps: the ordinary positive fixture has positions 1, 3, 4 because another tenant owns position 2. Global sequence is not per-stream version and must not be required to be gapless. Equal event payloads with different valid identities remain legal.

F2 repair: inspection must reject unknown nested recorded-envelope metadata with CorruptSource, matching the frozen malformed-envelope refusal used by the regression. User-defined JSON keys inside the event's data object remain legal and must be retained. Preserve existing general provider APIs and on-disk formats; use an inspection-specific strict admission boundary around the reused native journal decoding rather than an unrelated second journal parser.

F3 repair: apply common recorded-envelope admission from both inspectors before returning HistoryInspection. At minimum the measured four variants must return CorruptSource on each provider. Reuse the core constructor/metadata constraints where possible, with any recorded-only invariants explicit. Preserve valid field values exactly; do not trim, normalize, infer missing metadata, or silently repair the source. Other constructor rejection classes should be covered at the common boundary so the correction is not four provider-local ad hoc branches.

Expected post-correction measured lane: retain all eight tests, four formerly red cases become green, 102 top-level cases pass, no source entries or bytes change on any refusal. Inspectors remain history-only; nothing here proves blob inventory or archive completeness.

## 4. Controls exercised without a reproduced defect

- Both-provider complete metadata, tenant filtering, global ordering, source-byte and envelope/event result limits, missing sources, source preservation and redaction refusals remain green in the existing lane.
- File native journal checksum/shape integrity, unresolved recovery markers, published journal reuse and native writer-lock controls pass; source was never rewritten by inspection.
- SQLite actual supported cold rollback subset passes; ordinary clean WAL and sidecars, including dangling links, refuse in the existing tests. Fixture preparation is explicit and outside the inspected operation.
- SQLite exact schema and identity checks remain green, including this pass's explicit empty identity and absent-tenant controls.
- SQLite same-process native reader lock survives successful inspection; a separate process cannot start an exclusive writer either before or after inspection.
- The public descriptor registry is exercised in a fresh child process: 64 distinct inode successes, hard-link reuse at capacity, no new descriptor on the 65th-inode refusal, and a held native writer lock survives that refusal.
- Existing inspection-internal same-process and subprocess writer exclusion, SourceBusy serialization, detected source replacement refusal, and lifetime-retained descriptors pass in the full lane. Advisory locks do not promise to prevent arbitrary external filesystem replacement; detected replacement must refuse.
- No selected required test silently skips. Linux-specific cases execute on this Linux host. The child-process controls require an actual one-test pass before their parent passes.

## 5. Outside paths and handback

Public aliases: <cache> is the assigned local cache root; <worktrees> is the managed repository-tree root. Raw logs retain full local paths and are not public-copy artifacts. The report is public-safe. The exact private manifest is `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/outside-paths.txt`.

All persistent scratch files:
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/packages.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/adversary-final.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/file-duplicate-red.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/adversary_sqlite_public_descriptor_bound_keeps_prior_writer_lock.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/adversary_sqlite_success_preserves_preexisting_reader_lock-corrected.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/clippy.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/sqlite-envelope-red.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/file-unknown-red.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/adversary_sqlite_inadmissible_recorded_envelope_is_corruption.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/adversary_sqlite_success_preserves_preexisting_reader_lock.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/file-duplicate-first.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/adversary_sqlite_empty_identity_and_exact_source_cap.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/file-empty-control.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/adversary_sqlite_public_descriptor_bound_keeps_prior_writer_lock-corrected.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/clippy-final.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/file-envelope-red.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/adversary-report.md`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/outside-paths.txt`

All ephemeral fixture writes were descendants of the assigned TMPDIR above and were removed by the test fixtures' normal lifetime cleanup, not by worktree/cache cleanup. Compiler output was written under `<cache>/b10x-target/ekr-eventlog-inspection-20260922`. Worktree lease state was updated only through `worktree hook`, under session `codex-ekr-inspection-adversary`; the handback releases that session only. No live store, upstream source, commit, publication or managed-tree cleanup occurred.

The test files are stable and no compiler is running. Production source remains as handed in. Parent owns correction, any exact refusal-policy amendment, subsequent recording, and the full gate.

```findings
- file: crates/eventlog-file/src/inspection.rs
  line: 52
  category: boundary
  severity: blocker
  verdict: CONFIRMED
  origin: undecided
  message: File inspection accepts distinct persisted events with a repeated event identity.
- file: crates/eventlog-file/src/journal.rs
  line: 111
  category: contract-drift
  severity: blocker
  verdict: CONFIRMED
  origin: undecided
  message: File inspection silently discards an unsupported field inside a persisted RecordedEvent envelope.
- file: crates/eventlog-sqlite/src/inspection.rs
  line: 423
  category: boundary
  severity: blocker
  verdict: CONFIRMED
  origin: undecided
  message: Both inspectors accept recorded envelopes with empty event identity, event name or actor, and non-object data.
```

