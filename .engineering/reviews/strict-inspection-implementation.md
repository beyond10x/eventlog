unit:                   story:strict-read-only-history-inspection — Inspect File and SQLite history without changing source storage
verdict:                green
cases:                  executed 76→94, red 0
origin:                 n/a
wrote-outside-worktree: assigned scratch and dedicated compiler cache; complete inventory below
needs-coordinator:      no unapplied source patch; independent review, full repository/backend proof and publication remain coordinator work

This is a private worker handoff, with absolute scratch paths required by the implementor charter. It is not a source-publication or whole-repository gate receipt.

## 1. Unit and observed limits

Implemented the approved opt-in provider-owned history inspection contract: complete RecordedEvent envelopes for one tenant, optional existing identity, finite source/event/envelope limits, no writer authority, and typed refusals without source repair or creation.

Worker opening was d4620e9ad00535cd100b3cd14b5111b4c13d2e2e. Coordinator-owned contract corrections moved HEAD to 6e9d5b3bfd600ee95bcf687e7044de2c5e9739b1 during implementation. Worker source remains uncommitted. Managed tree: <worktrees>/eventlog/ekr-eventlog-inspection-20260922. Branch: codex/ekr-eventlog-inspection-20260922.

File uses the existing manifest/frame decoder and state fold, retaining a read-only native lock descriptor through the observation. Tests preserve manifest/history/lock/staging/blob bytes and entries. Both native atomic append observations and named busy refusals are exercised.

SQLite support is **Linux, rollback mode, exact published 0.2.1 event/identity schema**. The success fixture explicitly switches journal mode during fixture setup, before freezing its bytes; the inspector never does that. A cleanly closed ordinary published store remains WAL mode and is refused unchanged. This is not complete default-0.2.1 operational compatibility. No operator store was opened.

The initial READ_ONLY plus readonly_shm=1 primitive was measured on an ordinary cleanly closed synthetic store: query failed CannotOpen, created a WAL file, and left it after close. The final reader checks and refuses unsupported journal state before SQLite opening, under an OFD shared lock.

A second measured defect mattered: closing an extra FD after a failed OFD acquisition released an existing same-process SQLite writer's POSIX locks. The final Linux registry retains at most **64 actual source descriptors for process lifetime**, including refused/raced admissions; reserves capacity before open; reuses existing device/inode metadata without redundant open/close; serializes observations with a nonblocking mutex; and explicitly unlocks OFD after SQLite's own connection drops. Exhaustion/concurrent observation returns SourceBusy. This bound and its cost were approved in the coordinator's amended design. Writer-before, reader-before, same-process and subprocess regressions are green. Filesystem replacement remains outside advisory writer authority; detected path/inode replacement is SourceChanged.

History-only coverage exports no blob bodies, command/group/claim receipts, projections, lexical SQL/frame bytes, physical backup proof or replay certification. Existing unrelated evolution work and dependencies were not imported.

## 2. Observed scope

Tracked diff stat at handoff:

```
 Cargo.lock                                         | 19 +++++
 crates/eventlog-conformance/src/lib.rs             |  2 +
 crates/eventlog-core/src/lib.rs                    |  2 +
 crates/eventlog-file/src/journal.rs                | 96 +++++++++++++++++++++-
 crates/eventlog-file/src/lib.rs                    |  2 +
 .../eventlog-postgres/examples/production-proof.rs | 18 ++++
 crates/eventlog-sqlite/Cargo.toml                  |  3 +
 crates/eventlog-sqlite/src/lib.rs                  |  2 +
 8 files changed, 143 insertions(+), 1 deletion(-)
```

Git's tracked stat excludes these six new files; all are in the assigned scope:

| New path | Observed lines |
|---|---:|
| crates/eventlog-core/src/inspection.rs | 54 |
| crates/eventlog-conformance/src/inspection.rs | 107 |
| crates/eventlog-file/src/inspection.rs | 83 |
| crates/eventlog-file/tests/strict_inspection.rs | 244 |
| crates/eventlog-sqlite/src/inspection.rs | 723 |
| crates/eventlog-sqlite/tests/strict_inspection.rs | 220 |

Every inferred source/test path was checked against the checkout before creating it. Existing decoders and tests were inspected. The production roster really is the published-main single example file, not evolution's split required.rs. Source scope matched, with one explicit coordinator-approved extension: target-specific nix =0.30.1 with fs, plus Cargo.lock. Only new nix and cfg_aliases lock entries were added; rusqlite remains 0.37.0 and libsqlite3-sys remains 0.35.0.

No AEP, ESS, design, wave, other active tree, service, operator store, commit or remote publication was written by this worker. Coordinator-owned amendments are separate.

## 3. Red evidence and corrections

The API types and refusal stubs established executable acceptance tests before provider implementation. These were runtime failures, not unresolved-symbol compiler failures. The first File lane executed 4 and failed 4; first SQLite lane executed 4 and failed 4. The later lock-preservation defect failed after an initially plausible OFD implementation and was corrected through the approved bounded-registry contract.

Commands use this exact environment prefix throughout:

```sh
CARGO_TARGET_DIR=<cache>/b10x-target/ekr-eventlog-inspection-20260922 CARGO_BUILD_JOBS=2 TMPDIR=<cache>/ekr-completion-20260922/eventlog-inspection
```

Initial File command, exit 101:

```sh
cargo test --locked -p eventlog-file --test strict_inspection
```

```text
   Compiling eventlog-core v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-core)
   Compiling eventlog-file v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-file)
   Compiling eventlog-conformance v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-conformance)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 2.01s
     Running tests/strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/strict_inspection-bb798948664ee6b0)

running 4 tests
test file_inspection_missing_sources_never_create ... FAILED
test file_inspection_recovery_and_corruption_preserve_source ... FAILED
test file_inspection_native_lock_is_nonblocking ... FAILED
test file_inspection_history_preserves_source ... FAILED

failures:

---- file_inspection_missing_sources_never_create stdout ----

thread 'file_inspection_missing_sources_never_create' (248393) panicked at crates/eventlog-file/tests/strict_inspection.rs:45:5:
assertion `left == right` failed
  left: Err(UnsupportedSource)
 right: Err(MissingSource)
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- file_inspection_recovery_and_corruption_preserve_source stdout ----

thread 'file_inspection_recovery_and_corruption_preserve_source' (248395) panicked at crates/eventlog-file/tests/strict_inspection.rs:65:9:
assertion `left == right` failed
  left: Err(UnsupportedSource)
 right: Err(RecoveryRequired)

---- file_inspection_native_lock_is_nonblocking stdout ----

thread 'file_inspection_native_lock_is_nonblocking' (248394) panicked at crates/eventlog-file/tests/strict_inspection.rs:90:5:
assertion `left == right` failed
  left: Err(UnsupportedSource)
 right: Err(SourceBusy)

---- file_inspection_history_preserves_source stdout ----

thread 'file_inspection_history_preserves_source' (248392) panicked at crates/eventlog-conformance/src/inspection.rs:57:10:
called `Result::unwrap()` on an `Err` value: UnsupportedSource


failures:
    file_inspection_history_preserves_source
    file_inspection_missing_sources_never_create
    file_inspection_native_lock_is_nonblocking
    file_inspection_recovery_and_corruption_preserve_source

test result: FAILED. 0 passed; 4 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.13s

error: test failed, to rerun pass `-p eventlog-file --test strict_inspection`

```

Initial SQLite command, exit 101 (offline mode admitted the explicitly approved cached dependency and updated the lockfile):

```sh
cargo test --offline -p eventlog-sqlite --test strict_inspection
```

```text
     Locking 2 packages to latest Rust 1.91 compatible versions
      Adding cfg_aliases v0.2.2
      Adding nix v0.30.1 (available: v0.31.3)
   Compiling libc v0.2.189
   Compiling cfg_aliases v0.2.2
   Compiling nix v0.30.1
   Compiling getrandom v0.4.3
   Compiling uuid v1.24.1
   Compiling eventlog-core v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-core)
   Compiling tempfile v3.27.0
   Compiling eventlog-sqlite v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-sqlite)
   Compiling eventlog-conformance v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-conformance)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 3.30s
     Running tests/strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/strict_inspection-7f2c7cb478588204)

running 4 tests
test sqlite_inspection_missing_sources_never_create ... FAILED
test sqlite_inspection_wal_and_journal_refusals_preserve_source ... FAILED
test sqlite_inspection_history_preserves_source ... FAILED
test sqlite_inspection_native_writer_refuses_without_changes ... FAILED

failures:

---- sqlite_inspection_missing_sources_never_create stdout ----

thread 'sqlite_inspection_missing_sources_never_create' (266045) panicked at crates/eventlog-sqlite/tests/strict_inspection.rs:50:5:
assertion `left == right` failed
  left: Err(UnsupportedSource)
 right: Err(MissingSource)
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- sqlite_inspection_wal_and_journal_refusals_preserve_source stdout ----

thread 'sqlite_inspection_wal_and_journal_refusals_preserve_source' (266047) panicked at crates/eventlog-sqlite/tests/strict_inspection.rs:68:5:
assertion `left == right` failed
  left: Err(UnsupportedSource)
 right: Err(RecoveryRequired)

---- sqlite_inspection_history_preserves_source stdout ----

thread 'sqlite_inspection_history_preserves_source' (266044) panicked at crates/eventlog-conformance/src/inspection.rs:57:10:
called `Result::unwrap()` on an `Err` value: UnsupportedSource

---- sqlite_inspection_native_writer_refuses_without_changes stdout ----

thread 'sqlite_inspection_native_writer_refuses_without_changes' (266046) panicked at crates/eventlog-sqlite/tests/strict_inspection.rs:100:5:
assertion `left == right` failed
  left: Err(UnsupportedSource)
 right: Err(SourceBusy)


failures:
    sqlite_inspection_history_preserves_source
    sqlite_inspection_missing_sources_never_create
    sqlite_inspection_native_writer_refuses_without_changes
    sqlite_inspection_wal_and_journal_refusals_preserve_source

test result: FAILED. 0 passed; 4 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.14s

error: test failed, to rerun pass `-p eventlog-sqlite --test strict_inspection`

```

Actual lock hazard reproduction, exit 101:

```sh
cargo test --locked -p eventlog-sqlite --lib inspection_refusal_preserves_existing_process_writer_lock -- --nocapture
```

```text
   Compiling eventlog-sqlite v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-sqlite)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.20s
     Running unittests src/lib.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/eventlog_sqlite-21c1b5ae672c7bb8)

running 1 test

thread 'inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock' (286041) panicked at crates/eventlog-sqlite/src/inspection.rs:322:13:
inspector refusal released another connection's lock: 
running 1 test
test inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock ... FAILED

failures:

failures:
    inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

 
thread 'inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock' (286045) panicked at crates/eventlog-sqlite/src/inspection.rs:245:59:
called `Result::unwrap_err()` on an `Ok` value: ()
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock ... FAILED

failures:

failures:
    inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.01s

error: test failed, to rerun pass `-p eventlog-sqlite --lib`

```

Additional deliberate one-line mutations, all restored before final checks:

| Mutation | Exact focused command after cargo test --locked | Own runner result / exit |
|---|---|---|
| File redaction refusal disabled | -p eventlog-file --test strict_inspection file_inspection_identity_redaction_and_unknown_format | 0 passed, 1 failed / 101 |
| OFD acquisition changed from F_RDLCK to F_UNLCK | -p eventlog-sqlite --lib inspection::linux::tests::inspection_ofd_blocks_native_writer_process -- --exact | 0 passed, 1 failed / 101 |
| Descriptor capacity check disabled | -p eventlog-sqlite --lib inspection_descriptor_exhaustion_never_opens_or_closes_source | 0 passed, 1 failed / 101 |

Verbatim mutation output is retained respectively in file-redaction-mutation.log, sqlite-ofd-mutation.log and sqlite-exhaustion-mutation.log. The final complete package run re-executes all corrected cases green. No assertion was removed, ignored or weakened. The writer-first fixture's byte snapshot was moved before BEGIN IMMEDIATE because opening/closing a raw snapshot FD itself releases POSIX locks; the real writer-before-inspection subprocess regression then reproduced and closes the actual implementation defect.

## 4. Final checks and exact case inventory

```sh
cargo test --locked -p eventlog-core -p eventlog-conformance -p eventlog-file -p eventlog-sqlite
```

Own exit: 0. Counts come from the runner's summary lines, baseline-packages.log and final-packages.log.

| Lane | Executed before → after | Exit |
|---|---:|---:|
| Entire four-package suite | 76 → 94 | 0 |
| Core lib | 13 → 13 | 0 |
| Conformance lib | 0 → 0 | 0 |
| File lib | 7 → 7 | 0 |
| File conformance | 11 → 11 | 0 |
| File durability | 7 → 7 | 0 |
| File strict_inspection | absent on base; stub run 0 passed/4 failed → 6 passed/0 failed | 0 |
| SQLite lib | 0 → 7 | 0 |
| SQLite adversary | 1 → 1 | 0 |
| SQLite atomic_groups | 3 → 3 | 0 |
| SQLite conformance | 11 → 11 | 0 |
| SQLite evolution | 3 → 3 | 0 |
| SQLite input_validation_review | 4 → 4 | 0 |
| SQLite legacy_namespaces | 2 → 2 | 0 |
| SQLite repository | 12 → 12 | 0 |
| SQLite runtime_context | 2 → 2 | 0 |
| SQLite strict_inspection | absent on base; stub run 0 passed/4 failed → 5 passed/0 failed | 0 |
| Four doc-test targets | each 0 → 0 | 0 |

No existing lane lost cases. The two new integration targets did not exist on the opening base; the measured red stub runs are their first executions, rather than an invented base invocation.

Full final suite output:

```text
   Compiling eventlog-file v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-file)
   Compiling eventlog-sqlite v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-sqlite)
   Compiling eventlog-conformance v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-conformance)
   Compiling eventlog-core v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-core)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 3.69s
     Running unittests src/lib.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/eventlog_conformance-4c622f71bc4c5a4c)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/eventlog_core-50b45bd4d9def61d)

running 13 tests
test tests::a_command_with_no_events_is_refused ... ok
test tests::an_event_body_must_be_an_object ... ok
test tests::an_identity_that_names_a_person_is_refused ... ok
test tests::a_stream_cannot_be_named_without_a_tenant ... ok
test admission::tests::coordinates_preserve_boundaries_and_grants_cannot_be_reconstructed ... ok
test tests::depth_beyond_the_limit_is_refused ... ok
test tests::effect_evidence_and_boundary_inventory_are_machine_checked ... ok
test tests::effect_inventory_requires_unique_opaque_names_and_complete_coverage ... ok
test tests::effect_metadata_bounds_cover_required_optional_and_outcome_fields ... ok
test tests::effect_metadata_refuses_personal_text_in_every_identifier_and_code ... ok
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

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.52s

     Running tests/conformance.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/conformance-fd829e3c95f2a393)

running 11 tests
test file_claims_contract ... ok
test file_inline_contract ... ok
test file_callback_failure_contract ... ok
test file_scopes_contract ... ok
test file_rebuild_contract ... ok
test file_paging_contract ... ok
test file_projections_contract ... ok
test file_groups_contract ... ok
test file_storage_contract ... ok
test file_public_inputs_contract ... ok
test file_snapshots_contract ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.92s

     Running tests/durability.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/durability-5260062a3e574278)

running 7 tests
test process_writer ... ok
test conflicting_groups_commit_exactly_one_complete_request ... ok
test reopen_preserves_receipts_and_cache_deletion_preserves_authority ... ok
test blob_damage_and_missing_manifest_refuse_reopen ... ok
test redaction_fences_projection_reads_and_new_writes_until_complete_rebuild ... ok
test privacy_removes_active_bytes_and_never_reuses_feed_positions ... ok
test independent_processes_serialize_groups_and_duplicate_keys ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.43s

     Running tests/strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/strict_inspection-42cae3c2aa1fe523)

running 6 tests
test file_inspection_native_lock_is_nonblocking ... ok
test file_inspection_missing_sources_never_create ... ok
test file_inspection_recovery_and_corruption_preserve_source ... ok
test file_inspection_history_preserves_source ... ok
test file_inspection_identity_redaction_and_unknown_format ... ok
test file_inspection_concurrent_append_is_one_observation ... ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.28s

     Running unittests src/lib.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/eventlog_sqlite-21c1b5ae672c7bb8)

running 7 tests
test inspection::linux::tests::inspection_concurrent_call_cannot_unlock_another_observation ... ok
test inspection::linux::tests::inspection_ofd_detects_replaced_source ... ok
test inspection::linux::tests::inspection_ofd_blocks_native_writer_process ... ok
test inspection::linux::tests::inspection_ofd_blocks_native_writers_in_process ... ok
test inspection::linux::tests::inspection_refusal_preserves_existing_process_reader_lock ... ok
test inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock ... ok
test inspection::linux::tests::inspection_descriptor_exhaustion_never_opens_or_closes_source ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.67s

     Running tests/adversary.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary-b843d9a5c895ed07)

running 1 test
test legacy_projection_rows_are_erased_without_reregistering_retired_projectors ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.09s

     Running tests/atomic_groups.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/atomic_groups-800e8bdfb5adc91e)

running 3 tests
test ordered_groups_commit_and_rollback_as_one_unit ... ok
test group_retry_survives_database_reopen ... ok
test concurrent_groups_preserve_order_without_partial_commits ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.93s

     Running tests/conformance.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/conformance-dabec4c1848fb15b)

running 11 tests
test the_claim_rule_holds_in_memory ... ok
test the_inline_projection_exercise_passes_in_memory ... ok
test the_paging_rule_holds_in_memory ... ok
test rebuild_preserves_other_tenants_and_previous_view_on_failure ... ok
test the_projection_exercise_passes_in_memory ... ok
test the_shared_exercise_passes_in_memory ... ok
test scope_reservations_are_atomic_and_confined_in_memory_and_file ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test inline_failure_preserves_all_atomic_state_and_callback_authority ... ok
test the_shared_exercise_passes_on_a_file ... ok
test two_owners_share_a_database_without_sharing_a_table ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.31s

     Running tests/evolution.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/evolution-0e9014dc71a2f0a4)

running 3 tests
test a_body_written_under_an_older_version_still_folds ... ok
test a_version_with_no_upcaster_is_named_rather_than_guessed ... ok
test a_committed_vector_still_folds_when_loaded_from_disk ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/input_validation_review.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/input_validation_review-ebb535db6bb38ed0)

running 4 tests
test append_refuses_deserialized_streams_that_bypass_constructor_checks ... ok
test append_refuses_claim_fields_that_bypass_constructor_checks ... ok
test append_refuses_deserialized_events_that_bypass_constructor_checks ... ok
test public_input_validation_is_atomic_on_sqlite ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

     Running tests/legacy_namespaces.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/legacy_namespaces-984ef597a659f4d9)

running 2 tests
test ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure ... ok
test legacy_erasure_matches_literal_underscores_and_preserves_other_owners ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.24s

     Running tests/repository.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/repository-1e1459dfe611e57b)

running 12 tests
test a_concurrent_write_is_retried_once_and_then_refused ... ok
test a_second_event_type_folds_and_reads_back ... ok
test a_command_that_decided_nothing_writes_nothing ... ok
test a_stale_snapshot_schema_is_discarded_rather_than_trusted ... ok
test a_command_folds_into_the_state_it_produced ... ok
test a_retried_command_is_answered_not_written_again ... ok
test an_aggregate_stays_foldable_after_an_erasure ... ok
test a_delayed_unproven_snapshot_cannot_restore_redacted_state ... ok
test every_prefix_folds_the_same_way_with_or_without_a_snapshot ... ok
test snapshot_history_and_repository_privacy_interleavings ... ok
test a_snapshot_is_a_cache_and_the_fold_agrees_with_it ... ok
test committed_commands_survive_snapshot_storage_failure ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s

     Running tests/runtime_context.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/runtime_context-9b7b04c1adfa30b4)

running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

     Running tests/strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/strict_inspection-7f2c7cb478588204)

running 5 tests
test sqlite_inspection_history_preserves_source ... ok
test sqlite_inspection_identity_corruption_schema_and_uri ... ok
test sqlite_inspection_missing_sources_never_create ... ok
test sqlite_inspection_native_writer_refuses_without_changes ... ok
test sqlite_inspection_wal_and_journal_refusals_preserve_source ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.41s

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


```

Other final commands, each with its own exit status:

```sh
cargo fmt --all --check
# exit 0, empty output
cargo clippy --locked -p eventlog-core -p eventlog-conformance -p eventlog-file -p eventlog-sqlite --all-targets -- -D warnings
# exit 0
cargo check --locked -p eventlog-postgres --example production-proof
# exit 0
git diff --check
# exit 0, empty output
```

Clippy output:

```text
    Checking eventlog-sqlite v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-sqlite)
    Checking eventlog-file v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-file)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.82s

```

Changed production example compilation:

```text
   Compiling libc v0.2.189
    Checking typenum v1.20.1
   Compiling syn v3.0.3
   Compiling serde_core v1.0.229
    Checking hybrid-array v0.4.14
    Checking rand_core v0.10.1
   Compiling getrandom v0.4.3
    Checking cmov v0.5.4
    Checking ctutils v0.4.2
    Checking crypto-common v0.2.2
    Checking block-buffer v0.12.1
    Checking const-oid v0.10.2
   Compiling ring v0.17.14
    Checking digest v0.11.3
    Checking cpufeatures v0.3.0
    Checking tinyvec_macros v0.1.1
    Checking bytes v1.12.1
    Checking memchr v2.8.3
    Checking tinyvec v1.12.0
    Checking generic-array v0.14.7
    Checking deranged v0.5.8
    Checking serde_json v1.0.151
    Checking time v0.3.55
    Checking unicode-normalization v0.1.25
    Checking chacha20 v0.10.1
    Checking uuid v1.24.1
   Compiling tokio-macros v2.7.2
    Checking mio v1.2.2
    Checking getrandom v0.2.17
    Checking socket2 v0.6.5
   Compiling syn v2.0.119
    Checking unicode-properties v0.1.4
   Compiling parking_lot_core v0.9.12
    Checking futures-core v0.3.34
    Checking unicode-bidi v0.3.18
    Checking untrusted v0.9.0
    Checking futures-sink v0.3.34
    Checking zeroize v1.9.0
    Checking rustls-pki-types v1.15.1
   Compiling der_derive v0.7.3
    Checking stringprep v0.1.5
    Checking tokio v1.53.1
    Checking rand v0.10.2
    Checking crypto-common v0.1.7
    Checking block-buffer v0.10.4
    Checking hmac v0.13.0
    Checking sha2 v0.11.0
    Checking md-5 v0.11.0
    Checking const-oid v0.9.6
    Checking base64 v0.22.1
    Checking siphasher v1.0.3
    Checking fallible-iterator v0.2.0
    Checking scopeguard v1.2.0
    Checking flagset v0.4.7
    Checking byteorder v1.5.0
   Compiling rustls v0.23.43
    Checking der v0.7.10
    Checking postgres-protocol v0.6.12
    Checking lock_api v0.4.14
    Checking phf_shared v0.13.1
    Checking digest v0.10.7
    Checking rustls-webpki v0.103.15
   Compiling serde_derive v1.0.229
   Compiling thiserror-impl v2.0.20
    Checking futures-task v0.3.34
    Checking utf8parse v0.2.2
    Checking subtle v2.6.1
    Checking serde v1.0.229
    Checking anstyle-parse v1.0.0
    Checking futures-util v0.3.34
    Checking sha2 v0.10.9
    Checking thiserror v2.0.20
    Checking phf v0.13.1
    Checking parking_lot v0.12.5
    Checking spki v0.7.3
    Checking postgres-types v0.2.14
    Checking tokio-util v0.7.19
    Checking futures-channel v0.3.34
   Compiling async-trait v0.1.92
    Checking whoami v2.1.3
    Checking percent-encoding v2.3.2
    Checking log v0.4.33
    Checking is_terminal_polyfill v1.70.2
    Checking colorchoice v1.0.5
    Checking anstyle-query v1.1.5
    Checking anstyle v1.0.14
    Checking tokio-postgres v0.7.18
    Checking anstream v1.0.0
    Checking x509-cert v0.2.5
    Checking eventlog-core v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-core)
    Checking tokio-rustls v0.26.5
    Checking strsim v0.11.1
   Compiling heck v0.5.0
    Checking clap_lex v1.1.0
   Compiling clap_derive v4.6.4
    Checking clap_builder v4.6.6
    Checking tokio-postgres-rustls v0.14.0
    Checking eventlog-postgres v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-postgres)
    Checking clap v4.6.6
    Checking eventlog-conformance v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-conformance)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 15.89s

```

These 18 names were each found as executed ok in the full runner output and were added to the existing production roster without deleting any prior name:

- `file_inspection_history_preserves_source`
- `file_inspection_missing_sources_never_create`
- `file_inspection_recovery_and_corruption_preserve_source`
- `file_inspection_native_lock_is_nonblocking`
- `file_inspection_identity_redaction_and_unknown_format`
- `file_inspection_concurrent_append_is_one_observation`
- `sqlite_inspection_history_preserves_source`
- `sqlite_inspection_missing_sources_never_create`
- `sqlite_inspection_wal_and_journal_refusals_preserve_source`
- `sqlite_inspection_native_writer_refuses_without_changes`
- `sqlite_inspection_identity_corruption_schema_and_uri`
- `inspection::linux::tests::inspection_ofd_blocks_native_writers_in_process`
- `inspection::linux::tests::inspection_ofd_blocks_native_writer_process`
- `inspection::linux::tests::inspection_ofd_detects_replaced_source`
- `inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock`
- `inspection::linux::tests::inspection_refusal_preserves_existing_process_reader_lock`
- `inspection::linux::tests::inspection_concurrent_call_cannot_unlock_another_observation`
- `inspection::linux::tests::inspection_descriptor_exhaustion_never_opens_or_closes_source`

## 5. Deliberate limits and next owner

- No live-WAL reader: the measured default 0.2.1 mode is explicitly refused, not silently checkpointed or opened.
- No unsafe code, custom VFS, unchecked immutable mode, fork, unbounded FD leak, source copy/reset, schema migration, blob movement or recovery.
- No full PostgreSQL/hosted-role production, comparative or restart execution by this worker. Compiling the updated example is not backend proof. The coordinator's gate and independent adversary still own those obligations.
- No commit, tag, release or downstream dependency update. The coordinator verifies published main and required checks before all EKR pins advance together.
- Disk was 25 GB before the baseline build and 22 GB at final package verification, above the 10 GB pause threshold.
- The brief's dedicated build target overrides the implementor skill's default in-tree target; it is unique to this unit and no other source tree was compiled into it.

## 6. Outside-worktree writes

All manually produced unit artifacts are under the assigned private scratch directory:

- <cache>/ekr-completion-20260922/eventlog-inspection/baseline-packages.log
- <cache>/ekr-completion-20260922/eventlog-inspection/file-red.log
- <cache>/ekr-completion-20260922/eventlog-inspection/sqlite-red.log
- <cache>/ekr-completion-20260922/eventlog-inspection/sqlite-primitive.rs
- <cache>/ekr-completion-20260922/eventlog-inspection/sqlite-primitive
- <cache>/ekr-completion-20260922/eventlog-inspection/sqlite-primitive.log
- <cache>/ekr-completion-20260922/eventlog-inspection/providers-first-green.log
- <cache>/ekr-completion-20260922/eventlog-inspection/file-green.log
- <cache>/ekr-completion-20260922/eventlog-inspection/ofd-first.log
- <cache>/ekr-completion-20260922/eventlog-inspection/ofd-existing-writer-red.log
- <cache>/ekr-completion-20260922/eventlog-inspection/format-first.log
- <cache>/ekr-completion-20260922/eventlog-inspection/clippy-first.log
- <cache>/ekr-completion-20260922/eventlog-inspection/clippy-progress.log
- <cache>/ekr-completion-20260922/eventlog-inspection/providers-green.log
- <cache>/ekr-completion-20260922/eventlog-inspection/ofd-registry-green.log
- <cache>/ekr-completion-20260922/eventlog-inspection/file-redaction-mutation.log
- <cache>/ekr-completion-20260922/eventlog-inspection/sqlite-ofd-mutation.log
- <cache>/ekr-completion-20260922/eventlog-inspection/sqlite-exhaustion-mutation.log
- <cache>/ekr-completion-20260922/eventlog-inspection/final-packages.log
- <cache>/ekr-completion-20260922/eventlog-inspection/final-format.log
- <cache>/ekr-completion-20260922/eventlog-inspection/final-clippy.log
- <cache>/ekr-completion-20260922/eventlog-inspection/final-production-example.log
- <cache>/ekr-completion-20260922/eventlog-inspection/implementation-report.md

Compiler output is retained under <cache>/b10x-target/ekr-eventlog-inspection-20260922 as instructed. Rust test fixtures used TMPDIR=<cache>/ekr-completion-20260922/eventlog-inspection; tempfile cleanup occurred only for synthetic test-owned fixtures. No operator source was copied or cleaned. The coordinator's lock-contract.md in the same scratch directory was not written by this worker.

Managed worktree lease: codex-ekr-eventlog-inspector-implementor. It is released at handoff; the coordinator owns review, commits, publication and eventual cleanup.

