---
format: aep.planning-md/1
id: verification-report:feature-implementation-20260909
kind: verification-report
status: draft
title: Feature integration runner receipts
relations:
- reviews: story:integrate-reviewed-feature-contracts
revision: 2
---
unit:                   task:integrate-reviewed-feature-contracts — Integrate reviewed guarded refusal and effect contracts
verdict:                green
cases:                  executed 73→78, red 2
origin:                 n/a
wrote-outside-worktree: none
needs-coordinator:      yes — README/CHANGELOG prose below; publish commit and preserve scratch evidence

1. Unit and acceptance

Port code-only guarded refusal and effect vocabulary commits, enforce opaque effect metadata, prove shared guard rollback, and provide a validated semantic ESS home. Implemented commit 096cdbe77c0adf1c8ade9b3b3985fcdbbb8cfcd9 on impl/integrate-reviewed-feature-contracts. Author and committer both b10x-bot[bot] <316511680+b10x-bot[bot]@users.noreply.github.com>. No backend, Cargo, historical story or planning journal changes.

2. Diff
commit 096cdbe77c0adf1c8ade9b3b3985fcdbbb8cfcd9
Author: b10x-bot[bot] <316511680+b10x-bot[bot]@users.noreply.github.com>

    feat: integrate guarded refusals and opaque effect evidence contracts

 crates/eventlog-conformance/src/lib.rs |  40 +++-
 crates/eventlog-core/src/lib.rs        | 367 ++++++++++++++++++++++++++++++++-
 crates/eventlog-core/src/projection.rs |   4 +-
 ess/effects/domains/effects.yaml       |  67 ++++++
 ess/effects/system.yaml                |   5 +
 5 files changed, 476 insertions(+), 7 deletions(-)

3. Red evidence

New conformance contract was applied before adding the missing public enum variant. Command: CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo check -p eventlog-conformance --locked. Exit 101. This is compile-time API absence, not runtime rollback evidence.
    Blocking waiting for file lock on package cache
    Blocking waiting for file lock on package cache
   Compiling serde_core v1.0.229
    Checking typenum v1.20.1
   Compiling syn v3.0.3
   Compiling libc v0.2.189
   Compiling getrandom v0.4.3
    Checking cfg-if v1.0.4
    Checking generic-array v0.14.7
    Checking crypto-common v0.1.7
    Checking block-buffer v0.10.4
    Checking digest v0.10.7
   Compiling thiserror-impl v2.0.20
   Compiling serde_derive v1.0.229
    Checking deranged v0.5.8
    Checking zmij v1.0.23
    Checking itoa v1.0.18
    Checking memchr v2.8.3
    Checking cpufeatures v0.2.17
    Checking powerfmt v0.2.0
    Checking num-conv v0.2.2
    Checking time-core v0.1.9
    Checking serde_json v1.0.151
    Checking sha2 v0.10.9
    Checking time v0.3.55
    Checking serde v1.0.229
    Checking thiserror v2.0.20
    Checking uuid v1.24.1
    Checking eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
    Checking eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
error[E0599]: no variant named `GuardRefused` found for enum `EventLogError`
   --> crates/eventlog-conformance/src/lib.rs:856:28
    |
856 |         Err(EventLogError::GuardRefused { code }) => code,
    |                            ^^^^^^^^^^^^ variant not found in `EventLogError`

error[E0599]: no variant named `GuardRefused` found for enum `EventLogError`
   --> crates/eventlog-conformance/src/lib.rs:618:43
    |
618 |                 return Err(EventLogError::GuardRefused {
    |                                           ^^^^^^^^^^^^ variant not found in `EventLogError`

For more information about this error, try `rustc --explain E0599`.
error: could not compile `eventlog-conformance` (lib) due to 2 previous errors

After porting original effect code and adding negative cases, before hardening its validation: CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p eventlog-core --locked. Exit 101, 10 passed; 2 failed.
   Compiling typenum v1.20.1
   Compiling serde_core v1.0.229
   Compiling libc v0.2.189
   Compiling memchr v2.8.3
   Compiling thiserror v2.0.20
   Compiling generic-array v0.14.7
   Compiling getrandom v0.4.3
   Compiling block-buffer v0.10.4
   Compiling crypto-common v0.1.7
   Compiling uuid v1.24.1
   Compiling digest v0.10.7
   Compiling sha2 v0.10.9
   Compiling deranged v0.5.8
   Compiling serde v1.0.229
   Compiling serde_json v1.0.151
   Compiling time v0.3.55
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
    Finished `test` profile [unoptimized] target(s) in 4.57s
     Running unittests src/lib.rs (target/debug/deps/eventlog_core-01691989ec7aec74)

running 12 tests
test tests::a_command_with_no_events_is_refused ... ok
test admission::tests::coordinates_preserve_boundaries_and_grants_cannot_be_reconstructed ... ok
test tests::a_stream_cannot_be_named_without_a_tenant ... ok
test tests::an_identity_that_names_a_person_is_refused ... ok
test tests::effect_evidence_and_boundary_inventory_are_machine_checked ... ok
test tests::an_event_body_must_be_an_object ... ok
test tests::depth_beyond_the_limit_is_refused ... ok
test tests::effect_metadata_bounds_cover_required_optional_and_outcome_fields ... ok
test tests::effect_inventory_requires_unique_opaque_names_and_complete_coverage ... FAILED
test tests::effect_metadata_refuses_personal_text_in_every_identifier_and_code ... FAILED
test tests::the_same_body_hashes_the_same_way ... ok
test tests::effect_stages_preserve_wire_shape_and_explicit_coverage ... ok

failures:

---- tests::effect_inventory_requires_unique_opaque_names_and_complete_coverage stdout ----

thread 'tests::effect_inventory_requires_unique_opaque_names_and_complete_coverage' (730312) panicked at crates/eventlog-core/src/lib.rs:1168:13:
admitted "Jane Smith"
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- tests::effect_metadata_refuses_personal_text_in_every_identifier_and_code stdout ----

thread 'tests::effect_metadata_refuses_personal_text_in_every_identifier_and_code' (730314) panicked at crates/eventlog-core/src/lib.rs:1101:17:
field 0 admitted personal text "Jane Smith"


failures:
    tests::effect_inventory_requires_unique_opaque_names_and_complete_coverage
    tests::effect_metadata_refuses_personal_text_in_every_identifier_and_code

test result: FAILED. 10 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p eventlog-core --lib`

Runtime guard-code mutation changed the guard's returned code to tally.other_refusal while retaining the exact-code assertion. PostgreSQL: EVENTLOG_TEST_POSTGRES_URL=<disposable-fixture-url> CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --workspace --locked the_inline_projection_exercise. Exit 101. SQLite: CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p eventlog-sqlite --locked the_inline_projection_exercise. Exit 101. Mutation was reverted before final gate.
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 2.38s
     Running unittests src/lib.rs (target/debug/deps/eventlog_conformance-81307400d91e72d5)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_core-6e06060c4a60d048)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 12 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_postgres-69070d9c12dde61c)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out; finished in 0.00s

     Running tests/adversary.rs (target/debug/deps/adversary-ea79296f5cdc97de)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 1 test
test the_inline_projection_exercise_passes_on_postgresql ... FAILED

failures:

---- the_inline_projection_exercise_passes_on_postgresql stdout ----

thread 'the_inline_projection_exercise_passes_on_postgresql' (767098) panicked at crates/eventlog-conformance/src/lib.rs:859:5:
assertion `left == right` failed
  left: "tally.other_refusal"
 right: "tally.limit_reached"
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    the_inline_projection_exercise_passes_on_postgresql

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 25 filtered out; finished in 0.26s

error: test failed, to rerun pass `-p eventlog-postgres --test conformance`
   Compiling syn v3.0.3
   Compiling tempfile v3.27.0
   Compiling serde_derive v1.0.229
   Compiling thiserror-impl v2.0.20
   Compiling tokio-macros v2.7.2
   Compiling tokio v1.53.1
   Compiling thiserror v2.0.20
   Compiling serde v1.0.229
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
    Finished `test` profile [unoptimized] target(s) in 6.26s
     Running unittests src/lib.rs (target/debug/deps/eventlog_sqlite-c37871d8445dc672)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/adversary.rs (target/debug/deps/adversary-5aac8d442369ea87)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s

     Running tests/conformance.rs (target/debug/deps/conformance-b394245794afa3a0)

running 1 test
test the_inline_projection_exercise_passes_in_memory ... FAILED

failures:

---- the_inline_projection_exercise_passes_in_memory stdout ----

thread 'the_inline_projection_exercise_passes_in_memory' (777173) panicked at crates/eventlog-conformance/src/lib.rs:859:5:
assertion `left == right` failed
  left: "tally.other_refusal"
 right: "tally.limit_reached"
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    the_inline_projection_exercise_passes_in_memory

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 10 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p eventlog-sqlite --test conformance`

4. Green verification

Command: EVENTLOG_TEST_POSTGRES_URL=<disposable-fixture-url> CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 bash scripts/gate.sh
Exit 0. Ordinary workspace gate, not production-proof. Root owns final combined production proof. Full runner output:
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 0.47s
     Running `target/debug/examples/gate`
gate: cargo test --workspace --locked
   Compiling ring v0.17.14
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
   Compiling rustls v0.23.43
   Compiling rustls-webpki v0.103.15
   Compiling tokio-rustls v0.26.5
   Compiling tokio-postgres-rustls v0.14.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 3.54s
     Running unittests src/lib.rs (target/debug/deps/eventlog_conformance-81307400d91e72d5)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_core-6e06060c4a60d048)

running 12 tests
test admission::tests::coordinates_preserve_boundaries_and_grants_cannot_be_reconstructed ... ok
test tests::a_command_with_no_events_is_refused ... ok
test tests::an_event_body_must_be_an_object ... ok
test tests::a_stream_cannot_be_named_without_a_tenant ... ok
test tests::an_identity_that_names_a_person_is_refused ... ok
test tests::depth_beyond_the_limit_is_refused ... ok
test tests::effect_evidence_and_boundary_inventory_are_machine_checked ... ok
test tests::effect_inventory_requires_unique_opaque_names_and_complete_coverage ... ok
test tests::effect_metadata_refuses_personal_text_in_every_identifier_and_code ... ok
test tests::effect_metadata_bounds_cover_required_optional_and_outcome_fields ... ok
test tests::the_same_body_hashes_the_same_way ... ok
test tests::effect_stages_preserve_wire_shape_and_explicit_coverage ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_postgres-69070d9c12dde61c)

running 8 tests
test pool::tests::idle_checkout_has_one_coherent_owner ... ok
test pool::tests::shutdown_does_not_recycle_a_returning_connection_after_close ... ok
test pool::tests::reusable_retirement_preserves_two_connection_four_waiter_snapshot ... ok
test pool::tests::closed_idle_driver_is_joined_before_replacement_connects ... ok
test pool::tests::quarantine_retirement_cannot_count_a_replacement_twice ... ok
test pool::tests::shutdown_cancels_waiters_and_drains_quarantine ... ok
test pool::tests::zero_waiter_pool_reuses_and_refuses_without_queueing ... ok
test pool::tests::queued_cancellation_timeout_and_granted_cancellation_release_capacity ... ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.11s

     Running tests/adversary.rs (target/debug/deps/adversary-ea79296f5cdc97de)

running 2 tests
test schema_admission_refuses_inherited_event_children ... ok
test unrelated_xmin_cannot_make_committed_positions_noncontiguous ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.36s

     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 26 tests
test isolated_transport_cannot_hide_a_remote_address_behind_localhost ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test a_feed_cursor_cannot_pass_an_inflight_lower_position_with_a_newer_xid ... ok
test absent_deployment_scope_races_across_tenants_and_clients ... ok
test a_reader_never_skips_an_event_that_committed_late ... ok
test caught_reservation_cancellation_cannot_commit_an_unchecked_append ... ok
test incomplete_companion_schema_is_refused_before_serving ... ok
test independent_first_appends_return_contract_outcomes ... ok
test inline_failure_preserves_all_atomic_state_and_callback_authority ... ok
test killed_projector_restarts_competing_workers_without_partial_view_or_cursor ... ok
test legacy_populated_schema_migrates_atomically_and_unknown_checksums_refuse ... ok
test lost_commit_response_reconnects_to_exact_durable_receipt ... ok
test missing_scope_bootstrap_is_atomic_across_independent_processes ... ok
test rebuild_preserves_other_tenants_and_previous_view_on_failure ... ok
test registration_freezes_and_pool_refuses_bounded_overload ... ok
test schema_admission_refuses_triggers_policies_generation_and_foreign_sequences ... ok
test pool_observation_preserves_two_connections_and_four_waiters ... ok
test scope_reservations_are_atomic_and_confined ... ok
test public_queue_cancellation_and_broken_idle_reclaim_exact_capacity ... ok
test the_claim_rule_holds_on_postgresql ... ok
test shutdown_cancellation_preserves_public_pool_lifetimes ... ok
test the_inline_projection_exercise_passes_on_postgresql ... ok
test the_paging_rule_holds_on_postgresql ... ok
test the_projection_exercise_passes_on_postgresql ... ok
test the_shared_exercise_passes_on_postgresql ... ok
test verified_tls_requires_matching_server_and_separate_application_role ... ok

test result: ok. 26 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.39s

     Running tests/runtime_context.rs (target/debug/deps/runtime_context-936f9f892482b809)

running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.59s

     Running unittests src/lib.rs (target/debug/deps/eventlog_sqlite-4118c9a17598b586)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/adversary.rs (target/debug/deps/adversary-231eab852eb00512)

running 1 test
test legacy_projection_rows_are_erased_without_reregistering_retired_projectors ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/conformance.rs (target/debug/deps/conformance-9cca51e8c05339a7)

running 11 tests
test the_claim_rule_holds_in_memory ... ok
test the_inline_projection_exercise_passes_in_memory ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test the_paging_rule_holds_in_memory ... ok
test rebuild_preserves_other_tenants_and_previous_view_on_failure ... ok
test the_projection_exercise_passes_in_memory ... ok
test the_shared_exercise_passes_in_memory ... ok
test scope_reservations_are_atomic_and_confined_in_memory_and_file ... ok
test the_shared_exercise_passes_on_a_file ... ok
test inline_failure_preserves_all_atomic_state_and_callback_authority ... ok
test two_owners_share_a_database_without_sharing_a_table ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s

     Running tests/evolution.rs (target/debug/deps/evolution-1948f69ba95fd731)

running 3 tests
test a_version_with_no_upcaster_is_named_rather_than_guessed ... ok
test a_body_written_under_an_older_version_still_folds ... ok
test a_committed_vector_still_folds_when_loaded_from_disk ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/legacy_namespaces.rs (target/debug/deps/legacy_namespaces-a6f3ff8640f3614e)

running 2 tests
test ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure ... ok
test legacy_erasure_matches_literal_underscores_and_preserves_other_owners ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

     Running tests/repository.rs (target/debug/deps/repository-236462b0c2b57479)

running 9 tests
test a_command_that_decided_nothing_writes_nothing ... ok
test a_command_folds_into_the_state_it_produced ... ok
test a_concurrent_write_is_retried_once_and_then_refused ... ok
test a_second_event_type_folds_and_reads_back ... ok
test an_aggregate_stays_foldable_after_an_erasure ... ok
test a_stale_snapshot_schema_is_discarded_rather_than_trusted ... ok
test a_retried_command_is_answered_not_written_again ... ok
test every_prefix_folds_the_same_way_with_or_without_a_snapshot ... ok
test a_snapshot_is_a_cache_and_the_fold_agrees_with_it ... ok

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

     Running tests/runtime_context.rs (target/debug/deps/runtime_context-e6b1511c66680dc7)

running 2 tests
test every_store_method_completes_on_a_multi_thread_runtime ... ok
test every_store_method_completes_on_a_current_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

   Doc-tests eventlog_conformance

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests eventlog_core

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests eventlog_postgres

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests eventlog_sqlite

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

gate: cargo fmt --all --check
gate: cargo clippy --workspace --all-targets --locked -- -D warnings
    Checking eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
    Checking ring v0.17.14
    Checking rustls-webpki v0.103.15
    Checking eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
    Checking eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
    Checking rustls v0.23.43
    Checking tokio-rustls v0.26.5
    Checking tokio-postgres-rustls v0.14.0
    Checking eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 2.42s
gate: green

Per-lane executed counts from baseline.log and gate.log:
- core units: 7 → 12, exit 0.
- postgres units: 8 → 8, exit 0.
- postgres adversary: 2 → 2, exit 0.
- postgres conformance: 26 → 26, exit 0 (strengthened existing shared exercise).
- postgres runtime_context: 2 → 2, exit 0.
- sqlite adversary: 1 → 1, exit 0.
- sqlite conformance: 11 → 11, exit 0 (strengthened existing shared exercise).
- sqlite evolution: 3 → 3, exit 0.
- sqlite legacy_namespaces: 2 → 2, exit 0.
- sqlite repository: 9 → 9, exit 0.
- sqlite runtime_context: 2 → 2, exit 0.
- conformance/sqlite unit targets and doc-test targets contain zero cases before/after.
- fmt and workspace clippy checks: exit 0, from final gate's own execution/output.

ESS: installed ess 0.9.2 refuses `ess specify` as an unknown subcommand. Its advertised command is `ess validate --path ess/effects`, exit 0:
eventlog v1 — 2 file(s), valid
`ess compile --path ess/effects --format json` also exited 0; emitted canonical IR retained in ess-compiled.json.

5. Deliberately outside this unit and precise limitation

README/CHANGELOG prose for coordinator:
- eventlog-core adds EventLogError::GuardRefused { code }, preserving owner-selected stable codes while rolling back refused appends and guard projection writes on both backends.
- eventlog-core adds EffectStage, EffectEvidence, EffectBoundaryCoverage and validate_effect_boundary_inventory. Metadata shares existing CommandMeta attribution; opaque bounded identifiers and codes reject names/addresses. Owners explicitly validate before embedding metadata in their own events; backends do not interpret owner event bodies.
- ESS effects semantic vocabulary documents those values; actual serde stage wire shape remains the existing internally tagged object and is exhaustively tested for all five variants.

No public field/variant names from original feature changed. Error enum addition can affect consumer exhaustive matches. Stronger metadata validation intentionally refuses spaces/@ in all effect identifiers/codes and boundary names, addressing the whole class: attempt, operation, authority, optional grant/retry/downstream, failure/refusal code, inventory boundary.

ESS exact wire projection is unsupported: `/home/timo/beyond10x/ess/crates/generate/ess-gen/src/schema.rs:68` defines adjacent union content keys; `/home/timo/beyond10x/ess/crates/specify/ess-domain/src/types.rs:1289` tests rejection of empty struct variants. Rust's internally tagged mixed unit/struct EffectStage cannot be represented as that ESS union without changing public wire bytes. With coordinator agreement, the semantic model uses a stage-name enum plus optional outcome code and explicitly documents the gap. No schema generator is run or exact wire equivalence asserted. Types remain values; no entity identities, lifecycles or external ownership/cardinality are invented.

No full TLS production-proof or comparative/restart proof was run for this unit. Final combined release gate belongs to coordinator. No branches were pushed by worker; coordinator owns publication and worktree cleanup. Retain these scratch logs before finishing the tree. No container removed.

6. Outside-tree writes

None (managed worktree lease state is maintained by worktree CLI). All task scratch and build output remains under assigned worktree target/. Own lease released at handoff; adversary may have separate lease.
