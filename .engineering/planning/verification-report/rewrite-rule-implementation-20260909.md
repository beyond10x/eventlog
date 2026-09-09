---
format: aep.planning-md/1
id: verification-report:rewrite-rule-implementation-20260909
kind: verification-report
status: draft
title: Rewrite-rule admission repair receipts
relations:
- reviews: story:reject-postgres-rewrite-rules
revision: 1
---
unit: story:reject-postgres-rewrite-rules — Reject rewrite rules during PostgreSQL admission
verdict: green
cases: executed88→92, red4
origin: n/a
wrote-outside-worktree: none
needs-coordinator: yes — add four mandatory case names to the coordinator-owned production-proof roster

## Unit and acceptance
Every owned durable or declared projection table must reject PostgreSQL rewrite rules during admission/registration; supported schema and exact durable retry receipts continue to work.

The prior tests-only patch was verified against eac42e80b561ce50239a1d7391868cd186ab2fa256df1025aed760d1a29e659d, preserved, restored from the old checkout, and cleanly3-way applied to exact integrated snapshot base5c1ff2690f07ec0b91f14cce307fff2a6b65ddca. All snapshot tests remain present. The active owner artifact is maintained in the coordinator store; this worker never writes AEP.

## Actual diff
 crates/eventlog-postgres/src/schema.rs        |  19 ++-
 crates/eventlog-postgres/tests/conformance.rs | 207 ++++++++++++++++++++++++++
 2 files changed, 225 insertions(+), 1 deletion(-)

The shared shape validator now rejects pg_rewrite entries. Explicit migration of a declared projection also routes its created/existing table through that validator; CREATE TABLE IF NOT EXISTS previously returned success without checking hidden behavior. No DDL, public API, or persisted shape changed.

Class enumeration: all11 actual durable tables are enumerated from the fixture catalog and exercised, including new snapshot metadata; declared projection migration, create_projections, and inline registration are exercised; hosted verified-TLS application-role open is exercised. The rule-free control proves an ordinary same-command retry returns the exact original event/receipt. No source file outside schema.rs and tests/conformance.rs was changed.

## Test-first red records
Prior original finding records remain ../rewrite-rule-red.log and ../hosted-rewrite-rule-red.log: exact local/hosted cases each failed before implementation.
On the integrated base, new class cases and hosted case were then run before the guard existed:
Command: cargo test -p eventlog-postgres --test conformance --locked rewrite_rules -- --nocapture --test-threads=1
Exit101; executed3, 0passed/3failed. This filter excludes the separately retained original local-case name. Full output:
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-core)
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-conformance)
    Finished `test` profile [unoptimized] target(s) in 4.89s
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 3 tests
test hosted_schema_admission_refuses_rewrite_rules ... 
thread 'hosted_schema_admission_refuses_rewrite_rules' (1207056) panicked at crates/eventlog-postgres/tests/conformance.rs:160:13:
verified TLS/application-role admission accepted a command-discarding rewrite rule
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
FAILED
test rewrite_rules_are_rejected_during_projection_migration_and_registration ... 
thread 'rewrite_rules_are_rejected_during_projection_migration_and_registration' (1207124) panicked at crates/eventlog-postgres/tests/conformance.rs:69:5:
migration must refuse rewrite rules on declared projections: Ok(())
FAILED
test rewrite_rules_are_rejected_on_every_durable_table ... 
thread 'rewrite_rules_are_rejected_on_every_durable_table' (1207157) panicked at crates/eventlog-postgres/tests/conformance.rs:35:17:
rewrite rule on durable table rewrite_classes_blobs was admitted
FAILED

failures:

failures:
    hosted_schema_admission_refuses_rewrite_rules
    rewrite_rules_are_rejected_during_projection_migration_and_registration
    rewrite_rules_are_rejected_on_every_durable_table

test result: FAILED. 0 passed; 3 failed; 0 ignored; 0 measured; 33 filtered out; finished in 0.82s

error: test failed, to rerun pass `-p eventlog-postgres --test conformance`

## Green and mutation records
Focused command: cargo test -p eventlog-postgres --test conformance --locked rules -- --nocapture --test-threads=1
Exit0; executed4, 4passed/0failed; exact log focused-green.log. Both original and both class cases selected.
Mutation command: same focused command, changing only the shared pg_rewrite predicate to AND false while retaining its table parameter. Exit101; executed4, 0passed/4failed, exact log mutant.log. This independently catches hosted admission, every durable table, projection migration, and duplicate durable retries. Predicate restored before final gate.
Full command: bash scripts/gate.sh --production-proof
Exit0; executed88→92 using coordinator baseline88 and runner total92; failed0, skipped0, missing_required_cases=[]. PostgreSQL conformance32→36; unaffected lanes retained their expected counts. Workspace formatting and clippy exit0. Exact gate output follows; underlying combined runner output is production-proof.raw.log.
Environment: TMPDIR=$PWD/target/review-scratch; CARGO_BUILD_JOBS=4; CARGO_PROFILE_DEV_DEBUG=0; CARGO_PROFILE_TEST_DEBUG=0; EVENTLOG_REQUIRE_POSTGRES=1; dedicated PostgreSQL17.6 container eventlog-catchup-20260909 on32875; CA in ../tls/ca.crt with dedicated application-role URL. Existing rustup-update.log earlier this same session records stable1.98.1 current.
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 0.61s
     Running `target/debug/examples/gate --production-proof`
gate: cargo run --locked -p eventlog-postgres --example production-proof
   Compiling ring v0.17.14
   Compiling rustls v0.23.43
   Compiling rustls-webpki v0.103.15
   Compiling tokio-rustls v0.26.5
   Compiling tokio-postgres-rustls v0.14.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 1.66s
     Running `target/debug/examples/production-proof`

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 12 tests
test admission::tests::coordinates_preserve_boundaries_and_grants_cannot_be_reconstructed ... ok
test tests::a_command_with_no_events_is_refused ... ok
test tests::a_stream_cannot_be_named_without_a_tenant ... ok
test tests::an_event_body_must_be_an_object ... ok
test tests::an_identity_that_names_a_person_is_refused ... ok
test tests::depth_beyond_the_limit_is_refused ... ok
test tests::effect_evidence_and_boundary_inventory_are_machine_checked ... ok
test tests::effect_inventory_requires_unique_opaque_names_and_complete_coverage ... ok
test tests::effect_metadata_bounds_cover_required_optional_and_outcome_fields ... ok
test tests::effect_metadata_refuses_personal_text_in_every_identifier_and_code ... ok
test tests::effect_stages_preserve_wire_shape_and_explicit_coverage ... ok
test tests::the_same_body_hashes_the_same_way ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 9 tests
test pool::tests::closed_idle_driver_is_joined_before_replacement_connects ... ok
test pool::tests::idle_checkout_has_one_coherent_owner ... ok
test pool::tests::quarantine_retirement_cannot_count_a_replacement_twice ... ok
test pool::tests::queued_cancellation_timeout_and_granted_cancellation_release_capacity ... ok
test pool::tests::reusable_retirement_preserves_two_connection_four_waiter_snapshot ... ok
test pool::tests::shutdown_cancels_waiters_and_drains_quarantine ... ok
test pool::tests::shutdown_does_not_recycle_a_returning_connection_after_close ... ok
test pool::tests::zero_waiter_pool_reuses_and_refuses_without_queueing ... ok
test schema::snapshot_tests::snapshot_schema_upgrade_retains_history_and_refuses_partial_metadata ... ok

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.23s


running 2 tests
test schema_admission_refuses_inherited_event_children ... ok
test unrelated_xmin_cannot_make_committed_positions_noncontiguous ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.59s


running 36 tests
test a_feed_cursor_cannot_pass_an_inflight_lower_position_with_a_newer_xid ... ok
test a_reader_never_skips_an_event_that_committed_late ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test absent_deployment_scope_races_across_tenants_and_clients ... ok
test caught_reservation_cancellation_cannot_commit_an_unchecked_append ... ok
test committed_commands_survive_snapshot_storage_failure ... ok
test contended_catch_up_reuses_the_same_settled_session ... ok
test empty_catch_up_reuses_the_same_settled_session ... ok
test hosted_schema_admission_refuses_rewrite_rules ... ok
test incomplete_companion_schema_is_refused_before_serving ... ok
test independent_first_appends_return_contract_outcomes ... ok
test inline_failure_preserves_all_atomic_state_and_callback_authority ... ok
test isolated_transport_cannot_hide_a_remote_address_behind_localhost ... ok
test killed_projector_restarts_competing_workers_without_partial_view_or_cursor ... ok
test legacy_populated_schema_migrates_atomically_and_unknown_checksums_refuse ... ok
test lost_commit_response_reconnects_to_exact_durable_receipt ... ok
test missing_scope_bootstrap_is_atomic_across_independent_processes ... ok
test pool_observation_preserves_two_connections_and_four_waiters ... ok
test public_queue_cancellation_and_broken_idle_reclaim_exact_capacity ... ok
test rebuild_preserves_other_tenants_and_previous_view_on_failure ... ok
test registration_freezes_and_pool_refuses_bounded_overload ... ok
test rewrite_rules_are_rejected_during_projection_migration_and_registration ... ok
test rewrite_rules_are_rejected_on_every_durable_table ... ok
test schema_admission_refuses_rules_that_suppress_command_receipts ... ok
test schema_admission_refuses_triggers_policies_generation_and_foreign_sequences ... ok
test scope_reservations_are_atomic_and_confined ... ok
test shutdown_cancellation_preserves_public_pool_lifetimes ... ok
test snapshot_capture_waits_for_complete_tenant_erasure ... ok
test snapshot_history_and_repository_privacy_interleavings ... ok
test the_claim_rule_holds_on_postgresql ... ok
test the_inline_projection_exercise_passes_on_postgresql ... ok
test the_paging_rule_holds_on_postgresql ... ok
test the_projection_exercise_passes_on_postgresql ... ok
test the_shared_exercise_passes_on_postgresql ... ok
test unsettled_catch_up_rollback_never_recycles_a_session ... ok
test verified_tls_requires_matching_server_and_separate_application_role ... ok

test result: ok. 36 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 24.50s


running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.98s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test legacy_projection_rows_are_erased_without_reregistering_retired_projectors ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.26s


running 11 tests
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test inline_failure_preserves_all_atomic_state_and_callback_authority ... ok
test rebuild_preserves_other_tenants_and_previous_view_on_failure ... ok
test scope_reservations_are_atomic_and_confined_in_memory_and_file ... ok
test the_claim_rule_holds_in_memory ... ok
test the_inline_projection_exercise_passes_in_memory ... ok
test the_paging_rule_holds_in_memory ... ok
test the_projection_exercise_passes_in_memory ... ok
test the_shared_exercise_passes_in_memory ... ok
test the_shared_exercise_passes_on_a_file ... ok
test two_owners_share_a_database_without_sharing_a_table ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.58s


running 3 tests
test a_body_written_under_an_older_version_still_folds ... ok
test a_committed_vector_still_folds_when_loaded_from_disk ... ok
test a_version_with_no_upcaster_is_named_rather_than_guessed ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 2 tests
test ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure ... ok
test legacy_erasure_matches_literal_underscores_and_preserves_other_owners ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.70s


running 12 tests
test a_command_folds_into_the_state_it_produced ... ok
test a_command_that_decided_nothing_writes_nothing ... ok
test a_concurrent_write_is_retried_once_and_then_refused ... ok
test a_delayed_unproven_snapshot_cannot_restore_redacted_state ... ok
test a_retried_command_is_answered_not_written_again ... ok
test a_second_event_type_folds_and_reads_back ... ok
test a_snapshot_is_a_cache_and_the_fold_agrees_with_it ... ok
test a_stale_snapshot_schema_is_discarded_rather_than_trusted ... ok
test an_aggregate_stays_foldable_after_an_erasure ... ok
test committed_commands_survive_snapshot_storage_failure ... ok
test every_prefix_folds_the_same_way_with_or_without_a_snapshot ... ok
test snapshot_history_and_repository_privacy_interleavings ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.21s


running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-sqlite)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-conformance)
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-core)
    Finished `test` profile [unoptimized] target(s) in 4.58s
     Running unittests src/lib.rs (target/debug/deps/eventlog_conformance-81307400d91e72d5)
     Running unittests src/lib.rs (target/debug/deps/eventlog_core-6e06060c4a60d048)
     Running unittests src/lib.rs (target/debug/deps/eventlog_postgres-69070d9c12dde61c)
closed-idle replacement attempts before=1, while old driver paused=1
idle checkout snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 1, waiting: 0, idle: 1, closed: false }
quarantine replacement snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 2, waiting: 0, idle: 0, closed: false }
reusable retirement snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 1, waiting: 4, idle: 1, closed: false }
shutdown completed: closed=true, checked_out=0, idle=0
     Running tests/adversary.rs (target/debug/deps/adversary-ea79296f5cdc97de)
positions_and_xids=[(1, 4130), (2, 4128)]; unrelated_xid=4129; early_cursor=0; early_count=0; resumed_count=2; all_count=2
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)
catch-up reuse: contended=true, eight no-work polls retained backend 1122, cursor=1, tally=1
catch-up reuse: contended=false, eight no-work polls retained backend 1126, cursor=1, tally=1
public pool profile2/4: queued4, committed1, cancelled1, query_success=6, overload=58, samples=1687
public queue: four cancellations, four acquisition deadlines, four overloads, one granted-unpolled cancellation, own idle backend 1194 terminated; reconnect transport_refusals=0
rewrite-rule admission: refused all 11 durable tables; ordinary retry retained exact receipt
public shutdown cancellation: queued Closed; two bounded shutdown deadlines; accepted command retained; repeated shutdown drained 0/0/0
catch-up rollback: cancel=false, response withheld with occupancy=1/0/0; shutdown drained=0/0/0
catch-up rollback: cancel=true, response withheld with occupancy=1/0/0; shutdown drained=0/0/0
     Running tests/runtime_context.rs (target/debug/deps/runtime_context-936f9f892482b809)
     Running unittests src/lib.rs (target/debug/deps/eventlog_sqlite-4118c9a17598b586)
     Running tests/adversary.rs (target/debug/deps/adversary-231eab852eb00512)
retained_projection_after_successful_erasure=None
     Running tests/conformance.rs (target/debug/deps/conformance-9cca51e8c05339a7)
     Running tests/evolution.rs (target/debug/deps/evolution-1948f69ba95fd731)
     Running tests/legacy_namespaces.rs (target/debug/deps/legacy_namespaces-a6f3ff8640f3614e)
     Running tests/repository.rs (target/debug/deps/repository-236462b0c2b57479)
     Running tests/runtime_context.rs (target/debug/deps/runtime_context-e6b1511c66680dc7)
   Doc-tests eventlog_conformance
   Doc-tests eventlog_core
   Doc-tests eventlog_postgres
   Doc-tests eventlog_sqlite
{"backend_server":{"version":"17.6","version_num":"170006"},"binary_sha256":"069e20f38398cd9d22cf282c7ba00b871e313d75871adfd234ee284fb12b4640","capacity_admitted":false,"capacity_requirement":"comparative laboratory artifact is separately required; this runner does not manufacture capacity evidence","conformance_valid":true,"default_pool":{"acquisition_ms":2000,"connections":4,"transaction_ms":10000,"waiters":32},"duration_ms":37130,"failed":0,"finished_at":"2026-09-09 0:51:46.255985138 +00:00:00","format":"eventlog-production-proof/1","missing_required_cases":[],"owner_fixture_handoff":["SDK current authority and exact realm/service bindings","SDK original and generated stream/feed/cursor/view/effect vectors"],"passed":92,"runner_summaries":["test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.23s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.59s","test result: ok. 36 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 24.50s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.98s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.26s","test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.58s","test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.70s","test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.21s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s"],"schema_setup":"test-owned exact prefixes and hosted_owner schema; additive checksum admission exercised","skipped":0,"source_dirty":true,"source_revision":"5c1ff2690f07ec0b91f14cce307fff2a6b65ddca","started_at":"2026-09-09 0:51:09.125206092 +00:00:00"}
gate: cargo fmt --all --check
gate: cargo clippy --workspace --all-targets --locked -- -D warnings
    Checking eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-core)
    Checking ring v0.17.14
    Checking rustls-webpki v0.103.15
    Checking rustls v0.23.43
    Checking eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-conformance)
    Checking eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-sqlite)
    Checking tokio-rustls v0.26.5
    Checking tokio-postgres-rustls v0.14.0
    Checking eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 3.13s
gate: green

## Coordinator-owned required roster
- schema_admission_refuses_rules_that_suppress_command_receipts
- hosted_schema_admission_refuses_rewrite_rules
- rewrite_rules_are_rejected_on_every_durable_table
- rewrite_rules_are_rejected_during_projection_migration_and_registration

## Exclusions and handoff
No AEP, production-proof roster, Cargo, README, CHANGELOG, shared conformance/core, SQLite, or sibling input_validation.rs edits. The final integrated gate will incorporate the coordinator's roster and parallel input-validation changes. No release, publication, fixture restart/removal or cleanup performed by this worker. No path outside the assigned worktree was written. Evidence directory: target/review-scratch/schema-repair. Coordinator owns publication, further review and cleanup; worker lease released at handoff.

Commit identity:
6bbb06d5a21412a8c51ae69e5462b4eb28d36460
Author: b10x-bot[bot] <316511680+b10x-bot[bot]@users.noreply.github.com>
Committer: b10x-bot[bot] <316511680+b10x-bot[bot]@users.noreply.github.com>
