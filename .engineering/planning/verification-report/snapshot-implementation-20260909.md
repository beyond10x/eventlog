---
format: aep.planning-md/1
id: verification-report:snapshot-implementation-20260909
kind: verification-report
status: draft
title: Verified snapshot implementation receipts
relations:
- reviews: story:prevent-stale-snapshots-after-redaction
revision: 1
---
unit: story:prevent-stale-snapshots-after-redaction — Prevent stale snapshots from restoring redacted state
verdict: green
cases: executed 81→88, red 0
origin: n/a
wrote-outside-worktree: none
needs-coordinator: no

1. Acceptance: on both supported adapters, a snapshot derived before completed redaction cannot restore its contribution, and current-history snapshots still work.

Commit d25e5675e299119af905e0c2df9a8c862934e08a; author and committer both b10x-bot[bot] <316511680+b10x-bot[bot]@users.noreply.github.com>. Coordinator publishes the branch. Tree wt-f0ba183ee985, /home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985. No AEP writes or other units' source changes. Assigned inferred surfaces were confirmed: PostgreSQL admission, hosted permissions, test teardown and production roster all require the new metadata table; a new ESS metadata home was validated before implementation.

2. Shape: 11 files changed, 1116 insertions(+), 63 deletions(-). Core API and Repository; both adapters; PostgreSQL schema admission; shared conformance, SQLite repository and PostgreSQL conformance tests; production required-case roster; two ESS files. Snapshot/Loaded/Outcome layouts and existing physical snapshot columns remain unchanged. This is deliberately a storage contract change: legacy unproven saves return Invalid. Additive generation metadata certifies only checked saves. Automatic cache errors remain best effort after append; explicit snapshots retry one stale generation.

3. Red run: CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p eventlog-sqlite --test repository a_delayed_unproven_snapshot_cannot_restore_redacted_state -- --exact
Exit 101: existing implementation restored total 1 rather than 0. Full original output follows this summary.

Both required mutation probes replaced generation equality with a bound-parameter true predicate, leaving compilation and binding intact. Commands: cargo test -p eventlog-sqlite --test repository snapshot_history_and_repository_privacy_interleavings -- --exact; cargo test -p eventlog-postgres --test conformance snapshot_history_and_repository_privacy_interleavings -- --exact. Each exited 101, executed one case, and failed the delayed-save refusal assertion. Their full output follows. Both sources were restored exactly; sha256sum --check target/review-scratch/pre-mutation.sha256 printed OK for both files. No subsequent implementation edit was made after the green full gate.

4. Full gate: TMPDIR=$PWD/target/review-scratch EVENTLOG_TEST_POSTGRES_URL=postgresql://postgres@127.0.0.1:32877/postgres EVENTLOG_TEST_HOSTED_POSTGRES_URL=postgresql://eventlog_test_application@localhost:32877/postgres EVENTLOG_TEST_POSTGRES_CA=$PWD/target/review-scratch/tls/ca.crt CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 bash scripts/gate.sh --production-proof
Exit 0. Full original output follows the red/mutation logs. Runner total81→88; PostgreSQL unit8→9, PostgreSQL conformance29→32, SQLite repository9→12. Baseline whole-suite counts come from the coordinator's completed combined-baseline production runner, not a duplicate worker run. All remaining lane counts were unchanged because their selected cases were extended or their code remained unchanged. No added test was absent from its lane. Production proof reports PostgreSQL17.6,88 passed,0 failed,0 skipped, no missing required cases; formatting/clippy exit0.

The installed ESS CLI refused the skill's stale spelling (`ess specify validate`): error: unrecognized subcommand 'specify'. The advertised installed command `ess validate --path ess/snapshots` printed `eventlog v1 — 2 file(s), valid`, exit0. rustup check reports stable1.98.1 up to date.

5. Deliberately excluded: atomic domain decisions with concurrent privacy changes (separate contract); current broad-review input-validation and PostgreSQL rewrite-rule findings (assigned to other units); public source docs, Cargo/release metadata and AEP (coordinator-owned); consumer pin promotion/deployment (outside release scope). Current service-sdk has no snapshot callers needing migration. Preserve the fenced binary cutover: old direct snapshot writers cannot run concurrently with this protocol.

6. No files written outside assigned worktree. Durable review evidence is in target/review-scratch: baseline-red.log, sqlite-mutant.log, pg-mutant.log, pre-mutation.sha256, production-gate.log, implementation-summary.md and implementation-report.md. Earlier diagnosis logs check.log, sqlite-repository.log, pg-snapshots.log, pg-races.log, clippy.log and fixture.log remain available. target/review-scratch/tls contains test fixture CA/private material and must remain private; it is not committed. Disposable container eventlog-snapshots-20260909 uses port32877. target storage1.4GiB. Coordinator owns cleanup and publication.

The appended outputs are, in order: original baseline red; SQLite mutation; PostgreSQL mutation; complete green production gate.
   Compiling proc-macro2 v1.0.107
   Compiling quote v1.0.47
   Compiling unicode-ident v1.0.24
   Compiling version_check v0.9.5
   Compiling serde_core v1.0.229
   Compiling libc v0.2.189
   Compiling generic-array v0.14.7
   Compiling typenum v1.20.1
   Compiling cfg-if v1.0.4
   Compiling getrandom v0.4.3
   Compiling zmij v1.0.23
   Compiling shlex v2.0.1
   Compiling find-msvc-tools v0.1.11
   Compiling syn v3.0.3
   Compiling cc v1.4.4
   Compiling block-buffer v0.10.4
   Compiling crypto-common v0.1.7
   Compiling thiserror v2.0.20
   Compiling serde_json v1.0.151
   Compiling vcpkg v0.2.15
   Compiling pkg-config v0.3.34
   Compiling serde v1.0.229
   Compiling libsqlite3-sys v0.35.0
   Compiling serde_derive v1.0.229
   Compiling thiserror-impl v2.0.20
   Compiling digest v0.10.7
   Compiling powerfmt v0.2.0
   Compiling itoa v1.0.18
   Compiling num-conv v0.2.2
   Compiling bitflags v2.13.1
   Compiling memchr v2.8.3
   Compiling time-core v0.1.9
   Compiling cpufeatures v0.2.17
   Compiling foldhash v0.1.5
   Compiling sha2 v0.10.9
   Compiling hashbrown v0.15.5
   Compiling deranged v0.5.8
   Compiling uuid v1.24.1
   Compiling rustix v1.1.4
   Compiling hashlink v0.10.0
   Compiling tokio-macros v2.7.2
   Compiling pin-project-lite v0.2.17
   Compiling fallible-streaming-iterator v0.1.9
   Compiling linux-raw-sys v0.12.1
   Compiling fallible-iterator v0.3.0
   Compiling time v0.3.55
   Compiling smallvec v1.15.2
   Compiling tokio v1.53.1
   Compiling rusqlite v0.37.0
   Compiling fastrand v2.5.0
   Compiling once_cell v1.21.4
   Compiling tempfile v3.27.0
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-core)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-sqlite)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-conformance)
    Finished `test` profile [unoptimized] target(s) in 5.94s
     Running tests/repository.rs (target/debug/deps/repository-3e79b32af72754d0)

running 1 test
test a_delayed_unproven_snapshot_cannot_restore_redacted_state ... FAILED

failures:

---- a_delayed_unproven_snapshot_cannot_restore_redacted_state stdout ----

thread 'a_delayed_unproven_snapshot_cannot_restore_redacted_state' (881480) panicked at crates/eventlog-sqlite/tests/repository.rs:145:5:
assertion `left == right` failed
  left: 1
 right: 0
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    a_delayed_unproven_snapshot_cannot_restore_redacted_state

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 9 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p eventlog-sqlite --test repository`
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-core)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-sqlite)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-conformance)
    Finished `test` profile [unoptimized] target(s) in 1.34s
     Running tests/repository.rs (target/debug/deps/repository-3e79b32af72754d0)

running 1 test
test snapshot_history_and_repository_privacy_interleavings ... FAILED

failures:

---- snapshot_history_and_repository_privacy_interleavings stdout ----

thread 'snapshot_history_and_repository_privacy_interleavings' (1075919) panicked at crates/eventlog-conformance/src/lib.rs:535:5:
assertion failed: !store.save_snapshot_checked(&stream, &stale, &observed).await.unwrap()
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    snapshot_history_and_repository_privacy_interleavings

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 11 filtered out; finished in 0.01s

error: test failed, to rerun pass `-p eventlog-sqlite --test repository`
   Compiling ring v0.17.14
   Compiling rustls v0.23.43
   Compiling rustls-webpki v0.103.15
   Compiling tokio-rustls v0.26.5
   Compiling tokio-postgres-rustls v0.14.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 2.12s
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 1 test
test snapshot_history_and_repository_privacy_interleavings ... FAILED

failures:

---- snapshot_history_and_repository_privacy_interleavings stdout ----

thread 'snapshot_history_and_repository_privacy_interleavings' (1079381) panicked at crates/eventlog-conformance/src/lib.rs:535:5:
assertion failed: !store.save_snapshot_checked(&stream, &stale, &observed).await.unwrap()
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    snapshot_history_and_repository_privacy_interleavings

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 31 filtered out; finished in 0.22s

error: test failed, to rerun pass `-p eventlog-postgres --test conformance`
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-core)
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-postgres)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-conformance)
    Finished `dev` profile [unoptimized] target(s) in 1.26s
     Running `target/debug/examples/gate --production-proof`
gate: cargo run --locked -p eventlog-postgres --example production-proof
   Compiling ring v0.17.14
   Compiling rustls v0.23.43
   Compiling rustls-webpki v0.103.15
   Compiling tokio-rustls v0.26.5
   Compiling tokio-postgres-rustls v0.14.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 1.58s
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

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.90s


running 2 tests
test schema_admission_refuses_inherited_event_children ... ok
test unrelated_xmin_cannot_make_committed_positions_noncontiguous ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.43s


running 32 tests
test a_feed_cursor_cannot_pass_an_inflight_lower_position_with_a_newer_xid ... ok
test a_reader_never_skips_an_event_that_committed_late ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test absent_deployment_scope_races_across_tenants_and_clients ... ok
test caught_reservation_cancellation_cannot_commit_an_unchecked_append ... ok
test committed_commands_survive_snapshot_storage_failure ... ok
test contended_catch_up_reuses_the_same_settled_session ... ok
test empty_catch_up_reuses_the_same_settled_session ... ok
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

test result: ok. 32 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.97s


running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.64s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test legacy_projection_rows_are_erased_without_reregistering_retired_projectors ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.27s


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

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.77s


running 3 tests
test a_body_written_under_an_older_version_still_folds ... ok
test a_committed_vector_still_folds_when_loaded_from_disk ... ok
test a_version_with_no_upcaster_is_named_rather_than_guessed ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 2 tests
test ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure ... ok
test legacy_erasure_matches_literal_underscores_and_preserves_other_owners ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.28s


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

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.30s


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

   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-postgres)
   Compiling tempfile v3.27.0
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-sqlite)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-conformance)
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-core)
    Finished `test` profile [unoptimized] target(s) in 3.45s
     Running unittests src/lib.rs (target/debug/deps/eventlog_conformance-81307400d91e72d5)
     Running unittests src/lib.rs (target/debug/deps/eventlog_core-6e06060c4a60d048)
     Running unittests src/lib.rs (target/debug/deps/eventlog_postgres-69070d9c12dde61c)
closed-idle replacement attempts before=1, while old driver paused=1
idle checkout snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 1, waiting: 0, idle: 1, closed: false }
quarantine replacement snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 2, waiting: 0, idle: 0, closed: false }
reusable retirement snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 1, waiting: 4, idle: 1, closed: false }
shutdown completed: closed=true, checked_out=0, idle=0
     Running tests/adversary.rs (target/debug/deps/adversary-ea79296f5cdc97de)
positions_and_xids=[(1, 893), (2, 891)]; unrelated_xid=892; early_cursor=0; early_count=0; resumed_count=2; all_count=2
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)
catch-up reuse: contended=true, eight no-work polls retained backend 124, cursor=1, tally=1
catch-up reuse: contended=false, eight no-work polls retained backend 128, cursor=1, tally=1
public pool profile2/4: queued4, committed1, cancelled1, query_success=6, overload=58, samples=3207
public queue: four cancellations, four acquisition deadlines, four overloads, one granted-unpolled cancellation, own idle backend 193 terminated; reconnect transport_refusals=0
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
{"backend_server":{"version":"17.6","version_num":"170006"},"binary_sha256":"069e20f38398cd9d22cf282c7ba00b871e313d75871adfd234ee284fb12b4640","capacity_admitted":false,"capacity_requirement":"comparative laboratory artifact is separately required; this runner does not manufacture capacity evidence","conformance_valid":true,"default_pool":{"acquisition_ms":2000,"connections":4,"transaction_ms":10000,"waiters":32},"duration_ms":28526,"failed":0,"finished_at":"2026-09-09 0:36:00.178130909 +00:00:00","format":"eventlog-production-proof/1","missing_required_cases":[],"owner_fixture_handoff":["SDK current authority and exact realm/service bindings","SDK original and generated stream/feed/cursor/view/effect vectors"],"passed":88,"runner_summaries":["test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.90s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.43s","test result: ok. 32 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.97s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.64s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.27s","test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.77s","test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.28s","test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.30s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s"],"schema_setup":"test-owned exact prefixes and hosted_owner schema; additive checksum admission exercised","skipped":0,"source_dirty":true,"source_revision":"699c0e15a2c3669d88329543f113e6302ba9dc7e","started_at":"2026-09-09 0:35:31.651310802 +00:00:00"}
gate: cargo fmt --all --check
gate: cargo clippy --workspace --all-targets --locked -- -D warnings
    Checking ring v0.17.14
    Checking eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-sqlite)
    Checking rustls-webpki v0.103.15
    Checking rustls v0.23.43
    Checking tokio-rustls v0.26.5
    Checking tokio-postgres-rustls v0.14.0
    Checking eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-f0ba183ee985/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 1.77s
gate: green
