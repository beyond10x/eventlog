---
format: aep.planning-md/1
id: verification-report:catchup-implementation-20260909
kind: verification-report
status: draft
title: Catch-up implementation runner receipts
relations:
- reviews: story:reuse-postgres-connections-after-empty-catch-up
revision: 1
---
unit: story:reuse-postgres-connections-after-empty-catch-up — Reuse PostgreSQL connections after empty catch-up polls
verdict: green
cases: executed 73→76, red 3
origin: n/a
wrote-outside-worktree: none
needs-coordinator: no

## Unit and acceptance
Successful empty and advisory-lock-unavailable catch-up transactions return the exact healthy session for reuse only after confirmed rollback; failed/cancelled rollback remains quarantined.

## Change
Two successful early returns now await transaction.rollback() before client.settled(). Three integration regressions are mandatory in production-proof. No public API, schema or pool implementation changes. Inferred pool edit scope was checked and proved unnecessary: the existing quarantine/driver retirement implementation already enforces the safety requirement.

Class enumeration: run_catch_up has three successful outcomes. Unavailable lock and empty eligible feed now explicitly roll back then settle; event application already commits then settles. Error/cancellation paths leave the lease quarantined.

## Raw evidence
All files below are relative to this report's directory. Shell command exit statuses were read directly, without pipelines.
- initial baseline.log: package baseline before fixture configuration; exited101 solely because required TLS environment was absent (35 passed, 1 failed).
- red.log: cargo test -p eventlog-postgres --test conformance --locked catch_up_ -- --nocapture --test-threads=1; exit101, executed3, 0 passed/3 failed. Production source unmodified.
- focused-green.log: identical command; exit0, executed3, 3 passed/0 failed.
- mutant.log: cargo test -p eventlog-postgres --test conformance --locked unsettled_catch_up_ -- --nocapture --test-threads=1; exit101, executed1, 0 passed/1 failed. Mutant replaced explicit awaited rollback with drop(transaction), retaining premature settled(); assertion found idle=1 while rollback response withheld. Mutant restored.
- gate.log and production-proof.raw.log: bash scripts/gate.sh --production-proof; exit0; 76 passed, 0 failed, 0 skipped; missing_required_cases=[]; workspace fmt and clippy green.
- package-green.log: cargo test -p eventlog-postgres --locked -- --nocapture --test-threads=1; exit0; 41 passed, 0 failed; no skipped backend.
- baseline-gate.log and baseline-production-proof.raw.log: comparison on exact base32bf1596, with TLS fixture configured, preserving/restoring fixed files via EXIT trap.
- rustup-update.log: rustup update; exit0; stable1.98.1 unchanged.

Environment: CARGO_BUILD_JOBS=4, CARGO_PROFILE_DEV_DEBUG=0, CARGO_PROFILE_TEST_DEBUG=0, EVENTLOG_REQUIRE_POSTGRES=1. TMPDIR is this directory. The disposable eventlog-catchup-20260909 container remains on32875, PostgreSQL17.6. TLS files stay in tls/; private keys are disposable fixture material, not evidence to publish.

## Exclusions and handoff
No planning files, sibling source, or pool internals edited. No release or publication performed by this worker. Coordinator owns artifact evidence/lifecycle, publication, later review, container shutdown, and worktree cleanup. No files written outside assigned worktree. Scratch backups *.fixed.rs and implementation.patch preserve the exact tested change. Worker lease released at handoff.

## Red output (verbatim)
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 1.30s
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 3 tests
test contended_catch_up_reuses_the_same_settled_session ... 
thread 'contended_catch_up_reuses_the_same_settled_session' (728383) panicked at crates/eventlog-postgres/tests/conformance.rs:46:13:
assertion `left == right` failed: a successful no-work pass must return its settled connection
  left: (1, 0, 0)
 right: (0, 0, 1)
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
FAILED
test empty_catch_up_reuses_the_same_settled_session ... 
thread 'empty_catch_up_reuses_the_same_settled_session' (728515) panicked at crates/eventlog-postgres/tests/conformance.rs:46:13:
assertion `left == right` failed: a successful no-work pass must return its settled connection
  left: (1, 0, 0)
 right: (0, 0, 1)
FAILED
test unsettled_catch_up_rollback_never_recycles_a_session ... 
thread 'unsettled_catch_up_rollback_never_recycles_a_session' (728559) panicked at crates/eventlog-postgres/tests/conformance.rs:145:120:
proxy observed rollback: RecvError(())
FAILED

failures:

failures:
    contended_catch_up_reuses_the_same_settled_session
    empty_catch_up_reuses_the_same_settled_session
    unsettled_catch_up_rollback_never_recycles_a_session

test result: FAILED. 0 passed; 3 failed; 0 ignored; 0 measured; 26 filtered out; finished in 0.85s

error: test failed, to rerun pass `-p eventlog-postgres --test conformance`

## Green gate output (verbatim)
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 0.63s
     Running `target/debug/examples/gate --production-proof`
gate: cargo run --locked -p eventlog-postgres --example production-proof
   Compiling ring v0.17.14
   Compiling rustls v0.23.43
   Compiling rustls-webpki v0.103.15
   Compiling tokio-rustls v0.26.5
   Compiling tokio-postgres-rustls v0.14.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 1.46s
     Running `target/debug/examples/production-proof`

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 7 tests
test admission::tests::coordinates_preserve_boundaries_and_grants_cannot_be_reconstructed ... ok
test tests::a_command_with_no_events_is_refused ... ok
test tests::a_stream_cannot_be_named_without_a_tenant ... ok
test tests::an_event_body_must_be_an_object ... ok
test tests::an_identity_that_names_a_person_is_refused ... ok
test tests::depth_beyond_the_limit_is_refused ... ok
test tests::the_same_body_hashes_the_same_way ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 8 tests
test pool::tests::closed_idle_driver_is_joined_before_replacement_connects ... ok
test pool::tests::idle_checkout_has_one_coherent_owner ... ok
test pool::tests::quarantine_retirement_cannot_count_a_replacement_twice ... ok
test pool::tests::queued_cancellation_timeout_and_granted_cancellation_release_capacity ... ok
test pool::tests::reusable_retirement_preserves_two_connection_four_waiter_snapshot ... ok
test pool::tests::shutdown_cancels_waiters_and_drains_quarantine ... ok
test pool::tests::shutdown_does_not_recycle_a_returning_connection_after_close ... ok
test pool::tests::zero_waiter_pool_reuses_and_refuses_without_queueing ... ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.14s


running 2 tests
test schema_admission_refuses_inherited_event_children ... ok
test unrelated_xmin_cannot_make_committed_positions_noncontiguous ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.38s


running 29 tests
test a_feed_cursor_cannot_pass_an_inflight_lower_position_with_a_newer_xid ... ok
test a_reader_never_skips_an_event_that_committed_late ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test absent_deployment_scope_races_across_tenants_and_clients ... ok
test caught_reservation_cancellation_cannot_commit_an_unchecked_append ... ok
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
test the_claim_rule_holds_on_postgresql ... ok
test the_inline_projection_exercise_passes_on_postgresql ... ok
test the_paging_rule_holds_on_postgresql ... ok
test the_projection_exercise_passes_on_postgresql ... ok
test the_shared_exercise_passes_on_postgresql ... ok
test unsettled_catch_up_rollback_never_recycles_a_session ... ok
test verified_tls_requires_matching_server_and_separate_application_role ... ok

test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 11.71s


running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.31s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test legacy_projection_rows_are_erased_without_reregistering_retired_projectors ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.23s


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

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.08s


running 3 tests
test a_body_written_under_an_older_version_still_folds ... ok
test a_committed_vector_still_folds_when_loaded_from_disk ... ok
test a_version_with_no_upcaster_is_named_rather_than_guessed ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 2 tests
test ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure ... ok
test legacy_erasure_matches_literal_underscores_and_preserves_other_owners ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.62s


running 9 tests
test a_command_folds_into_the_state_it_produced ... ok
test a_command_that_decided_nothing_writes_nothing ... ok
test a_concurrent_write_is_retried_once_and_then_refused ... ok
test a_retried_command_is_answered_not_written_again ... ok
test a_second_event_type_folds_and_reads_back ... ok
test a_snapshot_is_a_cache_and_the_fold_agrees_with_it ... ok
test a_stale_snapshot_schema_is_discarded_rather_than_trusted ... ok
test an_aggregate_stays_foldable_after_an_erasure ... ok
test every_prefix_folds_the_same_way_with_or_without_a_snapshot ... ok

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s


running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Compiling pkg-config v0.3.34
   Compiling vcpkg v0.2.15
   Compiling foldhash v0.1.5
   Compiling bitflags v2.13.1
   Compiling hashbrown v0.15.5
   Compiling rustix v1.1.4
   Compiling linux-raw-sys v0.12.1
   Compiling libsqlite3-sys v0.35.0
   Compiling hashlink v0.10.0
   Compiling fallible-iterator v0.3.0
   Compiling fallible-streaming-iterator v0.1.9
   Compiling fastrand v2.5.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
   Compiling tempfile v3.27.0
   Compiling rusqlite v0.37.0
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-sqlite)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-conformance)
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-core)
    Finished `test` profile [unoptimized] target(s) in 3.74s
     Running unittests src/lib.rs (target/debug/deps/eventlog_conformance-81307400d91e72d5)
     Running unittests src/lib.rs (target/debug/deps/eventlog_core-6e06060c4a60d048)
     Running unittests src/lib.rs (target/debug/deps/eventlog_postgres-69070d9c12dde61c)
closed-idle replacement attempts before=1, while old driver paused=1
idle checkout snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 1, waiting: 0, idle: 1, closed: false }
quarantine replacement snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 2, waiting: 0, idle: 0, closed: false }
reusable retirement snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 1, waiting: 4, idle: 1, closed: false }
shutdown completed: closed=true, checked_out=0, idle=0
     Running tests/adversary.rs (target/debug/deps/adversary-ea79296f5cdc97de)
positions_and_xids=[(1, 1121), (2, 1119)]; unrelated_xid=1120; early_cursor=0; early_count=0; resumed_count=2; all_count=2
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)
catch-up reuse: contended=true, eight no-work polls retained backend 117, cursor=1, tally=1
catch-up reuse: contended=false, eight no-work polls retained backend 121, cursor=1, tally=1
public pool profile2/4: queued4, committed1, cancelled1, query_success=6, overload=58, samples=2516
public queue: four cancellations, four acquisition deadlines, four overloads, one granted-unpolled cancellation, own idle backend 186 terminated; reconnect transport_refusals=0
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
{"backend_server":{"version":"17.6","version_num":"170006"},"binary_sha256":"452190d928fe5f7e1ae7bffa5e4dba0a2b45ac008557db22adff1fc420b7622d","capacity_admitted":false,"capacity_requirement":"comparative laboratory artifact is separately required; this runner does not manufacture capacity evidence","conformance_valid":true,"default_pool":{"acquisition_ms":2000,"connections":4,"transaction_ms":10000,"waiters":32},"duration_ms":20712,"failed":0,"finished_at":"2026-09-09 0:10:54.362748683 +00:00:00","format":"eventlog-production-proof/1","missing_required_cases":[],"owner_fixture_handoff":["SDK current authority and exact realm/service bindings","SDK original and generated stream/feed/cursor/view/effect vectors"],"passed":76,"runner_summaries":["test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.14s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.38s","test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 11.71s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.31s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.23s","test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.08s","test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.62s","test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s"],"schema_setup":"test-owned exact prefixes and hosted_owner schema; additive checksum admission exercised","skipped":0,"source_dirty":true,"source_revision":"32bf1596c853cdedfb5477e1f790c060c5bc1d13","started_at":"2026-09-09 0:10:33.650611883 +00:00:00"}
gate: cargo fmt --all --check
gate: cargo clippy --workspace --all-targets --locked -- -D warnings
    Checking libc v0.2.189
    Checking cfg-if v1.0.4
    Checking typenum v1.20.1
    Checking rand_core v0.10.1
    Checking serde_core v1.0.229
    Checking memchr v2.8.3
    Checking generic-array v0.14.7
    Checking getrandom v0.4.3
    Checking zmij v1.0.23
    Checking powerfmt v0.2.0
    Checking time-core v0.1.9
    Checking itoa v1.0.18
    Checking num-conv v0.2.2
    Checking uuid v1.24.1
    Checking bytes v1.12.1
    Checking block-buffer v0.10.4
    Checking crypto-common v0.1.7
    Checking pin-project-lite v0.2.17
    Checking socket2 v0.6.5
    Checking digest v0.10.7
    Checking mio v1.2.2
    Checking smallvec v1.15.2
    Checking cpufeatures v0.2.17
    Checking tokio v1.53.1
    Checking thiserror v2.0.20
    Checking sha2 v0.10.9
    Checking once_cell v1.21.4
    Checking hybrid-array v0.4.14
    Checking cmov v0.5.4
    Checking block-buffer v0.12.1
    Checking ctutils v0.4.2
    Checking crypto-common v0.2.2
    Checking const-oid v0.10.2
    Checking tinyvec_macros v0.1.1
    Checking cpufeatures v0.3.0
    Checking digest v0.11.3
    Checking tinyvec v1.12.0
    Checking chacha20 v0.10.1
    Checking getrandom v0.2.17
    Checking unicode-normalization v0.1.25
    Checking zeroize v1.9.0
    Checking futures-sink v0.3.34
    Checking deranged v0.5.8
    Checking serde_json v1.0.151
    Checking serde v1.0.229
    Checking unicode-bidi v0.3.18
    Checking untrusted v0.9.0
    Checking unicode-properties v0.1.4
    Checking futures-core v0.3.34
    Checking stringprep v0.1.5
    Checking ring v0.17.14
    Checking rustls-pki-types v1.15.1
    Checking rand v0.10.2
    Checking hmac v0.13.0
    Checking sha2 v0.11.0
    Checking md-5 v0.11.0
    Checking base64 v0.22.1
    Checking byteorder v1.5.0
    Checking const-oid v0.9.6
    Checking fallible-iterator v0.2.0
    Checking scopeguard v1.2.0
    Checking time v0.3.55
    Checking siphasher v1.0.3
    Checking flagset v0.4.7
    Checking lock_api v0.4.14
    Checking phf_shared v0.13.1
    Checking der v0.7.10
    Checking postgres-protocol v0.6.12
    Checking rustls-webpki v0.103.15
    Checking parking_lot_core v0.9.12
    Checking subtle v2.6.1
    Checking utf8parse v0.2.2
    Checking futures-task v0.3.34
    Checking rustls v0.23.43
    Checking anstyle-parse v1.0.0
    Checking futures-util v0.3.34
    Checking parking_lot v0.12.5
    Checking spki v0.7.3
    Checking phf v0.13.1
    Checking futures-channel v0.3.34
    Checking tokio-util v0.7.19
    Checking whoami v2.1.3
    Checking anstyle-query v1.1.5
    Checking log v0.4.33
    Checking colorchoice v1.0.5
    Checking percent-encoding v2.3.2
    Checking anstyle v1.0.14
    Checking is_terminal_polyfill v1.70.2
    Checking x509-cert v0.2.5
    Checking tokio-rustls v0.26.5
    Checking anstream v1.0.0
    Checking strsim v0.11.1
    Checking clap_lex v1.1.0
    Checking bitflags v2.13.1
    Checking foldhash v0.1.5
    Checking clap_builder v4.6.6
    Checking libsqlite3-sys v0.35.0
    Checking hashbrown v0.15.5
    Checking clap v4.6.6
    Checking hashlink v0.10.0
    Checking fallible-iterator v0.3.0
    Checking eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-core)
    Checking postgres-types v0.2.14
    Checking linux-raw-sys v0.12.1
    Checking fallible-streaming-iterator v0.1.9
    Checking rustix v1.1.4
    Checking rusqlite v0.37.0
    Checking tokio-postgres v0.7.18
    Checking eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-conformance)
    Checking fastrand v2.5.0
    Checking tempfile v3.27.0
    Checking eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-sqlite)
    Checking tokio-postgres-rustls v0.14.0
    Checking eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 7.57s
gate: green

## Final comparison and handoff
Clean baseline production-proof: executed 73, passed73, failed0, skipped0, exit0. Fixed production-proof: executed76, passed76, failed0, skipped0, exit0. Both workspace formatting and clippy succeeded. Baseline comparison completed after the fixed gate and restored byte-identical fixed source (three cmp checks passed) before commit.
Commit: 15c0496 on impl/reuse-postgres-connections-after-empty-catch-up. Coordinator publishes.
15c049633363dd0f504c7ccaeb36ee01c1bb4bd6
Author: b10x-bot[bot] <316511680+b10x-bot[bot]@users.noreply.github.com>
Committer: b10x-bot[bot] <316511680+b10x-bot[bot]@users.noreply.github.com>
 .../eventlog-postgres/examples/production-proof.rs |   3 +
 crates/eventlog-postgres/src/lib.rs                |   4 +
 crates/eventlog-postgres/tests/conformance.rs      | 265 +++++++++++++++++++++
 3 files changed, 272 insertions(+)
