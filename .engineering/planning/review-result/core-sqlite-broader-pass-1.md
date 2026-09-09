---
format: aep.planning-md/1
id: review-result:core-sqlite-broader-pass-1
kind: review-result
status: active
title: Core and SQLite broader review pass 1
relations:
- reviews: story:integrate-reviewed-feature-contracts
revision: 1
---
unit: broader core/SQLite review at 096cdbe77c0adf1c8ade9b3b3985fcdbbb8cfcd9 plus tests-only working tree
verdict: NEEDS-CHANGE
cases: executed 78→81, red 3
origin: introduced 0 / pre-existing 0 / undecided 1
wrote-outside-worktree: none
needs-coordinator: file and fix the public input-validation boundary class, retain tests/logs, and verify both real backends before release

1. git --no-pager diff --stat
 .../tests/input_validation_review.rs               | 103 +++++++++++++++++++++
 1 file changed, 103 insertions(+)

2. Cases added and first red run

Only crates/eventlog-sqlite/tests/input_validation_review.rs was added. All three cases use the real SQLite in-memory backend, public serde constructors/struct fields, and public append. They assert that constructor-invalid events, streams, and claim metadata cannot be persisted. All three are red now. No implementation was edited. Formatting after the initial red run changed assertion line locations but not bytes under test.

Command: TMPDIR="$PWD/target/review-scratch" CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p eventlog-sqlite --test input_validation_review --locked
Exit: 101. Verbatim first-run output:
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
    Finished `test` profile [unoptimized] target(s) in 0.69s
     Running tests/input_validation_review.rs (target/debug/deps/input_validation_review-914d5ffd7d995a3b)

running 3 tests
test append_refuses_deserialized_streams_that_bypass_constructor_checks ... FAILED
test append_refuses_claim_fields_that_bypass_constructor_checks ... FAILED
test append_refuses_deserialized_events_that_bypass_constructor_checks ... FAILED

failures:

---- append_refuses_deserialized_streams_that_bypass_constructor_checks stdout ----

thread 'append_refuses_deserialized_streams_that_bypass_constructor_checks' (866434) panicked at crates/eventlog-sqlite/tests/input_validation_review.rs:35:9:
constructor-invalid stream was appended: Ok(AppendResult { first_version: 1, last_version: 1, events: [RecordedEvent { global_seq: 1, tenant: TenantId(""), stream_type: "item", stream_id: "one", version: 1, event_id: "01a08386-1424-7542-9b11-46ad0f465ac5", name: "item.received", schema_version: 1, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-09 0:16:37.41286981 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-command", trace_id: "trace-command", causation_id: None, causation_depth: 0, redacted_at: None, data: Object {"value": Number(1)} }], deduplicated: false })
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- append_refuses_claim_fields_that_bypass_constructor_checks stdout ----

thread 'append_refuses_claim_fields_that_bypass_constructor_checks' (866432) panicked at crates/eventlog-sqlite/tests/input_validation_review.rs:52:9:
constructor-invalid claim was appended: Ok(AppendResult { first_version: 1, last_version: 1, events: [RecordedEvent { global_seq: 1, tenant: TenantId("tenant"), stream_type: "item", stream_id: "0", version: 1, event_id: "01a08386-1424-7542-9b11-46caecb38207", name: "item.received", schema_version: 1, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-09 0:16:37.412944564 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-command", trace_id: "trace-command", causation_id: None, causation_depth: 0, redacted_at: None, data: Object {"value": Number(1)} }], deduplicated: false })

---- append_refuses_deserialized_events_that_bypass_constructor_checks stdout ----

thread 'append_refuses_deserialized_events_that_bypass_constructor_checks' (866433) panicked at crates/eventlog-sqlite/tests/input_validation_review.rs:19:9:
constructor-invalid event was appended: Ok(AppendResult { first_version: 1, last_version: 1, events: [RecordedEvent { global_seq: 1, tenant: TenantId("tenant"), stream_type: "item", stream_id: "0", version: 1, event_id: "01a08386-1424-7542-9b11-46bf11d9a858", name: "item.received", schema_version: 1, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-09 0:16:37.41292919 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-command", trace_id: "trace-command", causation_id: None, causation_depth: 0, redacted_at: None, data: Number(7) }], deduplicated: false })


failures:
    append_refuses_claim_fields_that_bypass_constructor_checks
    append_refuses_deserialized_events_that_bypass_constructor_checks
    append_refuses_deserialized_streams_that_bypass_constructor_checks

test result: FAILED. 0 passed; 3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p eventlog-sqlite --test input_validation_review`

3. Suite after cases existed

Command: TMPDIR="$PWD/target/review-scratch" EVENTLOG_TEST_POSTGRES_URL=postgres://postgres@127.0.0.1:32876/postgres CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --workspace --locked --no-fail-fast
Exit: 101. Baseline 78 from implementor handoff; after additions 81 executed: original 78 passed, new 3 failed. Verbatim runner output:
   Compiling ring v0.17.14
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
   Compiling rustls v0.23.43
   Compiling rustls-webpki v0.103.15
   Compiling tokio-rustls v0.26.5
   Compiling tokio-postgres-rustls v0.14.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 2.63s
     Running unittests src/lib.rs (target/debug/deps/eventlog_conformance-81307400d91e72d5)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_core-6e06060c4a60d048)

running 12 tests
test admission::tests::coordinates_preserve_boundaries_and_grants_cannot_be_reconstructed ... ok
test tests::a_command_with_no_events_is_refused ... ok
test tests::a_stream_cannot_be_named_without_a_tenant ... ok
test tests::an_event_body_must_be_an_object ... ok
test tests::an_identity_that_names_a_person_is_refused ... ok
test tests::depth_beyond_the_limit_is_refused ... ok
test tests::effect_evidence_and_boundary_inventory_are_machine_checked ... ok
test tests::effect_inventory_requires_unique_opaque_names_and_complete_coverage ... ok
test tests::effect_metadata_refuses_personal_text_in_every_identifier_and_code ... ok
test tests::the_same_body_hashes_the_same_way ... ok
test tests::effect_metadata_bounds_cover_required_optional_and_outcome_fields ... ok
test tests::effect_stages_preserve_wire_shape_and_explicit_coverage ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_postgres-69070d9c12dde61c)

running 8 tests
test pool::tests::zero_waiter_pool_reuses_and_refuses_without_queueing ... ok
test pool::tests::shutdown_cancels_waiters_and_drains_quarantine ... ok
test pool::tests::shutdown_does_not_recycle_a_returning_connection_after_close ... ok
test pool::tests::quarantine_retirement_cannot_count_a_replacement_twice ... ok
test pool::tests::reusable_retirement_preserves_two_connection_four_waiter_snapshot ... ok
test pool::tests::idle_checkout_has_one_coherent_owner ... ok
test pool::tests::closed_idle_driver_is_joined_before_replacement_connects ... ok
test pool::tests::queued_cancellation_timeout_and_granted_cancellation_release_capacity ... ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s

     Running tests/adversary.rs (target/debug/deps/adversary-ea79296f5cdc97de)

running 2 tests
test schema_admission_refuses_inherited_event_children ... ok
test unrelated_xmin_cannot_make_committed_positions_noncontiguous ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.34s

     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 26 tests
test isolated_transport_cannot_hide_a_remote_address_behind_localhost ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test a_reader_never_skips_an_event_that_committed_late ... ok
test absent_deployment_scope_races_across_tenants_and_clients ... ok
test a_feed_cursor_cannot_pass_an_inflight_lower_position_with_a_newer_xid ... ok
test incomplete_companion_schema_is_refused_before_serving ... ok
test caught_reservation_cancellation_cannot_commit_an_unchecked_append ... ok
test inline_failure_preserves_all_atomic_state_and_callback_authority ... ok
test independent_first_appends_return_contract_outcomes ... ok
test killed_projector_restarts_competing_workers_without_partial_view_or_cursor ... ok
test lost_commit_response_reconnects_to_exact_durable_receipt ... ok
test legacy_populated_schema_migrates_atomically_and_unknown_checksums_refuse ... ok
test missing_scope_bootstrap_is_atomic_across_independent_processes ... ok
test rebuild_preserves_other_tenants_and_previous_view_on_failure ... ok
test registration_freezes_and_pool_refuses_bounded_overload ... ok
test pool_observation_preserves_two_connections_and_four_waiters ... ok
test schema_admission_refuses_triggers_policies_generation_and_foreign_sequences ... ok
test public_queue_cancellation_and_broken_idle_reclaim_exact_capacity ... ok
test scope_reservations_are_atomic_and_confined ... ok
test the_claim_rule_holds_on_postgresql ... ok
test shutdown_cancellation_preserves_public_pool_lifetimes ... ok
test the_inline_projection_exercise_passes_on_postgresql ... ok
test the_paging_rule_holds_on_postgresql ... ok
test the_projection_exercise_passes_on_postgresql ... ok
test the_shared_exercise_passes_on_postgresql ... ok
test verified_tls_requires_matching_server_and_separate_application_role ... ok

test result: ok. 26 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.64s

     Running tests/runtime_context.rs (target/debug/deps/runtime_context-936f9f892482b809)

running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.15s

     Running unittests src/lib.rs (target/debug/deps/eventlog_sqlite-4118c9a17598b586)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/adversary.rs (target/debug/deps/adversary-231eab852eb00512)

running 1 test
test legacy_projection_rows_are_erased_without_reregistering_retired_projectors ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.53s

     Running tests/conformance.rs (target/debug/deps/conformance-9cca51e8c05339a7)

running 11 tests
test the_claim_rule_holds_in_memory ... ok
test the_inline_projection_exercise_passes_in_memory ... ok
test the_paging_rule_holds_in_memory ... ok
test rebuild_preserves_other_tenants_and_previous_view_on_failure ... ok
test the_projection_exercise_passes_in_memory ... ok
test the_shared_exercise_passes_in_memory ... ok
test scope_reservations_are_atomic_and_confined_in_memory_and_file ... ok
test inline_failure_preserves_all_atomic_state_and_callback_authority ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test the_shared_exercise_passes_on_a_file ... ok
test two_owners_share_a_database_without_sharing_a_table ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.12s

     Running tests/evolution.rs (target/debug/deps/evolution-1948f69ba95fd731)

running 3 tests
test a_body_written_under_an_older_version_still_folds ... ok
test a_version_with_no_upcaster_is_named_rather_than_guessed ... ok
test a_committed_vector_still_folds_when_loaded_from_disk ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/input_validation_review.rs (target/debug/deps/input_validation_review-9461c03b7fa23a39)

running 3 tests
test append_refuses_deserialized_events_that_bypass_constructor_checks ... FAILED
test append_refuses_deserialized_streams_that_bypass_constructor_checks ... FAILED
test append_refuses_claim_fields_that_bypass_constructor_checks ... FAILED

failures:

---- append_refuses_deserialized_events_that_bypass_constructor_checks stdout ----

thread 'append_refuses_deserialized_events_that_bypass_constructor_checks' (876485) panicked at crates/eventlog-sqlite/tests/input_validation_review.rs:19:9:
constructor-invalid event was appended: Ok(AppendResult { first_version: 1, last_version: 1, events: [RecordedEvent { global_seq: 1, tenant: TenantId("tenant"), stream_type: "item", stream_id: "0", version: 1, event_id: "01a08386-e4c8-78d0-a988-0dd807bda1ea", name: "item.received", schema_version: 1, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-09 0:17:30.824812406 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-command", trace_id: "trace-command", causation_id: None, causation_depth: 0, redacted_at: None, data: Number(7) }], deduplicated: false })
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- append_refuses_deserialized_streams_that_bypass_constructor_checks stdout ----

thread 'append_refuses_deserialized_streams_that_bypass_constructor_checks' (876486) panicked at crates/eventlog-sqlite/tests/input_validation_review.rs:35:9:
constructor-invalid stream was appended: Ok(AppendResult { first_version: 1, last_version: 1, events: [RecordedEvent { global_seq: 1, tenant: TenantId(""), stream_type: "item", stream_id: "one", version: 1, event_id: "01a08386-e4c9-7d81-89d5-d1de37dbdc45", name: "item.received", schema_version: 1, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-09 0:17:30.825343388 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-command", trace_id: "trace-command", causation_id: None, causation_depth: 0, redacted_at: None, data: Object {"value": Number(1)} }], deduplicated: false })

---- append_refuses_claim_fields_that_bypass_constructor_checks stdout ----

thread 'append_refuses_claim_fields_that_bypass_constructor_checks' (876484) panicked at crates/eventlog-sqlite/tests/input_validation_review.rs:52:9:
constructor-invalid claim was appended: Ok(AppendResult { first_version: 1, last_version: 1, events: [RecordedEvent { global_seq: 1, tenant: TenantId("tenant"), stream_type: "item", stream_id: "0", version: 1, event_id: "01a08386-e4c9-7d81-89d5-d1ee08db17d4", name: "item.received", schema_version: 1, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-09 0:17:30.825343389 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-command", trace_id: "trace-command", causation_id: None, causation_depth: 0, redacted_at: None, data: Object {"value": Number(1)} }], deduplicated: false })


failures:
    append_refuses_claim_fields_that_bypass_constructor_checks
    append_refuses_deserialized_events_that_bypass_constructor_checks
    append_refuses_deserialized_streams_that_bypass_constructor_checks

test result: FAILED. 0 passed; 3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p eventlog-sqlite --test input_validation_review`
     Running tests/legacy_namespaces.rs (target/debug/deps/legacy_namespaces-a6f3ff8640f3614e)

running 2 tests
test ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure ... ok
test legacy_erasure_matches_literal_underscores_and_preserves_other_owners ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.83s

     Running tests/repository.rs (target/debug/deps/repository-236462b0c2b57479)

running 9 tests
test a_command_that_decided_nothing_writes_nothing ... ok
test a_stale_snapshot_schema_is_discarded_rather_than_trusted ... ok
test a_retried_command_is_answered_not_written_again ... ok
test a_second_event_type_folds_and_reads_back ... ok
test an_aggregate_stays_foldable_after_an_erasure ... ok
test a_concurrent_write_is_retried_once_and_then_refused ... ok
test a_command_folds_into_the_state_it_produced ... ok
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

error: 1 target failed:
    `-p eventlog-sqlite --test input_validation_review`

4. Findings

| Source | Verdict | Origin | Finding |
|---|---|---|---|
| crates/eventlog-core/src/lib.rs:947 | NEEDS-CHANGE | undecided | Public deserialization and mutable fields bypass constructor invariants, and append persists scalar event bodies, empty tenant IDs, and invalid claim fields without rejection. |

What was measured: tests at input_validation_review.rs:7, :38, :63 each returned Ok(AppendResult) with durably appended events for (respectively) data=7, TenantId(""), and Claim.scope="". Targeted runner exited 101 with 0 passed / 3 failed; subsequent complete suite exited 101 with original 78 passing. Current formatted assertion lines are :29, :55, :97.

What reaches it: TenantId and StreamId publicly derive Deserialize at core lib.rs:65 and :95, while NewEvent and Claim expose public fields and Deserialize at :154 and :197. A normal owner can deserialize a request or construct/mutate these public values and call EventStore::append; SQLite's public append delegates through append_guarded at sqlite lib.rs:265 to the common validator at :541. validate_append at core lib.rs:947 checks batch cardinality and CommandMeta::validate only; meta validation omits Claim, and event checks are absent. This does not require a hand-edited database, forged storage, or a third backend. Empty tenant admission additionally produces rows whose normal read path invokes TenantId::new and cannot decode them.

Bounded corrective class: preserve wire fields and public compatibility while making TenantId/StreamId deserialization use constructor validation; expose/reuse NewEvent/Claim validation and apply it at append for publicly mutable fields. Enumerate each constructor invariant (empty, maximum length, illegal bytes, event body shape) across each public construction route. Because validate_append is shared by both backends, exercise the regression through both real backend conformance paths; do not rely only on SQLite tests. No new domain entity or backend is needed.

Origin is undecided by charter: source inspection shows these functions were unchanged by the feature integration, but no isolated base execution was available, and textual similarity is not claimed as a reproduced base failure. The coordinator can run these same tests against a permitted base checkout to establish pre-existing origin.

5. Other attacks without confirmed findings

- Aggregate load/decide/append retry and recorded-command paths, applying redactions and older event schema/upcaster handling; known stale-snapshot provenance work explicitly excluded.
- SQLite append transaction completion, inline callback rollback, admission reserve savepoints, cross-tenant callback restrictions, and registration lock ordering.
- Integer conversion/checked admission limits, page bounds, stream/feed cursor progression, and snapshot schema mismatch fallback.
- Tenant erasure's persisted/legacy projection roster traversal, literal prefix boundary checking, indexed column shape admission, scoped counter deletion, and transactional rollback on ambiguous tables.
- Projection registration/rebuild/queries, temporary table lifecycle, SQLite blocking/runtime context handling, and durable claim lookup semantics.
- No additional actionable finding is asserted from these surfaces in this pass.

6. Outside-worktree paths

None. Tests and logs remain under the assigned managed tree. This report and input-validation-red.log / broader-review-suite.log are under target/review-scratch. The new test file has intent-to-add index state so git diff can show the tests-only scope; no commit or planning mutation was made. Own lease released at handoff.

```findings
- file: crates/eventlog-core/src/lib.rs
  line: 947
  category: boundary
  severity: blocker
  verdict: NEEDS-CHANGE
  origin: undecided
  message: Public deserialization and mutable fields bypass constructor invariants, and append persists scalar event bodies, empty tenant IDs, and invalid claim fields without rejection.
```
