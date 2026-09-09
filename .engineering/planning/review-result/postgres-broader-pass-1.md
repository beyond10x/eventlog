---
format: aep.planning-md/1
id: review-result:postgres-broader-pass-1
kind: review-result
status: active
title: PostgreSQL broader review pass 1
relations:
- reviews: story:reuse-postgres-connections-after-empty-catch-up
revision: 1
---
unit: PostgreSQL runtime, schema admission, pool and proof tooling at 15c049633363dd0f504c7ccaeb36ee01c1bb4bd6 plus tests-only review diff
verdict: CONFIRMED
cases: executed 29→31, red 2
origin: introduced 0 / pre-existing 0 / undecided 1
wrote-outside-worktree: none
needs-coordinator: fix schema::shape rewrite-rule admission and make both regressions mandatory in production-proof

 crates/eventlog-postgres/tests/conformance.rs | 90 +++++++++++++++++++++++++++
 1 file changed, 90 insertions(+)

## Added cases and first execution
Both tests are in crates/eventlog-postgres/tests/conformance.rs. No production file or existing test was modified.

schema_admission_refuses_rules_that_suppress_command_receipts installs a PostgreSQL ON INSERT DO INSTEAD NOTHING rewrite rule on the commands table before reconnecting. The current public local constructor accepts it. Two identical appends then produce versions1 and2, retry_deduplicated=false, and durable_receipt=None. The test is red now.

hosted_schema_admission_refuses_rewrite_rules independently installs the same unsupported shape using a migration role, grants the dedicated application role only DML/sequence usage, and calls PostgresEventStore::open with verified TLS and valid connection admission. Hosted admission also accepts it. The test is red now.

Each exact case was run alone immediately after creation and before the broader suite. Commands used cargo test -p eventlog-postgres --test conformance --locked <exact-case-name> -- --exact --nocapture, exit101 each. The real fixture uses PostgreSQL17.6 on the assigned port32875, the assigned target/review-scratch/tls/ca.crt and EVENTLOG_TEST_HOSTED_POSTGRES_URL. No fabricated backend or mocked adapter was used.

First local-case raw output:
    Blocking waiting for file lock on package cache
   Compiling ring v0.17.14
   Compiling rustls v0.23.43
   Compiling rustls-webpki v0.103.15
   Compiling tokio-rustls v0.26.5
   Compiling tokio-postgres-rustls v0.14.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 5.82s
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 1 test
admitted rewrite rule: first_version=1, retry_version=2, retry_deduplicated=false, durable_receipt=None

thread 'schema_admission_refuses_rules_that_suppress_command_receipts' (861815) panicked at crates/eventlog-postgres/tests/conformance.rs:38:13:
schema admission accepted a rewrite rule that discards durable command receipts
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test schema_admission_refuses_rules_that_suppress_command_receipts ... FAILED

failures:

failures:
    schema_admission_refuses_rules_that_suppress_command_receipts

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.25s

error: test failed, to rerun pass `-p eventlog-postgres --test conformance`

First hosted-case raw output:
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 1.40s
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 1 test

thread 'hosted_schema_admission_refuses_rewrite_rules' (874140) panicked at crates/eventlog-postgres/tests/conformance.rs:66:13:
verified TLS/application-role admission accepted a command-discarding rewrite rule
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test hosted_schema_admission_refuses_rewrite_rules ... FAILED

failures:

failures:
    hosted_schema_admission_refuses_rewrite_rules

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 30 filtered out; finished in 0.49s

error: test failed, to rerun pass `-p eventlog-postgres --test conformance`

## Suite after the new cases
Command: cargo test -p eventlog-postgres --test conformance --locked -- --nocapture --test-threads=1
Exit101. Executed29→31, from the prior implementor production-proof summary29 to this runner's31. The29 existing cases still pass; the two new cases fail. TLS/application-role fixtures configured, no skips.
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-655da8b1406c/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 1.48s
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 31 tests
test a_feed_cursor_cannot_pass_an_inflight_lower_position_with_a_newer_xid ... ok
test a_reader_never_skips_an_event_that_committed_late ... ok
test a_table_of_ours_that_somebody_else_made_is_refused_by_name ... ok
test absent_deployment_scope_races_across_tenants_and_clients ... ok
test caught_reservation_cancellation_cannot_commit_an_unchecked_append ... ok
test contended_catch_up_reuses_the_same_settled_session ... catch-up reuse: contended=true, eight no-work polls retained backend 793, cursor=1, tally=1
ok
test empty_catch_up_reuses_the_same_settled_session ... catch-up reuse: contended=false, eight no-work polls retained backend 797, cursor=1, tally=1
ok
test hosted_schema_admission_refuses_rewrite_rules ... 
thread 'hosted_schema_admission_refuses_rewrite_rules' (878792) panicked at crates/eventlog-postgres/tests/conformance.rs:98:13:
verified TLS/application-role admission accepted a command-discarding rewrite rule
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
FAILED
test incomplete_companion_schema_is_refused_before_serving ... ok
test independent_first_appends_return_contract_outcomes ... ok
test inline_failure_preserves_all_atomic_state_and_callback_authority ... ok
test isolated_transport_cannot_hide_a_remote_address_behind_localhost ... ok
test killed_projector_restarts_competing_workers_without_partial_view_or_cursor ... ok
test legacy_populated_schema_migrates_atomically_and_unknown_checksums_refuse ... ok
test lost_commit_response_reconnects_to_exact_durable_receipt ... ok
test missing_scope_bootstrap_is_atomic_across_independent_processes ... ok
test pool_observation_preserves_two_connections_and_four_waiters ... public pool profile2/4: queued4, committed1, cancelled1, query_success=6, overload=58, samples=2839
ok
test public_queue_cancellation_and_broken_idle_reclaim_exact_capacity ... public queue: four cancellations, four acquisition deadlines, four overloads, one granted-unpolled cancellation, own idle backend 865 terminated; reconnect transport_refusals=0
ok
test rebuild_preserves_other_tenants_and_previous_view_on_failure ... ok
test registration_freezes_and_pool_refuses_bounded_overload ... ok
test schema_admission_refuses_rules_that_suppress_command_receipts ... admitted rewrite rule: first_version=1, retry_version=2, retry_deduplicated=false, durable_receipt=None

thread 'schema_admission_refuses_rules_that_suppress_command_receipts' (881132) panicked at crates/eventlog-postgres/tests/conformance.rs:52:13:
schema admission accepted a rewrite rule that discards durable command receipts
FAILED
test schema_admission_refuses_triggers_policies_generation_and_foreign_sequences ... ok
test scope_reservations_are_atomic_and_confined ... ok
test shutdown_cancellation_preserves_public_pool_lifetimes ... public shutdown cancellation: queued Closed; two bounded shutdown deadlines; accepted command retained; repeated shutdown drained 0/0/0
ok
test the_claim_rule_holds_on_postgresql ... ok
test the_inline_projection_exercise_passes_on_postgresql ... ok
test the_paging_rule_holds_on_postgresql ... ok
test the_projection_exercise_passes_on_postgresql ... ok
test the_shared_exercise_passes_on_postgresql ... ok
test unsettled_catch_up_rollback_never_recycles_a_session ... catch-up rollback: cancel=false, response withheld with occupancy=1/0/0; shutdown drained=0/0/0
catch-up rollback: cancel=true, response withheld with occupancy=1/0/0; shutdown drained=0/0/0
ok
test verified_tls_requires_matching_server_and_separate_application_role ... ok

failures:

failures:
    hosted_schema_admission_refuses_rewrite_rules
    schema_admission_refuses_rules_that_suppress_command_receipts

test result: FAILED. 29 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 13.97s

error: test failed, to rerun pass `-p eventlog-postgres --test conformance`

## Finding
| Source | Verdict | Origin | Measured result | Reachable caller |
|---|---|---|---|---|
| crates/eventlog-postgres/src/schema.rs:144 | CONFIRMED, P1 | undecided | Hidden INSERT rewrite rules pass shape admission; suppressing commands creates duplicate durable events on identical retries and leaves no recovery receipt. Two exact PostgreSQL tests fail, including verified TLS/application-role open. | PostgresEventStore::local calls schema::migrate at src/lib.rs:97; PostgresEventStore::open calls schema::validate at src/lib.rs:173; both reach shape. The subsequent ordinary EventStore::append(..., Expected::Any, ...) uses the admitted schema. |

The gap covers every admitted relation: shape checks triggers, row security, generation, collations and inheritance but never pg_rewrite. The same check is used for declared projections. Reject unsupported rewrite rules centrally rather than special-casing commands, and add both tests to the mandatory production-proof roster. No DDL or public API change is required.

Origin remains undecided under the adversary charter: the attack was executed against the supplied15c0496 tree, not a separately assigned historical-base tree. Static history is insufficient for a pre-existing execution claim.

## Remaining reviewed surfaces
Read the pool acquisition/queue accounting, cancellation and driver retirement paths; no additional confirmed defect from this pass. Existing pool observations remain in the implementor gate, not rerun as independent review evidence here.
Read append admission savepoints, caught cancellation poison, tenant-scoped projection operations, publication/watermark locks and rebuild view/cursor transaction boundaries; no additional reachable failing scenario established.
Read schema migration ledger, projection roster/shape, dedicated hosted-role privileges and connection limits; rewrite rules are the one confirmed admission gap.
Read production-proof required-case selection, comparative immutable-source manifests and child command exits, capacity metric failure admission, restart exact-envelope/receipt replay; no additional confirmed issue from this bounded pass. Comparative/restart workloads were not rerun for this static review.
Known snapshot/redaction defect and completed catch-up fix were outside this follow-up's attack scope.

## Handoff
Tests remain uncommitted for the coordinator, in the assigned managed tree. Exact transferable patch: target/review-scratch/rewrite-rule-tests.patch. Raw evidence: rewrite-rule-red.log, hosted-rewrite-rule-red.log, broader-conformance-red.log. No paths written outside the assigned tree. No implementation, AEP, release, cleanup, or container lifecycle change performed. Review lease released at handoff.

```findings
- file: crates/eventlog-postgres/src/schema.rs
  line: 144
  category: contract-drift
  severity: blocker
  verdict: CONFIRMED
  origin: undecided
  message: Schema admission ignores PostgreSQL rewrite rules, allowing a commands INSERT rule to discard durable receipts and duplicate events on ordinary same-command retries, including after verified TLS application-role admission.
```
