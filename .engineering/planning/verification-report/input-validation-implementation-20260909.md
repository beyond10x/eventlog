---
format: aep.planning-md/1
id: verification-report:input-validation-implementation-20260909
kind: verification-report
status: draft
title: Public input validation repair receipts
relations:
- reviews: story:validate-public-append-inputs
revision: 1
---
unit:                   story:validate-public-append-inputs — Enforce constructor invariants at public input boundaries
verdict:                green
cases:                  executed 88→94, red 3
origin:                 n/a
wrote-outside-worktree: none
needs-coordinator:      yes — publish commit and add exact required-case roster below; retain evidence before cleanup

1. Unit and acceptance

Both real supported backends reject constructor-invalid public input before events, receipts, claims or projections are written. Implemented commit eae613186532260679fb16f82e0a56e7c3faf44c on impl/validate-public-append-inputs at base5c1ff2690f07ec0b91f14cce307fff2a6b65ddca. Author and committer both verified as b10x-bot[bot] <316511680+b10x-bot[bot]@users.noreply.github.com>.

TenantId and StreamId Deserialize now call the existing constructors. NewEvent::validate and Claim::validate reuse existing constructor restrictions, and common append validation checks every event and CommandMeta's optional claim. Public fields and valid wire representations remain unchanged. No additional personal-text or schema-version constraints were introduced beyond the constructors' existing promises.

2. Diff
commit eae613186532260679fb16f82e0a56e7c3faf44c
Author: b10x-bot[bot] <316511680+b10x-bot[bot]@users.noreply.github.com>

    fix: validate deserialized and mutated append inputs

 crates/eventlog-conformance/src/lib.rs             | 157 +++++++++++++++++++++
 crates/eventlog-core/src/lib.rs                    | 101 +++++++++++--
 crates/eventlog-postgres/tests/input_validation.rs |  24 ++++
 .../tests/input_validation_review.rs               | 110 +++++++++++++++
 4 files changed, 381 insertions(+), 11 deletions(-)

3. Red evidence, preserved before implementation

The original three retained SQLite regressions were created before this implementation; their first executed run is input-validation-red.log from the prior adversary phase. Exact command: TMPDIR="$PWD/target/review-scratch" CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p eventlog-sqlite --test input_validation_review --locked. Exit101; all3 failed with accepted invalid records.
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

New shared exercise was added before the fix. Initial both-backends-red.log contains a real SQLite rejection failure and a PostgreSQL test-fixture prefix mistake; the prefix error is NOT counted as a product regression. After fixing the fixture (legal lowercase-only prefix), event/claim mutation runs below prove both actual backends detect removed validators. A subsequent invalid-NUL claim lookup fixture was adjusted because PostgreSQL rejects NUL query parameters; the test still demands Invalid on NUL append inputs, checks no event/receipt/projection writes, and checks a valid claim namespace remains absent. All other invalid claim coordinates are queried exactly as well.

Mutation1 removed event.validate at the shared append boundary. Mutation2 removed claim.validate in CommandMeta. Each targeted both-backend command exited101 with both wrappers red. Commands use TMPDIR="$PWD/target/review-scratch" EVENTLOG_TEST_POSTGRES_URL=postgres://postgres@127.0.0.1:32876/postgres CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --workspace --locked public_input_validation_is_atomic --no-fail-fast.
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 1.89s
     Running unittests src/lib.rs (target/debug/deps/eventlog_conformance-81307400d91e72d5)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_core-6e06060c4a60d048)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 13 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_postgres-69070d9c12dde61c)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out; finished in 0.00s

     Running tests/adversary.rs (target/debug/deps/adversary-ea79296f5cdc97de)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 32 filtered out; finished in 0.00s

     Running tests/input_validation.rs (target/debug/deps/input_validation-e7f2754254be9939)

running 1 test
test public_input_validation_is_atomic_on_postgresql ... FAILED

failures:

---- public_input_validation_is_atomic_on_postgresql stdout ----

thread 'public_input_validation_is_atomic_on_postgresql' (1267920) panicked at crates/eventlog-conformance/src/lib.rs:2118:9:
case 0 must fail before writing the first valid event
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    public_input_validation_is_atomic_on_postgresql

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.83s

error: test failed, to rerun pass `-p eventlog-postgres --test input_validation`
     Running tests/runtime_context.rs (target/debug/deps/runtime_context-936f9f892482b809)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_sqlite-4118c9a17598b586)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/adversary.rs (target/debug/deps/adversary-231eab852eb00512)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s

     Running tests/conformance.rs (target/debug/deps/conformance-9cca51e8c05339a7)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out; finished in 0.00s

     Running tests/evolution.rs (target/debug/deps/evolution-1948f69ba95fd731)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

     Running tests/input_validation_review.rs (target/debug/deps/input_validation_review-9461c03b7fa23a39)

running 1 test
test public_input_validation_is_atomic_on_sqlite ... FAILED

failures:

---- public_input_validation_is_atomic_on_sqlite stdout ----

thread 'public_input_validation_is_atomic_on_sqlite' (1268228) panicked at crates/eventlog-conformance/src/lib.rs:2118:9:
case 0 must fail before writing the first valid event
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    public_input_validation_is_atomic_on_sqlite

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p eventlog-sqlite --test input_validation_review`
     Running tests/legacy_namespaces.rs (target/debug/deps/legacy_namespaces-a6f3ff8640f3614e)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

     Running tests/repository.rs (target/debug/deps/repository-236462b0c2b57479)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 12 filtered out; finished in 0.00s

     Running tests/runtime_context.rs (target/debug/deps/runtime_context-e6b1511c66680dc7)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

error: 2 targets failed:
    `-p eventlog-postgres --test input_validation`
    `-p eventlog-sqlite --test input_validation_review`
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 1.69s
     Running unittests src/lib.rs (target/debug/deps/eventlog_conformance-81307400d91e72d5)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_core-6e06060c4a60d048)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 13 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_postgres-69070d9c12dde61c)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out; finished in 0.00s

     Running tests/adversary.rs (target/debug/deps/adversary-ea79296f5cdc97de)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 32 filtered out; finished in 0.00s

     Running tests/input_validation.rs (target/debug/deps/input_validation-e7f2754254be9939)

running 1 test
test public_input_validation_is_atomic_on_postgresql ... FAILED

failures:

---- public_input_validation_is_atomic_on_postgresql stdout ----

thread 'public_input_validation_is_atomic_on_postgresql' (1274138) panicked at crates/eventlog-conformance/src/lib.rs:2118:9:
case 2 must fail before writing the first valid event
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    public_input_validation_is_atomic_on_postgresql

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.24s

error: test failed, to rerun pass `-p eventlog-postgres --test input_validation`
     Running tests/runtime_context.rs (target/debug/deps/runtime_context-936f9f892482b809)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/eventlog_sqlite-4118c9a17598b586)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/adversary.rs (target/debug/deps/adversary-231eab852eb00512)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s

     Running tests/conformance.rs (target/debug/deps/conformance-9cca51e8c05339a7)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out; finished in 0.00s

     Running tests/evolution.rs (target/debug/deps/evolution-1948f69ba95fd731)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

     Running tests/input_validation_review.rs (target/debug/deps/input_validation_review-9461c03b7fa23a39)

running 1 test
test public_input_validation_is_atomic_on_sqlite ... FAILED

failures:

---- public_input_validation_is_atomic_on_sqlite stdout ----

thread 'public_input_validation_is_atomic_on_sqlite' (1274201) panicked at crates/eventlog-conformance/src/lib.rs:2118:9:
case 2 must fail before writing the first valid event
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    public_input_validation_is_atomic_on_sqlite

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p eventlog-sqlite --test input_validation_review`
     Running tests/legacy_namespaces.rs (target/debug/deps/legacy_namespaces-a6f3ff8640f3614e)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

     Running tests/repository.rs (target/debug/deps/repository-236462b0c2b57479)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 12 filtered out; finished in 0.00s

     Running tests/runtime_context.rs (target/debug/deps/runtime_context-e6b1511c66680dc7)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

error: 2 targets failed:
    `-p eventlog-postgres --test input_validation`
    `-p eventlog-sqlite --test input_validation_review`

Mutation3 bypassed TenantId::new during deserialization. Mutation4 bypassed StreamId::new. Each targeted retained stream regression failed with a real accepted invalid stream. Command: TMPDIR="$PWD/target/review-scratch" CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p eventlog-sqlite --test input_validation_review --locked append_refuses_deserialized_streams. Exit101 each. All four mutants were reverted before final production gate.
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
    Finished `test` profile [unoptimized] target(s) in 1.86s
     Running tests/input_validation_review.rs (target/debug/deps/input_validation_review-914d5ffd7d995a3b)

running 1 test
test append_refuses_deserialized_streams_that_bypass_constructor_checks ... FAILED

failures:

---- append_refuses_deserialized_streams_that_bypass_constructor_checks stdout ----

thread 'append_refuses_deserialized_streams_that_bypass_constructor_checks' (1276420) panicked at crates/eventlog-sqlite/tests/input_validation_review.rs:55:9:
constructor-invalid stream was appended: Ok(AppendResult { first_version: 1, last_version: 1, events: [RecordedEvent { global_seq: 1, tenant: TenantId(""), stream_type: "item", stream_id: "one", version: 1, event_id: "01a083a7-f7d9-7b60-9cc1-2bbf225a5798", name: "item.received", schema_version: 1, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-09 0:53:38.393520994 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-command", trace_id: "trace-command", causation_id: None, causation_depth: 0, redacted_at: None, data: Object {"value": Number(1)} }], deduplicated: false })
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    append_refuses_deserialized_streams_that_bypass_constructor_checks

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p eventlog-sqlite --test input_validation_review`
   Compiling eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
   Compiling eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
   Compiling eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
    Finished `test` profile [unoptimized] target(s) in 0.81s
     Running tests/input_validation_review.rs (target/debug/deps/input_validation_review-914d5ffd7d995a3b)

running 1 test
test append_refuses_deserialized_streams_that_bypass_constructor_checks ... FAILED

failures:

---- append_refuses_deserialized_streams_that_bypass_constructor_checks stdout ----

thread 'append_refuses_deserialized_streams_that_bypass_constructor_checks' (1278530) panicked at crates/eventlog-sqlite/tests/input_validation_review.rs:55:9:
constructor-invalid stream was appended: Ok(AppendResult { first_version: 1, last_version: 1, events: [RecordedEvent { global_seq: 1, tenant: TenantId("tenant"), stream_type: "", stream_id: "one", version: 1, event_id: "01a083a8-3d53-7383-a0e1-e2f00b63a498", name: "item.received", schema_version: 1, occurred_at: 1970-01-01 0:00:00.0 +00:00:00, recorded_at: 2026-09-09 0:53:56.179491025 +00:00:00, subject: "person-1", actor: "service-1", request_id: "request-command", trace_id: "trace-command", causation_id: None, causation_depth: 0, redacted_at: None, data: Object {"value": Number(1)} }], deduplicated: false })
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    append_refuses_deserialized_streams_that_bypass_constructor_checks

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p eventlog-sqlite --test input_validation_review`

4. Green full production gate

Command:
TMPDIR="$PWD/target/review-scratch" EVENTLOG_TEST_POSTGRES_URL=postgres://postgres@127.0.0.1:32876/postgres EVENTLOG_TEST_HOSTED_POSTGRES_URL=postgres://eventlog_test_application@127.0.0.1:32876/postgres EVENTLOG_TEST_POSTGRES_CA="$PWD/target/review-scratch/validation/tls/ca.crt" CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 bash scripts/gate.sh --production-proof

Exit0. Receipt passed94, failed0, skipped0, missing_required_cases=[], conformance_valid=true. PostgreSQL17.6. Fmt/clippy green. This was the exact final source diff before commit; receipt source_revision is base5c1ff269 and source_dirty=true, and the unchanged tree was then committed as eae6131. Root owns final combined clean-revision proof.

Baseline88 came from coordinator's completed combined production gate, not a repeated baseline build. Per-lane runner counts: core12→13; postgres units9→9; postgres adversary2→2; postgres conformance32→32; new postgres input-validation0→1; postgres runtime2→2; SQLite adversary1→1; SQLite conformance11→11; SQLite evolution3→3; SQLite input-validation0→4; SQLite legacy namespaces2→2; SQLite repository12→12; SQLite runtime2→2. Original lanes unchanged because cases were added in dedicated input targets plus one core unit. Every final lane exit0. Unit/doc targets with zero tests remain zero.

The first full proof attempt used the admin URL as the hosted application URL and correctly failed role ownership admission. Its output is production-gate.log. Environment was corrected to eventlog_test_application and the complete gate rerun. This was fixture setup, not a product-code failure.

Verbatim final gate output:
   Compiling ring v0.17.14
   Compiling rustls v0.23.43
   Compiling rustls-webpki v0.103.15
   Compiling tokio-rustls v0.26.5
   Compiling tokio-postgres-rustls v0.14.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 1.25s
     Running `target/debug/examples/gate --production-proof`
gate: cargo run --locked -p eventlog-postgres --example production-proof
   Compiling ring v0.17.14
   Compiling rustls v0.23.43
   Compiling rustls-webpki v0.103.15
   Compiling tokio-rustls v0.26.5
   Compiling tokio-postgres-rustls v0.14.0
   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 1.40s
     Running `target/debug/examples/production-proof`

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 13 tests
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
test tests::input_validation_preserves_valid_wire_and_exact_field_limits ... ok
test tests::the_same_body_hashes_the_same_way ... ok

test result: ok. 13 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


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

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.02s


running 2 tests
test schema_admission_refuses_inherited_event_children ... ok
test unrelated_xmin_cannot_make_committed_positions_noncontiguous ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.50s


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

test result: ok. 32 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 14.40s


running 1 test
test public_input_validation_is_atomic_on_postgresql ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.47s


running 2 tests
test every_store_method_completes_on_a_current_thread_runtime ... ok
test every_store_method_completes_on_a_multi_thread_runtime ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.77s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test legacy_projection_rows_are_erased_without_reregistering_retired_projectors ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.29s


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

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.75s


running 3 tests
test a_body_written_under_an_older_version_still_folds ... ok
test a_committed_vector_still_folds_when_loaded_from_disk ... ok
test a_version_with_no_upcaster_is_named_rather_than_guessed ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 4 tests
test append_refuses_claim_fields_that_bypass_constructor_checks ... ok
test append_refuses_deserialized_events_that_bypass_constructor_checks ... ok
test append_refuses_deserialized_streams_that_bypass_constructor_checks ... ok
test public_input_validation_is_atomic_on_sqlite ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s


running 2 tests
test ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure ... ok
test legacy_erasure_matches_literal_underscores_and_preserves_other_owners ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.82s


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

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.63s


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

   Compiling eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `test` profile [unoptimized] target(s) in 1.17s
     Running unittests src/lib.rs (target/debug/deps/eventlog_conformance-81307400d91e72d5)
     Running unittests src/lib.rs (target/debug/deps/eventlog_core-6e06060c4a60d048)
     Running unittests src/lib.rs (target/debug/deps/eventlog_postgres-69070d9c12dde61c)
closed-idle replacement attempts before=1, while old driver paused=1
idle checkout snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 1, waiting: 0, idle: 1, closed: false }
quarantine replacement snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 2, waiting: 0, idle: 0, closed: false }
reusable retirement snapshot: PoolStatus { max_connections: 2, max_waiters: 4, checked_out: 1, waiting: 4, idle: 1, closed: false }
shutdown completed: closed=true, checked_out=0, idle=0
     Running tests/adversary.rs (target/debug/deps/adversary-ea79296f5cdc97de)
positions_and_xids=[(1, 3885), (2, 3883)]; unrelated_xid=3884; early_cursor=0; early_count=0; resumed_count=2; all_count=2
     Running tests/conformance.rs (target/debug/deps/conformance-ffc713f485ccb72b)
catch-up reuse: contended=true, eight no-work polls retained backend 305, cursor=1, tally=1
catch-up reuse: contended=false, eight no-work polls retained backend 310, cursor=1, tally=1
public pool profile2/4: queued4, committed1, cancelled1, query_success=6, overload=58, samples=2795
public queue: four cancellations, four acquisition deadlines, four overloads, one granted-unpolled cancellation, own idle backend 375 terminated; reconnect transport_refusals=0
public shutdown cancellation: queued Closed; two bounded shutdown deadlines; accepted command retained; repeated shutdown drained 0/0/0
catch-up rollback: cancel=false, response withheld with occupancy=1/0/0; shutdown drained=0/0/0
catch-up rollback: cancel=true, response withheld with occupancy=1/0/0; shutdown drained=0/0/0
     Running tests/input_validation.rs (target/debug/deps/input_validation-e7f2754254be9939)
     Running tests/runtime_context.rs (target/debug/deps/runtime_context-936f9f892482b809)
     Running unittests src/lib.rs (target/debug/deps/eventlog_sqlite-4118c9a17598b586)
     Running tests/adversary.rs (target/debug/deps/adversary-231eab852eb00512)
retained_projection_after_successful_erasure=None
     Running tests/conformance.rs (target/debug/deps/conformance-9cca51e8c05339a7)
     Running tests/evolution.rs (target/debug/deps/evolution-1948f69ba95fd731)
     Running tests/input_validation_review.rs (target/debug/deps/input_validation_review-9461c03b7fa23a39)
     Running tests/legacy_namespaces.rs (target/debug/deps/legacy_namespaces-a6f3ff8640f3614e)
     Running tests/repository.rs (target/debug/deps/repository-236462b0c2b57479)
     Running tests/runtime_context.rs (target/debug/deps/runtime_context-e6b1511c66680dc7)
   Doc-tests eventlog_conformance
   Doc-tests eventlog_core
   Doc-tests eventlog_postgres
   Doc-tests eventlog_sqlite
{"backend_server":{"version":"17.6","version_num":"170006"},"binary_sha256":"069e20f38398cd9d22cf282c7ba00b871e313d75871adfd234ee284fb12b4640","capacity_admitted":false,"capacity_requirement":"comparative laboratory artifact is separately required; this runner does not manufacture capacity evidence","conformance_valid":true,"default_pool":{"acquisition_ms":2000,"connections":4,"transaction_ms":10000,"waiters":32},"duration_ms":25354,"failed":0,"finished_at":"2026-09-09 0:55:24.316710759 +00:00:00","format":"eventlog-production-proof/1","missing_required_cases":[],"owner_fixture_handoff":["SDK current authority and exact realm/service bindings","SDK original and generated stream/feed/cursor/view/effect vectors"],"passed":94,"runner_summaries":["test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 13 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.02s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.50s","test result: ok. 32 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 14.40s","test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.47s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.77s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.29s","test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.75s","test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.82s","test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.63s","test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s","test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s"],"schema_setup":"test-owned exact prefixes and hosted_owner schema; additive checksum admission exercised","skipped":0,"source_dirty":true,"source_revision":"5c1ff2690f07ec0b91f14cce307fff2a6b65ddca","started_at":"2026-09-09 0:54:58.962115327 +00:00:00"}
gate: cargo fmt --all --check
gate: cargo clippy --workspace --all-targets --locked -- -D warnings
    Checking eventlog-core v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-core)
    Checking ring v0.17.14
    Checking rustls-webpki v0.103.15
    Checking rustls v0.23.43
    Checking eventlog-conformance v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-conformance)
    Checking eventlog-sqlite v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-sqlite)
    Checking tokio-rustls v0.26.5
    Checking tokio-postgres-rustls v0.14.0
    Checking eventlog-postgres v0.1.0-dev.1 (/home/timo/.local/state/worktree/trees/b10x/eventlog/wt-21668c971b1d/crates/eventlog-postgres)
    Finished `dev` profile [unoptimized] target(s) in 3.54s
gate: green

5. Deliberate boundaries and coordinator changes

Required case names to add to the root-owned production-proof roster:
- public_input_validation_is_atomic_on_postgresql
- public_input_validation_is_atomic_on_sqlite
- append_refuses_deserialized_events_that_bypass_constructor_checks
- append_refuses_deserialized_streams_that_bypass_constructor_checks
- append_refuses_claim_fields_that_bypass_constructor_checks
- tests::input_validation_preserves_valid_wire_and_exact_field_limits

README/CHANGELOG prose: Public input validation now preserves TenantId/StreamId constructor restrictions through deserialization and rejects mutated/deserialized invalid event or claim fields at append; valid wire fields and retries remain compatible.

No AEP, backend implementation/schema, existing PostgreSQL conformance target, gate roster, README, CHANGELOG, or Cargo file changed. Separate PostgreSQL admission repair remains with its assigned worker. No container deletion or push performed. Existing feature branch already published by root; this new branch awaits publication by root. Snapshot logic was not modified.

The shared exercise enumerates50 invalid batches: empty,513-byte, newline,NUL,non-ASCII values across event names and3 claim fields, plus null/number/bool/string/list event bodies, each through public mutation and serde reconstruction. Every batch starts with a valid event to expose partial-write errors. It verifies no head, receipt, claim or projection state and an empty feed, then validates a normal append/retry. Every invalid tenant/type/id field is rejected through Deserialize. Positive core test covers exact512-byte fields and legacy printable-space/@ values to prevent accidental policy expansion.

6. Outside-tree paths and retention

None. Task-generated TLS keys/certificates remain ONLY in the explicit ignored target/review-scratch/validation/tls directory and the assigned disposable container; never publish those private files. Report/raw logs live under target/review-scratch/validation; prior three-red evidence remains target/review-scratch/input-validation-red.log. Existing original regression patch remains target/review-scratch/input-validation-regression.patch. Build output is task-local target/ (about1.7GiB at handoff); no shared CARGO_TARGET_DIR was used. Preserve reports/logs before coordinator cleanup. Own lease released at handoff; tracked tree clean.
