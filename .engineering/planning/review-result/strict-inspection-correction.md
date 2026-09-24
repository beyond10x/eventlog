---
format: aep.planning-md/2
id: review-result:strict-inspection-correction
kind: review-result
status: active
title: Independent inspection correction re-review
relations:
- reviews: story:strict-read-only-history-inspection
revision: 1
---
unit: strict-read-only-history-inspection, fd8a96b plus coordinator correction working tree
verdict: nothing found in the bounded re-review; all three original findings resolved
cases: executed 8→9 in the targeted review lane, red 0
origin: introduced 0 / pre-existing 0 / undecided 0
wrote-outside-worktree: assigned scratch reports/logs and existing assigned compiler target
needs-coordinator: coordinator mutation and full production gate remain

```text
git --no-pager diff --stat
 .engineering/planning/journal.jsonl                 |  2 +
 Cargo.lock                                         | 19 +++++
 crates/eventlog-conformance/src/lib.rs              |  2 +
 crates/eventlog-core/src/lib.rs                     |  2 +
 crates/eventlog-file/src/journal.rs                 | 96 +++++++++++++++++++++-
 crates/eventlog-file/src/lib.rs                     |  2 +
 .../eventlog-postgres/examples/production-proof.rs  | 27 ++++++
 crates/eventlog-sqlite/Cargo.toml                    |  3 +
 crates/eventlog-sqlite/src/lib.rs                    |  2 +
 docs/design/strict-history-inspection.md            |  7 ++
 10 files changed, 161 insertions(+), 1 deletion(-)
```

Owners: the whole-tree diff is shared work, not a reviewer-owned diff. The coordinator owns the three production corrections, the new positive case, required roster entries, and concurrent disjoint planning/design/report changes. The earlier implementor owns the inspection implementation being corrected. This reviewer owns the original two adversary files and scratch review outputs; this re-review made no repository edit at all. The reviewer attributes the original source findings and their resolution to the coordinator for correction tracking, without changing their originally undecided historical origin. No source authorship or approval is inferred from a passing review.

The original test files matched the dispatched hashes both before and after execution:

```text
f0564cb06c560171f1329555bfe8d08531a8d8eaf507d29f519aafa2df847874  crates/eventlog-file/tests/adversary_strict_inspection.rs
c544b747ac7ac6461879fa5e44e4e543318afde143b744a36a3d9beb5b75a647  crates/eventlog-sqlite/tests/adversary_strict_inspection.rs
```

No test was added, deleted, weakened, renamed or skipped in this re-review. The first review's behavioral red output remains in adversary-report.md and its raw logs. This pass checks only those three findings and the requested valid-body/schema-zero control.

The targeted lane previously ran the eight adversary cases. It now runs those same eight plus one coordinator-authored positive case: nine executed, nine passed, zero ignored. This is a selected lane count, not a claim to have independently rerun the full 103-case package lane. The coordinator's correction-packages.log was read and its per-binary totals sum to 103 passes, with no failures or ignored cases.

Source assessment and original finding outcomes:

| Original finding | Coordinator correction | Observed result |
|---|---|---|
| F1: File repeated persisted event identity | File inspection tracks returned event_id values and refuses repeated identity with CorruptSource. It leaves valid global-position gaps intact. | Unchanged duplicate-event case passes. Ordinary fixture positions 1, 3, 4 remain accepted. |
| F2: unsupported nested RecordedEvent metadata discarded | File inspection checks retained raw envelope keys against the existing RecordedEvent serialization before native replay. It still uses the native journal and operation parser, and preserves body objects. | Unchanged unknown-envelope case passes. New arbitrary-body-fields/schema-zero control passes with exact retained events and source bytes. |
| F3: malformed recorded envelopes admitted by both providers | Both inspectors call RecordedEvent::validate_for_inspection. It checks recorded sequence/version, causation depth, object body, stream, fields, identities and optional causation id using existing core field/identity rules. Redaction remains a named refusal. | Unchanged File and SQLite cases pass for all four independently measured variants: empty event identity, event name, actor, and array data. |

The common validator matches the applicable NewEvent and CommandMeta admission rules for fields actually retained in RecordedEvent. It neither fabricates absent command-only fields nor requires schema version to be positive. Existing general provider deserialization and persisted formats remain unchanged. File's raw-key check compares only envelope keys; it does not reject keys recursively inside user data. No second journal parser was introduced.

All commands used:
`CARGO_TARGET_DIR=<cache>/b10x-target/ekr-eventlog-inspection-20260922`,
`CARGO_BUILD_JOBS=2`,
`TMPDIR=<cache>/ekr-completion-20260922/eventlog-inspection/adversary`.
Free disk was checked before each cargo command and was 22 GB, above the 10 GB floor.

Command:
`cargo test --locked -p eventlog-file -p eventlog-sqlite --test adversary_strict_inspection --no-fail-fast`

Exit 0. Output, with only private path prefixes substituted:

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.05s
     Running tests/adversary_strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary_strict_inspection-f100e414764d74ff)

running 4 tests
test adversary_file_empty_identity_refuses_and_zero_result_caps_allow_absence ... ok
test adversary_file_unknown_recorded_envelope_field_is_not_discarded ... ok
test adversary_file_duplicate_event_identity_is_corruption ... ok
test adversary_file_inadmissible_recorded_envelope_is_corruption ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.46s

     Running tests/adversary_strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/adversary_strict_inspection-843029f592bb4282)

running 4 tests
test adversary_sqlite_inadmissible_recorded_envelope_is_corruption ... ok
test adversary_sqlite_empty_identity_and_exact_source_cap ... ok
test adversary_sqlite_success_preserves_preexisting_reader_lock ... ok
test adversary_sqlite_public_descriptor_bound_keeps_prior_writer_lock ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.85s


```

Command:
`cargo test --locked -p eventlog-file --test strict_inspection file_inspection_body_fields_and_zero_schema_are_preserved -- --exact --nocapture`

Exit 0. Output, with only private path prefixes substituted:

```text
   Compiling eventlog-core v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-core)
   Compiling eventlog-conformance v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-conformance)
   Compiling eventlog-file v0.2.1 (<worktrees>/eventlog/ekr-eventlog-inspection-20260922/crates/eventlog-file)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 2.23s
     Running tests/strict_inspection.rs (<cache>/b10x-target/ekr-eventlog-inspection-20260922/debug/deps/strict_inspection-bb798948664ee6b0)

running 1 test
test file_inspection_body_fields_and_zero_schema_are_preserved ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; finished in 0.03s


```

The four non-finding controls in the unchanged adversary files also pass: empty identity and absence/limits for both providers, successful SQLite inspection preserving a native same-process reader lock, and 64-FD lifetime registry/refusal preserving a native same-process writer lock. The last two still assert actual subprocess outcomes before and after inspection/refusal. The positive test writes through the actual FileEventStore with schema version zero and nested body keys that resemble malformed envelope fields, then compares complete returned events and source bytes.

No new broad exploration, mutation, PostgreSQL deployment check or full gate was performed in this bounded pass. No new findings remain to carry into the findings block. This is an observed result, not approval or a claim that untested paths are correct.

Public aliases use the same meaning as the first report. Additional persistent writes outside the tree are:
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/correction-eight.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/correction-positive.log`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/correction-review.md`
- `<cache>/ekr-completion-20260922/eventlog-inspection/adversary/correction-outside-paths.txt`
- Existing compiler target `<cache>/b10x-target/ekr-eventlog-inspection-20260922`

Synthetic fixtures lived under the assigned TMPDIR and used normal fixture-lifetime cleanup. Exact local paths are in correction-outside-paths.txt. Worktree session state was changed only by the managed hook commands for this reviewer's own session, codex-ekr-inspection-correction-review. No live stores, AEP commands, commits, publication, source edits, or managed-tree cleanup occurred. Source and tests are stable, all compiler processes for this pass have finished, and the own session lease is released at handback.

```findings
[]
```

