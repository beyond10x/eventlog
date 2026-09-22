unit: story:atomic-blob-append — Commit blob bindings and ordered event appends atomically
verdict: green
cases: executed 156→176, red 0
origin: n/a
wrote-outside-worktree: assigned scratch, assigned Cargo target, managed-session metadata, coordinator-owned synthetic PostgreSQL fixture; full private inventory retained separately
needs-coordinator: no unapplied source patch; independent review, normative status, integration and publication remain coordinator-owned

## 1. Unit and acceptance

File, SQLite and PostgreSQL now implement the opt-in `AtomicBlobEventStore`: one native transaction publishes tenant-scoped blob bindings, ordered appends, callback effects and the original group receipt; a retry returns the original result without callbacks or content resurrection.

Opening source: `fd7ce06d31796bcf9e90f8388d26c0cfe3f3e175`. Branch: `codex/ekr-eventlog-atomic-content-20260922`. No commit was made. The source was stable and all deliberate mutations restored before the final full suite and production gate. `source-sha256.txt` verifies the 12 authored source/test files. Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`, `cargo 1.98.1 (797e8a9bc 2026-08-05)`.

The story's current inferred paths were checked before implementation: both new `src/atomic_blob.rs` modules and all three provider `tests/atomic_blob.rs` files were absent. The observed story scope at revision 19 includes every authored path. The earlier scoper document's proposed `atomic_blob_group.rs` name was superseded by that recorded story scope. The existing group engines, native transactions, File journal operations and test harnesses matched the cited mechanisms; conformance and mutation evidence below exercise the resulting transaction behavior. No inferred location required an additional source path.

Security and persistence choices:

- The new fingerprint has explicit `eventlog-blob-append-group/1` discrimination, the unchanged group fingerprint, and sorted `{digest, byte_count, content_sha256}` entries. SHA-256 hashes the actual bytes, including empty content; caller-provided digest and request hash are not treated as proof of content equality. Empty blob sets, duplicate keys and malformed requests refuse. Legacy fingerprint code remains unchanged and has a frozen vector.
- All providers share the existing group identity namespace. Receipt reconstruction precedes binding, admission and projection. Equal existing bytes are reused; different bytes refuse. Tentative bindings are visible to tenant-confined guard and projector reads. Native transaction rollback covers all newly installed bindings and callback effects.
- SQLite uses the existing `BEGIN IMMEDIATE` and table. Only the new capability classifies failure after COMMIT begins as `UnknownCommit`; resolution uses the original receipt identity.
- PostgreSQL preserves publication, group and stream lock ordering. Sorted blob binding uses native uniqueness plus `SELECT ... FOR UPDATE`, which also serializes standalone deletion. Pool quarantine and unknown-commit resolution remain native to the existing transaction path.
- File keeps the existing Blob/Event/Group journal format. New physical objects are tracked as owned only after successful `create_new`; known aborts clean only those objects. Explicit cleanup failure reports retained unbound staging. An interrupted or uncertain publication never triggers destructive compensation. Post-publication cleanup failure is `UnknownCommit` for this capability, with original content and receipt recoverable. Test-only fault hooks are compiled under `cfg(test)`.
- The log contains blob references and hashes, never these payload bytes. No DDL, new File operation, dependency, lockfile, legacy fingerprint or inspector change was needed.

## 2. Observed diff and ownership

`git --no-pager diff --stat` at handback, including the coordinator's one ESS correction:

```text
 crates/eventlog-conformance/src/lib.rs             |   2 +
 crates/eventlog-core/src/lib.rs                    |   2 +
 crates/eventlog-file/src/journal.rs                | 134 +++++++++++-
 crates/eventlog-file/src/lib.rs                    | 228 ++++++++++++++++-----
 .../eventlog-postgres/examples/production-proof.rs |  20 ++
 crates/eventlog-postgres/src/atomic_group.rs       |  82 +++++++-
 crates/eventlog-sqlite/src/atomic_group.rs         | 141 ++++++++++++-
 ess/atomic-content/domains/atomic-content.yaml     |   2 +-
 8 files changed, 555 insertions(+), 56 deletions(-)
```

Git's unstaged stat excludes these five new authored files; `wc -l` output:

```text
  196 crates/eventlog-core/src/atomic_blob.rs
  458 crates/eventlog-conformance/src/atomic_blob.rs
  201 crates/eventlog-file/tests/atomic_blob.rs
  105 crates/eventlog-sqlite/tests/atomic_blob.rs
  419 crates/eventlog-postgres/tests/atomic_blob.rs
 1379 total
```

Authored: the seven tracked Rust diffs and five new Rust files. `authored.patch` includes both tracked and new source. The shared exercise is reused by each provider; provider cases cover reopen, competition and provider-specific failure boundaries. The production roster adds exactly the 20 new case names.

Inherited coordinator edit: `ess/atomic-content/domains/atomic-content.yaml` changes `byte_len` to `byte_count`. The approved fingerprint shape and earlier ESS disagreed. The implementor reported this; the coordinator corrected both trees and validated the ESS. `coordinator-ess.patch` preserves that ownership separately. The fingerprint case now compares the actual preimage's fields with the ESS using a runtime repository path. No ESS, AEP or normative text was authored by this implementor.

## 3. Behavioral red and mutations

The API was first staged with the new fingerprint delegating to the legacy group fingerprint. Before provider implementation, this exact command executed a behavioral failure; `red-first.log` retains all output:

```console
cargo test --locked -p eventlog-core atomic_blob_fingerprint_binds_actual_bytes_not_caller_hashes -- --nocapture
```

```text
running 1 test
legacy fingerprint = a4e85afbb7dd0e722b4b14e49e965b3185a0dadbf517638289edae81ba0a84ea
assertion `left != right` failed
  left: "a4e85afbb7dd0e722b4b14e49e965b3185a0dadbf517638289edae81ba0a84ea"
 right: "a4e85afbb7dd0e722b4b14e49e965b3185a0dadbf517638289edae81ba0a84ea"
test atomic_blob::tests::atomic_blob_fingerprint_binds_actual_bytes_not_caller_hashes ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 13 filtered out; finished in 0.00s
```

Exit: 101. These are verbatim selected runner lines; the complete raw log includes the compilation output and panic location.

The retained Rust mutation harness applies and restores one mutation at a time. Its command template is exact:

```console
cargo test --locked -p <package> --lib <case> -- --exact --nocapture
cargo test --locked -p <package> --test atomic_blob <case> -- --exact --nocapture
```

`mutations.rs` contains the exact replacement and selected case for every row. `mutations-complete.log` records **16/16 executed behavioral failures**, each exit 101 with `test result: FAILED. 0 passed; 1 failed;`. Compilation errors and zero-case selections do not count. Each `mutation-<label>.log` retains full output.

| Mutation label | Broken rule caught |
| --- | --- |
| actual-content | Constant content hash cannot distinguish equal-length changed bytes |
| format-discriminator | Changed fingerprint format is incompatible with frozen preimage |
| duplicate-input | Equal duplicate blob keys must refuse |
| eventlog-sqlite-binding | SQLite must install tentative bindings |
| eventlog-postgres-binding | PostgreSQL must install tentative bindings |
| sqlite-byte-collision | Existing SQLite binding must match actual bytes |
| postgres-byte-collision | Existing PostgreSQL binding must match actual bytes |
| file-byte-collision | Existing File binding must match actual bytes |
| file-owned-cleanup | File known abort removes its unbound staging |
| sqlite-receipt-first | Retry after erasure cannot recreate SQLite content |
| postgres-receipt-first | Retry after erasure cannot recreate PostgreSQL content |
| file-receipt-first | Retry after erasure cannot recreate File content |
| sqlite-unknown-commit | Native SQLite COMMIT failure must have the declared uncertainty outcome |
| postgres-row-lock | Existing content must remain locked while the callback transaction runs |
| postgres-unknown-commit | Lost COMMIT response must resolve through the receipt |
| file-postcommit-unknown | Cleanup failure after File publication cannot report a definite abort |

Two development outcomes are retained rather than counted as successes. The first content-hash mutation survived because the changed test payload also changed length. The test was strengthened to equal-length bytes, and that same mutation then failed. `mutations.log` and `mutation-actual-content-before.log` preserve this. A first PostgreSQL receipt-first mutation selected the wrong function anchor and failed compilation; `mutations-fixed.log` and `mutation-postgres-receipt-first-harness-error.log` preserve it. The harness was corrected to anchor `group_in_transaction`, then all 16 mutations were executed and caught in the complete sweep.

## 4. Final verification

All Cargo commands used the assigned isolated target and scratch, two build jobs and serial test execution. PostgreSQL URLs and the test CA came only from the coordinator-provided environment for its disposable fixture. No URL or private home path is included in this public-ready report; `commands-private.txt` records the complete environment and exact paths privately.

Same-command whole-suite comparison:

```console
cargo test --workspace --locked
```

Baseline `baseline.log`: **156 passed, 0 failed, 0 ignored**, exit 0. Final `full-final.log`: **176 passed, 0 failed, 0 ignored**, exit 0. Both totals are sums of the runner's summary lines, not source attribute counts. No existing lane fell or stopped executing. New integration binaries did not exist on the base; their before value below is explicitly absent, not an invented zero-case baseline run.

| Runner lane | Executed before → after | Exit |
| --- | ---: | ---: |
| conformance library | 0 → 0 | 0 |
| core library | 13 → 16 | 0 |
| File library | 7 → 9 | 0 |
| File adversary_strict_inspection | 4 → 4 | 0 |
| File atomic_blob (new binary) | absent → 5 | 0 |
| File conformance | 11 → 11 | 0 |
| File durability | 7 → 7 | 0 |
| File strict_inspection | 7 → 7 | 0 |
| PostgreSQL library | 10 → 10 | 0 |
| PostgreSQL adversary | 2 → 2 | 0 |
| PostgreSQL atomic_blob (new binary) | absent → 6 | 0 |
| PostgreSQL atomic_groups | 2 → 2 | 0 |
| PostgreSQL conformance | 36 → 36 | 0 |
| PostgreSQL input_validation | 1 → 1 | 0 |
| PostgreSQL runtime_context | 2 → 2 | 0 |
| SQLite library | 7 → 8 | 0 |
| SQLite adversary | 1 → 1 | 0 |
| SQLite adversary_strict_inspection | 4 → 4 | 0 |
| SQLite atomic_blob (new binary) | absent → 3 | 0 |
| SQLite atomic_groups | 3 → 3 | 0 |
| SQLite conformance | 11 → 11 | 0 |
| SQLite evolution | 3 → 3 | 0 |
| SQLite input_validation_review | 4 → 4 | 0 |
| SQLite legacy_namespaces | 2 → 2 | 0 |
| SQLite repository | 12 → 12 | 0 |
| SQLite runtime_context | 2 → 2 | 0 |
| SQLite strict_inspection | 5 → 5 | 0 |
| Each of the five documentation runners | 0 → 0 | 0 |

Complete runner summaries are retained in `runner-summaries.txt`; the raw logs contain every case and its result. The shared exercise is run through the new provider integration binaries, not through the intentionally empty conformance-library runner. The 20 new cases are enumerated by the required production roster and present in `production-proof-raw.log`.

The required production gate ran afterward:

```console
bash scripts/gate.sh --production-proof
```

Exit 0. `production-gate.log` contains:

```text
gate: cargo fmt --all --check
gate: cargo clippy --workspace --all-targets --locked -- -D warnings
gate: green
```

The gate runs each child sequentially and refuses any nonzero status: production runner exit 0, formatter exit 0, Clippy exit 0. `production-proof.json` records `passed:176`, `failed:0`, `skipped:0`, `missing_required_cases:[]`, `conformance_valid:true`, server `17.6`, `source_dirty:true`, source base `fd7ce06d31796bcf9e90f8388d26c0cfe3f3e175` and the running proof binary's SHA-256. The required verified-TLS and separate-application-role case executed and passed. The artifact explicitly has `capacity_admitted:false`; it does not claim comparative deployment capacity or restart approval.

Boundary evidence includes File child-process interruption before publication and after journal publication, a native SQLite deferred-constraint COMMIT failure, and an actual PostgreSQL connection proxy suppressing a committed response. Independent provider handles compete for the same blob keys; the loser has neither its fresh public binding nor its events. PostgreSQL also runs reversed blob-order requests on disjoint streams and tests that standalone deletion waits for the binding row lock. These are synthetic fixture operations, not operator store experiments.

Intermediate compile/lint corrections and the initial repeated PostgreSQL projection-fixture collision remain in their logs. The latter was corrected by resetting the new test's exact projection table along with its existing synthetic prefix; production schema/drop behavior was not changed. Existing assertions were not weakened or marked ignored.

`git diff --check`: exit 0. Final authored-file hash verification: all 12 OK. Privacy scan result is recorded in `privacy-scan.log` after this report was written.

## 5. Boundaries and remaining work

Independent adversary review is still required. The coordinator owns corrective dispatch, integration, final scope citations, the source commit and publication checks. No publication, release, consumer pin update, comparative capacity measurement or container restart is claimed by this local implementation report. Existing strict inspection source and cases remain unchanged. The normative design still says implementation was unexecuted at opening; the coordinator owns updating that status after review.

No separate-process competing-writer case was newly added: new competition tests use independent provider handles, while File crash tests use child processes and existing process-concurrency tests remain in the whole gate. Physical unbound File staging can survive interruption until normal recovery proves it unbound; the API promise is no losing committed public binding. Cleanup uncertainty is never resolved by deleting possibly committed content.

No source change was required outside the approved story scope. No independent put-then-append fallback, product payload in log, schema change, new File journal operation, planning mutation, private policy change or live-store access was performed.

## 6. Outside writes and handback

Public-safe aliases here designate the coordinator-assigned scratch and Cargo target. `outside-paths-private.txt` is the full-path private inventory and must not be published. It records scratch logs, mutation source/binary, temporary test directories and retained child-process outputs, report/patch/hash files, and the isolated Cargo target root. Managed lease metadata was written only through the worktree CLI for session `codex-eventlog-atomic-content-implementor`. Tests wrote synthetic tables in the coordinator-owned disposable PostgreSQL fixture; no container settings or lifecycle were changed.

All source/compiler/test processes started for this unit completed. The implementor's own lease release is recorded in `lease-release.log`; the tree and artifacts are retained for review and coordinator cleanup.

Owners: scoped Rust source/tests and this implementation evidence — implementor; ESS field correction, AEP/normative status, independent review, integration and publication — coordinator.
