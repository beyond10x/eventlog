unit: story:atomic-blob-append at fd7ce06d31796bcf9e90f8388d26c0cfe3f3e175 plus the handed-back atomic-content diff
verdict: nothing found
cases: executed 176→184, red 0
origin: introduced 0 / pre-existing 0 / undecided 0
wrote-outside-worktree: assigned review scratch and Cargo target, managed lease metadata, synthetic PostgreSQL test tables; full private inventory retained
needs-coordinator: add the eight exact new cases to the production roster; final integration and production gate

`git --no-pager diff --stat` remains the inherited implementation/coordinator diff:

```text
 crates/eventlog-conformance/src/lib.rs             |   2 +
 crates/eventlog-core/src/lib.rs                    |   2 +
 crates/eventlog-file/src/journal.rs                | 134 +++++++++++-
 crates/eventlog-file/src/lib.rs                    | 228 ++++++++++++++++-----
 .../eventlog-postgres/examples/production-proof.rs |  20 ++
 crates/eventlog-postgres/src/atomic_group.rs        |  82 +++++++-
 crates/eventlog-sqlite/src/atomic_group.rs          | 141 ++++++++++++-
 ess/atomic-content/domains/atomic-content.yaml     |   2 +-
 8 files changed, 555 insertions(+), 56 deletions(-)
```

These non-test edits were present in the dispatch. All 12 source/test hashes from the implementor's manifest were checked before and after review and remained identical. Git's tracked diff omits all new files. My only repository writes are these new test files:

```text
 crates/eventlog-file/tests/adversary_atomic_blob.rs     | 291 additions
 crates/eventlog-sqlite/tests/adversary_atomic_blob.rs   | 203 additions
 crates/eventlog-postgres/tests/adversary_atomic_blob.rs | 310 additions
```

Owners: the inherited Rust implementation, shared exercise, provider tests and original production roster belong to the implementor; the inherited ESS field correction and future roster/integration changes belong to the coordinator. The eight added test cases, their test-only import correction and this review report belong to the adversary. No implementation, existing test, ESS, design, planning or roster file was changed by this review.

## Cases written before execution

All eight cases were written before the first Cargo invocation. Each then ran alone with an exact name filter and selected one case; all passed on their first behavioral execution. No behavioral red case was observed. The first shell attempt failed to redirect output because the assigned scratch directory had not yet been created; Cargo did not start on that attempt. This is not a behavioral result.

The exact first-run command template was:

```console
cargo test --locked -p eventlog-<provider> --test adversary_atomic_blob <case-name> -- --exact --nocapture
```

Each complete output is retained as `first-<case-name>.log` in the assigned adversary scratch. Every command exited 0. File and PostgreSQL summaries were `1 passed; 0 failed; 0 ignored; 2 filtered out`; SQLite summaries were `1 passed; 0 failed; 0 ignored; 1 filtered out`.

| Exact case | Current outcome |
| --- | --- |
| file_adversary_equal_length_collision_rolls_back | pass |
| file_adversary_rebound_retry_preserves_receipt_and_current_binding | pass |
| file_adversary_cancelled_guard_preserves_existing_content_and_receipt | pass |
| sqlite_adversary_equal_length_collision_rolls_back | pass |
| sqlite_adversary_rebound_retry_preserves_receipt_and_current_binding | pass |
| postgres_adversary_equal_length_collision_rolls_back | pass |
| postgres_adversary_rebound_retry_preserves_receipt_and_current_binding | pass |
| postgres_adversary_cancelled_guard_preserves_existing_content_and_receipt | pass |

The collision cases deliberately keep byte length equal, stage an earlier sorted fresh key, then encounter conflicting existing content. They require refusal, disappearance of only the fresh binding, preservation of existing bytes, no event, and subsequent successful reuse of the refused request identity.

The rebound cases use two real handles. After the original success, another handle deletes and rebinds the same key to different equal-length bytes. An exact old request must resolve the original receipt before callbacks or current binding equality checks, leave the replacement binding unchanged, and return original events and ranges. A changed-byte retry with unchanged caller request hash refuses. A fresh command still checks the replacement binding. Another tenant independently uses the same request and blob keys.

The cancellation cases run actual File and PostgreSQL guarded calls in a task, confirm both tentative and pre-existing content inside the guard and absence of a foreign-only binding, then cancel while the guard is pending. Another handle observes no losing public binding or event, preserved shared and foreign content, and successful non-deduplicated retry. They measure cancellation before publication, not cancellation after COMMIT or process-crash behavior.

## Suite and checks

The 176 baseline is the implementor's reported completed whole-workspace run. No baseline suite was rerun before adding cases. The following full suite ran only after all eight first executions:

```console
cargo test --workspace --locked
```

Exit 0. The complete verbatim output is `workspace.log`; summing its runner summaries gives **184 passed, 0 failed, 0 ignored**. Each provider's new integration target selected File 3, SQLite 2 and PostgreSQL 3. Existing provider, conformance, strict-inspection, crash, concurrency, lost-response and verified-TLS/hosted-role cases executed in that same suite. PostgreSQL variables were required and pointed only at the assigned synthetic fixture; no case silently skipped for missing configuration.

The full run initially warned about an unused import in each new test file. The first targeted Clippy run exited 101 for that test-only import. Removing the unused imports changed no assertion. The final commands were:

```console
cargo test --locked -p eventlog-file -p eventlog-sqlite -p eventlog-postgres --test adversary_atomic_blob -- --nocapture
cargo clippy --locked -p eventlog-file -p eventlog-sqlite -p eventlog-postgres --test adversary_atomic_blob -- -D warnings
cargo fmt --all --check
git diff --check
```

All exited 0. Final test summaries, verbatim:

```text
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.26s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.69s
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.13s
```

These are File, PostgreSQL and SQLite respectively. Raw outputs are `targeted-final.log`, `clippy.log`, `clippy-final.log`, `fmt.log`, `fmt-final.log` and `diff-check.log`. Source identity checks are in `source-sha256-verification.log`; test identity is in `tests-sha256.txt`.

The review did not change or execute the final production roster, perform a new mutation sweep, publish, exercise operator stores, run comparative capacity admission or restart the PostgreSQL fixture. Existing implementation mutations are not presented as review-authored mutation evidence.

## Findings

Nothing found in this bounded attack.

## Boundaries attacked

- Receipt namespace, canonical sort versus ordered appends, actual-byte hashing and existing byte comparison were read against the published capability and exercised by the whole provider suite.
- New cases directly exercised equal-length byte collisions and rollback, receipt-first resolution after erase/rebind, independent handles and tenant isolation.
- New cancellation cases exercised tentative guard visibility, retained existing content and retry after a cancelled native transaction on File and PostgreSQL.
- Existing native File interruption/cleanup tests and PostgreSQL lost-response, reversed-lock-order and standalone-delete tests reran successfully. Those measured specific supported failpoints and interleavings; this report does not generalize them to all storage failures.
- Legacy fingerprint fixtures remained unchanged and passed; source review found no put-then-append fallback or new File journal operation.

## Outside writes and stable handback

All paths use the coordinator-assigned scratch/target; exact private paths and environment are retained in `commands-private.txt` and `outside-paths-private.txt`, which must not be published. The private inventory includes every retained scratch output, full temporary child-process artifact paths, the assigned Cargo target, and the managed-worktree metadata root. Test-created temporary directories lived under the assigned TMPDIR; test-created PostgreSQL tables used only the disposable fixture. New prefixes were `adversary_collision`, `adversary_rebind` and `adversary_cancel`; existing suite prefixes belong to its existing synthetic fixtures. No container lifecycle action was taken.

Only the review lease `codex-eventlog-atomic-content-adversary` was acquired, renewed and released. The release output is `lease-release.log`. All review compiler/test processes completed before release. The tree remains intact for the coordinator.

Final new-test SHA-256:

```text
23a4647dcb818814e4ab2899d064ff04469e3beb31364c8daf73f7b851142473  crates/eventlog-file/tests/adversary_atomic_blob.rs
9cfb4e78a56613e56eae1f0f25c5c44a0d2013d36398205f66da7b70a01ccf40  crates/eventlog-sqlite/tests/adversary_atomic_blob.rs
341202a3b0a5157a9f025961e7ec5a392a4a183e953016d509726216f9f8544f  crates/eventlog-postgres/tests/adversary_atomic_blob.rs
```

```findings
[]
```
