# Eventlog 0.3.0 SQLite integrity-reuse probe

Date: 2026-09-22. Verdict: one reproduced provider qualification defect, expressed by two independent failing refusal cases. Eight tests executed: six passed, two failed, zero ignored or filtered. Cargo exit status: 101. No correction was made.

Owners: this reviewer authored only the isolated synthetic probe and this evidence. Eventlog owns the affected SQLite binding path. The EKR coordinator owns dependency adoption, upstream routing and qualification of the upcoming blob-backed writer. No repository source, parser tree, primary checkout, governed planning record or operator store was modified by this probe.

## F1 — Fresh atomic publication accepts an unreadable existing blob

Severity: high for qualification of blob-backed durable publication. The current EKR inline-object implementation does not exercise this path, so this finding does not demonstrate a failure in its existing seed implementation.

Affected exact release: `eventlog-core` and `eventlog-sqlite` 0.3.0 at `ac6b1731654329d32f1e3c9cf164fefad6a5b46a`, using rusqlite exactly `0.40.2`. `Cargo.lock:99–115` retains the exact Git source; `:337–338` retains the SQL dependency version. The offline build log also identifies both released packages.

The new-request existing-binding path in `crates/eventlog-sqlite/src/atomic_group.rs:163–176` selects and compares only `bytes`. It does not validate stored integrity metadata. Ordinary blob reads and put readback do validate those metadata (`crates/eventlog-sqlite/src/lib.rs:2099–2173`). The SQL integrity design requires validation before acknowledging a put and validation of existing bindings before byte comparison (`docs/design/sql-blob-read-integrity.md:15–18`, `:73–75`), but its enumerated paths omit this atomic binding reuse. This is an integration gap between the atomic binding path and the new integrity contract.

Measured sequence, independently for unguarded publication and an allow-only guard:

1. Create a fresh synthetic current-schema database and write one valid binding through `EventStore::put_blob`.
2. Close the provider. Change only `integrity_sha256` to 64 lowercase `a` characters using a separate rusqlite connection. Do not change the blob bytes, byte count, tenant, digest or integrity edition.
3. Reopen the provider and observe `get_blob -> Backend("stored blob integrity metadata is invalid")`.
4. Submit a **fresh** `BlobAppendGroup`, with a new stream and idempotency key, the exact original bytes and a metadata-only event. Invoke the full `AtomicBlobEventStore` trait path.
5. Observe success with `deduplicated=false`. A separate SQL connection sees events change from 0 to 1 and group receipts change from 0 to 1. `read_stream` also returns the new event. The complete blob tuple remains unchanged and `get_blob` still refuses it.

Both failing tests stop at `src/lib.rs:154`, after printing all before/after observations and asserting that the blob tuple is unchanged. They fail for accepted publication, not setup, decoding, missing tools or a skipped branch.

Exact failing tests:

- `tests::corrupted_existing_binding_refuses_fresh_unguarded_publication` (`src/lib.rs:160`; `run.log:68–80`).
- `tests::corrupted_existing_binding_refuses_fresh_unobserving_guard_publication` (`src/lib.rs:165`; `run.log:81–92`).

Origin classification: reproduced in the released dependency. Source comparison shows the existing byte-only reuse branch was not brought under the new integrity metadata rule. The old release was not executed by this probe, so this report does not claim a measured cross-version regression or a previous occurrence of the same metadata corruption.

## Controls and complete outcomes

| Test | Result | Observed behavior |
| --- | --- | --- |
| `healthy_fresh_binding_publishes_event_receipt_and_bytes` | Pass | One new binding, event and receipt; bytes readable; provider stream returns event. |
| `healthy_existing_binding_is_visible_to_guard_and_reused` | Pass | Guard reads the exact bytes once; one event and receipt; existing blob tuple unchanged. |
| `corrupted_existing_binding_refuses_fresh_unguarded_publication` | Fail | New event and receipt published despite unreadable binding; no guard callback. |
| `corrupted_existing_binding_refuses_fresh_unobserving_guard_publication` | Fail | Same invalid publication; allow-only guard ran once. |
| `corrupted_binding_read_by_guard_refuses_without_publication` | Pass | Guard's transactional read detects corruption; zero events and receipts; tuple unchanged. |
| `swallowed_integrity_failure_still_refuses_without_publication` | Pass | Guard catches the read error, but provider poisoning still refuses the transaction; no publication. |
| `exact_prior_receipt_retry_ignores_later_corruption_and_skips_guard` | Pass | Existing successful receipt returns the exact original events, deduplicated; guard runs zero times; one event/receipt remain. |
| `exact_prior_receipt_retry_after_erasure_does_not_restore_content` | Pass | Exact original receipt returned without restoring deleted bytes or creating more events/receipts. |

The retained receipt controls are intentionally successful. A retry of an already committed receipt is not a new binding publication and must retain the original callback-free and non-restoration behavior. The defect concerns only a new request with no existing receipt.

## Minimum repair expectation

Before accepting an existing binding for a new atomic blob publication, load and validate its full byte/count/hash/edition tuple with the same integrity rule used by ordinary put/read. Refuse corrupt metadata with the provider's non-payload-bearing backend error and publish no new events, receipt or other tentative writes. Preserve the prior-receipt fast path before this check and retain both retry controls. The guard must not have to read every binding merely to supply the provider's integrity guarantee.

This probe does not qualify File, PostgreSQL, live-store migration, crash uncertainty, multiple writers or the completed EKR writer. It reproduces one bounded SQLite condition and its nearest controls; it is not a complete upstream gate.

## Exact execution and retained evidence

Executed from the standalone probe directory:

```sh
bash run.sh
```

The retained script sets the task-owned target and synthetic database directory, `CARGO_BUILD_JOBS=2`, `CARGO_INCREMENTAL=0`, both development/test debug levels to 0, and the assigned external TMPDIR. It measures free disk before compiling and refuses below 10 GiB. Recorded available space was 26,277,412,864 bytes. It then executes exactly:

```sh
cargo test --offline -- --nocapture --test-threads=1 > run.log 2>&1
```

The first invocation resolved and retained `Cargo.lock`, compiled successfully, then ran all eight tests. `run.status` contains 101. There was no second run, correction run, formatter rewrite, fixture mutation or repository build. The fixture refuses to overwrite an existing database; a repeat must use a new `PROBE_DATABASE_ROOT` and the same cargo command/environment rather than reusing the first database directory.

Artifacts retained together: `Cargo.toml`, `Cargo.lock`, `src/lib.rs`, `run.sh`, `run.log`, `run.status`, `disk-before.log`, eight synthetic databases under `databases-first/`, and this report. Raw logs and the script contain task-local paths and should not be copied into a public repository without the coordinator's normal privacy handling. This report itself uses repository-relative or artifact-relative paths only.

| Artifact | SHA256 |
| --- | --- |
| `Cargo.toml` | `806383698c5786bb592e4b4386b41a2d1c07d282fdbff33c76fa4bc04a5b276c` |
| `Cargo.lock` | `518f4be3a5fd00b08a4e0555de40157ea940ea8bdba88247f4b42697b91bc05a` |
| `src/lib.rs` | `1dd6f675ab101699d5c86829c59cf3207b5fc30c65f1915c61add9fb3fdbbd06` |
| `run.sh` | `b956fc345d5a0650c91886e1f652d05f1f32ef42d07a710f040c7c5e259a98b7` |
| `run.log` | `402d9af61f822d86d731ac0db566550c242b97b068826e028ecd15397752a7a8` |
| `run.status` | `39b8dc3fc8b44765c8e6f1adee04c5b465e555ab791cc42d0d9e810d5b64297c` |
