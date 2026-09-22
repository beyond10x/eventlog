---
format: aep.planning-md/1
id: review-result:consistent-tenant-capture-source-pass-1
kind: review-result
status: active
title: Consistent tenant capture source examination pass 1
relations:
- reviews: story:consistent-tenant-capture
revision: 1
---
unit: native consistent tenant capture, source `5b4d4f83efe23f85294de7b5fef8b7de3ebdc878` (tree `36331305b3aa38ba118594758fd34a2b4a34800d`), base `8746a693c6084ab516170278a72c6c8cf5c8e46c`; findings cover that commit plus my four untracked test files
verdict: CONFIRMED
cases: executed 200→214, red 3
origin: introduced 6 / pre-existing 0 / undecided 0
wrote-outside-worktree: 3 paths
needs-coordinator: whether a foreign SQL writer's rows are a state this reader must answer typed, and a PostgreSQL 18 server, which I was not assigned

---

## 1. `git --no-pager diff --stat`

Empty. No tracked file changed. `git status --porcelain` in
`~/.local/state/worktree/trees/b10x/eventlog/ess-evolution-capture-source-review-1-20260916`:

```
?? crates/eventlog-core/tests/
?? crates/eventlog-file/tests/capture_review_one.rs
?? crates/eventlog-postgres/tests/capture_review_one.rs
?? crates/eventlog-sqlite/tests/capture_review_one.rs
```

Every path is a test file. No implementation, manifest, design, schema, model, roster or planning
file was changed, even temporarily. `corrected-review-files.sha256` and
`corrected-execution-files.sha256` are byte-identical to each other on all 108 rows, and every
source hash in them matches `implementation-result.md` § 4.

| my file | lines | sha256 |
|---|---|---|
| `crates/eventlog-core/tests/capture_review_one.rs` | 304 | `7c0a6e0350e8107b6456f05972c8b1c304ca653392985e55615fae7f9001d15d` |
| `crates/eventlog-file/tests/capture_review_one.rs` | 299 | `39c5050c098648ded6ff6c9feeaf565b4be0ab39325fa2500420c2b26a6e630f` |
| `crates/eventlog-sqlite/tests/capture_review_one.rs` | 202 | `1b2e6a38b58076decba5c2af084564349d91125923b7760086083fb0c46b7d75` |
| `crates/eventlog-postgres/tests/capture_review_one.rs` | 175 | `efe853c67db4b3b2915fec336ae4a467f26a8c32aa1dfcc4e0964a5bc9221594` |

**Origin is verified mechanically, not asserted.** All three capture sources are absent at the base:

```
$ git cat-file -e 8746a693c6084ab516170278a72c6c8cf5c8e46c:crates/eventlog-sqlite/src/capture.rs
fatal: path 'crates/eventlog-sqlite/src/capture.rs' exists on disk, but not in '8746a69...'
```

Same for `crates/eventlog-postgres/src/capture.rs` and `crates/eventlog-core/src/capture.rs`. Every
finding below is therefore `introduced`; none could be `pre-existing`.

## 2. The cases, and each case run alone

14 cases added. Root executed each alone before any suite. Raw logs and per-command exits in
`execution/` (first run) and `corrected-execution/` (after fixture correction 1).

### 2.1 The fixture fault, first, because it is mine

The first execution's four SQL cases exited 101 at store construction on a prefix of mine carrying
the digit `1`; both providers admit lowercase ASCII and underscore only
(`crates/eventlog-sqlite/src/lib.rs:3008`, `crates/eventlog-postgres/src/lib.rs:1712`). **No capture
ran and no assertion was reached in any of the four.** They are fixture faults and are not counted
as findings anywhere in this report. `correction-note.md` carries the full account; the corrected
prefixes are `revone` and `rev_{label}_{suffix}` built the way the admitted `consistent_capture`
cases build theirs. The 10 core and file cases were unaffected and their files never moved.

### 2.2 Ten green, individually

Each exit 0, `execution/eventlog-{core,file}-<name>.exit`:

| crate | case |
|---|---|
| core | `a_repeated_projection_name_is_one_duplicate_request_whatever_its_fields` |
| core | `payload_accounting_counts_bodies_and_never_the_envelope` |
| core | `a_reported_cap_names_the_resource_that_crossed_it_and_its_own_limit` |
| core | `stream_versions_are_checked_per_stream_and_a_position_gap_is_ordinary` |
| core | `a_repeated_coordinate_is_found_after_sorting_not_only_when_it_arrives_adjacent` |
| core | `one_body_has_one_payload_length_whatever_order_its_keys_arrived_in` |
| file | `the_dirty_projection_refusal_has_no_reachable_producer` |
| file | `a_blob_object_that_is_not_a_regular_file_is_refused_without_being_followed` |
| file | `surplus_or_truncated_committed_bytes_are_corruption_and_nothing_is_repaired` |
| file | `a_zero_cap_names_the_resource_the_tenant_actually_holds` |

The file case `the_dirty_projection_refusal_has_no_reachable_producer` is green **by design** — it is
the evidence for finding F4, and a green result is what proves the variant unreachable.

### 2.3 Three red, individually, verbatim

All three were written before anything was executed, and each was run alone before any suite.

**`eventlog-sqlite a_foreign_row_key_storage_class_is_not_a_crossed_row_cap`** — exit 101:

```
running 1 test
test a_foreign_row_key_storage_class_is_not_a_crossed_row_cap ...
thread 'a_foreign_row_key_storage_class_is_not_a_crossed_row_cap' (2635056) panicked at crates/eventlog-sqlite/tests/capture_review_one.rs:145:5:
four stored rows cannot cross a cap of 64; a reported limit must actually have been exceeded: Err(LimitExceeded { resource: ProjectionRows, limit: 64 })
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
FAILED

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.09s
```

**`eventlog-sqlite an_overstated_stored_blob_length_is_corruption_not_a_crossed_payload_cap`** — exit 101:

```
running 1 test
test an_overstated_stored_blob_length_is_corruption_not_a_crossed_payload_cap ...
thread 'an_overstated_stored_blob_length_is_corruption_not_a_crossed_payload_cap' (2635100) panicked at crates/eventlog-sqlite/tests/capture_review_one.rs:195:5:
assertion `left == right` failed: a malformed stored length is corruption; the eleven bytes this tenant holds cross no cap
  left: Err(LimitExceeded { resource: PayloadBytes, limit: 4194304 })
 right: Err(Corrupt { material: Blob })
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
FAILED

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.03s
```

**`eventlog-postgres an_overstated_stored_blob_length_is_corruption_not_a_crossed_payload_cap`** — exit 101:

```
running 1 test
test an_overstated_stored_blob_length_is_corruption_not_a_crossed_payload_cap ...
thread 'an_overstated_stored_blob_length_is_corruption_not_a_crossed_payload_cap' (2635211) panicked at crates/eventlog-postgres/tests/capture_review_one.rs:121:5:
assertion `left == right` failed: a malformed stored length is corruption; the eleven bytes this tenant holds cross no cap
  left: Err(LimitExceeded { resource: PayloadBytes, limit: 4194304 })
 right: Err(Corrupt { material: Blob })
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
FAILED

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.14s
```

**`eventlog-postgres a_constraint_this_kit_did_not_create_refuses_physical_shape`** — exit 0:

```
running 1 test
test a_constraint_this_kit_did_not_create_refuses_physical_shape ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.34s
```

Every predicted observation in `READY.md` § 3.3 and § 3.4 matched the measured one, resource for
resource and limit for limit. The reported resources and limits were predicted from the source
before anything ran.

## 3. The suite, after the cases existed

`corrected-execution/workspace.log`, `workspace.exit` = **101**. Summed from its 35
`test result:` lines: **211 passed, 3 failed, 0 ignored, 214 executed.**

`<before>` is 200, taken from the implementing state's own `cases:` line
(`implementation-result.md` § 3, check 10: "31 suites, 200 passed, 0 failed, 0 ignored"), not from a
suite run predating my cases. 200 + 14 = 214, which is what the run reports, so every added case was
selected and none was filtered away.

The run's own closing list of failed targets, verbatim:

```
    `-p eventlog-postgres --test capture_review_one`
    `-p eventlog-sqlite --test capture_review_one`
```

**Only the two new SQL targets are red.** Every pre-existing target is green, including
`eventlog-postgres tests/consistent_capture` (7 passed), `eventlog-sqlite tests/consistent_capture`
(6 passed), `eventlog-file tests/consistent_capture` (9 passed), `eventlog-postgres
tests/conformance` (45 passed) and `eventlog-sqlite tests/conformance` (24 passed). Nothing I added
broke anything that was working.

`corrected-execution/fmt.exit` = 0 with an empty `fmt.log`; `corrected-execution/clippy.exit` = 0
with no diagnostic in `clippy.log`. My four files pass `cargo fmt --all --check` and
`cargo clippy --workspace --all-targets -- -D warnings` as written. No lint correction was needed at
either round.

## 4. Findings

### F1 — a stored `row_key` outside TEXT makes the SQLite resume predicate stop advancing

| | |
|---|---|
| **what was measured** | `crates/eventlog-sqlite/tests/capture_review_one.rs:145`, exit 101: `Err(LimitExceeded { resource: ProjectionRows, limit: 64 })` against four stored rows |
| **what reaches it** | *nothing in the kit.* `ProjectionStore::upsert` takes `key: &'a str` (`crates/eventlog-core/src/projection.rs:95`), so every row this kit writes has TEXT storage class. The state needs a direct SQL writer or storage-level corruption |

`read_rows` resumes with `WHERE tenant_id = ?1 AND row_key > ?2 ORDER BY row_key`
(`crates/eventlog-sqlite/src/capture.rs:375`), binding the previous page's last key as text
(`:400`). SQLite orders storage classes before values, so a `BLOB` key is greater than every text
key *and* greater than any text bound as the resume point. The foreign row is re-selected on every
page, `after` never advances past it, and the loop is bounded only by `max_projection_rows`
(`crates/eventlog-core/src/capture.rs:300`). With a large row cap it does not terminate at all,
inside the `BEGIN IMMEDIATE` transaction that excludes every writer
(`crates/eventlog-sqlite/src/capture.rs:80`).

Two design clauses are broken at once: *"Every reported limit must actually have been exceeded"*
and *"Undecodable stored keys or bodies are projection corruption, not omission"*. The unit's own
rationale says the same: *"A row this kit did not write can still be selected by a query. Refusing
it is the difference between a capture and a transcription of whatever is in the table"*
(`crates/eventlog-core/src/capture.rs:315-316`). `validate_captured_event` enforces that for events;
nothing enforces it for a row key's storage class.

Named, not applied: decide the resume point from the storage class as well as the value — read
`typeof(row_key)` in the same statement and refuse anything but `text` with
`Corrupt { material: Projection }`, before it becomes a resume point.

**Verdict `INFEASIBLE`, origin `introduced`, severity `warning`.** It holds, and I built the state;
I cannot show anybody reaches it.

### F2 / F3 — both SQL preflights decide the payload cap from an unvalidated stored length

| | |
|---|---|
| **what was measured** | `crates/eventlog-sqlite/tests/capture_review_one.rs:195` and `crates/eventlog-postgres/tests/capture_review_one.rs:121`, both exit 101, both `left: Err(LimitExceeded { resource: PayloadBytes, limit: 4194304 })` where `Err(Corrupt { material: Blob })` was required, on a tenant holding eleven bytes |
| **what reaches it** | *nothing in the kit.* `crates/eventlog-sqlite/src/lib.rs:2024` writes `byte_count` as `to_i64(bytes.len() as u64)` from the very bytes it is inserting. The state needs a direct SQL writer or storage-level corruption |

`preflight` proves the payload cap from `SUM(byte_count)` before any blob is decoded —
`crates/eventlog-sqlite/src/capture.rs:215-218` and `crates/eventlog-postgres/src/capture.rs:245-248`
— so the column decides the refusal before `validate_stored_blob`
(`crates/eventlog-core/src/blob_integrity.rs:46`) ever compares it against the bytes. The design
asks for both: *"SQL preflights counts and stored blob lengths inside the snapshot"* and *"A
malformed length is corruption"* and *"Every reported limit must actually have been exceeded"*. The
implementation satisfies the first and, on a length that is a lie, contradicts the other two.

Named, not applied: keep the preflight as a fast refusal but make it provisional — when the
proven total crosses a cap, decode the blobs and let `validate_stored_blob` speak first, so the
caller learns the length is corrupt rather than that a cap it never crossed was crossed.

**Verdict `INFEASIBLE`, origin `introduced`, severity `warning`** for each provider, filed as two
rows because they are two code sites the coordinator may route separately.

### F4 — `ProjectionCaptureRefusal::Dirty` has no producer in any provider

| | |
|---|---|
| **what was measured** | `crates/eventlog-file/tests/capture_review_one.rs`, exit 0: with the dirty marker set, capture returns `RedactedHistory` and never `ProjectionUnavailable { …, Dirty }`, before and after a rebuild. The green result is the evidence |
| **what reaches it** | nothing. The variant is unreachable through the public API of all three providers |

`dirty_views` is File-only (`crates/eventlog-file/src/state.rs:96`); SQLite's and PostgreSQL's
`admit_projection` never read a marker. In File the only writer with `dirty: true` is `redact`
(`crates/eventlog-file/src/lib.rs:887`), which sets `redacted_at` on the same event in the same
transaction — and `observe` checks the redacted-history condition at
`crates/eventlog-file/src/capture.rs:159-165`, **before** `admit_projection` at `:167-171`, the only
site that returns `Dirty` (`:249-254`). So the tenant whose marker exists is exactly the tenant
whose capture is already `RedactedHistory`. `rebuild_projection` clears the marker (`lib.rs:1005`)
and the redacted event survives, so there is no window either.

The code's precedence is the design's own. What cannot be satisfied is the variant at
`crates/eventlog-core/src/capture.rs:81`, the design's list of four refusal reasons, and the
acceptance line *"all projection refusal kinds"*.

Named, not applied: delete the variant and amend both lines, or state in the design that `Dirty` is
reserved and unreachable while redaction outranks it. A deletion, not a behaviour change.

**Verdict `CONFIRMED`, origin `introduced`, severity `note`.**

### F5 — the PostgreSQL shape check requires exactly one catalogued constraint

| | |
|---|---|
| **what was measured** | `crates/eventlog-postgres/tests/capture_review_one.rs`, exit 0: a second constraint on the projection table gives `PhysicalShapeMismatch`. The branch is live and strict, on PostgreSQL 17.6 |
| **what reaches it** | on 17.6, only a foreign `ALTER TABLE`. **Inferred and untested:** on PostgreSQL 18, every projection, because `NOT NULL` becomes a catalogued `pg_constraint` row there |

`crates/eventlog-postgres/src/capture.rs:498` destructures the table's constraints as
`[constraint]` and refuses anything else. On 17.6 the primary key is the only row, so this is
correct, and the green case above proves the branch is not dead. The PostgreSQL 18 claim is a
hypothesis I could not test: the assigned fixture is 17.6 and no 18 server was available. If it is
right, an upgrade turns every capture into `PhysicalShapeMismatch` with no other symptom.

Named, not applied: filter to `contype = 'p'` and require exactly one, rather than requiring the
catalogue to hold exactly one row of any kind.

**Verdict `INFEASIBLE`, origin `introduced`, severity `note`** — it could not be shown on a server
anybody here runs.

### F6 — File answers every refusal ahead of the per-handle divergence guard

| | |
|---|---|
| **what was measured** | read only. `crates/eventlog-file/src/capture.rs:132` evaluates `outcome?` before checking `extended` at `:134` |
| **what reaches it** | a handle whose observed history was replaced by another process, answering a request that refuses for any reason other than the two the design names |

The design permits exactly two: *"Missing-identity or redacted-history refusals may be returned from
validated state before that guard; a successful value must pass it."* A `LimitExceeded`,
`ProjectionUnavailable`, `Corrupt` or `RecoveryRequired` is also returned ahead of it. No value
escapes the guard, so I found no correctness consequence and wrote no case.

**Verdict `INFEASIBLE`, origin `introduced`, severity `note`** — recorded rather than probed.

### Reachable callers, for all six

`ConsistentTenantCapture::capture_tenant` has **no caller inside the kit**. Grepping the whole
`crates/` tree for `capture_tenant` and `ConsistentTenantCapture` returns the three provider
implementations, `crates/eventlog-conformance/src/consistent_capture.rs`, and six test files —
nothing else. `preflight` and `read_rows` are reached only through it
(`crates/eventlog-sqlite/src/capture.rs:130,136` and
`crates/eventlog-postgres/src/capture.rs:176,182`). The design's own § Dependencies and exclusions
names *"the consumer adapter and explicit synchronous bridge"* as still required. **Every finding
above is on a path no shipped consumer reaches today.** That bounds all six, and it is the single
most useful fact for pricing them.

## 5. What was attacked and could not be broken

One line each.

- Identity precedence in all three providers: absent identity before redacted history, both before
  projection admission. Matches the design's fixed order, and the conformance exercise drives it.
- Byte-for-byte identity preservation, and the empty, undecodable and wrong-storage-class refusals.
- `CaptureBudget` arithmetic: checked, never saturating, zero is a real cap, the count charged
  before the body, and the reported limit is the cap of the resource charged. Four green core cases.
- The payload charge excludes the envelope: a 400-byte name, event id, request id, trace id and
  causation id cost nothing, and the body's compact length is exact to the byte.
- Object key order does not change a body's payload length.
- Per-stream version contiguity against `State::apply`'s own replay rule
  (`crates/eventlog-file/src/state.rs:154-159`); the two agree, including a repeated version and
  `MAX_CAUSATION_DEPTH` at the boundary and one past it.
- `order_blobs` and `order_rows`: bytewise ordering and duplicate detection, including a duplicate
  that arrives non-adjacent, so arrival order cannot hide one.
- PostgreSQL lock-then-snapshot ordering; the session-level advisory lock sharing `ADVISORY_KEY` and
  `publication_identity` with the transaction-level publication gate; unlock verified before
  `settled()`; every failure path leaving the lease quarantined.
- SQLite `BEGIN IMMEDIATE` before the first stored read, with no DML and no DDL inside the capture
  transaction.
- File strict reader: the writers' `flock` held for the whole observation, no create and no
  truncate, a pending intent refused rather than recovered, and surplus or truncated journal bytes
  refused with the bytes left exactly as found — twice over, so a repair on the first call would
  show on the second.
- File non-mutation: a blob object that is a symlink is refused without being followed, and neither
  the link nor its target is touched.
- Keyset pagination in both SQL providers is complete for every key the kit itself writes, including
  the empty key — the first page carries no predicate — and, in SQLite, an embedded NUL.
- Zero caps name the resource the tenant actually holds, and a provisioned empty tenant satisfies
  all four.

## 6. Every path written outside the worktree

| path | what |
|---|---|
| `.ess-evolution/waves/0011-provider-capture/source-review-1/READY.md` | the execution request, twice |
| `.ess-evolution/waves/0011-provider-capture/source-review-1/correction-note.md` | fixture correction 1 |
| `.ess-evolution/waves/0011-provider-capture/source-review-1/report.md` | this file |

Three. No `/tmp`, no build directory, no external integration, no production store. My four test
files are inside the worktree and are listed in part 1. The lease
`ess-evolution-capture-source-review-1-opus-20260916` was released at the end of each turn and is
released now; the worktree is the coordinator's to clean up.

## 7. Findings block

```findings
- file: crates/eventlog-sqlite/src/capture.rs
  line: 375
  category: boundary
  severity: warning
  verdict: INFEASIBLE
  origin: introduced
  message: a stored row_key whose storage class is not text outranks every text resume point, so the keyset loop never advances past it and the caller is handed LimitExceeded for a row cap four stored rows do not cross
- file: crates/eventlog-sqlite/src/capture.rs
  line: 215
  category: acceptance
  severity: warning
  verdict: INFEASIBLE
  origin: introduced
  message: preflight proves the payload cap from SUM(byte_count) before validate_stored_blob ever reads the bytes, so a malformed stored length is reported as a crossed cap instead of as blob corruption
- file: crates/eventlog-postgres/src/capture.rs
  line: 245
  category: acceptance
  severity: warning
  verdict: INFEASIBLE
  origin: introduced
  message: preflight proves the payload cap from SUM(byte_count) before validate_stored_blob ever reads the bytes, so a malformed stored length is reported as a crossed cap instead of as blob corruption
- file: crates/eventlog-core/src/capture.rs
  line: 81
  category: contract-drift
  severity: note
  verdict: CONFIRMED
  origin: introduced
  message: ProjectionCaptureRefusal::Dirty has no producer in any provider, because the only writer of the marker is redact and the redacted-history condition is checked before projection admission
- file: crates/eventlog-postgres/src/capture.rs
  line: 498
  category: judgement
  severity: note
  verdict: INFEASIBLE
  origin: introduced
  message: physical shape admission requires the projection table to hold exactly one catalogued constraint of any kind, which is correct on the assigned PostgreSQL 17.6 and, inferred but untested, would refuse every projection on PostgreSQL 18 where NOT NULL is catalogued
- file: crates/eventlog-file/src/capture.rs
  line: 132
  category: judgement
  severity: note
  verdict: INFEASIBLE
  origin: introduced
  message: every refusal is returned ahead of the per-handle divergence guard, where the design permits only missing-identity and redacted-history to be
```
