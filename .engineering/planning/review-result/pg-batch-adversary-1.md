---
format: aep.planning-md/3
id: review-result:pg-batch-adversary-1
kind: review-result
status: active
title: PostgreSQL batch read and append, adversary pass 1
relations:
- reviews: story:postgres-batch-read-and-append
revision: 1
---
needs-revision

```
unit: story:postgres-batch-read-and-append, b3cb71b5 on unit/pg-batch-read, one untracked test file
verdict: CONFIRMED (2 warnings, 1 note)
cases: executed 801→807, red 3
origin: introduced 3 / pre-existing 0 / undecided 0
```

Cases in `crates/eventlog-postgres/tests/adversary_batch_read_and_append.rs`, each green on base b10ed05 and red on b3cb71b5 unless noted:

- R1 `a_batch_of_reads_that_each_fit_the_deadline_is_not_refused_for_its_size`: 98 reads, slowest separate read 88.6 ms, deadline 442.8 ms; `read_many` refused after 454.9 ms with `Deadline { operation: "store operation" }`.
- R2 `a_group_behind_several_short_lock_holders_commits_inside_its_deadline`: four holders of 0.4-1.6 s, lock timeout 0.9 s, statement timeout 1 s, transaction deadline 8 s; the group was refused after 1.07 s with `Deadline { operation: "database statement or lock" }`.
- R3 `a_group_returns_the_earlier_members_projector_refusal_before_a_later_members_content`: member 1 refused by an inline projector, member 2 carries `\u0000`; base returned the projector's refusal, now `Backend("PostgreSQL SQLSTATE 22P05")`.
- Probes, green on b3cb71b5: awkward stream coordinates at batch edges (999, 1,000, 1,001, 2,000 reads); `read_many_never_shows_half_a_group` (red on base: `half a group visible: [1, 2]`); overlapping groups in opposite member order over 1,200 streams do not deadlock.

Suite (`--no-fail-fast`, CI-shaped PostgreSQL 17.6 with TLS): 107 lanes, 804 passed, 3 failed (R1-R3).

Held: results equal `read_stream` for limits, after_version past head, missing and redacted streams, two tenants, duplicate reads, interleaved blobs; global_seq follows member order; refusal order of conflicts and guards as on base; no DDL change.

A group over about 7,000 distinct streams still fails with SQLSTATE 53200 on default `max_locks_per_transaction`, as on base.

```findings
- file: crates/eventlog-postgres/src/lib.rs
  line: 549
  category: contract-drift
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: read_many refuses a batch with Deadline when every separate read_stream of it fits the deadline, contradicting the trait's "as the separate call would have" (eventlog-core lib.rs:1159) and acceptance 1
- file: crates/eventlog-postgres/src/atomic_group.rs
  line: 158
  category: concurrency
  severity: warning
  verdict: CONFIRMED
  origin: introduced
  message: one statement taking up to 1,000 stream locks adds every lock wait against statement_timeout, so a group behind several short lock holders is refused where per-lock statements committed inside the transaction deadline
- file: crates/eventlog-postgres/src/atomic_group.rs
  line: 610
  category: contract-drift
  severity: note
  verdict: INFEASIBLE
  origin: introduced
  message: inserting every member before any projector lets a later member's deterministic content refusal (SQLSTATE 22P05) replace an earlier member's inline-projector refusal, which is more than the "environmental failures" the implementor reported
```
