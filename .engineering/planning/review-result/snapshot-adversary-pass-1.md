---
format: aep.planning-md/1
id: review-result:snapshot-adversary-pass-1
kind: review-result
status: active
title: Snapshot adversary pass 1
relations:
- reviews: story:prevent-stale-snapshots-after-redaction
revision: 1
---
unit: story:prevent-stale-snapshots-after-redaction at d25e5675e299119af905e0c2df9a8c862934e08a against699c0e15a2c3669d88329543f113e6302ba9dc7e
verdict: nothing found
cases: executed 0→0, red0 (read-only review; implementor reports88 green)
origin: introduced0 / pre-existing0 / undecided0
wrote-outside-worktree: none
needs-coordinator: none

Git diff --stat: empty. Git status --porcelain: empty. No test or implementation edit was made.

No additional reachable failure was established in this bounded pass. No new cases were added or executed, and no suite was rerun. The existing88-case gate and both predicate-mutant outputs were read as implementor-produced evidence, not claimed as new reviewer execution.

Read the complete11-file patch, owning acceptance/design constraints, snapshot ESS declaration and implementation report. Traced Repository::load_observed, handle/finish and snapshot_now through both adapters' capture/save/load methods, redaction rotation, and erasure. The observation precedes reads; PostgreSQL generation-row writes serialize stale-save/redaction races, checked metadata and cache bytes commit together, and SQLite uses one immediate transaction. Capturing between erasure statements is prevented by PostgreSQL's exclusive publication gate. A generation removed by erasure cannot match a recreated stream's fresh UUID. A generation mismatch causes a false checked-save result and leaves existing cache bytes/proof untouched.

Traced PostgreSQL legacy/current checksum admission, partial-metadata refusal, migration transaction, required metadata DML rights, and snapshot load join. Existing preupgrade cache rows remain untrusted until a checked write certifies them. Read the SQLite added metadata column admission and the verified-cache join. No additional reachable failure was established for the supported migration path.

Traced post-append cache serialization/storage failures: automatic caching is best effort after successful append; explicit snapshot_now surfaces errors and retries one stale-generation observation. Public Snapshot/Loaded/Outcome layouts remain unchanged. Legacy unchecked saves intentionally refuse; compatibility depends on the selected fenced writer cutover, as the implementation report states. Existing older writers concurrently overwriting physical cache bytes are outside that selected protocol, not a newly demonstrated supported scenario.

Read the existing real-backend privacy serialization barriers, stale/ABA/current-history checks, cache-error tests, hosted metadata permissions test, migration test and production required-case additions. Known core input-validation and PostgreSQL rewrite-rule findings were excluded as requested and remain coordinator-owned.

No new artefact, source, fixture or container mutation. This report is the only newly written path and is within the assigned target/review-scratch. Review lease snapshot-adversary-catchup-20260909 released at handoff.

```findings
[]
```
