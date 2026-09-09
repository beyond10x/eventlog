---
format: aep.planning-md/1
id: verification-report:repository-hygiene-and-code-review-20260909
kind: verification-report
status: draft
title: Repository hygiene and initial code review, 2026-09-09
relations:
- informed_by: story:eventlog-0-1-0-release
revision: 1
---
## Scope and outcome

Interactive operator request: preserve and publish pending Eventlog work, clean its checkouts, then start a repository code review. This record uses the existing release-candidate planning store. It does not adopt a second store on main or migrate the legacy docs/stories backlog. No decomposition or critic panel was needed.

Reviewed source: `734047203ce112b21ad5b9e3ea67fdacb8835def`, verified against advertised `refs/heads/main`. Initial review covered snapshots/redaction, aggregate loading, PostgreSQL pool and catch-up transactions, and backend validation. This is a first review pass; findings remain open and no fixes were applied.

## Preserved work

All direct commits have author and committer `b10x-bot[bot]`. Remote refs were read back after publication.

| Branch | Commit | Preserved work |
|---|---|---|
| recovery/workflow-diff-20260909 | ab86b130c613286628c87081b698c93feb4024c6 | Exact primary workflow edit; its old runtime pin is superseded on main |
| recovery/postgres-comparative-baseline-20260909 | 9e60b60ffa0f477cd89119bbeb96157e0ca5fa5b | Original adapter benchmark inputs; capacity.rs and recovery.rs match current main byte-for-byte |
| recovery/release-candidate-20260909 | 3f186bcf6b47df5ab7e929d98e3bf2648e80d19e | Pending 0.1.0 candidate, Apache license, guarded refusals, and existing planning evidence |
| impl/record-effect-attribution | d3971f21f876b6ea0488b34ab35cb0c08b2fd0c7 | Previously unpublished effect vocabulary commit |
| wave/cross-stack-actor-attribution | 63f252d5aff15b01ea025c90ff70ddecdce7e35b | Previously unpublished integration commit |
| fix/organization-asynchronous-releases | 734047203ce112b21ad5b9e3ea67fdacb8835def | Existing local branch, already represented on main |

The release-candidate branch also carries this audit record in a subsequent commit. Recovery publication is preservation, not integration or a release. Primary main was fast-forwarded only after the exact local workflow diff was committed and remotely verified. No stash exists and `git log --all --not --remotes --oneline` returns no commits.

## Findings

### P1: A delayed snapshot save resurrects state removed by redaction

Locations at the reviewed revision: `crates/eventlog-sqlite/src/lib.rs:1028` and `crates/eventlog-postgres/src/lib.rs:772`; normal aggregate snapshot writes are issued separately from append/load in `crates/eventlog-core/src/aggregate.rs:309` and `snapshot_now`.

Both save_snapshot implementations unconditionally upsert the supplied state without checking whether a redaction occurred after that state was folded. A writer can load version 1, pause, let redact(version 1) complete, and then save its previously folded snapshot. Redaction only deletes snapshots that exist during its own transaction. Repository::load trusts the newly inserted snapshot and starts replay after version 1, so the redacted event is never revisited and its old contribution returns. This applies to ordinary concurrent automatic snapshots and snapshot_now, not only arbitrary fabricated snapshots.

Reproduced through public APIs on SQLite and PostgreSQL 17.6: append one event, load a count of one, retain that loaded state as a delayed snapshot, redact the event, observe count zero, save the delayed snapshot, observe count one again.

```text
SQLite: fold immediately after redaction=0; fold after delayed snapshot save=1
PostgreSQL: fold immediately after redaction=0; fold after delayed snapshot save=1
```

A fix needs an atomic validity check tying the snapshot to the redaction/erasure generation it observed; merely serializing the final save statement with redact still admits a stale save after redact completes. Add a regression that exercises the interleaving through both adapters.

### P2: Successful empty catch-up polls discard healthy pooled connections

Location: `crates/eventlog-postgres/src/lib.rs:1191` (also the unsuccessful projector-lock branch near 1154).

run_catch_up quarantines its lease and opens a transaction, then returns Ok immediately when there are no eligible events. It neither completes the transaction explicitly nor calls settled(). Lease::drop therefore retires the connection. A caught-up projector repeatedly polling an empty log continually reconnects and repeats session/TLS setup, defeating pooling during its normal idle path. Contending projector workers take the same unnecessary retirement path when the advisory lock is unavailable.

Public-API reproduction: register a catch-up projector, observe one idle pooled connection, run_once for an empty tenant, allow asynchronous retirement to finish, then inspect pool_status.

```text
PostgreSQL idle catch-up: applied=0; idle connections before=1, after=0; checked_out=0
```

Explicitly roll back these normal no-work transactions and mark the lease settled only after rollback succeeds. Failed or cancelled rollback must continue to quarantine the connection.

## Validation

- Updated installed Rust toolchains; stable reports rustc 1.98.1.
- Main ordinary repository gate passed with a disposable real PostgreSQL database.
- Main `bash scripts/gate.sh --production-proof` passed with PostgreSQL 17.6, verified TLS, and the separate application role: 73 passed, 0 failed, 0 skipped, no missing required cases; formatting and clippy passed. Exact source was clean at the reviewed commit.
- Release candidate ordinary gate passed on a separate PostgreSQL database plus SQLite; formatter and clippy passed.
- Original comparative baseline ordinary gate passed without PostgreSQL configured. Its database lane was not proved by that run.
- The two review reproductions exited successfully after asserting the observed defective behavior. They are temporary review probes, not added regression tests or fixes.
- The organization brand check for Eventlog passed.
- Capacity/comparative laboratory proof and release publication were outside this first review pass. No release was cut.

Initial validation attempts are retained alongside successful evidence: main without a database refused; the first TLS fixture used disposable storage lost on restart and was replaced; the old release candidate encountered newer-schema projection registry tables when pointed at the main test database and was rerun successfully in its own fresh database. These attempts are not counted as passing evidence.

## Cleanup and handoff

The two pre-existing clean managed gate-input trees and the preserved benchmark/workflow trees were finished and removed through exact-id reviewed worktree GC. The remaining review and release-candidate trees are to be finished after this record is published. All source work survives in the remote refs listed above.

The operator owns the next decision: integrate wanted recovery branches and schedule fixes for the two findings. This audit does not claim the release candidate or attribution wave is merged. No approval records or lifecycle transitions were fabricated.
