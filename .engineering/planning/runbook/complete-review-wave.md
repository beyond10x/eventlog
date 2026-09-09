---
format: aep.planning-md/1
id: runbook:complete-review-wave
kind: runbook
status: draft
title: Complete Eventlog review and integration
revision: 6
---
## Authorization and ownership

Interactive operator approved the execution order and implementation: one coordinator and three workers. This run uses aep-drive:wave skill version 0.8.1. The coordinator owns all AEP writes, integration, shared release metadata, publication and cleanup. Existing session authorization includes commits and pushes. Source release remains a separate human gate after the review and required checks.

## Baseline and sequence

Start from main 734047203ce112b21ad5b9e3ea67fdacb8835def. Preserve the recovery planning history from 681a0ac5d407a12554e44980f7a9c186be11608f through a planning-only integration merge; retain main source and defer release licensing/version changes. Migrate the six completed legacy stories with source backlinks; consolidate guarded-refusal provenance through AEP rather than merging independent journals.

Three initial lanes: snapshot design; catch-up connection settlement; reviewed feature integration (guarded refusals then effect metadata). Next: implement snapshots against integrated contracts while two read-only reviewers cover core/SQLite and PostgreSQL/proof tooling. Fix every confirmed actionable finding, derive further waves from typed scope, and run final production plus comparative/restart proof. Prepare source release after all review findings close. Do not deploy or promote consumers; documentation is asynchronous.

## Resources and isolation

Measured previous task build directories: current production gate 1.1 GiB; release 488 MiB; baseline 556 MiB; standalone reproduction 373 MiB. Current available disk 43 GiB. Use at most three workers, four Cargo build jobs each, debug info disabled, separate managed worktrees and target directories. Each database fixture has task-owned persistent storage and a fixed loopback port, with its own test database per lane. Retain small evidence outside disposable targets.

## Managed records

Coordinator: wt-82ec9daed45d, branch wave/complete-review, session eventlog-complete-20260909. Atlas authority: wt-acbc7275f11d at 38033fb4557e3b01c85379be95530f9f5e15e6ca. Unit records and source/evidence commits will be appended as assigned. No worker may mutate planning, README, CHANGELOG, Cargo workspace metadata or another unit's assigned source files without a coordinator handoff.

## Scope and state

Initial scoping used aep-drive:story-scoper charters. Implementation uses aep-drive:implementor; independent attacks use aep-drive:adversary. This harness dispatches those charters through generic collaboration agents because it has no subagent_type selector; this is a reported harness deviation.

Planning baseline committed at 32bf1596c853cdedfb5477e1f790c060c5bc1d13. Catch-up worker wt-655da8b1406c and feature worker wt-21668c971b1d implement disjoint first-wave files. Snapshot design is complete and waits for the integrated contracts. Six historical foundation stories have been migrated with backlinks; verification-report:planning-consolidation-20260909 preserves independent guarded-refusal provenance.

Snapshot design decision: additive per-stream random generations, observation before folding, conditional cache writes, explicit Invalid for legacy unproven saves, and best-effort automatic caching after successful append. Existing cache fields and schema ledger format remain unchanged. Exact older PostgreSQL schema checksum admits a transactional additive upgrade; malformed or partial shapes refuse. A new ESS metadata model precedes implementation. This is an explicit storage-contract design change under AGENTS invariant 4.

## First wave result

Feature unit096cdbe77c0adf1c8ade9b3b3985fcdbbb8cfcd9 and catch-up unit15c049633363dd0f504c7ccaeb36ee01c1bb4bd6 integrated at699c0e15a2c3669d88329543f113e6302ba9dc7e. Both bounded adversary reviews found no additional issue; exact reports retained as review-result artifacts. The AEP validator warns these reviews have no findings block despite their literal empty findings YAML lists; observed CLI rendering limitation, not missing review text.

Combined production gate exit0:81 passed,0 failed,0 skipped; required-case set complete; fmt/clippy exit0. Source dirty flag reflects concurrent coordinator planning-only edits; final proof will run frozen exact source. Report: /home/timo/.cache/eventlog-complete-20260909/evidence/wave1-production-proof.json. No source release performed. Feature ESS semantic model validates; installed ESS cannot exactly project its internally tagged Rust wire layout, and its model states that limitation.

Snapshot implementation is assigned wt-f0ba183ee985, branch impl/verified-snapshot-generations. Two other workers review existing PostgreSQL and core/SQLite in parallel, tests-only; their confirmed findings are queued after overlapping snapshot source edits. Harness does not expose per-agent token/tool counts or elapsed metrics; these cannot be invented. Build observations: coordinator1.4GiB, catch-up1.1GiB, features1.6GiB;29GiB currently free.

## Review follow-up sequence

Broader core/SQLite review returned one NEEDS-CHANGE input-validation class with3 failing cases; broader PostgreSQL review returned one CONFIRMED rewrite-rule admission class with2 failing cases. Exact immutable reports and individual repair stories are recorded. Both are reachable through public APIs; their origin is explicitly undecided because the reviewers did not execute a separate historical checkout.

The typed next-wave result places story:reject-postgres-rewrite-rules and story:validate-public-append-inputs together, collisions=[],unassessed=[],cycles=[]. Both depend on snapshot implementation to avoid its shared files. Coordinator owns their production-proof roster additions. Operator standing scope is to fix every confirmed actionable finding; no additional wave approval is required.

An independent read-only review of the selected snapshot design found no reachable gap across redaction, erase/recreate ABA, pre-fold capture and publication locking. It did not claim tests on unfinished implementation.

Comparative baseline managed recordwt-d3a4aefef9fc, exact20e00c1eeab5d67bd5f749bbbd871b1fbfe7f796, coordinator lease. Disposable eventlog-complete-20260909 has the required2CPU/1GiB/256pids envelope; memory-swap was set explicitly to2GiB because Docker refused changing memory alone. Source proof awaits the final frozen candidate. Final publication is tracked by story:verify-and-publish-eventlog-source-release rather than conflated with the preserved historical implemented preparation record.

## Implementation closure and final proof

All four confirmed review defect classes are fixed: stale snapshots, catch-up connection reuse, public input validation and PostgreSQL rewrite-rule admission. Guard/effect feature contracts are integrated. Each unit received a bounded adversary pass; all post-fix passes found no additional issue. Broader review findings each have a fixed outcome recorded against their original review. Per-agent token/tool metrics remain unavailable from this harness.

Final code integration09c71ec3165e187f7baf6277ffa40079d863f3cd is clean and passed the full required production gate:98 passed,0failed,0skipped,missing_required_cases=[]. Formatting and clippy passed. All three ESS roots validate. Source and manifest metadata after this gate change only planning receipts and the laboratory storage declaration, which is corrected to a v7 Docker-volume profile before measurement without changing workloads or budgets.

Exact frozen-source final receipts will be retained under $HOME/.cache/eventlog-complete-20260909/evidence and the comparative run under $HOME/.cache/eventlog-complete-20260909/comparative-final. Read their source identities and verdicts directly; this prospective location record is not evidence of a future successful run. No source edits may occur during that comparative run. Source publication remains a separate human gate after integration and final proof, owned by story:verify-and-publish-eventlog-source-release.

Snapshot treewt-f0ba183ee985 was published, reports retained, leases ended, Cargo cleaned1.4GiB, then worktree finish and exact-id reviewed GC removed it; its disposable PostgreSQL fixture was stopped and removed. A coordinator heartbeat accidentally named a separate snapshot-review session; that coordinator-owned record was ended, and the actual reviewer's separate lease was left to its owner. Other unit and integration cleanup follows published recovery proof, retained evidence and process/lease checks.

## Observed final proof and cleanup handoff

Frozen candidate9bb72d903243b7bb54341771cb4953f3418f54b4 passed the full production gate (98/0/0, required cases complete), fmt and clippy, and fresh-fixture comparative proof (12 configurations,2 restarts,exit0). verification-report:final-integration-proof-20260909 records exact identities, paths and verdicts, including the earlier failed capacity and restart observations. No acceptance threshold was relaxed. Laboratory evidence does not admit a production deployment budget.

All implementation units and adversary reports are preserved, their wanted commits published, and their managed trees removed through reviewed exact-id GC: snapshotswt-f0ba183ee985, features/validationwt-21668c971b1d, catch-up/schemawt-655da8b1406c. Cargo clean reclaimed respectively1.4GiB,1.8GiB,1.4GiB. Comparative baselinewt-d3a4aefef9fc was published at recovery/comparative-proof-baseline-20260909 e40409bd5d3ed351d6effd4b5cd5ea1c5cdf5c33 after proof, Cargo cleaned232.9MiB, lease ended, finished and exact-id GC removed. All task-owned PostgreSQL fixtures have been stopped and removed with their anonymous volumes; private TLS material and small raw evidence remain outside the repositories in the task cache. Per-agent token/tool counters remain unavailable.

Only coordinatorwt-82ec9daed45d and exact Atlas authoritywt-acbc7275f11d remain for final publication and cleanup. Coordinator owns their next actions: publish this planning-only closure, merge the gated integration into clean main, verify the required GitHub workflow, then end own leases, clean disposable output, finish and review exact-id GC. Source runtime and workflows are unchanged from the frozen proof. Preserve all unmerged recovery branches and delete only this wave's merged local unit branches after ancestry checks.

All implementation stories are implemented. story:verify-and-publish-eventlog-source-release remains active for the separate human release stop and subsequent exact-tag source checks/artifacts. No release or documentation delivery is claimed.
