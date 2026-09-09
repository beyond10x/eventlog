---
format: aep.planning-md/1
id: runbook:complete-review-wave
kind: runbook
status: draft
title: Complete Eventlog review and integration
revision: 2
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
