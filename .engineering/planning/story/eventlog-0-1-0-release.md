---
format: aep.planning-md/1
id: story:eventlog-0-1-0-release
kind: story
status: implemented
title: Release Eventlog 0.1.0 with coded guard refusals
summary: Open-source the Eventlog workspace and expose a stable guard refusal code while preserving atomic rollback across both adapters.
tags:
- eventlog
- release
revision: 5
---
## Intent

Prepare the existing Eventlog workspace for its first source release and provide the domain-neutral refusal seam required by generated ESS services.

## Acceptance

- Workspace and lockfile versions are exactly `0.1.0`; source licensing is Apache-2.0 and crates remain source-distributed with `publish = false`.
- `EventLogError::GuardRefused { code }` carries a stable machine-readable owner code without introducing a domain or product concept into Eventlog.
- The shared inline-guard exercise runs against SQLite and PostgreSQL and proves the refusal code is preserved, no event is appended, and guard projection writes are rolled back.
- Changelog, release status, and guard documentation describe the `0.1.0` contract and bare tag convention.
- The repository gate passes against both supported adapters.

## Constraints

Eventlog remains tenant-oriented and gains no realm semantics. Its existing append-only, privacy, idempotency, projection, snapshot, redaction, and two-backend invariants remain unchanged.

## Recovery provenance and current delivery

This implemented record was preserved from recovery/release-candidate-20260909 at681a0ac5d407a12554e44980f7a9c186be11608f. It describes preparation on that historical branch, not a published release or the present main state. Its old source candidate was not blindly merged. Current implementation is owned by the reviewed integration stories; final exact-source proof and separately authorized publication are owned by story:verify-and-publish-eventlog-source-release. No release or tag was present at session inventory.
