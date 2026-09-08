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
revision: 4
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
