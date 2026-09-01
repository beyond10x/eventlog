---
format: aep.planning-md/1
id: story:guarded-append-refusals
kind: story
status: implemented
title: Stable guarded-append refusal codes
summary: Return coded domain guard refusals and prove complete transactional rollback across both Eventlog backends.
tags:
- eventlog
- service-sdk
- todo
revision: 4
---
## Intent

Expose the domain-neutral refusal seam required by standalone ESS services without teaching
Eventlog any service, Todo, realm, or product policy.

## Acceptance

- A guard can refuse an append with a stable machine-readable owner code.
- SQLite and PostgreSQL return the exact code without appending an event.
- A shared conformance probe writes through the guard's transactional projection view before
  refusing and proves that side effect rolls back on both supported backends.
- Existing async append, idempotency, projection, privacy, snapshot, and redaction contracts remain
  unchanged.
- The canonical repository gate and explicit conformance exercises pass with both backends run.

## Constraints

Eventlog remains a domain-neutral tenant-scoped persistence kit. Authentication context and optional
realm policy remain consumer concerns, and no third backend or new storage mutation path is added.
