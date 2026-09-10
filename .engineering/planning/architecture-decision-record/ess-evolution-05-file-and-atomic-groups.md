---
format: aep.planning-md/1
id: architecture-decision-record:ess-evolution-05-file-and-atomic-groups
kind: architecture-decision-record
status: proposed
title: 05 — File Eventlog and atomic append groups
refs:
- provider: ess
  reference: initiative:ess-evolution
revision: 1
---
## Context
The existing two-backend rule protected one shared storage contract from ad hoc product stores. Repository-local AEP journal authority now requires a durable JSONL provider, while ER ordered multi-entity execution requires atomic groups. Existing SQL physical schema and admission remain compatibility authority.

## Decision
The operator's 2026-09-10 implementation plan explicitly supersedes the two-backend/no-third rule in AGENTS.md and predecessor RFC 0020. Add file storage under the same applicable conformance contract. Retain domain neutrality and content separation. Add atomic append groups alongside EventStore, implemented as one transaction in every provider.

## Contract
A group has one tenant and idempotency identity. Ordered entries evaluate expectations against earlier entries in the same transaction, including repeated streams. Lock streams deterministically and apply request order. Changed group content under the same identity is refused. Guards, required inline projections and bookkeeping commit or roll back together. Transaction-scoped blob reads never re-enter the locked outer store.

File storage uses versioned JSONL transaction frames, sequence and previous digest verification, process-safe writer locking and durable referenced blobs before acknowledgement. Caches are disposable. Recover only provably incomplete trailing uncommitted writes; committed corruption and divergent Git histories refuse. Preserve active-store redaction/erasure; versioned Git history remains a separate archive.

## Rejected alternatives
Independent per-entry commits, a product-specific file store, concatenating Git merge drivers, snapshots as authority, and changing existing single-append bytes or public meanings.

## Compatibility and migration
Keep EventStore entrypoints. SQL DDL is additive; admission and hosted-role capabilities remain verified. Do not delete or reinterpret existing histories. Provider facade migrations happen in ER. File provider remains unavailable until implemented and verified.

## Acceptance evidence
Operator design authorization is recorded here. Shared SQL/file atomicity, retries, guard/projection rollback, concurrent writers, crash/reopen and redaction conformance must be observed before claiming implementation.
