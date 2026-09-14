---
format: aep.planning-md/1
id: story:blob-binding-conflicts
kind: story
status: active
title: Reject conflicting bytes under an existing blob digest
relations:
- informed_by: story:atomic-append-groups
- informed_by: story:file-eventlog
scope:
- confidence: inferred
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-conformance/src/lib.rs
- confidence: cited
  path: crates/eventlog-core/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: inferred
  path: crates/eventlog-postgres/tests/
- confidence: cited
  path: crates/eventlog-sqlite/src/lib.rs
- confidence: inferred
  path: crates/eventlog-sqlite/tests/
- confidence: cited
  path: docs/design/blob-binding-parity.md
revision: 11
---
## Outcome

Every Eventlog provider enforces immutable tenant/digest blob bindings: identical bytes retry
successfully, different bytes are refused without modifying the existing content, and concurrent
conflicting writers cannot both receive success.

## Authority and design

Approved ESS evolution plan ess-evolution-20260915 revision 1, operator decision 2026-09-15;
root-plan digest 7579145c3de5a1c6f8088fd7fb804d29dac8903ec505048f3ce595c45023b787.
Design: docs/design/blob-binding-parity.md. This is a conformance extension and SQL correction,
not a new persisted format. Existing file/atomic-group stories remain implemented.

## Acceptance

- Extend shared blob conformance first; retain real failing SQL output before implementation.
- Preserve original bytes after a differing retry and match EventLogError::Invalid.
- Cover tenant isolation, identical retries, explicit deletion/rebinding and concurrent differing writes.
- Execute file/SQLite/PostgreSQL lanes against an explicit disposable database; no skipped backend
  counts as passing.
- Verify the new guard by removing the comparison, observing failure, and restoring it.
- Pass affected tests, formatting and strict Clippy, then bash scripts/gate.sh with actual PostgreSQL
  execution before integration. Capture full production proof separately when its fixture is ready.

## Scope

Derived 2026-09-15 by the read-only Sol story-scoper and accepted by Astra.

- Primary surface: crates/eventlog-conformance/src/lib.rs — cited; shared blob exercise lacks a conflict assertion.
- SQL providers: crates/eventlog-sqlite/src/lib.rs and crates/eventlog-postgres/src/lib.rs — cited; ignored conflicting inserts return success.
- Public contract: crates/eventlog-core/src/lib.rs — cited; put_blob documents only invalid digest refusal.
- Provider regression tests: crates/eventlog-sqlite/tests/ and crates/eventlog-postgres/tests/ — inferred; use established concurrency test modules after inspection.
- Design: docs/design/blob-binding-parity.md — cited; coordinator-owned accepted binding design.
- Changelog: CHANGELOG.md — inferred; record the adopter-visible corrected refusal.
- Confidence: high — cited; the existing file refusal and SQL behavior were read directly, runtime reproduction remains acceptance work.
- Would collide with: any shared conformance or SQL provider implementation change — cited.

## Limits

No DDL change, digest-algorithm restriction, event-envelope change, blob overwrite, product concept,
release or publication. SQL tamper detection and complete group crash/restart qualification remain
separate explicitly tracked follow-up requirements.
