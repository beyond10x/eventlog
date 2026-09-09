---
format: aep.planning-md/1
id: story:validate-public-append-inputs
kind: story
status: draft
title: Enforce constructor invariants at public input boundaries
tags:
- code-review
- priority-p1
relations:
- derived_from: review-result:core-sqlite-broader-pass-1
- depends_on: story:prevent-stale-snapshots-after-redaction
scope:
- confidence: cited
  path: crates/eventlog-conformance/src/lib.rs
- confidence: cited
  path: crates/eventlog-core/src/lib.rs
- confidence: inferred
  path: crates/eventlog-postgres/tests/input_validation.rs
- confidence: cited
  path: crates/eventlog-sqlite/tests/input_validation_review.rs
revision: 5
---
## Problem and reachability

The broader review of source096cdbe77c0adf1c8ade9b3b3985fcdbbb8cfcd9 reproduced three invalid public inputs being durably appended on real SQLite: scalar event body, deserialized empty TenantId, and an empty Claim.scope. Public Deserialize derives and mutable fields bypass constructors; core/src/lib.rs:947 validate_append validates only batch cardinality and CommandMeta, which omits claims. The tests-only source crates/eventlog-sqlite/tests/input_validation_review.rs fails3/3 with real Ok(AppendResult) values. Exact caller and runner text: review-result:core-sqlite-broader-pass-1. This serves O2/O6 through durable record and privacy invariants.

## Acceptance

Both supported backends reject every constructor-invalid stream, event and claim presented through public deserialization or mutation before writing events, command receipts, claims or projections; valid inputs preserve existing wire representations and retry behavior.

## Implementation and verification

Reuse constructor validation in TenantId/StreamId deserialization and NewEvent/Claim validation at the common append boundary. Preserve public source compatibility and field names. Enumerate empty, oversize, illegal-byte and non-object cases; ensure an invalid later event leaves no partial batch. Run both real adapter regressions, prove required mutation sensitivity, then the complete production gate with TLS/application-role lanes. No new entity or third backend.

## Scope

Cited core/src/lib.rs defines the input types and shared validator; shared conformance/src/lib.rs carries backend-neutral assertions. Cited SQLite input_validation_review.rs holds the three retained adversarial cases. Inferred separate PostgreSQL tests/input_validation.rs invokes the same cases with real PostgreSQL. Coordinator owns required-case roster and shared docs. Serialize after snapshot's overlapping core and shared-conformance edits, then run concurrently with isolated PostgreSQL rewrite-rule admission repair.
