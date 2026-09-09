---
format: aep.planning-md/1
id: story:erasure-redaction-and-snapshot-invalidation
kind: story
status: implemented
title: Erasure, redaction and snapshot invalidation
tags:
- migrated-legacy
relations:
- depends_on: story:aggregates-repository-and-snapshots
revision: 4
---

# EL-005 — Erasure, redaction and snapshot invalidation

## Intent

A person asks to be forgotten, or a tenant leaves. An append-only log has to answer that without
becoming a log nobody can trust, and without erasing the audit trail along with the person.

## Acceptance

- `redact(stream, version, reason)` is the only `UPDATE` the kit ever issues against an events
  table. It replaces the body with a tombstone and sets `redacted_at`, keeping the event's id,
  version and position.
- Every snapshot at or after the redacted version is deleted in the same transaction, because the
  stream now folds to something else and a surviving snapshot would be a lie with a timestamp.
- `apply` receives a redacted event as a distinct case. The conformance exercise proves every
  aggregate stays total over it.
- Envelope identities stay opaque ids. A test asserts no name, email or handle can be written into
  `subject` or `actor`, so erasing a person happens in the identity directory and the attribution
  survives them.
- Tenant erasure removes that tenant's streams, snapshots, commands and projection rows in one
  transaction, and a test proves nothing of the tenant remains in any table the kit owns.

## Migration record

This is completed legacy work, not a new implementation request. The source declares `status: done` at line 4. Its existing contracts are exercised by the 73-case production gate at main `734047203ce112b21ad5b9e3ea67fdacb8835def`, recorded in verification-report:repository-hygiene-and-code-review-20260909. New review findings have separate follow-up stories.

## Provenance

Migrated from `docs/stories/EL-005-erasure-redaction-and-snapshot-invalidation.md`.

- First written 2026-08-22T00:30:40+02:00; last touched 2026-08-22T00:30:40+02:00; 1 revisions.
- Source status quoted: `status: done` (line 4).
- Source text retained; this artifact is the governed successor.
