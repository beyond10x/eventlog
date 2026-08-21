---
id: EL-005
title: Erasure, redaction and snapshot invalidation
status: done
depends_on: [EL-002]
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
