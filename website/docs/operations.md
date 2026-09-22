---
title: Operate Eventlog
---

## Provision before traffic

Select the storage path, namespace, trusted tenant and bounds in the host. File `open` may create
a store; `open_existing` refuses absent or incomplete authority but still has recovery authority.
SQLite also offers existing-only open. These are distinct from read-only inspection.

Register inline projectors before sealing or the first append. Reopening requires the host to
attach the matching projector implementation. `InlineProjectionAdmin` can attach an existing
projector without durable writes and rebuild from complete committed history and active blobs.

## Recovery

Keep writers stopped while taking a consistent backup of authority and its referenced content.
Test recovery on a disposable copy.

File's manifest selects the committed journal prefix. Opening verifies the history and active
blobs; later operations re-read and hash the committed prefix before reusing verified state.
A valid recovery intent is handled by an authorized opener. Inspectors refuse pending recovery
rather than carrying it out. Never delete a manifest or truncate a journal to clear a refusal.

For an uncertain append, retry the same command identity and request. A different id may create a
second command. Record the exact refusal and preserve the source before investigating corruption.

## Hosted PostgreSQL composition

1. Provide verified TLS roots or an explicitly owned transport authority.
2. Run `PostgresEventStore::migrate` with the migration role and complete projection roster.
3. Open traffic with a dedicated application login that owns no durable schema objects, has no
   role memberships or elevated privileges, and has no schema CREATE grant.
4. Set a finite role connection limit and budget replicas plus reserved connections together:
   `replicas * max_connections + reserved_connections <= database_connections`.
5. Register projections before traffic and fence old binaries during a protocol cutover.

Admission checks physical schema shape, migration checksums, projection roster, sequence behavior
and deterministic collations. A checksum alone is insufficient.

Pool defaults are four connections, 32 waiting acquisitions, two-second acquisition and connection
deadlines, five-second statements, two-second locks, ten-second operations and five-second shutdown.
Set them against the host's workload and resource budget. `pool_status()` reports local occupancy.
Saturation returns `Overloaded`; bounded acquisition returns `Deadline`; shutdown returns `Closed`.

Cancelled or unsettled connections stay quarantined until their driver stops. A lost commit reply
returns `UnknownCommit`; query the claim or retry the original request.

## Validate a deployment

The local repository gate is `bash scripts/gate.sh`. Set `EVENTLOG_TEST_POSTGRES_URL` to a
disposable PostgreSQL fixture first. Some 0.3.0 cases refuse a missing URL; other cases report
that they did not run. Neither result proves the PostgreSQL provider.

The required production proof uses disposable PostgreSQL with verified TLS and both migration
and application roles. Inspect the released fixture's options first:

```bash
cargo run --locked -p eventlog-postgres --example production-fixture -- --help
bash scripts/gate.sh --production-proof
```

The second command requires `EVENTLOG_TEST_POSTGRES_URL`,
`EVENTLOG_TEST_HOSTED_POSTGRES_URL` and `EVENTLOG_TEST_POSTGRES_CA`.
Set `EVENTLOG_PROOF_REPORT` and `EVENTLOG_PROOF_RAW` to retain receipts and runner output.
Never use deployed data for restart fixtures.

Conformance demonstrates storage behavior. Capacity approval needs the owner's declared workload,
latency, lag, queue and recovery budgets plus comparative measurements.

## Storage admission and effects

An `AdmissionPermit` belongs to the trusted host. A matching guard may reserve bounded counters in
the append transaction; domain projectors cannot use that authority. Exact retry does not charge
twice. Tenant and deployment reservation scopes remain distinct.

`EffectStage`, `EffectEvidence` and `EffectBoundaryCoverage` describe external-effect evidence.
Validate that metadata before embedding it. Eventlog stores evidence; domain policy remains with
the application.
