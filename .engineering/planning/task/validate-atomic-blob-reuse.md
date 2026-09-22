---
format: aep.planning-md/1
id: task:validate-atomic-blob-reuse
kind: task
status: implemented
title: Validate SQLite blob integrity before fresh atomic reuse
relations:
- derived_from: story:atomic-blob-append
- derived_from: story:sql-blob-read-integrity
- serves: vision:O2
- decomposes: story:atomic-blob-append
revision: 6
---
## Reproduced defect

Consumer qualification of the published release reproduced a fresh SQLite
AtomicBlobEventStore group publishing an event and receipt for an existing blob
whose bytes match the request but whose stored integrity hash is invalid.
Ordinary get_blob refuses that same binding. A guard that does not read the blob
does not close the gap. The private synthetic probe retains its original failing
source, databases and command output; no operator store was accessed.

The released SQLite atomic_group.rs reads only bytes at the existing-binding
branch. The ordinary blob port and PostgreSQL atomic path validate stored bytes,
length, integrity edition and checksum. This repair enforces the existing
integrity contract; it changes no storage schema, fingerprint or receipt format.

## Acceptance

A new atomic group must validate every existing binding it reuses before
publishing events, receipt or other tentative bindings. A corrupt hash, length
or unsupported integrity edition refuses and leaves the transaction unpublished.
Preserve the existing named integrity refusal. Prove the refusal both without
a guard and with a guard that does not read the content.

Healthy fresh/reused bindings still publish. Different bytes still conflict.
An exact already-committed receipt retry still returns its own result before
content or callbacks, including after corruption or erasure; it must not restore
erased content. Mixed batches roll back any earlier tentative new binding if a
later existing binding is invalid.

Retain the original failing probe unchanged and execute it against the repaired
candidate. Add provider regressions and require them in production proof.
Demonstrate mutation sensitivity. Run targeted checks, the repository gate and
independent review, followed by required real-backend CI before integration.

## Scope

Cited: crates/eventlog-sqlite/src/atomic_group.rs.
Cited regression target: crates/eventlog-sqlite/tests/atomic_blob.rs.
Cited required-case roster:
crates/eventlog-postgres/examples/production-proof/required.rs.
No PostgreSQL implementation change is needed: its atomic readback already
uses validate_stored_blob. Report another required source path before editing.
Root owns planning, shared documents, changelog, integration and publication.
The implementor owns only this scoped repair and regression cases.

## Boundaries

No automatic legacy trust migration, repair of corrupt stored bytes, data erasure,
new provider API or changed guard/receipt ordering. No operator store access.
This task is a prerequisite for the consumer's planned blob-backed writer,
not a claim that the consumer currently publishes blob-backed state.

## Independent review

The independent report is retained verbatim in
review-result:sqlite-atomic-integrity-review. It returns no new finding, with
its exact source identities, executed checks, fixture correction and limitations.
The added regression target is
crates/eventlog-sqlite/tests/atomic_blob_integrity_review.rs. The coordinator
includes its cases in the existing production-proof roster after review.
Full workspace and required real-backend CI remain pending.

Publication of the candidate branch is needed to trigger required CI. That
candidate is not integrated or selected by the consumer until those checks pass.

## Completed source qualification

The repaired source and all required regression roster entries merged in PR #15:
https://github.com/beyond10x/eventlog/pull/15
Its exact-head required persistence workflow passed:
https://github.com/beyond10x/eventlog/actions/runs/35720472752

That workflow supplied the disposable verified-TLS PostgreSQL fixture and passed
the required backend conformance/formatter/linter lane, comparative envelope and
restart replay lane. The local fixture-free full-gate refusal remains retained
and is not counted as passing PostgreSQL evidence. Independent SQLite review,
the original probe, validator mutation and complete local strict Clippy evidence
also remain retained. The consumer's full gate and direct old-to-repaired seed
compatibility proof passed separately; they do not replace provider CI.

No storage migration, operator data repair or tagged release was performed.
