---
format: aep.planning-md/1
id: story:native-transaction-sessions
kind: story
status: active
title: Compose dynamic Eventlog work inside one caller transaction
relations:
- depends_on: story:native-document-queries
- depends_on: story:atomic-append-groups
scope:
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-conformance
- confidence: cited
  path: crates/eventlog-core
- confidence: cited
  path: crates/eventlog-postgres
- confidence: cited
  path: docs/design/native-transactions.md
- confidence: cited
  path: ess/admission
revision: 5
---
## Acceptance
A caller can read its staged streams and inline documents, lock present or absent identities, reserve tenant-scoped sequence ranges and append dynamically chosen atomic groups within one PostgreSQL transaction, with complete outer rollback and no surviving prefix from a caught failed group.

## Design
Add an optional TransactionalEventStore capability and object-safe transaction view. The host supplies a scoped callback and owns its returned value; Eventlog invokes it once and commits only success. Group appends reuse the existing native group implementation and durable request fingerprint/ranges inside a savepoint. A caught group refusal restores the session without publishing its state or receipt. Cancellation of an in-flight session operation poisons the session so a caught/dropped future cannot turn partial work into a commit. An outer cancellation/unknown commit uses existing pool quarantine and UnknownCommit reporting; no automatic callback replay or fresh command identity is introduced.

The publication gate precedes all callback locks. Stream locks use the existing writer coordinates; logical identity locks and monotonically reserved counter coordinates remain tenant scoped and collision-separated from admission scopes. Reuse the existing ScopeCounter in ess/admission and its additive counter table, not a new event store. No DDL, event format or feed watermark change is needed. A caller that mixes multiple lock operations owns the wider lock order; this does not claim arbitrary transactions are deadlock-free. Sequence-only outer retries are not deduplicated: a caller must resolve an unknown outcome rather than retry blindly.

Use the existing ProjectionStore view for transaction-local document/blob reads and projection locks, with its tenant/inline/admission restrictions. Share stream reads with the outer provider. The API does not lend a database connection, credentials, runtime or commit operation to the callback. PostgreSQL provides the capability; other providers retain their existing APIs rather than emulating a transaction with independent appends.

## Scope
crates/eventlog-core transaction contracts; crates/eventlog-postgres transaction composition and stream-read reuse; focused PostgreSQL/shared applicable conformance; docs/design/native-transactions.md; CHANGELOG.md. This is one prerequisite for ER session and SQL facade convergence, not a parallel wave or completed consumer adoption.

## Verification
Use a disposable PostgreSQL fixture for read-your-writes, external invisibility, nested group rollback, exact retries, outer rollback including sequence reservations, absent-identity contention, tenant boundaries, group compatibility, cancellation and caught in-flight operation cancellation. Inject and restore savepoint/cancellation mutations. Run only affected checks; no full or remote persistence gate.

## Implementation and observed evidence

PostgreSQL implements the optional core TransactionalEventStore and object-safe TransactionSession. The callback owns dynamic composition but never receives SQL/commit authority. Shared stream readers preserve outer-provider semantics. Group savepoints reuse original group fingerprints and receipts; caught group refusal leaves no prefix. Every started operation, including direct projection operations, retains a cancellation marker until settlement. Successful callback results cannot hide unfinished SQL. Callback-propagated refusals retain their original error. Generation capture holds a shared row lock before history reads, fencing redaction through transaction end.

ScopeCounter in ess/admission/domains/admission.yaml records collision-separated q tenant sequences beside t/d admission scopes. ProjectionRegistration now models the existing native-query flag and its legacy false encoding. ESS validation passed and before/after compiled IR is retained. No DDL, event format, feed watermark or application-domain type changed. Old eraser binaries must be fenced because they do not erase q coordinates; unchanged physical tables do not authorize a mixed protocol deployment.

The final focused PostgreSQL run passed 13 cases: six native session cases, two existing atomic-group cases, three native document-query cases and two stream-inventory cases. Test execution totaled 0.91 seconds after 3.59 seconds of compilation. It covers staged visibility, dynamic reads/queries, exact retries, prefix/receipt/counter rollback, sequence bounds and tenant erasure, absent identity and ordinary writer contention, foreign reads, generation/redaction interleaving, and cancellation of a staged group or blocked projection write. The outer-deadline case explicitly observes a staged group before timeout. Removing group savepoint rollback failed the surviving-prefix assertion; disabling the pending-operation guard failed both cancellation cases; removing the generation row lock failed the redaction-blocking assertion. All mutations were restored before the passing final run.

Affected core/provider/conformance strict all-target Clippy passed after replacing wildcard and unused imports. Rustdoc with warnings denied, format/diff checks, and Rust 1.91 workspace all-target compilation passed. The compatibility check includes File and SQLite but is not a rerun of their persistence suites. No full gate, hosted production proof or remote expensive proof was run.

Evidence: local-evidence:ess-evolution-20260910/eventlog-sessions-final.log; eventlog-sessions-savepoint-mutation.log; eventlog-sessions-cancellation-mutation.log; eventlog-sessions-generation-mutation.log; eventlog-sessions-clippy.log; eventlog-sessions-rustdoc.log; eventlog-sessions-msrv.log; eventlog-session-admission-before.json; eventlog-session-admission-after.json.

The native prerequisite is implemented on the feature branch; keep this story active until integration. ER adoption, compatible SQL facade replacement, catalog dependency intent and the wider ESS/application acceptance remain required. The continuing session owns the next action: consume this native boundary in ER with shared recorded verification and transaction-local queries, preserving typed caller refusals and discarding rolled-back cached state. A sequence-only unknown outcome remains non-deduplicated; adoption must not invent automatic callback retries.
