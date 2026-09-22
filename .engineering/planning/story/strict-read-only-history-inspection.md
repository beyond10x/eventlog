---
format: aep.planning-md/1
id: story:strict-read-only-history-inspection
kind: story
status: active
title: Inspect File and SQLite history without changing source storage
scope:
- confidence: cited
  path: Cargo.lock
- confidence: inferred
  path: crates/eventlog-conformance/src/inspection.rs
- confidence: cited
  path: crates/eventlog-conformance/src/lib.rs
- confidence: inferred
  path: crates/eventlog-core/src/inspection.rs
- confidence: cited
  path: crates/eventlog-core/src/lib.rs
- confidence: inferred
  path: crates/eventlog-file/src/inspection.rs
- confidence: cited
  path: crates/eventlog-file/src/journal.rs
- confidence: cited
  path: crates/eventlog-file/src/lib.rs
- confidence: inferred
  path: crates/eventlog-file/tests/strict_inspection.rs
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof.rs
- confidence: cited
  path: crates/eventlog-sqlite/Cargo.toml
- confidence: inferred
  path: crates/eventlog-sqlite/src/inspection.rs
- confidence: cited
  path: crates/eventlog-sqlite/src/lib.rs
- confidence: inferred
  path: crates/eventlog-sqlite/tests/strict_inspection.rs
- confidence: inferred
  path: docs/design/strict-history-inspection.md
- confidence: inferred
  path: ess/inspection/domains/inspection.yaml
- confidence: inferred
  path: ess/inspection/system.yaml
revision: 8
---
## Outcome

File and SQLite expose a provider-owned strict read-only history inspector, compatible with published 0.2.1 stores. One call returns a tenant's complete recorded event envelopes under one native consistent observation, or a named refusal. Opening, inspecting, refusing and dropping the capability cannot create, repair, migrate, truncate, clean or otherwise write source files. The result is transient and grants no writer authority.

## Context

Ordinary File opening initializes and recovers its journal; ordinary SQLite opening selects WAL mode and executes DDL. Public read methods therefore do not constitute a nonmutating inventory API. Consumers need original event identity, event schema version, stream/global position, request identity and attribution, which already belong to RecordedEvent.

Implement this additive capability on published main 2e482aa1bbfcad7dc116a3eb1f442bb3512ebe8b, whose relevant provider formats match released 0.2.1 at 77cda0803b9392298952312e06b85b2d87548236. Do not import unrelated local evolution work. The active consistent-tenant-capture story has reusable strict File decisions, but its broader all-provider/blob/projection contract and SQLite BEGIN IMMEDIATE semantics do not satisfy this story unchanged. Coordinate overlapping ownership and retain existing acceptance.

## Contract before dispatch

Record the small opt-in inspection capability and transient coordinate/limit types in a repository-owned design and minimal validated ESS specification. Reuse TenantId, RecordedEvent and BoxFuture. Separate File and SQLite inspection types expose no EventStore writer, raw connection or callback. Existing writer APIs and persisted formats stay unchanged.

Return every RecordedEvent for the requested tenant exactly once in ascending global sequence, preserving every envelope field. Global sequence gaps are valid. Retain an existing tenant identity exactly if present, without requiring or minting one: a valid legacy store can have events without an identity-directory entry. Empty history with no stored identity means no recorded tenant evidence, not proof of a provisioned empty tenant.

Require finite explicit source/event/envelope-byte limits, checked arithmetic and no partial successful result. Named refusals cover missing source, unsupported format/platform, recovery required, corruption, redacted history, unavailable/changing source and exceeded limits. No inspection digest substitutes for an original validation receipt. This capability does not export command/group/claim tables, blob payloads, caches, projection rows or original lexical SQL/frame bytes, and its documentation must not claim complete-store backup or replay sufficiency.

## Acceptance

- File uses the existing provider manifest/frame decoder and state fold through a strict non-provisioning entry. It requires existing physical source and lock files, holds the native writer lock through observation, validates committed length/digest, and refuses pending append/privacy intent entries including dangling symlinks, unproven suffixes, missing authority files and unsupported/corrupt structures. It does no recovery or cleanup.
- SQLite bypasses ordinary construction and DDL, uses READ_ONLY without CREATE or writable SHM, and holds one native read transaction across exact supported schema admission, optional identity and event decoding. Initial support may be a documented strict cold-database subset. Healthy committed WAL is included only with demonstrated preservation, otherwise explicitly refused; main-file-only success is forbidden. Missing/unusable SHM or required hot-journal/recovery work refuses without creating or changing files. Live-source immutable mode and BEGIN IMMEDIATE are not permitted. Unsupported platform/VFS behavior is a named refusal, not a silent weakening. Demonstrate a successful supported SQLite fixture, and measure cleanly closed 0.2.1 WAL-mode stores explicitly; do not change their journal mode or checkpoint them as part of inspection.
- Both providers read synthetic frozen 0.2.1 fixtures, including history without tenant identity, and return exact event/request/schema/position/attribution values. Unknown provider shape or unsupported history does not become empty success. Redacted history produces a named refusal.
- Before/after entry and byte comparisons cover success, each refusal and handle drop with no concurrent writer, including SQLite DB/WAL/SHM/journal and File lock/manifest/log/staging/blob entries. Missing source or owner objects creates nothing. Concurrent append/checkpoint/privacy operations independently prove one native consistent observation or explicit refusal; do not misattribute writer changes to inspection.
- Limits are enforced before large input allocation or result accumulation, zero caps are respected and exceedance returns no partial successful history. No parallel EKR parser or product vocabulary enters Eventlog.
- Shared inspection semantics are exercised through both real File and SQLite providers. Keep every existing conformance case. Retain failing pre-fix or deliberate-mutation evidence for new regressions, restored green evidence, independent review and all mandatory repository proof.
- Publish the exact reviewed implementation through bot/common-Gates delivery; verify main reachability, source authority, required checks and the declared gate. A new version release is not required for exact-commit Git consumption. Consumer manifest and lock updates belong to the consumer's coordinated follow-up.

## Scope

Derived 2026-09-22 by story-scoper. Every entry distinguishes inspected source from proposed additions.

- **Primary API exports:** `crates/eventlog-core/src/lib.rs` — cited; RecordedEvent and BoxFuture already own the required envelope and async boundary.
- **New API module:** `crates/eventlog-core/src/inspection.rs` — inferred; separate transient limits/results/errors and read capability.
- **File exports:** `crates/eventlog-file/src/lib.rs` — cited; owns provider entry points and ordinary mutating transaction route.
- **File admission:** `crates/eventlog-file/src/journal.rs` — cited; existing manifest/frame decoder and mutating opener require a strict sibling.
- **File inspector:** `crates/eventlog-file/src/inspection.rs` — inferred; native lock and strict observation using existing state fold without changing its persisted encoding.
- **SQLite exports/decoder:** `crates/eventlog-sqlite/src/lib.rs` — cited; owns ordinary opener, schema and read_event decoder.
- **SQLite inspector:** `crates/eventlog-sqlite/src/inspection.rs` — inferred; read-only native admission/transaction without writer construction.
- **Shared test exports:** `crates/eventlog-conformance/src/lib.rs` — cited; existing shared exercise owner.
- **Shared inspection exercise:** `crates/eventlog-conformance/src/inspection.rs` — inferred; exact envelope, identity, limits and refusal parity.
- **File regression target:** `crates/eventlog-file/tests/strict_inspection.rs` — inferred; source preservation, native locking, recovery and frozen-format tests.
- **SQLite regression target:** `crates/eventlog-sqlite/tests/strict_inspection.rs` — inferred; WAL/SHM, no-DDL, source preservation, native snapshots and frozen-schema tests.
- **Required-case roster:** `crates/eventlog-postgres/examples/production-proof.rs` — cited; published main has the required array in this file, unlike evolution's split roster.
- **Design:** `docs/design/strict-history-inspection.md` — inferred; fixes scope, supported platforms/format, limits and refusal precedence before implementation.
- **ESS system:** `ess/inspection/system.yaml` — inferred; minimal value-only inspection specification.
- **ESS domain:** `ess/inspection/domains/inspection.yaml` — inferred; typed coordinates and limits without new persistent entities.
- **Confidence:** high — cited; current openers, envelopes, decoders, governance and the incompatible active capture acceptance were read directly; SQLite runtime preservation is still unproved.
- **Would collide with:** core exports; File journal/exports; SQLite exports/admission; shared conformance exports and production-proof roster — cited; active evolution capture/blob work owns these same surfaces. Sequence integration with that owner even when new module filenames differ.

## Verification and limits

Run focused provider/common cases, then bash scripts/gate.sh and the repository's mandatory real PostgreSQL TLS/hosted-role production proof, comparative and restart checks. Add exact new provider test names to the published-main production roster, removing none. Keep exact commands, source identity, test counts and mutation receipts. A green shared privacy check alone is not repository correctness.

No store cutover, history conversion, schema migration, object-byte relocation, blob reclamation, upstream dependency-family upgrade, PostgreSQL inspection or unrelated evolution integration is authorized by this story. Source bytes and identities remain available to the consumer's later verification; missing operation payloads or validation evidence remain a consumer migration refusal.

## Linux SQLite admission correction before implementation

The implementor's primitive measurement reports that READ_ONLY plus URI
readonly_shm=1 on an ordinary cleanly closed WAL-mode store can leave a newly
created WAL file even when opening fails. Keep the raw primitive evidence in the
unit report. A pathname/header preflight also races a native writer changing
journal mode; do not implement check-then-open as the no-write guarantee.

The coordinator approved a target-specific nix 0.30.1 dependency with its fs
feature and the lockfile update. Acquire a nonblocking Linux OFD shared lock over
the database before inspecting its header or sidecars and retain it through
connection drop. SQLite POSIX write locks must conflict even in the same process;
normal flock is insufficient. Unsupported platforms refuse. No repository unsafe
code, immutable-mode bypass or unrelated dependency-family upgrade is authorized.
Source path/inode checks must refuse detected replacement. The consistency claim
covers native cooperating SQLite writers, not arbitrary filesystem interference.

The updated design records this contract. Same-process and separate-process
writer tests, plus unchanged bytes/entries on success/refusal/drop, remain
required. Cleanly closed default WAL-mode sources may be named unsupported;
a separately prepared and frozen rollback-mode published-schema fixture must
establish useful success. Inspection never prepares or checkpoints its source.

## Descriptor preservation correction

The native primitive probe reproduced another side effect: closing an independent
database descriptor after refused OFD admission releases pre-existing same-process
SQLite POSIX locks. A separate process then acquires a write transaction.
See docs/design/strict-history-inspection.md and the wave's probe citation.

Adopt a Linux process-lifetime registry bounded at 64 descriptors. Reserve capacity
before any open; retain every successfully opened descriptor on success or refusal,
including identity mismatch and duplicate-inode races. Metadata-only lookup may
reuse retained descriptors. Nonblocking serialization prevents one inspection
unlocking another. Explicitly release the OFD lock only after native connection
drop; keep the descriptor alive. Exhaustion returns SourceBusy before opening.
No unsafe code, fork, unbounded retention or caller connection callback.

Runtime agreement remains unexecuted until the existing-writer/subprocess,
registry exhaustion/no-open preservation and concurrent-inspector cases pass.
This resource limitation is documented publicly; callers requiring additional
distinct sources use another process.
