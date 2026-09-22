---
format: aep.planning-md/1
id: story:caller-owned-postgres-connections-for-er-facades
kind: story
status: draft
title: Preserve caller-owned PostgreSQL connections in the existing bounded pool
refs:
- provider: er
  reference: story:eventlog-provider-facades-and-legacy-imports
- provider: ess
  reference: initiative:ess-evolution
relations:
- serves: vision:O2
- supersedes: task:caller-owned-postgres-connections-for-er-facades
scope:
- confidence: inferred
  path: CHANGELOG.md
- confidence: inferred
  path: Cargo.lock
- confidence: inferred
  path: README.md
- confidence: inferred
  path: crates/eventlog-postgres/Cargo.toml
- confidence: cited
  path: crates/eventlog-postgres/src/lib.rs
- confidence: cited
  path: crates/eventlog-postgres/src/pool.rs
- confidence: inferred
  path: crates/eventlog-postgres/tests/
- confidence: inferred
  path: docs/design/
revision: 4
---
# Preserve caller-owned PostgreSQL connection authority for ER facades

## Approved requirement and witness

ESS evolution approved revision1 §3/M2 requires compatible PostgreSQL facades; the existing full ER facade/import contract explicitly preserves caller-selected transport/TLS/client authority. ER PostgresStore::from_client accepts a caller-created postgres::Client. Exact Eventlog43ceaa09ceec610e25891815e33e03e8df92ee28 pool.rs:69–154 only admits URL plus CA roots and builds rustls with_no_client_auth; Pool::connect hardcodes MakeRustlsConnect/NoTls. PostgresEventStore::from_pool is private. Root inspected pool source and confirmed this is insufficient for caller-owned custom/mutual-TLS connection establishment. Keeping from_client only as a legacy acquisition reader does not prove destination equivalence.

This bounded companion implements the missing part of the EXISTING complete M2 facade assignment. It is not an administration review, provider qualification or new SQL backend. No persisted format, schema, blob, projection-administration or baseline change is required. Independent File/SQLite/import implementation proceeds meanwhile; only caller-owned PostgreSQL facade acceptance and downstream qualified M2/M3/M4/M7 integration depend on this capability.

## Acceptance and stopping condition

A host can supply its PostgreSQL connection authority to the existing provider pool and ER facade while retaining connection/waiter/time bounds, exact schema/role/budget admission, transaction cancellation quarantine, connection-driver lifetime and bounded shutdown; actual PostgreSQL/TLS and causal refusal tests plus the full provider gate establish that the supplied authority is used and no connection escapes accounting.

Add only a concrete public connection factory/authority and typed client-plus-owned-driver return, with a caller-owned PostgresConfig constructor (or equivalently bounded API). Existing pool owns timeout, spawning, reuse, quarantine and retirement. Cancellation before establishment cannot leak a driver or capacity; every established driver remains owned until joined/aborted. Existing verified/isolated config semantics and confidential Debug/error boundaries remain. No unsupported promise to reuse one synchronous Client across an async pool: preserve host authority through explicit connection creation and document the compatibility mapping. No unbounded callback/plugin system, second pool or SQL provider.

Source scope: crates/eventlog-postgres/src/pool.rs and constructor/reexports in crates/eventlog-postgres/src/lib.rs; focused tests and concrete API design/docs/CHANGELOG; manifests only if strictly necessary. Actual supplied custom/mutual-TLS connection test, connection-timeout/cancel/driver-failure/reuse/overload/shutdown/hosted-admission checks, strict fmt/lint/docs and full actual-PG Eventlog gate on the exact candidate. Preserve old evidence, no skip. Stop this companion after exact source/check report and root freeze; ER then pins that candidate and proves facade use. Root integrates only after all original qualification requirements, including unresolved administration review, are satisfied.

## Ownership and limits

Assigned within operation_fulfillment_correction's current complete M2 contract; no new worker or informal extension of a closed assignment. Worker creates a managed Eventlog checkout from exact43ceaa09; root sole AEP writer/freezer/integrator. No source edits until this scoped record is established. This story records the cross-repository API ownership, not a new completion outcome or reset review unit. M2's original whole facade/import source examination covers this companion delta; it cannot replace the terminated administration examination. Do not inspect or execute denied administration reviewer material/cases, alter administration code, retry its examination, switch reviewer/model to route around denial, or claim provider qualification. No publication/release/deployment/cutover.

## Scope record correction

The CLI refused machine scope on the initially created task because only stories own scope. That draft task is archived and this story supersedes it; no evidence or review budget is reset. This is one cross-repository connection-authority deliverable within the original M2 assignment.

## Verified local candidate and ER handoff

The bounded caller-owned PostgreSQL connection-authority companion is frozen as local bot candidate088c27b5df9745c68d8a2240dbb2038998b03efe,tree6f9c9721607916d2aefb948609e5b1545e1660d8,parent43ceaa09ceec610e25891815e33e03e8df92ee28. Exact five source paths and28check artifacts independently rehashed;57planning files unchanged. Regular full gate12 and production-proof13 exit0;248production cases,0failed,0skipped. The proof does not admit a production capacity budget. Existing administration review restriction and native provider qualification remain open.

Source manifest2182b213fc9a5aff5ae1131ff3a9ebe106bbf904a22e7fa303b5f8364700d39f; authorreport1a372c9b1b5dba767293a78e1b20c90ed9557595347ddeed881e7d4d273a2aa8. Root exactfreeze/common receipt at ~/beyond10x/.ess-evolution/waves/0007-er-eventlog-adapter/postgres-caller-authority/freeze/receipt.json. Standalone common-check defaults initially lacked installed protected configuration; existing repository hook config selected without policy changes or key disclosure, commoncheck/verify0. One local Cargo Git database seeded; no publication/integration claim. Root freeze lease ended.

ER facade author now pins this exact candidate and regenerates Cargo metadata offline. Original companion fixture was retired after both gates with exact ID/owner/absence proof. A new isolated PG17.6/TLS1.3 fixture supplies remaining original ER actual acceptance under the same M2 owner; custody acknowledged, full ER gate/independent examination/integration remain. No administration-review replacement or new review budget.
