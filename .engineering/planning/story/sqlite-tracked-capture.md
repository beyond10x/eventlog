---
format: aep.planning-md/3
id: story:sqlite-tracked-capture
kind: story
status: implemented
title: Prove unchanged captures and contiguous append deltas
refs:
- provider: github
  reference: beyond10x/entity-runtime#51
relations:
- serves: vision:O2
scope:
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: crates/eventlog-conformance
- confidence: cited
  path: crates/eventlog-core/src/capture.rs
- confidence: cited
  path: crates/eventlog-file
- confidence: cited
  path: crates/eventlog-postgres/tests
- confidence: cited
  path: crates/eventlog-sqlite
- confidence: cited
  path: crates/eventlog-tree/tests/split.rs
- confidence: cited
  path: docs/design/consistent-tenant-capture.md
- confidence: cited
  path: docs/evidence/sqlite-tracked-capture
- confidence: cited
  path: ess/capture
revision: 11
transitions:
- {from: "draft", to: "proposed", at: "2026-10-03T15:35:44Z", actor: "human:timo", revision: 6}
- {from: "proposed", to: "active", at: "2026-10-03T15:35:44Z", actor: "human:timo", revision: 7}
- {from: "active", to: "implemented", at: "2026-10-03T16:28:49Z", actor: "human:timo", revision: 11, decided_on: {"recorded":{"test_result":1,"static_analysis":1,"review_outcome":2}}}
---
## Context

Entity Runtime issue #51 measures shared-clock operations whose cost follows all previous batches. Existing complete capture remains authoritative but cannot provide a bounded unchanged answer. The operator approved fixing all issues and explicitly accepted SQLite change tracking on 2026-10-03: SQL writes through any connection invalidate warm proof; raw file edits bypassing SQLite are outside the warm guarantee; opens fully verify.

## Accepted design

The transient types were authored and validated in ess/capture/domains/capture.yaml before this story. docs/design/consistent-tenant-capture.md records the exact optional capture_tenant_since contract and SQLite protocol. Return Complete, Unchanged or a complete acknowledged AppendDelta. Opaque immutable checkpoints bind one provider instance, tenant/generation, ordered projections, exact limits and an accounted SQL boundary. Default implementations preserve complete capture. No persisted bytes, schema, authorization or domain concepts change.

Sample data_version, total_changes64 and schema_version under the same connection mutex after BEGIN IMMEDIATE. Only acknowledge journal entries after successful commit, retaining the external stamp from within that transaction. Unaccounted local/external writes, missing retention, uncertainty or wrong scope force complete capture. Cumulative resource limits apply to the resulting whole observation.

## Acceptance

Named conformance cases full_on_open_and_unchanged_without_scans, append_delta_matches_complete_capture, cumulative_capture_limits_remain_exact, foreign_or_expired_checkpoint_falls_back, external_sql_tampering_invalidates_unchanged and unacknowledged_append_never_supplies_delta must exercise real SQLite. Include projection before/after coalescing, orphan bindings, rollback and schema/identity/blob/event/projection mutations. Mutation controls removing external-stamp comparison and a projection change must go red. Existing provider conformance and full repository gate remain required; absent PostgreSQL fixture is not a pass.

## Scope and delivery

Cited surfaces: eventlog-core capture.rs and exports, eventlog-sqlite capture.rs/atomic_group.rs/lib.rs, conformance and SQLite tests, capture ESS and design. New journal module is inferred. Independent adversarial review follows implementation. Coordinator owns AEP, changelog, bot commit/publication and exact dependency handoff. Branch fix/er-51-capture-checkpoints on 06c1e99c86c7130169c7ed584ec7326f7e9a546e, managed tree er-51-capture-20261003; scratch target/issue-51-scratch, build target. One Entity Runtime PR consumes a bot-published exact upstream commit; no upstream PR, merge or release is requested.

## Coordinator verification and correction

Latest stable Rust1.99 full workspace gate passed before the independent review addition. The required PostgreSQL17.6/TLS persistence-proof run executed541 cases, failed0, skipped0 and reported no missing required cases; formatter and strict workspace Clippy also passed. These are working-tree observations over base06c1e99, not claims about a published revision. Original raw output and binary/source metadata are retained in assigned scratch. Independent review then reproduced a projection alias defect: public projector writes using the same physical table name but a different indexed-field specification were omitted from the delta. The fix forces a complete capture for that alias, and the original isolated red is now green. Final proof must include this correction and the added independent tests before publication.

Current Rust1.99 additionally required mechanical equivalent empty-value assertions in existing conformance, File, SQLite, PostgreSQL and Tree tests. This compatibility work preserves every assertion's behavior and uses no lint suppression. No unrelated provider behavior or persisted format changes.

## Final scope and verification

Confirmed source scope: core capture declarations/exports; SQLite capture, bounded journal, atomic append hooks, projector writes and private initializer; typed transient capture ESS and design. Independent tests are tracked_capture_review.rs; four cases add to the 19 implementor regressions. Core/conformance/SQLite package199 passed after correction. Named tests unchanged_capture_reuses_the_exact_provider_observation, acknowledged_groups_form_a_complete_delta_and_deduplication_adds_nothing, projection_before_after_values_and_usage_reconstruct_the_complete_capture, limits_apply_to_the_complete_result_and_changed_limits_require_full_capture, foreign_scope_unjournaled_writes_and_expired_journals_fall_back, warm_capture_detects_external_blob_tampering_without_an_event_head_change and refused_blob_group_does_not_publish_a_partial_delta are the actual executed acceptance cases, supplemented by alias/tenant/authority probes and private commit-race controls.

Both final gates exit0. The required real PostgreSQL17.6/TLS production proof executes545, fails0, skips0, with no missing required cases. Formatter and strict workspace Clippy exit0 under latest stable Rust1.99. Public raw runner JSON and exact crate/Cargo source hashes are retained in docs/evidence/sqlite-tracked-capture. The report records source_dirty=true over the original base; source hashes identify the tested implementation. No deployment-capacity claim, main merge or release follows from these observations. Capture ESS validates and compiles; Entity Runtime separately executes its seven authored SQLite/facade scenarios with ten generated command cases.

The original alias omission and corrected recheck are separate immutable review records. All reported introduced findings are fixed. Scope also includes the necessary mechanical current-Rust assertion updates, changelog, public verification artifacts and governed planning. Agent token/tool/duration accounting was not exposed by the harness and is unknown.
