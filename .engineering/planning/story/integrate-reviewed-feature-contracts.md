---
format: aep.planning-md/1
id: story:integrate-reviewed-feature-contracts
kind: story
status: active
title: Integrate reviewed guarded refusal and effect contracts
relations:
- informed_by: story:eventlog-0-1-0-release
scope:
- confidence: cited
  path: crates/eventlog-conformance/src/lib.rs
- confidence: cited
  path: crates/eventlog-core/src/lib.rs
- confidence: cited
  path: crates/eventlog-core/src/projection.rs
- confidence: inferred
  path: ess/effects/domains/effects.yaml
- confidence: inferred
  path: ess/effects/system.yaml
revision: 5
---
## Acceptance

The existing guarded-refusal implementation from 91e62a5010de1ca0fa7a1d64324c51b259e9a805 and effect-evidence implementation from d3971f21f876b6ea0488b34ab35cb0c08b2fd0c7 are integrated onto current main with negative metadata validation coverage, shared backend guard rollback proof, and a validated ESS home for introduced effect data.

## Scope

Cited: crates/eventlog-core/src/lib.rs, crates/eventlog-core/src/projection.rs, crates/eventlog-conformance/src/lib.rs. Inferred: ess/effects for code-derived metadata specification. Coordinator owns planning, README, CHANGELOG, workspace Cargo metadata, and legacy-story annotations.

## Constraints

Keep Eventlog domain-neutral, preserve serde field/variant names from the existing feature, and enforce the opaque identifiers and non-sensitive codes promised by its contract. Do not import either branch's independent AEP journal. Backend implementation is outside this unit. Run failing negative tests first, then apply the minimal fixes; validate ESS and run appropriate test/fmt/clippy gates. Both branches remain remote provenance, not evidence of main integration.
