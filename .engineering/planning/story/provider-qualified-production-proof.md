---
format: aep.planning-md/1
id: story:provider-qualified-production-proof
kind: story
status: active
title: Bind required production cases to their provider and test target
relations:
- serves: vision:O2
- informed_by: story:blob-binding-conflicts
- informed_by: story:atomic-append-groups
scope:
- confidence: inferred
  path: CHANGELOG.md
- confidence: inferred
  path: crates/eventlog-postgres/Cargo.toml
- confidence: inferred
  path: crates/eventlog-postgres/examples/
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof.rs
- confidence: inferred
  path: crates/eventlog-postgres/tests/
- confidence: cited
  path: docs/design/provider-qualified-production-proof.md
revision: 5
---
## Outcome

The required production proof refuses any missing provider/target case even when another selected
provider reports the same successful test name.

## Authority and design

Approved ESS evolution revision 1, plan ess-evolution-20260915, operator decision 2026-09-15.
Digest: 7579145c3de5a1c6f8088fd7fb804d29dac8903ec505048f3ce595c45023b787.
Binding design: docs/design/provider-qualified-production-proof.md, accepted by Astra as delegated
planner within that scope. Existing production-proof values and provider contracts are reused;
this story introduces no new domain entity or storage model.

## Acceptance

- Demonstrate the current false acceptance when one required SQL provider/target is missing while
  its peer passes the identical case name. Record the actual red regression before correction.
- Bind each required successful case to its exact package, test target and case name from actual
  execution authority. Retain all existing required cases and their applicable provider lanes.
- Refuse absent/wrong targets, selected-zero, ignored, skipped, failed and inconsistent execution.
  An unrelated target's output must never satisfy another target's requirement.
- Preserve full workspace and documentation coverage, fixture/TLS requirements, raw evidence,
  actual process exits, production-proof/1 meaning and the separate capacity-admission boundary.
- Ensure the new Rust regressions run under the actual repository gate. Removing target/provider
  attribution must make the decisive regression fail; restore it and retain both outputs.
- Pass affected tests, formatting, strict all-target Clippy, independent adversarial review and
  bash scripts/gate.sh --production-proof with the disposable PostgreSQL/TLS lanes actually run.

## Scope

- crates/eventlog-postgres/examples/production-proof.rs — cited; combined-output roster matcher.
- crates/eventlog-postgres/examples/ — inferred; a narrowly scoped Rust parser/runner support module
  may be needed, based on actual Cargo target ownership rather than an invented global registry.
- crates/eventlog-postgres/tests/ — inferred; gate-selected independent admission regression location.
- crates/eventlog-postgres/Cargo.toml — inferred; only if required to select the regression target.
- docs/design/provider-qualified-production-proof.md — cited; root-owned binding design.
- CHANGELOG.md — inferred; record the verifier correction without release claims.
- Confidence: high for the defect; mechanism and exact regression target must be verified in source.
- Would collide with: another production runner, PostgreSQL manifest or gate selection change.

## Limits

Do not modify provider storage code, DDL, frozen file format, production credentials or existing
test assertions. Any unassigned shared-file change is returned as an unapplied coordinator patch.
This unit does not complete broader provider qualification or any real planning-store cutover.
