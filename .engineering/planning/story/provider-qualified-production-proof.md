---
format: aep.planning-md/1
id: story:provider-qualified-production-proof
kind: story
status: implemented
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
  path: crates/eventlog-sqlite/tests/production_execution_environment.rs
- confidence: cited
  path: docs/design/provider-qualified-production-proof.md
revision: 9
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

Scope refinement from independent pass2: one new gate-selected regression under
crates/eventlog-sqlite/tests/production_execution_environment.rs proves the peer package's Cargo
runtime environment. Its exact review patch is root-retained; no SQLite provider implementation
or existing test assertion is in scope.

Do not modify provider storage code, DDL, frozen file format, production credentials or existing
test assertions. Any unassigned shared-file change is returned as an unapplied coordinator patch.
This unit does not complete broader provider qualification or any real planning-store cutover.

## Verification

The corrected runner passed the actual `bash scripts/gate.sh --production-proof` on 2026-09-15:
152 passed, zero failed, zero skipped, no missing required cases, PostgreSQL 17.6 with the normal
and hosted-role verified-TLS lanes, followed by formatting and strict workspace all-target Clippy.
The gate process terminated with exit 0. Report format remains `eventlog-production-proof/1`;
`capacity_admitted` remains false.

The two independent attacks found distinct execution-context defects. Their immutable records are
review-result:provider-proof-adversary-pass-1 and review-result:provider-proof-adversary-pass-2.
Both exact adversarial tests are retained unchanged. Following the two-pass correction rule, Astra
verified the second correction, its causal mutation and the whole gate; no third attack was run.
Removing package-environment application while keeping the corrected working directory produces
151 passed, one failed, zero skipped and exit 1 in the actual production runner. Restoring it passes.

Full-gate report SHA256: 38b6fcf702e868d921f4847a0bb96a71075e6732cc372d6004966b9264671aa9.
The tested source was the dirty correction on 149fdc798d56d972b395fc86800acbb7dd01b7d9; the report
records that fact. All nonignored files were snapshotted before and after the gate and matched.
The candidate file-manifest SHA256 is ffadacf19148f6d85bfd4bcaa19aa6138bbbf316869177f765e1294c09dd7397.
Only explanatory design/planning closure is added after that tested snapshot. The coordinator's
retained run is ess-evolution-20260915, wave 0003, full-gate; its exact report, raw output, source
manifest, actual exit, mutation and both original review reports remain available there.

This closes provider/target attribution and Cargo execution-context preservation. Broader atomic
group recovery, blob-read integrity, canonical-file qualification and real store cutovers remain
separate planned work. No release, publication, deployment or capacity admission is claimed.
