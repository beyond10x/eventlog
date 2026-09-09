---
format: aep.planning-md/1
id: story:verify-and-publish-eventlog-source-release
kind: story
status: draft
title: Verify integrated Eventlog and complete authorized source publication
tags:
- release
relations:
- informed_by: story:eventlog-0-1-0-release
- depends_on: story:prevent-stale-snapshots-after-redaction
- depends_on: story:validate-public-append-inputs
- depends_on: story:reject-postgres-rewrite-rules
scope:
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: Cargo.lock
- confidence: cited
  path: Cargo.toml
- confidence: inferred
  path: LICENSE
- confidence: cited
  path: README.md
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof.rs
revision: 7
---
## Intent and authorization boundary

The operator approved implementing the complete reviewed plan, including all confirmed findings and final proof. Commit/push authorization is already present. The aep-drive:wave release stop remains a separate human gate: no version bump, release tag or publication until release authorization after a concrete reviewed and gated result is available. This record prevents the historical implemented preparation story from being mistaken for publication.

## Acceptance

The combined branch has no open confirmed actionable finding; complete real-backend/TLS production, static and comparative/restart proof passes for a frozen exact source; its wanted commits are published and merged to main. After separate release approval, version/license/changelog/tag metadata agrees, repository required checks and GitHub Release/artifacts are verified for the exact tag. Documentation delivery is reported separately and may remain pending.

## Evidence and scope

Required scripts and examples are described in AGENTS.md Releases and README Required proof and comparative laboratory. Baseline20e00c1eeab5d67bd5f749bbbd871b1fbfe7f796 is isolated in managed wt-d3a4aefef9fc; copied harness/dependency changes are intentional and retained as proof, with original runtime bytes verified. Candidate source must remain frozen through the comparative run. Fixture resource limit is two CPUs,1GiB,256 pids.

Coordinator owns Cargo.toml/Cargo.lock, LICENSE, CHANGELOG.md, README.md, required test roster, planning records and publication. No consumer promotion, deployment, Atlas/Website source change, documentation shell release or private key upload is included. Ordinary source completion follows this repository's current authority.
