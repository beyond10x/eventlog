---
format: aep.planning-md/1
id: story:atlas-source-gate-reuse
kind: story
status: implemented
title: Reuse Eventlog proof through Atlas source admission
relations:
- informed_by: story:verify-and-publish-eventlog-source-release
scope:
- confidence: cited
  path: .github/workflows/persistence-proof.yml
- confidence: cited
  path: .github/workflows/shared-gates.yml
- confidence: cited
  path: AGENTS.md
- confidence: cited
  path: CHANGELOG.md
- confidence: cited
  path: README.md
revision: 7
---
## Superseded proposal

The operator's Public shared gates, independent of Atlas implementation plan replaces this unpublished design. No Atlas proof reuse implementation shipped. Gates owns common checks, signed local evidence and bot delivery; Eventlog retains its full correctness and release requirements. Atlas owns documentation coordination only. No expensive Eventlog proof reuse is included in the first release.

## Replacement acceptance

Adopt verified, immutable beyond10x/gates tooling with an explicit baseline and required shared check, preserve bot identity and all Persistence proof lanes, enable supported secret scanning/push protection, remove unpublished Atlas-authority guidance, and retain the separate Rust cache and superseded-PR cancellation improvements. Commit/publish succeeds with Atlas unavailable. Historical findings are reported without rewriting history.

## Scope and references

AGENTS.md and .github/workflows/persistence-proof.yml are the retained draft edits. Add a trusted base-branch shared gate caller and Gates configuration only after producer publication. Atlas task:eventlog-source-gate-reuse coordinates the authority migration. The prior planning journal is retained as historical evidence; its old intent is superseded by this revision. No persistence behavior changes or Eventlog release are requested.
