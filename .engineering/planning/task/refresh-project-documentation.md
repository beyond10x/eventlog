---
format: aep.planning-md/1
id: task:refresh-project-documentation
kind: task
status: active
title: Refresh released documentation and automatic project publication
relations:
- serves: vision:O2
revision: 4
---
## Context

Implement the operator-approved documentation refresh for Eventlog 0.3.0, Entity Runtime 0.19.0 and AEP 0.57.0. Preserve runtime behavior, public URLs, historical release records and passive portal bundles.

## Scope

Refresh this repository's README, public guides, navigation and contributor guidance; preserve validation checks and upload an exact main-commit b10x-project-site artifact including hidden files.

## Delivery order

1. Publish and verify build support and refreshed documentation in each source repository.
2. Generate the site callers through Atlas; land each caller before the Atlas roster/control change.
3. Validate Atlas reconciliation, provenance and Website portal contracts, then publish coordination changes.
4. Verify automatic deployments, exact source provenance, representative pages/assets and desktop/mobile layouts. Report portal propagation independently.

## Acceptance

The refreshed guides match the named releases, repository gates pass, and the three automatic project sites serve verified source commits at /eventlog/, /entity-runtime/ and /aep/ without root publication redeploying them.

## Evidence

Starting remote main: ac6b1731654329d32f1e3c9cf164fefad6a5b46a.

## Verified before build-support publication

The public site build and strict links passed. The 0.3.0 source passed the production-proof gate
with serial Rust tests and a disposable PostgreSQL 17.6 fixture using verified TLS, the hosted
application role and exact proof receipts. No Rust source or released storage behavior changed.
The read-only collector selects exactly README.md and the six public guide pages.
Generated caller installation and live verification remain pending.

The integration candidate incorporates concurrent SQLite repair a791284. The original draft and checks remain preserved on docs/refresh-project-site; this record is created through AEP on the new base, without splicing a sealed journal.

The integrated source at base a791284 passed the complete production-proof gate on 2026-09-22,
including the newly added SQLite integrity regressions. Its strict Docusaurus build passed.
