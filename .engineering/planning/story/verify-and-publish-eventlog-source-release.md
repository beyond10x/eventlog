---
format: aep.planning-md/1
id: story:verify-and-publish-eventlog-source-release
kind: story
status: implemented
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
  path: crates/eventlog-postgres/examples/observation/laboratory-profile.json
- confidence: cited
  path: crates/eventlog-postgres/examples/production-proof.rs
revision: 15
---
## Intent and authorization boundary

The operator approved implementing the complete reviewed plan, including all confirmed findings and final proof. Commit/push authorization is already present. The aep-drive:wave release stop remains a separate human gate: no version bump, release tag or publication until release authorization after a concrete reviewed and gated result is available. This record prevents the historical implemented preparation story from being mistaken for publication.

## Acceptance

The combined branch has no open confirmed actionable finding; complete real-backend/TLS production, static and comparative/restart proof passes for a frozen exact source; its wanted commits are published and merged to main. After separate release approval, version/license/changelog/tag metadata agrees, repository required checks and GitHub Release/artifacts are verified for the exact tag. Documentation delivery is reported separately and may remain pending.

## Evidence and scope

Required scripts and examples are described in AGENTS.md Releases and README Required proof and comparative laboratory. Baseline20e00c1eeab5d67bd5f749bbbd871b1fbfe7f796 is isolated in managed wt-d3a4aefef9fc; copied harness/dependency changes are intentional and retained as proof, with original runtime bytes verified. Candidate source must remain frozen through the comparative run. Fixture resource limit is two CPUs,1GiB,256 pids.

Coordinator owns Cargo.toml/Cargo.lock, LICENSE, CHANGELOG.md, README.md, required test roster, planning records and publication. No consumer promotion, deployment, Atlas/Website source change, documentation shell release or private key upload is included. Ordinary source completion follows this repository's current authority.

## Comparative fixture correction

Before this session's comparative measurement, Docker inspection returned `volume /var/lib/postgresql/data` for the assigned integration fixture. laboratory-profile.json:10 incorrectly described a container writable layer. Update the declaration to v7 with an explicit Docker data volume; preserve existing workloads, compiler policy, CPU/memory/pid limits and every acceptance budget. This corrects measured fixture provenance before measurement, not a threshold changed to pass a result. Earlier v6 receipts remain historical evidence. Root observed this directly on 2026-09-09 at00:59UTC.

## Observed implementation proof

Frozen candidate9bb72d903243b7bb54341771cb4953f3418f54b4 passed the full production gate (98/0/0, required cases complete), fmt and clippy, and fresh-fixture comparative proof (12 configurations,2 restarts,exit0). verification-report:final-integration-proof-20260909 records exact identities, paths and verdicts, including the earlier failed capacity and restart observations. No acceptance threshold was relaxed. Laboratory evidence does not admit a production deployment budget.

All implementation units and adversary reports are preserved, their wanted commits published, and their managed trees removed through reviewed exact-id GC: snapshotswt-f0ba183ee985, features/validationwt-21668c971b1d, catch-up/schemawt-655da8b1406c. Cargo clean reclaimed respectively1.4GiB,1.8GiB,1.4GiB. Comparative baselinewt-d3a4aefef9fc was published at recovery/comparative-proof-baseline-20260909 e40409bd5d3ed351d6effd4b5cd5ea1c5cdf5c33 after proof, Cargo cleaned232.9MiB, lease ended, finished and exact-id GC removed. All task-owned PostgreSQL fixtures have been stopped and removed with their anonymous volumes; private TLS material and small raw evidence remain outside the repositories in the task cache. Per-agent token/tool counters remain unavailable.

Only coordinatorwt-82ec9daed45d and exact Atlas authoritywt-acbc7275f11d remain for final publication and cleanup. Coordinator owns their next actions: publish this planning-only closure, merge the gated integration into clean main, verify the required GitHub workflow, then end own leases, clean disposable output, finish and review exact-id GC. Source runtime and workflows are unchanged from the frozen proof. Preserve all unmerged recovery branches and delete only this wave's merged local unit branches after ancestry checks.

All implementation stories are implemented. story:verify-and-publish-eventlog-source-release remains active for the separate human release stop and subsequent exact-tag source checks/artifacts. No release or documentation delivery is claimed.

## Release authorization

The operator answered “yes” to publishing Eventlog0.1.0 after the reviewed integration was complete and pushed at99ca640452de5ae397c8e3990364c714fc19a5a8. This is the separate source-release authorization required by the wave; no further release approval is pending. Release metadata follows the preserved plan: version0.1.0, Apache-2.0, source distribution with publish=false, dated changelog and annotated bare tag. Current Atlas release-unit eventlog/default declares tagged-github and required_artifacts=[]. Required persistence-proof artifacts will also be retained and verified for the exact release commit. Documentation delivery remains asynchronous.

Release checkoutwt-58b0b755e313 and Atlas authoritywt-778443cc0014 are owned by session eventlog-release-20260909. Atlas is clean at remote main38033fb4557e3b01c85379be95530f9f5e15e6ca. The previous integration worktrees and task fixtures were verified removed. Source publication will be recorded only after the exact tag, checks and bot-authored GitHub Release are observed.

## Verified source publication

Eventlog0.1.0 was published on2026-09-09 at01:41:45UTC: https://github.com/beyond10x/eventlog/releases/tag/0.1.0 . GitHub release385169693 is neither draft nor prerelease and its author is b10x-bot[bot]. Annotated tag object1662819ec855be7c5b658545f14df761adc9c7e0 peels to release commitdce9bcdbd0596c0f0756139b859a6ff2d05d8ba8, matching remote main at publication. Tagger, direct author and committer identities were verified as the organization bot.

Required Persistence proof workflow34299824161 passed on that exact clean commit:98 tests passed,0failed,0skipped,missing_required_cases=[]; formatting and clippy passed;12comparative configurations and2restart checks passed. Raw workflow artifacts were downloaded under /home/timo/.cache/eventlog-release-20260909/proof and their source identities and verdicts checked directly. Laboratory observations do not approve a production capacity budget.

All four workspace crates and Cargo.lock are0.1.0; source is Apache-2.0 and publish=false. Atlas release-unit eventlog/default declares tagged-github and required_artifacts=[]. No custom binary assets are required. The GitHub source tarball was downloaded and extracted; every file matched a local git archive of the tag. Download SHA256:561b9402bff67b00bf8feae8e71c06e125c27d2046737abd142d29292b647f6a. Release JSON, source archive, policy and metadata receipts are retained in the same task evidence directory.

Publication is complete. This subsequent planning-only closure does not move the release tag. Documentation publication remains pending/unverified and is handled asynchronously; no Atlas/Website source update, deployment or consumer promotion was performed. Coordinator owns publishing this closure and retiring releasewt-58b0b755e313 and authoritywt-778443cc0014 through reviewed exact-id managed cleanup. No disposable database fixture or build process was created for this metadata-only release step.
