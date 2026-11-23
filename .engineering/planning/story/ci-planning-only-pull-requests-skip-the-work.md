---
format: aep.planning-md/3
id: story:ci-planning-only-pull-requests-skip-the-work
kind: story
status: draft
title: A planning-only pull request runs no build or test job and its required checks still pass
summary: 'Rule 3 of 2026-11-21; PR #63 (only .engineering/) ran 6 workflows per round'
owner: eventlog
revision: 1
---
## Outcome

A pull request whose changed files are all planning or prose files (`.engineering/`, `*.md` outside the built docs site) runs no build, test, KPI or persistence-proof work, and every required check (`common / Security and privacy`, `real-backends`, `planning validate`) still reports success, so the pull request can merge. A change to the built docs site (`website/`) still runs the docs check and skips the code's build and test jobs. This is rule "Planning-only pull requests skip the tests" of `~/beyond10x/AGENTS.md` (Timo, 2026-11-21, proposal 3), raised by host-operator on 2026-11-23.

## Evidence

- Pull request #63 (only `.engineering/planning/story/*.md`, 9 files) ran 6 workflows per round on 2026-11-23: Persistence proof 2,581 s over 2 rounds, Planning store 2,471 s, Documentation source check 1,954 s, Documentation validation 1,804 s, KPI 1,198 s (1 round), Shared source gates 1,045 s (run_started_at to updated_at, from the Actions API).
- On 2026-11-23 the beyond10x runner queue was full and runbook's 0.8.0 release waited; host-operator had 4 queued runs of #63 cancelled (45134804944, 45134806598, 45134806675, 45134806673), which leaves #63 unmergeable until its required checks run again.

## Design constraint

`paths-ignore` on a workflow that provides a required check leaves that check pending forever. Each workflow that provides a required check (and each costly workflow: `persistence-proof.yml`, the KPI workflow, `shared-gates.yml`, the docs workflows) starts with a job that classifies the changed paths against one path list kept in the repository; the work jobs depend on it and are skipped, or their steps are, while the job that carries the required check's name still ends with success.

## Acceptance

A pull request that changes only `.engineering/` files merges with every required check green and no job that compiles or tests running longer than the classify job; a pull request that changes one `crates/` file runs the full set as today. The path list has a test case for both sides (a planning-only path set and a set with one code path), run by the planning or gate workflow.
