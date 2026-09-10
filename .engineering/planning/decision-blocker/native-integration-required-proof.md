---
format: aep.planning-md/1
id: decision-blocker:native-integration-required-proof
kind: decision-blocker
status: open
title: Resolve required persistence proof before native source integration
relations:
- blocks: story:native-transaction-sessions
- blocks: story:native-document-queries
- blocks: story:committed-stream-enumeration
revision: 1
---
## Observed integration boundary

On 2026-09-10, remote main remained 55d90845ac22689c64b9bc96dcad2f9750075804 and the published native feature tip was f5cfba50afe6418dedac92a5b34e3d5f8fd6577a. The feature is a clean fast-forward candidate containing three bot-authored and bot-committed changes. git diff --check passed. The retained focused PostgreSQL run proves 13 affected cases; it does not claim the hosted persistence proof.

GitHub ruleset 22757491, Required shared and repository gates, is active with bypass_actors: []. It requires common / Security and privacy and real-backends from the GitHub Actions integration, with strict freshness. The exact feature tip had no check runs at inspection. The real-backends job in .github/workflows/persistence-proof.yml runs production-proof and the comparative/restart proof with a PostgreSQL service and a 30-minute timeout.

## Decision required

The operator explicitly instructed the session to skip the expensive remote persistence workflow. Respect that instruction. Skipping or disabling the workflow would not satisfy the enforced required check. Do not manufacture a check result, change branch protection or treat focused local tests as production evidence.

Integration needs either authorization to run the existing required proof or an explicit administrator decision changing this integration requirement. Until then, retain the published candidate and continue independent ESS migration work. This blocks source integration of these prerequisites; it does not establish that the whole ESS evolution goal is blocked. Source release, deployment and consumer adoption remain separate and unproven.

## Evidence

GitHub API: repos/beyond10x/eventlog/rulesets/22757491 and repos/beyond10x/eventlog/commits/f5cfba50afe6418dedac92a5b34e3d5f8fd6577a/check-runs, inspected 2026-09-10. Existing local-evidence:ess-evolution-20260910/eventlog-sessions-final.log contains the focused test results. No persistence workflow was launched and no repository settings were changed during this integration inspection.
