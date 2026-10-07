# Durable capture continuity wave

Skill: aep:implementing 0.20.1 (wave mode). Approved by the operator on 2026-10-07 as one wave
with one integration branch and one pull request. Serves O2. Source:
https://github.com/beyond10x/eventlog/issues/39.

## Selection

N=1: story:durable-capture-continuity. `aep plan artifact waves --status active` output is
[durable-capture-continuity-selection.json](durable-capture-continuity-selection.json): one wave,
no collisions, no unassessed stories. Scope is inferred from
`docs/design/durable-capture-continuity.md`; the implementor's confirmation replaces it at close.
story:generate-capture-types-from-ess stays draft and outside the wave: it waits on an ESS release.

## Branches and trees

| role | branch | managed id / tree | build | scratch | stage |
|---|---|---|---|---|---|
| integration | `wave/durable-capture-continuity` | `wave-durable` | tree `target/` | `~/.cache/eventlog-dsp15/wave` | opened at `0b96dd51` (plan `4496d9ae` + main `cf7f61e1`) |
| unit | `feat/durable-capture-continuity` | `durable-continuity` | tree `target/` | `~/.cache/eventlog-dsp15` | unit commit `d9a07279`; review round 1 NEEDS-CHANGE (adversary 6 red, security 5 red); implementor fixing |

The unit tree was opened on the plan commit before the wave shape was set; it is the unit's branch.
Only the coordinator writes the planning store, in the integration tree.

## Commits this approval authorises

The unit commits on `feat/durable-capture-continuity` (implementation, review tests, review fixes); its merge into
`wave/durable-capture-continuity`; the closing store commit; one pull request from the integration
branch, merged into `main` after CI. The 0.8.0 release follows `AGENTS.md` § Releases as separate
work.

## Agents

`aep:implementor` (one), then `aep:adversary` and `aep:security-reviewer` on the green unit: the
change adds triggers and a durable journal holding projection row copies to stores that hold other
owners' durable state.

## Gate

`bash scripts/gate.sh --production-proof` once on the integration branch, against a disposable
PostgreSQL 17.6 with verified TLS, per-step output kept under the scratch root.

## Cost

| agent | tokens | tool uses | wall |
|---|---|---|---|
| `aep:implementor` | 557,859 | 162 | 66 min |
| `aep:adversary` pass 1 | 313,481 | 57 | 20 min |
| `aep:security-reviewer` pass 1 | 285,039 | 86 | 22 min |
