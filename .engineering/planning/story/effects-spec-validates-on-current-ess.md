---
format: aep.planning-md/3
id: story:effects-spec-validates-on-current-ess
kind: story
status: active
title: The effects specification validates on the current ESS
relations:
- serves: vision:O2
scope:
- confidence: cited
  path: ess/effects/domains/effects.yaml
revision: 4
transitions:
- {from: "draft", to: "proposed", at: "2026-10-08T09:07:14Z", actor: "human:timo", revision: 3}
- {from: "proposed", to: "active", at: "2026-10-08T09:07:14Z", actor: "human:timo", revision: 4}
---
## Outcome

`ess validate --path ess/effects` passes on ess 0.56.0 with the same meaning as before.

## Why

At ess 0.56.0 `ess/effects` was refused: `[type_mismatch] types.eventlog.effects.EffectBoundaryCoverage.invariants[1]: records_terminal == true or records_incomplete == true: operator == does not admit Boolean (Bool) and Text literal ...` (`domains/effects.yaml:67`, introduced in 096cdbe7). The compact predicate form has no `or`; ESS-SPEC-012 names structured `all`/`any`/`not` as the way to combine predicates.

## Acceptance

- Invariant 1 of `eventlog.effects.EffectBoundaryCoverage` is `any: [records_terminal == true, records_incomplete == true]`; every `ess/*` specification with a `system.yaml` validates at ess 0.56.0.
- No generated output and no Rust code change: `ess/effects` has no generated projection in this repository, so nothing regenerates.
