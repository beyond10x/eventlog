---
format: aep.planning-md/1
id: review-result:input-validation-adversary-pass-1
kind: review-result
status: active
title: Public input validation adversary pass 1
relations:
- reviews: story:validate-public-append-inputs
revision: 1
---
unit: story:validate-public-append-inputs at eae613186532260679fb16f82e0a56e7c3faf44c against5c1ff2690f07ec0b91f14cce307fff2a6b65ddca
verdict: nothing found
cases: executed0→0, red0 (read-only review; implementor reports94 green)
origin: introduced0 / pre-existing0 / undecided0
wrote-outside-worktree: none
needs-coordinator: none

Git diff --stat: empty. No source or test edit was made by this review.

No new reachable failure was established in this bounded pass. No test was invented merely to duplicate the supplied matrix, and no suite was rerun. The94-case production gate, focused red cases and predicate mutations remain implementor-produced evidence, not new reviewer execution.

Read the complete four-file change, original three adversarial cases, shared public-input matrix, public PostgreSQL test, owning acceptance criteria and all relevant constructor/append callers. Verified in source that both backends invoke validate_append before beginning durable work; the validator traverses the entire batch before optional Claim validation returns. NewEvent::new and NewEvent::validate share one name/body rule; Claim::new and Claim::validate share all three field checks, with CommandMeta invoking the latter. Thus public mutable fields and serde-created NewEvent/Claim values meet the same append boundary as constructor-created values. The shared tests place invalid events after a valid first event and inspect event head, command receipt, claims, feed and projection state on both actual adapters.

Read every public TenantId/StreamId construction route: fields remain private, constructors enforce existing field rules, TenantId Deserialize converts through String→TenantId::new and StreamId through a private deserialization structure→StreamId::new. The nested tenant is validated too. Existing serialized JSON scalar/object shapes and field names are preserved. Exact512-byte ASCII values remain allowed;513-byte, empty, non-ASCII and control-byte values are rejected using the unchanged validate_field predicate. This intentionally preserves the existing broad printable-name grammar instead of imposing new naming policy.

Read the valid wire/claim/event round trips, original historical ASCII compatibility value and same-command retry control. No changed acceptance requirement for schema_version or unrelated read-only claim lookups was inferred. Known PostgreSQL rewrite-rule and snapshot findings are separately handled; no duplicates raised here.

This review began read-only while the implementor owned the tree, with no lease/build/mutation. After exact commit handoff, only this scratch report and its directory were written under the reviewer's own input-adversary-catchup-20260909 lease. No AEP, fixture/container or product mutation; no path outside this assigned worktree was written. Own lease released at final handoff.

```findings
[]
```
