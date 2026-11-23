---
format: aep.planning-md/3
id: story:tree-accepts-a-512-byte-tenant
kind: story
status: implemented
title: A Tree store accepts a valid 512-byte tenant instead of File name too long (os error 36)
summary: tree_inline_contract hashed and nested fail on a 512-byte tenant directory name
owner: eventlog
relations:
- serves: vision:O2
revision: 4
transitions:
- {from: "draft", to: "proposed", at: "2026-11-23T13:45:31Z", actor: "human:timo", revision: 2, decided_on: {"recorded":{"test_result":1}}}
- {from: "proposed", to: "active", at: "2026-11-23T13:45:31Z", actor: "human:timo", revision: 3, decided_on: {"recorded":{"test_result":1}}}
- {from: "active", to: "implemented", at: "2026-11-23T13:45:31Z", actor: "human:timo", revision: 4, decided_on: {"recorded":{"test_result":1}}}
---
## Outcome

A Tree store accepts every tenant id the core accepts, including one of 512 bytes
(`MAX_FIELD_LEN`, `crates/eventlog-core/src/lib.rs:70`), instead of failing with a backend error
because the tenant's directory name exceeds the file-system limit (255 bytes on Linux).

## Evidence

Found by agent el-d2 while building the L78 size check (2026-11-23). It added a 512-byte tenant to
the shared inline projection exercise; both Tree layouts failed:

```
---- tree_inline_contract::hashed stdout ----
panicked at crates/eventlog-conformance/src/lib.rs:1541:10:
a value at the bound is indexed: Backend("File name too long (os error 36)")
```

`tree_inline_contract::nested` failed the same way (log `el-d2/t6-tree.log:33,39` in the
coordinator's job folder). Tree keeps each tenant in a directory under `tenants/`
(`crates/eventlog-tree/src/history.rs:449,473` on `fix/review-findings`). Commit `9b0b56be` in PR #62
therefore runs the 512-byte tenant only in the PostgreSQL test and uses a short tenant in the shared
exercise, naming this as a separate defect.

## Acceptance

The shared inline projection exercise runs with a 512-byte tenant on every provider, and both
`tree_inline_contract::hashed` and `tree_inline_contract::nested` pass with it, writing and reading
that tenant's events.
