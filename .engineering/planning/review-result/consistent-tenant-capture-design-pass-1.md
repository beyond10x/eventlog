---
format: aep.planning-md/1
id: review-result:consistent-tenant-capture-design-pass-1
kind: review-result
status: active
title: Consistent tenant capture design review pass 1
relations:
- reviews: story:consistent-tenant-capture
revision: 1
---
needs-revision
story:consistent-tenant-capture — The body must define whether `RedactedHistory` or `TenantIdentityMissing` wins when an ordinarily appended, never-provisioned tenant is later redacted, because append does not provision stream identity and both mandatory refusals then apply — docs/design/consistent-tenant-capture.md:58
story:consistent-tenant-capture — The body must define the accepted stored stream-identity grammar and treatment of legacy nonconforming strings, because it requires corruption for an “invalid” identity while the existing public contract exposes a stable nonempty `String` — docs/design/consistent-tenant-capture.md:62
story:consistent-tenant-capture — The body must name the projection-row key grammar or explicitly preserve every key accepted by current writers, because “decode and validate” otherwise introduces an undecided compatibility refusal for unrestricted `&str` keys — docs/design/consistent-tenant-capture.md:105
Read: 1 planning artifact (`story:consistent-tenant-capture`, revision 4), the complete design, both capture-model files, repository instructions, the core/File/SQLite/PostgreSQL source reached by identity, history, blob, projection, schema, locking and retirement behavior, the provider-qualified roster, and existing consumer complete-snapshot verification types, using `git rev-parse`, `sha256sum`, `rg`, `wc`, `nl`, and `sed`.
Could not establish: acceptance of the SQL blob-integrity dependency; its required second examination remains incomplete and was expressly excluded.
Could not establish: implementation correctness or provider-qualified execution; no capture implementation exists, and this pass performed no builds, tests, database, or network operations.
Could not establish: the future consumer adapter encoding; existing consumer types can verify complete records and terminal materializations, but the adapter and bridge remain outside this unit.
Could not independently establish the brief’s reported model validation/compile and planning-validation results because those frozen commands were not rerun.
```findings
- file: docs/design/consistent-tenant-capture.md
  line: 58
  category: design
  severity: blocker
  verdict: needs-revision
  origin: introduced
  message: The body must define whether `RedactedHistory` or `TenantIdentityMissing` wins when an ordinarily appended, never-provisioned tenant is later redacted, because append does not provision stream identity and both mandatory refusals then apply
- file: docs/design/consistent-tenant-capture.md
  line: 62
  category: design
  severity: blocker
  verdict: needs-revision
  origin: introduced
  message: The body must define the accepted stored stream-identity grammar and treatment of legacy nonconforming strings, because it requires corruption for an “invalid” identity while the existing public contract exposes a stable nonempty `String`
- file: docs/design/consistent-tenant-capture.md
  line: 105
  category: design
  severity: blocker
  verdict: needs-revision
  origin: introduced
  message: The body must name the projection-row key grammar or explicitly preserve every key accepted by current writers, because “decode and validate” otherwise introduces an undecided compatibility refusal for unrestricted `&str` keys
```

