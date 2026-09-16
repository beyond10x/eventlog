---
format: aep.planning-md/1
id: review-result:inline-projection-admin-design-pass-1
kind: review-result
status: active
title: Inline projection administration design review pass 1
relations:
- reviews: story:inline-projection-administration
revision: 1
---
needs-revision
story:inline-projection-administration — The PostgreSQL attachment section must select REPEATABLE READ or an equivalent single-snapshot rule because READ ONLY alone does not make its multi-query registry/catalog comparison coherent. — docs/design/inline-projection-administration.md:166
story:inline-projection-administration — The SQLite and PostgreSQL rebuild mechanics must name and hold registration/freeze coordination for the full operation because retaining one Arc does not enforce the stated guarantee that local registration remains unchanged. — docs/design/inline-projection-administration.md:108
story:inline-projection-administration — The File qualification must drive pending append and pending privacy intents through both new administration paths and verify refusal leaves canonical bytes untouched because the listed process-death/reopen case does not establish those explicit mechanics. — docs/design/inline-projection-administration.md:234

What I read: 8 proposal/dependency files, 12 accepted Eventlog Rust source files and the accepted File provider design via `sha256sum`, `nl -ba`, and read-only `git show 18322cbe19f0068e9b3fab84d874d6abea181f3e:<path>`.
What I could not establish: provider implementation or qualification, because consistent capture and SQL blob-integrity source remain unaccepted dependencies and no inline-administration implementation exists.
What I could not establish: ER source readiness, because the frozen consumer still states dirty attachment refusal and an Arc-taking rebuild at `docs/design/eventlog-recorded-indexes-v0.1.md:301` and `docs/design/eventlog-recorded-indexes-v0.1.md:359`; the selected amendments remain a prerequisite before source implementation.

```findings
- file: docs/design/inline-projection-administration.md
  line: 166
  category: design
  severity: blocker
  verdict: needs-revision
  origin: introduced
  message: The PostgreSQL attachment section must select REPEATABLE READ or an equivalent single-snapshot rule because READ ONLY alone does not make its multi-query registry/catalog comparison coherent.
- file: docs/design/inline-projection-administration.md
  line: 108
  category: design
  severity: warning
  verdict: needs-revision
  origin: introduced
  message: The SQLite and PostgreSQL rebuild mechanics must name and hold registration/freeze coordination for the full operation because retaining one Arc does not enforce the stated guarantee that local registration remains unchanged.
- file: docs/design/inline-projection-administration.md
  line: 234
  category: design
  severity: warning
  verdict: needs-revision
  origin: introduced
  message: The File qualification must drive pending append and pending privacy intents through both new administration paths and verify refusal leaves canonical bytes untouched because the listed process-death/reopen case does not establish those explicit mechanics.
```

