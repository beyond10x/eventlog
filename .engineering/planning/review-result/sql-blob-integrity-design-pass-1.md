---
format: aep.planning-md/2
id: review-result:sql-blob-integrity-design-pass-1
kind: review-result
status: active
title: SQL blob integrity independent technical design review, pass one
relations:
- reviews: story:sql-blob-read-integrity
revision: 1
---
needs-revision
story:sql-blob-read-integrity — The selected design requires a backfill UPDATE but states that it supersedes only the predecessor's no-DDL constraint, leaving the predecessor's explicit no-UPDATE rule contradictory; explicitly authorize only integrity-metadata backfill inside the fenced migration while retaining the runtime no-overwrite rule. — docs/design/sql-blob-read-integrity.md:21
story:sql-blob-read-integrity — The SQLite rebuild callback context uses the shadow prefix eventlog_rebuild and its get_blob derives the blob table from that same prefix, so the required rebuild success/corruption path targets a nonexistent shadow blob table; specify separate owner-blob and projection-target coordinates. — crates/eventlog-sqlite/src/lib.rs:1680
story:sql-blob-read-integrity — The PostgreSQL rebuild callback context uses the shadow prefix eventlog_rebuild and its get_blob derives the blob table from that same prefix, so the required rebuild success/corruption path targets a nonexistent shadow blob table; specify separate owner-blob and projection-target coordinates. — crates/eventlog-postgres/src/lib.rs:1181
story:sql-blob-read-integrity — The PostgreSQL migration-only result is undefined when schema commit succeeds but the current mandatory pool shutdown fails afterward, allowing a trusted import to commit without returning trusted_legacy_rows and a retry to report false/0; specify how an acknowledged report survives post-commit cleanup failure. — crates/eventlog-postgres/src/lib.rs:121
What I read: 2 artifacts via `nl -ba` (`story:sql-blob-read-integrity` and its implemented dependency), repository `AGENTS.md`, both binding designs, both ESS model files, all six SQLite/PostgreSQL byte-returning paths, provider constructors and DDL/admission/checksum code, every guard/inline/group/catch-up/rebuild callback owner, pool shutdown handling, existing conformance and production-roster source, using `git rev-parse`, `git status`, `rg`, `wc`, `sed`, and `nl -ba`.
What I could not establish: runtime DDL, locking, migration-failure, or callback behavior because this pass prohibited tests, builds, and fixture use; the brief-supplied model and planning validation results were not rerun.
What I could not establish: RFC 0020's normative predecessor schema because `AGENTS.md` says it is outside this tree; this pass used the checked-in Rust DDL and exact admission code.
```findings
- file: docs/design/sql-blob-read-integrity.md
  line: 21
  category: design
  severity: blocker
  verdict: needs-revision
  origin: introduced
  message: The selected design requires a backfill UPDATE but states that it supersedes only the predecessor's no-DDL constraint, leaving the predecessor's explicit no-UPDATE rule contradictory; explicitly authorize only integrity-metadata backfill inside the fenced migration while retaining the runtime no-overwrite rule.
- file: crates/eventlog-sqlite/src/lib.rs
  line: 1680
  category: design
  severity: blocker
  verdict: needs-revision
  origin: pre-existing
  message: The SQLite rebuild callback context uses the shadow prefix eventlog_rebuild and its get_blob derives the blob table from that same prefix, so the required rebuild success/corruption path targets a nonexistent shadow blob table; specify separate owner-blob and projection-target coordinates.
- file: crates/eventlog-postgres/src/lib.rs
  line: 1181
  category: design
  severity: blocker
  verdict: needs-revision
  origin: pre-existing
  message: The PostgreSQL rebuild callback context uses the shadow prefix eventlog_rebuild and its get_blob derives the blob table from that same prefix, so the required rebuild success/corruption path targets a nonexistent shadow blob table; specify separate owner-blob and projection-target coordinates.
- file: crates/eventlog-postgres/src/lib.rs
  line: 121
  category: design
  severity: blocker
  verdict: needs-revision
  origin: introduced
  message: The PostgreSQL migration-only result is undefined when schema commit succeeds but the current mandatory pool shutdown fails afterward, allowing a trusted import to commit without returning trusted_legacy_rows and a retry to report false/0; specify how an acknowledged report survives post-commit cleanup failure.
```
