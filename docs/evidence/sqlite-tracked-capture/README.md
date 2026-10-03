# SQLite capture continuity verification

`production-proof.json` is the unchanged output of the required PostgreSQL 17.6/TLS
production-proof runner after the projection-alias correction and independent review tests.
It records 545 passed, zero failed/skipped, and no missing required cases. The ordinary
workspace gate and the production-proof gate both exited 0, including formatter and strict
Clippy on Rust 1.99.0. This is conformance evidence, not deployment capacity admission.

The runner truthfully records the original base revision and `source_dirty: true`: the tested
implementation had not yet been committed. `source.sha256` binds the actual crate sources and
Cargo inputs used by this run. Verify it with `sha256sum --check` from the repository root.
The later commit adds this record and the planning evidence; it does not alter those inputs.
Raw runner logs remain in the managed worktree's private recovery archive.

The governed story and two immutable review results are in `.engineering/planning/` under
`sqlite-tracked-capture`. They retain the actual projection-alias failure and its fixed recheck.
