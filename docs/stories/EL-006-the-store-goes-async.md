---
id: EL-006
title: The store goes async
status: ready
depends_on: [EL-001]
---

# EL-006 — The store goes async

## Intent

Every consumer of this kit is an async server, yet `EventStore` is a sync trait
(`crates/eventlog-core/src/lib.rs:430`) and the postgres backend blocks on the sync `postgres`
crate (`crates/eventlog-postgres/src/lib.rs:25`). The sync/async bridge is therefore the
*caller's* job — each module wraps every store call in `spawn_blocking`, and forgetting the wrap
panics with "Cannot start a runtime from within a runtime" at startup (colab shipped this bug
twice, 2026-08-23/24). Move the bridge into the kit: an async trait, a natively async postgres
backend, and a blocking bridge internalised once for sqlite. Callers just `.await`; the bug class
is deleted along with all caller-side `spawn_blocking` scaffolding.

## Acceptance

- `EventStore` (and the traits that ride on it: repository, projections, catch-up) expose
  `async fn`; no public sync method remains on the store surface.
- `eventlog-postgres` runs on `tokio-postgres`; the sync `postgres` dependency is gone from the
  workspace.
- `eventlog-sqlite` keeps rusqlite but bridges to blocking *inside* the crate (dedicated thread or
  `spawn_blocking`), invisible to callers; no caller-side wrapping is needed for either backend.
- The conformance suite (34 functions) passes unchanged in *behaviour* against both backends —
  signatures gain `async`/`.await`, assertions do not change.
- Calling any store method from inside a tokio runtime is exercised by a test on both backends —
  the exact shape that panicked in colab — and completes without panicking.
- The physical schema (RFC 0020) is untouched: no migration, existing databases keep working.
- Golden vectors (EL-004) pass unchanged.

## Notes

- Consumers to migrate in the same change-set (monorepo side, separate commits): the
  `module_eventlog` SDK and the five modules (colab, ontology, planner, work, workspaces) drop
  their `spawn_blocking` wrappers and `.await` instead. planner-service/src/cli.rs:248,263 and the
  colab/workspaces fix branch are the known wrap sites.
- `async fn` in traits is native in current Rust; if object safety is needed for `dyn EventStore`,
  use the boxed-future form or `async-trait` — decide in the design doc, not ad hoc.
- This story supersedes the caller-side `spawn_blocking` fix pattern; that fix ships first as the
  deploy unblock and is removed here.
