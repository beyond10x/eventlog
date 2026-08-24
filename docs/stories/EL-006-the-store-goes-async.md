---
id: EL-006
title: The store goes async
status: in-progress
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

- [x] `EventStore` (and the traits that ride on it: repository, projections, catch-up) expose
  `async fn`; no public sync method remains on the store surface.
- [x] `eventlog-postgres` runs on `tokio-postgres`; the sync `postgres` dependency is gone from the
  workspace.
- [x] `eventlog-sqlite` keeps rusqlite but bridges to blocking *inside* the crate (dedicated thread
  or `spawn_blocking`), invisible to callers; no caller-side wrapping is needed for either backend.
- [x] The conformance suite (34 functions) passes unchanged in *behaviour* against both backends —
  signatures gain `async`/`.await`, assertions do not change.
- [x] Calling any store method from inside a tokio runtime is exercised by a test on both backends —
  the exact shape that panicked in colab — and completes without panicking.
- [x] The physical schema (RFC 0020) is untouched: no migration, existing databases keep working.
- [x] Golden vectors (EL-004) pass unchanged.

## Notes

- Consumers to migrate in the same change-set (monorepo side, separate commits): the
  `module_eventlog` SDK and the five modules (colab, ontology, planner, work, workspaces) drop
  their `spawn_blocking` wrappers and `.await` instead. planner-service/src/cli.rs:248,263 and the
  colab/workspaces fix branch are the known wrap sites.
- `async fn` in traits is native in current Rust; if object safety is needed for `dyn EventStore`,
  use the boxed-future form or `async-trait` — decide in the design doc, not ad hoc.
- **Decision (EL-006): the boxed-future form.** `dyn EventStore` is held by the repository, the
  catch-up runner and every consumer, and a native `async fn` trait is not usable as an object.
  Every method returns `BoxFuture<'a, T>` (`Pin<Box<dyn Future + Send + 'a>>`, defined in
  `eventlog-core`); callers just `.await`. No `async-trait` crate — the type alias plus `Box::pin`
  is the whole mechanism. Riding on that: `Guard::check`, `Projector::apply` and the
  `ProjectionStore` methods return `BoxFuture` too (a projection writes inside the backend's async
  transaction), and `Guard`/`Projector` cross into `append_guarded`/`run_catch_up`/
  `rebuild_projection`/`create_projections` as `Arc<dyn …>` rather than `&dyn …`, because the
  SQLite bridge carries them onto tokio's blocking pool, which requires ownership.
- This story supersedes the caller-side `spawn_blocking` fix pattern; that fix ships first as the
  deploy unblock and is removed here.

## Progress

- 2026-08-24: implemented on `impl/EL-006`. Failing-first proof: a runtime-context test calling
  the postgres store from inside both tokio runtime flavours panics at the merge base
  (`b52fa3b`) with "Cannot start a runtime from within a runtime" out of
  `postgres-0.19.14/src/config.rs:465`, with no database needed; green after the port.
  `eventlog-postgres` now rides `tokio-postgres` behind a `tokio::sync::Mutex<Client>` with the
  connection task spawned in `connect`. `eventlog-sqlite` wraps every call in
  `tokio::task::spawn_blocking` over an `Arc<Inner>`; guard/projector futures are driven on the
  blocking thread by a minimal park/unpark executor, and the two transactions that hand a
  `ProjectionStore` out (`append_guarded`, `run_catch_up`) manage BEGIN IMMEDIATE/COMMIT/ROLLBACK
  by hand because a `Send` projection view needs `&mut Connection`, which rusqlite's borrowing
  `Transaction` cannot give. Conformance assertions untouched; the two closure guards became the
  named `HoldsTallyRow`/`TallyLimit` guards because a sync closure cannot await. The postgres test
  binaries serialise their tests behind one async mutex: the watermark is instance-wide, so a test
  holding a transaction open races any test asserting on a feed — that race predates this story
  and became visible with async interleaving. Gate green twice against PostgreSQL 17 (37 tests,
  fmt, clippy `-D warnings`, brand). Next: the monorepo consumers (`module_eventlog` SDK + five
  modules) migrate against the new surface; every signature they touch is listed in the hand-off
  report.
