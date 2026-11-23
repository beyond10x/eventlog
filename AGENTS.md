# AGENTS.md — eventlog

The contract for changing **this** repository. Organization naming, the Rust language rule and
coordinated migrations are recorded in `atlas/AGENTS.md`. Public Gates owns common security/privacy
checks and bot delivery under ADR 0048. ADR 0001's old brand-exemption categories were superseded;
they do not authorize new public associations.

`README.md` orients a reader and shows how to run the backends. This file says what must not break.

## Serves

The objectives of the collection this repository moves, by id from `atlas/ROADMAP.md` — the only
cross-repository roadmap, and the page that says what each id means and which evidence closes it:

- **O2 — decisions as data, with evidence.** State as a fold over an append-only log: a decision is an event, and the projection is derived, never edited.
- **O6 — self-improvement, built into all of it.** The record every other objective is measured from has to be append-only to be believed.

A change here that moves none of these is a question for the operator, not a task.
`atlas/scripts/check-map.sh` fails a repository whose `AGENTS.md` names no objective.

## What this repository owns

The shared persistence kit: an append-only log, folds, snapshots and projections. SQLite,
PostgreSQL and the local JSONL file provider are implemented. The file provider's recovery,
privacy and operating boundaries are documented in `docs/design/file-provider.md`.
Every b10x owner may build-depend on these crates. That is the constraint every invariant below
exists to protect.

## Invariants

Each is a claim that can be checked. Breaking one is a design change, not a refactor.

1. **No domain type, no product concept, no policy lives in these crates.** Every owner may
   build-depend on the kit, which is only safe while that stays true. A type that names a product
   concept has already broken it.
2. **Every provider implements the same applicable conformance contracts.** In-memory remains
   SQLite `:memory:`. ADR `ess-evolution-05-file-and-atomic-groups` explicitly supersedes the
   former two-backend/no-third rule and admits a durable JSONL file provider. Product-specific
   storage forks remain prohibited. Atomic append groups commit together; never emulate them
   with independently committed single appends.
3. **No crate or module named `common`, `shared`, `utils`, `misc` or `helpers`.**
4. **The conformance exercise is the definition of correct behaviour.** A backend change that needs
   an exercise change is a design change — say so in the commit rather than editing the assertion.
5. **A new test fails without the fix.** The watermark test earns its place by failing when
   `WATERMARK` is replaced with `true`; every regression test here is held to that. Apply the
   one-line mutation, watch it fail, revert.
6. **DDL changes are additive only.** A kit release that changes a column is a migration in every
   owner at once, so a kit major version never forces one.
7. **`redact` is the only `UPDATE` this kit issues against an events table**, and it deletes the
   snapshots at or after the redacted version in the same transaction. A second write path against
   an events table is a second place to forget that.
8. **No payload bytes and no free personal text enter the log.** Blobs are content-addressed
   elsewhere and referenced by digest; identities are opaque ids resolved through the identity
   directory (`crates/eventlog-core/src/lib.rs:769`). This is what lets a person be forgotten in the
   directory while the log stays append-only.

## Safety envelope

- **The log is append-only and holds other people's durable state.** Erasure and redaction are the
  only paths that remove anything, and invariant 7 bounds them. Never add a delete, a compaction or
  a rewrite; a projection is what gets dropped and rebuilt.
- **Personal data must be unable to arrive**, not merely discouraged — the envelope refuses an
  address or display name where an opaque id belongs. Never relax that refusal to make a caller's
  migration easier.
- **Test credentials never enter the tree.** The PostgreSQL exercise reads
  `EVENTLOG_TEST_POSTGRES_URL` from the environment; no connection string, password or database file
  is committed.

## The watermark couples feed latency across owners

Measured on PostgreSQL 17.6 while implementing EL-003, and worth knowing before diagnosing a slow
feed: a feed reader stops at `pg_snapshot_xmin(pg_current_snapshot())`, and that snapshot is
cluster-wide. A long-running transaction belonging to **any** owner in the same PostgreSQL instance
holds every catch-up projection in that instance still, including ones in unrelated schemas. Nothing
is skipped and inline projections are unaffected — but a shared instance is not isolation for feed
latency. The conformance exercise drains with a bounded retry for exactly this reason.

## Out of scope

| Belongs elsewhere | Where |
|---|---|
| Domain events, aggregates and projections for a product | the owner that has the domain |
| Identity resolution behind an opaque id | `identity` |
| Blob storage | content-addressed storage, referenced here by digest |
| Product-specific storage forks | nowhere — see invariant 2 |

## The gate

```console
bash scripts/gate.sh
```

The compatibility shell entry point delegates argument parsing and selection to the Rust `gate`
example. In order: `cargo nextest run --workspace --locked`, `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets --locked -- -D warnings`, the same clippy for the slow
lane's `batch_timing` target (a `test = false` target is outside `--all-targets`), and for
`eventlog-kpi --features counters`, which compiles the providers' KPI counters.
Green here is the bar for `main`. `--kpi` runs the KPI lane alone (§ The KPI gate). `--slow` runs
the slow lane alone: the wall-clock budgets written for a release build (`eventlog-postgres`
`batch_timing`, declared `test = false` so no debug step selects it), in a release build, times
printed. The KPI counters CI job runs it.

The test step needs `cargo-nextest` 0.9.143 or later. `.config/nextest.toml` ends any test that
runs 60 s, and the gate then prints the run's ten slowest tests from nextest's JUnit report
(`target/nextest/default/junit.xml`). The folder's budget for the whole step is 120 s; the step
does not fit it yet on this host (story `test-step-fits-120-s`), so the run-wide limit is not set.
Test builds carry no debug information and are optimized (`.cargo/config.toml`: `opt-level = 1`
for the workspace, `2` for dependencies): unoptimized hashing made the two in-memory
`remembered_content` tests run 12 to 18 s, optimized they run 1 to 2 s. Most of the step's time
is file and database I/O, which the build profile does not change.

Nextest runs every test in a process of its own, so a `static` lock in a test binary orders
nothing there. The PostgreSQL `conformance` binary takes its `INSTANCE` lock for writing in a case
that reads the feed, catches up, rebuilds or uses the shared application role, and for reading
in every other case; `.config/nextest.toml` runs exactly the writing cases, and every
`runtime_context` case, after every other test and each with no test beside it (the watermark
couples feed latency across the instance, across databases too; see above). The reading cases
run beside the rest. `tests/production_admission.rs` fails when the configuration and the source
disagree. A new case that needs the instance to itself takes the lock for writing.

The required shared Gates check covers common security/privacy rules. Install coordinated local
hooks with `b10x-gates --repository beyond10x/eventlog install`; they inspect the index, messages
and every outgoing commit, including intermediate commits and annotated tags. Private policy and
signing keys stay outside public source. The explicit adoption baseline records historical
findings separately and does not authorize new occurrences.

Common-check receipts never replace the persistence proof below. Rust caching and cancellation of
superseded PR proof runs are independent cost improvements; expensive repository-specific proof
reuse is deferred.

Set `EVENTLOG_TEST_POSTGRES_URL` to a disposable fixture for the full gate. Some PostgreSQL
tests refuse a missing URL while others report that they did not run. A skipped backend has not
been proved. The required `bash scripts/gate.sh --production-proof` mode also requires the
hosted-role URL and test CA, rejects every selected-zero/ignored/skipped lane, and records exact
runner output and counts. The Persistence proof CI job runs this mode against PostgreSQL 17.6
with verified TLS. Comparative capacity and restart receipts remain separate evidence; neither
a local nor CI conformance run approves a deployment budget.

**A pull request of planning or prose only runs no build, test, KPI or proof.**
`scripts/ci-paths.sh` is the one path list: `.engineering/` is planning, `website/` is the site,
Markdown no code reads is prose, everything else is code. The first job of the Persistence
proof, KPI and Documentation validation workflows classifies the pull request's merge commit, and
the work jobs are skipped when it holds no code (the site build runs for a `website/` change); a
skipped job reports success to its required check. Never add `paths-ignore` to a workflow that
carries a required check: the check then stays pending. `scripts/ci-paths-test.sh`, run by the
`planning validate` job, holds both sides of the list and fails when code names a Markdown file
the list calls prose; add such a file to the code lines of the list.

**A green local gate does not guarantee a green CI.** The steps mirror each other; the toolchain
does not — CI installs whatever `stable` is that day, and a newer clippy can fail a commit that
passed locally. Run `rustup update` before pushing, and read the gate's own exit status, never a
pipeline's (`gate.sh 2>&1 | tail` reports `tail`'s status, not the gate's).

Gates enforces forbidden identifiers through a private policy overlay. Exceptions require an exact
rule, bounded location and content digest in trusted policy. Candidate ignore files and inline
suppression comments cannot disable organization rules.

## The KPI gate

What the kit costs is part of what must not break. `kpi/kpis.json` defines every KPI (id, what it
measures, unit, direction, fixture, tolerance) and `kpi/baseline.json` holds the recorded numbers.
The runner is `crates/eventlog-kpi` (`publish = false`).

- **Counters** are deterministic work counts on small fixtures: bytes read and written
  (`/proc/self/io`), SQLite VM steps, fold calls, feed map visits, blob-directory entries, Tree
  group files and digests, snapshot-generation rows, allocation bytes, PostgreSQL round trips
  (a counting proxy), write transactions (transaction ids) and buffers (`EXPLAIN ANALYZE`).
  `bash scripts/gate.sh --kpi` compares each with the baseline and fails on a regression beyond
  its tolerance. This is the gate.
- **Timings** (latency p50/p95, batch throughput) are never compared with a number from another
  machine. `eventlog-kpi ab --base <rev> --head <rev>` builds both revisions with the current
  runner, runs them interleaved for several rounds on the same machine and fails when a median
  is worse than `ab.max_ratio`. The baseline's timings are the recording machine's, for reading.
- Counters inside a provider (`eventlog-file`, `eventlog-sqlite`, `eventlog-tree`, module `kpi`)
  compile only with their non-default `kpi-counters` feature, enabled by the runner's `counters`
  feature; no product build carries them. A change that rewrites a counted path keeps its
  counter call in the rewritten code. The PostgreSQL `EXPLAIN` KPIs run a copy of the library's
  feed and capture-preflight statements (`crates/eventlog-kpi/src/counter.rs`); a change to
  either statement changes the copy in the same commit.
- Allocation counts use `stats_alloc` (the workspace forbids the `unsafe` a counting allocator
  needs), linked only with `counters`.
- PostgreSQL KPIs run only with `EVENTLOG_TEST_POSTGRES_URL` set and say so when skipped.
- An improvement beyond tolerance is reported, not ratcheted. Re-record in the same change, on a
  quiet machine with a disposable PostgreSQL:
  `cargo run --release --locked -p eventlog-kpi --features counters -- record`. Never edit the
  baseline by hand and never re-record to absorb a regression.
- CI: `.github/workflows/kpi.yml` has two jobs. `KPI / KPI counters` runs the counters with a
  PostgreSQL service on every pull request and push to `main`; it is the one meant to become a
  required check. `KPI / KPI timing` runs the A/B against the base on a pull request; it is
  informational (`continue-on-error`) and never a required check. In both, a change of only
  `.engineering/` or Markdown skips the measuring step and the job passes
  (`eventlog-kpi ci-scope`).

## Releases

Cut `CHANGELOG.md` under a version heading at a fully gated `main` commit, then write an annotated
tag whose name is the bare version — `0.1.0`, the version and nothing else (atlas § *Naming*). The
`eventlog-v` prefix was the monorepo's namespace and retired with it.

Direct commits, annotated tags and pushes use `b10x-gates bot`; signed common evidence is checked
and published with `b10x-gates check`, `verify` and `publish`. GitHub delivery uses the same
`b10x-bot[bot]` identity. Commit and publish paths require no Atlas checkout, current Atlas main or
organization-wide admission run. Require the shared check and this repository's correctness checks
before integration. Verify the resulting release author is the bot.

The shared workflow verifies signatures, exact source/range, current public/private policies,
scanner versions and complete common results before reusing evidence. Missing or stale receipts
run common scanners. PostgreSQL production, comparative and restart requirements remain mandatory.
Atlas validates documentation manifests and coordinates Website publication separately; ordinary
source publication follows the release completion boundary below.

## Where work is tracked

| What | Where |
|---|---|
| Current stories, tasks, review records and evidence | `.engineering/planning/`, managed only through `aep plan artifact` |
| Historical foundation stories, with backlinks to migrated artifacts | `docs/stories/`, indexed by `docs/stories/README.md` |
| What shipped | `CHANGELOG.md` |
| The decision this kit exists under | `architecture/adr/0055-durable-domain-state-is-a-fold-over-an-event-log.md` — predecessor-monorepo path, not in this tree |
| The normative design, including the physical schema | `architecture/rfcs/0020-state-is-a-fold-over-an-event-log.md` — same |

Read the RFC before changing storage behaviour: its physical schema is normative and this repository
does not contain it.

## Public source

This repository is public. Gates provides bot-authenticated delivery operations; organization
credentials, signing keys and private policy remain in protected configuration outside source.

<!-- b10x-docs-operations:start -->
## Public documentation operations

This repository owns the public source and presentation allowlist in `b10x.docs.yaml`. The generated credential-free `.github/workflows/b10x-docs-bundle.yml` passively packages only those declared files for the exact successful `main` commit; it must never run repository code. The generated `.github/workflows/b10x-docs-check.yml` runs the publisher's per-source checks on every pull request and main push, with read-only contents and no credentials; it is deliberately separate from the shared gate, which runs on `pull_request_target` with a secret and never reads candidate source. Atlas selects the latest successful bundle with every other catalog source, and Website plus Docs System own rendering, shared components, search, and feeds. Do not add a standalone docs deployer or put App credentials in this public repository. If Atlas catalogs a former Pages workflow, that file remains repository-owned validation: preserve its bespoke checks while keeping exact read-only permissions, an unconditional pull-request trigger, and no deployment primitives. Project Pages at `/eventlog/` is only the generated stable redirect façade in `.github/workflows/b10x-docs-pages.yml`; content-only publication never rebuilds it.

From the complete organization workspace, verify the contract with a clean Atlas checkout at the current remote `main`. Set `B10X_ATLAS_CHECKOUT` to a managed Atlas worktree when the primary checkout is dirty or stale; never infer command availability from the primary alone.

```bash
atlas_checkout="${B10X_ATLAS_CHECKOUT:-atlas}"
atlas_head="$(git -C "$atlas_checkout" rev-parse HEAD)"
atlas_main="$(git -C "$atlas_checkout" ls-remote origin refs/heads/main | awk '{print $1}')"
test -z "$(git -C "$atlas_checkout" status --porcelain)"
test "$atlas_head" = "$atlas_main"
cargo run --manifest-path "$atlas_checkout/Cargo.toml" --locked -q -- \
  --store "$atlas_checkout/catalog/store" docs reconcile --workspace . --check
```

Keep internal plans, stories, ADRs, decisions, worklogs, security material, and research out of the public allowlist unless a repository authority explicitly declares them public.
<!-- b10x-docs-operations:end -->

## PostgreSQL admission and callback boundaries

Hosted traffic uses verified TLS, explicit migration/application roles and a declared replica plus
reserve connection budget. The supported application role is dedicated, has no memberships or
DDL ownership/creation powers, and has a finite server connection limit within its pool share.
It reads and never writes the migration metadata (`schema_version`, `projection_registry`,
`settlement_floor`): erasure deletes a tenant from every registered projection, so a role that
could edit the roster could make erasure skip a table, and a role that could raise the floor could
let a feed cursor pass an append still in flight. Schema and projection admission compares actual physical shape and the persisted migration roster.
Never broaden that profile silently or treat a checksum alone as schema compatibility.

Registration freezes before traffic. A cancelled/unsettled connection stays quarantined until its
driver stops; an unknown commit is resolved through the original durable command identity.
Admission counters are generic storage coordinates with an ESS home in `ess/admission/`; no domain
policy or lifecycle membership belongs there. Tenant/deployment coordinates are structurally
distinct and encoded without boundary collisions. Trusted guard reservation access never gives a
domain projector cross-tenant query or write authority. Caught reservation errors and cancellation
must not commit partial counter changes. Rebuild and catch-up share tenant/projector locks and
commit the view and cursor atomically.

The committed-XID predicate alone is not a position-contiguity proof: a guard can assign an older
XID before another append obtains a lower sequence position. The PostgreSQL publication gate
therefore precedes every other owner lock. Append and redaction share it through commit; tenant
erasure takes it exclusively to prevent snapshot-generation capture from surviving erasure.
After it an append takes its claim or group lock, then its stream locks, then every attached inline
projector's owner/projector/tenant lock ordered by name, all before the guard; admission and
projection-row locks come later. The projector locks hold to commit so one projection is never
folded concurrently, across handles too. Keep that order and the two-writer deadlock regression.
Feed, catch-up and rebuild take it exclusively before a fresh READ COMMITTED query. Preserve the original watermark
predicate and stop before its first withheld global position in that same statement snapshot:
unrelated transactions can hold xmin between already committed owner XIDs. A rowwise filter can
otherwise leave a hole even while no owner writer is active. Keep both reversed-XID/position
regressions, including the unrelated-xmin case, plus the original late-commit
mutation case.

A logical restore copies each event's `committed_xid` from the source cluster's clock, and the
watermark would withhold the restored history and every later event of the owner until this
cluster's clock passed it. Timo, 2026-11-23 03:56 CET: L61 = A (settlement floor). Positions at or
below the highest `<prefix>_settlement_floor.settled_through` count as settled; the watermark and
the first withheld position apply above it, in the same statement snapshot. Only `migrate` writes
the floor: after its schema transaction commits it looks for a visible event above the floor whose
`committed_xid` is at or past this snapshot's xmax (no native row can be) and, if one exists,
records `MAX(global_seq)` under the exclusive publication gate. Hosted `open` refuses an owner with
such a row, and the floor is migration metadata the application role may only read. The restore
procedure is "restore, then run migrate". Never rewrite `committed_xid` to settle a restore: the
only UPDATE on events is redaction. Sequence CACHE 1 and deterministic column collations are admission requirements.
Mixed protocol generations require a fenced cutover; retaining physical old-reader formats does
not authorize concurrent old writer/feed binaries.

Snapshot writes require a generation observed before reading the cache or folding events.
Both backends reject delayed writes after redaction or erasure, ignore caches without matching
persisted provenance, and refuse the legacy unproven save API. Repository automatic caching is
best effort after a committed append; explicit snapshot creation reports storage errors and
retries a stale generation once. Keep the real-backend interleaving and cache-failure regressions.

<!-- b10x-release-operations:start -->
## Release completion

An ordinary release completes after this repository's exact tag, required source checks,
published release and required artifacts are verified. A pushed tag with unfinished checks or
uploads is queued; report it as released only after those requirements succeed.

Atlas reconciliation and public documentation publication run asynchronously. Do not wait for
Atlas or Website, update Website source locks or bootstrap snapshots, promote consumer pins,
release shared docs tooling, or redeploy documentation façades as part of an ordinary source
release. Report documentation as pending unless its publication was actually verified. A background
documentation failure does not invalidate a successful source release.

Keep this repository's provenance, correctness, security, compatibility and artifact verification
requirements. Shared rendering, routing or delivery-control changes still require their relevant
integration gates. A release request does not authorize deployment or downstream releases.
Repositories without a release unit retain their existing publication policy. This completion
boundary supersedes older instructions that attach synchronous documentation ceremony to each
source release.
<!-- b10x-release-operations:end -->
