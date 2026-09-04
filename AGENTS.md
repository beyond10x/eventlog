# AGENTS.md — eventlog

The contract for changing **this** repository. Org-wide rules — the naming convention, the language rule (anything that runs is Rust, not Python), the
former-brand rule (atlas ADR 0001) and its four exemption categories, and the rule that renaming
anything another repo verifies is a coordinated migration with an ADR — live in `atlas/AGENTS.md`
and are not restated here.

`README.md` orients a reader and shows how to run the backends. This file says what must not break.

## Serves

The objectives of the collection this repository moves, by id from `atlas/ROADMAP.md` — the only
cross-repository roadmap, and the page that says what each id means and which evidence closes it:

- **O2 — decisions as data, with evidence.** State as a fold over an append-only log: a decision is an event, and the projection is derived, never edited.
- **O6 — self-improvement, built into all of it.** The record every other objective is measured from has to be append-only to be believed.

A change here that moves none of these is a question for the operator, not a task.
`atlas/scripts/check-map.sh` fails a repository whose `AGENTS.md` names no objective.

## What this repository owns

The shared persistence kit: an append-only log, folds, snapshots, projections, on two backends.
Every b10x owner may build-depend on these crates. That is the constraint every invariant below
exists to protect.

## Invariants

Each is a claim that can be checked. Breaking one is a design change, not a refactor.

1. **No domain type, no product concept, no policy lives in these crates.** Every owner may
   build-depend on the kit, which is only safe while that stays true. A type that names a product
   concept has already broken it.
2. **There are two backends and no third.** In-memory *is* SQLite `:memory:`, which is why a
   property proved in a test is proved for the deployment. A hand-written third backend is the
   defect class recorded in `M-022-one-replay-rule-behind-one-port.md` (predecessor-monorepo path,
   not in this tree).
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
| A third backend of any kind | nowhere — see invariant 2 |

## The gate

```console
bash scripts/gate.sh
```

In order: `cargo test --workspace --locked`, `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets --locked -- -D warnings`.
Green here is the bar for `main`.

The PostgreSQL exercise runs only when `EVENTLOG_TEST_POSTGRES_URL` is set, and **reports itself as
not run rather than passing quietly** when it is not. A gate that skipped a backend has not proved
that backend.

**A green local gate does not guarantee a green CI.** The steps mirror each other; the toolchain
does not — CI installs whatever `stable` is that day, and a newer clippy can fail a commit that
passed locally. Run `rustup update` before pushing, and read the gate's own exit status, never a
pipeline's (`gate.sh 2>&1 | tail` reports `tail`'s status, not the gate's).

The former brand is fenced org-wide by `scripts/check-org-brand.sh` in the **atlas** repo, not here. There is no per-repo fence
itself. Do not add an exemption without the category in the atlas ADR that admits it.

## Releases

Cut `CHANGELOG.md` under a version heading at a fully gated `main` commit, then write an annotated
tag whose name is the bare version — `0.1.0`, the version and nothing else (atlas § *Naming*). The
`eventlog-v` prefix was the monorepo's namespace and retired with it.

## Where work is tracked

| What | Where |
|---|---|
| Stories, with `id`/`title`/`status`/`depends_on` frontmatter | `docs/stories/`, indexed by `docs/stories/README.md` |
| What shipped | `CHANGELOG.md` |
| The decision this kit exists under | `architecture/adr/0055-durable-domain-state-is-a-fold-over-an-event-log.md` — predecessor-monorepo path, not in this tree |
| The normative design, including the physical schema | `architecture/rfcs/0020-state-is-a-fold-over-an-event-log.md` — same |

Read the RFC before changing storage behaviour: its physical schema is normative and this repository
does not contain it.

## Public source

This repository is public. Organization delivery credentials and bot-authenticated remote
operations are provided by Atlas-owned tooling outside component source.

<!-- b10x-docs-operations:start -->
## Public documentation operations

This repository owns the public source and presentation allowlist in `b10x.docs.yaml`. The generated credential-free `.github/workflows/b10x-docs-bundle.yml` passively packages only those declared files for the exact successful `main` commit; it must never run repository code. Atlas selects the latest successful bundle with every other catalog source, and Website plus Docs System own rendering, shared components, search, and feeds. Do not add a standalone docs deployer or put App credentials in this public repository. If Atlas catalogs a former Pages workflow, that file remains repository-owned validation: preserve its bespoke checks while keeping exact read-only permissions, an unconditional pull-request trigger, and no deployment primitives. Project Pages at `/eventlog/` is only the generated stable redirect façade in `.github/workflows/b10x-docs-pages.yml`; content-only publication never rebuilds it.

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
