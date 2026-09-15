---
format: aep.planning-md/1
id: story:invariants-pinned-by-a-checker
kind: story
status: draft
title: Pin every invariant to a test, checked by the gate
summary: Give this repository the register entity-runtime already has — every invariant naming the test that holds it — with a Rust checker in the gate that fails when a named pin does not exist or does not run.
refs:
- provider: entity-runtime
  reference: docs/requirements.md
revision: 2
---
## Problem

`AGENTS.md` § Invariants opens with "Each is a claim, and each says below what checks it" — and four
of those claims are held by nothing but a reviewer's eye. Invariant 1 (no domain type, no product
concept, no policy), invariant 3 (no crate or module named `common`, `shared`, `utils`, `misc` or
`helpers`), invariant 6 (DDL changes are additive only) and the append-only bullet of § Safety
envelope carry no test and no fence. `grep -rn additive crates` finds a doc comment and a string in
a proof report, not an assertion; each SQL adapter holds seven `DELETE FROM` statements
(`grep -c 'DELETE FROM' crates/eventlog-sqlite/src/lib.rs crates/eventlog-postgres/src/lib.rs`) and
nothing counts them, so an eighth arrives silently. Those rows are now marked `*Review-only.*`,
which states the situation honestly but does not improve it.

## The artifact this reuses

`entity-runtime` has already built the mechanism: `docs/requirements.md` is a register of 95 rows in
which every row names a test, a type or a manifest that pins it, and `design` alone marks a gap and
is a story. `scripts/check-requirements.py` runs in its `task check` and fails when a cited test is
not a live `#[test]` under `crates/` (not merely a `fn`, and not `#[ignore]`d), when an `R-nn` is
referenced by no design, or when a row the register mentions cannot be parsed. Their `AGENTS.md`
invariant 10 is the rule the checker enforces: "Every requirement is pinned, and the pin exists and
runs."

Their own record of why it earns its place is worth reading first: a marker inside an id cell once
made 21 rows invisible to every one of those checks at once.

## Outcome

Every invariant in this repository's `AGENTS.md` names the test or fence that holds it, a checker in
the gate fails when a named test does not exist or does not run, and a row with no enforcement is
declared as such rather than reading like a checked claim.

## Acceptance

`bash scripts/gate.sh` fails when an invariant row cites a test function that is absent, renamed or
`#[ignore]`d, and fails when a row cites nothing and is not explicitly marked review-only; deleting
one cited `#[test]` function makes it fail, and restoring it makes it pass again (invariant 5's
mutation rule applies to the checker itself).

## Constraint — the checker is Rust

Atlas's touch rule makes the executable successor of a checker Rust unless the operator accepts an
exception. `entity-runtime`'s own `AGENTS.md` records its Python checker as legacy under that rule.
Do not port `scripts/check-requirements.py`. Take its *contract* — a register row, a cited pin, a
failure when the pin does not exist or does not run — and write the checker as a Rust binary or test
in this workspace, so it rides the existing `cargo test --workspace --locked` step rather than
adding a Python dependency to a repository that has none.

## First question to answer

Whether the register is a new document (`docs/invariants.md`, the `entity-runtime` shape) or the
`AGENTS.md` invariant list itself made machine-readable. The second keeps one copy of the claims and
avoids the drift a second document invites; the first is what the cited prior art actually does.
Decide before writing the parser.

## Origin

Org-state review 2026-09-15, lane `02-er-eventlog`, findings F13 (overpromise, note) and F15
(missed-opportunity, note), at `77cda08` (tag `0.2.1`).
