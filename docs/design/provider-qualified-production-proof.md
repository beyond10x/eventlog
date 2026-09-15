# Required production cases belong to their provider and target

Status: accepted for implementation under ESS evolution revision 1, 2026-09-15.
Owner: story:provider-qualified-production-proof. Serves O2, decisions with checkable evidence.

## Existing defect and contract

The production runner currently searches one combined stdout for successful test names
(`crates/eventlog-postgres/examples/production-proof.rs`, required roster and missing-case check).
SQLite and PostgreSQL both define `ordered_groups_commit_and_rollback_as_one_unit` in their
`tests/atomic_groups.rs`; both also define the rebuild conformance case. One selected provider can
therefore satisfy a missing peer. AGENTS.md already requires every applicable provider contract
and refuses skipped or selected-zero lanes. This change enforces that existing meaning.

## Decision

A required case is the exact tuple of Cargo package, test target kind/name and fully qualified
test name. The runner must obtain the package/target from the command it actually executes or
from Cargo's structured artifact output, and bind success to that execution's actual result.
A free-form test-output line, matching filename substring, global successful name or global
positive count cannot establish that tuple. Independent stdout/stderr capture cannot be zipped
together after execution to manufacture target association.

Direct artifact execution preserves the owning package's Cargo working directory and runtime
package environment. Exact Cargo metadata supplies the manifest paths, package identity, version
components and optional metadata. Clear inherited manifest and package variables before applying
that context, including empty values for absent optional metadata. A peer package must never run
under the production example's package context. The two independent review regressions pin both
the working directory and runtime environment; preserve their assertions.

Retain every existing required case, assign it to each applicable provider/target in current
source, and cover the supported SQLite storage modes where their established case owns both.
Do not silently narrow the required package, target or test selection. Preserve the workspace
test coverage, documentation tests, fixture validation, hosted role/TLS checks, exact exit status,
failure/ignored/skipped refusals and the separation from comparative capacity admission.

Only an exact required case that ran and passed in its selected target satisfies the roster.
Missing targets, zero-selected required lanes, nonzero process exits, ignored cases, explicit
skips, truncated/inconsistent runner results and duplicate or ambiguous attribution refuse proof.
An unrelated target containing the same successful name never fills a gap.

Retain eventlog-production-proof/1's envelope, field types and stated meaning. Its conformance
claim already requires every backend; this is a verifier defect correction, not a new admission
contract. A missing_required_cases diagnostic may identify the complete failed tuple. Preserve
raw runner evidence. If implementation requires a persisted envelope/meaning change, propose its
exact new version and relying-party migration before making that change.

## Required regression and sensitivity

Before replacing the current matcher, demonstrate its false acceptance with a required SQLite
and PostgreSQL pair where only one provider's target passes the shared name. Correct admission
must report the absent tuple. Also cover same-name wrong-target success, selected zero, ignored,
failed, skipped and malformed/truncated cases, plus a complete successful roster.

After the correction, remove package/target qualification and observe the regression turn red.
Retain the mutation and restored-run evidence. The Rust regression suite must itself be selected
by the repository's real gate. Run the complete production gate against the disposable normal and
hosted PostgreSQL fixtures before integration; a synthetic parser test alone is insufficient.

## Boundaries

All implementation is Rust. Do not change storage semantics, database DDL, eventlog-file/1 bytes,
test expectations or fixture custody. Wider crash, tamper and canonical-byte qualification stays
in separate owner work. No release, publication, deployment or remote mutation is authorized here.
