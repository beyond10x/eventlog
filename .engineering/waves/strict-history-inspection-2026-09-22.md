# Strict history inspection dependency unit

Skill: aep-drive:wave 0.9.3. The operator approved project completion through
preserving migration and instructed implementation. This bounded provider change
is required because ordinary opening can modify a source being inspected.
It serves O2 and O6. Source integration is authorized; no Eventlog release,
deployment, live-store opening or unrelated evolution integration is included.

## Selection and ownership

N=1: story:strict-read-only-history-inspection. The complete verb output is
[strict-history-inspection-selection.json](strict-history-inspection-selection.json):
the wave, collision list and unassessed list are retained verbatim there.
The [scope report](strict-history-inspection-scope.md) distinguishes cited and
inferred paths. AEP validated before opening. The coordinator owns every planning,
design and ESS edit; the worker owns only scoped Rust source and tests.

Base is published main 2e482aa1bbfcad7dc116a3eb1f442bb3512ebe8b, fetched again
before checkout creation. The bot API returned no open pull requests. Other local
evolution checkouts remain owned elsewhere and are untouched. Their broader capture
contract cannot satisfy this unit's strict SQLite boundary. Before integration,
re-fetch main and resolve actual published changes; do not overwrite or retire
another owner's branch. Proposed merge conflicts are not permission to merge the
unrelated evolution program.

Managed tree/id: `<worktrees>/eventlog/ekr-eventlog-inspection-20260922` /
`ekr-eventlog-inspection-20260922`.
Branch: `codex/ekr-eventlog-inspection-20260922`.
Build: `<cache>/b10x-target/ekr-eventlog-inspection-20260922`.
Scratch: `<cache>/ekr-completion-20260922/eventlog-inspection`.
Coordinator lease: `codex-ekr-eventlog-inspection-20260922`; released for worker
handoff, worker/reviewer acquire and release their own stable leases.

This additive dependency unit uses one managed checkout sequentially: coordinator
opening, implementation, independent review, then coordinator integration and gate.
Only the coordinator writes AEP, so no planning journal merge is needed. The host
has no plugin subagent_type selector; workers load aep-drive:implementor and the
independent reviewer loads aep-drive:adversary. The inspected scope came from
aep-drive:story-scoper. These dispatch mechanics are explicit deviations from the
host-specific skill examples.

During implementation the coordinator also owns bounded design/AEP corrections
in this checkout under its own lease, while the worker edits only Rust source and
tests. This is a disjoint-file exception to the sequential handoff above; neither
actor writes the other's files. The first lock-contract body update was written
after handoff before reacquiring the coordinator lease; the lease was immediately
restored. Subsequent coordinator writes use that lease.

Authorized commits are the opening, reviewed implementation and corrections,
closing evidence, and integration into main after required checks. No tag or
version bump is needed for a verified immutable Git dependency. The consumer
updates every Eventlog crate selector and its lockfile together after verification.

## Contract and preflight

[Strict inspection design](../../docs/design/strict-history-inspection.md) fixes
the transient API, limits, refusals and provider boundaries before source work.
`ess specify validate --path ess/inspection` succeeded on the new value-only
contract. Behavioral agreement is explicitly unexecuted. Retain the existing
complete RecordedEvent; the ESS coordinate projection is not a replacement wire
envelope. Initial SQLite support may refuse unsafe WAL/recovery states, but must
demonstrate a useful successful published-format fixture without changing it.

Use two compiler jobs and the dedicated target. Measure free space before each
large build; pause below the completion plan's 10 GB floor. No agent cost counters
were exposed and none are estimated. Exact preflight observations follow below.

Full historical planning-journal text scanning reports existing personal-path
findings on both the untouched base and current journal. The newly appended
records, new story and scope report independently pass scan-text. Do not rewrite
the append-only historical journal; ordinary scoped publication still must pass
the repository's enrolled baseline and signed common checks.

## Verification and stages

The implementation starts from the exact opening commit and establishes negative
or mutation evidence before claiming each new regression. Run shared semantics
through real File and SQLite, including source bytes/entries on success, refusal
and drop. Add exact required case names without deleting old roster entries.
Independent review may write tests only. Integration runs every gate step with
its own exit status, plus the existing mandatory production, comparative and
restart proof. Required CI and published main provenance must be verified before
consumer adoption. Stage at opening: source implementation unstarted.

Implementation is active. A measured READ_ONLY/readonly_shm primitive left a WAL
file on an opening failure; ordinary opening is therefore not reused. The design
now selects a Linux OFD shared source lock before admission through connection
drop, with a bounded target-specific nix dependency. Native same-process and
separate-process writer interleavings must prove the boundary. Default WAL-mode
inspection may refuse explicitly; the frozen rollback-mode fixture must prove
usable success. No source repair or checkpoint is permitted in inspection.

The next primitive probe found that closing an independent database descriptor
after a refused OFD admission released a pre-existing same-process writer's
POSIX lock. A separate process then acquired a write transaction. The correction
uses the design's bounded process-lifetime descriptor registry, with capacity
reserved before opening and explicit OFD release without descriptor close.
The observed red is retained in the unit scratch as
`ofd-existing-writer-red.log`; writer-before-inspection, bounded exhaustion and
concurrent inspector cases are required before this claim is accepted.

Free space at dispatch preparation:   25G

Toolchain: rustc 1.98.1 (48a229cea 2026-09-01)
