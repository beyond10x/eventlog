# Atomic blob and event publication dependency unit

N=1, story:atomic-blob-append, authorized by the operator's approved project
completion and preservation requirements. The complete scheduling result is
atomic-content-selection.json. The inspected scope is atomic-content-scope.md;
docs/design/atomic-blob-append.md and validated ess/atomic-content fix the contract.
This is one bounded capability, so the multi-story decomposition panel is not
applicable. Independent implementation review is required.

Implement on the reviewed strict-inspection source, after its local production
gate. That source's remote checks may run concurrently, but overlapping source
implementation is sequential. Keep strict-inspection cases and behavior intact.
The atomic unit uses its own managed checkout and target; it does not write the
inspection tree. Its source-only commit merges back into the coordinator tree,
whose single writer owns all AEP, design, specification and publication edits.
This avoids concurrent planning-journal writers.

Managed id: ekr-eventlog-atomic-content-20260922.
Branch: codex/ekr-eventlog-atomic-content-20260922.
Target: <cache>/b10x-target/ekr-eventlog-atomic-content-20260922.
Scratch: <cache>/ekr-completion-20260922/eventlog-atomic-content.
The worktree CLI supplies the exact path at creation. Every actor maintains its
own lease, releases it at handback and leaves source cleanup to the coordinator.
Two Cargo jobs and a measured free-space floor bound disposable build output.
The disposable PostgreSQL fixture is coordinator-owned synthetic test data;
serialize suites using it and retain verified-TLS/hosted-role proof separately.

Use the aep-drive implementor and independent adversary charters. This host has
no plugin subagent_type selector, so agents load the role explicitly. The worker
owns scoped Rust source/tests only, with no commits, AEP or normative edits.
Report any necessary new path for coordinator scope reconciliation. Preserve
legacy fingerprint bytes, provider schemas and all existing tests. No producer
may approximate the capability with an independent blob put followed by append.

Acceptance covers one native transaction across the blob bindings, ordered event
appends, admission callbacks, inline projections and durable group receipt in
File, SQLite and PostgreSQL. Failure/retry cases survive as refusals or exact
original results; fixture operations simulating erased bytes are never applied
to operator stores. Test code may stage corruption and interruptions under the
same established provider fixtures. No live store is opened in this unit.

The selected story records the required positive, negative, concurrent and
interruption cases. First retain behavioral red or mutation evidence, then run
the package lane, independent review, correction and coordinator mutation.
The whole gate and mandatory production, comparative and restart proof are
required before published main adoption. An exact verified main SHA suffices;
this dependency unit does not need a tag or release. Source implementation is
unexecuted at opening, and its opening is not project completion.

The implementor found that the inspected fingerprint spells its length field
byte_count while the coordinator's ESS projection used byte_len. The coordinator
verified the report against the specification and corrected both active copies
to byte_count before implementation. ESS validation passes. The future executable
fingerprint case must bind that declaration; agreement remains unexecuted here.
