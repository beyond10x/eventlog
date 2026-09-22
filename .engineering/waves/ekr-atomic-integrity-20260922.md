# SQLite atomic integrity repair

The approved consumer completion requires a provider integrity repair discovered
while qualifying the published release. This unit owns
task:validate-atomic-blob-reuse. Its reproduced defect and controls are retained in
.engineering/reviews/sqlite-atomic-integrity-probe.md. The source fix does not
change the existing integrity, receipt or storage contracts.

Managed tree: ekr-atomic-integrity-20260922.
Branch: codex/ekr-atomic-integrity-20260922.
Source base: published tag 0.3.0.
Target: <cache>/b10x-target/ekr-atomic-integrity-20260922.
Evidence: <cache>/ekr-completion-20260922/eventlog-atomic-integrity.
Coordinator lease: codex-ekr-atomic-integrity-coordinator.
Implementor lease: codex-ekr-atomic-integrity-implementor.

One source implementor owns the SQLite atomic module, its existing atomic_blob
test target and the exact production-proof case roster, as recorded on the
parent story. Root owns all planning and publication. Independent review and
the repository's required real-backend CI precede integration. Original probe
source, red output and synthetic databases stay retained unchanged.

The journal's full privacy scan finds inherited occurrences also present in the
published base. New journal bytes and artifact bodies are scanned separately;
no inherited history or private policy is rewritten. Publication must still
pass the enrolled Gates policy.

The implementor froze the three assigned files and released its lease. Its retained
implementation-report.md records the original regressions, mutation failure,
restored source hashes and passing scoped checks. Independent review now owns
lease codex-ekr-atomic-integrity-review and the same sequential build directory;
the coordinator did not compile there until review handback. The reviewer added
only crates/eventlog-sqlite/tests/atomic_blob_integrity_review.rs and released its
lease. Its verbatim no-new-findings report is retained in
.engineering/reviews/sqlite-atomic-integrity-review.md. The coordinator then
included every added case in the production-proof roster.

The local full gate executed but stopped in the PostgreSQL library tests because
EVENTLOG_TEST_POSTGRES_URL is absent. Both required-fixture failures and the
command's own exit status remain retained; no local full-gate success is claimed.
The required CI supplies the real PostgreSQL/TLS fixture and must pass before
integration. Populated legacy blob migration and the consumer's live stores
remain outside this repair.
