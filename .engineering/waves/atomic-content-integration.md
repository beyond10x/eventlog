# Atomic content integration, 2026-09-22

The source unit 5d3f5fa is integrated at 7e442bb. It adds native atomic blob
bindings and ordered appends for File, SQLite and PostgreSQL without changing
legacy fingerprint, journal operation or physical schema formats.

The implementation report records 156 to 176 executed cases and sixteen restored
behavioral mutations. The independent report adds eight cases, preserves every
implementation hash and reports 184 passed, zero failed and zero ignored.
Both reports are retained under `.engineering/reviews/`; their measured limits
remain part of this result.

The coordinator replaced SQLite byte equality with length equality. The
independent equal-length collision case failed its refusal assertion: exit101,
one executed failure. Restoration matched the implementation's SHA-256 manifest.
All eight independent cases are now mandatory production-roster entries.

## Integration checks

| Exact step | Exit | Result |
| --- | --- | --- |
| cargo run --locked -p eventlog-postgres --example production-proof | 0 | 184 passed, zero failed/skipped, no missing required cases |
| cargo fmt --all --check | 0 | no formatting drift |
| cargo clippy --workspace --all-targets --locked -- -D warnings | 0 | no warnings |
| ess specify validate --path ess/atomic-content | 0 | eventlog v1, two files, valid |
| aep plan artifact validate | 0 | valid; historical/empty-findings warnings retained |

The real PostgreSQL fixture ran server17.6 with the required verified-TLS and
application-role cases. The proof identifies source 7e442bb and dirty metadata
during execution; it is not represented as proof of a different clean commit.
It explicitly sets capacity_admitted=false. Toolchain stable was refreshed and
remained rustc1.98.1.

An initial ESS invocation selected only system.yaml and correctly refused its
absent domain source. Selecting the complete specification directory succeeded;
the failed invocation is retained separately and is not a product defect.

Exact-source CI, including comparative capacity and restart replay, is required
before main admission and consumer pinning. No operator store, deployment or
live capacity was changed by these synthetic fixtures.

Owners: implementor, provider source/shared exercises; independent reviewer,
eight adversarial cases; coordinator, restored mutation, roster, normative/AEP
reconciliation, integration and publication. Agent token/tool-cost data was not
exposed by this host and is not estimated.
