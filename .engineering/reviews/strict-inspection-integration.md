# Strict inspection integration evidence

The shared envelope validator now applies existing append field, opaque-identity,
causation-depth and object-body rules to inspected history. File checks retained
envelope keys before its native fold can discard unknown metadata and rejects
duplicate returned event identities. Application object keys and schema version
zero remain valid. The independent adversary tests were preserved unchanged;
strict-inspection-correction-review.md records their hashes and verified outcomes.

Owners: original inspection findings belong to the implementation; coordinator
owns their correction, integration, proof-runner correction and this evidence.
The original immutable review was recorded without the required Owners line;
this supplemental attribution does not rewrite that historical record.

Coordinator mutation removed only the object-body condition from the shared
envelope check. Both independent provider cases failed behaviorally, accepting
the invalid body, with cargo exit 101. Exact original source was
restored and compared before the final gate. Logs remain in the assigned unit
scratch as coordinator-mutation.log and correction-original.rs.

The first whole-workspace run refused because its PostgreSQL migration case
requires the missing fixture; no source was changed to skip that test. A dedicated
disposable PostgreSQL fixture was configured through the repository's TLS helper,
with the declared CPU, memory and process bounds. It serves synthetic tests only.

The first production run passed its cases but the report writer failed opening
its own executable after nested Cargo replaced that binary. The coordinator moved
capture of the running artifact bytes before spawning Cargo. This binds the same
executing artifact while avoiding the later unlinked path; strict-inspection-runner-review.md records
independent review. The failed run and corrected run are retained separately.

Final command: bash scripts/gate.sh --production-proof.
Exit: zero. Tests: 156 passed, 0 failed, 0 ignored; every required
case present. Formatter and workspace Clippy both exit zero. The JSON proof
declares conformance_valid=true and identifies the actual database server.
Its raw output, report and exact binary hash remain in assigned scratch as
production-proof-corrected-raw.log and production-proof-corrected.json.
This local run includes verified TLS and the restricted hosted application role.

Both ESS documents validate. The planning store validates; existing prose-only
reviews and the empty findings list still produce nonfatal diagnostic warnings.
Stable Rust was updated before publication and reported unchanged.

Required remote common checks, comparative capacity and restart proof remain
pending on the exact candidate. This record does not claim main publication,
consumer adoption, capacity admission, store migration or project completion.
