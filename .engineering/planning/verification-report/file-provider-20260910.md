---
format: aep.planning-md/1
id: verification-report:file-provider-20260910
kind: verification-report
status: draft
title: File Eventlog implementation and durable boundary verification
relations:
- verifies: story:file-eventlog
revision: 4
---
## Implementation
The new eventlog-file crate implements EventStore and AtomicEventStore. One versioned JSONL frame commits ordered events, inline projection writes, guards and receipts. A synchronized manifest selects the committed length and digest. Exact append intents permit recovery only of proved uncommitted tails; corruption and divergent observed histories refuse. Blob content is separately synchronized and hash verified. Snapshots are disposable, generation checked caches. Privacy rewrites are crash recoverable and remove obsolete active objects; redacted views are fenced until a complete atomic rebuild. Details and explicit platform/scale limits are in docs/design/file-provider.md.

## Verification
bash scripts/gate.sh --production-proof exited 0 against PostgreSQL 17.6 using the existing production-fixture TLS setup and dedicated application role. Its exact required receipt reports 128 passed, 0 failed, 0 skipped and no missing cases. This includes eleven unchanged shared file-provider contracts plus independently specified process, recovery, corruption, contention and privacy cases. Child dispatch entrypoints are included in the Rust runner's counts; behavior evidence comes from their parent tests. Formatting and all-target strict Clippy passed. The required-case roster now explicitly includes file-provider conformance and durability cases.

The source/toolchain invocation used CARGO_BUILD_JOBS=2, CARGO_PROFILE_DEV_DEBUG=0, CARGO_PROFILE_TEST_DEBUG=0, CARGO_INCREMENTAL=0 and explicit empty RUSTC_WRAPPER and RUSTC_WORKSPACE_WRAPPER. Native Cargo configuration was inherited once. Rust 1.98.1 ran the full gate. A separate target directory ran every file-provider test under Rust 1.91.0 with the same locked dependencies and passed. Adding the new package changed no existing locked dependency version.

Three deliberate mutations each exited 101 for the expected behavioral failure: omitting frame digest verification admitted altered committed data; omitting process locking broke independent writers; retaining invalidated snapshot files left the erased marker in active storage. All three were reverted before the final production gate and MSRV run.

## Evidence
Private ess-evolution-20260910 evidence directory: file-production-gate.log, file-production-proof.json, file-production-raw.log, file-msrv.log, file-mutation-digest.log, file-mutation-lock.log and file-mutation-cache-erasure.log. The file-provider tests live in crates/eventlog-file/tests and journal.rs's unit test module. New source and the inherited candidate remain uncommitted at base 2e59179003f59c5ffa1a564094186642b6b7136e; the base commit alone does not identify the tested candidate.

The initial ordinary workspace gate failed because an inherited PostgreSQL migration test required its live fixture. The fixture was then created and the complete mandatory gate passed; no missing lane or failed invocation was relabeled passing. The temporary PostgreSQL container used the same pinned 17.6 image as the previous SQL proof. It was stopped after verification.

## Limits
This establishes local behavior and process-death recovery on Linux, not hardware power-loss certification, network-filesystem support or production capacity. The provider replays full history per operation. Administrative privacy changes the epoch and other open handles must reopen. Git histories and backups are separate archives. No ER/AEP/SDK/application migration, publication, release or deployment is claimed by this report.

## Fixture cleanup

The explicitly named disposable PostgreSQL container and its anonymous volume were removed after verification. Certificate/key files remain only in the private fixture evidence directory; no credential or private key was written to component source.

## Final mixed-intent verification

Final review added refusal of simultaneously present append and privacy intents before either authority file can change. Such a pair cannot be produced by a serialized provider writer. The independent mixed-copy regression failed when this check was deliberately removed: recovery changed the journal before refusing. That fourth mutation was reverted.

The exact final source then passed bash scripts/gate.sh --production-proof again: 129 passed, 0 failed, 0 skipped and no missing required cases, followed by formatting and strict workspace/all-target Clippy. Every file-provider test passed on Rust 1.91.0 with the final source. Evidence: file-production-gate-final.log, file-production-proof-final.json, file-production-raw-final.log, file-msrv-final.log and file-mutation-mixed-intents.log. Earlier 128-test evidence remains retained against its earlier source. Both disposable PostgreSQL containers were stopped after their runs; the final container is being retired with its volume.

## Publication

The previously uncommitted candidate is now published in source commit 0d953650f161d0a43df14ce605339d9f519ba525 on Eventlog main (2026-09-10 remote-ref readback). All executable source and locked dependency checksums match the retained final verified candidate. Publication adds planning receipts and removes one trailing documentation blank line; no executable source changed after the 129-test proof. Author and committer were verified as b10x-bot[bot]. The operator requested [skip ci] for the expensive remote Actions workflow; no remote proof or comparative capacity result is asserted. Both temporary PostgreSQL containers and their volumes have been removed. Earlier sections describe the historical verification state before this publication.
