---
format: aep.planning-md/1
id: verification-report:final-integration-proof-20260909
kind: verification-report
status: draft
title: Final Eventlog integration and comparative proof
relations:
- verifies: story:verify-and-publish-eventlog-source-release
revision: 1
---
## Frozen implementation

Candidate 9bb72d903243b7bb54341771cb4953f3418f54b4, clean throughout measurement. Baseline 20e00c1eeab5d67bd5f749bbbd871b1fbfe7f796 with original runtime source verified unchanged. Compiler rustc 1.98.1 (48a229cea 2026-09-01). Raw evidence root: /home/timo/.cache/eventlog-complete-20260909.

Required production gate exited 0: 98 passed, 0 failed, 0 skipped, missing_required_cases=[]. Formatting and clippy exited 0. Frozen receipts are evidence/frozen-production.json and evidence/frozen-gate.log. The fresh-fixture repeat also passed the same 98 cases; evidence/fresh-production.json and evidence/fresh-gate.log. All three ESS roots validated before source freeze.

## Both comparative observations

The first comparative run exited 1. comparative-final/capacity/comparison.json records 10 of 12 valid configurations with zero correctness violations. At concurrency 32 hot-key, baseline p99=2436523us, maximum=2972622us; candidate p99=997643us, maximum=2149597us. The maximum bound is 2000000us. Independent restart-final failed the 15000ms recovery objective before PostgreSQL was ready; accumulated-fixture-postgres.log preserves its startup and recovery output. These failed observations remain retained, not overwritten or recast as successful.

This was the long-lived integration fixture used across several production gates and worker integrations. A single fresh disposable fixture was provisioned to compare against accumulated fixture state. This is an environmental diagnostic, not proof that accumulated state alone caused the failure. Host I/O pressure was also observed and retained; no unrelated workload was stopped. PostgreSQL image, Docker-volume storage, 2 CPUs, 1GiB memory, 256 pids, compiler, workloads and every acceptance threshold remained unchanged. A full 98-case production gate ran on the fresh fixture before its comparison.

comparative-fresh/proof.json records exit 0: laboratory_valid=true, 12 configurations, 2 restarts. Baseline restart-to-replay=1589ms; candidate=1336ms. All replayed events and retry receipts were retained, both correctness-violation counts zero. Both runs used identical source manifests and capacity/recovery binary hashes. Production capacity remains explicitly unadmitted; these are laboratory measurements, not a deployment budget.

## Publication and recovery

Implementation and review commits are published on wave/complete-review. The final receipt commit changes only governed planning records; runtime and workflow bytes remain those measured above. Main integration follows the green gate under AGENTS.md. GitHub workflow 34298444659 independently checks the frozen candidate; its current/final verdict must be read from https://github.com/beyond10x/eventlog/actions/runs/34298444659, not inferred from this record.

The baseline harness and resolved lockfile are preserved separately at e40409bd5d3ed351d6effd4b5cd5ea1c5cdf5c33 on recovery/comparative-proof-baseline-20260909. That recovery commit was made after measurement, and is not the immutable measured baseline HEAD.

All four confirmed review defect classes are fixed and their stories implemented. Guarded-refusal and effect contracts are integrated. Final source publication remains active pending separate release authorization and exact-tag checks/artifacts. No version bump, tag, GitHub Release or consumer promotion is claimed by this evidence.
