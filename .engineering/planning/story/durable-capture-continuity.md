---
format: aep.planning-md/3
id: story:durable-capture-continuity
kind: story
status: active
title: A later process continues capture from a persisted SQLite checkpoint
refs:
- provider: github
  reference: beyond10x/eventlog#39
relations:
- serves: vision:O2
scope:
- confidence: inferred
  path: CHANGELOG.md
- confidence: inferred
  path: crates/eventlog-core/src/capture.rs
- confidence: inferred
  path: crates/eventlog-core/src/lib.rs
- confidence: inferred
  path: crates/eventlog-sqlite/src
- confidence: inferred
  path: crates/eventlog-sqlite/tests
revision: 4
transitions:
- {from: "draft", to: "proposed", at: "2026-10-06T23:36:25Z", actor: "human:timo", revision: 2}
- {from: "proposed", to: "active", at: "2026-10-06T23:36:25Z", actor: "human:timo", revision: 3}
---
## Context

A consumer that runs one short process per command cannot keep an in-process `CaptureCheckpoint`, so every command pays a complete capture and verification whose cost grows with the store. https://github.com/beyond10x/eventlog/issues/39 asks for a checkpoint a later process can continue from, on SQLite. It is the first link of Entity Runtime's performance work (https://github.com/beyond10x/entity-runtime/issues/55).

## Accepted design

`docs/design/durable-capture-continuity.md`. Typed home: `ess/capture/domains/capture.yaml` (format `ess/23`, validated with ess 0.55.0), JSON Schema projection committed under `ess/capture-generated/schema/types/`. The Rust types are hand-written because the generated Rust crate enables `serde_json/arbitrary_precision`; `story:generate-capture-types-from-ess` records that gap.

## Acceptance

The design's Acceptance section, each item a named test on a real SQLite file with a new `SqliteEventStore` per step. Existing provider conformance, the in-process capture suite and the full production-proof gate stay green. A test holds the checkpoint encoding to the committed generated schema, and a test pins one `request_hash` value for a body containing `1.50`.

## Scope

eventlog-core `capture.rs` and exports; eventlog-sqlite `capture.rs`, `tracked_capture.rs`, `atomic_group.rs`, `inline_admin.rs`, `inspection.rs`, `lib.rs` and a new durable continuity module; SQLite tests; CHANGELOG.md; the design and ESS files named above.
