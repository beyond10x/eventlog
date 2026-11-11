---
format: aep.planning-md/3
id: approval-record:postgres-batch-read-and-append
kind: approval-record
status: accepted
title: Accept the PostgreSQL batch read and append for 0.9.3
relations:
- decides: story:postgres-batch-read-and-append
revision: 2
transitions:
- {from: "draft", to: "accepted", at: "2026-11-11T00:15:12Z", actor: "human:timo", revision: 2}
---
## Decision

Accepted: `story:postgres-batch-read-and-append`.

## By whom and when

Timo, 2026-11-11, in the spindle coordinating session: "dispatch postgres work now", after the
session presented three fixes for the per-instance PostgreSQL read in spindle (A: an eventlog batch
read, released as eventlog 0.9.3; B: concurrent reads in spindle; C: direct SQL in spindle) with A
as the default on silence.

## Covers

The story, its implementation in one unit branch, its review, merge and the 0.9.3 release.
