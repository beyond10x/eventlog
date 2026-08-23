# b10x eventlog — backlog

One file per story (`EL-NNN-<slug>.md`); frontmatter carries `id`, `title`, `status` and
`depends_on`. Status is one of `backlog | ready | in-progress | blocked | done`. The index below is
hand-written.

This component exists because of
[ADR 0055](https://github.com/daemonloom/daemonloom/blob/e01ea676da18fb855814e7621514e0c98fc57c2c/architecture/adr/0055-durable-domain-state-is-a-fold-over-an-event-log.md).
Read [RFC 0020](https://github.com/daemonloom/daemonloom/blob/e01ea676da18fb855814e7621514e0c98fc57c2c/architecture/rfcs/0020-state-is-a-fold-over-an-event-log.md) before
changing any of these stories: it is the design they implement, and its physical schema is
normative.

| ID | Title | Status |
|---|---|---|
| [EL-001](EL-001-the-log-and-its-two-backends.md) | The log and its two backends | done |
| [EL-002](EL-002-aggregates-repository-and-snapshots.md) | Aggregates, repository and snapshots | done |
| [EL-003](EL-003-projections-inline-and-catch-up.md) | Projections, inline and catch-up | done |
| [EL-004](EL-004-schema-evolution-and-golden-vectors.md) | Schema evolution and golden vectors | done |
| [EL-005](EL-005-erasure-redaction-and-snapshot-invalidation.md) | Erasure, redaction and snapshot invalidation | done |

The module conversions that consume this kit are
[E-006](https://github.com/daemonloom/daemonloom/blob/e01ea676da18fb855814e7621514e0c98fc57c2c/model/modules/docs/epics/E-006-every-module-persists-as-events.md).
