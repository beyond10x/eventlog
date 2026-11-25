---
format: aep.planning-md/3
id: story:kpi-baseline-ratchets-the-improved-counters
kind: story
status: draft
title: KPI baseline ratchets the counters that improved
summary: Re-record 25 improved counters through the CI record dispatch with ratchet; baseline.json committed unedited
relations:
- serves: vision:O2
- informed_by: story:projection-find-holds-its-bound-with-a-prepared-statement
scope:
- confidence: cited
  path: .github/workflows/kpi.yml
- confidence: cited
  path: kpi/baseline.json
revision: 2
---
## Outcome

`kpi/baseline.json` records the value each of 25 counters has today, so the `KPI counters` job
prints no `IMPROVED (re-record to ratchet)` line for them. The baseline then holds the gains
already made: a later change that gives one of them back fails as `REGRESSED` instead of passing
silently below a stale record.

## Why

On main `4ea0fc3b` the `KPI counters` job is green and prints 25 counters as
`IMPROVED (re-record to ratchet)` (log of run 45612151940). An improved counter that is not
re-recorded keeps its old, higher value in the baseline, so the slack between the old record and
the real value is room for a regression that no check sees.

The 25 ids:

- core: `core.fold_calls.151_commands`, `core.fingerprint.alloc_bytes`,
  `core.capture_budget.alloc_bytes`
- file: `file.open.bytes_read`, `file.feed.map_visits`, `file.idle_poll.journal_bytes`,
  `file.event_append.blob_dir_entries`, `file.event_append.blob_dir_syncs`,
  `file.projection_page.cursor_present`
- sqlite: `sqlite.absent_reads.generation_rows`, `sqlite.projection_find.vm_steps`,
  `sqlite.legacy_migration.vm_steps`, `sqlite.inline_rebuild.retained_events`,
  `sqlite.projection_page.cursor_present`
- tree: `tree.append.group_files_listed`, `tree.append.event_digests`,
  `tree.projection_find.vm_steps`, `tree.projection_page.cursor_present`
- postgres: `postgres.open.round_trips`, `postgres.blob_group.write_transactions`,
  `postgres.blob_group.round_trips`, `postgres.absent_reads.generation_rows`,
  `postgres.projection_page.cursor_present`, `postgres.feed_one_row.buffers`,
  `postgres.feed_one_row.rows_filtered`

## Mechanism

- The record path is the `record` dispatch of `.github/workflows/kpi.yml`. It runs
  `eventlog-kpi record --counters --ratchet <id,...>` on the CI runner and uploads
  `kpi/baseline.json` as the artifact `kpi-baseline`. Pull request 85 added the `ratchet` input.
- A record run keeps every recorded counter unless its id is named in `ratchet`. An id named
  there takes the runner's measurement; an id measured worse than recorded fails the job.
- Hand-editing `kpi/baseline.json` is barred. The file is committed by a person or the bot from
  the artifact, unedited.

## Acceptance

Each line is observable alone.

1. A `record` dispatch of `KPI` with `ratchet` naming exactly the 25 ids above ends green and
   uploads the artifact `kpi-baseline` (`kpi/baseline.json`). The run URL is named in the pull
   request.
2. The artifact is committed unedited in a change that touches only `kpi/baseline.json`
   (`git diff --stat` of that commit lists that one path; the file's bytes equal the artifact's).
3. After that commit, the `KPI counters` job on the pull request prints no `IMPROVED` line and no
   `REGRESSED` line for any of the 25 ids.
4. No counter outside the 25 changed value in that commit (diff of `kpi/baseline.json`: the
   changed counter ids are a subset of the 25; the revision and time fields move as the record
   path writes them).

## Scope

`kpi/baseline.json` is the one file that changes. `.github/workflows/kpi.yml` is read, not
edited. No code, test or workflow change belongs to this story.
