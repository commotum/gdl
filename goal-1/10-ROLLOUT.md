# 10-ROLLOUT

## Offline Verification

- `uv run python -m unittest discover -s tests -v`: passed before rollout.
- After the live-smoke attribution fix, the expanded focused suite and full
  suite passed again (88 tests total at final verification).
- `uv run python scripts/gallery_dl_x_runner.py --version`: `1.32.4`.
- Python compilation and `git diff --check`: passed.

## Production Dry Run

- `scripts/archive-x-context --user visakanv seed --dry-run` made zero network
  requests and created no production context database.
- It inspected 2 raw timeline files / 182,568 records and reported 152,761
  reply edges with 152,349 unique immediate parent IDs. These are queue-sizing
  counts, not a started backfill.

## Disposable Live Smoke

- A disposable archive under `/tmp/gdl-context-smoke` contained exactly one
  known child-parent edge.
- `run --max-posts 1` made one paced focal request and captured exactly the
  requested external root post. Status reported one fully captured
  conversation, one target, one edge, no cycle, no media, and no integrity
  errors.
- Export labeled the external author `relationship: context` and used stable
  requested user ID `16884623`. The smoke exposed and then regression-fixed an
  incorrect inherited `canonical_requested_handle` presentation field.
- Database, config, and log modes were 0600. Text and binary credential scans
  found no cookie/auth values.

## Timeline Recovery and Resume

- The stopped manifest proved cursor `3_1173685814485643265/`; state still held
  `3_1181651824673083392/` because the second interrupt preempted its commit.
- The cursor was atomically repaired with an exact stale-state guard and the
  interrupted run as provenance.
- Visakanv restarted in tmux session `x` as run
  `20260720T023918Z-cf57e4`. Its manifest records
  `resumed_from_cursor: 3_1173685814485643265/`; startup finalized the abandoned
  manifest and independently recovered the same checkpoint.
- The normal archive is running. It first handles the one bounded pending MP4
  retry, then continues the timeline around September 17, 2019. Final tmux
  inspection showed it had returned to timeline work and advanced into posts
  from September 16, 2019.
- The production reply-context backlog was not seeded or started, and
  `_state/context.sqlite3` remains absent for Visakanv.

## Explicit Future Operations

- Inventory: `scripts/archive-x-context --user visakanv seed --dry-run`
- Deliberately create the queue: `scripts/archive-x-context --user visakanv seed`
- Inspect: `scripts/archive-x-context --user visakanv status`
- Resolve a bounded batch only when the timeline is stopped:
  `scripts/archive-x-context --user visakanv run --max-posts N`
