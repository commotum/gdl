# 10-ROLLOUT

## Current Facts

- Offline implementation and verification are complete, production legacy
  state is explicitly initialized, and exactly one production UTC window has
  committed.
- The modern timeline cursor remains `3_29116490825/`; the original state is
  retained privately at
  `_state/backups/state.pre-legacy-init-0abdfa608814.json`.
- No archive process is intended to remain running after this stage. The long
  2008--2010 continuation is not authorized by this rollout.

## Updated Assumptions

- One disposable final-runner walk should reproduce the 16 target-account
  records and the four-distinct-empty-tail signal within six SearchTimeline
  calls.
- The first production window `[2010-10-29T00:00:00Z,
  2010-10-30T00:00:00Z)` should replay the known boundary and add several
  earlier October 29 posts without duplicating the boundary row.

## Big Picture Objective

Validate the final runner live, initialize production with exact stale guards,
and commit exactly one repeat-confirmed production UTC window, then stop with
an auditable next frontier.

## Detailed Implementation Plan

- Run one final-runner metadata-only disposable walk for October 28 UTC with a
  six-request cap and 180-second process bound.
- Compare safe ID/date/account fields and telemetry; remove disposable content
  after recording hashes/results.
- Recheck production hashes/processes, create a private pre-init state backup,
  execute the exact token, and verify initialization diff.
- Run `--windows 1` only. Verify two matching walks, canonical raw, dataset
  counts/oldest row, frontier, modern cursor, pending media, modes, manifests,
  locks, and stopped process state.
- Run post-smoke focused/full verification. Do not launch the remaining
  2008–2010 backfill.

## No-Cheating Checks

- The disposable walk uses no production state, dataset, media archive, or
  cookie update.
- Production cannot advance from one walk or an ambiguous result.
- Initialization token/source hashes are checked immediately before write.
- Only one UTC root window may commit; no ordinary timeline/context command is
  started.

## Completion Requirements

- A post older than `2010-10-29T19:30:34Z` is durably merged or decisive source
  limitation is recorded without false completion.
- State is contiguous, modern cursor preserved, backup present, and next
  command explicit.
- Full post-smoke suite/audits pass and tmux remains idle.

## Stage Results

- The first disposable final-runner probe failed closed before accepting data
  because the reviewed profile response stores the numeric identity at
  `data.user.result.rest_id`. The parser was corrected and covered offline;
  production remained untouched.
- The repeated disposable October 28 UTC probe passed. It accepted 16 records
  for numeric account `16884623`, stayed inside the exact UTC window, and
  terminated after four distinct successful empty cursors within the six-call
  cap. Its raw SHA-256 was
  `ad857d20b4d08cec1c11ee4e224994306780c405c4587027e14d994434e6e7e1`;
  disposable files were removed after recording the result.
- Stale-guarded initialization used token
  `0abdfa6088141c1a3c7d62e132c8d2045c963c44245ecca0825ddf263fea8742`.
  The private pre-init backup SHA-256 is
  `98821e48e631989607bef3e917d334e70d7f169f6dee97659851065edf384f67`,
  exactly matching the original state. The preserved stopped manifest remains
  `cc5e15fb28b226c00c6af8f18e243f522d53f1c3cef262494d398907da8fffee`.
- Production run `20260722T212205Z-1c6865` processed only
  `[2010-10-29T00:00:00Z, 2010-10-30T00:00:00Z)`. Both walks returned the same
  eight IDs and independently ended with `distinct_empty_tail` after five of
  six permitted search calls. There were no API errors, repeated cursors,
  splits, identity mismatches, or persisted opaque cursor values.
- Canonical raw SHA-256 is
  `ab91b4b11b3606b7a1513ba1bd8994ea406fcd6e1b0692f03e481f6e54ff1afc`.
  Five records were new after overlap deduplication, growing `posts.jsonl`
  from 258,065 to 258,070 records and `authored-posts.jsonl` from 257,981 to
  257,986; reposts remain 84. The resulting dataset SHA-256 is
  `fee363633161e49645a0efd070d76aa02085653651f059045d9356a3a4e4405c`.
- The contiguous source-visible frontier is now
  `2010-10-29T00:00:00Z`. The next bounded window is
  `[2010-10-28T00:00:00Z, 2010-10-29T00:00:00Z)`, and the exact continuation is
  `scripts/archive-x-legacy --user visakanv --output-root /mnt/Bibliotheque/gdl/x-archive run --windows 1`.
- The modern cursor remains `3_29116490825/`, pending media remains two, every
  run/state/backup/generated-document artifact checked is mode `0600`, and
  credential/cursor-leak scans were empty. The shared dataset README was
  refreshed without network access.
- Post-smoke verification passed all 126 tests, both runner compatibility
  checks report gallery-dl 1.32.4, Python compilation and `git diff --check`
  pass, and the disposable diagnostic directory was removed.
