# 8-UX

## Current Facts

- The normal parser and engine now support the complete lifecycle with no
  required phase budgets, but help/dry-run text still describes reply context
  as excluded or optional discovery.
- Main writes structured phase results but prints no coherent final summary.
- Repository documentation still contains routine standalone legacy/context
  run commands and required window/post counts from Goals 1 and 2.
- Dry-run currently validates cookies and compatibility without X requests or
  archive writes, but it does not inspect per-user legacy/context backlog state.

## Updated Assumptions

- Dry-run can safely inspect existing JSON/SQLite in read-only mode and must not
  migrate/create a database or acquire write locks.
- The routine docs should show exactly one command; specialized CLIs remain
  documented under an explicitly advanced maintenance section.
- Phase summaries should expose status/counts/frontiers, never cookies, signed
  URLs, raw legacy cursors, or claims beyond source visibility.

## Big Picture Objective

Make the one-command lifecycle understandable, previewable, and honest about
partial/manual-review/source-unavailable outcomes.

## Detailed Implementation Plan

- Replace stale parser and dry-run language with the automatic three-phase
  lifecycle and advanced-bound semantics.
- Add read-only per-target dry-run planning for modern mode, transition/legacy
  state, context queue/closure, and media backlog.
- Print one concise final per-target phase summary after completed invocations.
- Update README and archive dataset/context documentation so routine operation
  uses only `uv run scripts/archive-x --user USERNAME`.
- Keep standalone legacy/context status, integrity, retry, export, and bounded
  diagnostics clearly labeled as maintenance.
- Add golden/output tests for absent, pending, complete, manual-review,
  advanced-limit, retry-only, and diagnostic modes.

## No-Cheating Checks

- Patch network, SQLite creation/migration, atomic writes, and locks to fail in
  dry-run tests.
- Routine documentation search must find no required follow-up run pipeline.
- Summary derives from structured phase truth rather than inferring completion
  from an empty stdout pattern.

## Completion Requirements

- `--help`, dry-run, normal summary, README, and dataset docs agree on the exact
  one-command behavior and source-visibility limitations.
- Dry-run is provably network/write-free and shows relevant phase/backlog state.
- Specialized commands are retained but cannot be mistaken for normal setup.
- UX/golden tests, compilation, documentation scans, and diff checks pass.

## Stage Results

- Completed on 2026-07-22.
- `--help` now describes one conservative modern/legacy/ancestor/media
  lifecycle. Routine use needs only `uv run scripts/archive-x --user USERNAME`;
  the compatibility seed flag explicitly reports that it is deprecated and a
  no-op.
- Dry-run now prints all three phases plus each target's modern mode, validated
  legacy status/frontier/account floor, shared pending-media count, and
  read-only context pending/manual/media/integrity summary. Opaque cursors are
  redacted.
- Added a query-only SQLite inspection path. Tests patch atomic writes, lock
  acquisition, and migration backup to fail, then prove dry-run succeeds with
  unchanged database bytes and no newly created archive root.
- Real invocations now print a compact target/phase summary derived from the
  checkpointed structured results.
- Existing legacy `--full-rescan` and early `--since` modes are clamped to the
  Snowflake epoch, preventing a deliberate modern rescan from crossing into
  the preserved sequential-ID pagination domain.
- README and generated dataset documentation now present automatic legacy
  handoff, authoritative context bootstrap/closure, other-author parents,
  metadata/media independence, and source-visible limitations. Standalone
  legacy/context commands are labeled advanced maintenance only.
- Golden tests cover absent context, pending context, completed legacy,
  advanced rollout limits, cursor redaction, summary output, and read-only
  behavior.
- Verification: the expanded focused set passed 126 tests; the exact wrapper
  `uv run scripts/archive-x --help`, Python compilation, documentation scans,
  and `git diff --check` passed.
- No production request, archive write, lock acquisition, or process change
  occurred.
