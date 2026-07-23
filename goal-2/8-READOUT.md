# 8-READOUT

## Current Facts

- State distinguishes pending, active, manual-review, and complete, but raw
  state is too detailed for routine operator decisions.
- “Complete” must always be qualified as source-visible contiguous coverage.

## Updated Assumptions

- A compact JSON status is both scriptable and unambiguous if it includes the
  next exact command and explicit zero-request/zero-write guarantees.

## Big Picture Objective

Make coverage, uncertainty, pending media, active/retry state, and the next
safe operator action obvious without contacting X or changing files.

## Detailed Implementation Plan

- Add a normalized status summary for every lifecycle state.
- Add the exact stale-guarded initialization command to `plan`.
- Document boundary behavior, initialization, bounded runs, manual retry,
  source-visible semantics, and media independence in the repository and
  generated dataset README.
- Add golden/read-only tests with secret sentinels.

## No-Cheating Checks

- Status never calls a runner, validates cookies, acquires a write lock, or
  writes state.
- Completion language never claims all historical posts.
- Output excludes cookie values, headers, opaque cursors, and signed media
  URLs; the preserved ordinary boundary cursor is explicit non-secret state.

## Completion Requirements

- Pending, active, manual-review, complete, and not-initialized outputs pass.
- Commands shown by status match the guarded CLI.
- Documentation distinguishes metadata coverage and pending media.

## Stage Results

- `status` now reports lifecycle, numeric identity, source boundary, contiguous
  source-visible coverage, next UTC window, active/manual/last-completed state,
  pending media, preserved modern cursor, zero requests/writes, and the exact
  next command.
- `plan` prints the exact stale-guarded initialization command. Neither command
  validates cookies, runs gallery-dl, locks for writing, or changes files.
- Golden tests cover not-initialized, pending, active, manual-review, and
  complete output and prove unrelated secret sentinels do not leak.
- Repository and generated dataset documentation now explains the ID boundary,
  opt-in commands, two-walk rule, failure semantics, manual retry, media
  independence, and the limited meaning of source-visible completion.
