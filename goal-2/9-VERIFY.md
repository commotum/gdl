# 9-VERIFY

## Current Facts

- Stages 1–8 are implemented; production still has no legacy state or run.
- The full offline suite currently passes 124 tests.
- Stage 10 is the only stage allowed to initialize production or perform a
  bounded production smoke.

## Updated Assumptions

- Verification must independently prove read-only commands and ordinary
  timeline behavior did not create hidden legacy state or artifacts.

## Big Picture Objective

Audit the integrated implementation offline, re-derive the Visakanv plan,
prove production preservation, and leave an exact rollout boundary.

## Detailed Implementation Plan

- Run focused/full suites, compilation, both runner compatibility checks,
  whitespace, secret/static-pattern, permission, and lock audits.
- Run production `status`/`plan` and ordinary timeline dry-run read-only;
  compare state, manifest, dataset, and context hashes before/after.
- Prove a production `run --windows 1` fails before credentials/network while
  uninitialized.
- Review the complete diff for watchdog, context, normal runner, and unrelated
  worktree changes.

## No-Cheating Checks

- Tests may use no network or production path.
- Green unit tests are supplemented by independent hashes and artifact scans.
- No production init token is executed in this stage.
- No long archive process is started.

## Completion Requirements

- All commands and exact outcomes are recorded.
- Production state remains byte-identical and tmux remains idle.
- No secret, cookie value, opaque cursor, unsafe mode, or forbidden ID math is
  present in the legacy path.
- Stage 10 commands and approval boundaries are explicit.

## Stage Results

- Focused legacy tests passed 36 tests; final full offline discovery passed 126
  tests. All five Python modules compiled, both pinned runners reported 1.32.4,
  and `git diff --check` passed.
- Production `status` reported `not_initialized`, zero requests/writes, and the
  exact `plan` command. Production `plan` independently re-derived numeric ID
  `16884623`, 258,065 rows, cursor `3_29116490825/`, initial frontier
  `2010-10-30T00:00:00Z`, floor `2008-10-21T12:01:00Z`, and guarded token
  `0abdfa6088141c1a3c7d62e132c8d2045c963c44245ecca0825ddf263fea8742`.
- Ordinary `scripts/archive-x --dry-run` remained the normal timeline/profile
  plan with the three-window watchdog; it did not initialize or mention a
  legacy launch. `archive-x-legacy ... run --windows 1` with the explicit
  production root refused before credential validation/network because state
  was uninitialized.
- Before/after production hashes are identical: state
  `98821e48e631989607bef3e917d334e70d7f169f6dee97659851065edf384f67`,
  stopped manifest
  `cc5e15fb28b226c00c6af8f18e243f522d53f1c3cef262494d398907da8fffee`,
  and dataset
  `f7a86f799ace4ec6a87cbff4caf04bd9dc3ed4f59daafed1aedf104606204479`.
- State still has the modern cursor, two pending media records, no
  `legacy_backfill`, and no legacy run manifest. Goal 1's `context.sqlite3`
  does not exist and its code has no diff.
- New executables are mode `0700`; production state is `0600`. Static search
  found no `max_id`, Snowflake shift/epoch decode, or date-from-ID operation in
  the legacy path and no credential literal assignment in changed scope.
- Tmux pane `x` remains idle bash PID 1847252. The private disposable live
  diagnostic directory created in Stage 2 was removed after its non-secret
  results and hashes were recorded.
