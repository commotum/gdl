# 4-STATE

## Current Facts

- Legacy work now has a fixed, reviewed nested schema and a dedicated CLI, so
  ordinary timeline invocations need no legacy-mode branch.
- Production state is schema version 1 with the modern resume cursor and no
  `legacy_backfill` object.
- Existing atomic JSON writers and both exclusive archive locks can be reused.

## Updated Assumptions

- Initialization evidence can be derived locally from the state cursor, the
  matching stopped manifest, the minimum dataset record, and profile creation
  timestamp.
- A hash token over canonical evidence provides a concise stale guard while
  keeping the operator command non-secret.

## Big Picture Objective

Implement pure validation and transition helpers plus write-free
status/planning and explicit guarded initialization, without any X request or
production mutation.

## Detailed Implementation Plan

- Add a dedicated legacy module/wrapper with `status`, `plan`, `init`, and
  reserved `run` subcommands.
- Implement evidence discovery, canonical token generation, nested schema
  validation, initialization, window claim, manual-review, and confirmed
  window completion as pure/copying transitions.
- Acquire the existing global and archive locks for initialization and use the
  existing atomic mode-`0600` state writer.
- Add tests for absent, valid, stale, idempotent, corrupt, unknown-version,
  identity mismatch, invalid interval, and preservation paths.

## No-Cheating Checks

- `status` and `plan` are tested with write/network sentinels.
- Initialization cannot delete or modify the modern resume cursor or unrelated
  state keys.
- No automatic initialization occurs from import, install, normal archive, or
  the future `run` command.
- Transition helpers require exact active window/run/evidence guards.

## Completion Requirements

- Focused state/CLI tests and full offline suite pass.
- Dry planning against a fixture reports exact initial/floor bounds and token.
- Real production state hash remains unchanged.
- No X request or legacy run directory is created.

## Stage Results

- Added the dedicated `scripts/archive-x-legacy` entry point and
  `scripts/archive_x_legacy.py`; its `run` command deliberately fails closed
  until the fetch/orchestration stages are complete.
- Initialization planning derives and guards the numeric account ID, exact
  source cursor, matching stopped run/manifest hash, pre-init state hash,
  oldest dataset ID/timestamp, dataset row count, and account creation floor.
- The write-free Visakanv plan proposed
  `[2008-10-21T12:01:00Z, 2010-10-30T00:00:00Z)` with first frontier
  `2010-10-30T00:00:00Z`, source cursor `3_29116490825/`, 258,065 rows, and
  confirmation token
  `0abdfa6088141c1a3c7d62e132c8d2045c963c44245ecca0825ddf263fea8742`.
  The token is evidence, not authorization to initialize production.
- Added pure/copying validation, initialization, claim, manual-review, and
  completion transitions. Unknown schema, stale token, identity mismatch,
  cursor/oldest mismatch, invalid interval order, non-contiguous leaves, and
  wrong active-window guards fail closed.
- Focused state/characterization suite passed 15 tests. The full offline suite
  passed 103 tests. Compilation, runner 1.32.4 compatibility, and
  `git diff --check` passed.
- Fault injection proved an initialization write failure preserves prior state;
  fixture initialization is mode `0600`, preserves unrelated/modern fields,
  and repeats byte-for-byte idempotently.
- Production state and stopped manifest hashes remain
  `98821e48e631989607bef3e917d334e70d7f169f6dee97659851065edf384f67`
  and `cc5e15fb28b226c00c6af8f18e243f522d53f1c3cef262494d398907da8fffee`.
  No production legacy state, run directory, or X request was created.
