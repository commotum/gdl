# 1-EVIDENCE

## Current Facts

- Production is stopped: tmux session `x` contains an idle bash shell, not an
  archive process.
- Visakanv state remains at modern timeline cursor `3_29116490825/`.
- The latest run is `20260720T023918Z-cf57e4`, status `stalled`. Its four final
  checkpoints all contain `3_29116490825/`; the three-window no-progress
  watchdog stopped it without claiming completion.
- The minimum returned record is ID `29116490825` at
  `2010-10-29 19:30:34` UTC. The observed transition is ID
  `402691293450240` on November 5 followed by sequential ID `29675373972` on
  November 4.
- Installed gallery-dl 1.32.4 applies `(id - 0x400000) | 0x3fffff` to this
  sequential ID and clears the server cursor. That operation is Snowflake
  pagination math, not a valid legacy-ID predecessor operation.
- Goal 1 context code/state is out of scope and has not been changed or run.
- A pre-existing `x.txt` modification was observed and left untouched. It no
  longer appears in the final `git status`; no command in this stage wrote to
  or restored that path.

## Updated Assumptions

- The current saved cursor is valid evidence of the furthest safe modern
  timeline boundary; it is not a valid mechanism for continuing through
  legacy sequential IDs.
- Older posts may be search-visible, but that remains unproven until Stage 2's
  bounded disposable diagnostic.
- A date-windowed server-cursor search remains the preferred candidate, not an
  implementation decision yet.

## Big Picture Objective

Freeze the exact production boundary and reproduce the unsafe legacy
pagination behavior offline before selecting or implementing a repair.

## Detailed Implementation Plan

- Preserve hashes and fields for the state, stopped manifest, and raw stream.
- Store only dates, numeric IDs, cursor strings, and terminal status in a
  non-sensitive transition fixture; exclude post text, profile data, media
  URLs, cookies, and headers.
- Add characterization tests for the exact ID discontinuity, installed
  max-ID behavior, repeated boundary, watchdog status, and safe cursor choice.
- Run offline focused/full tests and compatibility/whitespace checks.
- Make no production write, network request, process change, or implementation
  change.

## No-Cheating Checks

- The regression executes the installed gallery-dl paginator and proves the
  transformed legacy boundary differs from `legacy_id - 1`.
- The fixture records all four repeated checkpoints and `stalled`, so an empty
  or repeated response cannot be relabeled completion.
- A separate assertion proves the saved advanced checkpoint still defeats the
  stale shutdown cursor.
- No fixture contains post content, media locations, cookie values, or signed
  request data.

## Completion Requirements

- Record artifact hashes, modes, tmux/process status, and worktree status.
- Focused characterization tests pass against the installed pinned version.
- Full offline suite, pinned runner version, compile check, and
  `git diff --check` pass.
- Production state and processes remain unchanged.

## Stage Results

- Approval purpose: this stage freezes evidence and demonstrates the defect
  without contacting X or mutating the archive. The goal authorization permits
  proceeding, and no external side effect is required.
- Preserved artifact evidence:
  - `_state/state.json`: mode `0600`, SHA-256
    `98821e48e631989607bef3e917d334e70d7f169f6dee97659851065edf384f67`.
  - stopped `manifest.json`: mode `0600`, SHA-256
    `cc5e15fb28b226c00c6af8f18e243f522d53f1c3cef262494d398907da8fffee`.
  - `raw/timeline.posts.incomplete.jsonl`: mode `0600`, size 183,816,251
    bytes, SHA-256
    `8aeae69f01b544a192efa932b4e642c3efe881834d6ed854f7f9d8254b9ee4c6`.
- Before this stage, the repository baseline was 88 passing tests, runner
  version 1.32.4, and a clean `git diff --check`; only `x.txt` was modified
  outside the goal scaffold.
- Focused characterization:
  `uv run python -m unittest tests.test_archive_x_legacy -v` passed 3 tests.
- Full offline suite:
  `uv run python -m unittest discover -s tests -p 'test*.py'` passed 91 tests.
- `uv run python -m py_compile scripts/archive_x.py
  scripts/gallery_dl_x_runner.py`, pinned runner version 1.32.4, and
  `git diff --check` all passed.
- Post-test hashes match the preserved state and manifest hashes above; the raw
  stream hash is
  `8aeae69f01b544a192efa932b4e642c3efe881834d6ed854f7f9d8254b9ee4c6`.
- Final process check: tmux pane `x` is `bash` with PID 1847252 and is not dead.
  No archive was started. The production cursor remains
  `3_29116490825/` by unchanged state hash.
- Stage conclusion: the stalled loop is reproduced without weakening the
  watchdog, and the current cursor recovery remains correct. Stage 2 may now
  compare bounded pagination primitives without changing production state.
