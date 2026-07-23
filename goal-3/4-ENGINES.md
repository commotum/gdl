# 4-ENGINES

## Current Facts

- Legacy execution was coupled to argparse only through its required
  `args.windows` and related option attributes; its state machine already ran
  directly without acquiring locks internally.
- Context seed/run/media/export functions were already directly callable, but
  `run_worker()` required an integer `max_posts` and treated no immediately
  claimable row as termination even when retries were delayed.
- The active real archive still owns the repository lock, so lock-owning CLI
  tests cannot provide a clean baseline yet.

## Updated Assumptions

- A typed immutable legacy options object is sufficient separation; the future
  orchestrator need not fabricate argparse or spawn the CLI.
- Context can retain its structured keyword API if no-budget behavior consults
  SQLite queue truth rather than just `claim()`.
- Specialized CLI limits can become optional without changing bounded inner
  operations or recovery meaning.

## Big Picture Objective

Expose reusable no-budget legacy and context engines while preserving all
existing state, retry, evidence, and maintenance-command behavior.

## Detailed Implementation Plan

- Add validated `LegacyRunOptions` with optional `max_root_windows`.
- Refactor `run_legacy_archive()` to accept that object directly.
- Make omitted root limit run toward terminal legacy state; make an explicit
  bound return `limited` after exact committed roots.
- Keep standalone `run --windows N` as an optional advanced adapter.
- Make context `max_posts` optional for metadata and media workers.
- Add SQLite `work_availability()` so a no-budget worker waits interruptibly
  for delayed retry/lease eligibility and stops only at terminal queue state.
- Keep standalone `--max-posts` optional and preserve every existing inner
  retry, timeout, lease, pacing, depth, fairness, and disk-space limit.

## No-Cheating Checks

- Neither engine acquires outer archive locks or invokes a CLI subprocess.
- No request, walk, split, retry, depth, lease, or timeout limit was removed.
- Explicit diagnostic bounds and no-budget execution share identical inner
  code and durable state transitions.
- No production path was invoked.

## Completion Requirements

- Synthetic no-budget legacy work reaches its exact floor.
- Explicitly bounded legacy work returns resumable `limited` state.
- Synthetic no-budget context work closes a multi-level ancestor chain and
  waits through a bounded delayed retry.
- Both standalone parsers accept omitted advanced limits.
- Context/full focused tests, legacy non-lock tests, compilation, and diff
  checks pass; real-lock CLI tests remain explicitly pending.

## Stage Results

- Completed on 2026-07-22.
- Added immutable validated `LegacyRunOptions`; shared legacy execution no
  longer accepts argparse state. `max_root_windows=None` runs to floor, while
  explicit bounds return `limited` without changing the frontier.
- Standalone legacy `--windows` and context `--max-posts` are now optional
  advanced flags.
- Context no-budget workers now inspect authoritative SQLite availability,
  wait in interruptible maximum-60-second slices for delayed retry/lease work,
  and retain all bounded attempts and pacing.
- All 24 context tests pass, including new no-budget chain, delayed retry, and
  optional parser tests.
- Five legacy orchestration tests pass, including exact no-budget completion
  and optional parser behavior. Twenty-three tested legacy non-state paths
  passed; one retry CLI test remained blocked solely by the real archive lock.
- Compilation and `git diff --check` pass.
