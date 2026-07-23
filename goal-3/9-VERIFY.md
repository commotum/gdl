# 9-VERIFY

## Current Facts

- Stages 1–8 implemented the direct unified lifecycle, recovery, and UX.
- The expanded non-locking focused set currently passes 126 tests plus five
  direct legacy recovery tests.
- Three standalone legacy CLI tests could not previously acquire the real
  repository lock because an unrelated `airkatakana` archive is still running
  in tmux `x` from the pre-Goal-3 code.
- Production Visakanv hashes/inventory were frozen before implementation and no
  production request/write has been authorized.

## Updated Assumptions

- Full-suite verification must wait for the real lock owner to finish; killing
  it is outside this goal's authorization.
- Read-only production dry-run/status/hash/integrity checks can run only if they
  do not interfere with that writer and must be compared to the frozen facts.
- The static audit must distinguish expected gallery runner subprocesses from
  forbidden standalone legacy/context CLI orchestration.

## Big Picture Objective

Prove offline that the unified implementation preserves all modern, legacy,
context, media, storage, identity, privacy, and compatibility invariants.

## Detailed Implementation Plan

- Run all tests, runner fingerprints, compilation, and whitespace checks after
  the live lock is released.
- Audit direct call paths, lock ownership, pagination authorities, inner safety
  limits, context scope, media independence, and status aggregation.
- Exercise the option matrix and exact wrapper on isolated fixture archives.
- Audit changed files, modes, temporary artifacts, credential-like content,
  and generated manifests for redaction.
- Recheck production process/mount/state/dataset/context hashes read-only and
  compare against the Stage 1 freeze.
- Record any lock-delayed check explicitly rather than weakening the gate.

## No-Cheating Checks

- No test exclusions in the final suite.
- No production mutation or full backlog launch.
- No subprocess or text-search result is accepted without classifying whether
  it is the required gallery runner versus a forbidden phase CLI.
- Hash comparisons name the exact files/datasets being protected.

## Completion Requirements

- Entire repository suite passes with the real lock available.
- Compatibility, SQLite integrity, state hashes, permissions, secrets,
  artifacts, compilation, and diff checks pass.
- Every success metric in `0-plan.md` maps to direct test or audit evidence.
- Production remains unchanged except for the separately running preexisting
  archive's own authorized target.

## Stage Results

- Completed on 2026-07-22.
- The no-exclusions repository suite passed all 176 tests with
  `uv run python -m unittest discover -s tests`.
- Three legacy CLI mutation tests originally collided with the unrelated real
  repository lock. They now map only their lock files into their existing
  temporary fixture roots while still using real `flock`; the dedicated
  second-worker contention test and unified one-owner/two-lock tests remain
  unchanged. This made the suite environment-independent without bypassing
  production lock behavior.
- Modern and legacy gallery runner version/source fingerprints passed against
  gallery-dl 1.32.4.
- Static audit found no standalone legacy/context CLI subprocess handoff and no
  nested lock in the unified call path. The only main subprocesses are local
  compatibility checks and the required gallery runner.
- Inner guards remain explicit: legacy request/walk/attempt/leaf/window bounds;
  context attempts, leases, retry delay, fairness quantum, depth, disk-space,
  and HTTP pacing; optional outer budgets do not replace them.
- Option/failure audit added guards for existing-legacy modern failure,
  retry-only identity failure, full-rescan domain clamping, exact initialized
  stall acceptance, actionable work coexisting with manual review, legacy
  runner mismatch, multi-user modern-first ordering, and waiting-invocation
  reconciliation.
- A separate advanced `--modern-max-posts` rollout bound now makes the planned
  all-phase production smoke hard-bounded. It can continue into legacy/context
  only when legacy is already initialized; a fresh/unproven archive remains a
  limited modern-only result. The full suite was rerun after this addition.
- Read-only mount detection now uses `statvfs` for both explicit/default roots.
  Dry-run reports the filesystem state, and a real run is proven to stop before
  invocation creation, archive writes, or lock acquisition when mounted `ro`.
- Exact production dry-run reported: incremental modern head; legacy pending at
  `2010-10-29T00:00:00Z` toward the `2008-10-21T12:01:00Z` account floor; two
  shared pending media items; absent context DB requiring authoritative
  bootstrap. It made no request/write.
- Frozen Visakanv hashes remained exactly unchanged after dry-run and all
  offline checks: state `47f4f2c...92a38`, posts dataset
  `fee36363...405c`, and pre-init backup `98821e48...f67`; no context DB exists.
- Compilation of all archive/runner modules, `git diff --check`, secret-pattern
  scan, artifact scan, file-mode audit, and direct wrapper help passed.
- The preexisting `airkatakana` archive remained live and untouched throughout;
  it continued to prove the real lock excludes other writers.
