# 3-DETECT

## Current Facts

- Stage 2 requires a pure three-way transition classifier and a separate
  `modern_head` checkpoint while preserving top-level historical `resume`.
- Existing `initialization_plan()` already stale-binds state, dataset count,
  oldest post/cursor, profile identity/floor, stopped source manifest hash, and
  repost policy, but it intentionally supports manual planning from broader
  stopped states than automatic detection may accept.
- The known production source manifest contains the strict automatic evidence:
  unrestricted `date_after: null`, `status: stalled`, exact watchdog failure
  stage, three stalled cycles, no interruption, no other error, raw records,
  non-complete metadata, and resume cursor matching the legacy record.
- The active unrelated archive does not import `archive_x_legacy.py`; focused
  pure-function tests can proceed without touching its runner or lock.

## Updated Assumptions

- Automatic classification should wrap—not weaken—the existing plan. The plan
  remains the stale-guarded state constructor; the classifier adds stricter
  eligibility evidence.
- A returned metadata timestamp before Twitter's exact Snowflake epoch is the
  strongest domain signal. It is safe for classification and is never used to
  synthesize legacy pagination.
- Existing initialized states need an idempotent `modern_head` migration tied
  to their hash-bound source manifest and exact pre-init backup.

## Big Picture Objective

Implement and test strict automatic transition classification, atomic legacy
setup, and separate modern-head state without integrating network orchestration.

## Detailed Implementation Plan

- Add constants and validation for schema-versioned `modern_head` state.
- Add a pure classifier returning `proven`, `not_applicable`, or `ambiguous`
  with safe reason codes and evidence hashes.
- Require exact watchdog/source/raw/time/identity/plan agreement for `proven`.
- Add an idempotent helper deriving `modern_head` from the exact legacy source
  manifest.
- Add an atomic automatic initializer that verifies/creates the exact backup,
  initializes legacy state through the existing constructor, adds modern-head
  state, and refuses stale/missing evidence.
- Add positive, negative, idempotency, preservation, and fault tests.

## No-Cheating Checks

- Do not reinterpret pre-Snowflake IDs into dates or use them for pagination.
- Do not classify the existing broad manual plan as automatic proof by itself.
- Do not clear or move `resume`.
- Do not make network requests or touch production in tests.

## Completion Requirements

- Exact transition fixture proves; every weak/failure variant does not.
- Automatic initialization writes an exact private backup before state.
- Existing initialized state migrates only with matching source/backup.
- Write faults preserve the prior state and prevent handoff.
- Focused tests and static checks pass; full suite remains deferred only if the
  real lock holder is still active.

## Stage Results

- Completed on 2026-07-22.
- Added `classify_legacy_transition()` with exact unrestricted watchdog,
  manifest, raw cursor, returned timestamp, stable identity, Snowflake epoch,
  and stale-plan agreement. It returns explicit proven/not-applicable/ambiguous
  decisions and never writes or contacts X.
- Added schema-validated `modern_head`, exact source-manifest derivation,
  backup-path verification, and `automatic_initialize_legacy()` for use under
  the future outer locks.
- Six focused tests cover exact proof, eleven weak/failure mutations,
  cursor/state preservation, private exact backup, idempotency, backup-write
  failure, state-write failure, and missing-backup refusal.
- Focused command passed:
  `uv run python -m unittest discover -s tests -p 'test_archive_x_legacy.py' -k AutomaticLegacyTransitionTests`.
- Python compilation and `git diff --check` pass.
- A read-only production check derived Visakanv's modern-head baseline as
  `2026-07-20T02:39:18Z` from source run
  `20260720T023918Z-cf57e4`; classification correctly reports
  `legacy_already_initialized`. Production was not written.
- The full suite remains pending only because the unrelated live archive holds
  the real repository lock; the new focused tests do not need it.
