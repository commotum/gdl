# 7-RECOVERY

## Current Facts

- All three engines commit their own authority independently, and the unified
  coordinator now calls them directly under one outer lock.
- Modern run manifests, legacy window manifests/state, and context SQLite
  transactions already have focused recovery tests.
- The combined invocation is currently written only after all follow-up phases
  return, so an interrupt or phase exception can leave no top-level record of
  already durable work.
- A per-target context seed/worker/export exception currently aborts later
  targets even when the failure is not a global authentication stop.
- Legacy migration currently happens after a new run directory is created,
  which can leave an empty run directory if validation fails.

## Updated Assumptions

- The combined invocation should be a checkpointed readout, never a new state
  authority; engine state remains sufficient to resume after hard process loss.
- Ordinary per-target failures should finalize that target truthfully and let
  independent targets continue. A typed authentication/global-stop error may
  still stop all network work.
- Catching `KeyboardInterrupt` must release the current lease/window through
  existing engine logic and atomically finalize the combined invocation.

## Big Picture Objective

Prove the unified lifecycle remains gap-free and resumable across interrupts,
phase failures, partial media, migration faults, and manual-review states.

## Detailed Implementation Plan

- Create and atomically checkpoint the combined invocation before network work,
  after modern results, and after each unified phase boundary.
- Finalize it as interrupted/failed without granting it cursor/frontier/queue
  authority.
- Add a structured checkpoint callback and per-target error capture to the
  direct orchestrator.
- Preserve global-stop semantics with a typed context authentication error.
- Move legacy-state preparation before new modern run-directory creation.
- Add fault injection for modern-to-transition, legacy-to-context, seed,
  context metadata/media, export, final checkpoint, and `Ctrl-C` boundaries.
- Prove reruns skip/reuse committed work, manual review is never reset, and
  historical/modern-head/legacy/context authorities remain separate.

## No-Cheating Checks

- Recovery tests restart the ordinary orchestrator, not specialized repair
  commands or hand-edited state.
- Invocation checkpoints contain phase readouts only and are never consulted
  to advance engine state.
- Exceptions after a durable commit do not delete or roll back that commit.
- No test replaces all engines with a single always-success stub.

## Completion Requirements

- Every phase boundary has an interrupt/failure assertion or is covered by an
  existing engine transaction test cited in the results.
- A later ordinary invocation resumes uncertain work without operator cursors,
  windows, or post counts.
- Independent users continue after per-target failures; typed global auth
  failures stop safely.
- Combined manifests are atomically truthful for success, partial, failure,
  and interruption.
- Focused recovery suites, compilation, and diff checks pass.

## Stage Results

- Completed on 2026-07-22.
- Combined invocations are created before lock acquisition and atomically
  checkpointed after each modern target and unified phase. Success, limited,
  failed, and interrupted terminal states now retain phase-by-phase evidence.
- `Ctrl-C` during modern work records the active target as interrupted;
  `Ctrl-C` after a unified phase preserves the last completed phase checkpoint.
  Engine manifests/state/SQLite remain the only resume authorities.
- Legacy-state validation/migration now occurs before a new modern run
  directory is created, eliminating empty abandoned runs on migration faults.
- Per-target transition, legacy, shared-media, seed, context worker, and export
  errors become truthful failed phase results without starving independent
  users. A typed `ContextAuthenticationError` remains a deliberate global
  network stop.
- An export-fault/rerun fixture proves the committed context source ledger is
  retained and skipped idempotently by the next ordinary orchestration.
- Manual-review context targets remain untouched and make the combined result
  `manual_review`; no automatic retry/reset path was introduced.
- Modern-head recovery tests prove historical cursors cannot cross domains.
  Existing context transaction/lease/media tests prove interrupted leases are
  released or reclaimed, captured metadata survives media failure, and seed
  transactions roll back atomically.
- Five direct legacy crash/frontier/manual-review recovery tests passed,
  proving replay/deduplication after dataset merge, exact manifest recovery
  after state commit, replay after frontier-write failure, guarded retry, and
  bounded attempt escalation.
- Verification: the expanded focused set passed 122 tests, the five additional
  legacy recovery tests passed, Python compilation passed, and
  `git diff --check` passed.
- The live `airkatakana` process and all production Visakanv artifacts remained
  untouched.
