# 7-RECOVERY

## Current Facts

- Active windows are persisted before network access and are replayed from the
  fixed query rather than resuming opaque cursors.
- Dataset merge precedes state commit, so crashes can create duplicates but
  should not create gaps.
- A crash immediately after state commit can leave a running manifest whose
  window fields do not yet say `state_committed`; nested state contains the
  authoritative completed-window hashes needed to reconcile it.

## Updated Assumptions

- Recovery can finalize only the one uncertain committed window when state
  `last_completed_window` exactly matches its ID and canonical hash.
- All other abandoned active windows must remain replayable and be labeled
  interrupted, never inferred successful from raw presence alone.

## Big Picture Objective

Prove and implement replay-safe behavior across every durability boundary,
plus an explicit stale-guarded operator retry for manual-review windows.

## Detailed Implementation Plan

- Reconcile abandoned legacy manifests against authoritative nested state and
  immutable canonical hashes.
- Add pure guarded manual-review retry that returns the same frontier to
  pending and records operator provenance.
- Inject faults before/during walks, before raw finalization, during/after
  dataset merge, before/after state commit, and during manifest finalization.
- Prove repeated recovery is idempotent and stale cursor log lines have no
  state authority.

## No-Cheating Checks

- Raw existence without two-walk/merge/state evidence cannot advance.
- Recovery never copies gallery-dl cursor text into legacy state.
- A committed window finalizes only on exact window/hash guards.
- Retry requires the exact manual-review window ID and never edits the modern
  cursor.

## Completion Requirements

- All fault points yield replay or exact reconciliation, with duplicates at
  worst.
- Bounded window attempts lead to manual review, not an infinite loop.
- Existing cursor recovery regressions remain green.
- Full suite and preservation audits pass.

## Stage Results

- Active-window restarts increment a bounded attempt counter and always replay
  the fixed leaf without an opaque cursor. The third failed window attempt in
  the default policy enters manual review without moving the frontier.
- Abandoned legacy manifests finalize as successful only when authoritative
  `last_completed_window` state matches numeric account, window ID/bounds, and
  canonical raw hash and the retained file re-hashes correctly. All others are
  marked interrupted and remain replayable. Repeated recovery is idempotent.
- Added exact guarded `retry --window-id ... --reason ...`; it resets only that
  manual-review frontier and records operator provenance while preserving the
  modern cursor.
- Fault tests cover interrupted/incomplete walks, a crash after dataset merge,
  a failed atomic frontier write, a crash after successful state commit but
  before manifest finalization, replay/deduplication, exact reconciliation,
  and bounded window attempts.
- Focused recovery/state/fetch suite passed 36 tests and the full suite passed
  124 tests. The two prior normal cursor regressions remain green.
