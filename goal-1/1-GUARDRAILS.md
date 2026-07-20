# 1-GUARDRAILS

## Current Facts

- The production Visakanv process is stopped and the tmux pane is an idle shell.
- The interrupted endpoint retained raw JSONL and the proven cursor
  `3_1173685814485643265/`; the older state cursor must not be trusted blindly.
- The main archive uses two exclusive locks, stable numeric identity, private
  permissions, mounted-root validation, conservative sleeps, bounded retries,
  immutable run evidence, and a version/fingerprint-pinned gallery-dl shim.
- Timeline context is currently excluded by a numeric-author post filter.
- gallery-dl 1.32.4's individual extractor has a focal-only path when
  conversation and quote expansion are disabled; this behavior must be pinned
  by generated config and tests.
- The existing worktree changes are the cursor-failure fix and its tests.

## Updated Assumptions

- A separate explicit CLI is the safest control surface for context seeding,
  resolving, media, status, integrity, and export.
- The context network worker must share the main archive locks so timeline and
  context requests cannot use the same X credentials concurrently.
- Raw context observations can live transactionally in the context database;
  deterministic JSONL exports provide immutable/auditable interchange views.
- Static URL files are not the scheduler. The SQLite worker chooses again
  after every observation so it can continue down the active ancestor chain.

## Big Picture Objective

- Establish executable boundaries that prevent context implementation from
  weakening or silently bypassing the main archive's safety model.

## Detailed Implementation Plan

- Add a dedicated `archive-x-context` CLI and module rather than overloading
  timeline behavior.
- Add offline configuration tests proving focal-only, no conversation, no
  quoted-source, and metadata-only defaults.
- Reuse archive-root, cookie, lock, identity, atomic-write, and version checks.
- Keep production backfill and media opt-in with explicit bounded commands.
- Record error classes and network pacing without response secrets.

## No-Cheating Checks

- No `expand`, `showreplies`, conversation traversal, proxy, concurrency, or
  unbounded-run default may appear in the context configuration.
- No context table may be added to `_state/downloads.sqlite3`.
- No ordinary unit test may open the production archive or contact X.
- No context failure path may call `update_timeline_state`.

## Completion Requirements

- Focal-only configuration and isolation tests exist and pass.
- Every existing main-archive safety behavior reused or intentionally kept
  separate is named in code/docs.
- The implementation plan is updated when installed-source evidence differs.

## Stage Results

- Current-state and interruption evidence were recorded before implementation.
- Production is stopped; no live request or production context write occurred.
- Subsequent stages will supply the executable characterization tests and fold
  any corrected assumptions back into `0-plan.md`.
