# 6-ORCHESTRATE

## Current Facts

- Shared legacy/context engines and complete seeding are now directly callable
  under an outer lock.
- The main selector still executes top-level historical `resume` even when
  legacy state exists; this must change before automatic resumption.
- Main currently owns both locks around its entire target loop and writes a
  shallow invocation summary after releasing them.
- A real archive remains active using the already-loaded pre-Goal-3 main
  module. New orchestration can be isolated in a module that process never
  imported; runner files remain untouched.

## Updated Assumptions

- Keep `archive_user()` as the modern engine, but add a modern-head selection/
  commit mode and remove its subprocess seed hook.
- A new `archive_x_unified.py` should coordinate direct engines and return
  structured per-phase results to main.
- The immutable stopped modern manifest must remain unchanged after automatic
  initialization because its SHA-256 is legacy source provenance. Transition
  acceptance belongs in the combined invocation, not by rewriting that run.

## Big Picture Objective

Make the normal wrapper directly orchestrate modern, automatic transition,
legacy, shared media, context metadata, context media, and export under one
outer lock.

## Detailed Implementation Plan

- Add separate modern-head selection/commit behavior and idempotent migration
  for existing legacy states.
- Add optional advanced rollout limits to the main parser.
- Implement direct transition acceptance and phase orchestration in a new
  module without subprocesses or nested locks.
- Run modern for every target before historical/context backlog work.
- Add round-robin legacy and context scheduling for multiple targets.
- Retry shared pending media after legacy metadata enqueueing.
- Seed, drain, media-process, integrity-check, and export context automatically.
- Record structured phase statuses in the top-level invocation summary.
- Remove/deprecate the old context-seed subprocess behavior.

## No-Cheating Checks

- The exact wrapper command must exercise direct functions, not shell commands.
- Historical resume, modern head, legacy frontier, and SQLite queue remain
  independently committed.
- No specialized limit is required by the normal parser path.
- The active unrelated process and production archive remain untouched.

## Completion Requirements

- Integration fixtures prove the exact wrapper lifecycle with no phase flags.
- Existing legacy states use modern-head mode and preserve historical resume.
- New exact transitions auto-initialize then hand off.
- Context seed/run/media/export happen automatically and other-author parents
  survive.
- Multiple targets receive modern work first and backlog fairness afterward.
- Locks are acquired once; tests prove no subprocess/nested-lock path.
- Focused tests, available suites, compilation, and diff checks pass.

## Stage Results

- Completed on 2026-07-22.
- `scripts/archive_x_unified.py` now coordinates transition acceptance, legacy
  scheduling, shared-media recovery, authoritative context seeding, metadata
  closure, context media, integrity, and export through direct engine calls.
- `scripts/archive_x.py` retains the sole repository/archive lock pair and
  records structured per-target phase results in the combined invocation.
- Normal parser defaults require no legacy-window or context-post budgets;
  optional bounds remain advanced rollout/diagnostic controls.
- Existing legacy archives now use a separate `modern_head` selector and
  commit path. Interrupted/download-only recovery is also domain-filtered:
  historical manifests can never populate `modern_head.active`, and head
  manifests can never overwrite the preserved historical `resume` cursor.
- A newly proven exact transition initializes once, preserves its source
  manifest and cursor evidence, performs one modern-head pass, then hands off
  to legacy work.
- Multiple users receive all modern passes first, then one legacy root window
  or one context fairness quantum per round.
- Integration coverage proves the exact no-phase-flag path invokes no legacy
  or context CLI subprocess, acquires only the two outer locks, recursively
  captures a parent and parent-of-parent authored by other accounts, and
  exports the resulting graph.
- Verification: `uv run python -m unittest tests.test_archive_x
  tests.test_archive_x_recovery tests.test_archive_x_context
  tests.test_archive_x_unified` passed 96 tests. Focused transition,
  modern-head recovery, multi-user fairness, compilation, and
  `git diff --check` also passed.
- The unrelated `airkatakana` archive remained live and untouched; no
  production Visakanv request or write occurred.
