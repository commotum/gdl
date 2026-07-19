# Goal 1 Execution Loop

Use this protocol for every stage in `goal-1/0-plan.md`. The plan is the
authoritative strategy document; stage files are evidence-bearing working
records, not substitutes for keeping the plan current.

## Repeatable Loop

1. Sync current state with actual files and tests. Inspect `git status`, the
   relevant implementation, existing tests, runtime state formats, and prior
   stage results. Preserve unrelated user changes.
2. Update `goal-1/0-plan.md` with current facts before starting the next stage.
   Correct stale assumptions explicitly rather than silently working around
   them.
3. Select the first incomplete stage whose prerequisites are satisfied.
4. Create or refresh `goal-1/[INDEX]-[SHORTHAND].md` from the template below.
5. Implement only that stage. If new evidence changes architecture or safety
   requirements, stop broad implementation and update the plan first.
6. Add verification and no-cheating checks that exercise the actual invariant,
   including negative and failure paths.
7. Run focused tests, full verification, and whitespace/diff checks appropriate
   to the repository. Normal tests must be network-free. Live X requests need
   explicit user approval and belong only to the approval-gated smoke stage.
8. Record commands, outputs, failures, and conclusions in the stage file.
9. Fold verified results, corrected assumptions, new risks, and remaining work
   back into `goal-1/0-plan.md`.
10. Continue toward the original objective. If stopping for the session, leave
    the goal resumable with current evidence, the exact next action, unblock
    steps, retry state, and assumptions that still need challenge.

## Invariants

- Do not narrow the user's objective without saying so.
- Do not mark a stage complete without evidence.
- Do not use tests or green checks as evidence unless they cover the stated
  requirement and relevant failure path.
- Prefer small, low-complexity stages that narrow uncertainty.
- Convert blockers into work items: decompose them, route around them, or turn
  them into proof and verification tasks.
- Preserve the distinction between implementation, verifier, diagnostic, and
  fallback paths.
- Do not make live network calls in ordinary tests or to compensate for weak
  fixtures.
- Do not start the historical context backfill without separate explicit user
  authorization.
- Do not let context code read or write timeline cursor authority except as a
  read-only discovery input where explicitly designed and tested.
- Do not weaken pacing, locks, retries, identity guards, credential hygiene,
  filesystem checks, or gallery-dl compatibility enforcement for convenience.
- Do not use conversation expansion or whole-thread capture as a substitute
  for explicit ancestor traversal.
- Do not let media success define metadata/graph success.
- Do not repurpose gallery-dl's download archive as the context queue.
- Do not assume the external `sqlite3` CLI exists; tests and operations use
  Python's `sqlite3` support.
- Prefer harmless duplicate attempts over any ordering that can lose a
  discovered edge or captured observation.
- Inspect the dirty worktree before every edit and preserve unrelated changes.

## Repository Verification Baseline

Adapt commands when current-state inspection proves a different command is
authoritative, and record the exact commands used.

```bash
git status --short
git diff --check
uv run python -m unittest discover -s tests -p 'test*.py'
```

Focused tests should target the stage's module or test class first. The full
test command remains required before stage completion when the stage changes
shared archive behavior. Dry-runs must be separately checked for zero network
and zero archive-state mutation. Use fake clocks and mocked request boundaries
instead of real sleeps in automated tests.

## Stage File Template

```markdown
# [INDEX]-[SHORTHAND]

## Current Facts

- Facts from current code, tests, docs, and previous stage results.

## Updated Assumptions

- Assumptions that still look valid.
- Assumptions that changed.
- Assumptions that need tests before being trusted.

## Big Picture Objective

- Restate the stage objective, adjusted for current facts.

## Detailed Implementation Plan

- Concrete code/doc/test changes for this stage.
- Files expected to change.
- New tests or commands required.

## No-Cheating Checks

- Explicit checks proving the implementation does not route through forbidden fallback paths.

## Completion Requirements

- Requirement-by-requirement checks.
- Required test commands.
- Documentation updates required.

## Stage Results

- Fill in at the end of the stage.
- Include tests run and outcomes.
- Include what was learned.
- Include what should change in `0-plan.md` before the next stage.
```

## Required Stop/Handoff State

If a stage cannot finish in one session, its stage file must contain:

- the last known-good commit/worktree facts without altering unrelated work;
- completed and incomplete requirements;
- the exact failing test, command, or invariant;
- whether any temporary database, lock, lease, process, or live smoke state
  exists and how to inspect it safely;
- the next smallest diagnostic or implementation action;
- any external approval still required;
- assumptions most likely to be wrong.

Never describe a stage as merely “mostly done.” Record requirement-level state
and evidence so another session can resume without repeating risky work.
