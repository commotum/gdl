# Goal 4 Execution Loop

Use this loop to implement `goal-4/0-plan.md` without weakening the original
objective or the archive's safety properties.

## Repeatable Loop

1. Sync current state with actual files, live-safe read-only state, git diff,
   and tests.
2. Update `goal-4/0-plan.md` with current facts before starting the next stage.
3. Select the first incomplete stage.
4. Create or refresh `goal-4/[INDEX]-[SHORTHAND].md` from the stage template.
5. Implement only that stage.
6. Add verification and no-cheating checks.
7. Run focused tests, full verification, and whitespace/diff checks appropriate
   to the repo.
8. Record results in the stage file.
9. Fold results back into `goal-4/0-plan.md`.
10. Continue toward the original objective. If stopping for the session, leave
    the goal in a resumable state with current evidence, next experiments,
    unblock actions, and assumptions to challenge.

## Invariants

- Do not narrow the user's objective without saying so.
- Do not mark a stage complete without evidence.
- Do not use tests or green checks as evidence unless they cover the
  requirement.
- Prefer small, low-complexity stages that narrow uncertainty.
- Convert blockers into work items: decompose them, route around them, or turn
  them into proof and verification tasks.
- Preserve the distinction between implementation, verifier, diagnostic, and
  fallback paths.
- Treat archive SQLite/state/manifests and engine results as truth; terminal
  prose and dashboard snapshots are derived presentation.
- Reporting must issue zero X requests and make zero queue, cursor, window, or
  completion decisions.
- Preserve locks, pacing, interruption, identity, storage, redaction,
  checksums, and fail-closed behavior.
- Never run expensive file-tree or whole-graph scans at display refresh
  frequency.
- Never show an unlabeled global percentage or ETA for a dynamic/unknown
  denominator.
- Never make tmux necessary for correctness, recovery, or status.
- Never mutate an unrelated tmux pane or assume pane ownership.
- Preserve complete raw logs even when interactive console output becomes
  quieter.
- Do not start or restart a production archive merely to verify presentation;
  use fixtures first and obtain separate authorization for a bounded smoke.
- Preserve pre-existing uncommitted changes and recheck the live archive before
  editing executable files.

## Verification Ladder

For every stage, use the smallest applicable ladder and record exact commands:

1. Static schema/type/format checks.
2. Focused unit tests for the changed layer.
3. Golden rendering and deterministic-clock tests where applicable.
4. Integration tests using temporary archive roots and isolated tmux servers.
5. Fault injection for interruption, stale/corrupt input, and renderer death.
6. Full repository test discovery.
7. Compatibility fingerprint verification.
8. `git diff --check` and deliberate diff review.
9. Separately authorized bounded live smoke only after fixture proof.

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

- Explicit checks proving the implementation does not route through forbidden
  fallback paths.

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

## Stop/Resume Handoff

Before stopping:

- Record the last completed stage and the first incomplete requirement.
- Record current git status and distinguish goal changes from pre-existing
  changes.
- Record whether any archive process is live; do not infer from stale
  invocation JSON alone.
- Record tests run, failures, and any verification intentionally deferred.
- Record current metric/ETA assumptions that still need production evidence.
- Record exact tmux ownership/cleanup state if Stage 5 has begun.
- Leave the next action small, explicit, and safe.

