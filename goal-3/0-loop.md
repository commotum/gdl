# Goal 3 Execution Loop

Use this protocol while implementing `goal-3/0-plan.md`.

## Repeatable Loop

1. Sync current state with actual files and tests.
2. Update `0-plan.md` with current facts before starting the next stage.
3. Select the first incomplete stage.
4. Create or refresh `goal-3/[INDEX]-[SHORTHAND].md` from the stage template.
5. Implement only that stage.
6. Add verification and no-cheating checks.
7. Run focused tests, full verification, and whitespace/diff checks appropriate
   to the repository.
8. Record results in the stage file.
9. Fold results back into `0-plan.md`.
10. Continue toward the original objective. If stopping for the session, leave
    the goal resumable with current evidence, next experiments, unblock actions,
    and assumptions to challenge.

## Invariants

- Do not narrow the user's objective without saying so.
- Do not mark a stage complete without evidence.
- Do not use tests or green checks as evidence unless they cover the actual
  requirement.
- Prefer small, low-complexity stages that narrow uncertainty.
- Convert blockers into work items: decompose them, route around them, or turn
  them into proof and verification tasks.
- Preserve the distinction between implementation, verifier, diagnostic, and
  fallback paths.
- The target UX is exactly `uv run scripts/archive-x --user USERNAME`; normal
  completion must not require separate legacy/context commands or counts.
- Preserve Goal 1's ancestor-only SQLite context semantics and Goal 2's
  UTC-window legacy semantics. Unify orchestration, not pagination domains.
- Do not replace internal windows/queues with unbounded requests. Remove
  mandatory operator budgets, not request, retry, depth, leaf, timeout, pacing,
  lease, or media-attempt limits.
- Do not call the standalone legacy/context CLIs as subprocesses or reacquire
  the same archive locks inside the unified process.
- Do not call queue seeding “context completion.” Resolve recursively to root
  or an explicit unavailable/depth/manual-review boundary.
- Seed context from every durable modern and legacy authored reply; preserve
  other-author parents and exclude siblings, descendants, and quoted sources.
- Preserve modern cursor evidence, legacy frontier, SQLite queue truth,
  pending media, raw runs, datasets, private modes, and credential redaction.
- Metadata completion and media completion remain independent and separately
  reported.
- Do not start Visakanv's remaining production legacy or context backlog during
  implementation/offline verification. Bounded smoke and full continuation
  require distinct authorization.
- Before any live request or production write, recheck hashes, process state,
  mount, locks, identity, SQLite integrity, and every advanced bound.

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

## Final Verification Gate

Before declaring the goal complete:

- Re-read the user's three numbered requirements and demonstrate each through
  the exact unified wrapper command.
- Prove no mandatory `--windows`, `--seed-reply-context`, `--max-posts`, or
  phase-specific follow-up invocation remains in normal operation.
- Prove automatic legacy detection fails closed on all negative fixtures.
- Prove internal legacy enumeration remains contiguous and repeat-confirmed.
- Prove all modern and legacy authored replies seed the context graph.
- Prove the context engine resolves ancestor chains rather than only creating
  queue entries, and preserves other-author parent posts.
- Prove metadata/media independence and recoverable pending assets.
- Prove repeated ordinary invocations update modern history and resume only
  incomplete legacy/context/media work.
- Prove one lock owner excludes standalone writers without self-deadlock.
- Run full tests, compatibility checks, compilation, SQLite integrity, state
  hashes, credential/permission/artifact audits, and `git diff --check`.
- Record bounded production rollout scope and leave the remaining backlogs
  stopped unless separately authorized.
- Carry every unresolved issue forward explicitly rather than weakening the
  original objective.

