# Goal 2 Execution Loop

Use this protocol for every stage in `goal-2/0-plan.md`. The plan is the
authoritative strategy document; stage files are evidence-bearing working
records and must not replace updates to the plan itself.

## Repeatable Loop

1. Sync current state with actual files and tests. Inspect `git status`, tmux
   and process state, relevant implementation and installed gallery-dl source,
   tests, production state/manifests/logs/raw boundaries, and prior stage
   results. Preserve unrelated user changes.
2. Update `goal-2/0-plan.md` with current facts before starting the next stage.
   Correct stale assumptions explicitly rather than silently working around
   them.
3. Select the first incomplete stage whose prerequisites and approval gates are
   satisfied.
4. Create or refresh `goal-2/[INDEX]-[SHORTHAND].md` from the template below.
5. Implement only that stage. If evidence changes pagination, state, or safety
   architecture, pause implementation and update the plan first.
6. Add verification and no-cheating checks for the actual requirement,
   including negative, crash, repeated-page, and ambiguous-response paths.
7. Run focused tests, full relevant verification, compatibility checks,
   permission/credential audits, and whitespace/diff checks. Ordinary tests
   must be offline; dry-runs must be write-free.
8. Record exact commands, results, failures, and conclusions in the stage file.
9. Fold verified results, changed assumptions, new risks, and remaining work
   back into `goal-2/0-plan.md`.
10. Continue toward the original objective. If stopping, leave the goal
    resumable with current evidence, exact next action, process/lock/state
    status, approval needs, retry information, and assumptions to challenge.

## Invariants

- Do not narrow the user's objective without saying so.
- Do not mark a stage complete without evidence.
- Do not use tests or green checks as evidence unless they exercise the stated
  requirement and relevant failure path.
- Prefer small, low-complexity stages that narrow uncertainty.
- Convert blockers into work items: decompose them, route around them, or turn
  them into proof, diagnostics, alternate pagination designs, or verifiers.
- Preserve the distinction between implementation, verifier, diagnostic,
  migration, and fallback paths.
- Never treat a repeated/empty/error response as historical completion.
- Never apply Snowflake timestamp arithmetic to a confirmed sequential ID.
- Never advance a date frontier until raw metadata is durable and merged.
- Prefer replay and deduplication over a potentially skipped time interval.
- Do not disable, bypass, or weaken the no-progress watchdog.
- Keep one worker and the existing shared archive locks; no concurrency,
  proxies, header spoofing, or rate-limit evasion.
- Keep stable numeric identity authoritative and handle-based queries
  subordinate to it.
- Keep metadata completion independent of pending media completion.
- Do not alter or start Goal 1's reply-context subsystem.
- No live diagnostic, production initialization, production write, or restart
  occurs without the explicit approval required by the current stage.
- Do not initialize legacy state automatically on install or ordinary dry-run.
- Preserve the existing stage-3 cursor, run artifacts, datasets, pending media,
  and unrelated dirty worktree changes.
- Do not claim “all historical tweets” when the evidence establishes only
  “all posts returned by X for completed windows.”

## Repository Verification Baseline

Adapt these only when current evidence identifies the authoritative command,
and record the exact alternative:

```bash
git status --short
git diff --check
uv run python -m py_compile scripts/archive_x.py scripts/gallery_dl_x_runner.py
uv run python scripts/gallery_dl_x_runner.py --version
uv run python -m unittest discover -s tests -p 'test*.py'
```

Production inspection must begin read-only. Record hashes or exact guarded
fields before any later approved migration. Automated tests may not access the
production archive or network. Use fixtures, fake clocks, mocked subprocesses,
and fault injection. Live diagnostics belong only to approval-gated stages and
must have disposable output plus a hard request/window bound.

## Stage File Template

```markdown
# [INDEX]-[SHORTHAND]

## Current Facts

- Facts from current code, installed source, tests, production evidence, and
  previous stage results.

## Updated Assumptions

- Assumptions that still look valid.
- Assumptions that changed.
- Assumptions that need tests or a bounded diagnostic before being trusted.

## Big Picture Objective

- Restate the stage objective, adjusted for current facts.

## Detailed Implementation Plan

- Concrete code/doc/test changes for this stage.
- Files expected to change.
- New tests, diagnostics, or commands required.
- Approval boundary and production impact, if any.

## No-Cheating Checks

- Explicit checks proving the implementation does not skip windows, infer
  false completion, reinterpret legacy IDs as Snowflakes, weaken safety, or
  route through a forbidden fallback.

## Completion Requirements

- Requirement-by-requirement checks.
- Required focused/full test commands and audits.
- Documentation and handoff updates required.

## Stage Results

- Fill in at the end of the stage.
- Include exact tests/commands and outcomes.
- Include what was learned and what changed in the plan.
- Include production process/state/lock status and the next safe action.
```

## Required Stop/Handoff State

If a stage cannot finish in one session, record:

- the current worktree and preserved unrelated changes;
- completed and incomplete requirements;
- the exact failing fixture, invariant, query, or command;
- whether any temporary database, run directory, lock, lease, process, live
  diagnostic, or production state exists and how to inspect it safely;
- the exact current modern cursor, legacy frontier/window, and their provenance;
- the next smallest diagnostic or implementation step;
- every external approval still required;
- assumptions most likely to be wrong.

Never describe a stage as merely “mostly done.” Record requirement-level state
and evidence so another session can continue without repeating risky work.

