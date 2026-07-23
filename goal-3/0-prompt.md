# Goal 3 Continuation Prompt

```text
Work through goal-3/0-plan.md using goal-3/0-loop.md.

The objective is a genuinely unified X archive operated by one normal command:

  uv run scripts/archive-x --user USERNAME

That command must (1) archive/update all source-visible modern Snowflake-ID
timeline posts using the current conservative workflow, (2) safely detect a
real transition to legacy sequential IDs and automatically initialize or
resume the remaining legacy archive to its honest source-visible floor, and
(3) automatically seed and drain the ancestor-only reply-context graph through
the existing archive-x-context machinery, recursively saving each available
parent and parent-of-parent plus recoverable context media.

Normal use must not require archive-x-legacy, archive-x-context, --windows,
--seed-reply-context, --max-posts, or operator-calculated budgets. Keep those
specialized interfaces only for status, integrity, retry, diagnostics, bounded
rollout, and maintenance.

Preserve the safety architecture. Modern cursors, legacy UTC frontiers, and the
context SQLite queue remain separate authorities. Keep strict transition
detection, stable target identity, two-walk legacy confirmation, exact
subdivision, context leases/retries/depth-first ancestor traversal, explicit
unavailable/manual-review boundaries, request/time/retry/depth/leaf limits,
metadata-before-media semantics, atomic writes, immutable evidence, mounted
storage, private modes, and credential redaction. Seed replies from both modern
and legacy history. Keep other-author parent posts, but never expand siblings,
descendants, quoted sources, or whole conversations. Refactor shared engines;
do not spawn standalone CLIs or nest archive locks.

For every stage, inspect actual files/state/tests first, update plan facts,
create the stage file from the loop template, implement only that stage, add
direct verification and no-cheating checks, run focused and full checks, record
exact results, and fold them into the plan. Convert blockers into diagnostics,
proof obligations, or explicit next work.

Completion means the exact unified command is proven across modern updates,
automatic transition setup, legacy completion/resume, full ancestor-context
closure, media recovery, interruption, crashes, manual review, options,
multi-user behavior, locking, documentation, and a strictly bounded production
smoke. Queue seeding alone, wrappers around separate commands, or green tests
that skip real phase transitions are insufficient. Do not start Visakanv's
remaining full production backlogs without separate authorization, and carry
every open issue forward rather than narrowing the objective.
```
