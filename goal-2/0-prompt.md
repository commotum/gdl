# Goal 2 Continuation Prompt

```text
Work through /home/jake/Developer/gdl/goal-2/0-plan.md using the repeatable protocol in /home/jake/Developer/gdl/goal-2/0-loop.md.

The objective is to repair the conservative X archiver so it can safely cross Twitter's November 2010 Snowflake-to-legacy-ID boundary and archive older source-visible history using a proven, resumable strategy. Prefer a separate date-windowed legacy phase with contiguous half-open UTC intervals and durable state; first characterize installed gallery-dl and prove the actual X behavior before committing to that design.

Preserve the proven `3_29116490825/` boundary, all existing raw data/datasets/manifests, stable numeric identity, one-worker locks, conservative pacing, bounded retries, private credentials, pending-media separation, fail-closed compatibility fingerprints, and the no-progress watchdog. Never run sequential IDs through Snowflake arithmetic, advance an ambiguous or incomplete window, infer completion from empty/repeated/error responses, start reply-context work, or automatically initialize/restart production.

At each stage, inspect actual code/tests/installed source/production evidence and the dirty worktree; update 0-plan.md; create the indexed stage file; implement only that stage; add requirement-specific negative and fault tests; run focused and full verification plus compatibility, permission, credential, and diff checks; record results; and fold findings back into the plan. Ordinary tests are offline and dry-runs are write-free. Live diagnostics, production migration, and production continuation occur only at their explicit approval gates.

Completion means the original repair is genuinely achieved: the selected pagination strategy is evidenced, contiguous legacy progress is crash-safe and auditable, a bounded approved production smoke advances older than October 29, 2010 or decisively establishes an upstream source limit, and all remaining long-running operational work is explicit. Carry every unresolved safety question, approval, source limitation, and next action forward rather than narrowing the goal or claiming false historical completeness.
```
