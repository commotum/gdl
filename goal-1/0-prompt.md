# Goal 1 Continuation Prompt

```text
Work through /home/jake/Developer/gdl/goal-1/0-plan.md using the repeatable protocol in /home/jake/Developer/gdl/goal-1/0-loop.md.

The objective is to implement and verify a safe, opt-in, SQLite-backed X reply-context resolver that records every reply edge and follows only the available ancestor chain to closure. Preserve the main timeline archiver's principles: timeline-state isolation, one worker, persistent conservative pacing, bounded retries, fail-closed gallery-dl compatibility, stable numeric identity, private credentials, crash-safe/idempotent state, explicit unavailable boundaries, archive-root safeguards, metadata-before-media, and deterministic auditable outputs.

Use bounded depth-first, conversation-aware scheduling: normally close the active ancestor chain, but park retry-delayed work and enforce fairness, cycle, and maximum-depth guards. Never use whole-conversation expansion, sibling/descendant capture, concurrency, proxy rotation, the gallery-dl downloads database as the queue, or media completion as a condition of graph completion. Ordinary tests must be offline. Do not start a live smoke test or the large production backfill without explicit user approval.

At each stage: inspect actual files/tests and the dirty worktree; update 0-plan.md with current evidence; create the indexed stage file from the template; implement only that stage; add requirement-specific failure-path and no-cheating checks; run focused tests, full relevant verification, and diff checks; record results; and fold findings back into the plan. Preserve unrelated user changes.

Completion means the original implementation objective is genuinely achieved and evidenced, not merely that tests are green. Carry every unresolved safety question, verification obligation, approval gate, and operational issue forward as explicit next work. The production backfill is a separate explicit operational authorization, not an implied part of this prompt.
```
