# Goal 4 Continuation Prompt

```text
Work through /home/jake/Developer/gdl/goal-4/0-plan.md using
/home/jake/Developer/gdl/goal-4/0-loop.md.

Objective: give the ordinary one-command X archiver a minimal, elegant live
dashboard that immediately shows health, active phase, durable accomplishments,
this-run gains, known remaining work, action-required state, rolling velocity,
and an honest phase-local ETA when the evidence supports one. Use tmux as an
optional safe presentation layer when useful, while retaining a portable
non-tmux experience and complete raw logs.

Start by syncing the plan with the actual code, tests, git diff, live-safe
read-only archive state, and current output. Select the first incomplete stage,
create its stage file from the loop template, implement only that stage, verify
it, record results, and fold new facts back into the plan before continuing.

Non-negotiable constraints:
- Archive state machines, SQLite, state JSON, manifests, and engine results
  remain authoritative; do not calculate truth by scraping terminal prose.
- Reporting must issue zero X requests and must not claim leases, alter pacing,
  move cursors/frontiers, change retries, or decide completion.
- Keep captured content, unavailable boundaries, retryable work, and manual
  review distinct.
- Do not show an unlabeled global percentage or ETA for a dynamic/unknown
  denominator. Use wall-clock net queue burn, confidence labels, and explicit
  qualifiers such as "known queue" or "discovering".
- Preserve raw logs, redaction, private modes, checksums, locks, interruption,
  and fail-closed safety.
- Do not poll expensive whole-tree or whole-graph calculations at display
  refresh frequency.
- Tmux must remain optional. Manipulate only a provably owned pane, and never
  allow dashboard or tmux failure to stop the archive worker.
- The normal command must not gain required dashboard/tmux/budget arguments.
- Preserve pre-existing uncommitted changes and do not start/restart production
  work without separate authorization.

Completion means the original operator experience is genuinely achieved and
verified: the normal command provides a calm high-level overview, every number
matches authoritative state, ETA behavior is honest under dynamic queues,
failure/interruption/tmux scenarios are safe, full verification passes, and
open issues are carried forward as explicit next work rather than hidden.
```
