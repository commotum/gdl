# Goal 4: Calm X Archive Dashboard

Shorthand: `X-DASHBOARD`

## Big-Picture Objective

Make the ordinary X archive command communicate, at a glance, that it is
healthy, what it has accomplished, what remains, and how long the active phase
might take:

```bash
uv run scripts/archive-x --user USERNAME
```

The operator should not have to interpret hundreds of `Waiting ...` lines,
pathnames, and error-shaped unavailable-post messages to answer:

1. Is the process alive and making progress?
2. Which phase is running, complete, blocked, or waiting?
3. How much useful archive content exists now?
4. What did this invocation add?
5. How much known work remains?
6. Is an ETA meaningful, and if so, roughly what is it?
7. Does anything require intervention?

The result should feel like a calm instrument panel, not a wall of telemetry.
Tmux may provide a small persistent dashboard pane while the existing pane
retains the event stream, but tmux must remain an optional presentation layer,
not a source of archive truth or a runtime dependency.

This is a scaffolding goal only. Creating `goal-4` does not authorize changes
to the running archive, its tmux layout, or executable code.

## Operator Empathy and Target Experience

The current screen makes normal safety behavior look unhealthy. Repeated waits
dominate the display. A successfully captured parent is only a long pathname.
Deleted, protected, and suspended parents appear as errors even though each is
a legitimate closure boundary. The screen provides no lifetime scorecard,
this-run delta, known remaining work, rate, or estimate. A long-running healthy
archive can therefore feel indistinguishable from a stuck one.

The likely desired experience is:

- Minimal: approximately seven high-signal lines, readable from across a room.
- Reassuring without hiding truth: `healthy`, `waiting`, `retrying`, `blocked`,
  and `failed` must mean different things.
- Accomplishment-first: show durable posts, media, context parents, and closed
  conversations before unavailable/retry details.
- Honest about uncertainty: a rough, labeled estimate is useful; false
  precision is not.
- Quiet: routine pacing and unavailable boundaries update counters rather than
  scrolling continuously.
- Inspectable: the complete raw event stream and immutable logs remain
  available when details matter.
- Automatic for the normal command, with no new required flags or phase
  knowledge.

### Target Compact Readout

The exact typography is subject to terminal-width prototypes, but the
information hierarchy should resemble:

```text
@visakanv  CONTEXT METADATA · healthy                         30h 59m
Archive    258,962 posts · 50,696 media · 27.1 GB
Context    6,861 parents saved · 3,553 unavailable · 0 review
Coverage   6,561 conversations closed · 117,936 known remaining
This run   +6,020 saved · +3,167 boundaries · 302 items/hour
Estimate   ~16d for known queue · low confidence; still discovering
Now        fetching 434278834846715904 · progress 3s ago
```

Routine deleted/private/suspended results should increment `unavailable` and,
when space permits, a neutral detail such as:

```text
Boundaries deleted 1,731 · private 1,436 · suspended 161
```

Only retry exhaustion, manual review, authentication failure, integrity
failure, a stale worker, or a failed phase should look like an alert.

## Scope

In scope:

- One structured progress model spanning modern, legacy, shared media, context
  seed, context metadata, context media, and export.
- Lifetime totals, invocation baselines/deltas, active phase, health, recent
  progress, known remaining work, and action-required counts.
- A compact one-shot and watch renderer that works without tmux.
- A safe tmux integration prototype for an automatically managed small
  dashboard pane when the normal command runs interactively inside tmux.
- A non-tmux fallback that emits periodic compact heartbeats without ANSI
  corruption when output is redirected.
- Phase-local rolling rates and confidence-labeled ETAs.
- Semantic console severity: progress, normal boundary, retryable warning,
  operator action, and fatal error.
- Durable raw logs and final invocation summaries independent of the compact
  display.
- Multiple-user rendering and fair representation without one large backlog
  hiding other users.
- Documentation, deterministic fixtures, interruption tests, and bounded live
  verification.

Out of scope unless evidence proves it necessary:

- A web server, browser dashboard, remote monitoring service, or notification
  system.
- Changing crawl scope, retry policy, rate limits, queue ordering, identity
  guards, completeness rules, or archive semantics.
- Concurrent X workers or any additional X requests for the sake of progress
  reporting.
- Exact all-phases completion dates when X does not expose a stable
  denominator.
- Scraping terminal text as the authoritative metric source.
- Replacing raw logs with a lossy summary.

## Design Objectives

Prioritized in this order:

1. **Truth before appearance.** Every number must come from structured archive
   state, an engine event, or a documented derivation—not regexes over console
   prose.
2. **No impact on archive correctness.** Reporting must not alter queue claims,
   pacing, cursors, windows, retries, locks, or interruption behavior.
3. **Immediate operator orientation.** Identity, active phase, health, elapsed
   time, durable accomplishments, remaining work, and intervention needs must
   be visible without scrolling.
4. **Calm semantic presentation.** Normal waits and unavailable boundaries are
   progress states; only actionable conditions receive warning/error emphasis.
5. **Honest estimates.** ETA must be phase-local, based on wall-clock net
   progress, confidence-labeled, and omitted when the denominator or burn rate
   is unstable.
6. **Minimal exterior.** The ordinary command gains the improved experience
   without new mandatory dashboard, tmux, interval, or estimation arguments.
7. **Durable observability.** A crash, `Ctrl-C`, renderer failure, tmux detach,
   or terminal resize must not erase archive truth or prevent later status
   inspection.
8. **Low overhead.** Do not repeatedly scan hundreds of thousands of files or
   run expensive whole-graph SQL queries at screen-refresh frequency.
9. **Responsive but not noisy.** The display may refresh frequently from a
   cheap snapshot while durable telemetry checkpoints remain bounded and
   atomic.
10. **Portable degradation.** TTY, non-TTY, tmux, narrow terminals, multiple
    users, and environments without Unicode/color must remain usable.

## Non-Negotiable Constraints and No-Cheating Rules

1. The archive state machines remain authoritative. The dashboard cannot mark
   a phase complete, healthy, or failed using its own weaker criteria.
2. Do not parse pathnames or gallery-dl log prose to calculate authoritative
   totals or progress.
3. Do not issue X requests, retry targets, claim leases, touch cursors, or
   advance legacy coverage from the renderer.
4. Do not query the full context graph every one or two seconds. Expensive
   totals must be maintained incrementally, sampled at a slower justified
   cadence, or read from atomic producer snapshots.
5. Do not present unavailable parents as captured content. Show captured and
   unavailable separately while recognizing both as resolved work.
6. Do not use `captured / currently-known targets` as an unlabeled global
   completion percentage. Context discovery can enlarge the denominator.
7. Do not show an ETA when net queue burn is non-positive, the sample is too
   young, a phase is blocked, or the required denominator is unknown.
8. ETA must include real waits and rate limits by using wall-clock throughput,
   not request-service time alone.
9. Legacy manual review, context manual review, authentication failures,
   integrity failures, and pending unavailable media remain visible and cannot
   be softened into generic `healthy`.
10. Raw logs, manifests, SQLite truth, checksums, redaction, private modes, and
    atomic writes remain intact.
11. Tmux automation may create, resize, or close only a pane it can prove it
    owns. It must never kill, reuse, or rearrange an unrelated pane.
12. Tmux detach or dashboard-pane failure cannot terminate the archive worker.
13. `Ctrl-C` must still reach the archive worker promptly and retain its
    resumable state. Dashboard cleanup must not delay or mask interruption.
14. Avoid cursor values, cookie contents, authorization headers, sensitive log
    lines, or full local cookie paths in the dashboard.
15. ANSI cursor control and color are allowed only on a capable interactive
    TTY. Redirected output must remain plain, append-only, and readable.
16. The final layout must fit an 80x24 terminal and take advantage of the
    observed 125x73 tmux pane without requiring that size.
17. Multiple users must show per-user state and an overall summary; no user's
    backlog may disappear behind an aggregate.
18. Installing or scaffolding the dashboard starts no archive work and does not
    mutate the currently running tmux session.
19. Preserve the existing uncommitted archive fixes and unrelated user work.

## Confirmed Current Facts

- The live `x` tmux pane is 125x73 and the unified command has been running for
  more than 30 hours.
- The live pane is dominated by `Waiting 4-8s before context request`, long
  media paths, `AuthRequired: Protected Tweet`, and explicit deleted/suspended
  boundaries.
- Terminal lines are prefixed in `archive_x.run_gallery_dl()`, while context
  pacing prints directly from `reserve_request()`. Output is presentation
  prose, not a structured event stream.
- The only compact unified phase summary is printed after the invocation ends.
- The invocation JSON is atomic and structured, but long context workers do not
  checkpoint meaningful live counts into it; the current invocation can remain
  stale for hours.
- `context.sqlite3` already knows target state, attempts, depth, observations,
  media state, reply edges, conversation closure, and pacing.
- At inspection time the live database held 83,914 captured targets, 3,553
  unavailable boundaries, 117,936 pending targets, one lease, 6,861
  network-fetched focal observations, and 594 pending context-media items.
  These are time-sensitive observations, not constants for implementation.
- The recent database timestamps implied approximately 302 resolved targets in
  the preceding hour, but no durable rolling-rate samples or invocation
  baseline currently exist.
- The context queue is dynamic: capturing a parent can discover and enqueue its
  parent. A simple percentage or `pending / recent completions` ETA can move
  backward and overstate confidence.
- Context `pacing.last_progress_at` is written at worker exit rather than on
  each durable item, so it is not presently a reliable live heartbeat.
- The modern manifest already records dataset posts, authored posts, reposts,
  media file count, media bytes, pending media, and endpoint status.
- Legacy state records the exact contiguous time frontier, account floor,
  active window, last completed window, conclusion, and manual-review state.
- The current legacy phase is independently in manual review while context
  metadata continues. A useful dashboard must show both rather than allowing
  the active phase to hide the blocked phase.
- Current exported datasets are not refreshed during every context capture, so
  line counts in `dataset/context-posts.jsonl` are not suitable live counters.
- The unified process already owns repository and archive locks. A read-only
  renderer must not contend for or reacquire the writer lock.
- Tmux is available, but accessing or manipulating its socket is an external
  presentation concern and must not be required for archive correctness.
- A live read-only aggregate benchmark took 1.408 seconds on the production
  Visakanv context database. Full conversation closure is therefore sampled
  every five minutes by the producer, never at renderer refresh frequency.

## Assumptions Requiring Proof

- A cheap producer-maintained snapshot can expose live context totals without
  increasing SQLite contention or weakening transaction boundaries.
- Engine callbacks can provide phase and counter deltas without duplicating
  state-machine logic.
- A run-start baseline can be captured after validation and recovered after an
  interruption without confusing lifetime totals and invocation deltas.
- Net queue burn sampled over 15- and 60-minute wall-clock windows will produce
  a useful context estimate often enough to justify showing it.
- Modern history has no reliable total denominator before exhaustion; oldest
  captured date and recent acquisition rate may be more honest than an ETA.
- Legacy date coverage can support an ETA only while contiguous windows are
  advancing and not splitting/retrying/manual-review blocked.
- Media queues have a sufficiently stable known denominator for a queue ETA,
  while download size variability may require a wide/low-confidence estimate.
- The operator will prefer an automatically managed tmux scorecard pane to
  same-pane ANSI redraw, provided it is small, reversible, and never touches
  unrelated panes. This must be tested with a prototype rather than assumed.
- A seven-to-nine-line scorecard can remain useful for multiple users; if not,
  a rotating or table layout will be required.
- Suppressing routine wait lines from the interactive console will feel calmer
  as long as raw logs remain complete and the dashboard visibly shows pacing.

## Metric Semantics

### Accomplished

- `archive_posts`: durable merged modern/legacy dataset posts.
- `archive_media_files` and `archive_media_bytes`: verified durable media
  assets from the authoritative media dataset/manifest.
- `context_parents_saved`: focal parent observations fetched from X, kept
  separate from locally known authored posts.
- `context_resolved`: captured plus terminal unavailable targets.
- `conversations_closed`: conversations whose known ancestor chain ends in a
  captured root or explicit unavailable boundary, with manual review separate.
- `this_run_*`: lifetime counters minus an immutable invocation baseline, or
  producer-recorded deltas when subtraction would be ambiguous.

### Remaining and Actionable

- `known_remaining`: pending, leased, and retryable context targets.
- `media_remaining`: due plus deferred media work, with confirmed unavailable
  assets separate.
- `manual_review`: phase-specific items requiring a deliberate operator action.
- `blocked_phase`: a non-active phase that prevents honest overall completion.
- `last_durable_progress_at`: the last successful transaction that captured,
  resolved, or durably advanced work—not merely the last printed line.

### Health

- `healthy`: active worker, integrity last known good, and recent progress or a
  justified pacing/rate-limit wait.
- `waiting`: no request should occur before a known future timestamp.
- `retrying`: bounded transient failure with a scheduled next attempt.
- `blocked`: manual review or another explicit operator gate.
- `stale`: active invocation with neither durable progress nor a justified wait
  beyond a phase-specific threshold.
- `failed`: authentication, integrity, identity, storage, compatibility, or
  engine failure.

## ETA Policy

ETAs are estimates of a specific phase, never a promise for the entire archive.

### Context Metadata

- Sample atomic snapshots over wall-clock time.
- Compute both gross resolution rate and net queue burn:
  `known_remaining_at_start - known_remaining_now`.
- Include discovery, retries, deleted/private fallbacks, and rate-limit waits
  naturally by measuring wall time.
- Show an ETA only after a minimum observation duration and item count, with
  sustained positive net burn.
- Use the slower/stabler rolling window when 15- and 60-minute estimates
  disagree materially.
- Label the result `known queue`, because undiscovered ancestors can still
  enlarge it.
- Confidence is `low`, `medium`, or `high` based on sample duration, queue
  growth volatility, retry/wait proportion, and agreement between windows.
- When unstable, display `discovering`, `rate settling`, `waiting until HH:MM`,
  or `ETA unavailable` rather than a number.

### Other Phases

- Modern initial history: show oldest durable post date and acquisition rate;
  do not invent a completion date before a trustworthy boundary exists.
- Modern incremental update: show items added and current endpoint health;
  usually no ETA is needed.
- Legacy: estimate remaining calendar coverage only while contiguous windows
  advance; hide ETA during splits, retries, or manual review.
- Media: use known queue burn and optionally byte throughput, but preserve a
  low-confidence label when file sizes vary.
- Export: use deterministic stages/items when available; otherwise show elapsed
  time only.

## Proposed Architecture

1. **Structured progress events**
   - Define a small typed event/snapshot schema shared by the orchestrator and
     engines.
   - Events describe semantic transitions such as phase start/end, durable
     capture, unavailable boundary, enqueue, retry schedule, media completion,
     window commit, wait-until, manual review, and fatal failure.
   - Existing engine return values and state remain authoritative; events
     report them rather than deciding them.

2. **Producer-owned atomic snapshot**
   - Maintain a private current-invocation progress snapshot with identity,
     phase truth, baseline, totals, deltas, recent samples, health, and safe
     current activity.
   - Write atomically at bounded cadence and on material transitions.
   - Avoid full file-tree scans and whole-graph aggregation per refresh.
   - Fold final telemetry into the invocation manifest so post-run status is
     durable.

3. **Read-only renderer**
   - Provide one-shot and watch modes over the atomic snapshot.
   - Render a responsive compact scorecard with plain-text and optional
     color/Unicode capability detection.
   - Renderer failure cannot affect the producer.

4. **Tmux adapter**
   - Detect an interactive tmux environment.
   - Create or reuse only a provably owned dashboard pane, sized from the
     current terminal.
   - Keep raw events in the command pane.
   - Close or leave a final scorecard according to tested operator preference,
     restoring only layout changes it owns.
   - Fall back cleanly when tmux is absent, too small, inaccessible, or opted
     out.

5. **Console policy**
   - Collapse routine waits and normal unavailable boundaries into dashboard
     counters/current activity.
   - Continue surfacing retries, action-required conditions, and fatal errors.
   - Preserve full logs on disk and append-only readable output for non-TTY
     callers.

## Success Metrics and Verification Requirements

The goal is complete only when all of the following are demonstrated:

1. The normal command provides a high-level live overview without required new
   flags.
2. Within two seconds of a material transition, the display identifies the
   correct user, active phase, health, and current activity.
3. The compact layout fits 80x24 and the observed 125x73 tmux environment.
4. Lifetime totals and this-run deltas match authoritative fixtures and direct
   database/manifest checks.
5. Captured parents, unavailable boundaries, retries, manual review, and media
   states are never conflated.
6. Deleted/private/suspended boundaries update neutral progress counters rather
   than appearing as fatal failures.
7. A blocked non-active phase remains visible while another phase progresses.
8. Context ETA tests cover shrinking, growing, newly discovered, retry-heavy,
   rate-limited, resumed, too-young, and zero-progress queues.
9. No ETA is displayed when the evidence policy says it is unreliable.
10. Reporting generates zero X requests and zero queue/cursor/window
    transitions.
11. Snapshot refresh does not perform whole-tree scans or expensive full-graph
    queries at display frequency.
12. Renderer death, tmux detach, tmux pane closure, terminal resize, broken
    pipe, and redirected output do not stop or corrupt the archive.
13. `Ctrl-C` retains the same prompt interruption and resumability guarantees.
14. Raw logs and final invocation manifests remain complete and redacted.
15. Multiple-user fixtures remain legible and fair.
16. Focused tests, the full test suite, compatibility fingerprint checks,
    whitespace/diff checks, and a bounded live smoke all pass.
17. Documentation explains metric meanings, ETA confidence, unavailable
    boundaries, raw-log access, tmux behavior, and fallback behavior.

## Implementation Outcome

Implemented on branch `goal-4-x-dashboard`. Stages 1 through 7 are complete;
their evidence is recorded in the corresponding indexed stage files.

- The normal command now produces safe telemetry and, when appropriate, an
  optional tmux scorecard with no required new argument.
- All 17 success metrics have implementation or verification evidence.
- The full 200-test suite, compilation, diff checks, production-scale read-only
  benchmark, and temporary live-data render smoke pass.
- No production archive command was started, restarted, stopped, or otherwise
  manipulated during implementation, and the user's tmux layout was not
  changed.

## Indexed Stages

### 1-SIGNALS

#### Big Picture Objective

Define one truthful progress vocabulary and metric contract across every
archive phase before building presentation code.

#### Detailed Implementation Plan

- Inventory structured state and current output in `scripts/archive_x.py`,
  `scripts/archive_x_unified.py`, `scripts/archive_x_legacy.py`,
  `scripts/archive_x_context.py`, run manifests, state JSON, and context SQLite.
- Build representative fixtures for initial modern history, modern update,
  legacy advance/split/manual review, context discovery/capture/unavailable/
  retry, media, export, interruption, and multiple users.
- Specify the progress event and snapshot schema, units, timestamps, health
  precedence, phase precedence, counter ownership, and redaction rules.
- Define lifetime totals versus invocation deltas and how a resumed invocation
  establishes its baseline.
- Benchmark candidate SQLite/status queries against a production-sized copy or
  read-only production snapshot.
- Record an accepted compact layout at 80x24 and 125x73.

#### Completion Requirements

- Every displayed field has an authoritative source and exact derivation.
- No field depends on parsing terminal prose.
- Health and blocked-state precedence are unambiguous.
- Query benchmarks identify which totals may be sampled live and which require
  producer-maintained counters.
- Fixtures and schema validation tests fail on secret-bearing or unknown
  fields.
- The target scorecard is reviewed against the operator questions in this
  plan.

### 2-TELEMETRY

#### Big Picture Objective

Emit durable, low-overhead structured progress without changing archive
semantics.

#### Detailed Implementation Plan

- Add a progress sink/interface with a no-op default so engines remain directly
  testable.
- Instrument orchestrator and engine transaction boundaries with semantic
  events and counter deltas.
- Capture an immutable invocation baseline after validation.
- Maintain a private atomic current snapshot at bounded cadence and material
  transitions.
- Record active phase, safe current activity, wait-until, last durable
  progress, totals, deltas, and action-required state.
- Fold final progress into the invocation manifest and finalize it on success,
  failure, or interruption.
- Ensure the renderer can read snapshots without writer locks.

#### Completion Requirements

- Event-order and snapshot-recovery tests cover every fixture from Stage 1.
- Engine outputs/state remain byte- or semantics-equivalent with a no-op sink.
- Killing the renderer has no producer effect.
- Atomicity tests never expose partial JSON.
- Event/snapshot writes are private, redacted, bounded, and recoverable.
- No added X request, lease claim, cursor change, or legacy advancement occurs
  in telemetry tests.

### 3-READOUT

#### Big Picture Objective

Create the minimal, elegant scorecard independently of tmux.

#### Detailed Implementation Plan

- Implement a read-only one-shot/watch renderer over the snapshot schema.
- Build responsive layouts for 80x24, 125x73, narrow fallback, and multiple
  users.
- Establish restrained symbols/color with plain ASCII/no-color fallback.
- Present accomplishments first, then remaining/actionable state, velocity,
  estimate, and current activity.
- Make unavailable-boundary details neutral and action-required states
  unmistakable.
- Provide a stale-snapshot indicator with the last durable progress time.
- Make non-TTY output append-only compact heartbeats rather than cursor redraws.

#### Completion Requirements

- Golden-output tests cover all health states, widths, Unicode/color modes, and
  multiple users.
- The default scorecard remains within the agreed line budget.
- Redirected output contains no ANSI control sequences.
- Snapshot corruption/missing/stale cases fail visibly but do not touch archive
  state.
- Screen-reader/plain-text ordering preserves the same information hierarchy.

### 4-ESTIMATE

#### Big Picture Objective

Add useful phase-local rates and ETAs without false precision.

#### Detailed Implementation Plan

- Add bounded rolling samples for 15-minute and 60-minute wall-clock windows.
- Implement gross completion, enqueue/discovery, retry, and net queue-burn
  calculations.
- Define minimum sample duration/count, confidence grades, instability
  thresholds, and humanized duration rounding.
- Add distinct estimators or explicit no-ETA policies for context, legacy,
  media, modern, and export.
- Preserve estimator samples across dashboard restarts and discard incompatible
  or stale samples safely.
- Surface the explanatory qualifier (`known queue`, `discovering`,
  `rate-limited`, `blocked`) alongside any number.

#### Completion Requirements

- Deterministic clock tests cover every ETA case listed in Success Metric 8.
- Estimates include wait/rate-limit wall time.
- Growing or zero-burn queues never produce a completion ETA.
- Resumed runs do not treat lifetime history as this-run velocity.
- Humanized output avoids minute-level precision for multi-day estimates.
- A backtest against captured production snapshots documents expected error and
  confidence behavior without tuning to one favorable interval.

### 5-TMUX

#### Big Picture Objective

Use tmux to keep the calm scorecard visible without making tmux intrusive or
required.

#### Detailed Implementation Plan

- Prototype top/bottom and side-by-side layouts at the observed 125x73 size and
  the 80x24 minimum.
- Select automatic versus same-pane default based on the prototype and explicit
  usability evidence, documenting any changed assumption.
- Implement an ownership token and exact pane lifecycle.
- Launch the read-only renderer without inheriting authority over the worker.
- Handle detach/reattach, resize, pane closure, session rename, nested tmux,
  inaccessible socket, multiple users, and process exit.
- Provide a simple environment-level opt-out if automatic tmux integration is
  selected; do not require a normal-use CLI argument.
- Preserve the raw event pane and never manipulate unrelated panes.

#### Completion Requirements

- Tests with isolated tmux servers prove only owned panes are changed.
- Killing or closing the dashboard pane leaves the archive alive.
- `Ctrl-C` reaches the worker and cleanup is bounded.
- Layout restoration affects only owned changes.
- Small/non-tmux/inaccessible environments use the documented fallback.
- No tmux command is needed for archive correctness or post-run status.

### 6-INTEGRATE

#### Big Picture Objective

Make the dashboard the natural one-command experience while retaining complete
diagnostics.

#### Detailed Implementation Plan

- Wire telemetry, renderer, and chosen tmux behavior into the normal unified
  command.
- Replace routine wait/path/boundary console spam with semantic progress
  updates on interactive runs.
- Keep retry, action-required, and fatal messages immediately visible.
- Preserve full per-endpoint logs and final structured summaries.
- Add post-run one-shot status using the finalized invocation snapshot.
- Cover dry run, bounded diagnostics, retry-only, explicit `--since`, multiple
  users, non-TTY, and standalone legacy/context tools.
- Update README/operator documentation with examples and raw-log locations.

#### Completion Requirements

- The ordinary command needs no new mandatory options.
- Existing exit codes, locks, pacing, interruption, and phase ordering are
  unchanged.
- Interactive output satisfies the target experience; redirected output stays
  plain.
- All advanced modes have deliberate, tested dashboard semantics.
- Documentation clearly distinguishes archive truth, dashboard snapshot, and
  ETA estimate.

### 7-PROVE

#### Big Picture Objective

Demonstrate that the dashboard is accurate, useful, low-overhead, and unable to
damage archive work.

#### Detailed Implementation Plan

- Replay synthetic and captured redacted event streams through the full stack.
- Compare every displayed total against direct authoritative queries.
- Benchmark producer overhead, snapshot cadence, renderer CPU, and SQLite
  contention.
- Fault-inject renderer crashes, malformed snapshots, read-only filesystems,
  broken pipes, tmux failures, terminal resizes, stale workers, and interrupts.
- Run focused suites, full verification, compatibility checks, and diff checks.
- Perform a separately authorized bounded live smoke without starting a large
  backlog unexpectedly.
- Record screenshots/text captures at target sizes and a concise operator
  acceptance checklist.

#### Completion Requirements

- All Success Metrics are mapped to passing evidence.
- No dashboard test or smoke changes archive completion semantics.
- Performance budgets from Stage 1 are met or transparently revised with
  evidence.
- The live smoke shows correct phase, totals, deltas, health, and estimate
  qualifiers.
- The final plan records limitations and explicit future work rather than
  hiding unresolved issues.
