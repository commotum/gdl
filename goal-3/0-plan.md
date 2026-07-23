# Goal 3: One-Command Complete X Archive

Shorthand: `X-UNIFIED`

## Big-Picture Objective

Make this the single normal command for a complete, resumable account archive:

```bash
uv run scripts/archive-x --user USERNAME
```

One invocation must orchestrate three phases in order:

1. Archive all source-visible modern Snowflake-ID timeline posts using the
   current conservative timeline workflow.
2. If the account reaches Twitter's legacy sequential-ID era, automatically
   detect that transition and archive the remaining source-visible legacy
   timeline history.
3. Seed and resolve the ancestor-only reply-context graph for every archived
   authored reply, recursively saving each available parent post and its
   recoverable media through the existing `archive-x-context` machinery.

The operator supplies the account, not implementation-specific phase commands,
window counts, or post budgets. UTC legacy windows, SQLite context queues,
request caps, leases, retries, pacing, checkpoints, and media queues remain
internal safety mechanisms.

This goal was explicitly started on branch `goal-3-unified-x-archive` on
2026-07-22. Production rollout remains separately bounded; starting the goal
does not authorize the remaining Visakanv backlogs.

## Scope and Completion Boundary

In scope:

- A unified lifecycle behind `scripts/archive-x` and its Python entry point.
- Automatic modern-to-legacy transition detection and guarded initialization.
- Automatic resumption of existing pending/active legacy state and execution
  toward the honest source-visible account floor.
- Automatic local seeding of reply edges from both modern and legacy timeline
  records, including a safe initial bootstrap over existing raw history.
- Automatic recursive resolution of immediate parents, then their parents,
  using the durable context SQLite queue and existing ancestor-chain policy.
- Automatic processing of recoverable timeline, legacy, and context media
  queues without allowing media failures to corrupt metadata coverage.
- One combined readout, manifest linkage, exit policy, interruption behavior,
  and restart experience.
- Optional advanced limits for tests, bounded rollout, and deliberate operator
  batching, but no mandatory `--windows`, `--seed-reply-context`, or
  `--max-posts` arguments for normal use.
- Continued availability of standalone legacy/context status, integrity,
  retry, export, and diagnostic commands.

Out of scope unless evidence makes it essential:

- Whole-conversation expansion, sibling replies, descendants, quote-source
  expansion, or “show more replies” crawling.
- Claiming deleted, private, withheld, suspended, or unindexed posts were
  recovered when X does not expose them.
- Weakening the modern no-progress watchdog, legacy two-walk rule, context
  retry/manual-review policy, or any identity/storage/credential invariant.
- Concurrent workers, credential rotation, proxies, or rate-limit evasion.
- Starting the remaining Visakanv production backfill or context backlog as
  part of implementation without a separate bounded rollout authorization.

## Design Objectives

Prioritized in this order:

1. **No gaps or false completion.** Automation must not turn generic failures,
   missing posts, or ambiguous pagination into coverage.
2. **One-command completion.** The ordinary invocation drives every necessary
   phase and later resumes it without operator reconstruction.
3. **Durable phase independence.** A safe commit in one phase remains valid if
   a later phase fails; each phase reports its own truth.
4. **Correct pagination domains.** Modern cursors, legacy UTC frontiers, and
   context SQLite targets remain distinct even under one orchestrator.
5. **Metadata before media.** Metadata coverage advances independently while
   failed assets remain explicit, durable, and retryable.
6. **Complete ancestor context.** Resolve immediate reply parents recursively
   to a root or explicit unavailable/manual-review boundary—never merely seed
   a queue and call the archive complete.
7. **Bounded internals, simple exterior.** Remove required operator budgets,
   not request/walk/retry/depth/leaf/time limits or interruptibility.
8. **Idempotent reruns.** Re-running the same command after `Ctrl-C`, crash,
   partial media, or API failure produces duplicate work at worst, never gaps.
9. **Transparent uncertainty.** Distinguish timeline coverage, legacy
   source-visible coverage, context graph closure, unavailable ancestors, and
   pending media.
10. **One implementation per engine.** Main orchestration calls shared modern,
    legacy, and context engines; it must not shell out to their CLIs or fork
    divergent logic.

## Non-Negotiable Constraints and No-Cheating Rules

1. Internal legacy UTC windows remain the atomic coverage/recovery units.
   Removing a required CLI count must not become one unbounded search query.
2. The context SQLite queue remains authoritative for leases, retries, chain
   depth, edges, closure, unavailable boundaries, and manual review.
3. Do not claim unified completion after only passing `--seed-reply-context`.
   The parent queue must be processed to its defined closure or reported
   blocked/incomplete.
4. Seed context from both modern and legacy authored replies. Do not omit
   legacy reply edges or require the operator to run a historical seed later.
5. Preserve ancestor-only scope: no sibling, descendant, quote, or entire
   conversation expansion.
6. Parent posts may be authored by other accounts. Preserve focal edge
   provenance and safety checks without incorrectly filtering parents to the
   archived account's numeric ID.
7. Do not spawn `archive-x-legacy` or `archive-x-context` as opaque subprocesses
   from the main command. Refactor shared, structured, directly testable
   engines.
8. Define one lock owner. The unified process must not reacquire the same
   non-reentrant repository/archive lock and deadlock itself.
9. Do not infer a legacy transition from account age, status count, one small
   ID, one empty response, one successful walk, or generic `stalled` status.
10. Automatic legacy initialization must create a private exact backup, retain
    stale guards, preserve the modern cursor/run evidence, and be idempotent.
11. `manual_review` is not an invitation to guess. Legacy or context manual
    review remains stopped until the corresponding explicit guarded retry.
12. Every individual operation stays bounded: HTTP requests, timeouts, retries,
    walks, leaves, cursor pages, context depth, leases, and media attempts.
13. A phase failure must not roll back earlier durable metadata or grant a
    later phase authority over an earlier phase's cursor/frontier.
14. A context metadata success may enqueue media; media failure must not replay
    or unresolve the parent post.
15. Preserve stable target identity, repost policy, mounted-root enforcement,
    private modes, credential redaction, immutable raw evidence, checksums,
    atomic writes, and conservative pacing.
16. `--dry-run`, `--post-limit`, `--retry-failed-only`, `--full-rescan`,
    `--no-reposts`, input files, and multiple users require deliberate phase
    semantics and tests.
17. Installing the code starts nothing. Running the documented command is the
    operator action authorizing its documented three-phase lifecycle.
18. Preserve existing Goal 1 and Goal 2 data, state, tests, documentation, and
    intentional worktree changes.
19. Production rollout must be explicitly bounded with advanced test controls;
    it must not accidentally launch the remaining multi-year archive.

## Confirmed Current Facts

- `scripts/archive-x` invokes the current modern archiver. The normal command
  does not run the legacy engine or fetch reply parents.
- `--seed-reply-context` currently runs local context `seed` only after a
  durable timeline merge. It makes zero context requests and does not process
  the historical parent backlog.
- The current seed hook consumes the just-written modern timeline raw path; it
  does not automatically seed replies found by the legacy engine.
- Actual parent retrieval currently requires the separate command
  `scripts/archive-x-context --user USER run --max-posts N`.
- Context media currently requires another separate bounded `media
  --max-posts N` command.
- The context resolver already stores durable state in private SQLite, follows
  the immediate parent recursively, prefers completing the active ancestor
  chain, periodically yields between chains, and records explicit unavailable
  and manual-review boundaries.
- The context resolver intentionally excludes siblings, descendants, quoted
  sources, and broad conversation expansion.
- The context and archive CLIs share archive locks. Calling their existing CLI
  entry points from an already locked main process risks nested lock
  acquisition and is not an acceptable integration.
- `scripts/archive-x-legacy` currently requires `run --windows N`; its internal
  engine already has safe UTC intervals, two matching walks, hard request caps,
  exact subdivision, recovery, dataset deduplication, and pending media.
- Visakanv's legacy state is already initialized and pending. The modern cursor
  `3_29116490825/` is preserved, and the proven contiguous legacy frontier is
  `2010-10-29T00:00:00Z` after one production window.
- Goal 2 ended with 258,070 dataset posts and two pending media items. At that
  point no context database existed for Visakanv; Stage 1 must recheck this
  rather than assume it remains true.
- Goal 2's 126-test baseline was committed before this branch. The first Goal 3
  baseline run passed 123 tests while three legacy CLI tests were blocked by a
  real archive process holding the repository lock; no executable code had
  changed.
- Work is isolated on `goal-3-unified-x-archive`. Tmux `x` is currently running
  the unrelated `airkatakana` modern archive, so executable edits and clean
  full-suite verification must not occur until it releases the shared lock.

## Assumptions Requiring Proof

- The modern result and raw/state evidence can distinguish a genuine legacy
  transition from all transient timeline failure classes.
- After legacy activation, later unified runs can update the modern head
  without replaying the exhausted historical tail to the legacy cursor.
- Legacy and context engines can be separated from their CLI parsing/locking
  without changing their state-machine behavior.
- A first unified context bootstrap can seed all existing modern and legacy
  reply edges idempotently without rescanning irrelevant files forever.
- Incremental context seeding can be tied to every committed dataset/raw merge
  so later ordinary runs only add new work.
- Removing the required context post count can safely drain the queue because
  every target has bounded attempts, pacing, depth, and manual-review behavior.
- Context metadata and media can share the unified lifecycle without media
  starvation or metadata replay.
- Phase ordering can maximize safe progress when legacy becomes blocked: the
  orchestrator may still resolve context for already durable posts while
  reporting the overall archive incomplete.
- Multiple-user invocations need a fairness policy so one historical/context
  backlog does not silently starve later targets.

## Target User Experience

The normal command is exactly:

```bash
uv run scripts/archive-x --user USERNAME
```

It should:

1. Validate storage, cookies, compatibility fingerprints, identity, state, and
   interrupted manifests.
2. Archive/update the account's modern timeline, profile, and media using the
   current conservative behavior.
3. Evaluate durable transition evidence. If proven and not initialized, create
   an exact backup and initialize legacy state automatically.
4. Resume legacy `pending`/recoverable `active` work through internal UTC
   windows until the account floor or a fail-closed stop. Skip it if complete;
   report rather than guess if it requires manual review.
5. Process recoverable legacy/timeline media without coupling media failure to
   metadata coverage.
6. Seed the context database from all durable authored replies not previously
   accounted for, covering both modern and legacy raw history.
7. Resolve each available reply parent and then that parent's parent until
   every chain reaches a root, explicit unavailable boundary, depth policy, or
   manual-review state.
8. Process context media through its durable queue.
9. Export/rebuild derived context views and print one phase-by-phase summary.

On a later invocation, completed phases skip cheaply, the modern head updates,
new replies seed new context work, and interrupted work resumes. The user does
not calculate day counts or post counts.

Standalone legacy/context commands remain advanced maintenance interfaces for
status, integrity, retry, export, diagnostics, and optional bounded operation.

## Completion Semantics

“Complete” must be qualified, not absolute:

- **Modern timeline complete:** the current modern source was enumerated under
  existing cursor/watchdog semantics.
- **Legacy source-visible complete:** every contiguous initialized UTC window
  reached the account-creation floor with repeat-confirmed search evidence.
- **Context metadata closed:** every discovered ancestor target is captured,
  a root, an explicit unavailable boundary, a configured depth boundary, or an
  operator-visible manual-review item. Manual review means the unified archive
  is not fully successful.
- **Media complete:** all recoverable timeline/legacy/context media is present
  and verified; otherwise the archive reports explicit pending/failed assets
  without invalidating metadata coverage.

None of these claims proves recovery of content X deleted, withholds, keeps
private, or no longer indexes.

## Success Metrics and Verification Requirements

1. `uv run scripts/archive-x --user USERNAME` is sufficient for routine modern,
   legacy, context-parent, and media work; no second run command or required
   internal budget is needed.
2. A fresh transition fixture completes modern enumeration, records proven
   transition evidence, creates an exact private backup, initializes legacy
   state once, and hands off automatically.
3. Transient stalls, 429s, API errors, identity mismatch, malformed data,
   repeated cursors outside the proven boundary, and media-only failures do not
   initialize legacy state.
4. Existing initialized pending/active legacy state resumes automatically;
   complete state skips; manual-review state remains explicit and unmodified.
5. Internal legacy windows run to the honest floor by default while every
   request/walk/split/retry remains bounded.
6. The unified command bootstraps context from the existing full timeline and
   legacy archive, then incrementally seeds every newly committed authored
   reply exactly/idempotently.
7. Context tests prove recursive parent traversal to root/boundary, depth-first
   chain preference, cycle handling, unavailable states, retry escalation, and
   exclusion of siblings/descendants/quotes.
8. Parent posts by other authors are retained with correct authorship and edge
   provenance rather than discarded by the focal-account filter.
9. Default context execution drains eligible metadata work to closure without
   a required `--max-posts`; optional bounds stop and resume without changing
   queue truth.
10. Context media is downloaded or remains explicitly retryable; media failure
    never uncommits captured parent metadata.
11. A second ordinary invocation after interruption resumes only uncertain
    work and deduplicates all modern, legacy, context, edge, and media records.
12. Subsequent ordinary runs update the modern head without replaying the known
    exhausted historical tail, then seed/resolve only newly discovered context.
13. One lock-owning orchestrator excludes concurrent normal, standalone legacy,
    context, and media writers without self-deadlock.
14. Fault injection covers every phase boundary and proves earlier durable
    commits survive later failure.
15. Dry-run is network/write-free and accurately previews all three phases,
    backlog state, and any advanced rollout limits.
16. Option-matrix tests cover post-limit, retry-only, full-rescan, no-reposts,
    input-file, multiple users, initialized/absent/complete/manual-review state,
    context bootstrap, and existing context databases.
17. Documentation presents only the unified command as normal operation and
    explains specialized commands as maintenance tools.
18. Focused/full tests, compatibility fingerprints, SQLite integrity, state
    hashes, credential scans, private-mode checks, compilation, artifact audit,
    and `git diff --check` pass.
19. A bounded production smoke drives modern, legacy, and context through the
    unified entry point, then stops. It does not start the full remaining
    Visakanv backlog without separate user authorization.

## Indexed Stages

### 1-FLOWMAP

Status: complete on 2026-07-22. The modern, legacy, context, media, state,
manifest, lock, option, and production boundaries are recorded in
`1-FLOWMAP.md`; no live process or production artifact was changed.

#### Big Picture Objective

Map all three existing engines, state authorities, locks, media paths, and
option interactions before changing orchestration.

#### Detailed Implementation Plan

- Trace normal archive phase order, incremental/history selection, timeline
  completion and stall classes, pending media, manifests, locks, and exits.
- Trace legacy CLI/engine separation, initialization, recovery, loop limits,
  media enqueueing, and state commits.
- Trace context seed/run/media/export paths, SQLite schema, leases, closure,
  depth-first scheduling, retry/manual review, locks, and exit behavior.
- Determine how modern updates should work after legacy activation and how
  existing raw files can be context-seeded exactly once/idempotently.
- Build a complete option and multi-user behavior matrix.
- Freeze current production hashes, counts, SQLite presence/integrity, process
  status, and mounts without network or writes.

#### Completion Requirements

- The stage record names every lock owner, state write, manifest, queue, media
  authority, and exit-status boundary involved in the unified lifecycle.
- Modern-after-legacy and context-bootstrap behavior is established from code
  and fixtures rather than assumption.
- Every unresolved interaction is converted to a design decision or test.
- Production files, processes, and network remain unchanged.

### 2-LIFECYCLE

Status: complete on 2026-07-22. The authoritative state split, strict
transition gate, `modern_head` checkpoint, phase order, context source ledger,
closure-aware no-budget engines, multi-user fairness, options, and exit
semantics are fixed in `2-LIFECYCLE.md`; production remained untouched.

#### Big Picture Objective

Specify the exact three-phase state machine, completion language, and operator
semantics before implementation.

#### Detailed Implementation Plan

- Define modern, transition, legacy, shared-media, context-seed,
  context-resolve, context-media, export, and final-readout ordering.
- Decide which independent later phases may run when an earlier phase is
  safely blocked, while preserving an overall non-success status.
- Define default run-to-boundary/closure behavior and optional advanced test
  budgets.
- Specify repeated invocation, `Ctrl-C`, multi-user fairness, dry-run, option
  interactions, and combined exit codes.
- Define manifest linkage without giving one phase authority over another's
  cursor, frontier, or queue.

#### Completion Requirements

- Every reachable combination of modern, legacy, context, and media state has
  one deterministic unified-command behavior.
- No normal path requires an operator-calculated window or post count.
- Completion/readout terms distinguish all four categories in the Completion
  Semantics section.
- The specification preserves Goal 1 and Goal 2 safety invariants.

### 3-DETECT

Status: complete on 2026-07-22. Strict automatic classification, exact backup
and initialization, separate schema-validated modern-head state, fault tests,
and read-only production derivation are implemented and recorded in
`3-DETECT.md`.

#### Big Picture Objective

Implement strict, auditable automatic legacy detection and guarded setup.

#### Detailed Implementation Plan

- Build a pure classifier over durable focal-account state, manifest, raw
  progress, cursor history, time/ID transition evidence, and failure class.
- Return explicit `proven`, `not_applicable`, or `ambiguous` outcomes with safe
  reason codes and evidence hashes.
- Refactor legacy initialization validation for automatic use with equivalent
  stale guards and idempotency.
- Create a private exact pre-transition backup before automatic state mutation.
- Add positive exact-transition and comprehensive negative fixtures.

#### Completion Requirements

- No single weak heuristic triggers legacy initialization.
- The known Visakanv transition fixture classifies as proven; transient and
  unrelated failures do not.
- Backup/write faults leave prior state valid and prevent handoff.
- Modern cursor/run evidence and unrelated state remain preserved.

### 4-ENGINES

Status: complete on 2026-07-22. Typed direct legacy execution, closure-aware
no-budget context execution, optional advanced CLI bounds, and focused tests
are implemented in `4-ENGINES.md`; inner safety limits are unchanged.

#### Big Picture Objective

Expose reusable legacy and context engines while preserving their existing
safety-critical state machines.

#### Detailed Implementation Plan

- Separate CLI parsing, preflight, lock acquisition, execution policy, and
  presentation from legacy run logic.
- Replace mandatory root-window policy with an optional engine budget; no
  budget means run toward the legacy terminal state.
- Separate context seed, resolve, media, and export engines from their CLI
  parsing and lock ownership.
- Replace mandatory context post/media budgets with optional engine budgets;
  no budget means drain eligible work to closure or fail-closed state.
- Preserve standalone commands as thin maintenance/diagnostic adapters over
  the exact same engines.

#### Completion Requirements

- The main orchestrator calls structured functions without subprocesses,
  fabricated argparse namespaces, or nested locks.
- All existing Goal 1/Goal 2 fixtures pass unchanged or with justified API-only
  updates.
- Bounded and unbounded-policy tests share identical inner safety limits and
  state transitions.
- Synthetic no-budget runs reach legacy floor and context closure; bounded
  runs stop cleanly and resume.

### 5-SEEDING

Status: complete on 2026-07-22. Context schema-v2 migration/backup,
manifest-authoritative modern+legacy source discovery, transactional source
ledger, persistent local-parent index, exclusions, recovery tests, and a
read-only production inventory are implemented in `5-SEEDING.md`.

#### Big Picture Objective

Guarantee every durable modern or legacy authored reply enters the context
graph exactly and efficiently.

#### Detailed Implementation Plan

- Define durable seeding provenance/checkpoints for existing raw history and
  future per-run merges.
- Bootstrap an absent/stale context database from all relevant immutable raw
  files without treating repost wrappers or non-reply records as edges.
- Seed new context work immediately after each successful modern or legacy
  metadata commit, with no network authority over those commits.
- Preserve stable child-parent mappings, conversation IDs, depth, run
  provenance, cycle checks, and idempotent conflict behavior.
- Ensure a seed failure is reported and retryable without corrupting timeline
  or legacy state.

#### Completion Requirements

- Fixture archives containing modern and legacy replies seed the complete
  expected edge/target set exactly once across repeated runs.
- Legacy reply edges cannot be omitted by only inspecting the modern raw path.
- Seed checkpoints cannot hide newly added raw files after crash/recovery.
- Seeding makes zero X requests and does not advance any archive cursor.

### 6-ORCHESTRATE

Status: complete on 2026-07-22. One-lock direct orchestration, automatic
transition/head recheck, legacy/context fairness, shared/context media,
structured phase results, and pagination-domain-safe recovery are implemented
and verified in `6-ORCHESTRATE.md`.

#### Big Picture Objective

Make `scripts/archive-x` the single lock-owning orchestrator for modern,
legacy, context, and media work.

#### Detailed Implementation Plan

- Add explicit phase orchestration and structured phase results to the main
  archive path.
- Automatically initialize/resume/skip/report legacy based on validated state.
- Drain context metadata chains and context media after corpus seeding,
  including safe progress when legacy is independently blocked.
- Process timeline/legacy pending media under the shared metadata-before-media
  policy.
- Link main, legacy, and context evidence in combined manifests and summaries.
- Implement the Stage 2 multi-user fairness and exit policy.

#### Completion Requirements

- `uv run scripts/archive-x --user USERNAME` performs all required phases in
  integration fixtures with no phase-specific flags.
- Locks are acquired once and still exclude every standalone writer.
- Later phase failure preserves earlier committed work and produces truthful
  combined non-success.
- A repeated unified invocation resumes rather than reinitializes or rebuilds
  completed work unnecessarily.

### 7-RECOVERY

Status: complete on 2026-07-22. Durable combined checkpoints, truthful
interruption, per-target fault isolation, typed global authentication stops,
idempotent ordinary reruns, and cross-engine recovery evidence are recorded in
`7-RECOVERY.md`.

#### Big Picture Objective

Prove the three-phase lifecycle is safe across crashes, interruption, partial
writes, and manual-review states.

#### Detailed Implementation Plan

- Inject failures around modern commit, transition evidence, backup,
  initialization, legacy claim/walk/raw/merge/frontier, context seed,
  SQLite lease/capture/edge commit, both media paths, export, and final manifest.
- Exercise `Ctrl-C` during requests and delays and between every phase.
- Verify legacy active-window replay, SQLite lease recovery, idempotent seed,
  duplicate metadata merge, pending media, and manifest reconciliation.
- Test guarded standalone legacy/context retry followed by ordinary-command
  continuation.

#### Completion Requirements

- Every failure produces retryable/duplicate work at worst, never a skipped
  legacy interval, lost context target, or false closure.
- Earlier phase state survives later phase failure exactly.
- A later ordinary command resumes without internal IDs/counts supplied by the
  operator.
- Manual-review items remain visible and are never automatically reset.

### 8-UX

Status: complete on 2026-07-22. The exact one-command UX, read-only phase-aware
dry-run, truthful summary, modern-domain option guard, README, and generated
dataset documentation are complete and recorded in `8-UX.md`.

#### Big Picture Objective

Make the unified operation and its honest partial states understandable through
one command and one coherent readout.

#### Detailed Implementation Plan

- Update CLI help, dry-run, README, dataset/context documentation, manifests,
  summaries, and error remediation.
- Remove `--seed-reply-context`, mandatory legacy windows, and mandatory
  context post counts from routine instructions while retaining advanced tools.
- Report modern, legacy, context-metadata, and media status separately.
- Show transition decision, legacy frontier, context queue/closure and
  unavailable counts, pending assets, and exact guarded remediation.
- Keep all readouts free of cookies, headers, signed URLs, and opaque legacy
  cursor values.

#### Completion Requirements

- Routine documentation contains the single command and no required follow-up
  pipeline commands.
- Dry-run is network/write-free and accurately predicts all phases.
- Golden tests cover fresh, incremental, pending, active, complete, unavailable,
  partial-media, manual-review, and interrupted combinations.
- Coverage limitations are explicit and cannot be confused with “every post
  that ever existed.”

### 9-VERIFY

Status: complete on 2026-07-22. All 176 repository tests, both runner
fingerprints, option/failure/static/privacy/mode/artifact audits, production
read-only dry-run, and frozen Visakanv hash checks passed as recorded in
`9-VERIFY.md`.

#### Big Picture Objective

Prove the unified system offline without regressing modern, legacy, context,
media, compatibility, storage, or privacy behavior.

#### Detailed Implementation Plan

- Run focused detector, engines, seeding, orchestration, lock, recovery,
  option-matrix, context-closure, media, redaction, and permissions tests.
- Run the entire repository suite and all gallery-dl runner fingerprints.
- Audit for subprocess handoff, nested locks, duplicate engines, lost inner
  limits, queue bypass, cursor/frontier conflation, and false completion.
- Exercise production dry-run/status/integrity read-only and compare frozen
  hashes/process state.
- Run compilation, SQLite integrity, secret scans, artifact/mode audits, and
  `git diff --check`.

#### Completion Requirements

- Tests directly prove every success metric and all existing suites pass.
- Production state, dataset, context database, and process status remain
  unchanged during offline verification.
- The no-budget normal path and optional bounded rollout path use the same
  engines and state semantics.
- The diff contains only goal-related implementation/docs/tests and preserves
  all existing archive evidence.

### 10-ROLLOUT

#### Big Picture Objective

Prove all three phases through the unified production entry point under strict
bounds, then leave the full operation stopped for explicit authorization.

#### Detailed Implementation Plan

- Recheck production hashes, state, context DB/integrity, locks, mounts,
  credentials, pending media, and tmux/process state.
- Run an explicitly approved bounded unified smoke using advanced internal
  limits for at most one legacy root window, a very small number of context
  targets, and a very small number of media attempts.
- Verify modern behavior, handoff, context seeding from modern and legacy data,
  one recursive parent-chain step, queue truth, media state, manifests, modes,
  cursors/frontiers, and stopped processes.
- Run all post-smoke verification and document the ordinary no-limit command.
- Do not start the remaining Visakanv legacy/context backlog without separate
  user authorization.

#### Completion Requirements

- The smoke is driven by `uv run scripts/archive-x --user visakanv` plus only
  explicitly marked rollout limits—not separate phase run commands.
- The bounded scope is honored exactly and all phase state remains resumable.
- Modern cursor, legacy frontier, context graph, and media queues are
  independently auditable and consistent.
- Final handoff states exact process status, remaining work, limitations, and
  confirms that normal full operation needs only the single command.

## Completion Boundary

This goal is complete only when `uv run scripts/archive-x --user USERNAME`
actually drives modern timeline archival, automatic legacy detection and
remaining legacy archival, complete ancestor-only context resolution, and
recoverable media handling with no required phase-specific follow-up commands
or operator-calculated budgets. A wrapper that merely seeds context, a main
command that still requires separate legacy/context invocations, or green tests
that never exercise the phase transitions do not satisfy the objective.
