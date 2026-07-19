# Goal 1: Safe X Reply-Context Resolver

Shorthand: `X-CONTEXT`

## Big-Picture Objective

Implement an opt-in, SQLite-backed reply-context subsystem for the existing X
archive. For every archived reply, the subsystem must durably record the
child-to-parent edge and resolve the available ancestor chain to its root,
without expanding unrelated sibling or descendant replies. It must preserve
the main archiver's conservative safety model: one worker, explicit pacing,
bounded retries, fail-closed compatibility checks, crash-safe progress,
stable numeric identity, private credentials, durable raw evidence, and no
silent fallback to a different disk or network strategy.

The implementation is complete only when offline verification proves the
queue, scheduler, recovery, classification, and export invariants; a bounded,
explicitly approved live smoke test proves the configured individual-post
path; and the operator can inspect status and deliberately start or resume a
production backfill. The implementation must never automatically launch the
large historical backfill.

## Scope Definition

“Full context” means **ancestor closure**, not whole-thread capture:

- Save the immediate post to which an archived reply points.
- If that parent is itself a reply, follow its parent.
- Continue until reaching a root, a previously captured ancestor, or an
  explicitly classified unavailable boundary.
- Do not fetch sibling replies, descendants, “show more” expansions, or every
  post in the conversation.
- Keep quote-source resolution out of this goal. It may become a later,
  separately scoped feature.

## Non-Negotiable Constraints and No-Cheating Rules

1. **Timeline isolation:** context discovery, fetching, retries, media, and
   failures must not block, roll back, advance, replace, or invalidate the main
   timeline cursor. Existing timeline recovery semantics remain authoritative.
2. **One worker:** no concurrent X requests, worker pools, proxy rotation,
   header spoofing, or rate-limit evasion. An exclusive lock must enforce the
   singleton execution model.
3. **Global pacing:** pacing and `not_before` state must survive individual
   extractors and process restarts. Do not assume gallery-dl's per-extractor
   deferred-rate-limit state protects a sequence of individual-post jobs.
4. **Ancestor-only scope:** do not use `expand: true`, conversation expansion,
   `showreplies`, or another path that archives siblings and descendants as a
   shortcut.
5. **Stable identity:** classify target-authored versus external context with
   the archive's bound numeric target user ID, never only by handle or by the
   individual extractor's `user` object.
6. **Separate state:** create a schema-versioned context database. Do not
   repurpose or mutate gallery-dl's `_state/downloads.sqlite3` schema.
7. **Durability before completion:** a fetch target may not become captured
   until its observation is durable. Recovery must prefer harmless duplicate
   work over lost content.
8. **Idempotence and deduplication:** repeated discovery, reseeding, overlapping
   runs, shared ancestors, and restarts must not create duplicate logical work
   or duplicate normalized posts.
9. **Metadata before media:** media failures and large downloads must never
   block ancestor discovery or metadata closure. Context media uses its own
   resumable state and existing archive-style integrity checks.
10. **Bounded depth-first scheduling:** prefer closing the current ancestor
    chain, but park retry-delayed or pathological chains and enforce cycle,
    maximum-depth, and fairness guards.
11. **Explicit incompleteness:** distinguish captured, terminally unavailable,
    retryable, pending, and currently leased work. Never silently discard a
    missing parent or retry a permanent tombstone forever.
12. **No automatic production run:** tests, normal timeline runs, migrations,
    and dry-runs must not automatically start the historical context backfill.
    A live smoke test and production start are explicit operator actions.
13. **Credential hygiene:** never store cookie values, authorization headers,
    or sensitive response headers in SQLite, manifests, logs, fixtures, or
    generated datasets. Preserve private file permissions.
14. **Storage safety:** retain the existing writable-archive-root checks, free
    space safeguards, and refusal to fall back to local disk.
15. **Fail closed:** retain the exact gallery-dl compatibility pin and extend
    compatibility verification for any individual-post behavior on which the
    resolver relies.
16. **Preserve user work:** inspect and work around the repository's existing
    dirty state. Do not overwrite, revert, or reformat unrelated changes.

## Current Facts

These facts are a starting snapshot and must be rechecked before implementation:

- `scripts/archive_x.py` records `reply_id`, `reply_to`, and
  `conversation_id`, so existing raw timeline data can seed parent edges
  without recrawling the timeline.
- `build_gallery_config()` currently filters timeline records with target
  numeric authorship or repost shape, intentionally excluding external reply
  context.
- Timeline extraction uses `strategy: "with_replies"`, `expand: false`,
  `showreplies: false`, and `quoted: false`.
- The historical search phase queries `from:HANDLE`; relaxing the timeline
  post-filter alone cannot provide complete historical parent context.
- The archive binds each handle to a stable numeric user ID before timeline
  downloads.
- The archive already has exclusive locks, conservative sleeps, bounded
  retries, a version-checked gallery-dl 1.32.4 runner, resumable timeline
  cursors, media recovery, private umask behavior, immutable run artifacts,
  and atomically rebuilt derived datasets.
- gallery-dl supports individual status URLs and input files, but a static
  input file cannot discover the next ancestor dynamically.
- The existing rate-limit shim stores deferred state on one `TwitterAPI`
  instance. A sequence of individual extractors needs an outer persistent
  pacing invariant.
- As observed during the live Visakanv crawl on 2026-07-19, the partial
  archive already referenced roughly 92,300 unique external immediate parent
  IDs across roughly 52,200 conversations. These counts are changing and are
  sizing evidence, not constants for code or tests.
- The `sqlite3` shell command is not installed in the current environment;
  Python's standard-library `sqlite3` module is available and must be the
  implementation and test interface.
- The worktree already contains user changes in `README.md`,
  `scripts/archive_x.py`, `scripts/gallery_dl_x_runner.py`, and tests. Their
  exact state must be inspected at each stage rather than assumed from this
  snapshot.

## Assumptions Requiring Verification

- A pinned gallery-dl configuration can fetch exactly one focal post by
  numeric ID without yielding unrelated conversation entries.
- Metadata can be captured without requiring media completion and without
  losing the parent post's own `reply_id`.
- X/gallery-dl exposes enough error information to distinguish at least
  transient network/5xx/429 failures from deleted, private, suspended,
  withheld, and authentication failures. Ambiguous errors must remain
  retryable or explicitly unknown rather than guessed terminal.
- `conversation_id` normally identifies a root and can help group work, but
  correctness must come from explicit child-to-parent IDs rather than trusting
  it as a complete chain.
- A single per-user context database best preserves the current self-contained
  archive design. Cross-user/global deduplication is not required by this goal.
- Bounded depth-first metadata resolution produces more useful complete chains
  than global breadth-first resolution, provided blocked chains are parked and
  fairness is enforced.

## Required Durable Invariants

- Every discovered reply edge is durable even if its parent fetch has never
  run.
- Every fetch target is in exactly one operational state.
- Every captured target has a durable observation and provenance.
- Every unavailable target has a classified reason and observation time.
- A retryable target has a bounded attempt count and a future eligibility
  time; it cannot spin immediately.
- A stale lease is reclaimable after a crash.
- A parent discovered from multiple children is fetched at most once
  logically, while every child edge remains represented.
- A successful fetch that reveals another parent durably records both the
  observation and newly discovered edge/work before acknowledging completion.
- The timeline cursor can be deleted from the context subsystem's mental
  model: context code has no authority to modify it.
- Derived JSONL can be rebuilt deterministically from durable state and raw
  observations.

## Success Metrics and Verification Requirements

1. Offline fixtures demonstrate idempotent discovery from raw timeline JSONL,
   including duplicates, self-replies, external replies, shared parents,
   missing fields, and handle changes.
2. Queue-state tests prove legal transitions and reject captured-without-data,
   duplicate logical work, invalid leases, and unsafe schema versions.
3. Crash tests interrupt the resolver before and after each durable boundary
   and prove no target or edge is lost.
4. Scheduler tests prove chain-first traversal, convergence on shared
   ancestors, blocked-chain parking, fairness quantum behavior, cycle
   detection, and maximum-depth protection.
5. Rate tests prove only one request is active, persistent `not_before` is
   honored after restart, 429/reset waits are enforced, and transient retries
   cannot busy-loop.
6. Classification tests cover captured, deleted/unavailable, private,
   suspended/withheld where distinguishable, authentication failure,
   transient network/5xx, malformed metadata, and unknown errors.
7. Media tests prove metadata/ancestor closure can finish while context media
   remains pending or failed.
8. Dataset tests prove correct stable-ID authorship, context relationship
   labeling, edge joins, tombstones, provenance, deterministic ordering, and
   idempotent rebuilds.
9. Existing focused and full unit tests pass without making live X requests.
10. A dry-run reports intended database paths, counts, policies, and commands
    without writing archive state or contacting X.
11. An explicitly approved, very small live smoke test proves the focal-only
    extractor, pacing, raw capture, ancestor discovery, resume behavior, and
    private credential handling. It must not seed or launch the production
    backlog.
12. `git diff --check` passes, generated files are permission-safe, README and
    dataset documentation match behavior, and unrelated worktree changes are
    preserved.

## Indexed Stages

### 1-GUARDRAILS

#### Big Picture Objective

Turn the agreed safety principles into executable boundaries before adding
queue behavior or network work.

#### Detailed Implementation Plan

- Reinspect the current worktree, `archive_x.py`, runner shim, tests, README,
  live-run state formats, locks, permissions, and archive-root checks.
- Document an implementation decision record covering ancestor-only scope,
  timeline isolation, one-worker operation, metadata/media separation,
  approval-gated live work, and the precise meaning of closure.
- Add offline characterization tests for the current gallery-dl config and
  normalization behavior, including the reason external context is currently
  excluded.
- Prove from installed source or isolated fixtures which individual-post
  configuration yields only the focal post. Add a fail-closed compatibility
  assertion for that behavior before relying on it.
- Define named safety invariants and error taxonomy in code-facing terms so
  subsequent stages can test them.

#### Completion Requirements

- Current-state evidence is recorded in the stage file and folded back here.
- No live requests or archive mutations occur.
- Characterization tests pass and would fail if focal-only or context-filter
  assumptions changed.
- The stage explicitly lists every existing safety behavior that later code
  must preserve.

### 2-STATE

#### Big Picture Objective

Create a separate, schema-versioned SQLite state model with transactional
invariants strong enough for crash recovery and idempotent graph discovery.

#### Detailed Implementation Plan

- Define the minimum logical entities for reply edges, fetch targets,
  observations, unavailability records, attempts/leases, pacing state, schema
  migrations, and provenance.
- Use Python `sqlite3`; configure explicit transactions, foreign keys,
  integrity checks, private permissions, and conservative durability suitable
  for a single writer on the archive mount. Do not enable concurrency-oriented
  settings without evidence that they are safe and needed.
- Keep the database separate from gallery-dl's download archive.
- Implement idempotent schema creation and forward migrations that fail closed
  on unknown/newer schema versions.
- Make illegal states structurally impossible where practical and checked at
  transaction boundaries otherwise.
- Provide read-only status and integrity commands that do not require the
  external `sqlite3` CLI.

#### Completion Requirements

- Fresh-create, reopen, migration, unknown-version, permission, transaction
  rollback, uniqueness, foreign-key, and integrity tests pass.
- Tests prove a target cannot be marked captured without durable observation
  data and cannot occupy conflicting states.
- No existing archive database schema is modified.
- Status inspection works against an offline temporary archive.

### 3-DISCOVERY

#### Big Picture Objective

Seed durable reply edges and missing-parent work from existing and future
timeline metadata without recrawling X or coupling to timeline state.

#### Detailed Implementation Plan

- Parse raw/derived timeline records using stable numeric IDs.
- Insert every valid child-to-parent edge idempotently, retaining discovery
  run, timestamp, conversation ID, and target account provenance.
- Treat already archived target-authored parents as satisfied observations or
  locally resolved references without fetching them again.
- Enqueue only unresolved parent IDs; converge duplicates and shared ancestors.
- Add an explicit opt-in discovery/backfill command and a non-writing dry-run
  that reports candidate edges, unique parents, already-resolved parents, and
  malformed records.
- Integrate future discovery only after the main timeline metadata has been
  durably merged; context discovery failure must be reported separately and
  must not alter timeline success or cursor state.

#### Completion Requirements

- Fixture tests cover duplicate runs, repeated seeding, shared parents,
  self-replies, external replies, missing IDs, numeric identity across handle
  changes, and partial/corrupt JSONL boundaries.
- Repeating discovery yields zero new logical work.
- Tests explicitly prove context discovery cannot call timeline-state update
  functions or change their files.
- Dry-run performs no database, archive, cookie, or network mutation.

### 4-RESOLVER

#### Big Picture Objective

Resolve one numeric parent target at a time, capturing metadata durably and
discovering its next ancestor without expanding the conversation or coupling
to media success.

#### Detailed Implementation Plan

- Build a long-lived single-worker resolver around the SQLite queue rather
  than a precomputed static URL file.
- Fetch by numeric-ID URL so stale handles do not control identity.
- Use a pinned focal-only gallery-dl path with conversation expansion, quoted
  expansion, sibling replies, and descendants disabled.
- Capture raw metadata and normalized context provenance before marking work
  successful.
- Classify authorship against the archive-bound requested-user numeric ID and
  label external parent rows `relationship: "context"`.
- When a captured parent has its own valid parent ID, transactionally add the
  new edge and target before acknowledging the current result.
- Keep media discovery as metadata only at this stage; enqueue asset work but
  do not let it govern graph success.

#### Completion Requirements

- Offline extractor fixtures prove only the focal post is accepted.
- Resolver tests cover roots, parent replies, shared parents, target-authored
  ancestors, renamed handles, malformed responses, and exact provenance.
- No context record can be misclassified as target-authored because the
  individual extractor sets its own `user` field.
- One successful metadata fetch can advance an ancestor chain without any
  media download.

### 5-SCHEDULER

#### Big Picture Objective

Implement bounded depth-first, conversation-aware ancestor closure that
finishes ordinary chains quickly without allowing blocked or pathological
chains to starve the backlog.

#### Detailed Implementation Plan

- Prefer the newly discovered parent of the active chain until reaching a
  root, captured node, unavailable boundary, retry delay, cycle, maximum depth,
  or fairness quantum.
- Group and report work by conversation ID, but derive correctness from
  explicit reply edges.
- Prioritize conversations by unresolved impact, then current-chain locality,
  then a documented stable tiebreaker such as recency/ID.
- Park retry-delayed chains immediately and select eligible work elsewhere.
- Detect cycles and implausible depth without silently declaring closure.
- Make the fairness quantum configurable within conservative bounds and prove
  that normal chains close before breadth pressure dominates.

#### Completion Requirements

- Deterministic scheduler tests prove bounded depth-first order.
- Shared ancestors are fetched once and close every represented edge.
- A long chain, cycle, or delayed retry cannot starve a short eligible chain.
- Closure metrics distinguish fully closed, unavailable-boundary,
  partially resolved, retry-delayed, and pending conversations.

### 6-PACING

#### Big Picture Objective

Preserve the main archiver's conservative network behavior across a sequence
of individual-post jobs and across process restarts.

#### Detailed Implementation Plan

- Enforce the singleton worker with the existing lock philosophy and a
  context-specific lock/state boundary.
- Persist a global next-request eligibility time and relevant rate-limit
  bucket observations without storing sensitive headers.
- Apply conservative request and extractor delays before every new focal-post
  request, including the first request of a new extractor.
- Honor 429 and reset information, classify authentication/lock failures as
  global stops, and apply bounded exponential backoff with jitter to transient
  failures.
- Ensure SIGINT/SIGTERM leaves the current lease recoverable and never skips
  the required next wait.
- Add watchdog/readout behavior for repeated waits or no durable progress,
  modeled after the timeline's clean-stop philosophy rather than infinite
  looping.

#### Completion Requirements

- Fake-clock tests prove persisted waits survive restart and clock edges.
- Tests prove at most one request can be active and a second worker fails
  closed.
- 429, authentication failure, account lock, transient 5xx/network failure,
  and no-progress behavior are covered without real sleeping or network use.
- No test or ordinary archive invocation can accidentally bypass pacing.

### 7-RECOVERY

#### Big Picture Objective

Make interruption, partial writes, ambiguous failures, and operator retries
safe and understandable.

#### Detailed Implementation Plan

- Define legal transitions among pending, leased, captured, retryable,
  unavailable, and manual-review states.
- Reclaim stale leases conservatively and reconcile any durable observation
  written before an interrupted state transition.
- Classify permanent versus transient failures only when evidence supports the
  distinction; retain raw reason/error evidence with secrets removed.
- Provide explicit commands for status, retrying selected terminal/unknown
  cases, resetting exhausted transient work, and pausing/resuming safely.
- Bound attempts and surface manual-review work instead of looping forever.
- Add database integrity and recovery checks before each worker start.

#### Completion Requirements

- Fault-injection tests interrupt before/after lease, request, observation,
  discovered-parent insertion, completion, and retry scheduling boundaries.
- Every test recovers without lost edges or silently completed targets.
- Terminal and unknown states are reversible only through explicit operator
  action.
- Recovery cannot mutate timeline state or gallery-dl's download database.

### 8-MEDIA

#### Big Picture Objective

Archive context media safely as a secondary, resumable concern without
weakening metadata closure or duplicating assets.

#### Detailed Implementation Plan

- Create a separate media-work projection from captured context metadata.
- Reuse applicable checksum, `.part`, bounded-retry, download-archive,
  free-space, archive-root, and pending-media principles from the main script.
- Key assets by stable post/media identity and preserve external author,
  source URL, provenance, and relationship labeling.
- Keep graph closure successful when media is pending, unavailable, or failed.
- Make context-media download independently opt-in and observable.

#### Completion Requirements

- Tests prove metadata closure with failed, delayed, duplicate, and unavailable
  media.
- Repeated runs do not redownload completed assets or erase richer metadata.
- Interrupted media resumes when supported and never redirects storage to a
  fallback root.
- Dataset status distinguishes metadata-complete from media-complete context.

### 9-READOUT

#### Big Picture Objective

Produce deterministic, auditable datasets and operator reporting that make
context coverage and incompleteness explicit.

#### Detailed Implementation Plan

- Generate `context-posts.jsonl` and `reply-edges.jsonl`, or better names if
  current-state inspection establishes a clearer compatible convention.
- Include stable IDs, authorship, relationship, timestamps, conversation/root
  hints, provenance, capture observations, unavailability boundaries, and
  media status without secrets.
- Add status summaries for direct-parent coverage, ancestor closure,
  unavailable boundaries, retry backlog, active lease, rate wait, last durable
  progress, depth distribution, and media backlog.
- Make output ordering and rebuilds deterministic and atomic.
- Update the per-account dataset README and root README with semantics,
  limitations, explicit commands, and the fact that other people's posts are
  now optionally retained as context.

#### Completion Requirements

- Golden/fixture tests prove deterministic rebuilds and correct joins.
- Every reply edge resolves to captured, unavailable, retryable, or pending
  state in readouts; silent missing IDs fail verification.
- A database integrity/status command returns nonzero for broken invariants.
- Documentation distinguishes ancestor closure from whole-thread archiving.

### 10-ROLLOUT

#### Big Picture Objective

Prove the integrated subsystem safely and leave production backfill as a
deliberate, resumable operator action.

#### Detailed Implementation Plan

- Run all focused and full offline tests, database integrity checks,
  permission checks, deterministic rebuild checks, and diff/whitespace checks.
- Exercise dry-run against the real archive without modifying it or contacting
  X; compare reported candidate counts to independent read-only calculations.
- With explicit user approval, run a very small live smoke test against a
  disposable/test scope and verify focal-only capture, pacing, one ancestor
  transition, unavailable handling if safely reproducible, interruption, and
  resume.
- Audit logs, SQLite content, manifests, and JSONL for cookie/header leakage.
- Document the exact opt-in production commands, expected long duration,
  monitoring/readout commands, stop/resume behavior, and rollback boundaries.
- Do not start the full Visakanv backfill unless the user separately and
  explicitly authorizes that operational action.

#### Completion Requirements

- Focused and full tests pass with recorded commands and results.
- The live smoke test, if approved, demonstrates the exact production path; if
  approval is withheld, the goal remains honest about that unverified
  obligation rather than marking it complete.
- Dry-run and independent counts agree or discrepancies are explained and
  tested.
- No credentials appear in generated artifacts.
- `git diff --check` passes and the final diff contains no unrelated reversions.
- The implementation is ready for an explicit production start, and every
  remaining operational task is documented rather than hidden.

## Completion Boundary

This goal implements and verifies the safe context resolver. Completion does
not mean waiting for the roughly 92,000-plus historical parent requests to
finish, and it does not authorize starting that production workload. It does
mean the resolver is demonstrably safe, resumable, observable, focal-only,
ancestor-closing, and ready for an explicitly authorized backfill that can run
for days or weeks without weakening the main timeline archive.
