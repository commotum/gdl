# 5-FALLBACKS

## Current Facts

- Stage 3 records one accepted descriptor generation per post/ordinal and puts
  count-without-descriptor work in `needs_refresh` without rolling back post or
  frontier metadata.
- Stage 4 consumes usable descriptors with zero X calls. CDN 403/404/410 is
  deliberately left as `needs_refresh`; it never silently launches gallery-dl.
- Schema v3 already separates per-asset CDN attempts from per-owner
  `descriptor_refresh_jobs`, but the refresh lifecycle and worker are not yet
  implemented.
- The existing focal-only context fetch path can make a metadata-only exact
  post extraction and capture all file descriptors exposed for that post.

## Selected Policy

- Automatic fallback is grouped by post, not asset. One refresh generation
  covers every missing ordinal and has at most three logical attempts for
  transient/ambiguous failures across restarts.
- A successful exact observation is the only automatic refresh generation for
  that post. If its new descriptor is rejected again, 404/410 becomes explicit
  unavailable evidence and 403 or an inconsistent response becomes manual
  review; normal runs do not create another generation.
- Deleted, protected, suspended, and withheld exact-post responses retain their
  distinct terminal classes and close all unresolved assets for that post.
  Account/cookie authentication evidence persists an account-wide stop and
  raises immediately.
- Descriptorless compatibility jobs first perform a local sidecar/hash check.
  Verified complete bytes are committed without any request. Only unresolved
  work is queued for exact refresh.
- Profile assets do not spend an extra exact-post request. The mandatory info
  observation is their refresh source; a still-rejected current profile URL is
  unavailable/manual review until a genuinely changed profile descriptor or
  explicit operator repair appears.
- Operator repair creates a new generation explicitly and is the only normal
  way to reopen terminal refresh/asset evidence. It does not occur implicitly
  on restart.
- Alert when automatically refreshed post owners exceed 2% of post owners with
  media descriptors once the denominator reaches 100. This is a quality alarm,
  not a reason to increase request density or fail archive metadata.

## Big-Picture Objective

Resolve exceptional missing or rejected descriptors with one durable,
post-grouped exact lookup while keeping ordinary direct media at zero X calls,
preserving terminal/auth semantics, and preventing restart retry loops.

## Detailed Implementation Plan

- Add indexed refresh enqueue, claim, stale-lease recovery, success, retry,
  terminal, authentication-stop, and explicit operator-repair methods to the
  SQLite owner.
- Reconcile verified local files before enqueueing any compatibility refresh.
- Parameterize the existing focal-only extractor so exact refresh descriptors
  are provenance-labeled `exact_refresh` and actual calls are separately
  counted without changing conversation behavior.
- Persist all returned descriptors for the one focal post in the same guarded
  completion transaction, reopening every covered asset ordinal together.
- Close missing ordinals conservatively after the successful observation:
  absent media becomes unavailable; claimed media without a usable descriptor
  becomes manual review.
- Pace through the existing per-account SQLite scheduler, persist rate reset,
  and honor/persist the same account-wide authentication stop. Stage 6 will
  move this reservation to the actual-call boundary without changing budgets.
- Add deterministic tests for bypass, local compatibility reconciliation,
  multi-asset grouping, expiry, terminal post states, transient ceilings,
  hidden exact fallback accounting, auth stop, stale leases, interruption,
  operator repair, privacy, and miss-ratio alerting.

## Safety Invariants

- A usable descriptor or verified local file can never enter the X refresh
  worker.
- Only one automatic refresh generation exists per post; restarts preserve its
  attempt count and lease.
- Network/file work occurs outside SQLite write transactions; every result is
  token-guarded on commit.
- A refresh response can persist descriptors only for its exact focal post and
  declared media count.
- No post-terminal response is collapsed into CDN unavailability, and no CDN
  response is mislabeled as post deletion/protection.
- Authentication stop is durable and prevents the next account request before
  sleeping or launching an extractor.
- Full URLs, cookies, headers, handles, and raw exception text never enter
  telemetry, refresh errors, progress, or this stage document.

## No-Cheating Checks

- The ordinary descriptor fixture records zero refresh claims and zero X
  requests.
- A three-asset missing fixture records one logical exact extraction and makes
  all three assets directly eligible.
- A verified compatibility file is captured with zero network calls.
- Three transient attempts remain one refresh generation; a fourth run makes
  no request.
- A refreshed descriptor rejected by CDN cannot enqueue generation two.
- Operator repair is an explicit API action and receives a new generation and
  lease identity.

## Completion Requirements

- Missing/stale/compatibility fixtures use no more than the documented budget
  and return covered assets to direct transfer.
- Multi-asset refresh is one post lookup, never one lookup per ordinal.
- Deleted/private/suspended/withheld/auth/transient/ambiguous outcomes retain
  distinct durable behavior.
- Restart, stale lease, interrupt, completion fault, and operator repair are
  idempotent with no false completion or retry loop.
- Focused, full, fingerprint, compile, diff, and deliberate review checks pass
  before Stage 6.

## Results Ledger

- Added `archive_x_refresh.py` as a post-grouped descriptor refresh worker. It
  reconciles verified compatibility files locally first, creates at most one
  automatic generation per post, and refreshes every unresolved ordinal from
  one focal-only extraction. Exact observations are labeled `exact_refresh`,
  while hidden bootstrap/detail calls are counted as actual
  `descriptor_refresh` requests.
- Extended schema v3 with durable refresh enqueue/claim/reclaim/completion,
  random lease ownership, attempt ceilings, account-wide authentication stop,
  explicit operator generations, refresh-quality aggregates, and a
  `destination_scope`. The latter retains authored, context, and profile path
  ownership during refresh instead of inferring every exact lookup as context.
- The exact production refresh claim scans
  `refresh_jobs_ready(owner_kind, next_attempt_at, refresh_id)` and creates no
  temporary ordering tree. Network and file work occur outside write
  transactions; completion remains token-guarded and reclaimable after a
  process or commit fault.
- A usable descriptor bypasses refresh. A verified local compatibility file
  repairs the gallery download ledger and asset state with zero network work,
  including when an authentication stop is active. Stale author-derived
  filenames are found by sidecar identity rather than causing a lookup.
- A successful exact observation cannot create a second automatic generation.
  Subsequent CDN 404/410 evidence closes the asset unavailable; 403,
  inconsistent media counts, missing declared ordinals, and unknown legacy
  destinations terminate in manual review. Deleted, private, suspended, and
  withheld post evidence remains separately classified.
- Transient and ambiguous outcomes receive no more than three durable logical
  attempts across restarts. Interrupts spend no attempt, stale leases resume
  the same generation, and injected completion or local-I/O faults cannot
  produce false success. Explicit `repair-descriptor POST_ID` is the sole
  ordinary generation-two path.
- Authentication failure atomically records a sanitized account-wide stop and
  releases the lease. `auth-stop` reports it and `auth-stop --clear` is the
  explicit operator recovery action; normal archive invocation gained no new
  required flag.
- Current profile descriptors are supplied by the already-mandatory info
  observation rather than another exact-post call. A rejected unchanged
  profile URL stays terminal until a genuinely changed info descriptor or an
  explicit repair reopens it.
- Added 21 focused fallback tests covering zero-call bypass and local repair,
  three-asset grouping, exact-call accounting, terminal distinctions,
  generation/attempt ceilings, auth persistence and clearing, restart and
  interruption, operator repair, profile behavior, privacy, quality alerting,
  and transaction boundaries. Full repository discovery passed 309 tests;
  both pinned runners reported `1.32.4`; `compileall` and `git diff --check`
  passed.
- No live request, production database migration, archive mutation, or
  orchestration cutover was performed. Stage 9 will place this worker between
  descriptor capture and direct-media completion; until then the existing
  archive command remains behaviorally unchanged.

## Stage Results

Stage complete. Stage 6 may replace the remaining logical-target pacing with
one durable actual-call scheduler without changing the fallback budgets or
terminal evidence policy.
