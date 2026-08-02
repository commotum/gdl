# 7-LEGACY

## Current Facts

- Legacy coverage currently claims fixed three-day root windows. A request-cap
  result splits the active leaf exactly in half, but a sparse successful window
  cannot enlarge the next interval and a dense-but-uncapped window cannot
  shrink it.
- Each leaf keeps only an in-memory `previous_valid` result. Any intervening
  invalid attempt clears that evidence, and a process restart discards it even
  though the immutable valid raw/telemetry artifacts remain on disk.
- Confirmation currently requires two consecutive matching valid walks. Three
  valid but pairwise different observations enter manual review only after all
  attempts; a valid mismatch is not durable evidence in the frontier state.
- Each walk launches the legacy search extractor, which resolves the same
  profile again. Stage 1 observed exactly one extra API call per Carmack walk:
  39 walks, 118 search API calls, and 157 total API calls.
- Every confirmed root window calls `update_post_dataset()`, rereading and
  rewriting the complete posts, authored-posts, and repost views and hashing
  the full posts view before the next window begins.
- Stage 1 measured 19 Carmack windows adding 100 posts. Its posts and authored
  views were about 56 MiB each, implying roughly four GiB of repeated logical
  I/O. This is read-only historical evidence; no production archive is being
  used in this stage.
- Schema v3 already has numeric account binding, normalized `archive_posts`,
  source/provenance, archive/export generations, descriptor jobs, and the
  Stage 6 request lane. It does not yet record a committed legacy interval.
- The source query already uses exact epoch seconds with a one-second overlap,
  validates accepted records against half-open UTC bounds and numeric identity,
  requires an explicit request cap and distinct empty-tail proof, and splits a
  capped leaf down to one-second precision.
- A read-only process check found no archive/legacy runner beyond the check
  itself. This stage uses temporary fixtures only and does not start a live
  archive.

## Baseline Ledger

- Real read-only Carmack evidence: 19 fixed root windows, 39 fresh walk
  processes/profile resolutions, 118 SearchTimeline calls, 157 total API
  calls, 100 canonical posts, and about four GiB of repeated logical portable-
  view I/O.
- Deterministic sparse benchmark definition: 360 days, fixed three-day roots,
  two matching walks per root, three SearchTimeline pages per walk, one profile
  API call per fresh walk, no caps, and no returned canonical posts. Baseline:
  120 roots, 240 walks, 720 search calls, 240 profile calls, and 960 total API
  calls.
- Adaptive target on the same fixture: begin at three days, double sparse
  intervals, cap at 90 days, and clip exactly at the account floor. Expected
  widths are 3, 6, 12, 24, 48, 90, 90, and 87 days: eight roots, 16 walks, 48
  search calls, and zero repeated profile calls after the mandatory numeric-ID
  binding. This is a 93.3% search-call and 95% total legacy-API reduction.
- Dense/capped fixture definition: identical exact bounds and records under a
  fixed three-day baseline; a valid walk using at least five of six request
  slots shrinks the next root, while a capped response splits the current root
  deterministically. Adaptive and baseline canonical sets/frontiers must match.
- Current local export cost is K full dataset merges, three full view writes,
  and K posts-view hashes for K committed root windows. Stage target is one
  coalesced compatibility export per ordinary run and one indexed transaction
  per committed window.

## Updated Assumptions

- Window growth is safe only after two matching valid observations with ample
  request-cap headroom. Growth never changes an already claimed interval and a
  later cap still forces complete deterministic subdivision of that interval.
- A valid observation is reusable only when its immutable raw and telemetry
  files, hashes, exact bounds, query digest, numeric account identity, request
  cap, and empty-tail proof all revalidate. A path reference or manifest flag
  alone is not evidence.
- Independence means separate source-visible SearchTimeline observations and
  immutable artifacts, not a new process, cookie jar, or profile lookup. The
  same artifact cannot acquire two observation identities.
- Invalid non-contradictory attempts add no confirmation evidence and do not
  erase valid evidence. Any two valid compatibility digests that differ are a
  contradiction and must stop the interval for manual review rather than be
  outvoted or forgotten.
- The mandatory modern info probe and state/database numeric binding can supply
  the search extractor's requested-account identity. Returned authored records
  still carry and must match numeric author identity; repost scope remains
  bound to the verified query account. No handle-only trust is introduced.
- Schema v3 is still an undeployed Goal 5 migration in this worktree, so a
  narrow committed-legacy-interval table may be finalized in the same version
  and required by the fail-closed catalog.
- Stage 7 may coalesce the compatibility JSONL export once per run. Stage 8
  still owns eliminating that remaining full-history merge through generic
  incremental ingestion and generation-based export.

## Big-Picture Objective

Reduce sparse legacy API and local-I/O cost without weakening the two-
observation completeness standard: persist each valid observation, adapt only
future roots from confirmed density/headroom, commit each window to indexed
truth, and materialize portable compatibility views at most once per run.

## Detailed Implementation Plan

- Add a versioned adaptive policy to legacy frontier state with persisted next
  interval seconds, a one-day minimum, a 90-day maximum, and a sanitized last
  decision. Initialize old states lazily from their configured three-day root;
  an active window's exact bounds never change on resume.
- Grow a confirmed sparse interval by two, retain a moderate interval, and
  shrink dense or subdivided intervals by two. Clip all roots at the exact
  account-creation floor. Persist the decision in the same atomic frontier
  commit as the completed window.
- Extend active leaves with a bounded valid-observation ledger containing only
  immutable relative paths, hashes, query/compatibility digests, allowlisted
  counters, operation identity, and descriptor evidence references.
- Revalidate retained observations from raw/telemetry artifacts on every use.
  Match any two same-digest valid observations across invalid attempts or
  restart; reject any conflicting valid digest. Persist a valid result before
  requesting another confirmation.
- Add a private numeric account binding to the pinned legacy runner. Replace
  its per-search profile resolution with a version-checked minimal requested-
  account assignment, record the bound identity source, and keep every raw
  record's existing numeric/bounds validation. Preserve ordinary runner mode
  and source fingerprints.
- Add a schema-v3 `legacy_intervals` table and bounds index. Commit canonical
  normalized posts, source/provenance, the confirmed interval, archive
  generation, and dirty export generations in one short SQLite transaction,
  with no file or network I/O inside it.
- Add a bounded pending-portable-export ledger to legacy state. Verify and
  coalesce all pending canonical window files, then call the compatibility
  dataset materializer exactly once per ordinary run. A crash may leave a
  dirty export but cannot roll back indexed/frontier truth or claim it current.
- Preserve raw snapshots, descriptors, media queueing, exact split order,
  window limits, manual review, modern cursor, restrictive modes, and progress
  events. Stage 9 will connect the Stage 6 worker/pacer and remove the old walk/
  window sleeps; Stage 7 does not weaken pacing on its own.

## Safety Invariants

- Every newly confirmed leaf has two distinct, hash-verified, source-visible
  valid observations with identical accepted IDs and stable metadata.
- A transient invalid attempt cannot delete valid evidence. A mismatching valid
  observation cannot be ignored, overwritten, or outvoted.
- Exact `[since, until)` UTC coverage, one-second query overlap, request cap,
  distinct empty-tail proof, cursor checks, numeric record identity, canonical
  dedupe, leaf contiguity, split limits, and account floor remain mandatory.
- Adaptive growth applies only to the next unclaimed root. Capped current work
  is completely split and confirmed; no interval is skipped or inferred from
  density.
- The verified numeric account ID comes from existing mandatory identity
  evidence and must match JSON and SQLite bindings. No additional account,
  proxy, cookie, header, fingerprint, or paid service is introduced.
- SQLite transactions contain validation/upserts only. Raw verification,
  hashing, descriptor reads, JSONL aggregation/export, and all network work
  occur before or after the transaction.
- Indexed interval/frontier truth may be ahead of a portable export only when
  the export is explicitly dirty/pending. A stale JSONL is never labeled
  current.
- Crash or `Ctrl-C` after one valid walk, two valid walks, indexed commit,
  frontier commit, or export placement remains replayable and cannot fabricate
  a completed interval or duplicate an observation.

## No-Cheating Checks

- Count SearchTimeline, profile, support/bootstrap, runner, and session starts
  separately. Process reuse or a renamed lookup cannot hide an API call.
- The sparse 360-day fixture compares exact canonical/frontier results and
  actual modeled calls, not elapsed sleep time or number of Python functions.
- Dense/capped fixtures compare every canonical post ID and exact coverage
  interval with the fixed baseline. Adaptive windows may not drop results to
  obtain a lower request count.
- A valid-invalid-valid sequence confirms; valid-A/valid-B cannot confirm even
  if a later observation matches A. Restart reuses one retained valid artifact
  and performs only the missing independent observation; two references to the
  same artifact never confirm.
- Legacy telemetry must report zero UserByScreenName API calls when bound
  identity is supplied, while raw numeric validation still rejects a wrong
  author/account.
- K confirmed windows cause K indexed commits but at most one full compatibility
  materialization/hash pass. An interrupted export remains pending rather than
  being reported current.

## Completion Requirements

- Sparse, dense, capped, floor-clipped, empty, repost, and overlap fixtures
  produce exactly the fixed-baseline canonical set and frontier.
- The sparse fixture lowers SearchTimeline calls by at least 50%; profile lookup
  count is one per mandatory proven account/session boundary, not one per walk.
- Evidence survives transient invalid attempts and restart, mismatches stop,
  same-artifact duplication fails, and corrupt/missing retained artifacts fail
  closed.
- Crash tests cover after first valid persistence, second valid persistence,
  split state, indexed commit, frontier commit, compatibility export placement,
  and pending-export clearance.
- Exact production query and interval index plans avoid a table scan/temp sort;
  SQLite lock/fault tests leave restartable state.
- Descriptor/media provenance and filenames remain compatible; no newly
  confirmed descriptor work falls back to an exact post lookup.
- Focused request-ledger, state, runner, fingerprint, compile, diff, privacy,
  and full-suite checks pass without live network or production mutation.

## Results Ledger

- Added a persisted versioned adaptive policy with exact one-day and 90-day
  bounds. Confirmed sparse roots double only for the next unclaimed interval;
  dense/headroom-limited roots shrink, capped current work still subdivides to
  exact half-open leaves, and the account floor clips the final root exactly.
- The deterministic 360-day sparse fixture now uses root widths of 3, 6, 12,
  24, 48, 90, 90, and 87 days. SearchTimeline calls fall from 720 to 48
  (93.3%), while eliminating the repeated profile call lowers the complete
  modeled legacy API ledger from 960 to 48 (95.0%).
- A separate 90-day end-to-end source fixture ran both fixed and adaptive
  policies through the actual confirmation, indexed-commit, frontier, and
  portable-export path. Both committed the exact same six canonical post IDs;
  fixed windows used 60 independent walks and adaptive windows used 10.
- Every valid observation is now written to the active leaf before another
  request. Restart revalidates immutable raw and telemetry paths, hashes,
  exact query/bounds, numeric identity, request cap, empty-tail proof, and
  compatibility digest. `valid -> invalid -> valid` confirms, one retained
  observation needs one new walk, two retained observations need no network,
  and a mismatch, reused artifact, missing artifact, or changed hash fails
  closed.
- The pinned legacy runner accepts the already verified numeric account ID and
  bypasses `UserByScreenName` without weakening returned-record identity
  checks. Telemetry proves zero profile API requests for a bound walk and the
  ordinary unbound runner behavior remains available and fingerprint-guarded.
- Added indexed `legacy_intervals` truth. Each confirmed window registers its
  canonical source, upserts normalized posts/provenance, records exact bounds
  and two-observation evidence, advances the archive generation, and dirties
  the affected export views in one short SQLite transaction. The exact bounds
  lookup uses `legacy_intervals_bounds` with no temporary ordering tree.
- A K-window run now performs K small indexed commits and one coalesced
  compatibility materialization. The two-window fixture records two interval
  commits but calls the full JSONL materializer once. A failed publication
  leaves the portable export explicitly pending; the next complete-state run
  repairs it without any X request.
- Fault fixtures cover `Ctrl-C` after the first valid observation, failure
  after the second, capped split persistence, indexed-commit rollback and
  idempotent replay, frontier write failure, manifest failure after state
  commit, export placement failure, and pending-export clearance. SQLite
  rollback leaves generation/post counts unchanged and retryable.
- Confirmed legacy descriptor artifacts are hash-bound to both observations,
  selected only for the canonical post, and produce a direct asset job with no
  exact-post refresh job. Repost, empty, and one-second overlap fixtures retain
  exact canonicalization and frozen repost policy.
- Public manifests omit the executable command and raw search query. Raw
  evidence remains private under the user archive; query identity is retained
  only as a digest.
- The Stage 7 cross-module gate passed 129 tests. Full repository discovery
  passed 359 tests; both pinned runners reported gallery-dl `1.32.4`, and
  `compileall` plus `git diff --check` passed. No live request or production
  archive mutation was used.

## Stage Results

Stage complete. Stage 8 may treat committed `archive_posts`,
`archive_sources`, `legacy_intervals`, dirty export generations, and pending
portable-export evidence as durable indexed truth while removing the remaining
whole-history ordinary-operation work.
