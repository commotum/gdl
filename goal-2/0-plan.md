# Goal 2: Safe Pre-Snowflake X Backfill

Shorthand: `X-LEGACY`

## Big-Picture Objective

Repair the conservative X timeline archiver so it can continue safely across
Twitter's November 2010 transition from Snowflake tweet IDs to legacy
sequential IDs. The repair must archive whatever older posts X still exposes,
without replaying the completed modern history, skipping unknown ranges,
weakening the no-progress watchdog, or allowing an ambiguous API response to
be mistaken for historical completion.

The preferred architecture is a distinct, date-windowed legacy backfill phase
with durable progress independent of gallery-dl's Snowflake `max_id`
arithmetic. That architecture remains a hypothesis until installed-source
characterization, offline fixtures, and a tightly bounded live diagnostic
prove the actual search behavior. If another strategy is demonstrably safer,
the evidence and decision must be recorded before implementation proceeds.

This goal is scaffolding only until explicitly started. Creating these files
does not authorize code changes, live X requests, production state migration,
or restarting Visakanv.

## Scope and Completion Boundary

In scope:

- Detect and represent the transition into pre-Snowflake history.
- Fetch older history using bounded, contiguous UTC date windows rather than
  interpreting legacy IDs as Snowflake timestamps.
- Resume safely after errors, rate limits, process crashes, and operator
  interruption.
- Preserve existing raw records, datasets, media state, timeline cursor
  evidence, context state, identity binding, locks, pacing, and manifests.
- Make gaps, unavailable intervals, API exhaustion, and completion claims
  explicit and auditable.
- Prove the production path with a small approval-gated live diagnostic and a
  bounded Visakanv rollout that advances earlier than October 29, 2010.

Out of scope unless evidence makes it essential to this repair:

- Reply-context backfill or changes to `context.sqlite3`.
- Whole-thread, sibling, descendant, or quote-source expansion.
- Concurrent workers, proxies, credential rotation, or rate-limit evasion.
- Claiming recovery of deleted, private, withheld, or search-index-omitted
  posts that X does not expose.
- Waiting for the entire 2008–2010 production crawl to finish as a condition
  for code completion. The implementation must be proven resumable and ready
  for deliberate continuation; the long production operation remains an
  explicit operator decision.

## Non-Negotiable Constraints and No-Cheating Rules

1. **Preserve the proven boundary.** Keep the stopped run, raw JSONL, manifest,
   and saved `3_29116490825/` cursor as evidence. Never overwrite or reinterpret
   them destructively.
2. **Separate pagination domains.** Do not pass legacy IDs through Snowflake
   timestamp arithmetic. Do not silently encode date progress in an ordinary
   stage-3 gallery-dl cursor.
3. **Contiguous coverage.** Legacy work must use explicit half-open UTC
   intervals `[since, until)` whose union is contiguous. No interval advances
   until its raw observations are durable and merged.
4. **No false completion.** An empty page, repeated post, repeated cursor,
   timeout, 429, API error, or watchdog stop is not proof that an interval—or
   the account's history—is complete.
5. **Prefer duplicate work to gaps.** After an ambiguous interruption, replay
   the active date window and deduplicate by tweet ID rather than advancing its
   lower boundary.
6. **Retain the watchdog.** Do not disable or inflate the existing three-window
   no-progress guard to push past a broken paginator.
7. **Bound every operation.** One worker, bounded windows, bounded HTTP retries,
   persistent conservative delays, and explicit limits for diagnostics and
   smoke tests.
8. **Stable identity.** Accept posts only under the archive-bound numeric user
   ID `16884623`; handles are query locators, not authorship authority.
9. **Timeline-state isolation.** A failed legacy window cannot advance normal
   incremental state, erase the modern resume evidence, or mark a full crawl
   successful.
10. **Metadata before media.** A download-only failure must remain pending media
    and must not cause a completed metadata window to be replayed forever.
11. **Fail-closed compatibility.** Keep gallery-dl pinned to reviewed source
    behavior. Fingerprint any additional upstream method on which the legacy
    path relies.
12. **Credential and storage hygiene.** Keep private permissions, mounted-root
    checks, cookie-value redaction, no local-disk fallback, and no sensitive
    headers in fixtures or state.
13. **No automatic migration.** Existing accounts must not enter legacy mode
    merely because updated code is installed. Initialization requires an
    explicit, stale-guarded operator action after dry-run inspection.
14. **No automatic production restart.** Offline tests and dry-runs may not
    contact X. Live diagnostics and Visakanv continuation require explicit
    approval at their designated stages.
15. **Preserve user work.** The current worktree contains an unrelated modified
    `x.txt`; implementation must inspect and preserve it and any later changes.

## Confirmed Current Facts

- Visakanv's latest run is
  `20260720T023918Z-cf57e4`; it ended `stalled`, and tmux session `x` is an idle
  bash shell.
- The run lasted about 22.4 hours and durably merged 77,360 new timeline
  records. The cumulative dataset contains 258,065 posts, of which 257,981 are
  labeled target-authored and 84 are reposts.
- The oldest archived record is tweet `29116490825`, posted
  `2010-10-29 19:30:34` UTC.
- The record stream visibly crosses the ID transition: November 5, 2010 posts
  have IDs around `4e14`, while November 4 and earlier posts have sequential
  IDs around `2.9e10`.
- After reaching `29116490825`, four logged checkpoints remained
  `3_29116490825/`; no raw metadata arrived across three complete rate-limit
  windows, so the existing watchdog cleanly stopped the endpoint.
- Both `_state/state.json` and the run manifest saved the advanced cursor
  `3_29116490825/`. The stale shutdown message containing
  `3_1173685814485643265/` did not overwrite it. The previous cursor-selection
  repair therefore worked.
- The isolated `RemoteDisconnected` download warning recovered and was not the
  stall cause. Two media assets remain pending independently.
- The profile says the account was created `2008-10-21 12:01:00` UTC and
  currently reports 274,859 statuses. This strongly suggests older material
  may exist, but neither creation date nor status count proves that every
  missing status is searchable or older than the boundary.
- Installed gallery-dl 1.32.4 constructs historical timeline queries with
  `max_id:TWEET_ID`. Its search paginator updates a Snowflake boundary using
  `(id - 0x400000) | 0x3fffff`, while its date conversion also derives time
  from Snowflake bits. Neither behavior is valid for old sequential IDs.
- The existing main archiver already provides stable numeric identity,
  exclusive locks, private files, bounded retries, immutable run evidence,
  rate-limit checkpoint logging, dataset deduplication, pending-media recovery,
  cursor recovery, and a no-progress watchdog.
- Goal 1's optional reply-context resolver is separate and must not be started
  or changed by this repair.

## Assumptions Requiring Proof

- X's current SearchTimeline path still exposes at least some Visakanv posts
  before October 29, 2010.
- A query bounded by `since:YYYY-MM-DD until:YYYY-MM-DD` avoids the legacy-ID
  failure and can terminate reliably using server cursors within that window.
- `until` is exclusive and `since` is inclusive in the actual endpoint used;
  fixtures alone cannot establish server semantics.
- A one-day UTC window is small enough to enumerate completely for this
  account. If not, the design needs an explicit smaller-window or manual-review
  fallback rather than silently truncating a busy day.
- A successful terminal response can be distinguished from missing cursors,
  API errors, inaccessible search history, and repeated-page behavior.
- Querying by the current canonical handle finds historical posts authored
  under the same numeric account identity. Every returned record must still be
  checked against the stable numeric ID.
- X may impose an undocumented historical-search floor. If so, the archive
  must report “source exhausted/unavailable before DATE,” not “complete to
  account creation.”

## Recommended Target Design

### Separate Legacy State

Add a schema-versioned `legacy_backfill` object under the existing per-user
state rather than overloading gallery-dl's `3_.../` cursor. Its minimum logical
fields should include:

- lifecycle state: `not_initialized`, `pending`, `active`, `complete`, or
  `manual_review`;
- immutable initialization provenance: source run, source cursor, oldest
  observed post ID/date, account-creation lower bound, and initialization time;
- `initial_until` and `next_until` UTC dates;
- active half-open window `[since, until)` and attempt/progress metadata;
- last completed window and completion timestamp;
- bounded retry/manual-review reason without cookie or response secrets;
- explicit source-coverage conclusion distinct from historical certainty.

Maintain an O(1) contiguous frontier: if `next_until` is `D`, every initialized
window from `D` through `initial_until` is durably complete. Detailed evidence
lives in immutable per-window run manifests rather than an ever-growing JSON
array.

### Date-Windowed Fetching

Use a dedicated legacy endpoint/query rather than the normal timeline stage:

- Query the canonical handle within explicit UTC `[since, until)` dates.
- Default to one-day windows for the first proven implementation. Larger
  adaptive windows are permitted only after tests show they cannot hide a
  per-query cap or gap.
- Use X's returned cursor only inside one fixed date query. Never carry that
  opaque cursor into a different interval.
- Reapply the stable numeric-author filter and existing repost policy.
- Preserve raw JSONL, config, log, manifest, checksums, and pending-media
  evidence under the normal run structure with an unambiguous legacy-window
  endpoint name.
- On a fully successful terminal response, merge raw metadata and pending
  media, atomically persist `next_until = since`, then select the next window.
- On interruption, repeated cursor/page, API error, or ambiguous empty result,
  retain the same window for replay.
- Stop at the UTC day containing account creation. Do not query indefinitely
  before the account existed.

### Explicit Initialization and Rollout

Provide a non-writing dry-run that derives the proposed initial window from
the preserved run evidence and reports all state mutations. Initialization
must use stale guards for the exact account ID, source run, source cursor,
oldest post ID/date, and absence of existing legacy state. It must be
idempotent and reversible by restoring the prior state file; it must not delete
the stage-3 cursor or old run.

Normal archive behavior for accounts without initialized legacy state remains
unchanged. Once initialized, a deliberate legacy-only or clearly documented
resume command processes a bounded number of windows. Whether normal timeline
invocations should automatically resume initialized legacy state is a design
decision gated on recovery tests and operator ergonomics; installation alone
must never initialize or launch it.

## Success Metrics and Verification Requirements

1. An offline fixture reproduces the exact transition and stall:
   `402691293450240` → `29675373972` → `29116490825` → repeated boundary.
2. Tests prove legacy dates come from returned metadata/window state, never
   Snowflake decoding of sequential IDs.
3. Query-generation tests prove adjacent `[since, until)` windows are
   contiguous, non-overlapping except for an explicitly documented safe
   replay overlap, and stop at the account-creation day.
4. A window cannot advance on 429, API error, missing terminal evidence,
   repeated cursor/page, malformed metadata, identity mismatch, watchdog stop,
   or operator interruption.
5. Fault-injection tests cover crashes before raw finalization, after raw
   finalization, during dataset merge, after merge but before state commit, and
   after state commit. Recovery produces duplicates at worst, never a gap.
6. Existing 258,065 dataset rows remain present and deterministic after legacy
   fixture merges. Replayed transition-day posts deduplicate by stable ID.
7. Download-only failures advance metadata coverage while remaining explicit
   pending media; extraction/API failures do not.
8. Unknown/new legacy-state versions and changed initialization evidence fail
   closed.
9. Normal timeline, context, recovery, cursor, runner, and dataset tests remain
   green without live network calls.
10. A dry-run against Visakanv performs no write and reports the exact proposed
    starting window, lower bound, state path, locks, limits, and command.
11. An explicitly approved disposable live diagnostic proves whether date
    windows return posts older than `2010-10-29 19:30:34` and establishes
    actual `since`/`until` and terminal-cursor semantics.
12. An explicitly approved bounded production smoke captures at least one
    older Visakanv post or produces decisive evidence that X exposes none,
    survives stop/resume, and leaves the next window auditable.
13. Credential scans, private-mode checks, runner fingerprints, database/state
    integrity, `git diff --check`, and the full test suite pass.
14. Documentation states exactly what “legacy backfill complete” means and
    distinguishes source-visible coverage from proof of all historical tweets.

## Indexed Stages

### 1-EVIDENCE

#### Big Picture Objective

Freeze and independently verify the production boundary before changing any
pagination or state behavior.

#### Detailed Implementation Plan

- Reinspect tmux/process state, the latest state file, manifest, timeline log,
  raw tail, dataset minimum date/ID, profile creation date/status count, pending
  media, locks, and dirty worktree.
- Copy only non-sensitive minimal transition records into offline fixtures.
- Record the exact modern/legacy discontinuity, repeated checkpoint sequence,
  raw-progress timestamps, selected cursor, and watchdog outcome.
- Characterize current normal timeline, search, cursor, runner, and state
  behavior with tests before modifying it.
- Explicitly inventory Goal 1/context boundaries that this goal must not touch.

#### Completion Requirements

- Evidence is reproducible from immutable run artifacts and contains no cookie
  values, signed media URLs, or sensitive headers.
- A fixture-driven characterization test fails for the current legacy loop but
  confirms the advanced cursor was safely retained.
- Current worktree changes are recorded and preserved.
- No implementation code, production state, process, or network is changed in
  this stage.

### 2-CHARACTERIZE

#### Big Picture Objective

Determine which X/gallery-dl pagination primitive can enumerate pre-Snowflake
history without guessing.

#### Detailed Implementation Plan

- Trace the installed gallery-dl 1.32.4 search extractor, query generation,
  server-cursor handling, result-stop rules, ID transformation, and date
  transformation. Record exact source fingerprints.
- Build offline response fixtures for legacy `max_id - 1`, current Snowflake
  arithmetic, fixed date windows with server cursor, empty terminal pages,
  repeated pages, and missing/error responses.
- Define a small approval-gated live diagnostic matrix against a disposable
  archive: at most one or two narrow dates around October 28–29, 2010; metadata
  only; one worker; explicit request cap; no production state.
- Compare candidate strategies using evidence: corrected legacy-ID decrement,
  date-window search, native server cursor, or a fail-closed combination.
- If X exposes no older result through any safe primitive, record that as an
  upstream limitation and design an honest manual-review boundary rather than
  fabricating completion.

#### Completion Requirements

- Offline fixtures deterministically reproduce current behavior and candidate
  behavior.
- Any live diagnostic has separate explicit approval, a hard request bound,
  disposable output, retained non-secret evidence, and no production writes.
- The stage ends with a documented decision explaining why the selected
  primitive is safer than every rejected alternative.
- No production cursor or archive invocation is changed.

### 3-SPEC

#### Big Picture Objective

Turn the selected primitive into precise coverage, state-transition, and
completion semantics before implementation.

#### Detailed Implementation Plan

- Specify half-open UTC interval semantics and prove adjacency algebraically.
- Define legacy lifecycle states, initialization provenance, active-window
  fields, contiguous-frontier invariant, retry/manual-review states, and
  source-coverage conclusions.
- Define what constitutes terminal success, retryable failure, permanent
  source unavailability, identity failure, and ambiguous no-progress.
- Decide the initial window and account-creation floor from stored evidence,
  including safe boundary overlap and deduplication.
- Specify whether an initialized legacy backfill is resumed by a dedicated
  command or a normal invocation; require bounded/operator-visible behavior.
- Define manifest/raw naming and how legacy data joins existing datasets and
  pending media without granting it normal cursor authority.

#### Completion Requirements

- Every state transition has preconditions, durable writes, recovery behavior,
  and negative tests identified.
- Coverage cannot advance without terminal evidence and a successful merge.
- Completion language distinguishes “all successfully enumerated windows” from
  “all tweets ever posted.”
- The specification is reviewed against the non-negotiable constraints before
  any production-path code is written.

### 4-STATE

#### Big Picture Objective

Implement durable, versioned, stale-guarded legacy progress without disturbing
the authoritative modern timeline evidence.

#### Detailed Implementation Plan

- Add pure validation/normalization helpers and the minimal legacy state model.
- Add non-writing initialization planning and explicit atomic initialization.
- Preserve source cursor/run/date/ID provenance and reject mismatched account,
  changed evidence, unknown schema versions, invalid date order, and
  non-contiguous transitions.
- Implement atomic claim/complete/retry/manual-review operations for one active
  window using the existing state-writing durability model.
- Ensure legacy operations cannot clear normal resume, last-successful state,
  pending media, identity binding, recovered-run lists, or context state.

#### Completion Requirements

- Fresh, absent, repeated, stale, corrupt, unknown-version, and identity-change
  tests pass.
- Transaction/fault tests prove the prior state survives a failed write.
- Tests compare unrelated state byte-for-byte or structurally before/after.
- Initialization remains opt-in and dry-run remains write-free.

### 5-FETCHER

#### Big Picture Objective

Fetch exactly one bounded legacy date window with reviewed, focal account
semantics and immutable evidence.

#### Detailed Implementation Plan

- Generate the chosen search URL/config from structured dates and canonical
  handle; prevent arbitrary query injection.
- Reuse the pinned runner, cookies, request delays, retry limits, download
  limits, postprocessors, raw JSONL, hashes, and numeric-author filter.
- Disable conversation/quote expansion and other unrelated capture paths.
- Keep any opaque server cursor scoped to the active fixed query.
- Add a compatibility fingerprint for every new gallery-dl method relied upon.
- Classify output as terminal success, media partial, transient/API failure,
  identity violation, repeated/no-progress, or interrupted without inferring
  success from missing data.

#### Completion Requirements

- Config/query fixtures prove the exact `[since, until)` scope and reject
  malformed dates/handles.
- Pagination fixtures prove no result outside the window or wrong numeric
  author is accepted into authored data.
- Compatibility checks fail closed on changed installed source.
- Ordinary tests perform no live requests and use no production paths.

### 6-ORCHESTRATE

#### Big Picture Objective

Safely drive a bounded descending sequence of legacy windows while preserving
the main archive's one-worker and metadata-before-media behavior.

#### Detailed Implementation Plan

- Add an explicit window/request bound; never default to an unbounded historical
  run.
- Acquire the same repository/archive locks used by normal timeline and context
  workers.
- For each eligible window: write provisional manifest, fetch, finalize raw,
  merge posts, merge pending media, rebuild derived data, then atomically
  advance the contiguous frontier.
- Stop promptly on interruption, authentication/identity failure, ambiguous
  pagination, watchdog, storage error, or manual-review state.
- Retain conservative inter-request/endpoint delays and persisted rate-reset
  behavior across subprocesses where applicable.

#### Completion Requirements

- Deterministic tests prove window order, hard bounds, lock exclusion, pacing,
  and immediate stop behavior.
- Multiple windows produce contiguous coverage with no skipped UTC date.
- Media-only failure advances metadata once and remains pending; API failure
  does not advance.
- Installing or invoking ordinary timeline/context commands cannot launch an
  uninitialized legacy backfill.

### 7-RECOVERY

#### Big Picture Objective

Make every interruption point replay-safe and prevent stale shutdown cursors
or partial windows from corrupting legacy progress.

#### Detailed Implementation Plan

- Inject faults before request, during request, before/after raw finalization,
  during dataset merge, before/after state commit, and during manifest
  finalization.
- Reconcile abandoned manifests only when raw evidence, terminal status, and
  dataset merge can be proven; otherwise replay the same window.
- Ensure a stale gallery-dl `Use -o cursor=...` line cannot replace the date
  frontier or modern cursor.
- Bound retries and surface repeated intervals as manual review instead of
  looping forever.
- Add explicit operator retry/reset operations with stale guards and audit
  provenance.

#### Completion Requirements

- Fault tests lose no interval and advance no ambiguous interval.
- Recovery is idempotent across repeated startups.
- Replayed raw posts deduplicate without erasing richer observations.
- Modern cursor recovery tests and the exact prior two failure regressions
  remain green.

### 8-READOUT

#### Big Picture Objective

Make legacy coverage, remaining dates, failures, and historical uncertainty
obvious to the operator and downstream dataset users.

#### Detailed Implementation Plan

- Add dry-run/status output for lifecycle state, source boundary, completed
  frontier, active/next window, account floor, attempts, last progress, pending
  media, manual-review reason, and exact next command.
- Record per-run/window query bounds, terminal evidence, raw counts, unique
  counts, oldest/newest returned dates/IDs, and state transition.
- Extend dataset documentation with legacy coverage semantics and limitations.
- Add deterministic coverage/readout data only if it materially improves audit
  and can be atomically rebuilt from state/manifests.
- Never display cookies, signed headers, or sensitive response material.

#### Completion Requirements

- Golden tests cover pending, active, retryable, manual-review, source-exhausted,
  and complete readouts.
- Every completion claim links to contiguous window evidence.
- Dry-run and status are network-free; dry-run is write-free.
- Documentation explicitly says the repair cannot recover posts X withholds or
  no longer indexes.

### 9-VERIFY

#### Big Picture Objective

Prove the integrated repair offline and establish that existing archive
behavior has not regressed.

#### Detailed Implementation Plan

- Run transition, query, state, orchestration, recovery, fault, media, dataset,
  permission, lock, and redaction tests.
- Run the full repository suite and exact gallery-dl compatibility preflight.
- Exercise dry-run against Visakanv and independently verify its proposed dates
  and preserved production state hashes/fields.
- Verify no production legacy state, run directory, context database, or X
  request is created by tests or dry-run.
- Audit the diff for unrelated worktree changes, generated artifacts, unsafe
  permissions, secrets, whitespace, and accidental watchdog weakening.

#### Completion Requirements

- Focused and full tests pass with commands/results recorded.
- Dry-run agrees with independent read-only calculations.
- Production state is unchanged and tmux remains stopped.
- `git diff --check` and credential/permission audits pass.

### 10-ROLLOUT

#### Big Picture Objective

Demonstrate real pre-2010 progress safely, then leave the long backfill in an
explicit, resumable operator-controlled state.

#### Detailed Implementation Plan

- Obtain explicit approval for a disposable, metadata-only live diagnostic of
  one narrowly bounded date window older than October 29, 2010.
- Verify query boundaries, stable authorship, focal scope, terminal evidence,
  request count, pacing, raw capture, and no credential leakage.
- If the diagnostic succeeds, obtain separate approval for stale-guarded
  Visakanv legacy-state initialization and a one-window production smoke.
- Stop and resume that same window or the immediately following window to prove
  recovery without gaps.
- Compare dataset counts/oldest date, manifests, state frontier, pending media,
  file modes, locks, and logs before and after.
- Only after all evidence passes, document the bounded continuation command.
  Do not launch the long 2008–2010 run without separate operator instruction.

#### Completion Requirements

- A real returned post older than the current boundary is durably archived, or
  decisive bounded evidence records an upstream source limitation honestly.
- Production smoke state is contiguous, integrity-checked, resumable, and
  independently auditable.
- Existing modern data and context state are unchanged except for intended
  deterministic dataset enrichment.
- All post-smoke focused/full tests, compatibility checks, permission/secret
  audits, and `git diff --check` pass.
- The final handoff names the exact stopped/running process state, next window,
  next command, pending failures, and whether long production continuation is
  authorized.

## Completion Boundary

This goal is complete when the archiver has a proven, fail-closed mechanism for
crossing the legacy-ID boundary, can durably capture and resume at least one
older production window, and reports any remaining source limitations and
operational work honestly. It is not complete merely because a query returned
zero items, a cursor changed, tests passed without exercising the transition,
or the watchdog was disabled. It does not implicitly authorize the full
2008–2010 production backfill.

