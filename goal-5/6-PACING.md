# 6-PACING

## Current Facts

- Every current modern/profile/retry endpoint, legacy walk, context target, and
  descriptor refresh launches a fresh pinned runner. Context and refresh make
  a durable reservation per logical target, while modern/legacy rely mainly on
  gallery-dl request sleeps plus outer endpoint/walk/window sleeps.
- A logical target is not an actual-call boundary. One TweetResult operation
  can make a bootstrap/support request, its intended API request, and a hidden
  TweetDetail fallback. Redirects and lower transport retries are likewise
  separate actual attempts.
- Stage 1 proved that urllib3 `_make_request` and urllib `do_open` observe the
  installed stack's actual attempts. Those hooks already classify requests
  without retaining a URL, query, header, cookie, post ID, handle, or cursor.
- Schema v3 has one per-account `pacing` row with a durable not-before time,
  rate-reset time, reservation token/start, and authentication stop, but the
  reservation fields are not yet enforced at the actual transport boundary.
- Gallery-dl creates one Requests session per extractor by default. Injecting
  an account-scoped session before extractor initialization reduced 1,000
  synthetic extractor sessions to one.
- The Stage 1 control-protocol model used 11 process starts for 1,000 items
  with a 100-item cap and one crash, versus 1,000 starts currently. It retained
  237 acknowledged predecessors and replayed only the item that had begun but
  not returned a result.
- Repeated calls to `gallery_dl.main()` share process-global configuration and
  logging state unless each item explicitly resets them. Process reuse without
  per-item cleanup is therefore not an acceptable implementation.

## Baseline Ledger

- Context/exact/refresh: one runner start and normally one new Requests session
  per logical target; current durable wait is outside the runner and cannot see
  hidden actual calls.
- Modern/profile/retry: one runner start per endpoint, gallery-dl
  `sleep-request=4-8s`, `sleep-extractor=2-5s`, and outer endpoint delay
  `10-20s` where applicable. The first info endpoint has no shared durable
  boundary with the next phase.
- Legacy: one runner/session and one repeated profile resolution per walk,
  request sleep inside the extractor plus independent walk/window sleeps.
- Carmack's read-only Stage 1 sample contained 39 walk starts, 118 search API
  calls, and 39 additional API calls consistent with profile resolution.
- Existing request artifacts report actual attempts, minimum within-process
  spacing, peak concurrency, session starts, and connection starts. They do
  not yet attribute durable scheduler wait source, and each per-target artifact
  currently reports a runner start even if a future worker is reused.

## Updated Assumptions

- The authoritative gate must run immediately before each classified X
  transport attempt, not before a subprocess, extractor, target, or
  `TwitterAPI._call`. Media CDN and unrelated external requests are outside the
  X lane.
- A short SQLite `BEGIN IMMEDIATE` may claim/release the one request lease;
  network and sleeping remain outside the transaction. A killed process may
  leave one lease, so the lease has a conservative stale horizon longer than a
  supported HTTP attempt and is restart-reclaimable.
- Every successful reservation schedules the next gap from its actual start.
  A long idle period never accumulates credits, so restart or quota-reset wakeup
  cannot produce a catch-up burst.
- Low-quota and 429 reset evidence is persisted at response completion. The
  pinned gallery-dl handler remains authoritative for its current in-process
  response behavior; sharing the absolute durable boundary does not add calls.
- Bounded reuse will be account-scoped, sequential, and parent-controlled. One
  command receives one `begin` and one token-matched `result`; the worker never
  commits archive queue truth. It retires after at most 100 items or its age
  limit, whichever comes first.
- The first Twitter extractor initializes cookies and the Requests session in
  the normal pinned implementation. Later extractors in the same worker may
  reuse that session only within the same validated account scope. The session
  is closed when the bounded worker retires.

## Big-Picture Objective

Make every real X attempt pass through one durable, restart-safe account lane,
then provide a bounded account worker that safely reuses imports, cookies,
connections, and sessions across sequential items. This stage establishes and
proves the mechanisms; Stage 9 performs the one-command orchestration cutover.

## Detailed Implementation Plan

- Add a narrow pacing module that validates the bound account database, claims
  one actual-request lease, waits outside transactions, persists spacing/reset/
  429 boundaries, reclaims only stale leases, and blocks on durable auth state.
- Extend the actual-boundary recorder with an optional authoritative gate and
  sanitized wait-source/timing fields. Scheduler failure is fail-closed even
  though telemetry-file failure remains observability-only.
- Add strict private runner options for scheduler database, numeric account
  scope, gap range, request-lease horizon, and 429 backoff. All scheduler
  options are required together and are removed before gallery-dl parses its
  arguments.
- Add a versioned newline-JSON control protocol shared by the base and legacy
  pinned runners. Commands carry only an ephemeral item ID, durable lease
  token, account-scope digest, and argument vector; acknowledgements never echo
  URLs, cookies, paths, or arguments.
- Emit `begin` before archive work and a token-matched `result` afterward.
  Retire cleanly after the item/age cap. On crash, EOF leaves only the current
  parent-owned lease unresolved; completed results cannot be replayed as
  unacknowledged work.
- Install an account-scoped Twitter session pool inside worker mode. Reset
  gallery-dl configuration/logging output state after every item, while
  retaining only the deliberately shared session and pinned monkey patches.
- Add a parent control client with one active item, bounded shutdown and
  interrupt handling, stderr streaming, protocol validation, and explicit
  worker-loss evidence. It must never retry or commit an item itself.
- Keep `sleep-request`, extractor, endpoint, walk, and window delays unchanged
  in the existing orchestration until Stage 9 routes all X-capable phases
  through the gate. Tests will prove which layers can then be set to zero
  without reducing actual-call spacing.

## Safety Invariants

- At most one classified X request is in flight for the bound account. CDN and
  external traffic cannot acquire or alter the X reservation.
- A scheduler reservation is committed before the network attempt; its release
  is token guarded. No network call or sleep occurs inside a SQLite write
  transaction.
- Authentication/account-stop evidence is checked before waiting and again
  before claim. No phase, worker restart, or operation label can bypass it.
- Every actual start schedules a fresh next gap. Neither restart, long idle,
  expired lease, reset wakeup, nor process replacement creates request credit.
- Rate-limit reset and 429 evidence can lengthen but never shorten the already
  committed not-before boundary.
- Worker reuse is sequential, same-account only, maximum 100 items and bounded
  age. It does not rotate identity, cookies, proxies, headers, or fingerprints.
- The parent remains the sole owner of durable target/asset/window commits.
  A worker result without the current lease token has no authority.
- Protocol, request telemetry, scheduler state, errors, and stage artifacts
  contain no private URL, query, cookie, header, signed token, handle, post ID,
  or opaque cursor.

## No-Cheating Checks

- A bootstrap, redirect, intended API call, retry, and hidden TweetDetail each
  acquire a distinct actual-call reservation; one logical operation cannot
  collapse them.
- CDN calls are still counted but record zero X-gate acquisitions.
- A two-scheduler deterministic timeline proves peak X concurrency one and no
  gap below the configured floor, including stale-lease recovery and restart.
- A persisted future not-before/reset makes a new process wait; a long clock
  advance permits only one immediate claim, not a burst.
- A durable auth stop blocks info, timeline, retry, profile, context, refresh,
  and legacy operation labels before an actual request.
- A 1,000-item fixture with a kill after `begin` meets at least 98% runner-start
  reduction, preserves every prior result, and replays at most the killed item.
- Per-item configuration/logging cleanup tests prove one command cannot leak
  its config or handlers into the next. Session tests prove one account session
  is reused and a different scope is rejected.

## Completion Requirements

- Deterministic actual-call timelines prove one X lane, no catch-up burst,
  durable restart/reset/429 behavior, and spacing at least the retained 4s
  request-delay floor once redundant logical sleeps are modeled as removed.
- Reservation crash/expiry, SQLite busy/fault, response exception, auth stop,
  challenge classification, Ctrl-C, worker EOF, stale result, and clean
  retirement are restart-safe and fail closed.
- Base and legacy runners retain their pinned source/version fingerprints and
  support ordinary one-shot invocation unchanged when private control/pacing
  options are absent.
- The production control implementation meets the 1,000-item startup metric,
  shares a same-account session, and bounds unfinished work to one item.
- Focused, full, fingerprint, compile, diff, privacy, and deliberate review
  checks pass before Stage 7. No production archive or live network is used.

## Results Ledger

- Added one account-bound `DurableRequestScheduler` at the actual urllib3/
  urllib transport boundary. It gates X API, X web/bootstrap/support, and X
  redirect attempts while leaving media-CDN and unrelated external attempts
  outside the X lane. Scheduler arguments are private, all-or-none, and bound
  to the database's numeric account identity before any request can run.
- Selected a retained `4-8s` actual-request gap, a conservative `180s` stale
  request-lease horizon, and a `300s` fallback for a 429 without a usable reset.
  Stage 9 supplies these defaults when it performs the orchestration cutover;
  existing endpoint/extractor/walk sleeps remain unchanged until then.
- Deterministic restart/long-idle timelines kept every X start at least four
  seconds apart, peak X concurrency at one, and produced no catch-up credit.
  A phase-by-phase model removed every outer logical sleep and still kept the
  four-second actual-call floor across all current operation labels.
- Persisted spacing, low-quota reset, 429, active-lease, and authentication-stop
  state in schema v3. The exact `1..5` random quota threshold chosen by the
  pinned runner reaches response completion unchanged. Reset/429 evidence can
  lengthen, never shorten, an existing boundary.
- A killed request leaves one token-guarded lease and becomes reclaimable only
  after its stale horizon. SQLite busy/setup/completion faults, stale tokens,
  account mismatch, gate-install/removal failure, and auth evidence fail closed.
  `Ctrl-C` during a wait spends no request; auth evidence appearing during that
  wait prevents the claim on wakeup.
- Request telemetry schema v2 records sanitized pacing wait time/source and
  reused-worker start counts while continuing to normalize existing v1
  artifacts. Hidden TweetResult-to-TweetDetail fallback creates two actual X
  reservations; CDN work creates none.
- Added a strict versioned parent/worker protocol and one same-account Twitter
  session pool. Workers are sequential and retire after at most 100 completed
  items or 15 minutes by default. Per-item gallery-dl configuration, logging,
  and renderer state is cleared; sessions close on retirement. The legacy
  pinned runner is a safe superset, so ordinary and legacy work can share one
  bounded process/session after Stage 9 routes them through it.
- The production worker loop processed the deterministic 1,000-item fixture in
  10 runner/session starts instead of 1,000: a 99% reduction, above the required
  90%. A forced process exit after `begin` preserved every prior result and
  left only the current token-matched item unresolved for replay.
- Renderer/callback failure is observability-only and cannot deadlock the
  output barrier. Unterminated carriage-return-style output is separated from
  the barrier. Worker EOF, broken pipe, stale/wrong acknowledgements, interrupt,
  age/item retirement, and shutdown paths close descriptors without
  `ResourceWarning` failures.
- Focused pacing/control/telemetry/base-runner/legacy-runner/schema tests passed
  under `-W error::ResourceWarning`. Full repository discovery passed 344
  tests in 16.941 seconds. Both pinned runners reported `1.32.4`; `compileall`,
  `git diff --check`, permissions review, privacy review, and deliberate diff
  inspection passed. No live request or production archive mutation was used.
- Actual network-call counts do not change in this mechanism-only stage. Stage
  9 must wire every X-capable unified-command phase through the gate, reuse the
  worker, and remove stacked sleeps as one atomic cutover; accepting the new
  modules while retaining old orchestration is explicitly not the final
  performance result.

## Stage Results

Stage complete. One durable actual-call lane and one bounded account worker are
proved safe for integration. Stage 7 may optimize legacy intervals without
inventing another pacing or identity mechanism.
