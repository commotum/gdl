# Goal 5: Efficient Low-Footprint X Archive

Shorthand: `LEAN-X-ARCHIVE`

## Big-Picture Objective

Make the ordinary unified X archive command materially faster and gentler by
eliminating redundant X requests, repeated client/bootstrap work, unnecessary
retries, and whole-archive rewrites while preserving the safety properties that
made the current downloader trustworthy.

The command remains:

```bash
uv run scripts/archive-x --user USERNAME
```

The immediate trigger for this goal is the context-media design: the current
workflow first extracts a context post without downloading, then later sends
the same post through another X extractor solely to rediscover its media URLs.
Goal 5 must make the first bounded extraction produce durable media descriptors
for accepted posts and download those assets directly from the media CDN.

The full-downloader review found other high-value work that belongs in the same
goal:

- reuse descriptors for failed authored and legacy media instead of re-fetching
  each post from X;
- use profile image/banner descriptors already returned by the mandatory info
  probe instead of running separate X profile extractors every time;
- make legacy search windows adaptive, reuse valid confirmation evidence, and
  stop resolving the same verified profile once per walk;
- count actual X API/bootstrap requests rather than treating one subprocess as
  one request;
- pace every X call through one durable per-account request lane so redundant
  layers of sleep can be removed without creating bursts;
- amortize or safely reuse extractor processes/sessions when measurements prove
  that doing so reduces bootstrap and connection churn;
- incrementally index posts, media, context seeds, and queue priorities instead
  of repeatedly scanning, rewriting, and hashing hundreds of megabytes or
  gigabytes for small updates;
- materialize portable JSONL exports from durable indexed truth at controlled
  commit/export boundaries rather than after every legacy window or unchanged
  rerun.

The intended lifecycle is:

```text
one durable per-account X request lane
  -> mandatory identity proof + incremental modern extraction
  -> adaptive, independently confirmed legacy extraction when applicable
  -> bounded context extraction with accepted post + media descriptors
  -> short SQLite commits for posts, edges, sources, descriptors, and jobs
  -> direct CDN transfers from persisted descriptors
  -> bounded exact-post refresh only when a descriptor is absent or stale
  -> one coherent incremental/export checkpoint and live aggregate telemetry
```

This folder is scaffolding only. It does not authorize implementation, schema
migration, a live smoke, or interruption of a production archive.

## Low Footprint, Not Evasion

The operational aim is to minimize unnecessary traffic and behave predictably
and gently toward X and its CDNs. No design can promise that automation is
"undetectable," and this goal must not attempt fingerprint spoofing, identity or
cookie rotation, proxy rotation, challenge/CAPTCHA bypass, header mimicry, or
other evasion techniques.

The allowed strategy is simpler and safer:

- make fewer real requests;
- keep at most one X request in flight;
- persist the next eligible request time across phases and restarts;
- honor rate-limit resets, 429s, authentication stops, and account locks;
- avoid retrying deterministic failures;
- reuse already returned data and established sessions within bounded,
  testable lifetimes;
- prevent catch-up bursts after a wait, crash, or restart;
- give CDN transfers their own bounded lane and bandwidth budget;
- use no paid API, paid proxy, or paid external service.

## Operator Outcome

The operator should run one command and see useful work begin without having to
choose window sizes, media modes, or repair scripts. In normal operation:

- a context post with reusable media descriptors needs no second X tweet
  lookup;
- a failed authored/legacy media asset retries from its saved descriptor before
  any exact-post refresh;
- unchanged profile media causes no separate X extractor and no CDN transfer;
- legacy search keeps independent completeness evidence while avoiding fixed
  tiny windows and repeated profile lookups;
- X request spacing remains conservative enough to be service-friendly, but
  delays are attached to actual calls rather than stacked at process, endpoint,
  and phase boundaries;
- large videos or CDN failures cannot block or roll back metadata discovery;
- a tiny timeline update does not force several gigabytes of JSONL reads,
  writes, hashes, and fsyncs;
- already seeded raw sources are not rehashed and reparsed on every invocation;
- queue selection remains fast as the context database grows;
- dashboards use durable counters and stay observability-only;
- `Ctrl-C`, restart, low disk, expired descriptors, unavailable posts, and
  partially written files remain resumable and auditable.

## Prioritized Design Objectives

1. **Correctness and provenance first.** Every retained post, edge, descriptor,
   file, boundary, and cursor must remain tied to authoritative source evidence.
2. **Measure actual network work.** Count HTTP/API calls by safe host category,
   logical operation, retry, and outcome; a subprocess count is insufficient.
3. **Zero redundant happy-path media lookups.** Context, authored, legacy, and
   profile media should use descriptors returned by work already performed.
4. **One durable X lane.** All phases and refreshes share account-scoped pacing,
   quota resets, authentication stops, and restart-safe not-before times.
5. **Fewer bootstraps and reconnects.** Reuse bounded processes/sessions only
   where it reduces real bootstrap/TLS work and preserves per-item durability.
6. **Efficient legacy proof.** Retain two independent matching observations for
   new legacy evidence while using adaptive windows, persisted valid evidence,
   and one verified identity binding per run/session.
7. **Metadata independent from bytes.** Media slowness or failure cannot roll
   back metadata, reply edges, cursors, or legacy frontier state.
8. **Incremental durable state.** Apply small deltas to indexed storage; make
   portable JSONL files reproducible materialized views rather than the unit of
   every internal commit.
9. **No repeated historical ingestion.** Process a committed raw source once in
   ordinary operation, with a separate explicit full-integrity audit path.
10. **Bounded local overhead.** Queue claims, progress refreshes, recovery scans,
    and exports should scale with actionable/new work, not total history, unless
    an explicit audit/export was requested.
11. **Minimal exterior.** No new mandatory flags, windows, phase commands, or
    operator bookkeeping.
12. **Proven improvement.** Request ledgers, query plans, I/O accounting, fault
    tests, and deterministic benchmarks must demonstrate the gain.

## What Must Stay Conservative

The audit must not mislabel these safeguards as inefficiencies:

- the numeric account-ID identity guard before archive mutation;
- one X request lane and global authentication/account stops;
- X rate-limit and 429 waits;
- the incremental timeline overlap unless equivalent no-gap evidence replaces
  it;
- cursor replay after a crash before durable commit;
- bounded conversation scope, focal-post validation, max depth, and cycle
  detection;
- independent legacy observations, exact half-open UTC bounds, request caps,
  empty-tail evidence, and split-on-cap behavior;
- atomic raw snapshots, restrictive file modes, checksums, sidecars, and file
  verification;
- durable retry budgets and explicit unavailable/manual-review outcomes;
- global archive locks and fail-closed gallery-dl compatibility fingerprints;
- read-only dashboard behavior.

An optimization may replace one of these only if it supplies directly tested,
at-least-equivalent evidence. Convenience or speed alone is not proof.

## Confirmed Audit Findings

### Request and process accounting

- `scripts/archive_x_context.py` launches a fresh Python/gallery-dl runner for
  each claimed context target. The process rebuilds extractor/client state and
  cannot reuse its HTTP session across targets.
- Context's `counts["requests"]` counts calls to `fetch_post()`, not actual X API
  calls. An exact TweetResult lookup can internally fall back to TweetDetail,
  and client/bootstrap traffic is not represented by that logical count.
- Modern, retry-media, avatar, background, context, and each legacy walk are
  separate runner processes.
- Modern request spacing currently combines gallery-dl request delay (`4-8s`),
  extractor delay (`2-5s`), and outer endpoint delay (`10-20s`). Those layers
  are not one durable account-wide schedule and can stack even when only one
  real request boundary needs protection.
- Context alone has durable SQLite pacing. Modern and legacy mainly rely on
  process-local sleeps, so a restart cannot inherit one authoritative
  next-request time across all phases.

### Duplicate media discovery

- Context metadata uses one bounded TweetDetail response with `--no-download`.
  Its saved post JSON contains media `count` but no reusable URL, variant,
  extension, or filename descriptor.
- The context-media phase therefore performs a focal-only X extraction for
  every incomplete media-bearing context post before downloading bytes.
- Failed authored media is retained in `state.json`, then retried one post at a
  time through a fresh exact-post X extractor.
- Legacy search is metadata-only and queues media-bearing posts by count; it
  does not retain the file descriptors needed for a direct CDN transfer.
- Media CDN requests are unavoidable. The avoidable work is re-querying X to
  rediscover file URLs already present during an earlier extraction.

### Profile work

- The info probe is safety-critical because it binds the archive to a stable
  numeric account ID.
- The same info record already contains current profile-image and banner
  descriptors.
- After every successful unbounded timeline run, the archiver still waits and
  invokes separate avatar and background X extractors. Gallery-dl may skip an
  unchanged file through its ledger, but the separate process/extractor work
  has already occurred.
- A safer optimization target is therefore descriptor-based conditional CDN
  download from the mandatory info result, not removal of the identity probe.

### Legacy search

- The unified legacy configuration uses fixed three-day root windows, up to
  three attempts, and requires two consecutive matching valid walks.
- A transient invalid walk clears the previous valid result. A
  `valid -> API error -> matching valid` sequence can become manual review even
  though the two valid observations agree and the middle result provides no
  contradictory evidence.
- Valid partial-window evidence is not generally reused as an independent
  confirmation after restart; repeated work can be required.
- Read-only Carmack evidence showed 19 three-day windows, 39 walk processes,
  118 search requests, and 157 total API requests to add 100 posts. The exact
  one-extra-API-call-per-walk pattern is consistent with repeated profile
  resolution in each fresh search process.
- Thirty-eight of those walks ended with valid distinct-empty-tail evidence;
  this sparse workload is a strong candidate for adaptive window growth while
  retaining two independent complete observations and split-on-cap safety.

### Whole-dataset and source I/O

- `update_post_dataset()` loads the full `posts.jsonl`, merges a small delta in
  memory, sorts all records, and atomically rewrites `posts.jsonl`,
  `authored-posts.jsonl`, and `reposts.jsonl`.
- Legacy calls that function and hashes the full resulting posts dataset after
  every committed root window.
- In the observed Carmack run, the posts and authored views were roughly
  56 MiB each. Nineteen window commits therefore caused on the order of four
  GiB of repeated logical read/write/hash I/O to add 100 posts, excluding raw
  files, manifests, temporary copies, and fsync overhead.
- The Visakanv portable posts and authored views are roughly 891 MiB each.
  A small modern-head delta currently rebuilds both full files.
- `update_media_dataset()` recursively scans every media sidecar and rebuilds
  the complete media JSONL after timeline work and at several interruption or
  retry boundaries.
- `canonical_seed_sources()` scans all run manifests and hashes every committed
  raw source each time context seeding starts.
- `seed_context()` then parses every authoritative source once to rediscover
  global reply candidates and parses each new source again during insertion.
  It still performs the first historical pass when every source is already
  registered as seeded.
- Visakanv's committed raw post snapshots total roughly 602 MiB, so an
  unchanged context-seed pass can hash and read hundreds of megabytes before
  doing no new work.
- `export_datasets()` loads all context observations/edges and rewrites the
  complete context JSONL views every unified run. The observed Visakanv
  context post/edge exports total roughly 406 MiB.

### SQLite queue and observability

- The context `targets` table has only its primary-key index. It has no index
  for metadata state/eligibility, media state/eligibility, or lease expiry.
- The metadata claim query scans all targets, executes a correlated parent-edge
  count, and creates a temporary ordering B-tree for every claimed item. The
  read-only query plan confirms `SCAN t` and `USE TEMP B-TREE FOR ORDER BY`.
- Stale-lease reclamation also runs before every claim.
- The current production context databases are already large enough for this
  to matter (about 130 MiB for Carmack and 1.8 GiB for Visakanv at audit time).
- Live progress refreshes grouped context counters every five seconds and
  periodically scans manifests/closure state. Observability must be benchmarked
  and moved to maintained counters/current pointers if it measurably contends
  with the worker, but it is lower priority than request and commit-path work.

## Prioritized Efficiency Backlog

| Priority | Work | Why it belongs in Goal 5 | Safety condition |
|---|---|---|---|
| P0 | Actual host/category request ledger | No request-saving claim is reliable without it | Redact URLs, queries, cookies, and headers |
| P0 | Context descriptor reuse | Removes the dominant one-X-lookup-per-media-post behavior | Persist only descriptors for accepted posts |
| P0 | Shared authored/legacy descriptor reuse | Stops exact-post re-fetch as the default repair path | Bounded refresh on missing/stale descriptor |
| P0 | One durable account-wide X scheduler | Allows redundant sleep layers to be removed without bursts | One X lane; persist resets/not-before across restart |
| P1 | Info-derived profile media | Removes two routine X extractors per full run | Keep the mandatory numeric identity probe |
| P1 | Adaptive legacy windows + evidence reuse | Fixed tiny windows dominate sparse history | Preserve two matching valid observations and split on cap |
| P1 | Reuse verified legacy identity/session | Avoids one profile call per walk | Numeric identity remains bound and every record is validated |
| P1 | Bounded runner/session reuse | Reduces Python, transaction-key, TLS, and cookie churn | Per-item commits, max age/work cap, clean auth stop |
| P1 | Incremental post/media indexes | Avoids full JSONL and sidecar rebuilds for tiny deltas | Raw snapshots stay authoritative and exports reproducible |
| P1 | Incremental context source ingestion | Avoids hashing/parsing old raw history every run | Mutation detection plus explicit full-integrity audit |
| P1 | Indexed/materialized queue priority | Removes full target scan/sort per item | Preserve depth/fairness/shared-parent semantics |
| P1 | Generation-aware context export | Avoids 400+ MiB unchanged rewrites | Never report stale export as current |
| P2 | Current-manifest pointers and recovery indexes | Bounds startup/dashboard scans as run count grows | Manifests remain immutable audit evidence |
| P2 | Further CDN variant/concurrency tuning | May save failed variant probes or idle time | Separate one-lane default, bandwidth cap, measured need |

P0 and P1 items are part of the goal's required design and verification. A
P1 experiment may be rejected only with recorded evidence that it yields no
material benefit or cannot preserve the stated safety condition; it may not be
silently skipped. P2 work is implemented only if Stage 1/8 measurements cross
the agreed overhead threshold.

## Non-Negotiable Constraints and No-Cheating Rules

1. Do not claim fewer requests by counting subprocesses or logical fetches.
   Count actual X API, X bootstrap/support, CDN, retry, and redirect calls.
2. Do not hide an X request inside gallery-dl, yt-dlp, a fallback, a compatibility
   path, or a persistent worker.
3. Do not enable unrestricted media download on a conversation response.
   Returned-but-rejected nearby posts cannot create files or durable jobs.
4. Context selection is authoritative. Descriptor artifacts remain provisional
   until the capture transaction accepts their exact post IDs.
5. Require structured post ID, media ordinal, type/variant, and provenance;
   never infer descriptor ownership from ordering or a filename alone.
6. Keep cookies, authorization data, signed headers, transaction secrets, and
   full descriptor URLs out of logs, telemetry, fixtures, dashboards, and
   stage documents. Store private URLs with restrictive permissions.
7. Do not weaken identity, focal-post, bounded-conversation, reply-edge,
   max-depth, cycle, cursor, legacy-boundary, or record-scope guards.
8. Do not reduce actual per-call spacing merely because process-level sleeps
   were removed. First install and prove durable request-level pacing.
9. At most one X request may be in flight. Any overlap is CDN-only and must
   remain independently bounded/stoppable.
10. Do not rotate accounts, cookies, proxies, device/browser fingerprints, or
    headers to evade controls. Stop on challenge, lock, or global auth failure.
11. No paid API, paid proxy, or external paid queue/storage service.
12. Metadata commits cannot depend on media success. Network/file I/O never
    occurs inside a SQLite write transaction.
13. A file is complete only after verified content and atomic placement;
    neither an archive-ledger row nor an existing path is sufficient.
14. Preserve deterministic paths, sidecars, SHA-256 evidence, restrictive
    modes, download-ledger compatibility, and `context_media_complete()` until
    a migration proves an intentional equivalent replacement.
15. Bound CDN attempts, exact-post refresh generations, process/session age,
    and work batch size durably across restart.
16. Deleted, protected, suspended, withheld, missing-media, stale-descriptor,
    transient-CDN, local-I/O, and implementation failures stay distinct.
17. Existing descriptorless work gets a lazy compatibility path, but old
    behavior cannot remain the default for newly captured descriptors.
18. Preserve two independent matching valid legacy observations for newly
    established source-visible history. They need not be consecutive if an
    intervening attempt is invalid and non-contradictory.
19. Never widen a legacy interval without request-cap headroom and deterministic
    split/retry behavior. Exact half-open UTC bounds remain authoritative.
20. Raw run snapshots remain immutable provenance. An indexed store may become
    the commit/query authority only if exports are reproducible and migrations
    are idempotent.
21. Routine source caching cannot erase integrity checking. Detect changed
    path/inode/size/mtime evidence, use manifest-bound source hashes, and keep
    an explicit full-content audit mode.
22. Portable exports may be deferred/coalesced, but status must distinguish
    durable indexed truth from an older materialized export generation.
23. Queue indexing/materialization must preserve full-depth chain preference,
    shared-parent benefit, fairness quantum, retries, and lease semantics.
24. `Ctrl-C` must promptly stop child work, release/recover leases, and retain
    raw, descriptor, partial-file, cursor, and pacing truth.
25. Low disk must stop new byte transfers without corrupting metadata or
    manufacturing terminal outcomes.
26. Dashboard/readers cannot initiate work or change archive outcomes.
27. Do not use an active production archive as an implementation fixture. A
    bounded live smoke requires separate explicit authorization.
28. Preserve the current dirty worktree and unrelated user changes. Never
    reset, overwrite, or silently absorb them into Goal 5.

## Target State Model

Stage 2 decides whether the safest migration is one expanded per-user SQLite
database or a small set of attached/state databases. Do not force a multi-GiB
copy merely to satisfy a conceptual preference for one filename. The durable
model must nevertheless provide one coherent transaction/ownership story for:

- stable account identity and canonical handle;
- per-host-category request telemetry and an account-wide pacing boundary;
- immutable raw-source registry with expected digest and ingestion generation;
- canonical normalized posts keyed by post ID and provenance;
- reply edges, observations, depth/fairness priority, and availability state;
- descriptor generations for context, modern, legacy, retry, and profile
  sources;
- individual asset jobs with ordinal, type, URL/variant, expected naming,
  transfer state, attempts, hash, and final path;
- exact-post descriptor refresh attempts separate from CDN transfer attempts;
- current durable generation and last exported generation for each JSONL view;
- cheap maintained counters for progress and ETA.

Network and file transfer occur outside write transactions. A worker claims a
small job, performs work, and commits a short result transaction. Raw snapshots
remain independently inspectable and exports remain reproducible.

## Success Metrics and Verification Requirements

Goal 5 is complete only when all of the following have direct evidence:

1. **Actual request ledger:** tests report X API, X bootstrap/support, CDN, and
   retry calls separately, with no sensitive URL/query/header content.
2. **Context media:** a media-bearing accepted context post uses its metadata
   extraction and zero additional X tweet requests on the happy path.
3. **Conversation batch benefit:** one bounded response can create descriptor
   jobs for every accepted record while creating none for rejected records.
4. **Shared media repair:** failed modern/legacy media with a usable descriptor
   performs zero exact-post X lookups before CDN retry.
5. **Profile work:** an unchanged-profile fixture performs the mandatory info
   probe, zero avatar/background X extractor calls, and zero profile CDN calls;
   a changed descriptor performs only the required CDN transfer.
6. **Pacing:** actual X calls remain sequential, obey configured minimum/jitter
   policy and rate resets, persist not-before state, and do not burst after
   restart. Removing stacked waits cannot reduce this guarantee.
7. **Legacy proof:** sparse and dense fixtures return exactly the same canonical
   records as the fixed-window baseline, retain two matching valid observations,
   split safely at caps, accept matching valid evidence across an intervening
   transient failure, and reuse valid evidence after restart.
8. **Legacy efficiency:** on the deterministic sparse fixture, actual X API
   calls are at least 50% lower than fixed three-day/fresh-profile-per-walk
   behavior. If real API shapes make that threshold unsafe, the stage must
   present measurements and a revised explicit threshold before implementation.
9. **Process/session churn:** a 1,000-target deterministic context workload has
   at least 90% fewer runner startups if bounded reuse passes safety tests; if
   reuse is rejected, evidence must show why and quantify the remaining cost.
10. **Incremental commit:** adding a small modern delta or each legacy window
    does not reread/rewrite all historical post JSONL views. Multiple legacy
    windows cause at most one ordinary final materialization, not one per
    window.
11. **Source ingestion:** a second ordinary seed with no new/changed source
    reads zero source payload records and performs zero full source hashes;
    changed evidence is detected, and explicit integrity audit rehashes all.
12. **Queue plan:** metadata/media claim and stale-lease queries use appropriate
    indexes or maintained priority structures and do not full-scan/sort all
    targets per item. Query-plan tests enforce this.
13. **Export generation:** unchanged context/post/media state causes zero
    full-export rewrite. Changed state produces a coherent reproducible export
    whose recorded generation matches durable truth.
14. **I/O benchmark:** a small-delta fixture over a large synthetic archive
    reduces ordinary bytes read/written by at least 90% versus baseline while
    an explicit full export/audit remains available.
15. **File correctness:** images, multi-image, GIF/video, mixed media, profile
    assets, and failure fixtures retain expected names, ordinals, sidecars,
    hashes, paths, and unavailable semantics.
16. **Failure safety:** crash/`Ctrl-C` at each request, transaction, descriptor,
    transfer, file placement, cursor, export, and schema boundary is resumable
    with no skipped or falsely completed work.
17. **Compatibility:** existing context schema v2, state JSON queues, download
    ledgers, raw snapshots, complete files, manual review, dashboards, and
    ordinary CLI behavior migrate idempotently without forced re-download.
18. **Operational footprint:** no X concurrency, no catch-up burst, no unbounded
    retry, no new required flag, and no paid/evasive mechanism is introduced.
19. **Repository proof:** focused tests, full test discovery, compatibility
    fingerprints, `git diff --check`, deliberate diff review, and documentation
    all pass.
20. **Production evidence:** only after fixture proof and explicit permission,
    a bounded isolated smoke validates request categories, descriptor hit rate,
    pacing, and migration behavior without touching another active archive.

## Stages

### 1-MEASURE

#### Big-Picture Objective

Establish a trustworthy whole-pipeline request/process/I/O baseline and prove
what file descriptors gallery-dl exposes before selecting implementation
mechanisms.

#### Detailed Implementation Plan

- Add a sanitized test telemetry hook around actual HTTP request boundaries in
  the pinned runner and download path.
- Categorize calls as X API, X bootstrap/support, profile resolution, media CDN,
  redirect, and retry without persisting full URLs, queries, cookies, headers,
  or signed tokens.
- Link actual calls to logical operations: info, timeline page, legacy walk,
  context detail, exact fallback, profile asset, authored/legacy retry, and CDN
  asset.
- Record runner startups, session/bootstrap initializations, TLS connections
  where observable, wall time, bytes, raw-source reads/hashes, JSONL bytes
  rewritten, fsyncs, SQLite statements/query plans, and dashboard refresh cost.
- Build deterministic sanitized fixtures for modern incremental update, sparse
  and dense legacy intervals, context conversations with accepted/rejected
  records, image/multi-image/video/mixed media, profile changes, transient and
  terminal failures, restart, and large synthetic local datasets.
- Prove which structured gallery-dl file events are available during
  `--no-download` and whether they include post ID, ordinal, variant, URL,
  extension, author/date naming fields, and expiry information.
- Compare bounded process/session reuse approaches: multi-URL gallery-dl job,
  runner control protocol, library worker, or no reuse.
- Measure the current P2 scans and set a documented threshold: implement them
  if they consume over 1% of worker CPU/wall time, issue material storage I/O,
  or measurably delay commits on the large fixture.
- Write findings and selected contracts in `goal-5/1-MEASURE.md`.

#### Completion Requirements

- Request tests count actual calls and expose hidden fallback/bootstrap work.
- Descriptor fixtures prove a viable no-download capture mechanism or document
  an evidence-backed per-media exception.
- Baseline tables cover every network phase and major local commit path.
- Query plans and byte accounting reproduce the confirmed audit findings.
- Explicit performance thresholds and selected runner/descriptor mechanisms
  are recorded.
- No production state is changed; focused tests and `git diff --check` pass.

### 2-STATE

#### Big-Picture Objective

Design and migrate the minimum coherent durable state needed for descriptors,
assets, pacing, source ingestion, incremental indexes, and export generations.

#### Detailed Implementation Plan

- Compare expanding context SQLite in place, adding a per-user archive-state
  database, and using attached specialized databases; choose based on migration
  size, lock behavior, atomicity, and compatibility rather than aesthetics.
- Model request pacing/aggregate telemetry, source registry, normalized posts,
  reply queue priority, descriptor generations, asset jobs, and export
  generations.
- Preserve post-level context `media_state` as a validated rollup or define an
  explicit compatible replacement.
- Define uniqueness/provenance keys across modern, legacy, context, retry, and
  profile descriptor sources.
- Add eligibility/lease/priority indexes or maintained counters needed by the
  measured claim queries.
- Define lazy migration for schema-v2 context databases, state JSON
  `pending_media`, existing download ledgers/files, and descriptorless work.
- Keep private URLs encrypted only if a locally available, restart-safe key
  design is justified; otherwise rely on current restrictive modes and strict
  telemetry redaction. Do not add a paid secret service.
- Define source verification cache and explicit full-integrity audit semantics.
- Define durable/current versus exported-generation truth and recovery after an
  interrupted materialization.

#### Completion Requirements

- Fresh and existing-state migrations are idempotent and rollback-safe on large
  fixture copies.
- No migration requires an unnecessary multi-GiB duplicate of production
  state.
- Constraints reject cross-post assets, invalid state transitions, duplicate
  sources, stale generations, and unsafe lease ownership.
- Existing complete files and captured context remain complete without network
  work.
- Query-plan tests demonstrate indexed/maintained work selection.
- Privacy, permissions, backup, interruption, and downgrade/fail-closed tests
  pass.

### 3-DESCRIPTORS

#### Big-Picture Objective

Capture reusable media descriptors during work the archiver already performs,
and retain them only for records that become authoritative archive scope.

#### Detailed Implementation Plan

- Extend the chosen runner/file-event path to write private structured
  descriptor artifacts in no-download and normal-download modes.
- Capture descriptors from bounded context responses, modern timeline records,
  confirmed legacy walks, retry observations, and the mandatory info profile
  record.
- Validate post ID, media ordinal, type, selected variant, URL host, extension,
  naming inputs, source extraction, and descriptor digest.
- Have context and legacy selection expose exact accepted/canonical post IDs;
  persist descriptors only for those IDs and discard other response records.
- For two legacy confirmation walks, require compatible descriptor ownership
  but allow the newest usable signed URL generation after post-record agreement.
- Mark count-without-descriptor as `needs_refresh` while still committing post
  metadata, reply edges, and legacy progress.
- Deduplicate identical generations and supersede stale URLs without reopening
  already verified files.
- Bind temporary artifacts to run/operation IDs so crash residue cannot be
  consumed by another target.

#### Completion Requirements

- Every supported media fixture produces correctly associated descriptors from
  existing extraction work with no new X call.
- Returned-but-rejected context/search records produce no persistent job.
- Missing/malformed descriptors never roll back metadata or frontier state.
- Repeated extraction, crash replay, two-walk legacy confirmation, and changed
  descriptor tests remain coherent and idempotent.
- Profile descriptor tests preserve historical profile assets and identity.
- Sensitive URL data appears only in private durable/temp state.

### 4-DIRECT-MEDIA

#### Big-Picture Objective

Download and verify media directly from persisted descriptors without an X
tweet/profile extractor on the normal or ordinary retry path.

#### Detailed Implementation Plan

- Implement the lowest-risk direct downloader selected in Stage 1, reusing
  proven gallery-dl behavior where possible.
- Claim individual asset jobs in short transactions; perform CDN I/O outside
  the transaction; atomically commit verified results.
- Preserve context, authored, legacy, and profile path/filename/sidecar/hash
  contracts and existing download-ledger reconciliation.
- Use bounded timeouts, partial files, resume where verified safe, bandwidth
  limit, one default CDN lane, SHA-256, and post-write verification.
- Handle image size variants, multi-image ordinals, GIF/video selection,
  redirects, expiry/403, 404/410, checksum mismatch, disk exhaustion, and local
  I/O distinctly.
- Prioritize metadata and small assets; do not let a large video hold the X
  request lane or metadata transaction.
- Update post-level/media progress rollups from individual asset truth.

#### Completion Requirements

- Usable descriptors cause zero X calls and exactly the expected CDN attempts.
- Existing verified files cause zero network work; corrupt/partial files repair
  safely.
- All media forms retain deterministic names, paths, ordinals, sidecars, and
  hashes.
- Metadata remains committed through every injected transfer failure.
- `Ctrl-C`, timeout, low disk, redirect loop, and checksum failures are
  restart-safe with no false capture.
- No rejected conversation/search asset can reach the downloader.

### 5-FALLBACKS

#### Big-Picture Objective

Provide a bounded exact-post descriptor refresh for exceptional cases without
letting compatibility or failure paths erase the request savings.

#### Detailed Implementation Plan

- Define evidence for direct CDN retry, descriptor refresh, terminal
  unavailable, authentication stop, and manual review per media source.
- Route existing descriptorless context/state-JSON jobs through one lazy
  refresh; new descriptor-bearing jobs bypass it.
- Share the account-wide X scheduler, cookies, reset state, and global auth
  stop.
- Persist refresh generations separately from CDN attempts so restart cannot
  reset either budget.
- Reuse refreshed descriptors for all missing assets of the post rather than
  starting another extractor per filename.
- Preserve deleted/protected/suspended/withheld post semantics separately from
  an expired URL or missing media asset.
- Define explicit operator repair for historical manual-review entries; normal
  runs do not retry forever.
- Alert on a descriptor miss/refresh ratio above the Stage 1 threshold.

#### Completion Requirements

- Usable descriptors never hit fallback.
- Missing/stale fixtures use no more than the documented exact-refresh budget
  and return to direct CDN work.
- A refreshed multi-asset post uses one X refresh generation, not one per file.
- Auth/rate-limit faults stop or wait correctly.
- Repeated failures terminate durably as unavailable/review with no restart
  loop.
- Compatibility jobs migrate lazily; verified complete files never refresh.

### 6-PACING

#### Big-Picture Objective

Replace fragmented sleep layers with one durable actual-call scheduler and,
where proven, bounded runner/session reuse that lowers bootstrap/reconnect work
without increasing X request density.

#### Detailed Implementation Plan

- Reserve pacing immediately before each actual X API/bootstrap request, not
  merely before a subprocess or logical target.
- Persist per-account next-request time, observed quota reset, 429 backoff,
  global stop, and last request/progress across modern, legacy, context, and
  refresh phases.
- Preserve configurable jitter/minimum gaps and prevent catch-up bursts after
  restart or a long wait.
- Remove endpoint/extractor/walk sleep duplication only after tests prove the
  actual-call timeline remains at least as conservative.
- Implement the selected bounded runner/session strategy with structured
  per-item begin/result messages, work caps, maximum age, clean shutdown, and
  restartable leases.
- Reuse transaction keys/cookies/session only within their proven lifetime;
  refresh on explicit expiry rather than spoofing or cycling identity.
- Keep CDN pacing/bandwidth separate and default to one transfer lane; allow
  bounded CDN overlap only during X wait/idle time.
- Make telemetry report request density, wait source, retries, session age, and
  restarts without exposing sensitive data.

#### Completion Requirements

- Deterministic call timelines prove one X lane, no burst, correct resets, and
  equal-or-greater minimum spacing after redundant sleeps are removed.
- Kill/restart tests inherit the durable not-before boundary.
- Persistent/batched worker failure loses no completed item and leaves at most
  bounded leased work for reclamation.
- The 1,000-target startup-reduction metric is met or a recorded rejection
  proves session reuse unsafe/unhelpful.
- Actual X calls are never increased to fill previously idle time.
- Challenge/lock/auth tests stop every X-capable phase globally.

### 7-LEGACY

#### Big-Picture Objective

Reduce sparse legacy backfill calls and repeated evidence work while retaining
the source-visible completeness standard.

#### Detailed Implementation Plan

- Derive an adaptive root-window policy from returned page density and request
  cap headroom: grow sparse windows, shrink/split dense/capped windows, and
  persist the chosen next interval.
- Preserve exact half-open UTC bounds, overlap validation, empty-tail proof,
  record numeric identity, canonical dedupe, and deterministic splitting.
- Persist valid walk evidence independently of invalid attempts and run
  boundaries. Accept two matching valid observations even when a transient
  invalid non-contradictory attempt occurs between them.
- Reject genuinely mismatching valid walks and retain manual review/splitting.
- Bind profile identity once through the mandatory verified run/session
  evidence and avoid one profile API call per walk while still validating every
  returned record against numeric ID.
- Reuse one descriptor set from confirmed records for legacy media jobs.
- Commit each canonical window to indexed durable truth, but coalesce portable
  JSONL materialization until a safe final/bounded checkpoint.
- Add recovery tests for crash after first valid walk, after second walk,
  during split, after indexed commit, and before export.

#### Completion Requirements

- Sparse/dense/capped fixtures return exactly the baseline canonical post set
  and frontier.
- Every new interval has two independent matching valid observations; transient
  invalid attempts do not erase valid evidence.
- Restart reuses valid evidence and never treats the same artifact as two
  observations.
- Sparse fixture API calls fall by the explicit target while dense behavior
  stays bounded and safe.
- Profile lookup count is one per proven run/session boundary rather than one
  per walk, or evidence documents why an equivalent safe cache is impossible.
- No per-window full portable-dataset rewrite remains.

### 8-LOCAL

#### Big-Picture Objective

Make normal commit, seeding, queue selection, recovery, export, and dashboard
work proportional to new/actionable data rather than total archive size.

#### Detailed Implementation Plan

- Upsert normalized posts/media/source provenance into indexed durable storage
  as raw snapshots are committed.
- Keep raw snapshots authoritative and make JSONL views reproducible,
  generation-tagged exports.
- Materialize post/authored/repost/media views at most once per invocation when
  changed, or through a documented explicit export; atomically publish a
  generation only after all views complete.
- Register and ingest each modern/legacy raw source once. Parse new sources once
  while adding local posts and reply edges; avoid the current unconditional
  historical pre-pass.
- Cache verified source digest/stat evidence and make full rehash a separate
  integrity audit. Detect ordinary mutation and fail closed.
- Add claim/eligibility/lease indexes or maintained parent-demand priority so
  metadata/media selection avoids full scan, correlated count, and temp sort on
  every item.
- Reclaim leases on a bounded cadence/indexed expiry rather than an unindexed
  update before every claim.
- Maintain cheap transactional counters/generations for progress; benchmark
  grouped scans and closure/manifests.
- If P2 thresholds are crossed, add current-manifest pointers, processed-run
  registry, bounded recovery index, and dashboard counters while preserving
  immutable manifests as audit evidence.
- Provide explicit `verify`/`export` behavior for operators who want a full
  content audit or fresh portable snapshots.

#### Completion Requirements

- Large synthetic small-delta benchmark meets the 90% ordinary I/O reduction.
- K legacy windows perform K small indexed commits and at most one export.
- Unchanged seed/export/media state produces no payload re-read, full hash, or
  full JSONL rewrite.
- Query-plan regression tests forbid target full scans/temp sorts on the hot
  claim path.
- Export crash leaves the prior generation coherent and the next run repairs
  or retries without re-fetching X.
- Full integrity audit detects injected raw/source/file corruption.
- Dashboard/recovery optimizations, if triggered, remain read-only or
  transactionally maintained and never change archive outcomes.

### 9-INTEGRATE

#### Big-Picture Objective

Make all optimized paths automatic under the one command, with coherent
ordering, progress, migration, and failure semantics.

#### Detailed Implementation Plan

- Integrate identity, modern extraction, adaptive legacy, context descriptor
  capture, direct media, bounded refresh, and incremental export through one
  scheduler/state contract.
- Compare safe scheduling of opportunistic CDN drains versus one CDN worker
  during X waits; choose the simpler design that improves wall time without
  more X traffic or metadata starvation.
- Ensure phase handoffs do not redo completed work and no separate repair
  command is required for ordinary legacy/context/media progression.
- Migrate existing state lazily with checkpoints and clear rollback/manual
  recovery instructions.
- Update progress/manifests to show actual X calls, CDN calls/assets, descriptor
  hit/refresh ratio, durable versus exported generation, queue counts, rate,
  and ETA using maintained aggregates.
- Keep dashboard placement/display independent of worker correctness and make
  renderer failure harmless.
- Preserve multi-user fairness without interleaving extra X work merely to fill
  quota waits.
- Update dry-run and docs so the operator understands low-footprint behavior,
  exceptional refreshes, unavailable media, explicit audit/export, and current
  durable/exported state.

#### Completion Requirements

- `uv run scripts/archive-x --user USERNAME` automatically performs every
  applicable phase with no new required option.
- Completed phases and compatible existing work are not redone.
- Tests prove one X lane and bounded CDN-only overlap.
- Migration, lock contention, renderer death, child failure, `Ctrl-C`, and
  restart retain coherent phase/state truth.
- Dashboard/final summary reconcile with SQLite/manifests/exports and never show
  a stale export as current.
- Multi-user tests preserve isolation/fairness and global auth stop.

### 10-PROVE

#### Big-Picture Objective

Demonstrate end-to-end correctness, lower actual X traffic, lower process/I/O
cost, compatibility, and operational clarity before declaring Goal 5 done.

#### Detailed Implementation Plan

- Run end-to-end temporary-root fixtures covering modern update, profile
  unchanged/changed, sparse/dense legacy, context batching, every media type,
  descriptor refresh, unavailable outcomes, migration, restart, export, and
  dashboard rendering.
- Run no-cheating request-ledger verification at actual HTTP boundaries.
- Compare baseline/new request, startup, connection, retry, wall-time, CPU,
  bytes-read, bytes-written, fsync, and query-plan metrics.
- Fault-inject rate reset, 429, challenge/auth stop, ambiguous response,
  descriptor corruption/expiry, CDN failure, partial file, checksum mismatch,
  low disk, SQLite busy/corruption, process kill, `Ctrl-C`, migration failure,
  and interrupted export.
- Verify that raw snapshots and portable exports can reproduce the indexed
  canonical dataset and that integrity audit finds intentional corruption.
- Run focused suites, complete repository discovery, shim fingerprints,
  whitespace checks, and deliberate diff review against the pre-existing dirty
  baseline.
- Update operator docs and recovery guidance.
- Only with explicit permission, run a bounded isolated live smoke and compare
  aggregate request categories/descriptor hit rate to the recorded baseline.

#### Completion Requirements

- Every success metric has a recorded artifact or test.
- Happy-path context/shared/profile media uses no redundant X lookup.
- Legacy and local-I/O thresholds are met without weaker completeness evidence.
- Actual X request density/concurrency does not increase and restart cannot
  burst.
- All integrity, migration, failure, compatibility, and operator-command tests
  pass.
- Focused and full tests, compatibility fingerprints, `git diff --check`, and
  deliberate diff review pass from an understood worktree.
- Any rejected P1 experiment has measured rationale and no unaddressed high-cost
  fallback; unresolved correctness cases remain explicit rather than hidden.

## Definition of Done

Goal 5 is done when the unified archiver reuses metadata-derived descriptors,
performs direct durable media transfers, paces actual X calls through one
restart-safe lane, backfills legacy history with fewer calls but equivalent
evidence, and applies small archive deltas without whole-history hot-path I/O.
Tests and benchmarks must prove fewer actual X requests, fewer process
bootstraps, lower local I/O, no increased request density, and unchanged archive
safety under the same one-command interface.

Renaming phases, hiding calls in a persistent worker, shortening sleeps without
actual-call pacing, routing all work through exact-post fallback, or deferring
full rewrites to another automatic phase does not complete this goal.
