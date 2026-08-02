# 4-DIRECT-MEDIA

## Current Facts

- Stage 3 now creates an individual `asset_jobs` row backed by one active,
  accepted descriptor generation for modern, context, legacy, retry, and
  profile work.
- The existing context-media phase launches an exact X extractor and lets
  gallery-dl discover and download the same post again. Authored/legacy retry
  does the same through `retry-media-*` endpoints.
- Existing gallery-dl files use deterministic paths plus adjacent JSON
  sidecars. Context completion additionally verifies the sidecar SHA-256
  against file bytes.
- The schema already separates short asset leases from descriptor state and
  supports captured, retryable, needs-refresh, unavailable, and manual-review
  outcomes.

## Baseline Ledger

- One media-bearing context post currently costs one later X exact extraction
  before its unavoidable CDN transfer.
- A usable persisted descriptor should instead cost zero X calls and one CDN
  call in the ordinary no-redirect success case.
- A verified existing file should cost zero network calls.
- Metadata and frontier commits already precede media; Stage 4 must not move a
  transfer inside those transactions.

## Updated Assumptions

- Use a small Requests-based streaming transfer because it reaches the proven
  actual-request telemetry boundary, supports bounded connect/read timeouts,
  and permits explicit redirect-origin validation. Gallery-dl is retained for
  X extraction, not relaunched merely to consume a saved URL.
- Resume is enabled only when a retained partial has a verified byte count and
  the server explicitly returns a matching `206 Content-Range`; otherwise the
  partial is replaced from byte zero.
- One CDN lane is the default. Any later overlap requires separate measured
  authorization and must remain CDN-only.
- Stage 4 classifies stale/forbidden descriptors as `needs_refresh`; Stage 5
  owns the bounded X refresh and final unavailable decision.

## Big-Picture Objective

Claim accepted asset jobs, transfer their bytes directly from the media CDN,
atomically publish deterministic files and compatible sidecars, and commit
verified completion without any X tweet/profile request.

## Detailed Implementation Plan

- Add a direct-media module with strict destination/redirect validation,
  bounded streaming, private partial state, optional safe resume, SHA-256,
  sidecar construction, atomic file placement, and directory fsync.
- Add token-guarded asset claim, reclaim, success, retry, refresh, interrupt,
  and post-level rollup methods to the SQLite owner.
- Reconcile an existing file only when its sidecar and digest verify; never
  trust a path or download-ledger row alone.
- Keep all HTTP/file work outside write transactions. Commit only stat/hash/path
  evidence after both asset and sidecar are durable.
- Classify redirects, 403, 404/410, 429, 5xx, timeout/network, checksum/content
  length, low disk, local I/O, and interruption distinctly.
- Count actual CDN sends, redirects, retries, bytes, and peak concurrency via
  the Stage 1 telemetry hook without persisting URLs.
- Add image, multi-image, GIF/video, mixed-media, profile, existing-file,
  partial/resume, corruption, low-disk, fault, and interruption fixtures.

## Safety Invariants

- Only an active descriptor owned by the claimed asset can be transferred.
- Every initial and redirected URL remains HTTPS on the allowlisted media CDN
  origins; cookies and X authentication are never sent to the CDN worker.
- At most one CDN transfer is in flight by default and no X request is made.
- File/network I/O never occurs in a SQLite write transaction.
- A captured job requires a nonempty file, verified SHA-256, byte count,
  deterministic final path, adjacent sidecar, and durable atomic placement.
- `Ctrl-C`, timeout, crash, low disk, and local I/O leave a reclaimable lease or
  retryable job and never manufacture completion.
- Returned-but-rejected records have no job and therefore cannot reach claim.

## No-Cheating Checks

- The success fixture records zero X-category calls and exactly one CDN call.
- A redirect is counted as another CDN attempt and is accepted only on the
  allowlist; an external redirect is rejected before a second send.
- Existing verified bytes perform zero network work; an empty, partial,
  mismatched, or sidecarless file is not accepted.
- Descriptor 403/404/410 outcomes become explicit refresh-needed evidence and
  do not silently invoke gallery-dl.
- Metadata rows remain present through every injected transfer/file/database
  completion fault.

## Completion Requirements

- Direct transfers preserve names, ordinals, paths, sidecars, hashes, and
  post-level media rollups for every supported media form.
- Leases and partials recover across crash/interrupt without false capture.
- CDN attempts and redirects are visible; usable descriptors produce no X
  request.
- Existing verified files and repeated completion are idempotent and network
  free.
- Focused, full, fingerprint, compile, diff, and deliberate review checks pass
  before Stage 5.

## Results Ledger

- Added `scripts/archive_x_media.py`, a one-lane Requests streaming worker that
  consumes only active accepted descriptors. It creates its own unauthenticated
  session with environment proxies disabled, validates every initial and
  redirected HTTPS CDN origin, uses bounded connect/read timeouts and a default
  8 MiB/s limit, and never invokes an X extractor.
- Added per-asset priority, indexed claim, random lease token, stale-lease
  recovery, guarded completion/failure, stat/hash evidence, and post rollups to
  schema v3. `ASSET_CLAIM_SQL` uses the partial `asset_jobs_ready` index with no
  temporary sort. A lock-probe fixture proves the CDN callback runs after the
  SQLite write transaction commits.
- Added private hashed partial state, validator-guarded Range resume, safe 416
  restart, atomic sidecar/file placement, directory fsync, SHA-256, and a
  second post-placement hash. A complete partial or already verified final
  asset is recovered without another network request.
- Preserved deterministic context/authored/legacy/profile paths, gallery-dl
  compatible sidecars, and both existing download ledgers. A ledger-write
  fault leaves the job retryable and the complete bytes reusable, so the next
  run repairs the ledger locally without re-downloading.
- Distinguished descriptor expiry/rejection, terminal CDN response, transient
  HTTP/network/stream failure, redirect-origin violation/loop, invalid content,
  low disk, local write failure, operator interruption, and checksum failure.
  Attempts are capped; low-disk, interruption, and local ledger repair do not
  consume the network retry budget.
- The ordinary success fixture records exactly one `media_cdn` request, zero X
  requests, and peak concurrency one. Mixed photo/animated-GIF/video work
  records exactly three CDN requests for three assets. A verified existing file
  records zero network requests. Redirects are counted individually, while
  URLs and exception messages are absent from telemetry and durable errors.
- Added 22 direct-media tests covering success, mixed and multi-asset rollup,
  profile history, priority, existing/corrupt files, 403/404/410 refresh,
  bounded redirect and timeout behavior, low disk, interruption/resume, 416,
  incomplete bodies, attempt ceilings, unsafe paths, sidecar/DB/ledger faults,
  privacy, file modes, download ledgers, and portable `media.jsonl` output.
- Full repository discovery passed 288 tests. Both pinned gallery-dl runners
  reported `1.32.4`; `compileall`, `git diff --check`, permissions checks, and
  deliberate code/diff review passed. No live request or production archive
  state was used.
- This stage supplies and proves the direct worker but deliberately does not
  switch the unified command yet. Stage 5 supplies bounded exceptional refresh
  and Stage 9 performs the compatibility migration and orchestration cutover.

## Stage Results

Stage complete. Stage 5 may now add bounded exact-post descriptor refresh for
the exceptional `needs_refresh` path without changing ordinary direct-transfer
semantics.
