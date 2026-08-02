# 1-MEASURE

## Current Facts

- Stage started on `master` at commit `e1bff3f`; the worktree was clean.
- No tmux session or `archive-x`, `archive_x.py`, context runner, or gallery-dl X
  runner process was present at stage start. Production state is out of scope
  for mutation.
- The base pinned runner patches `TwitterAPI._call` but has no generic actual
  HTTP request ledger.
- The legacy runner counts `TwitterAPI._call` and search calls, but that is API
  method telemetry rather than a transport-wide ledger and does not include CDN
  requests.
- Context increments a logical request counter once per `fetch_post()` call.
  The patched TweetResult path may internally call TweetDetail, so this counter
  is not proof of actual X request count.
- Gallery-dl API and ordinary media transfers reach urllib3's
  `HTTPConnectionPool._make_request`, which is below Requests' redirect and
  retry orchestration and corresponds to one HTTP attempt. Instrumenting
  `requests.Session.send` would count logical sends but misattribute an initial
  redirect and hide urllib3-internal retries.
- The installed yt-dlp can use either its Requests or urllib handler. The former
  reaches the urllib3 boundary; the latter reaches
  `urllib.request.AbstractHTTPHandler.do_open`. Both paths are covered by the
  selected ledger.
- Twitter extraction creates one `Message.Directory` post record followed by
  one `Message.Url` per media item. Each URL dictionary includes the transformed
  post metadata, `tweet_id`, `num`, selected URL, extension, type, dimensions,
  alt text, and video variant fields where applicable.
- `metadata-url: media_url` attaches the selected URL before
  `DownloadJob.handle_url()`. A metadata postprocessor on the `prepare` event
  runs after filename construction and before archive/file download checks.
- `--no-download` keeps URL dispatch and `prepare` hooks active while replacing
  the byte-transfer method. This is the leading descriptor-capture mechanism
  to verify with deterministic fixtures.
- Current hot context claim SQL has no eligibility/lease index and its observed
  query plan includes a full target scan, correlated parent count, and temporary
  global ordering B-tree.

## Baseline Ledger

### Read-only observed archive evidence

- Carmack legacy sample: 19 root windows, 39 walk processes, 118 search calls,
  157 total API calls, and 100 canonical posts added.
- The 39-call difference between total API and search calls is exactly one per
  walk and is consistent with repeated profile resolution.
- Carmack post and authored JSONL views were about 56 MiB each. The 19
  per-window full merge/write/hash commits imply roughly four GiB of repeated
  logical local I/O for those 100 posts, excluding raw/manifests/fsync overhead.
- Visakanv post and authored JSONL views were about 891 MiB each; committed raw
  post sources were about 602 MiB; context post/edge exports were about 406 MiB;
  and context SQLite was about 1.8 GiB at audit time.
- Carmack context SQLite was about 130 MiB at audit time.

These figures are starting evidence, not a live benchmark. Stage fixtures must
make every baseline reproducible without production network or mutation.

## Updated Assumptions

- Hooks at urllib3 `_make_request` and urllib `do_open` count actual HTTP
  attempts without storing request secrets. Session and connection creation are
  counted separately and do not inflate request totals.
- Host/category plus an allowlisted operation label is sufficient for
  no-cheating request budgets; full URL paths and queries are unnecessary and
  forbidden.
- Response status, elapsed time, advertised content length, redirect flag, and
  ordinal are safe aggregate fields.
- Connection-start counts may be collected at urllib3 connect boundaries where
  stable; if the hook is too invasive or misses yt-dlp handlers, runner/session
  starts remain the conservative reconnect proxy and the limitation is
  documented.
- A `prepare`-event JSONL artifact is likely the lowest-risk descriptor source,
  but tests must prove it remains populated under no-download and that rejected
  records can be filtered later by exact post ID.
- Process/session reuse cannot be selected until a bounded multi-target
  experiment demonstrates per-target result boundaries and restart safety.

## Big-Picture Objective

Create trustworthy, sanitized measurement infrastructure and deterministic
baselines for every Goal 5 network and major local-I/O claim. Select the
descriptor and bounded-runner mechanisms from evidence before Stage 2 commits
their state model.

## Detailed Implementation Plan

- Add a runner telemetry module with strict CLI parsing, fixed operation labels,
  host-category classification, actual requests/yt-dlp hooks, aggregate/event
  output, atomic private writes, and no raw URL/query/header/cookie storage.
- Wire generic telemetry through base and legacy pinned runners without
  weakening version/source fingerprint checks or legacy telemetry.
- Wire per-operation telemetry paths through modern/profile/retry, context, and
  legacy commands and expose safe summaries in endpoint/walk results.
- Add deterministic fake-transport tests covering X API, X bootstrap/profile,
  CDN, redirect, retry, yt-dlp, malformed options, interruption, and redaction.
- Add a no-download descriptor fixture using gallery-dl URL dispatch and a
  `prepare` metadata hook. Cover image, multi-image, video/GIF, mixed media, and
  accepted/rejected post association.
- Add a read-only measurement utility for dataset/source/export sizes, legacy
  request totals, SQLite table/index/query-plan evidence, and safe process/run
  counts. It must refuse writes and avoid opaque cursor or URL output.
- Add a large synthetic baseline test for full post rewrite, repeated legacy
  commit, source seed/hash/reparse, context export, and target claim query plan.
- Prototype multi-URL versus bounded control-protocol runner behavior with fake
  extractors. Record startup reduction, per-item result boundaries, and crash
  recovery; do not select a persistent production design on process timing
  alone.
- Measure dashboard/recovery scans against the large fixture and record whether
  the P2 threshold is crossed.

Expected implementation files include:

- `scripts/archive_x_request_telemetry.py` (new);
- `scripts/gallery_dl_x_runner.py`;
- `scripts/gallery_dl_x_legacy_runner.py`;
- `scripts/archive_x.py`;
- `scripts/archive_x_context.py`;
- `scripts/archive_x_legacy.py`;
- focused new/updated tests;
- an optional read-only measurement utility if it remains narrowly scoped.

## Safety Invariants

- Telemetry cannot contain full URLs, query strings, cookies, headers, request
  bodies, opaque cursors, signed tokens, or raw post/user metadata.
- Telemetry failure cannot change an archive outcome.
- The pinned runner continues to fail closed on unsupported gallery-dl source.
- Actual request instrumentation does not add network requests, retries, or
  concurrency.
- No production archive is used for fixture, performance, or descriptor tests.
- No request spacing or archive behavior changes in Stage 1.

## No-Cheating Checks

- A direct-result-to-detail fallback produces two actual X events even if the
  logical operation is one target.
- A redirect produces both actual sends while remaining one logical operation.
- Gallery-dl HTTP and yt-dlp requests both reach the ledger.
- Raw secret sentinels embedded in URL query, headers, cookies, and bodies do
  not occur anywhere in the telemetry JSON.
- Descriptor fixtures issue zero CDN requests under no-download while retaining
  all required structured file fields.
- Rejected conversation records may appear in the provisional artifact but are
  demonstrably removable solely by accepted post-ID set.
- Local-I/O baselines report bytes/queries from reproducible fixtures rather
  than inferring improvement from wall time alone.

## Completion Requirements

- Generic actual-boundary telemetry is wired and tested for all current runner
  paths, including yt-dlp and legacy.
- Safe summaries distinguish X API, X bootstrap/support/profile, CDN,
  redirect/retry events, runner starts, and operation labels.
- Descriptor fixtures prove or reject the `prepare`-event mechanism for every
  supported media class and accepted/rejected association.
- Reproducible baseline artifacts cover every network phase and major local
  commit path named in `0-plan.md`.
- Query plans and byte accounting reproduce the audit's full-scan/full-rewrite
  findings.
- A bounded session/process mechanism is selected or rejected with measured
  evidence and safety analysis.
- The P2 dashboard/recovery threshold decision is recorded.
- Focused tests, full repository tests, compatibility fingerprints,
  `git diff --check`, and deliberate diff review pass.
- `0-plan.md` is updated with authoritative Stage 1 findings before Stage 2.

## Results Ledger

### Actual-request mechanism

- `archive_x_request_telemetry.py` now wraps the two actual HTTP boundaries
  used by the pinned gallery-dl and installed yt-dlp stack. It records fixed
  operation/category/endpoint/status labels, timings, advertised bytes,
  redirects, sanitized exception classes, session starts, connection starts,
  runner starts, and peak concurrency.
- The event cap is 20,000 per runner, but aggregate totals remain exact after
  truncation. Artifacts are atomically written with mode `0600`; telemetry
  installation/write failure is non-authoritative and cannot change the
  gallery-dl exit status.
- Safe-summary validation uses allowlisted keys and reconciles category,
  endpoint, transport, status, event, and timing totals before anything is
  copied into a run result.
- The exact-fetch fixture records one `TweetResultByRestId` plus the hidden
  `TweetDetail` fallback as two X API attempts under one logical
  `context_exact` operation. A separate yt-dlp `UrllibRH` fixture records one
  CDN attempt at the urllib HTTP boundary.
- The phase matrix covers `info`, `timeline`, `retry_media`, avatar, banner,
  context metadata, context exact fallback, context media, and legacy walk.
  Redirect, 500, and successful retry attempts remain separate status events.
- Secret sentinels in URL paths/queries, cookies, authorization headers, and
  exception messages do not occur in serialized telemetry. No URL, hostname,
  query, header, cookie, body, handle, post ID, or cursor field exists in the
  schema.

### Descriptor mechanism

- A real gallery-dl `DownloadJob` with `download=false`,
  `metadata-url=media_url`, and a metadata postprocessor on `prepare` emitted
  one descriptor row for each of six file events while creating zero media
  files and recording zero HTTP/CDN attempts.
- The fixture covers two-image, video, animated-GIF, and mixed-media posts. The
  rows retain exact post ID, ordinal, media type, selected URL, extension,
  filename input, author/date naming inputs, dimensions, alt text, bitrate, and
  duration where supplied.
- Filtering the provisional artifact by the authoritative accepted-ID set kept
  all five accepted assets and removed the one nearby/rejected post without
  relying on order or filename.
- X media URLs do not provide a dependable explicit expiry field. The selected
  contract therefore stores capture generation/time and treats an eligible
  403/expiry outcome as a reason for one bounded descriptor refresh rather than
  inventing an expiry timestamp.
- Selected implementation mechanism: a private structured descriptor
  postprocessor at the `prepare` event, followed by exact accepted-ID filtering
  in the same metadata commit. A full generic metadata dump is fixture-only;
  Stage 3 must write a narrow validated descriptor schema.

### Reproducible local-I/O baseline

The deterministic fixtures use temporary roots only:

| Workload | Baseline result |
|---|---:|
| 5,000 normalized posts + three one-post deltas | 4,988,811 bytes reread + 9,985,170 bytes rewritten for 924 raw bytes |
| Small-delta read/write amplification | 16,205.6x |
| One 2,000-record seed source, first seed | 4,000 record visits + 606,679 hash bytes |
| Same source, unchanged second seed | 2,000 record visits + 606,679 hash bytes; zero files processed |
| Unchanged 100-sidecar media view | all 100 sidecars read and full view replaced again |
| Unchanged 1,000-row context export | full JSONL replaced; inode changed with identical size |
| Two completion checks on one 2 MiB context video | 4 MiB rehashed |

The read-only measurement fixture also reproduces the legacy process/request
ledger, full-view rewrite formula, committed-source revisit bytes, and both hot
claim plans. It validates that inspecting an archive creates or modifies no
file and opens SQLite with `mode=ro` plus `query_only`.

### Queue, dashboard, and recovery baseline

On the deterministic 100,000-target / 50,000-edge / 33,333-observation SQLite
fixture (about 8.0 MiB):

- metadata claim: 108.6-110.2 ms and a plan containing `SCAN t`, correlated
  `reply_edges` count, and `USE TEMP B-TREE FOR ORDER BY`;
- media claim: 24.8-24.9 ms with `SCAN targets` and a temporary ordering tree;
- the four status aggregates used by progress: 123.1 ms per refresh.

At the current five-second worker refresh cadence, the status aggregates alone
consume about 2.46% of wall time on this fixture, before closure/manifests. This
crosses the P2 threshold, so Stage 8 must use transactionally maintained
counters/current pointers rather than leaving dashboard scans as optional.

A 5,000-manifest temporary fixture (470,000 payload bytes) took 179.7 ms for one
sorted parse pass. Startup currently has several independent manifest passes,
so the measured recovery/current-manifest P2 work is also selected for Stage 8;
immutable manifests remain the audit evidence.

### Bounded process/session experiment

- Gallery-dl creates a Requests session per extractor unless an account-scoped
  session is explicitly supplied. The 1,000-extractor fixture created 1,000
  independent sessions versus one shared session when injected before
  initialization.
- A durable per-item begin/result control-protocol model with a 100-item process
  cap used 11 starts for 1,000 jobs when killed after beginning job 238, versus
  1,000 current starts: 98.9% fewer. All 237 acknowledged predecessors stayed
  committed; only job 238 was replayed, and every other item had one attempt.
- A plain multi-URL CLI is rejected because it does not provide a durable
  per-item acknowledgement boundary and new extractors still create sessions.
  A process-only reuse without explicit session injection would remove Python
  imports but not connection churn.
- Selected mechanism for Stage 6: one pinned, account-scoped control worker
  with a shared session, structured `begin`/`result` messages, one durable item
  lease at a time, maximum 100 items plus a maximum age, clean auth stop, and
  parent-owned commits. The prototype is evidence for the mechanism, not a
  production behavior change in this stage.

### Read-only archive evidence retained

- Carmack: 19 legacy root windows, 39 walks, 118 search calls, 157 total API
  calls, and 100 canonical additions; roughly four GiB of repeated post-view
  logical I/O.
- Visakanv: about 891 MiB each for posts/authored views, 602 MiB of committed
  raw post sources, 406 MiB of context exports, and a 1.8 GiB context database.
- These figures were not refreshed through a live archive run. All executable
  benchmarks and request tests used temporary deterministic fixtures.

## Rejected Alternatives

- `requests.Session.send` alone: correct logical-send count but wrong
  redirect/retry attribution.
- yt-dlp `RequestDirector.send` alone: misses lower handler attempts and does
  not cover gallery-dl's ordinary Requests traffic.
- unfiltered metadata JSONL as durable descriptor state: contains far more
  post metadata than the asset queue needs and would enlarge the private attack
  surface.
- unrestricted media download during a conversation/search response: could
  write assets for rejected nearby records.
- multi-URL gallery-dl without structured control/leases: no per-item durable
  result boundary and no guaranteed session reuse.
- relying on wall time or sleep removal as an efficiency metric: does not prove
  fewer calls, bytes, sessions, or historical I/O.

## Verification

- `uv run python -m unittest -v tests.test_archive_x_request_telemetry` — 11
  passed.
- `uv run python -m unittest -v tests.test_archive_x_descriptor_capture` — 1
  passed.
- `uv run python -m unittest -v tests.test_archive_x_measure` — 2 passed.
- `uv run python -m unittest -v tests.test_archive_x_stage1_io_baseline` — 4
  passed.
- `uv run python -m unittest -v tests.test_archive_x_runner_reuse_prototype` —
  2 passed.
- Base and legacy pinned-runner focused suites passed, including source
  fingerprints and generic telemetry handoff.
- `uv run python -m unittest discover -s tests -p 'test_*.py'` — 247 tests
  passed at the final Stage 1 boundary.
- `uv run python -m py_compile ...` for all changed scripts — passed.
- `git diff --check` — passed.

## Stage Results

Stage 1 completion requirements are satisfied. Actual-boundary telemetry,
descriptor capture, full-rewrite/source/export/queue baselines, bounded-runner
selection, and both P2 decisions now have direct fixture evidence. Stage 2 may
define and migrate the durable state model; no production archive behavior,
schema, pacing, or downloader path was changed beyond private observation.
