# 9-INTEGRATE

## Current Facts

- Stages 1–8 are complete and individually proven, but the ordinary unified
  command still enters several pre-Goal-5 paths in `archive_x.py` and
  `archive_x_unified.py`.
- The mandatory `info` endpoint already captures and commits profile
  descriptors, but a normal unbounded modern run still launches separate
  `avatar` and `background` extractors.
- Modern timeline completion still calls `update_post_dataset()` and
  `update_media_dataset()`, rebuilding portable JSONL and walking media
  sidecars instead of ingesting the run-owned raw source once into SQLite.
- State-JSON `pending_media` still sends each due post through a
  `retry-media-POST_ID` X extractor before the descriptor/direct-CDN workers
  get control.
- Unified follow-up still runs a distinct metadata context scheduler and then
  a distinct media scheduler. Newly captured descriptors make the latter a
  CDN drain, not a second X lookup, but scheduling and progress still present
  it as the old two-pass design.
- Startup recovery functions can accept indexed manifest candidates, yet the
  command does not populate that index or pass those candidates, so ordinary
  startup still globs historical run directories.
- Stage 8 provides one-time manifest/source/media reconciliation, direct run
  registration, cached source ingestion, maintained counters, and atomic
  generation export APIs. None may run before the numeric account-ID guard.
- The global archive lock already serializes one user invocation. Stage 6's
  durable X lane and bounded worker/session implementation already preserve a
  single X request in flight.

## Baseline Ledger

- Happy-path modern update: one info extractor, one timeline extractor, two
  profile extractors, full posts/authored/reposts rebuild, and one or more
  complete media-sidecar walks.
- Due authored/legacy media: one fresh exact-post extractor per post before
  bytes are retried.
- Context: one bounded metadata extraction per target followed by a separate
  media phase; descriptor-bearing work can now use zero X calls, but the
  unified phase boundary has not yet been cut over and verified end to end.
- Recovery: multiple readers independently sort/glob historical manifests.
- Export: the old command materializes portable views at modern/legacy/context
  phase boundaries even when only durable indexed truth needs advancing.
- Stage 8 proof provides the target local behavior: exact-stat source reuse
  reads zero payload, 5,000 manifests are scanned once, 100,000-target progress
  uses maintained counters only, and small-delta historical payload I/O falls
  by at least 90%.

## Updated Assumptions

- Identity proof remains the first operation allowed to authorize archive
  migration or source/media reconciliation. Creating the run directory and
  running read-only recovery discovery before proof is acceptable; changing
  indexed archive truth is not.
- The profile descriptor returned by `info` is sufficient to replace both
  profile extractor calls. Direct CDN transfer remains independently
  retryable and cannot change identity/profile metadata truth.
- Portable exports need not run after every small phase. SQLite durable truth
  may be ahead of its published export generation when clearly reported; an
  automatic bounded checkpoint must eventually publish without requiring a
  new normal-use flag.
- A separate CDN drain can remain internally because metadata and bytes have
  different failure boundaries. What must disappear is the redundant X
  rediscovery request and the operator-visible requirement to manage it.

## Big-Picture Objective

Make every proven Goal-5 path automatic under
`uv run scripts/archive-x --user USERNAME`, with one identity guard, one
durable X lane, no duplicate profile/tweet lookups, proportional local work,
coherent restart behavior, and live maintained-counter progress.

## Detailed Implementation Plan

- Import the Stage 8 local-state layer in the unified command. Immediately
  after numeric identity binding, reconcile historical manifests, sources,
  and portable media exactly once; register the current manifest and use the
  registry for recovery candidates thereafter.
- Register every current manifest transition and canonical raw source. Replace
  modern `update_post_dataset()` with `ingest_source_once()` and remove normal
  `update_media_dataset()` calls after the indexed media migration boundary.
- Queue/commit the `info` profile descriptors and drain them through the direct
  media worker. Remove normal avatar/background endpoint processes.
- Route authored, legacy, profile, and context asset jobs through descriptor
  selection and direct CDN transfer first. Use the bounded exact-post refresh
  worker only for absent/expired descriptors; bridge historical state-JSON
  pending records once and stop generating new ones.
- Treat context capture plus direct media as one automatic operator phase while
  retaining separate SQLite transactions and failure outcomes. Permit bounded
  CDN-only work during X waits only where the Stage 6 scheduler proves it
  cannot increase X density or starve metadata.
- Pass indexed manifest candidates into abandoned/download-only/stalled
  recovery, mark each processed result, and retain a pre-v3/read-only fallback
  only until one-time reconciliation completes.
- Define and test an automatic export checkpoint policy: first migration,
  explicit closure, dirty-byte/row threshold, or maximum dirty age publishes;
  tiny intermediate deltas remain durable and visibly dirty without rewriting
  GiB views. Final invocation summary must distinguish durable and exported
  generations.
- Switch dashboard phase labels and metrics to maintained truth, including
  actual X/CDN calls, descriptor hits/refreshes, durable/export generations,
  and current actionable queue. Renderer failure remains harmless.
- Preserve multi-user isolation/fairness, retry-only compatibility, diagnostic
  limits, and current no-required-flag CLI behavior.

## Safety Invariants

- No migration or archive mutation occurs before the observed numeric account
  ID matches the bound archive identity.
- At most one X request is in flight per account; CDN work cannot consume the X
  lane, create a catch-up burst, or cause an extra exact-post lookup when a
  usable descriptor exists.
- Metadata, edges, cursors/frontiers, descriptors, assets, and exports retain
  separate resumable commit boundaries. Media/export failure cannot roll back
  discovery.
- A current manifest/source is registered only with confined canonical paths
  and immutable provenance. Recovery never treats an unverified temp file or
  stale portable view as durable truth.
- Existing complete files, unavailable/manual-review outcomes, state-JSON
  queues, raw snapshots, and pre-v3 archives migrate idempotently with no
  forced download.
- Dashboard and compatibility publication remain observability/materialization
  only and cannot influence queue order or archive completion.

## No-Cheating Checks

- A normal descriptor-bearing context/authored/legacy/profile asset records
  zero X exact-post/profile calls and visible CDN attempts only.
- Normal modern completion performs no full historical posts read/rewrite and
  no recursive sidecar walk after migration.
- Normal startup after reconciliation performs no historical manifest glob or
  parse; recovery candidates come from the registry.
- An automatically deferred export is reported dirty, never current. A
  published current generation verifies both its database batch and atomic
  filesystem pointer.
- Removing avatar/background/retry endpoint sleeps does not compress actual X
  request gaps; actual-boundary telemetry remains the authority.
- Persistent workers cannot hide bootstrap, fallback, or background calls.

## Completion Requirements

- The unchanged one-command interface automatically performs identity,
  incremental modern, proven legacy, reply ancestors, and all applicable media
  work without a repair command or new required option.
- Happy-path profile/context/authored/legacy media performs no redundant X
  lookup; exceptional refresh is reasoned, bounded, and durable.
- Completed phases and compatible files/sources/manifests are not rescanned,
  rehashed, rewritten, or redownloaded on an unchanged rerun.
- Tests cover first migration, post-migration rerun, run registration, indexed
  recovery, profile replacement, state-JSON queue bridge, tiny dirty export,
  forced checkpoint, renderer failure, lock contention, child failure,
  `Ctrl-C`, restart, auth stop, and multi-user isolation.
- Dashboard and final summary reconcile with SQLite/manifests and distinguish
  durable versus exported truth.
- Focused integration/recovery/request-ledger tests, compilation,
  fingerprints, full discovery, `git diff --check`, and deliberate diff review
  pass without a live request or production archive mutation.

## Results Ledger

- The ordinary command now runs `info` and a metadata/descriptor-only modern
  timeline, ingests that run-owned source once into SQLite, and no longer
  invokes normal avatar, background, per-post retry-media, full-post rebuild,
  or recursive media-sidecar paths.
- Numeric identity proof now precedes recovery mutation, legacy preparation,
  schema migration, source reconciliation, and media-queue migration. A
  recycled-handle regression proves prior state and manifests remain
  byte-identical. Recovery is restricted to manifests that predate the current
  invocation, so the live manifest cannot classify itself as abandoned.
- The run registry is now the ordinary recovery index. Historical candidates
  are marked processed after one inspection, successful current runs are
  marked processed at completion, and a post-migration rerun returns an empty
  recovery candidate set rather than accumulating historical scans.
- Authored, legacy, context, and profile assets share one direct-CDN worker and
  one bounded exact-descriptor refresh exception path. The integration fixture
  downloads a descriptor-bearing parent with exactly one CDN call, zero X
  calls, and zero work on its unchanged rerun.
- Existing portable media evidence is migrated into captured asset jobs using
  its confined sidecar digest plus exact stat evidence, without redownload or
  an ordinary full-file hash. Missing ordinals remain actionable instead of
  being falsely completed.
- Context metadata and bytes retain separate commit/failure boundaries, but
  the old context-media X rediscovery scheduler is no longer used. Partial
  metadata commits can still drain their saved descriptors safely.
- Legacy window commits advance indexed truth only. Portable views publish on
  the initial generation, a forced/large/old checkpoint, or remain explicitly
  deferred for a tiny fresh delta. Dashboard and final summary report durable
  and published generations separately.
- Profile work uses descriptors from the mandatory identity/info result;
  normal tests prove the endpoint sequence is exactly `info`, `timeline`.
- Focused regression evidence is green: 120 order-sensitive unified/context/
  direct-media/refresh tests pass in one process; the broader integration,
  recovery, state, legacy, request-ledger, and runner-control sweep completed
  without a production archive or live X request. Compilation and
  `git diff --check` pass.
- A test harness reload that replaced `archive_x_context` midway through a
  multi-module process was removed. Fault-injection mocks and exception classes
  are now order-independent instead of appearing flaky only in aggregate.
- The Stage 6 cutover is now complete. Existing v3 archives pace the mandatory
  `info` identity request through their already-bound account lane, so a saved
  authentication stop and prior not-before time apply before transport. A new
  or pre-v3 archive necessarily performs one identity probe before an account
  database exists; after identity proof its completion boundary is persisted
  into the new lane, allowing local migration time to absorb the gap without a
  blind outer sleep.
- Modern timeline, bounded context/TweetDetail fallback, exceptional descriptor
  refresh, and every legacy cursor page now install the private actual-request
  scheduler arguments. The production floor is clamped to at least four
  seconds. Runner retry/429 sleeps and context/legacy logical-call, walk,
  window, and scheduler-round sleeps are removed only on these gated paths.
- Context targets and descriptor refreshes reuse one lazy base worker per
  account; independent legacy observations reuse one lazy legacy worker. Both
  remain sequential and retire after at most 100 items or 15 minutes. Parent
  queue/window leases remain authoritative, worker protocol IDs are random,
  and no process result commits archive truth itself.
- The mandatory info endpoint is now metadata/descriptor-only. Profile bytes,
  authored/legacy/context assets, and compatibility media share direct CDN
  transfer; direct CDN requests never acquire the X lane. Tests prove the same
  durable claim token reaches the controlled worker and that stacked logical
  pacing does not run.
- The compact dashboard deliberately continues to prioritize posts, context
  closure, asset completion, rate/ETA, and durable/export generations. Exact
  host/category request counts remain in sanitized per-operation telemetry and
  the Stage 10 audit rather than being approximated in the UI from logical
  targets or subprocesses.

## Stage Results

Stage complete after the Stage 10 audit/reopen. The one-command path now uses
the descriptor/direct-media, adaptive-legacy, incremental-local, indexed-
recovery, controlled-export, durable actual-request, and bounded reusable-
worker subsystems together. Cross-module regressions cover the existing/new
identity boundary, scheduler flags, persistent context/refresh/legacy lease
tokens, zero stacked logical sleeps, and controlled parent output parsing. No
live smoke or production archive mutation occurred.
