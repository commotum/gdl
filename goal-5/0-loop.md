# Goal 5 Execution Loop

Use this loop to implement `goal-5/0-plan.md` without weakening the original
objective: make the unified X archiver materially faster and lower-footprint by
reusing extraction results, reducing actual X calls and client churn, and
making normal local work incremental while preserving archive evidence and the
one-command interface.

## Stage Order

Work through the first incomplete stage unless current evidence requires the
plan to be corrected:

1. `1-MEASURE`
2. `2-STATE`
3. `3-DESCRIPTORS`
4. `4-DIRECT-MEDIA`
5. `5-FALLBACKS`
6. `6-PACING`
7. `7-LEGACY`
8. `8-LOCAL`
9. `9-INTEGRATE`
10. `10-PROVE`

Do not skip measurement and state design to make a quick network patch. The
current logical request counters miss internal API/bootstrap work, and the
schema choice affects every later stage.

## Repeatable Loop

1. Sync current state with actual files, tests, git status, existing stage
   evidence, and live-safe read-only facts.
2. Determine whether a production archive is running. Do not stop, restart, or
   mutate it without explicit authorization.
3. Update `goal-5/0-plan.md` when code or measured facts contradict the plan.
4. Select the first incomplete stage and create or refresh
   `goal-5/[INDEX]-[SHORTHAND].md` from the template below.
5. Write the stage's baseline, request/I/O budget, safety invariants, and
   no-cheating verifier before implementation.
6. Implement the smallest coherent slice that can satisfy the stage.
7. Add focused unit, request-ledger, query-plan, migration, fault, and
   integration tests appropriate to the slice.
8. Run the verification ladder, record exact commands/results, and inspect the
   diff deliberately.
9. Record stage results, changed assumptions, rejected alternatives, and any
   measured P1/P2 decision.
10. Fold authoritative results back into `0-plan.md` before advancing.
11. Continue until the original objective and all required P0/P1 outcomes are
    genuinely achieved. If stopping, leave a small explicit next action and a
    complete restart-safe handoff.

## Core Invariants

- Do not narrow the user's objective or silently convert required work into a
  future idea.
- Do not mark a stage complete merely because tests are green; tests must
  directly cover its completion requirements.
- Count actual HTTP/API calls, not subprocesses, targets, or `fetch_post()`
  invocations.
- Report X API, X bootstrap/support/profile, CDN, redirect, and retry calls
  separately.
- Never log full URLs/query strings, cookies, authorization data, signed
  headers, transaction secrets, or private descriptor content.
- The context happy path must not invoke an X tweet extractor after usable
  descriptors were captured.
- Authored/legacy media with usable descriptors must attempt direct CDN repair
  before exact-post refresh.
- The mandatory info identity probe stays unless a tested replacement provides
  identical numeric-ID protection before mutation.
- Unchanged profile media must not cause separate avatar/background X
  extractors.
- Do not download media for every post in a conversation/search response.
  Persist jobs only for exact accepted/canonical post IDs.
- Keep metadata, reply edges, cursor/frontier progress, and media transfer on
  separate failure boundaries.
- Never hold a SQLite write transaction during X/CDN/file I/O.
- Keep at most one X request in flight. Concurrency introduced here is CDN-only
  and bounded.
- Removing process/endpoint sleeps requires prior proof that durable actual-call
  pacing preserves or increases minimum spacing and prevents restart bursts.
- Honor rate resets, 429s, account locks, challenges, and global authentication
  stops. Do not use evasion tactics, rotating identities/proxies, or paid
  services.
- Preserve modern overlap/cursor replay unless equivalent no-gap evidence is
  proven.
- Preserve two independent matching valid observations for new legacy
  intervals, request caps, empty-tail proof, exact time bounds, identity
  validation, and deterministic splitting.
- An invalid non-contradictory walk may not erase an earlier valid observation;
  a mismatching valid walk must not be ignored.
- Raw snapshots remain immutable provenance. Indexed truth and JSONL exports
  must be reproducible from committed sources.
- Ordinary incremental work should not read/rewrite total history. Explicit
  full audit/export may do so and must be labeled as such.
- Source-ingestion caching must detect ordinary mutation and retain a separate
  full-content integrity audit.
- Queue optimizations preserve full-depth preference, shared-parent priority,
  fairness, lease recovery, retries, cycle limits, and manual-review truth.
- A download ledger or pathname alone is not file completion; verify file and
  hash/sidecar evidence.
- Bound process/session lifetime, batch size, descriptor refresh generations,
  CDN attempts, and retry backoff durably across restart.
- Existing descriptorless queues may use explicit compatibility fallback, but
  newly captured descriptors cannot silently route through old behavior.
- Do not automatically reopen or retry historical manual-review work forever.
- Low disk, renderer failure, export failure, or `Ctrl-C` cannot manufacture a
  false terminal/captured outcome.
- Preserve current dirty/unrelated user work and fail closed on unexpected
  overlap.

## P0/P1/P2 Decision Rule

- P0 items are mandatory and cannot be rejected.
- P1 items require implementation or a stage artifact with measurements,
  safety analysis, and an explicit plan amendment explaining why the item has
  no material benefit or cannot be made equivalently safe.
- P2 items are implemented only when Stage 1/8 shows more than 1% worker
  CPU/wall overhead, material storage I/O, or measurable commit delay on the
  large deterministic fixture.
- Do not reject an optimization because the current small unit fixture is too
  small to reveal its cost; use the planned large fixture.
- Do not accept an optimization based only on fewer sleeps. Verify actual calls,
  request density, bytes, process/session starts, and failure behavior.

## Required Baseline and Result Ledger

Every performance-affecting stage records the same before/after fields:

- actual X API calls;
- X bootstrap/support/profile calls;
- CDN calls, redirects, retries, and bytes;
- logical operations and accepted records/assets;
- runner process starts and bounded session restarts;
- first/last call timestamps, minimum gap, peak X concurrency, and wait source;
- exact descriptor hits, misses, refreshes, and generations;
- legacy windows, valid/invalid walks, search calls, total API calls, and
  canonical records;
- raw/source bytes hashed and parsed;
- JSONL bytes read/written and materialization generations;
- SQLite rows scanned/changed, query plan, temp sorts, and transaction time;
- files verified, captured, retryable, unavailable, or review;
- wall time, CPU time where stable, and interruption/restart result.

Aggregate "requests," "items/hour," or total wall time alone is insufficient.

## No-Cheating Request Checks

Before claiming lower network footprint, the verifier must prove:

- telemetry observes the actual HTTP boundary used by gallery-dl/yt-dlp/direct
  downloader, including internal exact-result-to-detail fallback;
- a usable context descriptor causes zero subsequent X calls;
- a usable authored/legacy descriptor causes zero exact-post X calls;
- unchanged profile descriptors cause zero avatar/background extractor calls
  and zero CDN calls;
- descriptor refreshes are explicit, reasoned, and bounded;
- CDN attempts remain visible rather than being relabeled as local work;
- persistent/batched runners cannot perform unaccounted background calls;
- rate-limit waits and retries are attributed to the call that caused them;
- no additional users/accounts/proxies are used to increase throughput.

## No-Cheating Local-I/O Checks

Before claiming incremental behavior, the verifier must prove:

- adding a small delta does not parse or rewrite all historical normalized
  posts;
- K legacy windows do not materialize full JSONL K times;
- an unchanged context seed does not hash or parse prior source payloads;
- an unchanged export generation writes zero full-view bytes;
- queue hot-path query plans contain no target-table full scan or temporary
  global ordering sort;
- progress/recovery readers do not become hidden full scans at high frequency;
- a deferred/coalesced export cannot be reported as current before atomic
  publication;
- explicit audit/export remains capable of full verification/materialization.

## Verification Ladder

For each stage, record every applicable rung and exact command:

1. Static schema, descriptor, telemetry, privacy, type, and format checks.
2. Focused unit tests for the changed runner, scheduler, database, downloader,
   legacy, export, or progress layer.
3. Actual-boundary request-ledger/no-cheating tests with fake transports.
4. SQLite `EXPLAIN QUERY PLAN` regression checks and transaction/lock tests.
5. Temporary-root integration tests with sanitized X response/file-event/CDN
   fixtures.
6. Migration and compatibility tests using constructed or copied fixture state.
7. Fault injection at request, pacing, process, transaction, descriptor,
   transfer, raw commit, cursor/frontier, and export boundaries.
8. Deterministic sparse/dense legacy and large-archive small-delta benchmarks.
9. Restart/`Ctrl-C` tests proving leases, pacing, partials, and children recover.
10. Full repository test discovery and pinned-runner fingerprint checks.
11. `git diff --check`, status review, and deliberate diff inspection against
    the recorded dirty baseline.
12. Separately authorized bounded live smoke only after all fixture proof.

## Stage File Template

```markdown
# [INDEX]-[SHORTHAND]

## Current Facts

- Facts from current code, tests, docs, prior stage results, and safe read-only
  evidence.

## Baseline Ledger

- Actual request categories/counts and timing.
- Process/session starts.
- Local bytes/query plans/materialization behavior.
- Fixture/workload definition so results are reproducible.

## Updated Assumptions

- Assumptions still valid.
- Assumptions changed by evidence.
- Assumptions requiring a test before trust.

## Big-Picture Objective

- Restate this stage's contribution to lower footprint and archive safety.

## Detailed Implementation Plan

- Concrete code, schema, migration, docs, and test changes.
- Files expected to change.
- Commit/restart boundaries and compatibility behavior.

## Safety Invariants

- Requirement-level invariants this stage must preserve.

## No-Cheating Checks

- Actual-request and/or local-I/O checks that forbid hidden fallback/full scans.

## Completion Requirements

- Requirement-by-requirement evidence.
- Exact verification commands and thresholds.
- Documentation/migration updates.

## Results Ledger

- Before/after values using the required ledger.
- Tests/commands and outcomes.
- Faults exercised.

## Stage Results

- What changed and what was learned.
- Rejected alternatives with evidence.
- P1/P2 decisions.
- What must change in `0-plan.md` before the next stage.
```

## Stop/Resume Handoff

Before stopping:

- Record the last completed stage and first incomplete requirement.
- Record exact git status and separate Goal 5 edits from the pre-existing dirty
  archive/dashboard work.
- Record whether production archive processes/tmux panes are live based on
  read-only evidence; never infer from stale JSON alone.
- Record schema version, migration state, backup path, and whether rollback was
  tested.
- Record baseline/result ledger values and fixture definitions.
- Record actual request categories, descriptor hit/refresh ratio, minimum X
  gap, peak concurrency, and session/process count.
- Record legacy valid evidence, active interval/frontier, and any split/recovery
  state without exposing opaque cursors.
- Record source/export generations, bytes read/written, query plans, and whether
  a portable export is current.
- Record tests run, failures, compatibility fingerprints, and verification
  deferred.
- Record partial files, leases, next-request time, temporary artifacts, and
  isolated test roots needing cleanup.
- Leave the next action small, explicit, safe, and independent of production
  mutation where possible.
