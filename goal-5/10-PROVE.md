# 10-PROVE

## Current Facts

- Stages 1–9 are complete. The normal one-command path is cut over to captured
  descriptors, direct CDN media, bounded exceptional X refresh, adaptive
  legacy acquisition, indexed local truth, controlled exports, and live
  maintained counters.
- Stage 9's focused cross-module tests, compilation, and whitespace checks are
  green. A real combined-process test-order leak was removed before this stage.
- No live X request, production archive mutation, or resume has been used as
  proof. The user explicitly stopped the production run while this goal is
  implemented.
- The worktree contains the full Goal-5 implementation plus pre-goal changes;
  proof must preserve unrelated user work and review the complete diff without
  destructive cleanup.

## Baseline Ledger

- Stage 1 records the before/after measurement contract and actual-request
  boundary.
- Stages 3–7 record descriptor, direct-media, fallback, pacing, and adaptive
  legacy thresholds.
- Stage 8 records at least 90% small-delta payload-I/O reduction, one-time
  5,000-manifest reconciliation, maintained 100,000-target counters, atomic
  export recovery, and explicit corruption detection.
- Stage 9 proves normal descriptor media uses one CDN request and zero X
  requests, profile uses no separate extractor, current timeline payload is
  parsed once, successful runs leave no recovery backlog, and durable/exported
  generation skew is visible.

## Updated Assumptions

- Deterministic temporary-root fixtures and actual transport-boundary ledgers
  are authoritative for this stage. A live smoke is optional and remains
  prohibited without explicit permission.
- Gallery-dl compatibility must be checked against the installed pinned
  version and the repository's exact shim fingerprints; passing unit behavior
  alone cannot waive a fingerprint mismatch.
- Full discovery must run in its natural order and also retain the combined
  order-sensitive proof from Stage 9.
- Documentation must describe the normal command, first-run migration cost,
  direct-versus-refresh media behavior, durable-versus-portable generations,
  unavailable/manual-review outcomes, restart semantics, and explicit audit or
  export behavior without introducing a required operator flag.

## Big-Picture Objective

Produce a final, reproducible body of evidence that Goal 5 is correct, safer,
materially leaner, compatible with the pinned extractor, understandable to the
operator, and ready to resume—without touching the production archive.

## Detailed Verification Plan

1. Run full natural-order test discovery and record the exact test count and
   elapsed time.
2. Run compile checks for every changed/new Python entry point, shell syntax
   for wrappers, and `git diff --check`.
3. Run the pinned gallery-dl version/fingerprint preflights for modern and
   legacy runners; fail closed on any mismatch.
4. Re-run the explicit no-cheating and scale proofs: one-CDN/zero-X descriptor
   path, actual request ledger, sparse legacy reduction, 5,000-manifest
   one-time scan, 100,000-target maintained counters, and >=90% local I/O
   reduction.
5. Verify deterministic export reproduction, initial/deferred/forced
   generations, source/media corruption audits, low-disk behavior, and restart
   recovery from partial publication.
6. Review the complete diff for secret leakage, unsafe path handling,
   pre-identity mutation, unbounded retry/concurrency, stale legacy portable
   rewrites, duplicated X lookup paths, accidental production defaults, and
   dead compatibility branches that can still enter normal execution.
7. Update operator-facing documentation and this results ledger with exact
   commands, outcomes, remaining caveats, and the safe resume command.

## Safety Invariants

- No verification command may contact X, mutate `/mnt/Bibliotheque`, use real
  cookies, or resume an archive.
- Tests use isolated temporary roots and fake transports; no result may depend
  on suppressing a request or bypassing a safety check.
- Fingerprints, identity binding, one-X-lane pacing, independent legacy
  confirmation, atomic evidence, retry ceilings, unavailable/manual-review
  outcomes, and fail-closed path validation remain mandatory.
- A failed proof reopens the owning implementation stage; it is never recorded
  as an accepted caveat merely to finish the goal.

## No-Cheating Checks

- Descriptor-bearing context, authored, legacy, and profile assets show zero
  exact X refresh calls; the exceptional fixture shows the bounded refresh
  explicitly.
- Actual transport telemetry, not subprocess counts, proves request totals and
  concurrency.
- Unchanged reruns perform zero payload read/rewrite/redownload for indexed
  sources, exports, compatible media, and processed manifests.
- Portable generation is never presented as current when durable indexed truth
  is ahead.
- Sparse legacy speedup retains two independent matching observations and the
  same canonical result set.

## Completion Requirements

- Full discovery, compile/shell checks, fingerprints, whitespace check, focused
  no-cheating/scale tests, and deliberate diff review all pass.
- Operator documentation accurately explains behavior, first-run migration,
  progress/export semantics, exceptional refreshes, and safe restart.
- Every Goal-5 success metric is linked to an exact test or recorded command.
- No unresolved P0/P1 correctness, safety, compatibility, or data-loss issue
  remains.
- Results explicitly state that no live smoke occurred unless the user later
  authorizes one.

## Results Ledger

- Natural-order discovery completed with **396 tests in 38.561 seconds**
  (`ELAPSED=38.96`), exit zero. This run occurred after the Stage 9 scheduler/
  worker cutover, identity bridge, metadata-only info change, and documentation
  update.
- Both pinned compatibility runners reported exactly `1.32.4`. `compileall`
  passed for `scripts` and `tests`; `bash -n` passed for all archive wrappers;
  `git diff --check` passed.
- The explicit 14-test no-cheating/scale ladder passed in 13.819 seconds. It
  covered one-CDN/zero-X media, the unified descriptor path, the actual HTTP
  gate, zero-outer-sleep and restart spacing, adaptive sparse legacy reduction,
  >=90% small-delta I/O reduction, one-time 5,000-manifest reconciliation,
  maintained-counter 100,000-target progress, deterministic zero-payload
  unchanged exports, live durable/export generation truth, the 1,000-item
  bounded-worker startup/session metric, and persistent context/refresh leases.
- The explicit 10-test failure ladder passed in 1.091 seconds. Low disk spent no
  request or attempt; Ctrl-C/partial Range recovery, crash after file placement,
  corrupt existing media, same-size source corruption audit, source replay,
  every export-publication crash boundary, durable auth stop, and pre-identity
  mismatch all retained fail-closed/restart-safe truth.
- Deliberate normal-path review found the Stage 6 integration omission recorded
  by the earlier audit and did not waive it. The repaired path now gates existing
  account info, modern timeline, context/fallback, descriptor refresh, and
  legacy actual calls; persists the one unavoidable new-account identity-probe
  bridge; and reuses bounded account workers for repeated context/refresh/
  legacy items. The normal path has no avatar, background, per-post retry-media,
  logical context-delay, legacy walk/window-delay, or full-history rewrite.
- The same review found that `info` still permitted byte downloads. It is now
  explicitly metadata/descriptor-only; the shared direct-CDN worker owns
  profile bytes. Direct CDN work remains sequential in the normal pipeline—the
  optional overlap experiment was rejected because the proven request savings
  and local I/O reductions do not require added concurrency or commit states.
- Protocol/control IDs are random and token matched. Scheduler/telemetry state
  retains only allowlisted aggregate labels and timing; no URL, query, cookie,
  header, signed descriptor, handle, post ID, or opaque cursor is added to those
  artifacts. Existing private archive logs/manifests retain their established
  confined evidence behavior.
- Operator documentation now describes the one-time identity-guarded local
  migration, durable actual-request lane, bounded process/session reuse,
  descriptor-direct media with exceptional refresh, unavailable/manual-review
  semantics, adaptive legacy policy, live 72x20 bottom dashboard, and deferred
  durable-versus-published export generations.
- No test contacted X, read real cookies, resumed an archiver, or mutated
  `/mnt/Bibliotheque`. No live smoke was run. The safe normal resume command is
  `scripts/archive-x --user USERNAME` (or
  `uv run scripts/archive-x --user USERNAME`).

## Stage Results

Stage complete. The earlier 388-test proof correctly reopened Stage 9 instead
of accepting an isolated mechanism as production integration. After repairing
that omission, the final 396-test suite, focused proof ladders, fingerprints,
static checks, documentation review, and deliberate normal-path audit all pass.
No unresolved P0/P1 correctness, safety, compatibility, data-loss, or automatic
high-cost fallback remains. Production may be resumed with the unchanged normal
command when the operator chooses.
