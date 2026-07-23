# 2-LIFECYCLE

## Current Facts

- Stage 1 mapped three sound but separately orchestrated engines. The main CLI
  owns both global locks; the legacy and context CLIs independently own the
  same locks for network work.
- Modern historical state currently uses top-level `resume`; legacy state uses
  `legacy_backfill.next_until`; context uses `_state/context.sqlite3`.
- Visakanv's `resume.cursor` is valuable immutable evidence of the modern-to-
  legacy boundary, but the current selector would also execute it on every
  later ordinary run.
- Current context seeding is local and idempotent at the graph level but has no
  durable source ledger and discovers only modern `timeline.posts*` raw files.
- A real `airkatakana` archive still owns the shared lock. This stage is a
  specification-only change and does not alter its files or process.

## Updated Assumptions

- The correct unified architecture is one outer invocation scheduler holding
  both locks and calling direct structured phase engines.
- Modern historical completion/boundary evidence and modern-head incremental
  work need separate state. Reusing or clearing `resume` would either replay
  the exhausted tail forever or destroy evidence.
- “Run without a user budget” means continue through bounded atomic units until
  a terminal/blocked state, not remove the units or their inner limits.
- Context unavailable parents are honest terminal graph boundaries; context
  `manual_review`, cycles, or maximum-depth items are not successful closure.
- Multi-user fairness is best served by phase-wide scheduling: update every
  modern head first, then round-robin historical/context backlogs.

## Big Picture Objective

Define one deterministic, resumable lifecycle for modern timeline, automatic
legacy transition, legacy history, reply-parent metadata, and all media before
refactoring any engine.

## Detailed Implementation Plan

## Authoritative State Separation

The unified command coordinates these authorities but never merges them:

| Authority | Durable location | Meaning |
|---|---|---|
| Modern historical boundary | `_state/state.json:resume` | Existing stage-3 cursor and source-run evidence. Preserved after legacy activation. |
| Modern incremental head | `_state/state.json:modern_head` | Separate current-head cutoff and optional interrupted head cursor. |
| Legacy coverage | `_state/state.json:legacy_backfill` | Contiguous source-visible UTC frontier and active window. |
| Shared timeline/legacy media | `_state/state.json:pending_media` | Recoverable post-level media failures independent of metadata coverage. |
| Context graph/work/media | `_state/context.sqlite3` | Reply edges, observations, leases, retries, pacing, closure, and context-media state. |
| Raw evidence | `runs/*` | Immutable/provisional per-phase observations and manifests. |
| Combined invocation | `archive_root/runs/*.json` | Links phase results; owns no cursor, frontier, or queue. |

### `modern_head` state

Add a schema-versioned top-level object only when legacy state exists:

```text
modern_head = {
  schema_version,
  baseline_started_at,
  last_successful_started_at,
  last_successful_completed_at,
  active: null | {cursor, started_at, date_after, saved_at}
}
```

- Automatic legacy initialization derives `baseline_started_at` from the
  exact source manifest. Reaching a legacy boundary proves that descending
  traversal already covered posts newer than that run's start cutoff.
- Existing initialized archives migrate idempotently from the hash-bound legacy
  source manifest. A missing/mismatched source manifest fails closed.
- A head run selects `modern_head.active` if present; otherwise it starts with
  no cursor and `date_after = last_successful_started_at - overlap` (or the
  baseline when no head run completed).
- Successful head completion clears only `modern_head.active` and advances its
  timestamps. Interrupted head work may store its cursor under
  `modern_head.active`. It never writes or clears top-level historical
  `resume`.
- Archives without legacy state retain current historical/incremental semantics
  until history completes or a transition is proven.

## Strict Transition Decision

Automatic initialization is evaluated only after the current modern raw data
has been merged and its safe resume state has been atomically committed.

A new transition is `proven` only when all of the following agree:

1. No legacy state already exists.
2. The run is an unrestricted historical traversal—not `--since`,
   `--post-limit`, retry-only, or an incremental modern-head run.
3. Stable profile identity is bound and matches the returned focal account.
4. The timeline stopped through the configured no-progress watchdog, was not
   interrupted, and contains no authentication/API/unknown extraction error
   that could explain the stop.
5. A durable resume cursor exists and matches the oldest accepted raw record
   and the oldest merged dataset post.
6. The source manifest, state, raw file, profile, dataset count, and cursor
   satisfy the existing stale-guarded `initialization_plan()` invariants.
7. The oldest returned metadata timestamp predates the Twitter Snowflake epoch,
   making the record's pagination domain legacy; this is detection evidence
   only and is never used for legacy pagination math.
8. The profile creation floor is earlier than the proposed frontier.

The pure classifier returns `proven`, `not_applicable`, or `ambiguous` plus
stable reason codes and non-secret evidence hashes. `ambiguous` never launches
legacy search. An ordinary modern failure remains a modern failure.

When proven, the already-held lock is the operator/stale guard:

1. Recompute the plan from disk after the modern commit.
2. Atomically write an exact private pre-init backup named by the plan token.
3. Atomically add legacy state with the existing initializer.
4. Add/migrate `modern_head` from the exact source manifest.
5. Revalidate both objects before any legacy request.

Repeated initialization is a no-op only when all evidence matches.

## Single-User Phase Order

For a normal invocation, execute:

1. **Preflight/recovery:** storage, cookies, runner fingerprints, identity,
   abandoned manifests, state validation, SQLite integrity if present.
2. **Shared pending-media retry:** retry existing timeline/legacy post media so
   older failures remain visible and do not starve indefinitely.
3. **Modern:**
   - no legacy state: current historical/incremental selector;
   - legacy state: separate modern-head selector;
   - bind identity, merge raw, commit the appropriate modern state, then
     profile/avatar/background behavior.
4. **Transition:** for an eligible watchdog stop only, classify and atomically
   initialize. A proven transition is a phase transition, not an overall
   modern failure.
5. **Legacy metadata:** resume the exact active window first, then use
   three-day root windows with recursive saturation splits until account
   floor, manual review, interruption, or advanced limit. Every leaf still
   requires two matching walks, with two distinct empty tail pages per walk.
6. **Shared media:** drain due legacy/timeline media with bounded per-item
   attempts and yt-dlp variant recovery. Transient failures persist a retry
   time; repeated refreshed 404/410 evidence becomes unavailable and yields
   `complete_with_unavailable_media` instead of perpetual partial status.
7. **Context seed:** inventory all authoritative canonical modern and committed
   legacy raws, update SQLite source ledger/local-post index/edges, and capture
   locally available parent posts.
8. **Context metadata:** drain eligible ancestor targets, preferring chain
   depth within the fairness quantum, recursively enqueueing parents.
9. **Context media:** drain media targets for captured context observations.
10. **Export/readout:** integrity-check SQLite, atomically rebuild context
    datasets, finalize phase manifests, and write combined invocation status.

If legacy is blocked in manual review, context seed/metadata/media may still
run for already durable posts. The combined result remains `manual_review`.
If modern fails due authentication, identity, or unexplained API/extraction
failure, later network phases do not run; local integrity/readout may run. This
avoids multiplying requests under unsafe credentials/source behavior.

## Context Source Ledger and Bootstrap Semantics

Upgrade the context schema transactionally to include:

- `seed_sources(relative_path PRIMARY KEY, sha256, source_kind, run_id,
  processed_at, record_count, edge_count)`;
- `local_posts(post_id PRIMARY KEY, raw_json, sha256, source_path,
  source_kind, observed_at)` for target-authored canonical timeline records.

Migration of an existing context database requires an exact private SQLite
backup before schema change, transactional schema/version update, integrity
checks before/after, and idempotent reopen behavior.

Authoritative sources are:

- finalized modern timeline raw paths whose valid records were offered to the
  shared dataset merge, including safely stalled raws;
- canonical legacy window raw paths referenced by manifests with
  `metadata_confirmed` and `state_committed` true;
- never legacy walk raws, `.tmp` files, uncommitted canonical raws, profile
  raws, context fetch raws, or arbitrary files outside the user run tree.

Seeding each source transactionally:

1. Verify relative path, private archive containment, SHA-256, and source
   manifest authority.
2. Upsert target-authored raw observations into `local_posts` using a
   deterministic richness/observation rule.
3. Add every target-authored non-repost reply edge.
4. Capture any needed parent found in `local_posts`, even when its source was
   processed during an earlier invocation.
5. Record `seed_sources` only after all work commits.

A known relative path whose hash changes is manual review, not an automatic
reseed. A crash before the ledger commit safely repeats the transaction.

## No-Budget Engine Semantics

### Legacy

- Engine input `max_root_windows: int | None`; `None` is the normal default.
- `None` loops only across existing bounded root/leaf/walk/request operations
  until `complete`, `manual_review`, interruption, or error.
- An integer is an advanced diagnostic/rollout cap and returns `limited` after
  that many committed root windows without changing frontier meaning.

### Context metadata and media

- Engine input `max_posts: int | None`; `None` is the normal default.
- After each item, query authoritative queue state rather than inferring
  closure from `claim() is None`.
- No pending/retryable/leased targets means the subphase is terminal.
- Retryable work with a future eligibility time waits interruptibly until the
  earliest target, using short logged sleep slices, then continues.
- Manual-review targets remain blocked and make metadata/media subphase
  `manual_review`; explicit unavailable targets are honest terminal boundaries.
- Authentication/account-state evidence stops all remaining network phases.
- Optional integer bounds return `limited` with queue truth unchanged.
- Existing per-request timeouts, maximum attempts, exponential backoff cap,
  lease recovery, max depth, fairness quantum, disk-space guard, and pacing
  remain mandatory internals.

## Multi-User Scheduling

One invocation continues to hold one pair of global locks, but schedules by
phase to avoid starvation:

1. Preflight and modern phase once for every target in input order.
2. Round-robin one committed legacy root window per eligible target until all
   are terminal/blocked or an advanced global per-target cap is reached.
3. Seed every target locally.
4. Round-robin one context fairness quantum per target until metadata terminal.
5. Round-robin context-media quanta until terminal.
6. Export and finalize every target independently.

An authentication failure may stop network work globally. Other per-target
manual review/failure does not starve later targets; `--keep-going` becomes
effectively true across independent backlog phases while final exit remains
nonzero if any target is unsuccessful.

## Option Semantics

| Option | Unified behavior |
|---|---|
| no phase options | Full lifecycle toward all honest terminal states. |
| `--dry-run` | Validate local readable inputs and print phase/backlog plan; zero writes/network/DB creation. |
| `--post-limit` | Modern diagnostic only; current no-state-advance rule remains and automatic transition/legacy/context network work is skipped. |
| `--since` | Explicit modern-only acquisition boundary; do not infer a new legacy transition or launch backlogs. Local readout remains allowed. |
| `--full-rescan` | Full modern history; may prove a new transition, then continues unified lifecycle. Existing legacy/context state is never reset. |
| `--retry-failed-only` | Skip new metadata; retry shared timeline/legacy media and eligible context media, then export/readout. |
| `--no-reposts` | Applies to new modern material and a newly initialized legacy policy. Existing legacy state retains its immutable source policy; context parent scope is unaffected. |
| `--keep-going` | Retained for compatibility; independent target backlogs already continue safely, while final status remains truthful. |
| advanced phase limits | Optional test/rollout controls; produce `limited`, never routine requirements. |

`--seed-reply-context` becomes a deprecated no-op/compatibility alias during
one release (with a message that seeding/resolution is automatic), then can be
removed separately. It must not change phase behavior.

## Status and Exit Semantics

Each target reports structured substatus for `modern`, `transition`, `legacy`,
`shared_media`, `context_seed`, `context_metadata`, `context_media`, and
`context_export`.

- `success`: all applicable metadata phases reached honest terminal states,
  no manual review exists, and all recoverable media is complete.
- `partial`: metadata coverage/closure is valid but media remains pending or
  unavailable after bounded attempts.
- `manual_review`: legacy or context ambiguity/cycle/depth/retry exhaustion
  requires explicit operator action.
- `limited`: an explicitly supplied advanced diagnostic cap stopped otherwise
  safe resumable work.
- `failed`: authentication, identity, schema, integrity, storage, or unexplained
  source failure.
- `interrupted`: operator/signal stop; active state/lease is replayable.

Unavailable/deleted/private parent boundaries count as context graph closure
when conservatively classified. They remain enumerated in readout and do not
become “captured.” `success` and intentional `limited` return zero; partial,
manual review, failed, and interrupted return nonzero (interrupt remains 130).

## No-Cheating Checks

- This design retains every inner safety bound while removing mandatory user
  counts.
- Historical `resume`, `modern_head`, legacy frontier, and context queue are
  explicitly separate.
- Context completion requires worker closure and export, not only seeding.
- Legacy replies enter the same authoritative source ledger as modern replies.
- Other-author parents are stored; siblings, descendants, quotes, and broad
  conversations remain excluded.
- Specialized CLIs become adapters over shared engines, not subprocesses used
  by the main command.

## Completion Requirements

- Every phase/state/option has deterministic behavior above.
- Normal single-user and multi-user paths require no operator budget.
- Modern-after-legacy selection preserves boundary evidence without replaying
  it.
- Transition initialization, context bootstrap, closure, media, and blocked
  semantics are fully specified.
- Stage 3 can implement the pure detector/state pieces without inventing
  lifecycle policy.

## Stage Results

- Specification completed on 2026-07-22 with no executable or production
  changes.
- Resolved the central state issue by introducing separate `modern_head`
  checkpointing while preserving top-level historical `resume`.
- Chose one outer lock owner, direct-call engines, phase-wide multi-user
  fairness, context schema/source-ledger migration, and closure-aware optional
  budgets.
- Defined exact phase, option, status, failure, and retry behavior for
  implementation and tests.
