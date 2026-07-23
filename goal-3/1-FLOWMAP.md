# 1-FLOWMAP

## Current Facts

- Work is isolated on branch `goal-3-unified-x-archive`, created from clean
  commit `61b2252` on 2026-07-22.
- A real unrelated archive is currently running in tmux `x`:
  `uv run python scripts/archive_x.py --user airkatakana`. Its gallery-dl
  timeline child holds the shared repository/archive locks. No executable code
  may be edited underneath it and it was not interrupted by this stage.
- The pre-change suite attempted all 126 tests. Three legacy CLI tests were
  prevented from acquiring the real repository lock (two errors and one
  consequent assertion failure); the other 123 tests passed. This is an
  environmental baseline obstruction, not evidence of a code regression.
- Both gallery-dl runners report pinned version 1.32.4. The current wrapper and
  legacy wrapper are mode `0700`; the context wrapper is mode `0755`.
- Visakanv remains at 258,070 dataset posts, 257,986 authored rows, 84 reposts,
  two pending timeline/legacy media items, modern cursor
  `3_29116490825/`, and legacy frontier `2010-10-29T00:00:00Z`.
- Visakanv still has no `_state/context.sqlite3`. The state, dataset, and exact
  pre-legacy backup SHA-256 values remain respectively
  `47f4f2c552c2307dfe55bb0f729ca24d9ea49101f16102acec67182d38092a38`,
  `fee363633161e49645a0efd070d76aa02085653651f059045d9356a3a4e4405c`,
  and `98821e48e631989607bef3e917d334e70d7f169f6dee97659851065edf384f67`.

## Updated Assumptions

- The main process is already the correct single lock owner: `main()` acquires
  the repository lock and mounted archive lock once around the entire ordered
  target loop.
- Calling either specialized CLI from inside the unified process would
  deadlock or fail immediately because both independently reacquire those
  exact locks.
- The existing `archive_user()` is not itself a complete modern phase API. It
  creates/finalizes its own run, performs identity and pending-media recovery,
  timeline work, optional context subprocess seeding, profile media, state
  writes, and status aggregation in one function.
- Once legacy is initialized, the current modern selector would still choose
  `state.resume.cursor` and replay the exhausted historical tail. A unified
  implementation needs an explicit modern-head path while retaining that
  cursor as immutable boundary evidence.
- Full context bootstrapping cannot rely on `timeline_raw_paths()` as currently
  written because it matches only `raw/timeline.posts*.jsonl*`; canonical
  legacy window raw files have different names.
- A no-budget context engine can drain current eligible work safely only if it
  distinguishes “nothing eligible now” from closure, delayed retry work, and
  manual review. `run_worker()` currently returns when `claim()` returns none
  and does not by itself state why.

## Big Picture Objective

Establish the exact current control-flow and authority boundaries that the
unified lifecycle must refactor without changing production or live behavior.

## Detailed Implementation Plan

### Modern flow and authority

- `scripts/archive-x` is a bash wrapper that executes
  `uv run python scripts/archive_x.py`.
- `archive_x.main()` validates inputs/cookies/version, resolves the mounted
  root, acquires both global locks once, then processes targets sequentially.
  It stops on the first non-success unless `--keep-going`; a single large
  target therefore currently blocks every later target.
- `archive_user()` creates one immutable run directory and manifest, recovers
  abandoned/stalled/download-only evidence, selects cursor/cutoff, probes and
  binds numeric identity, retries shared pending media, runs the timeline,
  merges raw metadata, rebuilds datasets, commits timeline state, optionally
  shells out to context `seed`, and only after complete timeline metadata runs
  avatar/background endpoints.
- `select_timeline_state()` gives a saved `resume` cursor precedence. Without
  a cursor, ordinary incremental work uses
  `last_successful_started_at - overlap_hours`. A completed timeline clears
  `resume`; a failed/stalled timeline retains or advances it.
- Dataset merge precedes `update_timeline_state()`, so replay is the safe crash
  behavior. Download-only failures enter `pending_media` and may still permit
  metadata completion.
- A stalled timeline returns before profile media and is an overall failure.
  There is no transition decision, phase linkage, legacy call, context worker,
  context media, or context export in the normal flow.

### Legacy flow and authority

- Pure helpers validate separate schema-versioned `legacy_backfill` state,
  derive an exact stale-guarded initialization plan, initialize idempotently,
  claim/complete/retry windows, validate walks, split leaves, and recover
  manifests.
- The CLI owns plan/status/init/retry/run parsing and lock acquisition.
  `run_legacy_archive()` assumes an initialized state but still receives an
  argparse namespace and requires integer `args.windows`.
- One root window is claimed from `next_until` toward `floor_since`. Each leaf
  is fetched twice until two consecutive valid walks match. Request-cap leaves
  split exactly, newer first; ambiguous/mismatched work reaches manual review.
- Canonical raw is written, merged into the shared post dataset, legacy media
  is appended to the shared `state.pending_media`, and only then is
  `next_until` committed. The modern `resume` cursor is untouched.
- `run_legacy_archive()` creates a separate immutable run manifest. Its
  `window_limit` is execution policy, not coverage semantics; internal request,
  walk, leaf, retry, and timeout limits are independent.

### Context flow and authority

- `ContextDB` owns a private schema-versioned SQLite database with target,
  edge, observation, pacing, identity, lease, retry, media, and chain-fairness
  state. Transactions/savepoints and integrity checks are already central.
- `seed_context()` is local-only. It binds stable target identity, scans either
  all `timeline_raw_paths()` or explicitly supplied raw paths, accepts only
  authored reply candidates, creates child-to-parent edges, and immediately
  captures a parent already present in local raw input when possible.
- The main archive's `--seed-reply-context` hook launches the context CLI as a
  subprocess after modern dataset/state commit and supplies only the new
  timeline raw path. It makes no parent request and has no legacy seed path.
- `run_worker(..., media=False)` claims one metadata target at a time. It
  reserves durable pacing, fetches exactly one focal post, conservatively
  classifies failures, captures arbitrary-author parents, enqueues the next
  ancestor, and prefers the current chain within a bounded fairness quantum.
- `run_worker(..., media=True)` uses the same target table's independent media
  state, enforces 5 GiB free space, verifies asset sidecars/SHA-256, and never
  rolls back captured metadata.
- The context CLI requires `--max-posts` for both `run` and `media`, owns both
  global locks for network work, and separately exposes seed/status/integrity/
  export/retry. Seed/export/status do not consistently share the main lock,
  which must be considered when the orchestrator writes SQLite/datasets.
- `export_datasets()` atomically rebuilds `context-posts.jsonl`,
  `reply-edges.jsonl`, and `context-status.json`. Other-author parents are
  normalized with relationship `context`; authored parents retain post/reply
  semantics.

### State and manifest authorities

- `_state/state.json`: modern identity/cursor/incremental timestamps, legacy
  state, shared pending timeline/legacy media, and recovery provenance.
- `_state/context.sqlite3`: context targets/edges/observations/pacing/leases/
  metadata and media state. It must not acquire timeline or legacy authority.
- `runs/RUN/manifest.json`: one modern or legacy run's immutable/provisional
  evidence. A unified invocation needs structured linkage rather than copying
  phase truth into one cursor.
- `archive_root/runs/INVOCATION.json`: current top-level per-target status only;
  it can become the combined phase summary without replacing phase manifests.
- Derived dataset JSONL/JSON is rebuildable; raw JSONL, manifests, state, and
  SQLite are authoritative evidence.

### Option matrix to settle in Stage 2

- Default: run modern head/history, safe legacy transition/resume, shared
  media recovery, context seed/resolve/media/export.
- `--dry-run`: all phase planning, zero writes/network.
- `--post-limit`: diagnostic modern-only or explicitly bounded all-phase
  smoke; it cannot authorize state advancement accidentally.
- `--retry-failed-only`: shared timeline/legacy media only under current
  semantics; decide whether context media is included without timeline work.
- `--full-rescan`/`--since`: modern acquisition controls only and must not
  reset legacy/context authorities.
- `--no-reposts`: controls new timeline/legacy source policy but must not alter
  context parents or skip already queued media.
- Multiple users: require explicit fairness/ordering because full historical
  and context closure for the first target may run for days.
- Advanced rollout limits: separate optional completed-root legacy,
  context-metadata, and context-media budgets; omission means continue toward
  terminal state.

## No-Cheating Checks

- No executable source, production state, dataset, SQLite database, or archive
  process was changed or stopped.
- The mapping comes from the actual branch code and production readouts, not
  only prior plans.
- The failed baseline checks are recorded as lock contention rather than
  relabeled passing.
- Context seeding is explicitly distinguished from context resolution and
  media completion.

## Completion Requirements

- All current lock/state/write/manifest/media authorities are named above.
- Modern-after-legacy and legacy-aware context bootstrap gaps are identified
  with concrete functions and paths.
- Every current CLI-only budget and subprocess boundary is identified.
- Production hashes/counts and the active unrelated process are frozen.
- Stage 2 has an explicit option/lifecycle decision list.

## Stage Results

- Stage completed as a read-only characterization on 2026-07-22.
- Commands inspected all function inventories, main/engine/parser paths,
  wrappers, tests, production state/counts/hashes, runner versions, and host
  process state.
- 123/126 tests passed; three legacy CLI tests were blocked by the active real
  archive lock. The full suite must be rerun after tmux `x` releases it.
- The main architectural decisions now required are: one external lock owner,
  shared direct-call engines, a post-legacy modern-head mode, complete raw
  seeding across both ID eras, closure-aware no-budget context loops, and a
  multi-user fairness policy.
