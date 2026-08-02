# 8-LOCAL

## Current Facts

- No production `archive-x`, context, legacy, or pinned-runner process was
  visible at stage start; the read-only process check returned only its own
  sandbox command. This stage uses temporary roots only.
- Stage 7 made confirmed legacy windows indexed truth and reduced their
  compatibility materialization to one pass per run. Modern timeline commits
  still call `update_post_dataset()`, which reads the complete posts view and
  rewrites posts, authored posts, and reposts for each small delta.
- `canonical_seed_sources()` still sorts and parses every historical run
  manifest and hashes every selected raw source. `seed_context()` then parses
  every source once to build global candidate sets and parses each new source a
  second time during insertion. An unchanged rerun therefore hashes and parses
  all historical payloads before skipping them.
- `update_media_dataset()` recursively reads every sidecar and rewrites the
  complete media view even when nothing changed. Direct asset jobs already
  retain final path, hash, size, stat identity, and descriptor metadata, but
  there is no indexed portable-media row for existing or newly captured files.
- `export_datasets()` loads all context observations/edges and replaces all
  three context views on every call. Schema v3 has archive/export generations
  and batches, but no generation materializer or atomic current-export
  pointer uses them yet.
- Stage 2 installed indexed hot claim paths and transactionally maintained
  target/media/edge/observation counters. The live dashboard and read-only
  summary still group-scan targets, observations, errors, and conversation
  closure, and archive totals still scan every run manifest.
- Startup still performs multiple independent full manifest passes for
  abandoned-run finalization, stalled/download recovery, media
  reclassification, dashboard totals, and seed discovery. `run_registry` and
  `current_pointers` exist but are not populated by ordinary runs.
- Raw snapshots remain immutable provenance. Existing top-level JSONL files
  are compatibility views and cannot be treated as current at a new database
  generation without verified publication evidence.

## Baseline Ledger

- Reproducible post fixture: 5,000 normalized posts plus three one-post deltas
  rereads 4,988,811 bytes and rewrites 9,985,170 bytes for 924 delta bytes, a
  16,205.6x read/write amplification.
- Reproducible source fixture: a 2,000-record, 606,679-byte source is visited
  4,000 times on first seed; an unchanged second seed still visits 2,000
  records and hashes all 606,679 bytes while processing zero files.
- Reproducible media fixture: an unchanged 100-sidecar archive reads all 100
  sidecars and replaces the full media view again.
- Reproducible context fixture: an unchanged 1,000-observation export replaces
  the full context JSONL with a new inode and identical payload size.
- The 100,000-target / 50,000-edge / 33,333-observation fixture spends 123.1 ms
  on four progress aggregates. At a five-second refresh this alone is 2.46% of
  wall time, above the required P2 threshold.
- The 5,000-manifest / 470,000-byte fixture takes 179.7 ms for one sorted parse
  pass; startup currently performs several such passes. This also exceeds the
  selected P2 threshold when multiplied across recovery, seed, and dashboard
  readers.
- Read-only archive scale retained from Stage 1: Visakanv has about 891 MiB
  each in posts/authored views, 602 MiB of committed raw post sources, 406 MiB
  of context exports, and a 1.8 GiB SQLite database. Carmack's 19 old
  per-window commits implied about four GiB of repeated logical view I/O for
  100 posts.
- Stage target: a large small-delta fixture must reduce ordinary payload bytes
  read plus written by at least 90%; unchanged source, media, export, dashboard,
  and recovery paths must perform zero full-payload reads/writes.

## Updated Assumptions

- The existing expanded `_state/context.sqlite3` remains the one per-user
  indexed truth. A second local database would duplicate identity,
  generations, recovery, and transaction boundaries without reducing I/O.
- New raw sources can be hashed and parsed in one streaming pass into
  connection-local SQLite staging rows. Only the final verified merge becomes
  visible; a crash can discard staging and replay the source without exposing
  partial posts or edges.
- A committed source with matching device, inode, size, and nanosecond mtime
  needs no ordinary payload read. A stat change requires a full digest; a
  digest mismatch is archive mutation and fails closed. A separate explicit
  integrity audit always rehashes every source.
- Existing historical sources and media require one bounded local
  reconciliation. A durable migration/current-pointer marker makes that scan
  one-time; future runs register their own source, manifest, and media deltas
  directly.
- Multiple top-level files cannot be atomically replaced as one filesystem
  object. The coherent portable export is therefore a generation directory
  plus an atomically replaced current-export manifest. Top-level JSONL paths
  remain compatibility links/copies and are repairable, but only the current
  manifest and matching database batch may claim a coherent generation.
- Conversation closure must be materialized by affected target/edge updates;
  hiding the same group join behind a differently named dashboard function is
  not an optimization.

## Big-Picture Objective

Make ordinary local archive work proportional to new or actionable data:
stream each new raw source once into indexed truth, reuse verified committed
sources without reading them, maintain media/progress/recovery state
transactionally, and publish portable views only when their durable generation
changed.

## Detailed Implementation Plan

- Finalize schema v3 with connection-local source staging support, indexed
  portable media rows, maintained conversation rollups, the missing archive/
  reason/focal/closure counters, and any exact indexes required by export,
  source-cache, or registry queries. Keep migration additive and rollback-safe.
- Add a local-state module that validates a run-owned canonical source path,
  streams bytes once while hashing and parsing JSONL, stages bounded normalized
  rows, verifies numeric account and source evidence, then atomically merges
  posts, provenance, local parents, reply edges, source status, dirty views,
  counters, and one durable generation.
- Extend the indexed legacy transaction to insert the same local-parent/reply
  evidence from its already canonical raw records so a later seed pass never
  reparses a confirmed legacy window.
- Cache committed source stat identity and digest. Skip unchanged sources with
  zero hash/parse bytes; rehash a stat-changed source and fail closed on changed
  content. Add an explicit full-source integrity audit that ignores the cache.
- Reconcile historical manifests/sources exactly once, then register each new
  run manifest and canonical source directly. Use `run_registry` and
  `current_pointers` for current/running/recovery candidates so normal startup
  and dashboard refresh do not glob and parse all history.
- Import the existing media compatibility view once into indexed media truth,
  verify paths/stat/hash evidence at the migration boundary, and have each
  future asset completion upsert one portable media row and dirty the media
  view transactionally. Unchanged media performs no sidecar walk.
- Implement generation exports under a private temporary directory, fsync and
  hash each changed view, build a complete generation manifest that reuses
  unchanged view evidence, atomically publish the current-export pointer, then
  finalize `export_batches`, `export_views`, and `current_pointers` in one
  transaction. Recover every crash ordering without labeling a stale view
  current.
- Stream posts/authored/reposts/media/context posts/reply edges from indexed
  truth in deterministic order. Update top-level compatibility paths from the
  published generation without rereading history when hard-link replacement
  is available; leave a repair marker if compatibility publication fails.
- Switch context seed/export/status and progress readers to source registry,
  maintained counters, conversation rollups, and current pointers, with
  read-only fallback only for pre-v3 archives. Preserve an explicit full
  export/audit mode.
- Add small-delta byte accounting, source mutation/audit, media migration,
  generation publication, query-plan, 100,000-row dashboard, 5,000-manifest
  registry, lock, `Ctrl-C`, and fault-injection tests before Stage 9 cuts the
  unified command over.

## Safety Invariants

- No raw/media/manifest file read, hash, network request, or CDN transfer occurs
  inside a SQLite write transaction. Connection-local staged rows may be read
  by the final indexed transaction; no unverified row is visible beforehand.
- A source becomes committed only after path confinement, complete parse,
  digest/stat verification, stable numeric identity, and normalized-record
  validation. Crash residue cannot be mistaken for a committed source.
- Raw snapshots remain immutable provenance. A stat change triggers digest
  verification; changed bytes stop ordinary ingestion and remain explicit for
  audit/manual repair.
- Post, local-parent, reply-edge, media, source, provenance, counter, dirty-view,
  and durable-generation changes for one source/asset share one transaction.
- Existing captured observations, manual review, unavailable boundaries,
  files, paths, ordinals, hashes, and profile/context separation remain intact.
- An export is current only when all manifest-listed files verify and both the
  atomic pointer and SQLite published batch agree. A partial top-level
  compatibility refresh is never authoritative.
- Export or dashboard failure cannot roll back metadata, edges, media, cursor,
  or legacy frontier truth. Low disk and `Ctrl-C` leave dirty generations
  replayable.
- Hot counter/registry readers are read-only and cannot change queue ordering,
  closure, recovery decisions, or archive outcomes.

## No-Cheating Checks

- Instrument raw iteration, hashing, JSONL writes, fsyncs, manifest loads, and
  sidecar loads. An unchanged ordinary invocation must report zero historical
  source hashes/parses, zero media-sidecar scans, and zero full-view writes.
- A one-post delta over a large indexed archive may read the delta and write
  SQLite pages plus changed export bytes once; it may not load the prior posts
  JSONL to merge.
- K legacy windows retain K indexed commits and at most one export; no deferred
  automatic phase may secretly repeat K full materializations.
- Exact production queue/progress/source/registry/export query plans must use
  intended indexes and contain no target-table full scan, correlated parent
  count, or temporary global ordering tree on the hot path.
- A renderer that reads maintained counters is not allowed to refresh those
  counters by scanning targets first.
- A stat-cache hit must be proven by previously committed digest plus exact
  stat identity. Merely trusting a path or manifest flag does not count.
- An explicit integrity audit must still detect an injected same-size content
  mutation even when ordinary operation would otherwise use cached evidence.
- A published generation must reproduce indexed canonical records byte-for-
  byte; skipping an export cannot be reported as current when its durable
  generation changed.

## Completion Requirements

- Large synthetic small-delta accounting demonstrates at least 90% fewer
  ordinary payload bytes read/written than the Stage 1 baseline.
- New modern and legacy sources are parsed once, indexed atomically, and seed
  their local posts/reply edges without a historical pre-pass. An unchanged
  second run hashes/parses zero source payload bytes.
- Unchanged post/media/context export state writes zero full-view bytes and
  preserves the published generation. Changed state publishes one coherent,
  reproducible generation; export faults recover without X work.
- Existing media migrates once; subsequent unchanged runs do not walk
  sidecars. New direct assets update one indexed row and retain exact
  compatibility names, hashes, bytes, and scopes.
- Progress and read-only summaries use maintained counters; recovery/dashboard
  use indexed current pointers after one historical reconciliation. Large
  query/manifest fixtures remain proportional to current/actionable rows.
- Source stat mutation, content corruption, malformed JSON, identity conflict,
  SQLite busy/rollback, low disk, `Ctrl-C`, export placement, pointer, database
  finalization, and compatibility-link faults all fail or recover explicitly.
- Explicit full integrity audit and full export remain available and detect
  injected corruption.
- Focused state/local/context/progress/legacy/media tests, full repository
  discovery, pinned-runner fingerprints, compilation, `git diff --check`, and
  deliberate diff review pass without a live network or production mutation.

## Results Ledger

- Expanded the existing per-user `context.sqlite3` schema rather than adding a
  second database. Additive schema-v3 objects cover normalized archive posts,
  provenance, source stat/digest evidence, portable media, run/manifest
  registry, current pointers, maintained archive/context counters,
  conversation rollups, and coherent export generations.
- New raw JSONL is streamed and hashed once into connection-local staging,
  validated against the bound numeric account identity, and merged atomically.
  A committed exact-stat second visit reads and hashes zero source bytes. A
  stat change rehashes once; changed content fails closed. The explicit audit
  path ignores the cache and detects injected same-size mutations.
- The 5,000-post plus three one-post-delta fixture reduced ordinary historical
  payload I/O by at least 90% versus the former three full-view rebuilds. The
  three deltas leave one dirty generation and do not read the old portable
  views.
- A 5,000-manifest fixture pays one bounded historical reconciliation. The
  next ordinary reconciliation loads zero manifests, reads zero manifest
  bytes, and completes below 0.25 seconds.
- A 100,000-target dashboard fixture reads maintained counters only, performs
  no query against `targets`, `observations`, `reply_edges`, or
  `conversation_rollups`, and completes below 0.05 seconds.
- Existing portable media and raw-source history migrate once. Subsequent
  ordinary runs perform zero sidecar walk and zero historical source parse;
  direct asset completion updates its portable media row and rollups in the
  same transaction as job completion.
- Changed exports publish deterministic generation directories plus an atomic
  current-generation pointer. Unchanged exports write zero view payload.
  Fault tests cover placement, manifest, filesystem pointer, database
  finalization, compatibility publication, low disk, stale generations, and
  temporary-artifact cleanup without falsely advancing current truth.
- Legacy indexed commits now retain canonical posts, provenance, local
  parents, reply edges, source evidence, counters, and dirty generations in
  the same transaction; no later historical seed pass is required.
- Verification on 2026-08-01:
  - `uv run python -m unittest -q tests.test_archive_x_recovery
    tests.test_archive_x_unified tests.test_archive_x_context
    tests.test_archive_x_legacy tests.test_archive_x_media
    tests.test_archive_x_refresh tests.test_archive_x_progress
    tests.test_archive_x_state_v3 tests.test_archive_x_local`: 262 tests,
    all passed in 27.271 seconds.
  - `uv run python -m compileall -q scripts tests`: passed.
  - `git diff --check`: passed.
- No X request, CDN request, live archive run, or production archive mutation
  was used for Stage 8 verification.

## Stage Results

Stage complete. Ordinary source ingestion, media reconciliation, progress,
recovery indexing, and export bookkeeping now scale with new/actionable work
after a one-time migration. Indexed truth and immutable raw evidence are the
durable commit boundary; portable JSONL is a reproducible generation rather
than the hot-path database. Stage 9 must cut the unified command over to these
APIs, eliminate its remaining legacy JSONL/sidecar/glob paths, and choose the
automatic export checkpoint policy before any production run resumes.
