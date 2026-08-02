# 2-STATE

## Current Facts

- Stage 1 completed with 247 repository tests passing and no production archive
  mutation or live smoke.
- Existing per-user `context.sqlite3` already owns reply targets, observations,
  source ingestion, context pacing, and the largest durable queue. Production
  examples are roughly 130 MiB and 1.8 GiB.
- The database is schema v2, mode `0600`, single-writer, `DELETE` journal,
  `FULL` synchronous, and opened under the existing global/user locks.
- Creating a separate archive-state database would avoid renaming the context
  file but would require attached-database transactions for accepted context
  records plus descriptors, duplicate identity/pacing ownership, and more
  recovery states.
- Adding tables/columns/indexes in place is transactional in SQLite and does not
  require copying the existing observations or local-post payloads.
- Current target claims have no eligibility/lease indexes, use one shared lease
  timestamp for metadata and media, calculate parent demand through a correlated
  subquery, and sort a temporary global result on every fallback claim.
- Existing portable JSONL, raw snapshots, state JSON, context schema v2,
  download ledgers, sidecars/files, and manual-review outcomes must remain
  compatible and cannot become pending merely because new state tables exist.

## Baseline Ledger

- 100,000-target metadata claim: 108.6-110.2 ms, full target scan, correlated
  parent count, and temporary sort.
- 100,000-target media claim: 24.8-24.9 ms, full target scan and temporary sort.
- Progress aggregate refresh on the same fixture: 123.1 ms.
- Existing schema-v2 migration target may be multi-GiB; a v2-to-v3 migration
  must create only new schema/index pages and must not make a full database
  backup/copy.
- Queue index construction is a one-time migration cost. Ordinary claims after
  migration must use partial priority/lease indexes and no temporary ordering
  tree.

## Updated Assumptions and State Choice

- **Selected database:** expand `_state/context.sqlite3` in place from schema v2
  to v3 and treat it as the coherent per-user archive database while retaining
  the existing filename and `ContextDB` compatibility API.
- This choice avoids a 1.8-GiB copy and makes accepted context posts, edges,
  descriptor generations, asset jobs, counters, and export generations one
  transaction. The semantic filename is less important than one ownership
  boundary.
- Schema-v1 keeps its existing verified pre-v2 backup because that older
  destructive migration changes payload layout. The new v2-to-v3 migration is
  additive, transactional, and deliberately creates no full backup.
- Private descriptor URLs remain plaintext only inside the mode-`0600` SQLite
  file. There is no justified restart-safe local key service in scope; strict
  redaction, restrictive permissions, and narrow access are safer than an
  encryption design whose key would sit beside the database.
- Raw snapshots remain immutable provenance. Indexed posts become ordinary
  query/commit truth only after their source and per-record digests are bound;
  JSONL remains a reproducible materialized view.
- Parent demand is materialized on `targets` and maintained by reply-edge
  triggers. Partial priority indexes may scan the small actionable subset but
  must not scan/sort the full target table.
- Existing post-level `media_state` remains a compatibility rollup. Individual
  asset jobs become authoritative only after later stages reconcile/migrate a
  post; merely adding the tables changes no completion outcome.

## Big-Picture Objective

Install the minimum coherent, constrained, rollback-safe schema needed by the
remaining Goal 5 stages, migrate existing state lazily without network or file
redownload, and remove the measured queue query-plan defects without yet
changing descriptor capture or media transfer behavior.

## Detailed Implementation Plan

- Bump context schema to v3 and add a transactional v2-to-v3 migration whose
  schema-version update is the final statement.
- Add account identity, raw-source registry, normalized post/provenance,
  descriptor generation, asset job, descriptor-refresh, request aggregate,
  export generation/batch, progress counter, processed-run, and current-pointer
  tables.
- Add uniqueness, digest, owner/post/ordinal, state, generation, lease, and
  foreign-key constraints plus terminal descriptor/asset transition triggers.
- Add `parent_demand`, separate media lease timestamp, and metadata/media lease
  token columns to targets; initialize parent demand from existing edges and
  maintain it with insert/delete/update triggers.
- Add partial priority and indexed stale-lease paths. Rewrite claim/reclaim SQL
  to use materialized demand, separate media lease time, and indexes while
  preserving active-chain quantum, depth, shared-parent benefit, and post-ID
  tie breaks.
- Mirror existing bound identity into the account row during migration when it
  is available; update both compatibility metadata and account truth in future
  `bind_identity()` calls.
- Lazily register existing `seed_sources` in the broader source registry from
  already verified digests/stat evidence without rehashing or reparsing source
  payloads during migration.
- Initialize export/progress metadata without declaring old JSONL current at a
  generation it cannot prove. Existing complete files and post media states
  remain untouched.
- Add fresh-schema, v1-to-v3, v2-to-v3, idempotent reopen, rollback injection,
  permissions, constraints, compatibility, and query-plan tests. Use a large
  constructed v2 fixture to prove no backup/copy and bounded additive growth.

## Safety Invariants

- Migration performs no network request, media lookup, descriptor refresh,
  export, source payload hash, source payload parse, or archive file move.
- Schema version changes to 3 only in the same transaction as every required
  table, column, index, trigger, and compatibility row.
- A killed/failed migration leaves a valid v2 database that the next open can
  migrate; a committed migration reopens idempotently as v3.
- No SQLite write transaction spans network or file transfer.
- Stable numeric identity remains guarded by state JSON and mirrored database
  truth; mismatch fails before changing either binding.
- Existing captured observations, target/media states, retries, manual review,
  local posts, seed digests, pacing times, and files are preserved exactly.
- New descriptors/assets cannot cross owner/post/ordinal boundaries, reuse an
  invalid generation, claim without a lease token, or report captured without
  verified path/hash evidence.
- Private URLs never appear in errors, logs, progress, fixtures, or stage docs.
- Queue changes preserve chain-first behavior, fairness quantum, depth,
  shared-parent priority, retry eligibility, and stale-lease recovery.

## No-Cheating Checks

- Query-plan tests inspect the exact production metadata/media claim and stale
  lease SQL; merely adding unused indexes does not count.
- Migration byte accounting excludes the existing database payload and rejects
  a backup/copy of the v2 file.
- Registering existing source evidence during migration reads table/stat data,
  not raw source payload bytes.
- Existing files are reconciled as existing/unknown, never marked newly
  downloaded or rehashed in this stage.
- Export generation zero/unknown cannot be displayed as current durable truth.
- New counters are initialized from one bounded migration aggregation and later
  maintained transactionally; a dashboard scan is not relabeled as a counter.

## Completion Requirements

- Fresh, v1, and v2 databases reach schema v3 idempotently; fault injection
  proves rollback and retry.
- The large v2 fixture migrates without a full backup/copy and preserves every
  preexisting row/state/digest/pacing value.
- Constraints reject cross-post assets, duplicate source/provenance identity,
  invalid generations/states/leases, and stale export publication.
- Existing captured observations/files and manual review stay terminal without
  network work.
- Exact metadata/media claim and stale-lease query plans use intended indexes,
  contain no correlated parent count, and contain no temporary ordering tree.
- Focused/full tests, fingerprints, `git diff --check`, and deliberate diff
  review pass before Stage 3.

## Results Ledger

- Expanded `_state/context.sqlite3` transactionally from schema v2 to v3. The
  existing path, inode, context API, observations, graph, retry/manual-review
  states, source ledger, pacing evidence, and portable files remain in place.
- Added coherent account, source, normalized-post/provenance, descriptor,
  per-asset, descriptor-refresh, request aggregate, archive/export generation,
  maintained counter, run registry, and current-pointer state.
- Added separate metadata/media lease timestamps and random ownership tokens.
  Migration moves the old shared media lease timestamp into its correct lane,
  preserves active work, and gives old active leases migration ownership
  tokens. Triggers reject leased states without both token and timestamp and
  reject non-leased states retaining either.
- Added fail-closed v3 catalog validation. A v3 version marker without every
  required table, column, hot index, critical trigger, and migration record is
  rejected rather than silently repaired or used.
- Added materialized `parent_demand`, maintained target/edge/observation
  counters, partial priority indexes, and separate indexed lease-expiry paths.
  The exact production claim SQL has no correlated subquery or temporary
  ordering tree.
- A constructed 100,000-target v2 database migrated in about 1.25 seconds in
  the local deterministic benchmark. It grew from 14,663,680 to 17,838,080
  bytes (1.216x), retained the same inode, created no database copy/backup, and
  neither hashed nor parsed archive payloads.
- On that 100,000-target fixture, warm mean metadata selection fell from
  143.021 ms to 0.015 ms (99.99%), media selection from 17.653 ms to 0.010 ms
  (99.94%), and maintained-counter reads replaced 124.970 ms grouped scans
  with 0.023 ms reads (99.98%). These are deterministic local measurements,
  not production throughput claims.
- A 20,000-row compatibility fixture proves row/state/source/pacing retention,
  mode `0600`, no full copy, idempotent reopen, and exact rollback/retry after
  injected failure immediately before the schema-version commit.
- Constraint tests reject cross-owner assets, active jobs bound to stale
  descriptors, duplicate sources/generations, invalid target/asset leases,
  captured work without durable evidence, descriptor reactivation, and stale
  or incomplete export publication.
- Existing v1 backup behavior remains intact; v1 now proceeds through v2 into
  v3. A normal write-capable open tightens the database to mode `0600`, while
  the existing dry-run/read-only summary path still performs no migration.
- Both pinned gallery-dl runners reported the expected `1.32.4` compatibility
  fingerprint. Full repository discovery passed 255 tests; `compileall` and
  `git diff --check` passed. No live request or production archive mutation was
  used.
- Before schema v3 was deployed, Stages 3 and 4 finalized its descriptor
  sidecar fields and per-asset transfer priority. The required-column catalog,
  migration, constraints, and exact `ASSET_CLAIM_SQL` plan were extended in
  place; the plan scans the partial `asset_jobs_ready` index and creates no
  temporary ordering tree. The final pre-deployment schema passed the Stage 4
  288-test full suite as well as the original migration fixtures.
- Stage 5 finalized the same still-undeployed v3 contract with per-asset
  `destination_scope`, durable account authentication stops, and an exact
  refresh-ready index ordered by owner, eligibility time, and refresh ID. The
  production refresh claim uses that index without a temporary ordering tree;
  refresh completion remains lease-token guarded. The resulting schema passed
  the 309-test full suite and all original migration/rollback fixtures.
- Stage 6 activated the v3 pacing lease contract at the actual HTTP boundary:
  `not_before_reason`, `request_sequence`, `reservation_recoveries`, and the
  allowlisted last-operation/category fields now have token-consistent insert/
  update triggers and fail-closed catalog validation. Restart, stale-lease,
  reset, 429, auth-stop, SQLite-fault, and two-connection serialization fixtures
  passed without network use; the resulting schema and repository passed the
  344-test full suite.
- Stage 7 finalized the still-undeployed v3 catalog with indexed
  `legacy_intervals` keyed by exact UTC bounds and canonical source/evidence
  hashes. Confirmed legacy windows now upsert normalized posts/provenance and
  advance durable generation transactionally while leaving portable views
  explicitly dirty until one coalesced export. Idempotent replay, injected
  rollback, indexed query-plan, adaptive equivalence, and export-repair
  fixtures passed; the repository passed the 359-test full suite.

## Stage Results

Stage complete. Stage 3 may persist accepted descriptor generations and asset
jobs against this schema without changing the migration contract.
