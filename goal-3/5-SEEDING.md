# 5-SEEDING

## Current Facts

- Current default discovery globs only modern `timeline.posts*` raws and has no
  source ledger. It cannot discover committed legacy canonical raws.
- Context graph operations are idempotent, but rescanning every raw source on
  every ordinary invocation would be increasingly expensive.
- Local parent capture currently searches all selected raws in a second pass;
  an incremental-only seed could therefore miss a parent stored in a source
  processed during an earlier invocation.
- Context schema version 1 has no source/local-post tables. Visakanv has no
  context DB, but other archives may and must migrate safely.

## Updated Assumptions

- Manifest-authoritative canonical source discovery is safer than filename
  glob expansion: modern dataset-merged raws and state-committed legacy
  canonical raws are eligible; walk/tmp/uncommitted/context raws are not.
- A `local_posts` SQLite index is needed alongside the source ledger so a new
  child can resolve to an old locally archived parent without refetching X.
- Existing SQLite migration requires an exact private byte backup before any
  schema write.

## Big Picture Objective

Make context seeding complete across both ID eras, incremental, idempotent,
crash-safe, and independently auditable.

## Detailed Implementation Plan

- Upgrade context schema transactionally with `seed_sources` and `local_posts`.
- Create/verify an exact private pre-migration backup for schema-v1 databases.
- Discover canonical raws through finalized modern/legacy manifest evidence.
- Validate archive containment, hashes, source type, run linkage, and commit
  markers before accepting a source.
- Process each new source and its ledger row in one transaction.
- Store target-authored raw metadata in `local_posts`, add reply edges, then
  capture any pending parent available in the local index.
- Refuse a previously processed relative path whose bytes changed.
- Keep dry-run network/write-free and explicit raw selection authority-checked.
- Add modern+legacy, migration, idempotency, mutation, exclusion, and rollback
  tests.

## No-Cheating Checks

- Never seed legacy walk evidence or uncommitted canonical output.
- Never infer reply edges from reposts, other-author timeline entries, quotes,
  or context fetch raws.
- Do not mark a source processed before all its graph/local-post work commits.
- Do not use the derived JSONL dataset as the sole graph authority.

## Completion Requirements

- Modern and committed legacy replies seed one complete graph.
- Repeated runs skip unchanged sources and still find old local parents for new
  edges.
- Source mutation, unsafe paths, walk raws, and failed transactions cannot hide
  work.
- Schema migration preserves data, produces an exact mode-0600 backup, and is
  idempotent.
- Context tests, compilation, and diff checks pass.

## Stage Results

- Completed on 2026-07-22.
- Context schema v2 adds `seed_sources` and `local_posts`. A v1 database is
  byte-copied to a hash-named private mode-0600 backup before a transactional
  migration; reopen is idempotent and graph data is preserved.
- Canonical discovery now follows completed modern `post_dataset` evidence and
  state-committed legacy canonical window paths. It rejects walk raws,
  temporary/uncommitted output, changed source bytes, arbitrary paths, and
  conflicting provenance.
- Each new source transaction atomically updates the target-authored local raw
  index, reply edges, and its ledger row. A failed edge write rolls back all
  three and a later invocation recovers normally.
- The persistent local index lets a newly discovered legacy/modern reply
  capture a parent from a source processed in an earlier invocation without an
  X request.
- All 29 context tests pass. New tests cover exact v1 backup/migration,
  modern+legacy graph union, walk exclusion, source mutation, transaction
  rollback, prior-source local parent capture, no-budget closure, and optional
  standalone limits.
- Compilation and `git diff --check` pass.
- A production read-only inventory found three authoritative Visakanv sources
  (two modern, one legacy), 258,096 raw observations, 205,172 unique child
  edges, 204,183 unique parents, and 77,044 locally available parent
  candidates. It created no DB; production state/dataset hashes remain
  unchanged.
