# 1-SIGNALS

## Current Facts

- The repository is clean on the new `goal-4-x-dashboard` branch.
- Production is still running from the shared checkout, so this stage will not
  execute or restart the archive.
- Invocation manifests contain durable modern/media totals and phase results,
  but they are not refreshed during the long context worker.
- Context SQLite contains authoritative queue, observation, boundary, media,
  and closure state. Read-only aggregate queries are suitable for bounded
  producer sampling, not screen-refresh polling.
- Existing terminal prose is intentionally not a metric source.

## Updated Assumptions

- A producer snapshot refreshed on material events can decouple the renderer
  from SQLite.
- Full context closure can be sampled once per minute; cheap queue counters can
  be sampled more often if benchmarks support it.
- Seven lines remain the default per-user layout; multiple users require a
  compact repeated block.

## Big Picture Objective

- Define and test the authoritative progress schema, metric derivations,
  health precedence, redaction rules, and phase-local estimate contract.

## Detailed Implementation Plan

- Add `scripts/archive_x_progress.py` with a strict snapshot schema, validation,
  authoritative read-only metric collection, humanized units, health
  derivation, and pure ETA functions.
- Add `tests/test_archive_x_progress.py` with representative fixtures for
  totals, unavailable boundaries, blocked phases, dynamic queues, redaction,
  and unknown fields.
- Benchmark the production-sized read-only context aggregates and record the
  cadence decision here.

## No-Cheating Checks

- Do not parse terminal output.
- Open SQLite with `mode=ro` and `PRAGMA query_only=ON`.
- Do not write snapshots or integrate engines in this stage.
- Reject secret-bearing and unknown schema fields.

## Completion Requirements

- Every target readout field has an exact source/derivation.
- Health precedence and ETA omission rules are covered by deterministic tests.
- Focused tests and `git diff --check` pass.
- Benchmark evidence justifies the producer sampling cadence.

## Stage Results

- Added a versioned, strict `gdl-x-progress` schema and validator. Unknown
  fields and secret-shaped field names fail closed.
- Added read-only SQLite derivations for target states, media work,
  network-captured parents, unavailable reasons, and conversation closure.
  Captured, unavailable, retryable, pending, and manual-review states remain
  distinct.
- Added manifest-derived archive totals, health precedence, wall-clock net
  queue-burn estimates, confidence labels, and false-precision suppression.
- Six deterministic focused tests pass, including a byte-for-byte proof that
  metric collection does not mutate its SQLite fixture.
- A live read-only benchmark against Visakanv's 200k+-target database took
  1.408 seconds total. The state aggregate took about 0.182 seconds and the
  81,185-row closure aggregate about 0.734 seconds. Therefore the producer
  defaults full authoritative refreshes to once per five minutes; the
  renderer may refresh its already-written JSON every second at negligible
  archive cost. Semantic events still update phase/activity immediately.
