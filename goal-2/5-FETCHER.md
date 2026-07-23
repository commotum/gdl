# 5-FETCHER

## Current Facts

- State and explicit initialization are implemented offline; network execution
  still fails closed.
- The selected query uses exact UTC epoch seconds and native opaque cursors.
- Pinned gallery-dl does not expose enough structured terminal evidence to the
  parent process, so a separate fingerprinted legacy runner must record it.

## Updated Assumptions

- Search response telemetry can be limited to counts, hashes, error flags, and
  termination classification; actual opaque cursors and headers need not be
  persisted.
- The existing metadata JSONL processor can capture search records without
  media downloads or the media archive database influencing enumeration.

## Big Picture Objective

Fetch and classify one logical UTC leaf twice-safe-ready: exact structured
query, hard SearchTimeline request cap, immutable metadata, hashed cursor
telemetry, local timestamp and numeric-identity validation, and no coverage
state authority.

## Detailed Implementation Plan

- Add a separate pinned legacy gallery-dl runner that wraps SearchTimeline
  calls, enforces a hard request cap, and atomically emits non-secret telemetry.
- Add exact query/URL and metadata-only config builders.
- Add one-walk execution/classification that cannot write the legacy frontier.
- Validate raw records by returned occurrence timestamp, numeric identity,
  relationship, and logical bounds.
- Test explicit-no-cursor, four-distinct-empty-tail, repeated cursor, API error,
  request cap, malformed telemetry, wrong identity, and overlap filtering.

## No-Cheating Checks

- The legacy query/config contains no `max_id` and selects no upstream date-ID
  paginator.
- The runner fails closed on any changed gallery-dl source fingerprint.
- Actual cursor values, cookies, headers, and signed URLs never enter telemetry.
- One valid walk has no state-advance function; orchestration must compare two
  independent walk results later.

## Completion Requirements

- Focused runner/fetch tests and full suite pass offline.
- Config and raw/telemetry permissions are private.
- Normal runner/timeline behavior is unchanged.
- Production hashes and process state remain unchanged.

## Stage Results

- Added `gallery_dl_x_legacy_runner.py`, pinned to the reviewed search,
  pagination, search-extractor, ordinary runner, and gallery-dl 1.32.4 source
  fingerprints.
- The runner caps SearchTimeline calls, records only query/cursor hashes,
  response counts, API-error flags, live numeric profile ID, and terminal
  classification, and redacts opaque quota-checkpoint cursors from logs.
- Added exact one-second-overlapped UTC query generation, metadata-only config
  with no download archive, local half-open filtering, numeric author/repost
  identity checks, and one-walk immutable raw/config/log/telemetry evidence.
- Valid termination is limited to an explicit missing bottom cursor or four
  successful empty pages with distinct cursors. Request cap, repeated cursor,
  API error, changed query, bad identity, process error, watchdog, malformed
  telemetry, and interruption are ambiguous.
- Focused legacy state/fetch/runner suite passed 28 tests; the full suite passed
  116 tests. Both pinned runners report 1.32.4, compilation and
  `git diff --check` pass, and production hashes remain unchanged.
- One walk cannot mutate state. It exposes records only to the two-walk
  orchestrator implemented in Stage 6.
