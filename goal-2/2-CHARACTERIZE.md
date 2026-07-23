# 2-CHARACTERIZE

## Current Facts

- The timeline stage builds `from:HANDLE max_id:ID` and delegates to
  `TwitterAPI.search_timeline`.
- Search defaults to `search-pagination = max_id`. Each page discards the
  server cursor and rewrites the query boundary using Snowflake arithmetic.
- `search-pagination = until` is also unsafe for legacy IDs because it obtains
  the date by Snowflake-decoding the returned numeric ID.
- Any other `search-pagination` value leaves `update_variables` unset. The
  fixed raw query is then paginated with X's opaque bottom cursor.
- The upstream paginator normally returns when there is no bottom cursor, but
  it also returns normally when the returned cursor equals the submitted
  cursor. Those outcomes are not distinguishable from process exit status
  alone.
- Stage 1 preserved the cursor and production evidence and proved the failure
  offline.

## Updated Assumptions

- A fixed `since`/`until` query with opaque server-cursor pagination is the
  only installed primitive that does not reinterpret a legacy ID. It still
  requires a fail-closed observation layer for repeated cursor, errors,
  request count, and terminal evidence.
- `max_id - 1` is technically meaningful for sequential IDs but is rejected
  as the primary design: it makes correctness depend on detecting the ID
  domain and does not provide an auditable calendar coverage frontier.
- Actual historical availability and `since`/`until` behavior still require a
  bounded live diagnostic.

## Big Picture Objective

Prove which installed pagination mechanism can enumerate a fixed legacy date
window without Snowflake assumptions, and establish the observable signals
needed to prevent false completion.

## Detailed Implementation Plan

- Fingerprint and test installed search query/pagination methods.
- Exercise native server-cursor, terminal empty, repeated cursor, API error,
  current max-ID transformation, and date transformation with offline data.
- Prepare at most two disposable, metadata-only searches for adjacent dates
  around October 28–29, 2010. Use one worker, a hard output limit, hard request
  limit, process timeout, conservative delay, private temporary output, and no
  production state/database path.
- Retain only non-secret query bounds, request/terminal signals, returned IDs,
  dates, numeric author IDs, and hashes needed for the decision.

## No-Cheating Checks

- Neither candidate query contains `max_id`; neither converts a returned ID to
  a date.
- The raw query must remain byte-for-byte fixed while only the opaque cursor
  changes.
- A repeated cursor must be classified ambiguous, never successful terminal.
- Diagnostic output must live outside `/mnt/Bibliotheque/gdl/x-archive` and
  must not use its download archive database.
- An elapsed timeout alone cannot be treated as terminal coverage.

## Completion Requirements

- Offline tests deterministically demonstrate the accepted and rejected
  primitives.
- Source fingerprints match gallery-dl 1.32.4 and are recorded.
- The bounded live diagnostic either returns older target-authored posts or
  records a precise upstream limitation; no zero-result response alone proves
  historical completion.
- A documented primitive decision is folded back into `0-plan.md` before
  Stage 3.

## Stage Results

- Approval purpose: permit narrowly bounded read-only observation of X while
  preventing production mutation or an accidental historical crawl. The goal
  authorization permits this diagnostic once its hard bounds are in place.
- Installed source SHA-256 fingerprints:
  - `TwitterAPI.search_timeline`:
    `a6a27d4168ae98bee3ed1608bd8c8acec674d07e5ff4acad9651b20af32a48c3`
  - `TwitterAPI._pagination_tweets`:
    `6857fde6c5b21099cb52d5503d58f938f19796137fe9e680d34114dc93b5f69c`
  - `TwitterAPI._update_variables_search_maxid`:
    `384994405713974e5f520ccf8106f733ceb937ac41562fa7dc54b5dbc5568320`
  - `TwitterAPI._update_variables_search_date`:
    `3ab68e7ea4e444e803aef4d72b5b7a12a61ca2b9f6c90dd8c12e34f13873df90`
  - `TwitterSearchExtractor`:
    `dbb0ddd1a4d7ad39421407a8865c64e085f7fcb5b7f703e786204df50a0a0dc1`
- Offline characterization now has 7 passing tests. It proves fixed-query
  opaque-cursor behavior, normal-looking repeated-cursor termination, API
  error failure, the exact Snowflake max-ID transformation, the incorrect
  legacy-ID date decode (`2010-11-04 01:43:01` instead of the returned
  `2010-10-29 19:30:34`), and safe saved-cursor selection.
- Diagnostic 1 queried `from:visakanv since:2010-10-29
  until:2010-10-30`. It returned 12 records for numeric account `16884623`,
  including the saved boundary and older ID `29116231217`, but four records
  were dated as late as `2010-10-30 06:56:57` UTC. Plain date operators
  therefore do not express UTC-midnight intervals and are rejected.
- Diagnostic 2 queried the exact epoch interval
  `[1288224000,1288310400)`, or `[2010-10-28T00:00:00Z,
  2010-10-29T00:00:00Z)`, using `since_time`/`until_time`. It returned 16
  records, all inside the interval and all bound to numeric account
  `16884623`. The newest was ID `29008210113` at `17:18:30` UTC and the
  oldest was ID `28977829098` at `10:59:27` UTC—decisive evidence that X
  exposes posts older than the stalled boundary.
- Each diagnostic used one profile request and five SearchTimeline requests,
  below a six-request total cap, a 50-post cap, and a 180-second process cap.
  Both had zero API errors and no repeated cursor. Each data page was followed
  by four empty successful pages carrying distinct cursors; gallery-dl then
  stopped via its `search-stop` heuristic. X did not return a cursor-less
  terminal page, so exit code 0 alone is explicitly insufficient evidence of
  interval completion.
- Disposable raw files were mode `0600`. Their SHA-256 values were
  `a7f5da3641762a58681dd51d9fcce816f790218baf11c979f507a2539c706d39`
  (plain dates) and
  `b71b56295f5f0182851d6417f751a91d2884c8d88b2d3950fa800778ef29f597`
  (epoch bounds). Production state and manifest hashes remained unchanged.
- Selected primitive: fixed `since_time`/`until_time` UTC epoch bounds with
  locally enforced half-open timestamps and opaque cursors scoped to one
  query. A successful window will require two independent bounded walks with
  identical accepted ID sets and independently valid termination evidence;
  one empty page, one heuristic exit, any mismatch, saturation/request-cap
  event, repeated cursor, or API error will not advance coverage. Busy windows
  may be deterministically subdivided rather than weakening request bounds.
- Rejected alternatives: upstream `max_id` and `until` both perform Snowflake
  math; a custom legacy `max_id - 1` requires brittle ID-domain switching and
  lacks an auditable time frontier; plain date operators are not UTC; opaque
  cursors without observation/reconfirmation have ambiguous termination.
