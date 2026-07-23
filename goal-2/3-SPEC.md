# 3-SPEC

## Current Facts

- Exact UTC epoch search operators returned 16 Visakanv posts from October 28,
  2010; plain date operators used a non-UTC boundary.
- Native cursor mode leaves the fixed query unchanged, but live searches ended
  through gallery-dl's four-empty-page `search-stop` heuristic while X kept
  supplying distinct cursors.
- One walk or one empty page is therefore not sufficient completion evidence.
- The existing archive uses atomic mode-`0600` JSON state, immutable run
  directories, stable numeric identity, global/archive locks, deterministic
  dataset deduplication, and pending-media retries.

## Updated Assumptions

- The source-visible legacy history can be enumerated with exact epoch bounds,
  but completeness must be operationally defined as repeat-confirmed identical
  bounded walks—not “every tweet ever posted.”
- One UTC day is the default durable frontier unit. A day that cannot finish
  under the request cap must split into deterministic half-open subintervals;
  it must never silently truncate or increase the cap.
- Empty intervals are allowed only after two independent walks produce the
  same empty accepted-ID set and each walk has valid multi-page terminal
  telemetry. A single empty response remains ambiguous.

## Big Picture Objective

Specify a versioned state machine and evidence protocol in which legacy UTC
coverage advances only after two bounded, identical, identity-checked
enumerations are durable and merged.

## Detailed Implementation Plan

### Operator Surface

- `scripts/archive-x-legacy --user HANDLE status` is a separate, network-free,
  write-free command. Keeping a dedicated executable makes it impossible for
  an ordinary timeline invocation to enter legacy mode accidentally.
- `scripts/archive-x-legacy --user HANDLE plan` is network-free and
  write-free. It derives initialization
  evidence and prints a non-secret SHA-256 confirmation token plus the exact
  guarded initialization command.
- `scripts/archive-x-legacy --user HANDLE init --token TOKEN` acquires both
  normal locks, recalculates the token from
  current state/run/dataset evidence, and atomically creates legacy state only
  when every guard matches. Repeating the same initialization is idempotent;
  differing evidence fails closed.
- `scripts/archive-x-legacy --user HANDLE run --windows N` is the only
  networked legacy command. `N` is required and positive; installation and
  ordinary timeline invocations do not initialize or run legacy work.

### Initialization and State

Absence of `legacy_backfill` means `not_initialized`. Version 1 is:

```json
{
  "schema_version": 1,
  "status": "pending",
  "requested_user_id": "16884623",
  "source": {
    "run_id": "20260720T023918Z-cf57e4",
    "manifest_sha256": "...",
    "state_sha256_before_init": "...",
    "cursor": "3_29116490825/",
    "oldest_post_id": "29116490825",
    "oldest_post_at": "2010-10-29T19:30:34Z"
  },
  "initialized_at": "...",
  "initial_until": "2010-10-30T00:00:00Z",
  "next_until": "2010-10-30T00:00:00Z",
  "floor_since": "2008-10-21T12:01:00Z",
  "active_window": null,
  "last_completed_window": null,
  "coverage_conclusion": "in_progress",
  "manual_review": null
}
```

The source object is immutable. Unknown versions, account mismatch, invalid
UTC timestamps, `floor_since > next_until`, `next_until > initial_until`, a
non-null malformed active window, or a frontier inconsistent with its last
completed window aborts before network or write.

`next_until = D` means every initialized interval from `D` through
`initial_until` has repeat-confirmed, durably merged source-visible coverage.
The first window is `[2010-10-29T00:00:00Z,
2010-10-30T00:00:00Z)`, deliberately replaying the known boundary day. Each
next day is `[max(floor_since, D-24h), D)`. The final clipped interval ends at
the exact stored account-creation timestamp, not midnight before it.

### Active Window and Subdivision

Claiming a window atomically changes `pending` to `active` and stores its
immutable `window_id`, UTC bounds, owner run ID, attempt, and a deterministic
leaf stack. The root leaf is the day. A leaf that hits the SearchTimeline
request cap or cannot produce valid termination telemetry is bisected at the
integer midpoint second. The left and right leaves exactly partition the
parent; neither zero-length leaves nor overlap are allowed. Saturation at a
one-second leaf becomes `manual_review`.

Completed leaf detail belongs in immutable run observation/manifests. State
keeps only the active stack/current leaf and O(1) day frontier. Restart never
resumes an opaque cursor: it replays the current leaf from its fixed query.

### Fixed Query and Local Acceptance

For logical leaf `[S,U)`, query the canonical handle with
`since_time:S-1 until_time:U+1` and locally accept only returned occurrence
timestamps satisfying `S <= timestamp < U`. The one-second query overlap
removes dependence on undocumented server inclusivity. Dates come only from
returned metadata; IDs are opaque decimal strings and are never decoded or
decremented.

The query builder accepts only a validated handle and integer UTC seconds; it
adds the existing repost terms only when the archive's repost policy requires
them. Quotes, conversations, descendants, and reply-context expansion remain
disabled. Every accepted original/reply must have author ID `16884623`; every
accepted repost must have wrapper/requested-user ID `16884623`. Any other
shape is an identity violation, not silently accepted data.

### Two-Walk Evidence Protocol

Each leaf is enumerated twice from no cursor, with independent raw JSONL and a
private telemetry file. A walk is valid only when:

1. the exact query hash and UTC bounds match the claimed leaf;
2. profile identity matches the state-bound numeric user ID;
3. every API response is successful and parseable, with no API error;
4. SearchTimeline requests remain within the configured hard cap;
5. cursor hashes never repeat and actual opaque cursors are not persisted in
   state/manifests;
6. every accepted record passes local timestamp/identity checks;
7. termination is either a cursor-less successful page or the pinned
   gallery-dl 1.32.4 terminal pattern: four consecutive successful empty pages
   with distinct forward cursor hashes after the last data page;
8. the walk was not interrupted and did not stop by timeout, post range,
   request cap, watchdog, malformed response, or process error.

The leaf is confirmed only when two valid walks have identical sorted accepted
tweet-ID sets. Their full normalized static records must also be compatible;
richer volatile metadata may merge by the existing deterministic rule. A
single empty walk, two walks with different ID sets, or a changed terminal
pattern never confirms a leaf. Bounded retries may seek two consecutive
matching walks; exhausting them sets `manual_review` without moving
`next_until`.

### Durability and Commit Order

For a confirmed day:

1. atomically finalize both raw walks and telemetry, then fsync their parent;
2. finalize a canonical deduplicated day raw file and provisional manifest;
3. merge accepted records into `posts.jsonl` and rebuild derived authored and
   repost views deterministically;
4. enqueue every metadata-discovered media-bearing post in the existing
   pending-media mechanism, including expected asset count;
5. atomically commit `next_until = window.since`, clear `active_window`, set
   `last_completed_window`, and return to `pending` (or `complete` at floor);
6. finalize the run manifest.

A crash before step 5 replays the same window; dataset ID deduplication makes
that harmless. A crash after step 5 is reconciled by matching the immutable
window/run ID and finalizing its manifest—never by replaying an earlier
frontier or advancing a new one. Coverage commits independently of media.
Post-level media retries use the existing individual-status path; failed or
unattempted expected assets remain pending without holding the UTC frontier.

### Outcomes

- `pending`: ready for the next bounded invocation.
- `active`: claimed window has not committed; recovery replays its current
  leaf.
- `manual_review`: bounded retries, one-second saturation, identity failure,
  incompatible raw data, or source ambiguity; frontier unchanged.
- `complete`: `next_until == floor_since` and every interval is confirmed and
  merged. `coverage_conclusion = source_visible_to_account_creation` means
  only that all queries in this protocol completed; it does not claim deleted,
  private, withheld, or unindexed posts.
- A durable upstream refusal may set
  `coverage_conclusion = source_unavailable_before` plus the exact frontier
  and evidence, but it remains `manual_review`, not `complete`.

## No-Cheating Checks

- Query and timestamp tests search for no `max_id`, Snowflake epoch, ID shift,
  or legacy decrement in the legacy path.
- Adjacency tests prove `next.since == prior.until` and exact root/child union.
- Negative tests cover one empty walk, mismatched repeats, repeated cursor,
  request cap, 429/API error, timeout, malformed response, wrong identity,
  outside-window records, and changed upstream fingerprints.
- Fault tests at every durability step prove duplicates at worst and no
  frontier advance before merge.
- Byte/structural comparisons prove modern `resume`, context state, recovery
  lists, identity fields, and existing pending media survive every transition.
- Ordinary commands and dry-runs cannot create legacy state or network calls.

## Completion Requirements

- Stage 4 implements and tests the exact schema, validation, confirmation
  token, initialization, claim, retry/manual-review, and commit transitions.
- Stages 5–7 implement fingerprinted telemetry, exact query/acceptance,
  bounded two-walk enumeration, subdivision, commit order, and recovery.
- Readouts always use “source-visible” coverage language and include the exact
  next window/command.
- The production state remains unchanged until Stage 10 initialization.

## Stage Results

- The specification was reviewed against all 15 non-negotiable constraints.
  It preserves the modern cursor, avoids ID arithmetic, uses contiguous UTC
  bounds, requires more than an empty response, retains hard caps/watchdogs,
  binds numeric identity, separates media, fails closed on upstream changes,
  requires explicit initialization, and never starts through a normal archive
  invocation.
- The live finding changed the original design from date strings to exact
  epoch bounds and from one cursor walk to repeat-confirmed bounded walks.
- No implementation or production state changed in this stage.
