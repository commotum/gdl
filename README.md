# gdl

Personal operations repo for `gallery-dl`.

## Layout

- This repo contains scripts, dependency lockfiles, and gallery-dl config.
- Downloads are written to `/mnt/Bibliotheque/gdl/gallery-dl`.
- Cookies, archive databases, URL lists, and notes live in ignored `state/`
  subdirectories.

## Usage

Run downloads through the wrapper so the local config is always used:

```bash
scripts/gdl "URL"
```

Examples:

```bash
scripts/gdl "https://x.com/USER/media"
scripts/gdl "https://www.behance.net/anadiasphotography"
scripts/gdl --input-file state/lists/urls.txt
```

## Conservative X archive

Use the dedicated archiver when the goal is a durable, training-ready record
of an X account rather than a one-off media download:

```bash
uv run scripts/archive-x --user USERNAME
uv run scripts/archive-x --input-file x.txt
```

That one command updates the modern timeline, automatically resumes any
strictly proven pre-Snowflake legacy history, then seeds and drains the
ancestor-only reply-context queue (including recoverable context media). No
legacy window count, context post count, seed flag, or follow-up command is
required for normal operation.

The first run after this storage upgrade performs one identity-guarded local
migration: it indexes prior manifests and raw sources, reconciles existing
media/sidecars, and publishes the initial portable generation. This can take
noticeably longer on a large archive but makes no extra X requests and does not
redownload verified files. Once its reconciliation pointers are committed,
unchanged runs use the SQLite registry and exact stat evidence instead of
re-globbing, reparsing, or rehashing the full archive.

The input file accepts one bare handle, `@handle`, or `x.com`/`twitter.com`
profile URL per line. Blank lines and lines beginning with `#` are ignored,
and duplicate handles are removed. The file is parsed by the wrapper rather
than passed to gallery-dl, so entries cannot act as gallery-dl command-line
directives. Batch order is end-to-end: each account completes its modern,
legacy, media, reply-context, and export phases before the inter-user delay
and the next line begins.

The default is deliberately restrained and fail-closed:

- one archive process at a time, protected by an exclusive lock;
- every actual X API/bootstrap attempt—not merely every script phase—passes
  through one durable per-account lane with a 4–8 second floor, at most one X
  request in flight, and no accumulated burst credit after an idle period;
- the lane persists spacing, quota-reset, HTTP 429, and authentication-stop
  state across phases, worker replacement, Ctrl-C, and restart; direct media
  CDN transfers do not consume or weaken that X lane;
- repeated context and legacy items reuse an account-scoped worker/session for
  at most 100 items or 15 minutes, then retire cleanly; workers remain
  sequential and never rotate cookies, identity, headers, or proxies;
- X rate-limit reset headers are respected, account-lock errors abort, and
  retries are bounded;
- successful responses received at the end of an X quota window are processed
  before waiting for the reset, using a version-checked gallery-dl 1.32.4
  compatibility runner that fails closed after an unreviewed upgrade;
- three consecutive rate-limit windows without any new raw metadata trigger a
  clean, resumable checkpoint instead of an unbounded old-search loop; change
  the threshold with `--stalled-rate-limit-cycles`;
- no proxy rotation, header spoofing, concurrency, or local-disk fallback;
- a profile-info probe binds each handle archive to its stable numeric X user
  ID before timeline downloads, so a recycled handle fails closed;
- initial runs backfill as far as X exposes; later runs use a best-effort
  48-hour overlap, with pinned-item injection disabled so an old pin cannot
  silently terminate the incremental scan;
- interrupted timeline cursors are recorded for a later resume when provided
  by gallery-dl, together with the original date cutoff; a legacy terminal
  rate-limit loop that omitted its cursor is recovered conservatively from the
  oldest saved post rather than restarting the full historical crawl;
- reposts are included by default, retain the original author, and are marked
  `relationship: "repost"`; use `--no-reposts` to exclude them;
- embedded non-focal conversation modules are excluded from the account's own
  timeline dataset using numeric author IDs; the later ancestor phase fetches
  only the immediate replied-to post and its ancestors, retaining their true
  authorship;
- siblings, descendants, quoted sources, and “show more replies” expansion are
  never crawled, and separately yielded quoted-source media is excluded.

X's transformed reply-timeline data does not always retain the account ID of
the repost wrapper. Repost attribution is therefore best effort: an unusual
repost-shaped item embedded as conversation context can be retained as a
repost. The raw metadata is kept so this can be reclassified later. Use
`--no-reposts` if strict target-authorship filtering is more important than
retaining reposts.

Run a network-free validation first:

```bash
uv run scripts/archive-x --user USERNAME --dry-run
```

Run a deliberately incomplete live smoke test with a small post limit:

```bash
uv run scripts/archive-x --user USERNAME --post-limit 5
```

Limited runs save what they observe but are never marked as a completed
backfill. Other useful controls include `--since 2026-01-01`,
`--full-rescan`, `--keep-going`, and `--output-root PATH`. By default output
goes to a writable Bibliotheque mount under `gdl/x-archive`; the command exits
instead of silently filling the local disk.

### Recovering incomplete media

Modern, legacy, context, and profile extraction save private media descriptors
from responses the archiver already needed. A shared worker then downloads the
selected asset directly from the allowlisted X media CDN; it does not re-query
the post just to rediscover the same URL. Existing files are accepted only with
confined path, sidecar, size/stat, and digest evidence. Interrupted transfers
retain private partial state and use an HTTP Range request when the server and
validator permit a safe resume.

Transient CDN failures receive durable eligibility/backoff and a bounded
attempt budget. A rejected or expired descriptor (`403`, `404`, or `410`)
enters one bounded exact-post descriptor-refresh generation; all missing
ordinals for that post share the result instead of spending one X lookup per
asset. Deleted, protected, suspended, withheld, or confirmed media-absent
sources become explicit unavailable outcomes. Repeated transient or malformed
results stop at `manual_review` instead of looping forever. An otherwise
complete archive reports `complete_with_unavailable_media` without pretending
that missing bytes were recovered.

To retry only recorded incomplete media without crawling the timeline, run:

```bash
uv run scripts/archive-x --user USERNAME --retry-failed-only
```

The direct media worker uses at most 2 automatic attempts per asset and a
300-second read-inactivity timeout by default; adjust these with
`--media-retries` and `--media-timeout`. `--rate-limit` controls CDN bandwidth.
The X scheduler remains authoritative only for the exceptional descriptor
refresh, so CDN work cannot compress X request spacing.

If actionable media remains, the recovery result stays resumable. After a
recovery-only run, use the normal command when a current timeline and context
update is also wanted:

```bash
uv run scripts/archive-x --user USERNAME
```

Incremental stopping relies on timeline order supplied by X. A 48-hour
overlap and disabled pin injection address the common failure mode, but X can
still return non-monotonic thread modules. Periodic `--full-rescan` runs are
the maximum-completeness option; gallery-dl and X themselves can still impose
historical visibility limits.

### Pre-Snowflake history

Twitter changed from sequential post IDs to Snowflake IDs in November 2010.
The modern timeline crawler stops cleanly if gallery-dl's Snowflake arithmetic
reaches that boundary. The unified command initializes legacy work only when
the stopped manifest, raw metadata, saved cursor, oldest merged row, stable
numeric identity, pre-Snowflake timestamp, and watchdog failure class all
agree. It first creates and verifies an exact private state backup. Ambiguous
or generic failures never trigger the handoff.

Once initialized, the same normal command resumes bounded internal UTC windows
until the source-visible account-creation floor or an explicit manual-review
stop. Operators do not calculate or supply a window count. The policy begins
at three UTC days, adapts future roots between one and ninety days from proven
page density, and splits recursively when dense; an interrupted active window
always retains its original exact bounds.

Each UTC interval is queried with exact epoch-second bounds, never by decoding
or decrementing a legacy ID. Coverage advances only after two independent,
bounded cursor walks return the same numeric-identity-checked ID set and each
ends with two distinct empty cursor pages (or no cursor). Their raw observations
must be durable and the dataset merge complete. A saturated query splits into
smaller contiguous intervals. An ambiguous tail, repeated cursor, API error,
timeout, request cap, mismatched repeat, or interruption cannot advance the
frontier.

The status phrase `source_visible_to_account_creation` means every contiguous
window in this protocol was repeat-confirmed against X. It does **not** prove
recovery of deleted, private, withheld, or search-index-omitted posts. Ambiguous
windows enter `manual_review`; after inspection, replay only the exact guarded
window shown by `status`:

```bash
scripts/archive-x-legacy --user USERNAME retry \
  --window-id LEGACY_WINDOW_ID --reason 'operator review reason'
```

Legacy metadata completion is independent of media completion. Confirmed walks
commit their returned descriptors with the canonical window, after which the
shared direct-CDN worker handles the bytes. Only missing or rejected descriptor
evidence enters the bounded exact-post refresh path.

The standalone legacy CLI is an advanced maintenance interface. Its
network-free `status`/`plan`, exact guarded `retry`, and optional bounded `run`
are useful for inspection and rollout; they are not part of routine setup:

```bash
scripts/archive-x-legacy --user USERNAME status
scripts/archive-x-legacy --user USERNAME plan
scripts/archive-x-legacy --user USERNAME run --windows 1  # bounded maintenance
```

### Reply-context ancestors

After modern/legacy metadata commits, the unified command inventories every
authoritative raw timeline source in a private SQLite ledger. Every authored
reply seeds its immediate parent, and the resolver follows that parent and its
parent until a root, an explicit unavailable boundary, the depth guard, or a
manual-review item. Parents authored by other accounts are retained with their
true authorship. Siblings, descendants, quoted sources, and broad conversation
pagination remain out of scope.

For metadata, the worker reads at most the first bounded TweetDetail response.
It retains the focal post, other targets that were already independently queued,
and only the parent paths directly verified by that response. Unrelated
siblings and descendants are discarded. If a successful response omits the
focal post, the worker performs one paced exact lookup instead of treating
absence as unavailability. Media requests remain focal-only.

The worker prefers finishing the current ancestor chain, periodically yields
between chains/users, and has bounded attempts, leases, timeouts, and backoff.
Every hidden bootstrap, TweetResult, fallback TweetDetail, redirect, and retry
uses the same durable actual-request lane as modern and legacy work. One
account-scoped gallery-dl process/session is reused within its bounded lifetime.
No `--max-posts` value is required for normal closure.

Stopping with Ctrl-C or SIGTERM leaves the current target retryable. Deleted,
private, suspended, and withheld boundaries are recorded; ambiguous failures
are retried with bounded backoff and eventually require manual review. Use
`retry POST_ID...` for an explicit reclassification retry. Rebuild the
portable views with `export`.

An exact lookup whose successful X response omits its Tweet result gets exactly
one bounded confirmation through the more stable TweetDetail endpoint. A focal
post recovered there is archived normally; a second response without the focal
post becomes an explicit deleted boundary. Other response-shape failures stay
ambiguous and retain the normal bounded-retry behavior.

Metadata closure is independent of media, but it is no longer a second X pass.
The first context response commits metadata and reusable descriptors together;
the direct-CDN worker drains those descriptors automatically, verifies SHA-256
sidecars, and refuses to start below 5 GiB free. Failures remain explicit and
retryable without unresolving captured metadata. Metadata-only requests never
write to a download ledger; completed bytes use
`_state/context-downloads.sqlite3`, so observing metadata cannot masquerade as
a completed file. Descriptors committed before a later metadata failure can
still be drained safely.

The standalone context CLI remains available for advanced read-only status,
integrity, export, guarded retry, and deliberately bounded maintenance:

```bash
scripts/archive-x-context --user USERNAME status
scripts/archive-x-context --user USERNAME integrity
scripts/archive-x-context --user USERNAME export
scripts/archive-x-context --user USERNAME run --max-posts 1  # bounded maintenance
scripts/archive-x-context --user USERNAME media --max-posts 1
scripts/archive-x-context --user USERNAME repair-media-skips  # read-only preview
```

For a deliberately bounded unified production smoke, use the advanced
`--modern-max-posts`, `--legacy-max-windows`, `--context-max-posts`, and
`--context-media-max-posts` controls. A bounded result remains resumable and is
never reported as full completion.

### Calm progress dashboard

The ordinary command now maintains a private atomic progress snapshot with
lifetime totals, this-run gains, active phase, health, known remaining context
work, rolling throughput, and a confidence-labeled phase-local estimate:

```bash
uv run scripts/archive-x --user USERNAME
```

When launched interactively inside tmux at 72x20 or larger, it automatically
opens an eight-line pane at the bottom titled `archive-x-dashboard:RUN_ID`.
The original pane keeps the complete event stream. The bottom pane refreshes
from live maintained SQLite counters every few seconds, owns no archive state,
and exits when the invocation reaches a final status; failure of that pane
cannot stop the worker. Set `ARCHIVE_X_DASHBOARD=off` to suppress automatic
pane creation.

Outside tmux the worker emits a compact heartbeat at the slower telemetry
cadence. A snapshot can also be inspected once, or watched, without contacting
X or opening the SQLite database:

```bash
scripts/archive-x-dashboard --archive-root /mnt/Bibliotheque/gdl/x-archive
scripts/archive-x-dashboard --archive-root /mnt/Bibliotheque/gdl/x-archive --watch
```

`known remaining` is the currently discovered actionable context queue, not a
promise that discovery is finished. ETA is deliberately omitted while the
queue grows, before enough completed-item or legacy-window evidence exists, or
when work is blocked. The media phase has its own remaining count and ETA; it
does not reuse a metadata estimate that has already reached zero. Deleted,
protected, and suspended parents count as neutral unavailable boundaries;
manual review and authentication/integrity failures remain actionable.

### X archive contents

Each account is self-contained under `users/HANDLE/`:

```text
users/HANDLE/
├── _state/                  # timeline state plus coherent context.sqlite3
├── media/YYYY/MM/           # original images/videos plus JSON sidecars
├── media/profile/           # avatar and header history
├── runs/RUN_ID/             # immutable raw JSONL, configs, logs, manifest
└── dataset/
    ├── posts.jsonl          # authored posts, replies, and labeled reposts
    ├── authored-posts.jsonl # only content authored by HANDLE
    ├── reposts.jsonl        # repost-only view with original author retained
    ├── media.jsonl          # portable local asset index and SHA-256 values
    ├── context-posts.jsonl  # captured ancestor metadata
    ├── reply-edges.jsonl    # child-to-parent graph and boundary states
    ├── context-status.json  # queue, closure, pacing, and media readout
    └── profile.json         # latest observed profile metadata
```

The post records retain text; stable author/requested-user IDs; reply,
conversation, and repost IDs; language, hashtags, mentions, sensitive-content
flags, and article HTML; plus point-in-time likes, views, reposts, quotes,
replies, and bookmark counts. `posted_at` is the target account's timeline
event time. On repost rows, `reposted_at` records that action while
`original_posted_at` records the original author's post time. A user's own
quote post is retained, but with quoted-source extraction disabled X/gallery-dl
does not reliably provide a structured ID for the quoted target. Records also
store
`first_captured_at` and `last_captured_at`, because engagement counts describe
the crawl time rather than a permanent historical total. Raw run snapshots
remain immutable provenance. Indexed SQLite truth advances transactionally as
small deltas; `dataset/*.jsonl` are reproducible portable views published as
atomic generations. The initial generation, a forced checkpoint, 1,000
generations of accumulated change, or 24 hours of dirty age triggers
publication. Until then the dashboard and final summary explicitly show
durable versus published generations rather than rewriting large JSONL files
after every small phase.

New media assets receive SHA-256 hashes before their sidecar metadata is
written. Cookie values are never placed in manifests or logs, and the process
uses a private umask. Archive only material you are entitled to retain and use.

The config points Instagram and Behance at local ignored files under `state/`.
Twitter/X uses an ignored Netscape cookie file at
`state/cookies/x.cookies.txt`. The recommended way to create it is with the
repo's dedicated Firefox profile, which works on both macOS and Linux.
The archiver requires usable `auth_token` and `ct0` cookies on `.x.com` and
rejects a `.twitter.com`-only export or an expired cookie.

Open the dedicated X login profile (on the MacBook or Ubuntu desktop):

```bash
scripts/open-x-firefox-login
```

Log into X in that Firefox window, close Firefox, then export and verify the
needed auth cookies:

```bash
scripts/check-x-firefox-cookies
scripts/save-x-cookies
```

Chrome can be tried as an alternative, though its `auth_token` may be encrypted
behind the desktop keyring:

```bash
scripts/save-x-cookies --browser chrome
```

`state/` is intentionally ignored by Git. To create the cookies on the MacBook
and use them on Ubuntu, copy `state/cookies/x.cookies.txt` to the same path on
the Ubuntu checkout using a private transfer such as `scp`, then set its mode:

```bash
chmod 600 state/cookies/x.cookies.txt
```

The wrapper prefers Bibliotheque at `/mnt/Bibliotheque` and falls back to the
current manual mount at `/tmp/Bibliotheque`. It exits instead of accidentally
writing downloads to the local filesystem when the disk is missing.

## Automount

Configure the stable `/mnt/Bibliotheque` systemd automount from a terminal:

```bash
scripts/setup-bibliotheque-automount
```

The script uses sudo to add an `/etc/fstab` entry for the Bibliotheque UUID,
creates `/mnt/Bibliotheque`, reloads systemd, and starts the automount unit.

## Verify

```bash
scripts/gdl --version
uv --cache-dir /tmp/uv-cache run gallery-dl --version
```
