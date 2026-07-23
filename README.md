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

The input file accepts one bare handle, `@handle`, or `x.com`/`twitter.com`
profile URL per line. Blank lines and lines beginning with `#` are ignored,
and duplicate handles are removed. The file is parsed by the wrapper rather
than passed to gallery-dl, so entries cannot act as gallery-dl command-line
directives.

The default is deliberately slow and fail-closed:

- one archive process at a time, protected by an exclusive lock;
- 4–8 seconds between X extraction requests and 1–3 seconds before assets;
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

A download-only media error does not force another historical timeline
backfill. When timeline enumeration otherwise completed, the archiver advances
the timeline state, records the incomplete asset as pending, and marks the run
`partial`. Transient failures receive a durable `next_retry_at` and are skipped
until due rather than retried by every invocation. Video recovery delegates
variant selection to yt-dlp instead of repeating only gallery-dl's original
highest-bitrate CDN URL.

Two refreshed attempts at least 24 hours apart that return only HTTP `404` or
`410` across the available variants classify an asset as source-unavailable.
It leaves the automatic queue but retains its post identity, failure history,
and evidence. An otherwise complete run reports
`complete_with_unavailable_media`, exits successfully with a warning, and does
not pretend the missing bytes were recovered.

To retry only recorded incomplete media without crawling the timeline, run:

```bash
uv run scripts/archive-x --user USERNAME --retry-failed-only
```

gallery-dl preserves an interrupted download as a `.part` file and resumes it
with an HTTP Range request when the server supports resuming. Pending-media
recovery uses up to 8 retries and a 300-second inactivity timeout by default;
these can be changed with `--media-retries` and `--media-timeout`. The normal
request and endpoint delays still apply.

If an asset remains incomplete, the recovery run stays `partial` and exits
nonzero; rerunning the same command continues from the retained `.part` file.
After a recovery-only run succeeds, run the normal archive command when a
current timeline and profile-media refresh is also wanted:

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
stop. Operators do not calculate or supply a window count. New roots target
three UTC days and split recursively when dense; an interrupted active window
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

Legacy metadata completion is independent of media completion. Media-bearing
posts enter the existing pending-media queue and are retried through the normal
individual-post recovery path.

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
expansion remain out of scope.

The worker makes one focal-post request at a time, persists its next-safe
request time, prefers finishing the current ancestor chain, periodically yields
between chains/users, and has bounded attempts, leases, timeouts, and backoff.
No `--max-posts` value is required for normal closure.

Stopping with Ctrl-C or SIGTERM leaves the current target retryable. Deleted,
private, suspended, and withheld boundaries are recorded; ambiguous failures
are retried with bounded backoff and eventually require manual review. Use
`retry POST_ID...` for an explicit reclassification retry. Rebuild the
portable views with `export`.

Metadata closure is independent of media. Context media is processed
automatically after metadata, verifies SHA-256 sidecars, and refuses to start
below 5 GiB free. Failures remain explicit and retryable without unresolving
captured metadata.

The standalone context CLI remains available for advanced read-only status,
integrity, export, guarded retry, and deliberately bounded maintenance:

```bash
scripts/archive-x-context --user USERNAME status
scripts/archive-x-context --user USERNAME integrity
scripts/archive-x-context --user USERNAME export
scripts/archive-x-context --user USERNAME run --max-posts 1  # bounded maintenance
scripts/archive-x-context --user USERNAME media --max-posts 1
```

For a deliberately bounded unified production smoke, use the advanced
`--modern-max-posts`, `--legacy-max-windows`, `--context-max-posts`, and
`--context-media-max-posts` controls. A bounded result remains resumable and is
never reported as full completion.

### X archive contents

Each account is self-contained under `users/HANDLE/`:

```text
users/HANDLE/
├── _state/                  # timeline state plus separate context.sqlite3
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
remain the source of truth, while `dataset/*.jsonl` are atomically rebuilt
portable views intended for later indexing or LLM dataset preparation.

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
