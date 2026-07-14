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
scripts/archive-x --user tszzl
scripts/archive-x --input-file x.txt
```

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
- no proxy rotation, header spoofing, concurrency, or local-disk fallback;
- a profile-info probe binds each handle archive to its stable numeric X user
  ID before timeline downloads, so a recycled handle fails closed;
- initial runs backfill as far as X exposes; later runs use a best-effort
  48-hour overlap, with pinned-item injection disabled so an old pin cannot
  silently terminate the incremental scan;
- interrupted timeline cursors are recorded for a later resume when provided
  by gallery-dl, together with the original date cutoff;
- reposts are included by default, retain the original author, and are marked
  `relationship: "repost"`; use `--no-reposts` to exclude them;
- non-repost reply-thread context is excluded using numeric author IDs, and
  separately yielded quoted-source media is excluded.

X's transformed reply-timeline data does not always retain the account ID of
the repost wrapper. Repost attribution is therefore best effort: an unusual
repost-shaped item embedded as conversation context can be retained as a
repost. The raw metadata is kept so this can be reclassified later. Use
`--no-reposts` if strict target-authorship filtering is more important than
retaining reposts.

Run a network-free validation first:

```bash
scripts/archive-x --user tszzl --dry-run
```

Run a deliberately incomplete live smoke test with a small post limit:

```bash
scripts/archive-x --user tszzl --post-limit 5
```

Limited runs save what they observe but are never marked as a completed
backfill. Other useful controls include `--since 2026-01-01`,
`--full-rescan`, `--keep-going`, and `--output-root PATH`. By default output
goes to a writable Bibliotheque mount under `gdl/x-archive`; the command exits
instead of silently filling the local disk.

Incremental stopping relies on timeline order supplied by X. A 48-hour
overlap and disabled pin injection address the common failure mode, but X can
still return non-monotonic thread modules. Periodic `--full-rescan` runs are
the maximum-completeness option; gallery-dl and X themselves can still impose
historical visibility limits.

### X archive contents

Each account is self-contained under `users/HANDLE/`:

```text
users/HANDLE/
├── _state/                  # per-user media archive DB and resume state
├── media/YYYY/MM/           # original images/videos plus JSON sidecars
├── media/profile/           # avatar and header history
├── runs/RUN_ID/             # immutable raw JSONL, configs, logs, manifest
└── dataset/
    ├── posts.jsonl          # authored posts, replies, and labeled reposts
    ├── authored-posts.jsonl # only content authored by HANDLE
    ├── reposts.jsonl        # repost-only view with original author retained
    ├── media.jsonl          # portable local asset index and SHA-256 values
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
