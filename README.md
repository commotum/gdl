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
- initial runs backfill as far as X exposes; later runs use a 48-hour overlap;
- interrupted timeline cursors are recorded for a later resume when provided
  by gallery-dl;
- reposts are included by default, retain the original author, and are marked
  `relationship: "repost"`; use `--no-reposts` to exclude them;
- unrelated reply-thread participants and separately yielded quoted-source
  media are excluded so they are not mislabeled as the requested user.

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

The post records retain original post date and text; author/requested-user
identity; reply, conversation, quote, and repost IDs; language, hashtags,
mentions, sensitive-content flags, and article HTML; plus point-in-time likes,
views, reposts, quotes, replies, and bookmark counts. They also record
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
