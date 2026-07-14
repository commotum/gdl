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

The config points Instagram and Behance at local ignored files under `state/`.
Twitter/X uses a dedicated Firefox profile at `state/firefox-x-profile`.

Open the X login profile:

```bash
scripts/open-x-firefox-login
```

Log into X in that Firefox window, then close it. Verify gallery-dl can see the
needed auth cookies:

```bash
scripts/check-x-firefox-cookies
```

Chrome auth is not the supported path for this setup because Chrome's X
`auth_token` is encrypted behind the desktop keyring. The old
`scripts/save-x-cookies` helper is kept for reference, but Firefox is the path
used by `gallery-dl.conf`.

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
