# 10-ROLLOUT

## Current Facts

- Offline implementation and verification are complete; all 176 tests pass.
- Visakanv remains exactly at the frozen pre-smoke state: legacy frontier
  `2010-10-29T00:00:00Z`, historical cursor preserved, two shared pending media
  items, and no context database.
- The preexisting `airkatakana` archive finished naturally. Tmux `x` is back
  at a Bash prompt and no archive worker is running.
- The host mount is healthy/read-write (`/dev/sdb`, ext4) with about 13 TB
  free; filtered kernel history shows no ext4 error/remount. Codex's default
  filesystem sandbox intentionally exposes `/mnt` read-only, so its dry-run
  truthfully reports the current process view as `ro`. Any approved smoke must
  use the authorized host execution path; the wrapper now refuses before an
  invocation/archive write if its actual process view is read-only.
- Production writes/network remain unapproved. The goal explicitly requires a
  separate authorization for the bounded smoke and forbids starting the full
  remaining backlogs.
- The advanced rollout path is now hard-bounded independently for modern,
  legacy, context metadata, context media, and shared-media attempts.

## Updated Assumptions

- The safest useful smoke target is the already initialized Visakanv archive,
  because it can exercise all three phases without allowing bounded modern
  output to prove or initialize a new transition.
- Context bootstrap itself must inventory all authoritative 618 MB of existing
  raw sources and create the durable SQLite graph; its network drain remains
  limited to one metadata target and one media target.
- One legacy root window may internally subdivide and perform repeat walks, but
  cannot commit more than one root interval.

## Big Picture Objective

Run one explicitly authorized, strictly bounded production invocation through
the exact unified wrapper, verify every independent authority, then leave all
remaining work stopped and resumable.

## Detailed Implementation Plan

- Confirm tmux `x` / `airkatakana` released the real lock naturally (complete).
- Recheck the host mount read-write immediately before launch; no remount or
  filesystem repair is currently indicated or authorized.
- Recheck mount, cookies, runner fingerprints, process/locks, identity, free
  space, state/dataset/backup hashes, pending media, and context DB absence.
- Preview this exact read-only command, then run it only after authorization:

  ```bash
  uv run scripts/archive-x --user visakanv \
    --modern-max-posts 5 \
    --legacy-max-windows 1 \
    --context-max-posts 1 \
    --context-media-max-posts 1 \
    --media-retries 1 \
    --media-timeout 60
  ```

- Monitor it through completion/interruption without launching a second worker.
- Verify: modern state did not advance under its bound; at most one legacy root
  window committed; context sources/edges were bootstrapped; at most one
  context metadata and one context-media target were attempted; shared media
  attempts honored the reduced retry/timeout; all queues remain truthful.
- Run post-smoke hashes, SQLite quick/foreign/integrity checks, manifests,
  permissions, full tests, and process checks.
- Leave the normal no-limit command stopped.

## No-Cheating Checks

- Use the unified wrapper only—no standalone phase run command.
- Count committed root windows and attempted context/media rows from durable
  manifests/SQLite, not stdout assumptions.
- Do not reinterpret local bootstrap size as permission to drain its network
  backlog.
- Do not remove limits, rerun automatically, or start the full normal command.

## Completion Requirements

- User explicitly authorizes the exact bounded production mutation/network
  scope.
- The unrelated lock owner exits before launch.
- Every requested bound is observed and independently auditable.
- Post-smoke state is valid, private, resumable, and all remaining processes are
  stopped.
- Final handoff states what changed, what remains, and the sole normal command.

## Stage Results

- Read-only preflight completed on 2026-07-22. The exact bounded command plus
  `--dry-run` validated cookies, gallery-dl 1.32.4, all four phase bounds,
  initialized legacy state, the pending frontier/floor, two shared media items,
  and absent context DB without a production write/request.
- Frozen state/dataset/backup hashes still match Stage 1 exactly. Cookie,
  state, and dataset modes are private (`0600`); the host mount has 13 TB free.
- Host/kernel inspection distinguished the Codex sandbox's read-only bind from
  the healthy host `rw` ext4 mount; no repair/remount is indicated.
- Tmux `x` is back at a Bash prompt after `airkatakana` finished naturally; no
  archive worker is running.
- Blocked only pending explicit bounded-production authorization. No smoke has
  started.
