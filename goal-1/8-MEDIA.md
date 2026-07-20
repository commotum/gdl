# 8-MEDIA

## Result

- Network-fetched observations with assets project separate pending media work;
  locally satisfied target-authored parents do not duplicate timeline media.
- `media --max-posts N` is opt-in and uses the focal-only worker, context media
  directory, existing gallery-dl archive/checksum postprocessors, separate
  attempts/leases, and the shared singleton locks.
- Completion requires an asset, JSON sidecar, and matching SHA-256. Downloads
  refuse to start with less than 5 GiB free. Media failure never reopens a
  captured metadata target or blocks ancestor closure.

## Evidence

- Metadata/media separation, failed/stale media, and checksum corruption tests
  pass offline.

