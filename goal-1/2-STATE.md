# 2-STATE

## Result

- Added a private per-user `_state/context.sqlite3`, separate from
  `downloads.sqlite3`, with schema version 1 and fail-closed version checks.
- Added targets, reply edges, raw observations, pacing, attempts, leases, and
  media state with foreign keys, checks, triggers, full synchronization, and
  rollback-safe transactions/savepoints.
- Bound each database to the archive's stable numeric requested-user ID.
- Added `status` and `integrity` commands that use Python's SQLite interface.

## Evidence

- Fresh create/reopen, mode 0600, unknown schema, identity conflict,
  captured-without-observation, integrity, savepoint, and rollback tests pass.

