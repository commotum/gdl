# 3-DISCOVERY

## Result

- `seed` scans existing raw timeline JSONL without X requests and inserts
  child-to-parent edges idempotently by numeric ID.
- Shared parents converge; conflicting parents fail atomically; target-authored
  parents already present in timeline raw data become durable observations.
- `seed --dry-run` reports counts, paths, policies, and next commands without
  creating SQLite state. `--raw-path` supports bounded post-merge discovery and
  refuses paths outside that account's immutable `runs/` tree.
- Main timeline discovery is opt-in via `--seed-reply-context`, occurs only
  after dataset and cursor commits, and is separately reported without network
  work or authority over timeline status.

## Evidence

- Duplicate seed, shared/local parent, external record, malformed tail,
  malformed ID, path containment, no-write dry-run, and identity tests pass.

