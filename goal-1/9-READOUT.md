# 9-READOUT

## Result

- `status` reports target/media states, edges, distinct conversations, cycles,
  depth distribution, closure categories, pacing, and integrity errors.
- `export` atomically and deterministically rebuilds `context-posts.jsonl`,
  `reply-edges.jsonl`, and `context-status.json` from SQLite observations.
- Edge rows retain discovery provenance and unavailable boundaries. Context
  posts retain raw gallery-dl evidence and stable-ID authorship labels.
- Root and per-account README documentation now distinguishes ancestor closure
  from whole-thread capture and lists explicit bounded commands.

## Evidence

- Deterministic byte-for-byte rebuild, edge join, context/target authorship,
  integrity, and status tests pass.

