# 7-PROVE

## Verification Results

- Focused progress/context/unified suite: 72 tests passed.
- Full repository suite: 200 tests passed.
- Python compilation and whitespace checks pass.
- Production-scale read-only metric benchmark: 1.408 seconds, leading to a
  five-minute producer cadence and one-second JSON-only renderer refresh.
- A read-only live smoke against Visakanv rendered 258,962 posts, 50,696 media
  files (27.1 GB), 7,700 fetched parents, 4,185 unavailable boundaries, 14,797
  closed conversations, and 116,475 currently known remaining. The smoke wrote
  only to an auto-cleaned temporary directory and made no X request.
- Tmux behavior is unit-tested with an injected runner. The production tmux
  session and archive process were deliberately left untouched.

## Acceptance Audit

- The normal command, seven-line overview, 80-column fit, authoritative totals,
  distinct outcomes, action-required visibility, ETA omission, zero reporting
  requests, slow aggregate cadence, renderer isolation, interruption
  finalization, multiple-user blocks, docs, and fallbacks all have direct test
  or inspection evidence.
- Honest limitations: estimates cover the currently known context queue, not
  undiscovered ancestors or the whole archive; full closure totals can be up to
  five minutes old; a pane whose worker is killed before finalization becomes
  visibly stale rather than being allowed to mutate archive state.
