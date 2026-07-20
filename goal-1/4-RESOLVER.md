# 4-RESOLVER

## Result

- Added a numeric-ID focal resolver using gallery-dl's REST single-Tweet path.
- Conversation, sibling, descendant, quote, pin, and expansion behaviors are
  disabled; output containing anything except the one requested ID fails.
- Metadata is stored transactionally before capture. A revealed parent edge
  and target are committed in the same transaction.
- Authorship and exported relationship use the archive-bound numeric user ID,
  not the individual extractor's `user` object.
- Individual-Tweet gallery-dl source is now fingerprint-pinned in addition to
  the existing version and API-call fingerprint.

## Evidence

- Config, non-focal rejection, stable-ID authorship, root/ancestor-chain, and
  transactional observation tests pass offline.

