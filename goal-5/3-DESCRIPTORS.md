# 3-DESCRIPTORS

## Current Facts

- Stage 2 established schema v3 descriptor generations, descriptor
  observations, individual asset jobs, source provenance, constraints, and
  indexed claims in the existing per-user context database.
- Stage 1 proved that gallery-dl's `prepare` event retains the selected media
  URL, post ID, ordinal, type, extension, naming inputs, dimensions, duration,
  bitrate, and alt text while `--no-download` performs no CDN transfer.
- Modern timeline/retry, bounded context, legacy search, and profile work all
  used different runner paths. None previously emitted the same narrow,
  validated descriptor contract.
- Context conversation and legacy search responses can contain records outside
  authoritative scope, so file-event capture must remain provisional until the
  post-selection transaction accepts exact IDs.

## Baseline Ledger

- Context metadata retained only a post-level media count; a later media phase
  needed a second exact X extraction to rediscover each post's file URLs.
- Authored/legacy pending media likewise retained filenames/failure evidence
  but not a reusable selected descriptor.
- The mandatory info record already contained avatar and banner URLs, but the
  ordinary full run still launched separate profile extractors.
- Stage 1's deterministic prepare-event workload emitted six provisional file
  records and zero HTTP/CDN requests. Five accepted assets were selectable by
  exact post ID and one nearby record was rejectable.
- Descriptor capture adds no runner or network request. Stage 3 does not yet
  change the old context-media or retry execution path; that occurs in Stages
  4, 5, and 9.

## Updated Assumptions

- The selected descriptor has no trustworthy explicit expiry timestamp.
  Capture time and URL generation are durable; eligible stale/403 evidence will
  trigger a bounded exact refresh in Stage 5.
- A signed/query-bearing URL change creates a new descriptor generation. An
  identical descriptor from another operation creates only another provenance
  observation.
- A verified captured file remains captured when only its usable URL
  generation changes. A profile path change creates pending work for the new
  historical asset without overwriting the prior file.
- Descriptor loss is recoverable and therefore cannot make accepted metadata,
  reply edges, a modern cursor, or a legacy frontier fail to commit.

## Big-Picture Objective

Capture reusable file descriptors during extraction work already required by
the archive, bind them to exact source/run/operation evidence, and persist jobs
only for posts or profile assets admitted into authoritative scope.

## Detailed Implementation

- Added `scripts/archive_x_descriptors.py` with a private gallery-dl
  `prepare`-event postprocessor, a narrow versioned schema, strict CDN-host and
  path validation, descriptor digests, artifact preparation/finalization, and
  safe summaries.
- Both pinned X runners install the postprocessor only after their existing
  gallery-dl compatibility checks pass.
- Modern timeline, exact retry, and legacy walk runs write descriptor artifacts
  beside immutable raw evidence. Artifacts are mode `0600`, hash-bound in run
  results, and operation/run/source-bound when loaded.
- Bounded context extraction writes a random operation-bound ephemeral
  artifact. The same accepted metadata transaction commits reply records,
  edges, descriptors, and jobs; normal completion removes the ephemeral file,
  while an interrupted residue cannot be consumed by a later operation.
- Confirmed legacy walks persist descriptors only after the two post-record
  observations agree. Both descriptor observations are retained in walk order,
  so the newest usable URL becomes active without weakening the record proof.
- The mandatory info record directly derives avatar and banner descriptors,
  including the historical naming dates used by the existing profile paths.
- Persistence filters by exact accepted post IDs and declared media counts,
  versions changed descriptors, deduplicates replays, records observations,
  preserves verified files, and creates `needs_refresh` jobs for missing
  ordinals.
- Descriptor parsing and persistence failures are sanitized. A descriptor
  savepoint rolls back independently and makes missing assets refreshable while
  leaving metadata captured.

## Safety Invariants

- The custom postprocessor observes file preparation only and never initiates
  a transfer or changes gallery-dl's extraction outcome.
- Only HTTPS URLs on the allowlisted X media CDN hosts and safe archive-relative
  destinations can enter durable state.
- Post ownership requires a positive exact post ID and positive media ordinal;
  profile ownership is restricted to the bound account avatar/banner slots.
- Returned conversation/search records outside the accepted ID set create no
  descriptor generation or asset job.
- Private URLs occur only in mode-`0600` artifacts/database state. Manifests,
  telemetry, progress, errors, and safe summaries retain only hashes, counts,
  paths, operations, and sanitized classes.
- No network or filesystem transfer occurs inside a SQLite write transaction.
- Existing identity, context bounds, depth/cycle, legacy interval, independent
  confirmation, raw snapshot, and cursor/frontier guards remain unchanged.

## No-Cheating Checks

- A real gallery-dl `DownloadJob` fixture captures multi-image, video,
  animated-GIF, and mixed-media descriptors with `download=false`, zero actual
  HTTP requests, and zero media files.
- A one-request context worker fixture commits the accepted post and its asset
  descriptor from that same result; no exact media rediscovery is invoked.
- Conversation and modern endpoint fixtures include a nearby record and prove
  it is rejected before durable persistence.
- A changed artifact hash or operation, malformed JSON, invalid UTF-8, unsafe
  origin, invalid digest, or conflicting ordinal does not become a job and does
  not expose input content in the error summary.
- A count without a usable descriptor becomes `needs_refresh`; it is not
  silently called complete and does not roll back metadata.

## Completion Evidence

- Custom prepare capture covers photo, multi-image, video, animated GIF, and
  mixed-media forms with exact deterministic paths and no transfer.
- Modern, retry, context, legacy, and info/profile construction all configure
  the same validated mechanism.
- Hash/operation binding, accepted-ID filtering, missing ordinals, injected
  persistence failure, replay deduplication, URL supersession, captured-file
  preservation, two-walk selection, profile history, and one-fetch context
  integration are covered by focused tests.
- Exact verification commands passed:

  - `uv run python -m unittest -v tests.test_archive_x_descriptors`
  - focused modern/context/legacy/runner/unified/recovery suites
  - `uv run python -m unittest discover -s tests -v` — 266 tests
  - `uv run python scripts/gallery_dl_x_runner.py --version` — `1.32.4`
  - `uv run python scripts/gallery_dl_x_legacy_runner.py --version` — `1.32.4`
  - `uv run python -m compileall -q scripts tests`
  - `git diff --check`
- No live request, production archive mutation, process start/stop, or bounded
  smoke was used.

## Stage Results

Stage complete. The durable queue now has sufficient exact descriptors for
Stage 4 to replace the normal context/shared/profile media rediscovery path
with direct, verified CDN transfer. The old transfer phases intentionally
remain active until that downloader and its failure/recovery tests are proven;
Stage 3 alone does not claim the request savings as deployed behavior.
