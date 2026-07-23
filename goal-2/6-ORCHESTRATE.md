# 6-ORCHESTRATE

## Current Facts

- A single walk is bounded and auditable but intentionally cannot advance
  state.
- The dedicated CLI uses both shared archive locks and requires initialized
  nested state before validating credentials or launching the runner.
- Repost inclusion is frozen in initialization provenance so different
  windows cannot silently change policy.

## Updated Assumptions

- Two consecutive valid walks with identical IDs and compatible stable fields
  are sufficient operational evidence for source-visible enumeration.
- A request-capped leaf can be bisected deterministically down to one second;
  saturation at one second or at the leaf-count cap requires manual review.

## Big Picture Objective

Drive a required, bounded number of newest-first legacy UTC windows, merge only
repeat-confirmed records, enqueue media separately, and commit the frontier
only after the dataset is durable.

## Detailed Implementation Plan

- Implement bounded walk retries, exact bisection, newest-leaf-first order,
  manifest evidence, canonical raw creation, dataset merge, media enqueue, and
  atomic state commit.
- Preserve the active window before the first request and replay it after any
  interruption.
- Add deterministic tests for matching/mismatching walks, caps/splits, bounds,
  locks, pacing, media separation, and isolation from ordinary archive mode.

## No-Cheating Checks

- No leaf confirms from one walk; invalid walks reset consecutive matching.
- Request saturation splits; it never raises or disables caps.
- The dataset merge precedes `next_until` mutation.
- Metadata-discovered media becomes pending work and cannot hold or roll back a
  confirmed metadata frontier.

## Completion Requirements

- Exact multi-window bounds and newest-first subdivision tests pass.
- API/identity/interruption paths stop without frontier advance.
- Installing/running ordinary timeline commands cannot invoke legacy code.
- Full offline regression and production preservation checks pass.

## Stage Results

- Added required `run --windows N` bounds, request/walk/window/leaf caps,
  conservative delays, and the existing global/archive lock pair.
- Each active leaf requires two consecutive matching valid walks. Three
  mismatching attempts enter manual review without advancing; request caps
  split the interval exactly and process the newer child first.
- Canonical raw metadata is finalized and merged before atomic frontier state.
  Media-bearing post IDs and expected asset counts enter the existing pending
  queue; normal per-post recovery can download them independently.
- Focused tests prove matching advancement, mismatch stop, exact split order,
  modern cursor preservation, deterministic dataset enrichment, and pending
  media creation. The full suite passes 116 tests.
- Production remains uninitialized and stopped; no live command was run.
