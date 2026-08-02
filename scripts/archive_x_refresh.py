#!/usr/bin/env python3
"""Bounded exact-post descriptor refresh for exceptional X media work."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Callable

import archive_x
import archive_x_context as context_x
import archive_x_descriptors as descriptor_x
import archive_x_media as media_x


DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 300.0
DEFAULT_LEASE_SECONDS = 900.0


def _verified_candidate(
    archive_root: Path,
    user_dir: Path,
    job: dict[str, Any],
) -> media_x.TransferEvidence | None:
    relative = job.get("expected_relative_path")
    if relative:
        try:
            evidence = media_x.verify_existing(archive_root, user_dir, job)
        except media_x.DirectMediaError:
            evidence = None
        if evidence is not None:
            return evidence
    if job.get("owner_kind") != "post":
        return None
    media_root = user_dir / "media"
    if not media_root.is_dir():
        return None
    pattern = f"*_{job['owner_id']}_{int(job['media_ordinal'])}_*.json"
    matches: list[media_x.TransferEvidence] = []
    for sidecar in media_root.rglob(pattern):
        asset = Path(str(sidecar)[:-5])
        if not asset.is_file():
            continue
        try:
            relative_path = asset.resolve().relative_to(archive_root.resolve())
        except (OSError, ValueError):
            continue
        candidate = dict(job)
        candidate["expected_relative_path"] = relative_path.as_posix()
        try:
            evidence = media_x.verify_existing(
                archive_root, user_dir, candidate
            )
        except media_x.DirectMediaError:
            continue
        if evidence is not None:
            matches.append(evidence)
    unique = {evidence.relative_path: evidence for evidence in matches}
    return next(iter(unique.values())) if len(unique) == 1 else None


def reconcile_verified_assets(
    *,
    archive_root: Path,
    user_dir: Path,
    database: context_x.ContextDB,
    account_id: str,
) -> dict[str, int]:
    counts = {"examined": 0, "captured": 0}
    for job in database.assets_needing_refresh():
        counts["examined"] += 1
        try:
            evidence = _verified_candidate(archive_root, user_dir, job)
        except media_x.DirectMediaError:
            evidence = None
        if evidence is None:
            continue
        ledger_job = dict(job)
        sidecar = archive_x.load_json(
            Path(str(archive_root / evidence.relative_path) + ".json"), {}
        )
        if isinstance(sidecar, dict):
            for key in ("retweet_id", "date"):
                if sidecar.get(key) is not None:
                    ledger_job[
                        "posted_at" if key == "date" else key
                    ] = sidecar[key]
        try:
            media_x.update_download_archive(
                user_dir, ledger_job, account_id=account_id
            )
        except (OSError, sqlite3.Error, media_x.DirectMediaError) as exc:
            raise context_x.ContextError(
                "local media ledger reconciliation failed"
            ) from exc
        database.reconcile_asset_succeeded(
            asset_id=int(job["asset_id"]),
            final_relative_path=evidence.relative_path,
            sha256=evidence.sha256,
            byte_count=evidence.byte_count,
            stat_result=evidence.stat_result,
            portable_record=evidence.portable_record,
        )
        counts["captured"] += 1
    return counts


def _add_request_telemetry(
    counts: dict[str, Any], result: context_x.FetchResult
) -> None:
    summary = result.request_telemetry
    if not isinstance(summary, dict):
        return
    counts["actual_requests"] += int(summary.get("actual_requests") or 0)
    categories = summary.get("by_category")
    if isinstance(categories, dict):
        counts["x_api_requests"] += int(categories.get("x_api") or 0)
        counts["x_support_requests"] += int(categories.get("x_support") or 0)


def _discard_batches(result: context_x.FetchResult) -> None:
    for batch in result.descriptor_batches:
        descriptor_x.discard_ephemeral_artifact(batch)


def run_descriptor_refresh_worker(
    *,
    repo_dir: Path,
    archive_root: Path,
    user_dir: Path,
    db_path: Path,
    handle: str,
    cookie_file: Path,
    max_posts: int | None = None,
    request_delay: str = "4-8",
    retry_delay: float = DEFAULT_RETRY_DELAY,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
    fetcher: Callable[..., context_x.FetchResult] = context_x.fetch_post,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    runner: Any | None = None,
) -> dict[str, Any]:
    if max_posts is not None and max_posts < 1:
        raise context_x.ContextError("descriptor refresh limit must be positive")
    if retry_delay < 0 or max_attempts < 1 or lease_seconds <= 0:
        raise context_x.ContextError("descriptor refresh retry settings are invalid")
    target_id, canonical_handle = context_x.target_identity(user_dir)
    counts: dict[str, Any] = {
        "attempted": 0,
        "logical_requests": 0,
        "actual_requests": 0,
        "x_api_requests": 0,
        "x_support_requests": 0,
        "local_existing": 0,
        "refreshes_created": 0,
        "complete": 0,
        "retryable": 0,
        "unavailable": 0,
        "manual_review": 0,
        "descriptor_rows": 0,
    }
    actual_boundary_pacing = fetcher is context_x.fetch_post or runner is not None
    with context_x.ContextDB(db_path, create=False) as database:
        database.bind_identity(target_id, canonical_handle)
        local = reconcile_verified_assets(
            archive_root=archive_root,
            user_dir=user_dir,
            database=database,
            account_id=target_id,
        )
        counts["local_existing"] = local["captured"]
        prepared = database.prepare_descriptor_refreshes()
        counts["refreshes_created"] = prepared["created"]
        counts["prepared"] = prepared

        while max_posts is None or counts["attempted"] < max_posts:
            now = clock()
            refresh = database.claim_descriptor_refresh(
                now=now, lease_seconds=lease_seconds
            )
            if refresh is None:
                break
            counts["attempted"] += 1
            refresh_id = int(refresh["refresh_id"])
            lease_token = str(refresh["lease_token"])
            post_id = str(refresh["owner_id"])
            destination_scope = database.refresh_destination_scope(post_id)
            if destination_scope == "unknown":
                database.descriptor_refresh_failed(
                    refresh_id=refresh_id,
                    lease_token=lease_token,
                    state="manual_review",
                    error_class="refresh_destination_unknown",
                    count_attempt=False,
                )
                counts["manual_review"] += 1
                continue
            result: context_x.FetchResult | None = None
            try:
                if not actual_boundary_pacing:
                    context_x.reserve_request(
                        database, request_delay, now=clock, sleep=sleep
                    )
                fetch_kwargs: dict[str, Any] = {}
                if actual_boundary_pacing:
                    fetch_kwargs.update(
                        request_delay=request_delay,
                        runner=runner,
                        control_lease_token=lease_token,
                    )
                result = fetcher(
                    repo_dir=repo_dir,
                    archive_root=archive_root,
                    user_dir=user_dir,
                    handle=canonical_handle or handle,
                    post_id=post_id,
                    cookie_file=cookie_file,
                    media=False,
                    conversation=False,
                    descriptor_source_kind="exact_refresh",
                    descriptor_source_operation="exact_refresh",
                    destination_scope=destination_scope,
                    request_operation="descriptor_refresh",
                    **fetch_kwargs,
                )
                counts["logical_requests"] += 1
                _add_request_telemetry(counts, result)
            except KeyboardInterrupt:
                database.descriptor_refresh_failed(
                    refresh_id=refresh_id,
                    lease_token=lease_token,
                    state="retryable",
                    error_class="interrupted",
                    count_attempt=False,
                )
                raise
            except OSError:
                state = (
                    "manual_review"
                    if int(refresh["attempts"]) >= max_attempts
                    else "retryable"
                )
                next_attempt_at = (
                    0.0
                    if state == "manual_review"
                    else now
                    + min(
                        retry_delay
                        * (2 ** max(0, int(refresh["attempts"]) - 1)),
                        86400.0,
                    )
                )
                database.descriptor_refresh_failed(
                    refresh_id=refresh_id,
                    lease_token=lease_token,
                    state=state,
                    error_class="refresh_execution_error",
                    next_attempt_at=next_attempt_at,
                )
                counts[state] += 1
                continue
            except archive_x.ArchiveError:
                database.descriptor_refresh_failed(
                    refresh_id=refresh_id,
                    lease_token=lease_token,
                    state="manual_review",
                    error_class="refresh_execution_error",
                )
                counts["manual_review"] += 1
                continue

            if not actual_boundary_pacing:
                context_x.persist_rate_reset(database, result.rate_reset)
            if result.interrupted:
                database.descriptor_refresh_failed(
                    refresh_id=refresh_id,
                    lease_token=lease_token,
                    state="retryable",
                    error_class="interrupted",
                    count_attempt=False,
                )
                _discard_batches(result)
                raise KeyboardInterrupt
            if result.metadata is not None:
                summary = database.descriptor_refresh_succeeded(
                    refresh_id=refresh_id,
                    lease_token=lease_token,
                    metadata=result.metadata,
                    descriptor_batches=result.descriptor_batches,
                )
                _discard_batches(result)
                state = str(summary["refresh_state"])
                counts[state] += 1
                counts["descriptor_rows"] += int(
                    summary.get("rows_accepted") or 0
                )
                continue

            error_class, terminal, global_stop = context_x.classify_failure(result)
            if global_stop:
                database.descriptor_refresh_authentication_stopped(
                    refresh_id=refresh_id,
                    lease_token=lease_token,
                    error_class=error_class,
                    now=clock(),
                )
                _discard_batches(result)
                raise context_x.ContextAuthenticationError(
                    "descriptor refresh stopped on authentication/account state"
                )
            if terminal:
                state = "unavailable"
                next_attempt_at = 0.0
            elif int(refresh["attempts"]) >= max_attempts:
                state = "manual_review"
                next_attempt_at = 0.0
            else:
                state = "retryable"
                exponent = max(0, int(refresh["attempts"]) - 1)
                next_attempt_at = now + min(
                    retry_delay * (2**exponent), 86400.0
                )
            database.descriptor_refresh_failed(
                refresh_id=refresh_id,
                lease_token=lease_token,
                state=state,
                error_class=error_class,
                next_attempt_at=next_attempt_at,
            )
            counts[state] += 1
            _discard_batches(result)

        counts["quality"] = database.descriptor_refresh_quality()
        remaining = database.connection.execute(
            """SELECT state,COUNT(*) FROM descriptor_refresh_jobs
                 GROUP BY state ORDER BY state"""
        )
        counts["remaining"] = {str(row[0]): int(row[1]) for row in remaining}
    return counts
