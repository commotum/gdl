#!/usr/bin/env python3
"""Direct, lock-external orchestration for the complete X archive lifecycle."""

from __future__ import annotations

import time
from argparse import Namespace
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Callable

import archive_x
import archive_x_context as context_x
import archive_x_legacy as legacy_x
import archive_x_local as local_x
import archive_x_media as media_x
import archive_x_refresh as refresh_x


SUCCESSFUL_MODERN = {
    "success",
    "partial",
    "complete_with_unavailable_media",
}


def user_dir_for(archive_root: Path, handle: str) -> Path:
    return archive_root / "users" / handle


def accept_transition(
    user_dir: Path, modern_result: dict[str, Any]
) -> dict[str, Any]:
    state = archive_x.load_json(user_dir / "_state" / "state.json", {})
    if isinstance(state.get("legacy_backfill"), dict):
        legacy = legacy_x.validate_legacy_state(
            state["legacy_backfill"],
            expected_user_id=str(state.get("requested_user_id") or ""),
        )
        return {
            "status": "already_initialized",
            "source_run_id": legacy["source"]["run_id"],
        }
    if modern_result.get("status") != "stalled":
        return {"status": "not_applicable", "reason": "modern_did_not_stall"}
    classification = legacy_x.classify_legacy_transition(
        user_dir, expected_run_id=str(modern_result.get("run_id") or "")
    )
    if classification["decision"] != "proven":
        return {"status": "ambiguous", **classification}
    initialized = legacy_x.automatic_initialize_legacy(
        user_dir,
        initialized_at=legacy_x.second_utc(archive_x.utc_now()),
        expected_run_id=str(modern_result.get("run_id") or ""),
    )
    return {
        "status": "initialized",
        **classification,
        "legacy_initialized": initialized["legacy_initialized"],
        "modern_head_initialized": initialized["modern_head_initialized"],
        "backup_path": str(
            Path(initialized["backup_path"]).relative_to(user_dir)
        ),
    }


def legacy_options(args: Namespace, max_root_windows: int | None) -> legacy_x.LegacyRunOptions:
    return legacy_x.LegacyRunOptions(
        cookies=args.cookies,
        max_root_windows=max_root_windows,
        request_limit=6,
        root_window_days=legacy_x.DEFAULT_ROOT_WINDOW_DAYS,
        empty_tail_pages=legacy_x.DEFAULT_EMPTY_TAIL_PAGES,
        walk_attempts=3,
        window_attempts=3,
        max_leaves=64,
        request_delay=args.request_delay,
        walk_delay="10-20",
        window_delay="5-15",
        retries=args.retries,
        http_timeout=args.http_timeout,
        stalled_rate_limit_cycles=args.stalled_rate_limit_cycles,
    ).validate()


def legacy_state_status(user_dir: Path) -> str:
    state = archive_x.load_json(user_dir / "_state" / "state.json", {})
    legacy = state.get("legacy_backfill")
    if not isinstance(legacy, dict):
        return "not_applicable"
    legacy_x.validate_legacy_state(
        legacy, expected_user_id=str(state.get("requested_user_id") or "")
    )
    return str(legacy["status"])


def _run_legacy_scheduler_impl(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    version: str,
    handles: list[str],
    *,
    progress: Any | None = None,
    runners: dict[str, Any] | None = None,
) -> dict[str, Any]:
    results = {
        handle: {
            "status": legacy_state_status(user_dir_for(archive_root, handle)),
            "runs": [],
        }
        for handle in handles
    }
    eligible = [
        handle
        for handle in handles
        if results[handle]["status"] in {"pending", "active"}
    ]
    if not eligible:
        return results
    try:
        legacy_x.verify_legacy_runner(repo_dir, version)
    except archive_x.ArchiveError as exc:
        for handle in eligible:
            results[handle]["status"] = "failed"
            results[handle]["error"] = str(exc)
        return results
    requested_limit = getattr(args, "legacy_max_windows", None)
    completed = {handle: 0 for handle in eligible}

    def run_one(handle: str, max_windows: int | None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if runners is not None and handle in runners:
            kwargs["runner"] = runners[handle]
        if progress is not None:
            def report(event: dict[str, Any]) -> None:
                name = str(event.get("event") or "legacy_progress")
                since = str(event.get("since") or "")[:10]
                until = str(event.get("until") or "")[:10]
                if name == "window_committed":
                    activity = (
                        f"committed legacy {since} to {until} "
                        f"(+{int(event.get('new_posts') or 0)} posts)"
                    )
                elif name == "walk_completed":
                    activity = (
                        f"verified legacy pass {int(event.get('attempt') or 0)} "
                        f"for {since} to {until}"
                    )
                else:
                    activity = f"verifying legacy {since} to {until}"
                progress.event(
                    handle,
                    phase="legacy",
                    phase_status="running",
                    activity=activity,
                    progress=name == "window_committed",
                    force=name == "window_committed",
                )

            kwargs["progress_callback"] = report
        return legacy_x.run_legacy_archive(
            legacy_options(args, max_windows),
            repo_dir,
            archive_root,
            handle,
            version,
            **kwargs,
        )

    if len(eligible) == 1:
        handle = eligible[0]
        try:
            run = run_one(handle, requested_limit)
        except archive_x.ArchiveError as exc:
            results[handle] = {
                "status": "failed",
                "runs": [],
                "error": str(exc),
            }
            return results
        results[handle]["runs"].append(
            {"run_id": run["run_id"], "status": run["status"]}
        )
        results[handle]["status"] = run["status"]
        return results

    active = set(eligible)
    while active:
        made_progress = False
        for handle in eligible:
            if handle not in active:
                continue
            if requested_limit is not None and completed[handle] >= requested_limit:
                results[handle]["status"] = "limited"
                active.remove(handle)
                continue
            try:
                run = run_one(handle, 1)
            except archive_x.ArchiveError as exc:
                results[handle]["status"] = "failed"
                results[handle]["error"] = str(exc)
                active.remove(handle)
                continue
            results[handle]["runs"].append(
                {"run_id": run["run_id"], "status": run["status"]}
            )
            committed = sum(
                1 for window in run.get("windows", ()) if window.get("state_committed")
            )
            completed[handle] += committed
            made_progress = made_progress or bool(committed)
            state_status = legacy_state_status(user_dir_for(archive_root, handle))
            if state_status in {"complete", "manual_review"}:
                results[handle]["status"] = state_status
                active.remove(handle)
            elif requested_limit is not None and completed[handle] >= requested_limit:
                results[handle]["status"] = "limited"
                active.remove(handle)
        if active and not made_progress:
            raise archive_x.ArchiveError("legacy scheduler made no durable progress")
    return results


def run_legacy_scheduler(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    version: str,
    handles: list[str],
    *,
    progress: Any | None = None,
    persistent_runner: bool = False,
) -> dict[str, Any]:
    with ExitStack() as stack:
        runners: dict[str, Any] | None = None
        if persistent_runner:
            runners = {}
            for handle in handles:
                user_dir = user_dir_for(archive_root, handle)
                account_id, _canonical = context_x.target_identity(user_dir)
                runners[handle] = stack.enter_context(
                    archive_x.x_runner_control_client(
                        repo_dir, account_id, legacy=True
                    )
                )
        return _run_legacy_scheduler_impl(
            args,
            repo_dir,
            archive_root,
            version,
            handles,
            progress=progress,
            runners=runners,
        )


def retry_shared_media(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    handle: str,
    version: str,
) -> dict[str, Any]:
    del version  # direct assets and bounded refresh use the already pinned workers
    return run_media_pipeline(
        args,
        repo_dir,
        archive_root,
        handle,
        max_assets=getattr(args, "context_media_max_posts", None),
        max_refreshes=getattr(args, "context_media_max_posts", None),
        persistent_runner=True,
    )


def _asset_state_counts(db_path: Path) -> dict[str, int]:
    with context_x.ContextDB(db_path, create=False) as database:
        return {
            str(row[0]): int(row[1])
            for row in database.connection.execute(
                "SELECT state,COUNT(*) FROM asset_jobs GROUP BY state"
            )
        }


def run_media_pipeline(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    handle: str,
    *,
    max_assets: int | None = None,
    max_refreshes: int | None = None,
    persistent_runner: bool = False,
) -> dict[str, Any]:
    """Drain descriptors directly, refresh exceptions once, then drain again."""
    user_dir = user_dir_for(archive_root, handle)
    db_path = user_dir / "_state" / "context.sqlite3"
    if not db_path.is_file():
        return {
            "status": "complete",
            "pending_before": 0,
            "pending_after": 0,
            "unavailable": 0,
            "manual_review": 0,
            "descriptor_hits": 0,
            "direct_attempted": 0,
            "captured": 0,
            "existing": 0,
            "downloaded": 0,
            "cdn_bytes": 0,
            "refresh_attempted": 0,
            "x_api_requests": 0,
            "x_support_requests": 0,
            "passes": [],
        }
    before = _asset_state_counts(db_path)
    passes: list[dict[str, Any]] = []
    totals = {
        "direct_attempted": 0,
        "captured": 0,
        "existing": 0,
        "downloaded": 0,
        "cdn_bytes": 0,
        "refresh_attempted": 0,
        "x_api_requests": 0,
        "x_support_requests": 0,
    }
    session = media_x.requests.Session()
    session.trust_env = False
    try:
        with ExitStack() as stack:
            runner = None
            if persistent_runner:
                account_id, _canonical = context_x.target_identity(user_dir)
                runner = stack.enter_context(
                    archive_x.x_runner_control_client(repo_dir, account_id)
                )
            for _cycle in range(3):
                asset_remaining = (
                    None
                    if max_assets is None
                    else max(0, max_assets - totals["direct_attempted"])
                )
                direct: dict[str, Any] = {"attempted": 0}
                if asset_remaining is None or asset_remaining > 0:
                    direct = media_x.run_direct_media_worker(
                        archive_root=archive_root,
                        user_dir=user_dir,
                        db_path=db_path,
                        max_assets=asset_remaining,
                        max_attempts=max(
                            1, int(getattr(args, "media_retries", 2))
                        ),
                        timeout=(
                            min(
                                30.0,
                                float(getattr(args, "http_timeout", 60)),
                            ),
                            float(getattr(args, "media_timeout", 300)),
                        ),
                        rate_limit=getattr(args, "rate_limit", "8M"),
                        session=session,
                    )
                    totals["direct_attempted"] += int(
                        direct.get("attempted") or 0
                    )
                    totals["captured"] += int(direct.get("captured") or 0)
                    totals["existing"] += int(direct.get("existing") or 0)
                    totals["downloaded"] += int(direct.get("downloaded") or 0)
                    totals["cdn_bytes"] += int(direct.get("bytes") or 0)

                refresh_remaining = (
                    None
                    if max_refreshes is None
                    else max(
                        0, max_refreshes - totals["refresh_attempted"]
                    )
                )
                refresh: dict[str, Any] = {"attempted": 0}
                if refresh_remaining is None or refresh_remaining > 0:
                    refresh = refresh_x.run_descriptor_refresh_worker(
                        repo_dir=repo_dir,
                        archive_root=archive_root,
                        user_dir=user_dir,
                        db_path=db_path,
                        handle=handle,
                        cookie_file=args.cookies,
                        max_posts=refresh_remaining,
                        request_delay=args.request_delay,
                        max_attempts=3,
                        runner=runner,
                    )
                    totals["refresh_attempted"] += int(
                        refresh.get("attempted") or 0
                    )
                    totals["x_api_requests"] += int(
                        refresh.get("x_api_requests") or 0
                    )
                    totals["x_support_requests"] += int(
                        refresh.get("x_support_requests") or 0
                    )
                passes.append({"direct": direct, "refresh": refresh})
                if not int(direct.get("attempted") or 0) and not int(
                    refresh.get("attempted") or 0
                ):
                    break
                if (
                    max_assets is not None
                    and totals["direct_attempted"] >= max_assets
                    and max_refreshes is not None
                    and totals["refresh_attempted"] >= max_refreshes
                ):
                    break
    finally:
        session.close()

    after = _asset_state_counts(db_path)
    actionable = sum(
        after.get(state, 0)
        for state in ("pending", "leased", "retryable", "needs_refresh")
    )
    manual = after.get("manual_review", 0)
    unavailable = after.get("unavailable", 0)
    bounded = bool(
        (max_assets is not None and totals["direct_attempted"] >= max_assets)
        or (
            max_refreshes is not None
            and totals["refresh_attempted"] >= max_refreshes
        )
    )
    if actionable:
        status = "limited" if bounded else "partial"
    elif manual:
        status = "manual_review"
    elif unavailable:
        status = "complete_with_unavailable_media"
    else:
        status = "complete"
    return {
        "status": status,
        "pending_before": sum(
            before.get(state, 0)
            for state in ("pending", "leased", "retryable", "needs_refresh")
        ),
        "pending_after": actionable,
        "unavailable": unavailable,
        "manual_review": manual,
        "descriptor_hits": totals["direct_attempted"],
        **totals,
        "passes": passes,
    }


def context_phase_status(db_path: Path, *, media: bool) -> dict[str, Any]:
    with context_x.ContextDB(db_path, create=False) as database:
        status = database.status()
        availability = database.work_availability(
            now=time.time(), lease_seconds=900.0, media=media
        )
    actionable = availability["total"] - availability["manual_review"]
    if actionable:
        phase = "pending"
    elif availability["manual_review"]:
        phase = "manual_review"
    elif media and status["media"].get("unavailable", 0):
        phase = "complete_with_unavailable_media"
    else:
        phase = "complete"
    return {"status": phase, "availability": availability, "queue": status}


def run_context_worker(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    handle: str,
    *,
    media: bool,
    max_posts: int | None,
    progress: Any | None = None,
    runner: Any | None = None,
) -> dict[str, Any]:
    user_dir, db_path = context_x.user_paths(archive_root, handle)
    counts = context_x.run_worker(
        repo_dir=repo_dir,
        archive_root=archive_root,
        user_dir=user_dir,
        db_path=db_path,
        handle=handle,
        cookie_file=args.cookies,
        max_posts=max_posts,
        request_delay=args.request_delay,
        retry_delay=300.0,
        max_attempts=3,
        lease_seconds=900.0,
        fairness_quantum=50,
        max_depth=1000,
        media=media,
        progress=(
            None
            if progress is None
            else lambda state, post_id, durable: progress.event(
                handle,
                activity=f"{state} {post_id}",
                progress=durable,
            )
        ),
        runner=runner,
    )
    result = context_phase_status(db_path, media=media)
    if max_posts is not None and result["status"] == "pending":
        result["status"] = "limited"
    result["counts"] = counts
    return result


def _run_context_scheduler_impl(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    handles: list[str],
    *,
    media: bool,
    progress: Any | None = None,
    runners: dict[str, Any] | None = None,
) -> dict[str, Any]:
    requested_limit = getattr(
        args, "context_media_max_posts" if media else "context_max_posts", None
    )
    if len(handles) <= 1:
        results = {}
        for handle in handles:
            try:
                kwargs = {"media": media, "max_posts": requested_limit}
                if progress is not None:
                    kwargs["progress"] = progress
                if runners is not None and handle in runners:
                    kwargs["runner"] = runners[handle]
                results[handle] = run_context_worker(
                    args, repo_dir, archive_root, handle, **kwargs
                )
            except context_x.ContextAuthenticationError:
                raise
            except context_x.ContextError as exc:
                results[handle] = {"status": "failed", "error": str(exc)}
        return results
    results: dict[str, Any] = {}
    attempted = {handle: 0 for handle in handles}
    active = set(handles)
    while active:
        made_progress = False
        future: list[float] = []
        for handle in handles:
            if handle not in active:
                continue
            remaining = (
                None
                if requested_limit is None
                else requested_limit - attempted[handle]
            )
            if remaining is not None and remaining <= 0:
                status = context_phase_status(
                    context_x.user_paths(archive_root, handle)[1], media=media
                )
                status["status"] = "limited"
                results[handle] = status
                active.remove(handle)
                continue
            quantum = 50 if remaining is None else min(50, remaining)
            try:
                kwargs = {"media": media, "max_posts": quantum}
                if progress is not None:
                    kwargs["progress"] = progress
                if runners is not None and handle in runners:
                    kwargs["runner"] = runners[handle]
                result = run_context_worker(
                    args, repo_dir, archive_root, handle, **kwargs
                )
            except context_x.ContextAuthenticationError:
                raise
            except context_x.ContextError as exc:
                results[handle] = {"status": "failed", "error": str(exc)}
                active.remove(handle)
                continue
            count = int(result["counts"].get("attempted", 0))
            attempted[handle] += count
            made_progress = made_progress or count > 0
            if result["status"] in {
                "complete",
                "complete_with_unavailable_media",
                "partial",
                "manual_review",
            }:
                results[handle] = result
                active.remove(handle)
            elif requested_limit is not None and attempted[handle] >= requested_limit:
                result["status"] = "limited"
                results[handle] = result
                active.remove(handle)
            else:
                next_at = result["availability"].get("next_eligible_at")
                if next_at is not None:
                    future.append(float(next_at))
        if active and not made_progress:
            if not future:
                raise context_x.ContextError(
                    "context scheduler made no progress and has no retry time"
                )
            time.sleep(max(0.01, min(min(future) - time.time(), 60.0)))
    return results


def run_context_scheduler(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    handles: list[str],
    *,
    media: bool,
    progress: Any | None = None,
    persistent_runner: bool = False,
) -> dict[str, Any]:
    with ExitStack() as stack:
        runners: dict[str, Any] | None = None
        if persistent_runner:
            runners = {}
            for handle in handles:
                user_dir, _db_path = context_x.user_paths(archive_root, handle)
                account_id, _canonical = context_x.target_identity(user_dir)
                runners[handle] = stack.enter_context(
                    archive_x.x_runner_control_client(repo_dir, account_id)
                )
        return _run_context_scheduler_impl(
            args,
            repo_dir,
            archive_root,
            handles,
            media=media,
            progress=progress,
            runners=runners,
        )


def overall_status(phases: dict[str, Any]) -> str:
    statuses = []
    transition_status = str(
        (phases.get("transition") or {}).get("status") or ""
    )
    for name, value in phases.items():
        if not isinstance(value, dict) or not value.get("status"):
            continue
        status = str(value["status"])
        if (
            name == "modern"
            and status == "stalled"
            and transition_status == "initialized"
        ):
            continue
        if status in {"failed", "interrupted", "stalled", "ambiguous"}:
            return "failed"
        statuses.append(status)
    for candidate in (
        "failed",
        "manual_review",
        "partial",
        "limited",
        "complete_with_unavailable_media",
    ):
        if candidate in statuses:
            return candidate
    return "success"


def run_unified_followups(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    version: str,
    modern_results: dict[str, dict[str, Any]],
    *,
    checkpoint: Callable[[dict[str, dict[str, Any]]], None] | None = None,
    progress: Any | None = None,
) -> dict[str, dict[str, Any]]:
    combined: dict[str, dict[str, Any]] = {
        handle: {"modern": result} for handle, result in modern_results.items()
    }

    def emit() -> None:
        if checkpoint is not None:
            checkpoint(combined)

    def phase(handle: str, name: str, status: str, activity: str) -> None:
        if progress is not None:
            progress.event(
                handle, phase=name, phase_status=status,
                activity=activity, progress=status not in {"running", "pending"},
                force=status not in {"running", "pending"},
            )

    eligible: list[str] = []
    newly_initialized: list[str] = []
    diagnostic_modern_only = bool(args.post_limit or args.since is not None)
    bounded_modern_rollout = getattr(args, "modern_max_posts", None) is not None
    for handle, modern in modern_results.items():
        if diagnostic_modern_only:
            combined[handle]["transition"] = {"status": "skipped_diagnostic"}
            combined[handle]["status"] = "limited"
            continue
        if args.retry_failed_only:
            combined[handle]["transition"] = {"status": "skipped_retry_only"}
            if modern.get("status") in SUCCESSFUL_MODERN:
                eligible.append(handle)
            else:
                combined[handle]["status"] = "failed"
            emit()
            continue
        try:
            transition = accept_transition(
                user_dir_for(archive_root, handle), modern
            )
        except archive_x.ArchiveError as exc:
            combined[handle]["transition"] = {
                "status": "failed",
                "error": str(exc),
            }
            combined[handle]["status"] = "failed"
            emit()
            continue
        combined[handle]["transition"] = transition
        if transition["status"] == "initialized":
            newly_initialized.append(handle)
        if (
            modern.get("status") in SUCCESSFUL_MODERN
            or transition["status"] == "initialized"
            or (
                bounded_modern_rollout
                and modern.get("status") == "limited"
                and transition["status"] == "already_initialized"
            )
        ):
            eligible.append(handle)
        elif bounded_modern_rollout and modern.get("status") == "limited":
            combined[handle]["status"] = "limited"
        else:
            combined[handle]["status"] = "failed"
        emit()

    # A just-proven historical boundary gets one normal modern-head pass so the
    # modern/profile phase itself finishes without rewriting the hash-bound
    # stopped source manifest.
    for handle in newly_initialized:
        try:
            head = archive_x.archive_user(
                args, repo_dir, archive_root, handle, version
            )
        except archive_x.ArchiveError as exc:
            combined[handle]["modern_head_after_transition"] = {
                "status": "failed",
                "error": str(exc),
            }
            eligible.remove(handle)
            combined[handle]["status"] = "failed"
            emit()
            continue
        combined[handle]["modern_head_after_transition"] = head
        if head["status"] not in SUCCESSFUL_MODERN:
            eligible.remove(handle)
            combined[handle]["status"] = "failed"
        emit()

    if not eligible:
        emit()
        return combined

    if not args.retry_failed_only:
        for handle in eligible:
            phase(handle, "legacy", "running", "checking legacy coverage")
        legacy = run_legacy_scheduler(
            args, repo_dir, archive_root, version, eligible,
            progress=progress, persistent_runner=True,
        )
        for handle in eligible:
            combined[handle]["legacy"] = legacy[handle]
            phase(
                handle, "legacy", str(legacy[handle].get("status", "complete")),
                "legacy coverage recorded",
            )
            emit()

    for handle in eligible:
        phase(handle, "shared_media", "running", "checking authored media")
        try:
            combined[handle]["shared_media"] = retry_shared_media(
                args, repo_dir, archive_root, handle, version
            )
        except archive_x.ArchiveError as exc:
            combined[handle]["shared_media"] = {
                "status": "failed",
                "error": str(exc),
            }
        emit()
        phase(
            handle, "shared_media",
            str(combined[handle]["shared_media"].get("status", "complete")),
            "authored media checked",
        )

    if args.retry_failed_only:
        context_handles = [
            handle
            for handle in eligible
            if context_x.user_paths(archive_root, handle)[1].is_file()
        ]
    else:
        context_handles = []
        for handle in eligible:
            phase(handle, "context_seed", "running", "discovering reply parents")
            try:
                user_dir, db_path = context_x.user_paths(archive_root, handle)
                combined[handle]["context_seed"] = {
                    "status": "complete",
                    **context_x.seed_context(
                        user_dir, db_path, dry_run=False, max_depth=1000
                    ),
                }
            except context_x.ContextError as exc:
                combined[handle]["context_seed"] = {
                    "status": "failed",
                    "error": str(exc),
                }
            else:
                context_handles.append(handle)
            emit()
            phase(
                handle, "context_seed",
                str(combined[handle]["context_seed"].get("status", "complete")),
                "reply-parent queue seeded",
            )
        for handle in context_handles:
            phase(
                handle, "context_metadata", "running",
                "fetching reply-parent context",
            )
        metadata = run_context_scheduler(
            args, repo_dir, archive_root, context_handles, media=False,
            progress=progress, persistent_runner=True,
        )
        for handle in context_handles:
            combined[handle]["context_metadata"] = metadata[handle]
            phase(
                handle, "context_metadata",
                str(metadata[handle].get("status", "complete")),
                "reply-parent context recorded",
            )
            emit()

    media: dict[str, dict[str, Any]] = {}
    for handle in context_handles:
        if args.retry_failed_only:
            media[handle] = dict(combined[handle]["shared_media"])
            continue
        phase(
            handle,
            "context_media",
            "running",
            "downloading saved media descriptors",
        )
        try:
            media[handle] = run_media_pipeline(
                args,
                repo_dir,
                archive_root,
                handle,
                max_assets=getattr(args, "context_media_max_posts", None),
                max_refreshes=getattr(args, "context_media_max_posts", None),
                persistent_runner=True,
            )
        except (archive_x.ArchiveError, context_x.ContextError) as exc:
            media[handle] = {"status": "failed", "error": str(exc)}
    for handle in context_handles:
        combined[handle]["context_media"] = media[handle]
        phase(
            handle, "context_media",
            str(combined[handle]["context_media"].get("status", "complete")),
            "context media checked",
        )
        phase(handle, "context_export", "running", "checking export checkpoint")
        try:
            user_dir, db_path = context_x.user_paths(archive_root, handle)
            with context_x.ContextDB(db_path, create=False) as database:
                local_ready = database.connection.execute(
                    "SELECT 1 FROM current_pointers "
                    "WHERE pointer_name='local_history_reconciled'"
                ).fetchone() is not None
            export = (
                local_x.checkpoint_exports(user_dir, db_path)
                if local_ready
                else {
                    "status": "complete",
                    **context_x.export_datasets(user_dir, db_path),
                }
            )
            export["legacy_checkpoint_recorded"] = (
                legacy_x.record_unified_export_checkpoint(user_dir, export)
                if local_ready
                else False
            )
            combined[handle]["context_export"] = export
        except (context_x.ContextError, archive_x.ArchiveError) as exc:
            combined[handle]["context_export"] = {
                "status": "failed",
                "error": str(exc),
            }
        emit()
        phase(
            handle, "context_export",
            str(combined[handle]["context_export"].get("status", "complete")),
            "portable export checkpoint recorded",
        )

    for handle in combined:
        combined[handle]["status"] = overall_status(combined[handle])
    emit()
    return combined
