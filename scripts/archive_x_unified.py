#!/usr/bin/env python3
"""Direct, lock-external orchestration for the complete X archive lifecycle."""

from __future__ import annotations

import copy
import time
from argparse import Namespace
from pathlib import Path
from typing import Any, Callable

import archive_x
import archive_x_context as context_x
import archive_x_legacy as legacy_x


SUCCESSFUL_MODERN = {"success", "partial"}


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
        walk_attempts=3,
        window_attempts=3,
        max_leaves=64,
        request_delay=args.request_delay,
        walk_delay="10-20",
        window_delay="30-60",
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


def run_legacy_scheduler(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    version: str,
    handles: list[str],
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
    if len(eligible) == 1:
        handle = eligible[0]
        try:
            run = legacy_x.run_legacy_archive(
                legacy_options(args, requested_limit),
                repo_dir,
                archive_root,
                handle,
                version,
            )
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
        progress = False
        for handle in eligible:
            if handle not in active:
                continue
            if requested_limit is not None and completed[handle] >= requested_limit:
                results[handle]["status"] = "limited"
                active.remove(handle)
                continue
            try:
                run = legacy_x.run_legacy_archive(
                    legacy_options(args, 1),
                    repo_dir,
                    archive_root,
                    handle,
                    version,
                )
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
            progress = progress or bool(committed)
            state_status = legacy_state_status(user_dir_for(archive_root, handle))
            if state_status in {"complete", "manual_review"}:
                results[handle]["status"] = state_status
                active.remove(handle)
            elif requested_limit is not None and completed[handle] >= requested_limit:
                results[handle]["status"] = "limited"
                active.remove(handle)
        if active and not progress:
            raise archive_x.ArchiveError("legacy scheduler made no durable progress")
        if active:
            archive_x.sleep_random("30-60", "before next legacy round")
    return results


def retry_shared_media(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    handle: str,
    version: str,
) -> dict[str, Any]:
    state = archive_x.load_json(
        user_dir_for(archive_root, handle) / "_state" / "state.json", {}
    )
    pending = state.get("pending_media")
    before = len(pending) if isinstance(pending, list) else 0
    if not before:
        return {"status": "complete", "pending_before": 0, "pending_after": 0}
    retry_args = copy.copy(args)
    retry_args.retry_failed_only = True
    retry_args.full_rescan = False
    retry_args.since = None
    retry_args.post_limit = None
    run = archive_x.archive_user(
        retry_args, repo_dir, archive_root, handle, version
    )
    state = archive_x.load_json(
        user_dir_for(archive_root, handle) / "_state" / "state.json", {}
    )
    remaining = state.get("pending_media")
    after = len(remaining) if isinstance(remaining, list) else 0
    return {
        "status": "complete" if after == 0 else "partial",
        "run_id": run["run_id"],
        "pending_before": before,
        "pending_after": after,
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
        phase = "partial"
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
    )
    result = context_phase_status(db_path, media=media)
    if max_posts is not None and result["status"] == "pending":
        result["status"] = "limited"
    result["counts"] = counts
    return result


def run_context_scheduler(
    args: Namespace,
    repo_dir: Path,
    archive_root: Path,
    handles: list[str],
    *,
    media: bool,
) -> dict[str, Any]:
    requested_limit = getattr(
        args, "context_media_max_posts" if media else "context_max_posts", None
    )
    if len(handles) <= 1:
        results = {}
        for handle in handles:
            try:
                results[handle] = run_context_worker(
                    args,
                    repo_dir,
                    archive_root,
                    handle,
                    media=media,
                    max_posts=requested_limit,
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
        progress = False
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
                result = run_context_worker(
                    args,
                    repo_dir,
                    archive_root,
                    handle,
                    media=media,
                    max_posts=quantum,
                )
            except context_x.ContextAuthenticationError:
                raise
            except context_x.ContextError as exc:
                results[handle] = {"status": "failed", "error": str(exc)}
                active.remove(handle)
                continue
            count = int(result["counts"].get("attempted", 0))
            attempted[handle] += count
            progress = progress or count > 0
            if result["status"] in {"complete", "partial", "manual_review"}:
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
        if active and not progress:
            if not future:
                raise context_x.ContextError(
                    "context scheduler made no progress and has no retry time"
                )
            time.sleep(max(0.01, min(min(future) - time.time(), 60.0)))
    return results


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
    for candidate in ("failed", "manual_review", "partial", "limited"):
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
) -> dict[str, dict[str, Any]]:
    combined: dict[str, dict[str, Any]] = {
        handle: {"modern": result} for handle, result in modern_results.items()
    }

    def emit() -> None:
        if checkpoint is not None:
            checkpoint(combined)

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
        legacy = run_legacy_scheduler(args, repo_dir, archive_root, version, eligible)
        for handle in eligible:
            combined[handle]["legacy"] = legacy[handle]
            emit()

    for handle in eligible:
        if args.retry_failed_only:
            recovery = modern_results[handle].get("media_recovery") or {}
            combined[handle]["shared_media"] = {
                "status": (
                    "complete"
                    if int(recovery.get("pending_after") or 0) == 0
                    else "partial"
                ),
                **recovery,
            }
        else:
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

    if args.retry_failed_only:
        context_handles = [
            handle
            for handle in eligible
            if context_x.user_paths(archive_root, handle)[1].is_file()
        ]
    else:
        context_handles = []
        for handle in eligible:
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
        metadata = run_context_scheduler(
            args, repo_dir, archive_root, context_handles, media=False
        )
        for handle in context_handles:
            combined[handle]["context_metadata"] = metadata[handle]
            emit()

    media = run_context_scheduler(
        args, repo_dir, archive_root, context_handles, media=True
    ) if context_handles else {}
    for handle in context_handles:
        combined[handle]["context_media"] = media[handle]
        try:
            user_dir, db_path = context_x.user_paths(archive_root, handle)
            with context_x.ContextDB(db_path, create=False) as database:
                errors = database.integrity_errors()
            if errors:
                raise context_x.ContextError("; ".join(errors))
            combined[handle]["context_export"] = {
                "status": "complete",
                **context_x.export_datasets(user_dir, db_path),
            }
        except context_x.ContextError as exc:
            combined[handle]["context_export"] = {
                "status": "failed",
                "error": str(exc),
            }
        emit()

    for handle in combined:
        combined[handle]["status"] = overall_status(combined[handle])
    emit()
    return combined
