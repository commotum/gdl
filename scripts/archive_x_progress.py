#!/usr/bin/env python3
"""Read-only archive progress signals and atomic producer snapshots.

The archive engines remain authoritative. The producer writes phase events;
the live reader overlays cheap durable counters without mutating archive state.
"""

from __future__ import annotations

import copy
import json
import math
import os
import re
import sqlite3
import subprocess
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

SCHEMA = "gdl-x-progress"
SCHEMA_VERSION = 1
DASHBOARD_MIN_COLUMNS = 72
DASHBOARD_MIN_LINES = 20
DASHBOARD_PANE_LINES = 8
FINAL_STATUSES = {
    "success", "limited", "complete_with_unavailable_media", "failed",
    "interrupted",
}
TARGET_STATES = {
    "pending", "leased", "captured", "retryable", "unavailable",
    "manual_review",
}
MEDIA_STATES = {
    "none", "pending", "leased", "captured", "retryable", "unavailable",
    "manual_review",
}
ASSET_STATES = {
    "pending", "leased", "captured", "retryable", "needs_refresh",
    "unavailable", "manual_review",
}
SECRET_KEY = re.compile(
    r"(cookie|authorization|auth[_-]?token|csrf|ct0|proxy|cursor|password)",
    re.I,
)
ALLOWED_TOP = {
    "schema", "schema_version", "invocation_id", "started_at", "updated_at",
    "status", "users",
}
ALLOWED_USER = {
    "handle", "phase", "health", "activity", "last_progress_at", "wait_until",
    "phases", "totals", "baseline", "delta", "samples", "rate", "estimate",
    "action_required", "legacy",
}
ALLOWED_TOTALS = {
    "archive_posts", "archive_media_files", "archive_media_bytes",
    "archive_durable_generation", "archive_exported_generation",
    "archive_dirty_views",
    "context_captured", "context_parents_saved", "context_unavailable",
    "context_manual_review", "context_known_remaining",
    "context_media_remaining", "context_media_actionable",
    "context_media_captured", "context_media_unavailable",
    "context_media_manual_review", "conversations_closed",
    "conversations_total", "boundaries_deleted", "boundaries_private",
    "boundaries_suspended", "boundaries_other",
}
ALLOWED_SAMPLE = {"at", "known_remaining", "resolved"}
ALLOWED_RATE = {
    "items_per_hour", "coverage_days_per_hour", "window_seconds", "unit",
}
ALLOWED_ESTIMATE = {
    "seconds", "label", "confidence", "qualifier", "known_remaining",
}
ALLOWED_LEGACY = {
    "status", "initial_until", "next_until", "floor_since", "active_since",
    "active_until", "total_seconds", "completed_seconds", "remaining_seconds",
    "percent", "committed_windows", "committed_posts", "elapsed_seconds",
    "coverage_days_per_hour", "eta_seconds", "confidence", "wait_until",
    "last_progress_at",
}
ALLOWED_PHASES = {
    "starting", "modern", "legacy", "shared_media", "context_seed",
    "context_metadata", "context_media", "context_export",
}
ALLOWED_PHASE_STATUSES = {
    "pending", "active", "running", "success", "complete",
    "complete_with_unavailable_media", "limited", "partial", "stalled",
    "retrying", "retryable", "manual_review", "blocked", "failed",
    "interrupted", "ambiguous", "initialized", "not_initialized", "valid",
    "not_applicable", "already_initialized",
    "skipped_diagnostic", "skipped_retry_only", "published", "deferred",
    "unchanged", "empty",
}


class ProgressError(ValueError):
    pass


class NullProgressTracker:
    """Failure-isolating drop-in used when observability cannot initialize."""

    path: Path | None = None

    def event(self, *args: Any, **kwargs: Any) -> None:
        return None

    def refresh(self, *args: Any, **kwargs: Any) -> None:
        return None

    def finalize(self, *args: Any, **kwargs: Any) -> None:
        return None

    def snapshot(self) -> None:
        return None


class SafeProgressTracker:
    """Keep a renderer/disk failure from changing archive execution."""

    def __init__(self, tracker: "ProgressTracker") -> None:
        self.tracker = tracker
        self.path = tracker.path
        self.failed = False

    def _call(self, name: str, *args: Any, **kwargs: Any) -> None:
        if self.failed:
            return
        try:
            getattr(self.tracker, name)(*args, **kwargs)
        except Exception as exc:
            self.failed = True
            print(f"Dashboard disabled after telemetry error: {type(exc).__name__}: {exc}")

    def event(self, *args: Any, **kwargs: Any) -> None:
        self._call("event", *args, **kwargs)

    def refresh(self, *args: Any, **kwargs: Any) -> None:
        self._call("refresh", *args, **kwargs)

    def finalize(self, *args: Any, **kwargs: Any) -> None:
        self._call("finalize", *args, **kwargs)

    def snapshot(self) -> dict[str, Any] | None:
        if self.failed:
            return None
        try:
            return self.tracker.snapshot()
        except Exception:
            return None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def parse_time(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def human_number(value: int) -> str:
    return f"{int(value):,}"


def human_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1000 or unit == "TB":
            precision = 0 if unit == "B" else 1
            return f"{size:.{precision}f} {unit}"
        size /= 1000
    return f"{size:.1f} TB"


def human_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        hours, remainder = divmod(seconds, 3600)
        return f"{hours}h {remainder // 60}m"
    days = max(1, round(seconds / 86400))
    return f"{days}d"


def _group(connection: sqlite3.Connection, column: str) -> dict[str, int]:
    if column not in {"state", "media_state"}:
        raise ProgressError("unsupported aggregate")
    return {
        str(row[0]): int(row[1])
        for row in connection.execute(
            f"SELECT {column},COUNT(*) FROM targets GROUP BY {column}"
        )
    }


def _open_context(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _maintained_counters(
    connection: sqlite3.Connection,
) -> dict[str, int] | None:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='progress_counters'"
    ).fetchone()
    if table is None:
        return None
    counters = {
        str(row[0]): int(row[1])
        for row in connection.execute(
            "SELECT counter_name,value FROM progress_counters"
        )
    }
    required = {
        "targets_total",
        "observations_focal",
        "reply_edges_total",
        *(f"targets_state_{state}" for state in TARGET_STATES),
        *(f"targets_media_{state}" for state in MEDIA_STATES),
    }
    return counters if required <= set(counters) else None


def _context_fast_metrics(connection: sqlite3.Connection) -> dict[str, int]:
    """Collect the indexed counters that are safe to refresh frequently."""
    counters = _maintained_counters(connection)
    if counters is not None:
        states = {
            state: counters.get(f"targets_state_{state}", 0)
            for state in TARGET_STATES
        }
        media = {
            state: counters.get(f"targets_media_{state}", 0)
            for state in MEDIA_STATES
        }
        asset_total = counters.get("asset_jobs_total", 0)
        if asset_total:
            media = {
                state: counters.get(f"asset_jobs_state_{state}", 0)
                for state in ASSET_STATES
            }
        parents = counters["observations_focal"]
        private = counters.get("targets_unavailable_private", 0)
        deleted = counters.get("targets_unavailable_deleted", 0)
        suspended = counters.get("targets_unavailable_suspended", 0)
        other = counters.get("targets_unavailable_other", 0)
    else:
        states = _group(connection, "state")
        media = _group(connection, "media_state")
        unknown_states = set(states) - TARGET_STATES
        unknown_media = set(media) - MEDIA_STATES
        if unknown_states or unknown_media:
            raise ProgressError("context database contains unknown states")
        parents = int(connection.execute(
            "SELECT COUNT(*) FROM observations WHERE source_kind='x:focal'"
        ).fetchone()[0])
        reasons = {
            str(row[0] or "other"): int(row[1])
            for row in connection.execute(
                "SELECT last_error_class,COUNT(*) FROM targets "
                "WHERE state='unavailable' GROUP BY last_error_class"
            )
        }
        private = sum(
            value for key, value in reasons.items()
            if key in {"private", "protected", "auth_required"}
        )
        deleted = sum(value for key, value in reasons.items() if "deleted" in key)
        suspended = sum(
            value for key, value in reasons.items() if "suspend" in key
        )
        other = max(
            0,
            states.get("unavailable", 0) - private - deleted - suspended,
        )
    unavailable = states.get("unavailable", 0)
    return {
        "context_captured": states.get("captured", 0),
        "context_parents_saved": parents,
        "context_unavailable": unavailable,
        "context_manual_review": states.get("manual_review", 0),
        "context_known_remaining": sum(
            states.get(name, 0) for name in ("pending", "leased", "retryable")
        ),
        "context_media_actionable": sum(
            media.get(name, 0)
            for name in ("pending", "leased", "retryable", "needs_refresh")
        ),
        "context_media_captured": media.get("captured", 0),
        "context_media_unavailable": media.get("unavailable", 0),
        "context_media_manual_review": media.get("manual_review", 0),
        "context_media_remaining": sum(
            media.get(name, 0)
            for name in (
                "pending", "leased", "retryable", "needs_refresh",
                "manual_review",
            )
        ),
        "boundaries_deleted": deleted,
        "boundaries_private": private,
        "boundaries_suspended": suspended,
        "boundaries_other": other,
    }


def collect_context_fast_metrics(db_path: Path) -> dict[str, int]:
    """Read live queue counters without running the chain-closure query."""
    connection = _open_context(db_path)
    try:
        return _context_fast_metrics(connection)
    finally:
        connection.close()


def _context_closure_metrics(connection: sqlite3.Connection) -> dict[str, int]:
    counters = _maintained_counters(connection)
    closure_names = (
        "fully_captured",
        "unavailable_boundary",
        "retry_delayed",
        "pending",
        "manual_review",
    )
    if counters is not None and all(
        f"conversations_state_{state}" in counters for state in closure_names
    ):
        closure = {
            state: counters[f"conversations_state_{state}"]
            for state in closure_names
        }
        return {
            "conversations_closed": (
                closure["fully_captured"] + closure["unavailable_boundary"]
            ),
            "conversations_total": sum(closure.values()),
        }
    closure = {
        "fully_captured": 0, "unavailable_boundary": 0,
        "retry_delayed": 0, "pending": 0, "manual_review": 0,
    }
    for row in connection.execute(
        """SELECT COALESCE(e.conversation_id,e.child_id) AS chain_id,
                  SUM(t.state='unavailable') AS unavailable,
                  SUM(t.state='retryable') AS retryable,
                  SUM(t.state IN ('pending','leased')) AS pending,
                  SUM(t.state='manual_review') AS manual
             FROM reply_edges e JOIN targets t ON t.post_id=e.parent_id
            GROUP BY COALESCE(e.conversation_id,e.child_id)"""
    ):
        if row["manual"]:
            closure["manual_review"] += 1
        elif row["pending"]:
            closure["pending"] += 1
        elif row["retryable"]:
            closure["retry_delayed"] += 1
        elif row["unavailable"]:
            closure["unavailable_boundary"] += 1
        else:
            closure["fully_captured"] += 1
    return {
        "conversations_closed": (
            closure["fully_captured"] + closure["unavailable_boundary"]
        ),
        "conversations_total": sum(closure.values()),
    }


def collect_context_metrics(db_path: Path) -> dict[str, int]:
    """Collect all authoritative aggregates without taking a write lock."""
    connection = _open_context(db_path)
    try:
        result = _context_fast_metrics(connection)
        result.update(_context_closure_metrics(connection))
        return result
    finally:
        connection.close()


def queue_progress_values(
    totals: dict[str, int], phase: str
) -> tuple[int, int]:
    if phase == "context_media":
        return (
            totals["context_media_actionable"],
            totals["context_media_captured"]
            + totals["context_media_unavailable"]
            + totals["context_media_manual_review"],
        )
    return (
        totals["context_known_remaining"],
        totals["context_captured"] + totals["context_unavailable"],
    )


def infer_media_phase_started_at(
    samples: list[dict[str, int | float]],
) -> float | None:
    """Infer an old producer's metadata-to-media handoff timestamp."""
    saw_metadata_work = False
    for sample in samples:
        remaining = int(sample["known_remaining"])
        if remaining > 0:
            saw_metadata_work = True
        elif saw_metadata_work:
            return float(sample["at"])
    return None


def collect_media_terminal_since(db_path: Path, since: float) -> int:
    connection = _open_context(db_path)
    try:
        counters = _maintained_counters(connection)
        if counters is not None and counters.get("asset_jobs_total", 0):
            return int(connection.execute(
                "SELECT COUNT(*) FROM asset_jobs "
                "WHERE state IN ('captured','unavailable','manual_review') "
                "AND updated_at>=?",
                (_timestamp(since),),
            ).fetchone()[0])
        return int(connection.execute(
            "SELECT COUNT(*) FROM targets "
            "WHERE media_state IN ('captured','unavailable','manual_review') "
            "AND updated_at>=?",
            (_timestamp(since),),
        ).fetchone()[0])
    finally:
        connection.close()


DATASET_COUNT_KEYS = ("dataset_posts", "posts", "total_posts")
MEDIA_FILE_KEYS = ("media_files", "files", "file_count")
MEDIA_BYTE_KEYS = ("media_bytes", "bytes", "total_bytes")


def _metric_block(
    source: dict[str, Any], names: tuple[str, ...], keys: tuple[str, ...]
) -> dict[str, Any]:
    for name in names:
        value = source.get(name)
        if isinstance(value, dict) and any(key in value for key in keys):
            return value
    return {}


def _latest_manifest(user_dir: Path) -> dict[str, Any]:
    candidates = list((user_dir / "runs").glob("*/manifest.json"))
    if not candidates:
        return {}
    result: dict[str, Any] = {}

    def merge(name: str, value: dict[str, Any], keys: tuple[str, ...]) -> None:
        if not value:
            return
        target = result.setdefault(name, {})
        for key in keys:
            if key in value:
                target[key] = max(
                    int(target.get(key) or 0), int(value.get(key) or 0)
                )

    def modified(path: Path) -> int:
        try:
            return path.stat().st_mtime_ns
        except OSError:
            return 0

    for path in sorted(candidates, key=modified, reverse=True):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(value, dict):
            continue
        direct = _metric_block(
            value, ("dataset", "post_dataset"), DATASET_COUNT_KEYS
        )
        merge("post_dataset", direct, DATASET_COUNT_KEYS)
        # A legacy run records the authoritative merged-dataset total on each
        # committed window instead of at manifest top level.
        for window in reversed(value.get("windows") or []):
            if not isinstance(window, dict):
                continue
            nested = window.get("dataset")
            if (
                window.get("state_committed")
                and isinstance(nested, dict)
                and any(key in nested for key in DATASET_COUNT_KEYS)
            ):
                merge("post_dataset", nested, DATASET_COUNT_KEYS)
                break
        direct_media = _metric_block(
            value,
            ("media", "media_dataset"),
            MEDIA_FILE_KEYS + MEDIA_BYTE_KEYS,
        )
        merge(
            "media_dataset", direct_media, MEDIA_FILE_KEYS + MEDIA_BYTE_KEYS
        )
    return result


def collect_archive_metrics(
    user_dir: Path, modern_result: dict[str, Any] | None = None
) -> dict[str, int]:
    db_path = user_dir / "_state" / "context.sqlite3"
    indexed: dict[str, int] | None = None
    if db_path.is_file():
        connection = _open_context(db_path)
        try:
            counters = _maintained_counters(connection)
            ready = connection.execute(
                "SELECT 1 FROM current_pointers "
                "WHERE pointer_name='local_history_reconciled'"
            ).fetchone()
            if counters is not None and ready is not None:
                indexed = {
                    "archive_posts": counters.get("archive_posts_total", 0),
                    "archive_media_files": counters.get("archive_media_files", 0),
                    "archive_media_bytes": counters.get("archive_media_bytes", 0),
                }
        except sqlite3.Error:
            indexed = None
        finally:
            connection.close()
    if indexed is not None:
        if modern_result:
            post_block = _metric_block(
                modern_result, ("dataset", "post_dataset"), DATASET_COUNT_KEYS
            )
            media_block = _metric_block(
                modern_result,
                ("media", "media_dataset"),
                MEDIA_FILE_KEYS + MEDIA_BYTE_KEYS,
            )
            indexed["archive_posts"] = max(
                indexed["archive_posts"],
                *(int(post_block.get(key) or 0) for key in DATASET_COUNT_KEYS),
            )
            indexed["archive_media_files"] = max(
                indexed["archive_media_files"],
                *(int(media_block.get(key) or 0) for key in MEDIA_FILE_KEYS),
            )
            indexed["archive_media_bytes"] = max(
                indexed["archive_media_bytes"],
                *(int(media_block.get(key) or 0) for key in MEDIA_BYTE_KEYS),
            )
        return indexed
    sources = [_latest_manifest(user_dir)]
    if modern_result:
        sources.append(modern_result)
    datasets = [
        _metric_block(source, ("dataset", "post_dataset"), DATASET_COUNT_KEYS)
        for source in sources
    ]
    media_sets = [
        _metric_block(
            source,
            ("media", "media_dataset"),
            MEDIA_FILE_KEYS + MEDIA_BYTE_KEYS,
        )
        for source in sources
    ]

    def maximum(values: list[dict[str, Any]], names: tuple[str, ...]) -> int:
        return max(
            (
                int(value.get(name) or 0)
                for value in values
                for name in names
            ),
            default=0,
        )

    return {
        "archive_posts": maximum(datasets, DATASET_COUNT_KEYS),
        "archive_media_files": maximum(media_sets, MEDIA_FILE_KEYS),
        "archive_media_bytes": maximum(media_sets, MEDIA_BYTE_KEYS),
    }


def collect_export_metrics(db_path: Path) -> dict[str, int]:
    """Read constant-size durable/export generation state without payload I/O."""
    result = {
        "archive_durable_generation": 0,
        "archive_exported_generation": 0,
        "archive_dirty_views": 0,
    }
    connection = _open_context(db_path)
    try:
        try:
            generation = connection.execute(
                "SELECT current_generation FROM archive_generation WHERE singleton=1"
            ).fetchone()
            pointer = connection.execute(
                """SELECT generation FROM current_pointers
                     WHERE pointer_name='portable_export'"""
            ).fetchone()
            dirty = connection.execute(
                """SELECT COUNT(*) FROM export_views
                     WHERE status<>'current'
                        OR durable_generation<>exported_generation"""
            ).fetchone()
        except sqlite3.Error:
            return result
        result.update(
            {
                "archive_durable_generation": (
                    int(generation[0]) if generation is not None else 0
                ),
                "archive_exported_generation": (
                    int(pointer[0]) if pointer is not None else 0
                ),
                "archive_dirty_views": int(dirty[0]) if dirty is not None else 0,
            }
        )
    finally:
        connection.close()
    return result


RATE_RESET_RE = re.compile(r"Archive rate-limit reset=(\d+)")


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def _legacy_manifest(user_dir: Path, owner_run_id: str | None) -> tuple[
    dict[str, Any], Path | None
]:
    if owner_run_id and re.fullmatch(r"[A-Za-z0-9_-]+", owner_run_id):
        path = user_dir / "runs" / owner_run_id / "manifest.json"
        value = _read_object(path)
        if value.get("mode") == "legacy_backfill":
            return value, path
    candidates = list((user_dir / "runs").glob("*/manifest.json"))

    def modified(path: Path) -> int:
        try:
            return path.stat().st_mtime_ns
        except OSError:
            return 0

    candidates.sort(key=modified, reverse=True)
    for path in candidates:
        value = _read_object(path)
        if value.get("mode") == "legacy_backfill":
            return value, path
    return {}, None


def _legacy_runtime_signals(
    run_dir: Path | None, *, now: float, window_id: str | None = None
) -> tuple[str | None, float | None]:
    """Return a future rate reset and latest observable worker activity."""
    if run_dir is None or not run_dir.is_dir():
        return None, None
    latest_activity: float | None = None
    future_resets: list[int] = []
    pattern = f"{window_id}-*.log" if window_id else "*.log"
    for path in run_dir.glob(pattern):
        try:
            stat = path.stat()
            latest_activity = max(latest_activity or 0, stat.st_mtime)
            with path.open("rb") as stream:
                stream.seek(max(0, stat.st_size - 131072))
                tail = stream.read().decode("utf-8", errors="replace")
        except OSError:
            continue
        future_resets.extend(
            int(match)
            for match in RATE_RESET_RE.findall(tail)
            if now < int(match) <= now + 6 * 3600
        )
    reset = max(future_resets, default=None)
    return (_timestamp(reset) if reset is not None else None), latest_activity


def collect_legacy_metrics(
    user_dir: Path, *, now: float | None = None
) -> dict[str, Any] | None:
    """Summarize the durable date frontier and current-run wall-clock rate."""
    current = time.time() if now is None else now
    state_path = user_dir / "_state" / "state.json"
    state = _read_object(state_path)
    legacy = state.get("legacy_backfill")
    if not isinstance(legacy, dict):
        return None
    initial_text = legacy.get("initial_until")
    next_text = legacy.get("next_until")
    floor_text = legacy.get("floor_since")
    initial = parse_time(initial_text)
    frontier = parse_time(next_text)
    floor = parse_time(floor_text)
    if None in {initial, frontier, floor} or not floor <= frontier <= initial:
        return None
    active = legacy.get("active_window")
    active = active if isinstance(active, dict) else {}
    owner = str(active.get("owner_run_id") or "") or None
    manifest, manifest_path = _legacy_manifest(user_dir, owner)
    committed_windows = 0
    committed_posts = 0
    run_coverage = 0.0
    seen_windows: set[tuple[str, str]] = set()
    for window in manifest.get("windows") or []:
        if not isinstance(window, dict) or not window.get("state_committed"):
            continue
        since_text = str(window.get("since") or "")
        until_text = str(window.get("until") or "")
        key = (since_text, until_text)
        if key in seen_windows:
            continue
        since, until = parse_time(since_text), parse_time(until_text)
        if since is None or until is None or until <= since:
            continue
        seen_windows.add(key)
        committed_windows += 1
        run_coverage += until - since
        committed_posts += int(
            window.get("canonical_post_count")
            or (window.get("dataset") or {}).get("new_run_posts")
            or 0
        )
    started = parse_time(manifest.get("started_at"))
    elapsed = max(0.0, current - started) if started is not None else 0.0
    coverage_rate = (
        run_coverage / 86400 / elapsed * 3600
        if run_coverage > 0 and elapsed >= 1
        else None
    )
    remaining = max(0.0, frontier - floor)
    eta = (
        int(remaining / run_coverage * elapsed)
        if run_coverage > 0 and elapsed >= 1
        else None
    )
    if committed_windows >= 20 and elapsed >= 3600:
        confidence = "high"
    elif committed_windows >= 5 and elapsed >= 1200:
        confidence = "medium"
    elif committed_windows:
        confidence = "low"
    else:
        confidence = "none"
    wait_until, log_mtime = _legacy_runtime_signals(
        manifest_path.parent if manifest_path else None,
        now=current,
        window_id=str(active.get("window_id") or "") or None,
    )
    mtimes = [value for value in (log_mtime,) if value is not None]
    for path in (state_path, manifest_path):
        if path is None:
            continue
        try:
            mtimes.append(path.stat().st_mtime)
        except OSError:
            pass
    last_progress = _timestamp(max(mtimes)) if mtimes else None
    total = max(0.0, initial - floor)
    completed = max(0.0, initial - frontier)
    return {
        "status": str(legacy.get("status") or "pending"),
        "initial_until": str(initial_text),
        "next_until": str(next_text),
        "floor_since": str(floor_text),
        "active_since": active.get("since"),
        "active_until": active.get("until"),
        "total_seconds": int(total),
        "completed_seconds": int(completed),
        "remaining_seconds": int(remaining),
        "percent": round(completed / total * 100, 1) if total else 100.0,
        "committed_windows": committed_windows,
        "committed_posts": committed_posts,
        "elapsed_seconds": int(elapsed),
        "coverage_days_per_hour": (
            round(coverage_rate, 1) if coverage_rate is not None else None
        ),
        "eta_seconds": eta,
        "confidence": confidence,
        "wait_until": wait_until,
        "last_progress_at": last_progress,
    }


def legacy_estimate(
    legacy: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    remaining_days = math.ceil(int(legacy["remaining_seconds"]) / 86400)
    estimate = {
        "seconds": legacy.get("eta_seconds"),
        "label": (
            f"~{human_duration(legacy['eta_seconds'])}"
            if legacy.get("eta_seconds") is not None
            else None
        ),
        "confidence": str(legacy.get("confidence") or "none"),
        "qualifier": (
            "account-creation boundary"
            if legacy.get("eta_seconds") is not None
            else "waiting for first committed legacy window"
        ),
        "known_remaining": remaining_days,
    }
    coverage_rate = legacy.get("coverage_days_per_hour")
    rate = None
    if coverage_rate is not None:
        rate = {
            "coverage_days_per_hour": coverage_rate,
            "window_seconds": int(legacy.get("elapsed_seconds") or 0),
            "unit": "archive_days_per_hour",
        }
    return estimate, rate


def legacy_activity(legacy: dict[str, Any]) -> str:
    status = str(legacy.get("status") or "pending")
    if status == "complete":
        return "legacy coverage reached account creation"
    if status == "manual_review":
        return "legacy window needs review"
    since = str(legacy.get("active_since") or "")[:10]
    until = str(legacy.get("active_until") or "")[:10]
    if since and until:
        return f"verifying legacy window {since} to {until}"
    frontier = str(legacy.get("next_until") or "")[:10]
    return f"preparing legacy window before {frontier}" if frontier else (
        "preparing next legacy window"
    )


def empty_totals() -> dict[str, int]:
    return {key: 0 for key in sorted(ALLOWED_TOTALS)}


def collect_user_totals(
    archive_root: Path,
    handle: str,
    modern_result: dict[str, Any] | None = None,
    *,
    include_context_closure: bool = True,
) -> dict[str, int]:
    user_dir = archive_root / "users" / handle
    totals = empty_totals()
    totals.update(collect_archive_metrics(user_dir, modern_result))
    db_path = user_dir / "_state" / "context.sqlite3"
    if db_path.is_file():
        try:
            totals.update(collect_export_metrics(db_path))
        except sqlite3.Error:
            pass
        totals.update(
            collect_context_metrics(db_path)
            if include_context_closure
            else collect_context_fast_metrics(db_path)
        )
    return totals


def derive_health(
    *, status: str, phase_status: str = "running",
    action_required: int = 0, wait_until: str | None = None,
    last_progress_at: str | None = None, now: float | None = None,
    stale_after: float = 900,
) -> str:
    if status == "failed" or phase_status == "failed":
        return "failed"
    if phase_status in {"manual_review", "blocked"}:
        return "blocked"
    if phase_status in {"retrying", "retryable"}:
        return "retrying"
    current = time.time() if now is None else now
    waiting = parse_time(wait_until)
    if waiting is not None and waiting > current:
        return "waiting"
    progress = parse_time(last_progress_at)
    if status == "running" and progress is not None and current - progress > stale_after:
        return "stale"
    return "healthy"


def estimate_known_queue(
    samples: list[dict[str, int | float]], known_remaining: int,
    *, blocked: bool = False,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    estimate = {
        "seconds": None, "label": None, "confidence": "none",
        "qualifier": "collecting samples", "known_remaining": known_remaining,
    }
    if len(samples) < 2:
        if blocked:
            estimate["qualifier"] = "phase blocked"
        return estimate, None
    first, last = samples[0], samples[-1]
    elapsed = float(last["at"]) - float(first["at"])
    resolved = int(last["resolved"]) - int(first["resolved"])
    burn = int(first["known_remaining"]) - int(last["known_remaining"])
    # Five minutes plus 20 completed targets is enough for a deliberately
    # low-confidence first estimate. Confidence rises with observation time.
    required_resolved = min(20, max(2, known_remaining))
    if elapsed < 300 or resolved < required_resolved:
        if blocked:
            estimate["qualifier"] = "phase blocked"
        return estimate, None
    gross_rate = resolved / elapsed * 3600
    rate = {
        "items_per_hour": round(gross_rate, 1),
        "window_seconds": int(elapsed),
    }
    if blocked:
        estimate["qualifier"] = "phase blocked"
        return estimate, rate
    if burn <= 0:
        estimate["qualifier"] = "still discovering"
        return estimate, rate
    net_rate = burn / elapsed
    seconds = known_remaining / net_rate if net_rate else math.inf
    confidence = "medium" if elapsed >= 3600 else "low"
    if elapsed >= 4 * 3600:
        confidence = "high"
    estimate.update({
        "seconds": int(seconds), "label": f"~{human_duration(seconds)}",
        "confidence": confidence, "qualifier": "known queue",
    })
    return estimate, rate


def phase_queue_samples(
    samples: list[dict[str, int | float]], phase: str
) -> list[dict[str, int | float]]:
    """Discard pre-seed observations when metadata queue work begins."""
    copied = [dict(sample) for sample in samples]
    if phase != "context_metadata" or not copied:
        return copied
    # Old producers did not tag samples with their phase. A zero-to-positive
    # transition is the durable signature of context seeding. Keep at most the
    # two newest post-seed producer samples so a dashboard restart cannot fold
    # hours of modern/legacy runtime into the context rate.
    last_zero = -1
    for index, sample in enumerate(copied[:-1]):
        if int(sample["known_remaining"]) == 0:
            last_zero = index
    post_seed = copied[last_zero + 1:]
    return post_seed[-2:]


def _check_keys(value: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = set(value) - allowed
    if unknown:
        raise ProgressError(f"unknown {where} fields: {sorted(unknown)}")
    secret = [key for key in value if SECRET_KEY.search(key)]
    if secret:
        raise ProgressError(f"sensitive {where} fields are forbidden")


def validate_snapshot(snapshot: dict[str, Any]) -> None:
    if not isinstance(snapshot, dict):
        raise ProgressError("snapshot must be an object")
    _check_keys(snapshot, ALLOWED_TOP, "snapshot")
    if snapshot.get("schema") != SCHEMA or snapshot.get("schema_version") != 1:
        raise ProgressError("unsupported progress schema")
    if not isinstance(snapshot.get("users"), list):
        raise ProgressError("users must be a list")
    for user in snapshot["users"]:
        if not isinstance(user, dict):
            raise ProgressError("user must be an object")
        _check_keys(user, ALLOWED_USER, "user")
        handle = user.get("handle")
        if not isinstance(handle, str) or not re.fullmatch(r"[A-Za-z0-9_]{1,50}", handle):
            raise ProgressError("invalid handle")
        for name in ("totals", "baseline", "delta"):
            value = user.get(name)
            if not isinstance(value, dict):
                raise ProgressError(f"{name} must be an object")
            _check_keys(value, ALLOWED_TOTALS, name)
            if any(not isinstance(item, int) for item in value.values()):
                raise ProgressError(f"{name} values must be integers")
        phases = user.get("phases")
        if not isinstance(phases, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in phases.items()
        ):
            raise ProgressError("phases must map names to statuses")
        if set(phases) - ALLOWED_PHASES:
            raise ProgressError("snapshot contains an unknown phase")
        if set(phases.values()) - ALLOWED_PHASE_STATUSES:
            raise ProgressError("snapshot contains an unknown phase status")
        if user.get("phase") not in ALLOWED_PHASES:
            raise ProgressError("snapshot contains an unknown active phase")
        for sample in user.get("samples", []):
            _check_keys(sample, ALLOWED_SAMPLE, "sample")
        if user.get("rate") is not None:
            _check_keys(user["rate"], ALLOWED_RATE, "rate")
        _check_keys(user.get("estimate", {}), ALLOWED_ESTIMATE, "estimate")
        legacy = user.get("legacy")
        if legacy is not None:
            if not isinstance(legacy, dict):
                raise ProgressError("legacy progress must be an object")
            _check_keys(legacy, ALLOWED_LEGACY, "legacy")
            required = {
                "status", "initial_until", "next_until", "floor_since",
                "total_seconds", "completed_seconds", "remaining_seconds",
                "percent", "committed_windows", "committed_posts",
                "elapsed_seconds", "confidence",
            }
            if not required.issubset(legacy):
                raise ProgressError("legacy progress is incomplete")


def atomic_write_snapshot(path: Path, snapshot: dict[str, Any]) -> None:
    validate_snapshot(snapshot)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(snapshot, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


@dataclass
class LiveProgressReader:
    """Overlay cheap authoritative signals on producer snapshots.

    The producer remains the source of phase transitions and activity labels.
    This reader makes durable counters/frontiers live even while an older or
    blocked producer is inside a long-running phase.
    """

    archive_root: Path
    refresh_seconds: float = 5
    closure_seconds: float = 60
    clock: Callable[[], float] = time.time
    _last_refresh: dict[str, float] = field(default_factory=dict)
    _last_closure: dict[str, float] = field(default_factory=dict)
    _closure: dict[str, dict[str, int]] = field(default_factory=dict)
    _overlay: dict[str, dict[str, Any]] = field(default_factory=dict)
    _samples: dict[str, list[dict[str, int | float]]] = field(
        default_factory=dict
    )
    _sample_phase: dict[str, str] = field(default_factory=dict)

    def _context_activity(self, db_path: Path) -> str | None:
        mtimes = []
        for path in (db_path, Path(f"{db_path}-wal")):
            try:
                mtimes.append(path.stat().st_mtime)
            except OSError:
                pass
        return _timestamp(max(mtimes)) if mtimes else None

    def _seed_phase_samples(
        self,
        source: dict[str, Any],
        totals: dict[str, int],
        db_path: Path,
        *,
        phase: str,
        now: float,
    ) -> list[dict[str, int | float]]:
        raw = [dict(sample) for sample in source.get("samples", [])]
        if phase != "context_media":
            return phase_queue_samples(raw, phase)
        actionable, resolved = queue_progress_values(totals, phase)
        if raw:
            last_remaining = int(raw[-1]["known_remaining"])
            tolerance = max(50, round(actionable * 0.25))
            if last_remaining > 0 and abs(last_remaining - actionable) <= tolerance:
                return raw[-2:]
        started = infer_media_phase_started_at(raw)
        if started is None or not db_path.is_file():
            return []
        started = max(started, now - 4 * 3600)
        try:
            terminal_since = collect_media_terminal_since(db_path, started)
        except (OSError, ValueError, sqlite3.Error):
            return []
        return [{
            "at": int(started),
            "known_remaining": actionable + terminal_since,
            "resolved": max(0, resolved - terminal_since),
        }]

    def _refresh_user(
        self, source: dict[str, Any], *, now: float
    ) -> dict[str, Any]:
        handle = str(source["handle"])
        user_dir = self.archive_root / "users" / handle
        totals = empty_totals()
        totals.update(source["totals"])
        totals.update(collect_archive_metrics(user_dir))
        db_path = user_dir / "_state" / "context.sqlite3"
        context_activity = None
        if db_path.is_file():
            try:
                totals.update(collect_export_metrics(db_path))
                totals.update(collect_context_fast_metrics(db_path))
                context_activity = self._context_activity(db_path)
                if handle not in self._closure:
                    self._closure[handle] = {
                        key: int(source["totals"].get(key) or 0)
                        for key in (
                            "conversations_closed", "conversations_total"
                        )
                    }
                    self._last_closure[handle] = now
                elif now - self._last_closure[handle] >= self.closure_seconds:
                    connection = _open_context(db_path)
                    try:
                        self._closure[handle] = _context_closure_metrics(
                            connection
                        )
                    finally:
                        connection.close()
                    self._last_closure[handle] = now
                totals.update(self._closure[handle])
            except (OSError, ValueError, sqlite3.Error):
                # A context DB can exist briefly before seeding has created
                # every table. Keep the independently valid archive overlay.
                pass

        phase = str(source["phase"])
        if self._sample_phase.get(handle) != phase:
            self._samples[handle] = self._seed_phase_samples(
                source, totals, db_path, phase=phase, now=now
            )
            self._sample_phase[handle] = phase
        samples = self._samples[handle]
        known_remaining, resolved = queue_progress_values(totals, phase)
        sample = {
            "at": int(now),
            "known_remaining": known_remaining,
            "resolved": resolved,
        }
        if not samples or samples[-1] != sample:
            samples.append(sample)
        cutoff = now - 4 * 3600
        samples[:] = [item for item in samples if item["at"] >= cutoff][-2881:]

        phase_status = str(source["phases"].get(phase, "running"))
        last_progress = source.get("last_progress_at")
        if (
            parse_time(context_activity) is not None
            and (
                parse_time(last_progress) is None
                or parse_time(context_activity) > parse_time(last_progress)
            )
        ):
            last_progress = context_activity
        overlay: dict[str, Any] = {
            "phase": phase,
            "totals": totals,
            "samples": samples,
            "activity": source.get("activity"),
            "last_progress_at": last_progress,
            "wait_until": source.get("wait_until"),
        }
        legacy = collect_legacy_metrics(user_dir, now=now)
        if legacy is not None:
            overlay["legacy"] = legacy
        if phase == "legacy" and legacy is not None:
            overlay["estimate"], overlay["rate"] = legacy_estimate(legacy)
            overlay["activity"] = legacy_activity(legacy)
            overlay["wait_until"] = legacy.get("wait_until")
            observed = legacy.get("last_progress_at")
            if (
                parse_time(observed) is not None
                and (
                    parse_time(last_progress) is None
                    or parse_time(observed) > parse_time(last_progress)
                )
            ):
                overlay["last_progress_at"] = observed
            if legacy.get("status") in {"manual_review", "complete"}:
                phase_status = str(legacy["status"])
        else:
            overlay["estimate"], overlay["rate"] = estimate_known_queue(
                samples,
                known_remaining,
                blocked=phase_status in {"manual_review", "blocked", "failed"},
            )
        overlay["health"] = derive_health(
            status="running", phase_status=phase_status,
            action_required=int(source.get("action_required") or 0),
            wait_until=overlay.get("wait_until"),
            last_progress_at=overlay.get("last_progress_at"), now=now,
        )
        return overlay

    def enrich(
        self, snapshot: dict[str, Any], *, now: float | None = None
    ) -> dict[str, Any]:
        validate_snapshot(snapshot)
        current = self.clock() if now is None else now
        result = copy.deepcopy(snapshot)
        observed = False
        for user in result["users"]:
            handle = str(user["handle"])
            cached = self._overlay.get(handle)
            phase_changed = cached is not None and cached.get("phase") != user["phase"]
            if (
                cached is None
                or phase_changed
                or current - self._last_refresh.get(handle, 0)
                >= self.refresh_seconds
            ):
                try:
                    cached = self._refresh_user(user, now=current)
                except (OSError, ValueError, sqlite3.Error):
                    cached = self._overlay.get(handle)
                else:
                    self._overlay[handle] = cached
                    self._last_refresh[handle] = current
            if cached is None:
                continue
            observed = True
            for key in (
                "totals", "samples", "activity", "last_progress_at",
                "wait_until", "estimate", "rate", "health", "legacy",
            ):
                if key in cached:
                    user[key] = copy.deepcopy(cached[key])
            baseline = empty_totals()
            baseline.update(user["baseline"])
            user["baseline"] = baseline
            user["delta"] = {
                key: int(user["totals"][key]) - int(baseline[key])
                for key in ALLOWED_TOTALS
            }
            user["action_required"] = (
                user["totals"]["context_manual_review"]
                + sum(
                    1 for value in user["phases"].values()
                    if value in {"manual_review", "blocked"}
                )
            )
        if observed:
            result["updated_at"] = _timestamp(current)
        validate_snapshot(result)
        return result


@dataclass
class ProgressTracker:
    path: Path
    archive_root: Path
    invocation_id: str
    handles: list[str]
    started_at: str
    clock: Callable[[], float] = time.time
    # The production-sized closure query is intentionally producer-side and
    # slow-cadence; the renderer can reread the resulting JSON every second.
    refresh_seconds: float = 300
    status: str = "running"
    users: dict[str, dict[str, Any]] = field(default_factory=dict)
    _last_refresh: float = 0

    def __post_init__(self) -> None:
        now = self.clock()
        for handle in self.handles:
            totals = collect_user_totals(self.archive_root, handle)
            legacy = collect_legacy_metrics(
                self.archive_root / "users" / handle, now=now
            )
            resolved = totals["context_captured"] + totals["context_unavailable"]
            user = {
                "handle": handle, "phase": "starting", "health": "healthy",
                "activity": "validating archive state", "last_progress_at": utc_now(),
                "wait_until": None, "phases": {}, "totals": totals,
                "baseline": dict(totals), "delta": empty_totals(),
                "samples": [{
                    "at": int(now),
                    "known_remaining": totals["context_known_remaining"],
                    "resolved": resolved,
                }],
                "rate": None, "estimate": {
                    "seconds": None, "label": None, "confidence": "none",
                    "qualifier": "collecting samples",
                    "known_remaining": totals["context_known_remaining"],
                },
                "action_required": totals["context_manual_review"],
            }
            if legacy is not None:
                user["legacy"] = legacy
            self.users[handle] = user
        self._last_refresh = now
        self.write()

    def event(
        self, handle: str, *, phase: str | None = None,
        phase_status: str | None = None, activity: str | None = None,
        progress: bool = False, wait_until: str | None = None,
        force: bool = False,
    ) -> None:
        user = self.users[handle]
        previous_phase = user["phase"]
        if phase:
            user["phase"] = phase
        if phase in {"context_metadata", "context_media"} and phase != previous_phase:
            totals = user["totals"]
            known_remaining, resolved = queue_progress_values(totals, phase)
            user["samples"] = [{
                "at": int(self.clock()),
                "known_remaining": known_remaining,
                "resolved": resolved,
            }]
            user["rate"] = None
            user["estimate"] = {
                "seconds": None, "label": None, "confidence": "none",
                "qualifier": "collecting samples",
                "known_remaining": known_remaining,
            }
        if phase and phase_status:
            user["phases"][phase] = phase_status
        if activity:
            user["activity"] = activity[:120]
        if progress:
            user["last_progress_at"] = utc_now()
        user["wait_until"] = wait_until
        self.refresh(force=force)

    def refresh(self, *, force: bool = False) -> None:
        now = self.clock()
        full = force or now - self._last_refresh >= self.refresh_seconds
        initial = self._last_refresh == 0
        if full:
            for handle, user in self.users.items():
                totals = collect_user_totals(self.archive_root, handle)
                legacy = collect_legacy_metrics(
                    self.archive_root / "users" / handle, now=now
                )
                if legacy is not None:
                    user["legacy"] = legacy
                user["totals"] = totals
                user["delta"] = {
                    key: totals[key] - user["baseline"][key]
                    for key in ALLOWED_TOTALS
                }
                known_remaining, resolved = queue_progress_values(
                    totals, user["phase"]
                )
                user["samples"].append({
                    "at": int(now),
                    "known_remaining": known_remaining,
                    "resolved": resolved,
                })
                cutoff = now - 4 * 3600
                user["samples"] = [
                    sample for sample in user["samples"]
                    if sample["at"] >= cutoff
                ][-241:]
                user["action_required"] = (
                    totals["context_manual_review"]
                    + sum(
                        1 for value in user["phases"].values()
                        if value in {"manual_review", "blocked"}
                    )
                )
                phase_status = user["phases"].get(user["phase"], "running")
                if user["phase"] == "legacy" and legacy is not None:
                    estimate, rate = legacy_estimate(legacy)
                    user["wait_until"] = legacy.get("wait_until")
                    observed = legacy.get("last_progress_at")
                    if (
                        parse_time(observed) is not None
                        and (
                            parse_time(user.get("last_progress_at")) is None
                            or parse_time(observed)
                            > parse_time(user.get("last_progress_at"))
                        )
                    ):
                        user["last_progress_at"] = observed
                    user["activity"] = legacy_activity(legacy)
                    if legacy.get("status") in {"manual_review", "complete"}:
                        phase_status = str(legacy["status"])
                        user["phases"]["legacy"] = phase_status
                else:
                    estimate, rate = estimate_known_queue(
                        user["samples"],
                        known_remaining,
                        blocked=phase_status in {
                            "manual_review", "blocked", "failed"
                        },
                    )
                user["estimate"], user["rate"] = estimate, rate
                user["health"] = derive_health(
                    status=self.status, phase_status=phase_status,
                    action_required=user["action_required"],
                    wait_until=user["wait_until"],
                    last_progress_at=user["last_progress_at"], now=now,
                )
            self._last_refresh = now
        self.write()
        if full and not initial and not os.environ.get("TMUX"):
            for user in self.users.values():
                totals = user["totals"]
                print(
                    f"@{user['handle']} {user['phase']} {user['health']}: "
                    f"{human_number(totals['context_parents_saved'])} parents "
                    f"saved, {human_number(totals['context_known_remaining'])} "
                    "known remaining."
                )

    def finalize(self, status: str) -> None:
        self.status = status
        self.refresh(force=True)

    def snapshot(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA, "schema_version": SCHEMA_VERSION,
            "invocation_id": self.invocation_id, "started_at": self.started_at,
            "updated_at": utc_now(), "status": self.status,
            "users": [self.users[handle] for handle in self.handles],
        }

    def write(self) -> None:
        atomic_write_snapshot(self.path, self.snapshot())


def create_tracker(
    *args: Any, **kwargs: Any
) -> SafeProgressTracker | NullProgressTracker:
    try:
        return SafeProgressTracker(ProgressTracker(*args, **kwargs))
    except Exception as exc:
        print(f"Dashboard unavailable: {type(exc).__name__}: {exc}")
        return NullProgressTracker()


def start_tmux_dashboard(
    snapshot_path: Path | None,
    invocation_id: str,
    repo_dir: Path,
    *,
    environ: dict[str, str] | None = None,
    isatty: bool | None = None,
    terminal_size: os.terminal_size | None = None,
    runner: Callable[..., Any] = subprocess.run,
) -> str | None:
    """Create one provably owned pane; fail closed and never affect the worker."""
    env = os.environ if environ is None else environ
    if (
        snapshot_path is None
        or not env.get("TMUX")
        or env.get("ARCHIVE_X_DASHBOARD", "auto").lower() in {"0", "off", "false"}
        or not (os.isatty(1) if isatty is None else isatty)
    ):
        return None
    size = terminal_size or shutil.get_terminal_size((80, 24))
    if (
        size.columns < DASHBOARD_MIN_COLUMNS
        or size.lines < DASHBOARD_MIN_LINES
    ):
        return None
    command = [
        "tmux", "split-window", "-v", "-d", "-l",
        str(DASHBOARD_PANE_LINES), "-P", "-F",
        "#{pane_id}", "-c", str(repo_dir),
        str(repo_dir / "scripts" / "archive-x-dashboard"),
        str(snapshot_path), "--watch", "--exit-when-final",
    ]
    try:
        result = runner(
            command, check=True, capture_output=True, text=True, timeout=5
        )
        pane_id = str(result.stdout).strip()
        if not re.fullmatch(r"%\d+", pane_id):
            return None
        runner(
            [
                "tmux", "select-pane", "-t", pane_id, "-T",
                f"archive-x-dashboard:{invocation_id}",
            ],
            check=True, capture_output=True, text=True, timeout=5,
        )
        return pane_id
    except (OSError, subprocess.SubprocessError):
        return None
