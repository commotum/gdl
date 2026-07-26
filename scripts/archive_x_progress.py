#!/usr/bin/env python3
"""Read-only archive progress signals and atomic producer snapshots.

The archive engines remain authoritative.  This module only summarizes their
durable state; the renderer never opens the context database.
"""

from __future__ import annotations

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
    "action_required",
}
ALLOWED_TOTALS = {
    "archive_posts", "archive_media_files", "archive_media_bytes",
    "context_captured", "context_parents_saved", "context_unavailable",
    "context_manual_review", "context_known_remaining",
    "context_media_remaining", "conversations_closed",
    "conversations_total", "boundaries_deleted", "boundaries_private",
    "boundaries_suspended", "boundaries_other",
}
ALLOWED_SAMPLE = {"at", "known_remaining", "resolved"}
ALLOWED_RATE = {"items_per_hour", "window_seconds"}
ALLOWED_ESTIMATE = {
    "seconds", "label", "confidence", "qualifier", "known_remaining",
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
    "skipped_diagnostic", "skipped_retry_only",
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


def collect_context_metrics(db_path: Path) -> dict[str, int]:
    """Collect bounded authoritative aggregates without taking a write lock."""
    uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
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
        private = sum(
            value for key, value in reasons.items()
            if key in {"private", "protected", "auth_required"}
        )
        deleted = sum(
            value for key, value in reasons.items() if "deleted" in key
        )
        suspended = sum(
            value for key, value in reasons.items() if "suspend" in key
        )
        unavailable = states.get("unavailable", 0)
        classified = private + deleted + suspended
        closed = closure["fully_captured"] + closure["unavailable_boundary"]
        return {
            "context_captured": states.get("captured", 0),
            "context_parents_saved": parents,
            "context_unavailable": unavailable,
            "context_manual_review": states.get("manual_review", 0),
            "context_known_remaining": sum(
                states.get(name, 0) for name in ("pending", "leased", "retryable")
            ),
            "context_media_remaining": sum(
                media.get(name, 0)
                for name in ("pending", "leased", "retryable", "manual_review")
            ),
            "conversations_closed": closed,
            "conversations_total": sum(closure.values()),
            "boundaries_deleted": deleted,
            "boundaries_private": private,
            "boundaries_suspended": suspended,
            "boundaries_other": max(0, unavailable - classified),
        }
    finally:
        connection.close()


def _latest_manifest(user_dir: Path) -> dict[str, Any]:
    candidates = list((user_dir / "runs").glob("*/manifest.json"))
    if not candidates:
        return {}
    result: dict[str, Any] = {}
    for path in sorted(
        candidates, key=lambda item: item.stat().st_mtime_ns, reverse=True
    ):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(value, dict):
            continue
        for key in ("post_dataset", "media_dataset"):
            if key not in result and isinstance(value.get(key), dict):
                result[key] = value[key]
        if len(result) == 2:
            break
    return result


def collect_archive_metrics(
    user_dir: Path, modern_result: dict[str, Any] | None = None
) -> dict[str, int]:
    source = modern_result or _latest_manifest(user_dir)
    dataset = source.get("dataset") or source.get("post_dataset") or {}
    media = source.get("media") or source.get("media_dataset") or {}
    return {
        "archive_posts": int(
            dataset.get("dataset_posts") or dataset.get("posts")
            or dataset.get("total_posts") or 0
        ),
        "archive_media_files": int(
            media.get("media_files") or media.get("files")
            or media.get("file_count") or 0
        ),
        "archive_media_bytes": int(
            media.get("media_bytes") or media.get("bytes")
            or media.get("total_bytes") or 0
        ),
    }


def empty_totals() -> dict[str, int]:
    return {key: 0 for key in sorted(ALLOWED_TOTALS)}


def collect_user_totals(
    archive_root: Path, handle: str, modern_result: dict[str, Any] | None = None
) -> dict[str, int]:
    user_dir = archive_root / "users" / handle
    totals = empty_totals()
    totals.update(collect_archive_metrics(user_dir, modern_result))
    db_path = user_dir / "_state" / "context.sqlite3"
    if db_path.is_file():
        totals.update(collect_context_metrics(db_path))
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
    if elapsed < 600 or resolved < 20:
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
            resolved = totals["context_captured"] + totals["context_unavailable"]
            self.users[handle] = {
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
        self._last_refresh = now
        self.write()

    def event(
        self, handle: str, *, phase: str | None = None,
        phase_status: str | None = None, activity: str | None = None,
        progress: bool = False, wait_until: str | None = None,
        force: bool = False,
    ) -> None:
        user = self.users[handle]
        if phase:
            user["phase"] = phase
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
                user["totals"] = totals
                user["delta"] = {
                    key: totals[key] - user["baseline"][key]
                    for key in ALLOWED_TOTALS
                }
                resolved = (
                    totals["context_captured"] + totals["context_unavailable"]
                )
                user["samples"].append({
                    "at": int(now),
                    "known_remaining": totals["context_known_remaining"],
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
                estimate, rate = estimate_known_queue(
                    user["samples"],
                    totals["context_known_remaining"],
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
    if size.columns < 80 or size.lines < 24:
        return None
    command = [
        "tmux", "split-window", "-v", "-b", "-l", "9", "-P", "-F",
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
