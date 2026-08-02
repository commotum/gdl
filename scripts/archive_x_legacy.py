#!/usr/bin/env python3
"""Fail-closed, date-windowed backfill for pre-Snowflake X history."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import archive_x
import archive_x_context as context_x
import archive_x_descriptors as descriptor_x
import archive_x_pacing as pacing_x


LEGACY_SCHEMA_VERSION = 1
MODERN_HEAD_SCHEMA_VERSION = 1
LEGACY_STATUSES = {"pending", "active", "manual_review", "complete"}
TOKEN_RE = re.compile(r"[0-9a-f]{64}\Z")
SHA256_RE = TOKEN_RE
CURSOR_RE = re.compile(r"3_(\d+)/\Z")
LEGACY_TERMINAL_REASONS = {"no_cursor", "distinct_empty_tail"}
DEFAULT_ROOT_WINDOW_DAYS = 3
DEFAULT_EMPTY_TAIL_PAGES = 2
MIN_ROOT_WINDOW_SECONDS = 24 * 60 * 60
MAX_ROOT_WINDOW_SECONDS = 90 * 24 * 60 * 60
MAX_RETAINED_OBSERVATIONS_PER_LEAF = 8
MAX_PENDING_PORTABLE_EXPORTS = 10_000
LEGACY_POLICY_SCHEMA_VERSION = 1
# Twitter's documented Snowflake epoch. Returned metadata before this instant
# is evidence about the ID domain only; it is never used to paginate legacy IDs.
SNOWFLAKE_EPOCH = datetime(2010, 11, 4, 1, 42, 54, 657000, tzinfo=timezone.utc)


def _relative_evidence_path(value: Any, field: str) -> str:
    text = str(value or "")
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts:
        raise archive_x.ArchiveError(f"legacy {field} path is invalid")
    return text


def _positive_counter(value: Any, field: str, *, allow_zero: bool = True) -> int:
    minimum = 0 if allow_zero else 1
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise archive_x.ArchiveError(f"legacy {field} counter is invalid")
    return value


def validate_window_policy(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("schema_version") != (
        LEGACY_POLICY_SCHEMA_VERSION
    ):
        raise archive_x.ArchiveError("legacy adaptive window policy is invalid")
    required = {
        "schema_version",
        "next_window_seconds",
        "minimum_seconds",
        "maximum_seconds",
        "last_decision",
    }
    if not required <= set(value):
        raise archive_x.ArchiveError("legacy adaptive window policy is incomplete")
    minimum = _positive_counter(
        value["minimum_seconds"], "adaptive minimum", allow_zero=False
    )
    maximum = _positive_counter(
        value["maximum_seconds"], "adaptive maximum", allow_zero=False
    )
    next_seconds = _positive_counter(
        value["next_window_seconds"], "adaptive next window", allow_zero=False
    )
    if (
        minimum != MIN_ROOT_WINDOW_SECONDS
        or maximum != MAX_ROOT_WINDOW_SECONDS
        or not minimum <= next_seconds <= maximum
    ):
        raise archive_x.ArchiveError("legacy adaptive window bounds changed")
    if value["last_decision"] not in {
        "initial",
        "sparse_grow",
        "dense_shrink",
        "steady",
        "floor_clip",
    }:
        raise archive_x.ArchiveError("legacy adaptive decision is invalid")
    for field in (
        "last_search_requests",
        "last_api_requests",
        "last_post_count",
        "last_leaf_count",
    ):
        if field in value:
            _positive_counter(value[field], field)
    last_window_id = value.get("last_window_id")
    if last_window_id is not None and (
        not isinstance(last_window_id, str)
        or not last_window_id
        or len(last_window_id) > 80
    ):
        raise archive_x.ArchiveError("legacy adaptive last window is invalid")
    return value


def new_window_policy(seconds: int) -> dict[str, Any]:
    bounded = max(
        MIN_ROOT_WINDOW_SECONDS, min(MAX_ROOT_WINDOW_SECONDS, int(seconds))
    )
    return {
        "schema_version": LEGACY_POLICY_SCHEMA_VERSION,
        "next_window_seconds": bounded,
        "minimum_seconds": MIN_ROOT_WINDOW_SECONDS,
        "maximum_seconds": MAX_ROOT_WINDOW_SECONDS,
        "last_decision": "initial",
    }


@dataclass(frozen=True)
class LegacyRunOptions:
    cookies: Path
    max_root_windows: int | None = None
    request_limit: int = 6
    root_window_days: int = DEFAULT_ROOT_WINDOW_DAYS
    empty_tail_pages: int = DEFAULT_EMPTY_TAIL_PAGES
    walk_attempts: int = 3
    window_attempts: int = 3
    max_leaves: int = 64
    request_delay: str = "4-8"
    walk_delay: str = "10-20"
    window_delay: str = "5-15"
    retries: int = 1
    http_timeout: int = 60
    stalled_rate_limit_cycles: int = 3

    def validate(self) -> "LegacyRunOptions":
        if self.max_root_windows is not None and self.max_root_windows < 1:
            raise archive_x.ArchiveError("legacy root-window limit must be positive")
        for name in (
            "request_limit",
            "root_window_days",
            "empty_tail_pages",
            "walk_attempts",
            "window_attempts",
            "max_leaves",
            "retries",
            "http_timeout",
            "stalled_rate_limit_cycles",
        ):
            if not isinstance(getattr(self, name), int) or getattr(self, name) < 1:
                raise archive_x.ArchiveError(f"legacy {name} must be positive")
        if self.walk_attempts < 2:
            raise archive_x.ArchiveError(
                "legacy run requires at least two walk attempts"
            )
        if self.empty_tail_pages >= self.request_limit:
            raise archive_x.ArchiveError(
                "legacy empty-tail pages must be below the request limit"
            )
        for value in (self.request_delay, self.walk_delay, self.window_delay):
            archive_x.parse_duration(value)
        return self

    @classmethod
    def from_namespace(cls, args: argparse.Namespace) -> "LegacyRunOptions":
        return cls(
            cookies=args.cookies,
            max_root_windows=args.windows,
            request_limit=args.request_limit,
            root_window_days=args.root_window_days,
            empty_tail_pages=args.empty_tail_pages,
            walk_attempts=args.walk_attempts,
            window_attempts=args.window_attempts,
            max_leaves=args.max_leaves,
            request_delay=args.request_delay,
            walk_delay=args.walk_delay,
            window_delay=args.window_delay,
            retries=args.retries,
            http_timeout=args.http_timeout,
            stalled_rate_limit_cycles=args.stalled_rate_limit_cycles,
        ).validate()


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def parse_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise archive_x.ArchiveError(f"legacy {field} must be a UTC Z timestamp")
    try:
        result = archive_x.parse_datetime(value)
    except argparse.ArgumentTypeError as exc:
        raise archive_x.ArchiveError(f"invalid legacy {field}: {value!r}") from exc
    if result.microsecond:
        raise archive_x.ArchiveError(
            f"legacy {field} must use whole-second precision"
        )
    return result


def second_utc(value: datetime) -> str:
    value = value.astimezone(timezone.utc).replace(microsecond=0)
    return value.isoformat().replace("+00:00", "Z")


def parse_modern_timestamp(value: Any, field: str) -> datetime:
    """Parse normal archive timestamps without legacy-window truncation."""
    if not isinstance(value, str) or not value.endswith("Z"):
        raise archive_x.ArchiveError(
            f"legacy modern head {field} must be a UTC Z timestamp"
        )
    try:
        return archive_x.parse_datetime(value)
    except argparse.ArgumentTypeError as exc:
        raise archive_x.ArchiveError(
            f"invalid legacy modern head {field}: {value!r}"
        ) from exc


def require_sha256(value: Any, field: str) -> str:
    text = str(value or "")
    if not SHA256_RE.fullmatch(text):
        raise archive_x.ArchiveError(f"legacy {field} must be a SHA-256 digest")
    return text


def validate_source(source: Any, expected_user_id: str | None = None) -> None:
    if not isinstance(source, dict):
        raise archive_x.ArchiveError("legacy source provenance is missing")
    required = {
        "run_id",
        "manifest_sha256",
        "state_sha256_before_init",
        "cursor",
        "oldest_post_id",
        "oldest_post_at",
        "dataset_post_count",
        "reposts_included",
        "confirmation_token",
    }
    if not required.issubset(source):
        raise archive_x.ArchiveError("legacy source provenance is incomplete")
    require_sha256(source["manifest_sha256"], "source manifest hash")
    require_sha256(source["state_sha256_before_init"], "source state hash")
    require_sha256(source["confirmation_token"], "confirmation token")
    cursor_match = CURSOR_RE.fullmatch(str(source["cursor"]))
    if not cursor_match or cursor_match.group(1) != str(source["oldest_post_id"]):
        raise archive_x.ArchiveError(
            "legacy source cursor and oldest post ID do not match"
        )
    parse_utc(source["oldest_post_at"], "source oldest_post_at")
    if not isinstance(source["dataset_post_count"], int) or source[
        "dataset_post_count"
    ] < 1:
        raise archive_x.ArchiveError(
            "legacy source dataset_post_count must be positive"
        )
    if not isinstance(source["reposts_included"], bool):
        raise archive_x.ArchiveError("legacy source repost policy is invalid")
    confirmations = source.get("transition_confirmations", [])
    if not isinstance(confirmations, list):
        raise archive_x.ArchiveError(
            "legacy source transition confirmations must be a list"
        )
    seen_runs: set[str] = set()
    for confirmation in confirmations:
        if not isinstance(confirmation, dict):
            raise archive_x.ArchiveError(
                "legacy source transition confirmation is invalid"
            )
        required_confirmation = {
            "run_id",
            "manifest_sha256",
            "raw_sha256",
            "cursor",
            "stalled_rate_limit_cycles",
        }
        if not required_confirmation.issubset(confirmation):
            raise archive_x.ArchiveError(
                "legacy source transition confirmation is incomplete"
            )
        run_id = str(confirmation.get("run_id") or "")
        if not run_id or Path(run_id).name != run_id or run_id in seen_runs:
            raise archive_x.ArchiveError(
                "legacy source transition confirmation run is invalid"
            )
        seen_runs.add(run_id)
        require_sha256(
            confirmation["manifest_sha256"],
            "transition confirmation manifest hash",
        )
        require_sha256(
            confirmation["raw_sha256"],
            "transition confirmation raw hash",
        )
        if confirmation["cursor"] != source["cursor"]:
            raise archive_x.ArchiveError(
                "legacy source transition confirmation cursor changed"
            )
        cycles = confirmation["stalled_rate_limit_cycles"]
        if not isinstance(cycles, int) or cycles < 1:
            raise archive_x.ArchiveError(
                "legacy source transition confirmation cycle count is invalid"
            )
    if expected_user_id is not None and not expected_user_id.isdecimal():
        raise archive_x.ArchiveError("legacy expected numeric account ID is invalid")


def validate_retained_observation(
    value: Any, *, leaf_since: str, leaf_until: str
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise archive_x.ArchiveError("legacy retained observation is invalid")
    required = {
        "observation_id",
        "archive_run_id",
        "walk_id",
        "since",
        "until",
        "query_sha256",
        "compatibility_sha256",
        "raw_path",
        "raw_sha256",
        "telemetry_path",
        "telemetry_sha256",
        "request_limit",
        "empty_tail_pages",
        "search_requests",
        "api_requests",
        "accepted_count",
    }
    if not required <= set(value):
        raise archive_x.ArchiveError("legacy retained observation is incomplete")
    for field in (
        "observation_id",
        "query_sha256",
        "compatibility_sha256",
        "raw_sha256",
        "telemetry_sha256",
    ):
        require_sha256(value[field], f"retained {field}")
    if value["since"] != leaf_since or value["until"] != leaf_until:
        raise archive_x.ArchiveError("legacy retained observation bounds changed")
    for field in ("archive_run_id", "walk_id"):
        text = str(value.get(field) or "")
        if not text or len(text) > 200 or "\x00" in text:
            raise archive_x.ArchiveError(
                f"legacy retained observation {field} is invalid"
            )
    _relative_evidence_path(value["raw_path"], "retained raw")
    _relative_evidence_path(value["telemetry_path"], "retained telemetry")
    request_limit = _positive_counter(
        value["request_limit"], "retained request limit", allow_zero=False
    )
    empty_tail = _positive_counter(
        value["empty_tail_pages"], "retained empty tail", allow_zero=False
    )
    if empty_tail >= request_limit:
        raise archive_x.ArchiveError("legacy retained request proof is invalid")
    for field in ("search_requests", "api_requests", "accepted_count"):
        _positive_counter(value[field], f"retained {field}")
    if value["api_requests"] < value["search_requests"]:
        raise archive_x.ArchiveError("legacy retained API counters are inconsistent")
    descriptor_path = value.get("descriptor_artifact_path")
    descriptor_hash = value.get("descriptor_artifact_sha256")
    if (descriptor_path is None) != (descriptor_hash is None):
        raise archive_x.ArchiveError(
            "legacy retained descriptor evidence is incomplete"
        )
    if descriptor_path is not None:
        _relative_evidence_path(descriptor_path, "retained descriptor")
        require_sha256(descriptor_hash, "retained descriptor hash")
    return value


def validate_pending_exports(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > MAX_PENDING_PORTABLE_EXPORTS:
        raise archive_x.ArchiveError("legacy pending portable exports are invalid")
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, dict) or not {
            "window_id",
            "since",
            "until",
            "canonical_raw_path",
            "canonical_raw_sha256",
            "indexed_generation",
        } <= set(item):
            raise archive_x.ArchiveError(
                "legacy pending portable export is incomplete"
            )
        window = str(item.get("window_id") or "")
        if not window or window in seen:
            raise archive_x.ArchiveError(
                "legacy pending portable export identity is invalid"
            )
        seen.add(window)
        since = parse_utc(item["since"], "pending export since")
        until = parse_utc(item["until"], "pending export until")
        if not since < until:
            raise archive_x.ArchiveError(
                "legacy pending portable export bounds are invalid"
            )
        if window_id(item["since"], item["until"]) != window:
            raise archive_x.ArchiveError(
                "legacy pending portable export bounds changed"
            )
        _relative_evidence_path(
            item["canonical_raw_path"], "pending canonical raw"
        )
        require_sha256(
            item["canonical_raw_sha256"], "pending canonical raw hash"
        )
        _positive_counter(
            item["indexed_generation"],
            "pending indexed generation",
            allow_zero=False,
        )
    return value


def validate_active_window(active: Any, floor: datetime, initial: datetime) -> None:
    if not isinstance(active, dict):
        raise archive_x.ArchiveError("legacy active_window must be an object")
    required = {"window_id", "since", "until", "owner_run_id", "attempt", "leaves"}
    if not required.issubset(active):
        raise archive_x.ArchiveError("legacy active_window is incomplete")
    since = parse_utc(active["since"], "active_window.since")
    until = parse_utc(active["until"], "active_window.until")
    if not floor <= since < until <= initial:
        raise archive_x.ArchiveError("legacy active_window bounds are invalid")
    if not isinstance(active["attempt"], int) or active["attempt"] < 1:
        raise archive_x.ArchiveError("legacy active_window attempt is invalid")
    leaves = active["leaves"]
    if not isinstance(leaves, list) or not leaves:
        raise archive_x.ArchiveError("legacy active_window leaves are missing")
    expected = since
    observation_ids: set[str] = set()
    observation_paths: set[str] = set()
    for leaf in leaves:
        if not isinstance(leaf, dict) or not {"since", "until", "status"} <= set(leaf):
            raise archive_x.ArchiveError("legacy active leaf is malformed")
        if set(leaf) - {"since", "until", "status", "observations"}:
            raise archive_x.ArchiveError("legacy active leaf fields are invalid")
        leaf_since = parse_utc(leaf["since"], "active leaf since")
        leaf_until = parse_utc(leaf["until"], "active leaf until")
        if leaf_since != expected or not leaf_since < leaf_until:
            raise archive_x.ArchiveError("legacy active leaves are not contiguous")
        if leaf["status"] not in {"pending", "confirmed"}:
            raise archive_x.ArchiveError("legacy active leaf status is invalid")
        observations = leaf.get("observations", [])
        if (
            not isinstance(observations, list)
            or len(observations) > MAX_RETAINED_OBSERVATIONS_PER_LEAF
        ):
            raise archive_x.ArchiveError(
                "legacy active leaf observation count is invalid"
            )
        for observation in observations:
            validate_retained_observation(
                observation,
                leaf_since=leaf["since"],
                leaf_until=leaf["until"],
            )
            observation_id = observation["observation_id"]
            raw_path = observation["raw_path"]
            if observation_id in observation_ids or raw_path in observation_paths:
                raise archive_x.ArchiveError(
                    "legacy retained observation is duplicated"
                )
            observation_ids.add(observation_id)
            observation_paths.add(raw_path)
        expected = leaf_until
    if expected != until:
        raise archive_x.ArchiveError("legacy active leaves do not cover the window")


def validate_legacy_state(
    legacy: Any, *, expected_user_id: str | None = None
) -> dict[str, Any]:
    if not isinstance(legacy, dict):
        raise archive_x.ArchiveError("legacy_backfill must be an object")
    if legacy.get("schema_version") != LEGACY_SCHEMA_VERSION:
        raise archive_x.ArchiveError(
            "unsupported legacy_backfill schema version: "
            f"{legacy.get('schema_version')!r}"
        )
    status = legacy.get("status")
    if status not in LEGACY_STATUSES:
        raise archive_x.ArchiveError(f"invalid legacy_backfill status: {status!r}")
    user_id = str(legacy.get("requested_user_id") or "")
    if not user_id.isdecimal():
        raise archive_x.ArchiveError("legacy requested_user_id must be numeric")
    if expected_user_id is not None and user_id != str(expected_user_id):
        raise archive_x.ArchiveError("legacy numeric account identity changed")
    validate_source(legacy.get("source"), user_id)
    parse_utc(legacy.get("initialized_at"), "initialized_at")
    initial = parse_utc(legacy.get("initial_until"), "initial_until")
    frontier = parse_utc(legacy.get("next_until"), "next_until")
    floor = parse_utc(legacy.get("floor_since"), "floor_since")
    if not floor <= frontier <= initial:
        raise archive_x.ArchiveError("legacy frontier order is invalid")
    active = legacy.get("active_window")
    if status == "active":
        validate_active_window(active, floor, initial)
        if parse_utc(active["until"], "active_window.until") != frontier:
            raise archive_x.ArchiveError(
                "legacy active window does not begin at the frontier"
            )
    elif active is not None:
        raise archive_x.ArchiveError(
            "legacy active_window must be null outside active status"
        )
    conclusion = legacy.get("coverage_conclusion")
    if conclusion not in {
        "in_progress",
        "source_visible_to_account_creation",
        "source_unavailable_before",
    }:
        raise archive_x.ArchiveError("legacy coverage conclusion is invalid")
    if status == "complete" and (
        frontier != floor or conclusion != "source_visible_to_account_creation"
    ):
        raise archive_x.ArchiveError("legacy complete state lacks full frontier proof")
    if status != "complete" and conclusion == "source_visible_to_account_creation":
        raise archive_x.ArchiveError("legacy completion conclusion is premature")
    manual = legacy.get("manual_review")
    if status == "manual_review" and not isinstance(manual, dict):
        raise archive_x.ArchiveError("legacy manual-review evidence is missing")
    if status != "manual_review" and manual is not None:
        raise archive_x.ArchiveError("legacy manual_review must otherwise be null")
    policy = legacy.get("window_policy")
    if policy is not None:
        validate_window_policy(policy)
    validate_pending_exports(legacy.get("pending_portable_exports"))
    return legacy


def oldest_dataset_record(user_dir: Path) -> tuple[dict[str, Any], int]:
    oldest = None
    oldest_at = None
    count = 0
    for record in archive_x.iter_jsonl(user_dir / "dataset" / "posts.jsonl"):
        count += 1
        value = record.get("posted_at")
        try:
            posted = archive_x.parse_datetime(str(value))
        except argparse.ArgumentTypeError:
            continue
        if oldest_at is None or posted < oldest_at:
            oldest = record
            oldest_at = posted
    if oldest is None or oldest_at is None:
        raise archive_x.ArchiveError("cannot derive a valid oldest dataset post")
    return oldest, count


def _matching_timeline(manifest: dict[str, Any], cursor: str) -> dict[str, Any] | None:
    return next(
        (
            item
            for item in manifest.get("endpoints", ())
            if isinstance(item, dict)
            and item.get("endpoint") == "timeline"
            and item.get("resume_cursor") == cursor
        ),
        None,
    )


def _transition_run_evidence(
    user_dir: Path,
    manifest_path: Path,
    *,
    cursor: str,
    oldest_post_id: str,
    oldest_post_at: str,
    requested_user_id: str,
    require_unchanged_window: bool,
) -> dict[str, Any] | None:
    """Return immutable evidence for one clean run at an exact boundary."""
    manifest = archive_x.load_json(manifest_path, None)
    if not isinstance(manifest, dict):
        return None
    manifest_status = manifest.get("status")
    if manifest_status not in {"stalled", "interrupted"}:
        return None
    if (
        manifest.get("limited_run") is not False
        or manifest.get("retry_failed_only") is not False
        or manifest.get("date_after") not in {None, ""}
    ):
        return None
    if (
        manifest_status == "stalled"
        and manifest.get("failure_stage") != "timeline_no_progress_watchdog"
    ):
        return None
    timeline = _matching_timeline(manifest, cursor)
    if not isinstance(timeline, dict):
        return None
    timeline_status = timeline.get("status")
    if timeline_status not in {"stalled", "interrupted"}:
        return None
    coherent_stop = (
        timeline_status == "stalled"
        and timeline.get("stalled") is True
        and timeline.get("interrupted") is False
    ) or (
        timeline_status == "interrupted"
        and timeline.get("interrupted") is True
        and timeline.get("stalled") in {None, False}
    )
    if not coherent_stop or manifest_status != timeline_status:
        return None
    if (
        timeline.get("metadata_complete") is not False
        or timeline.get("raw_has_record") is not True
        or timeline.get("other_error_count") != 0
        or not isinstance(timeline.get("exit_code"), int)
        or timeline.get("exit_code") == 0
    ):
        return None
    cycles = timeline.get("stalled_rate_limit_cycles")
    if not isinstance(cycles, int) or cycles < 0:
        return None
    if require_unchanged_window and cycles < 1:
        return None

    log_path = manifest_path.parent / "timeline.log"
    try:
        log_text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        log_text = ""
    if (
        "[twitter][warning] API errors" in log_text
        or "[twitter][error]" in log_text
        or "Unable to retrieve Tweets" in log_text
    ):
        return None

    raw_relative = str(timeline.get("raw_path") or "")
    raw_path = (user_dir / raw_relative).resolve()
    runs_dir = (user_dir / "runs").resolve()
    if (
        not raw_relative
        or not raw_path.is_file()
        or runs_dir not in raw_path.parents
        or raw_path.name.endswith(".tmp")
        or archive_x.synthetic_search_cursor(raw_path) != cursor
    ):
        return None
    boundary = next(
        (
            record
            for record in archive_x.iter_jsonl(raw_path)
            if archive_x.id_string(record.get("tweet_id")) == oldest_post_id
        ),
        None,
    )
    if not isinstance(boundary, dict):
        return None
    try:
        boundary_at = archive_x.parse_datetime(str(boundary.get("date") or ""))
        expected_at = parse_utc(oldest_post_at, "source oldest_post_at")
    except (argparse.ArgumentTypeError, archive_x.ArchiveError):
        return None
    if second_utc(boundary_at) != second_utc(expected_at):
        return None
    boundary_user_id = archive_x.id_string((boundary.get("user") or {}).get("id"))
    boundary_author_id = archive_x.id_string(
        (boundary.get("author") or {}).get("id")
    )
    if requested_user_id not in {boundary_user_id, boundary_author_id}:
        return None

    run_id = str(manifest.get("run_id") or manifest_path.parent.name)
    if not run_id or Path(run_id).name != run_id:
        return None
    return {
        "run_id": run_id,
        "manifest_sha256": archive_x.sha256_file(manifest_path),
        "raw_sha256": archive_x.sha256_file(raw_path),
        "cursor": cursor,
        "stalled_rate_limit_cycles": cycles,
        "completed_at": str(manifest.get("completed_at") or ""),
    }


def transition_confirmations(
    user_dir: Path,
    *,
    cursor: str,
    oldest_post_id: str,
    oldest_post_at: str,
    requested_user_id: str,
) -> list[dict[str, Any]]:
    """Collect clean, independent no-progress observations at one boundary."""
    confirmations = []
    for manifest_path in sorted((user_dir / "runs").glob("*/manifest.json")):
        evidence = _transition_run_evidence(
            user_dir,
            manifest_path,
            cursor=cursor,
            oldest_post_id=oldest_post_id,
            oldest_post_at=oldest_post_at,
            requested_user_id=requested_user_id,
            require_unchanged_window=True,
        )
        if evidence is not None:
            confirmations.append(evidence)
    confirmations.sort(key=lambda item: (item["completed_at"], item["run_id"]))
    return confirmations


def transition_watchdog_policy(
    user_dir: Path,
    state: dict[str, Any],
    *,
    ambiguous_cycles: int,
) -> dict[str, Any]:
    """Use one unchanged window only for an identity-bound legacy-era floor."""
    if not isinstance(ambiguous_cycles, int) or ambiguous_cycles < 1:
        raise archive_x.ArchiveError("transition watchdog cycle limit is invalid")
    default = {
        "cycles": ambiguous_cycles,
        "reason": "ambiguous_or_snowflake_boundary",
    }
    if state.get("legacy_backfill") is not None:
        return {"cycles": ambiguous_cycles, "reason": "legacy_already_initialized"}
    requested_user_id = str(state.get("requested_user_id") or "")
    resume = state.get("resume")
    cursor = str(resume.get("cursor") or "") if isinstance(resume, dict) else ""
    match = CURSOR_RE.fullmatch(cursor)
    if not requested_user_id.isdecimal() or not match:
        return default
    try:
        oldest, _count = oldest_dataset_record(user_dir)
        oldest_at = archive_x.parse_datetime(str(oldest.get("posted_at") or ""))
    except (archive_x.ArchiveError, argparse.ArgumentTypeError):
        return default
    if (
        str(oldest.get("post_id") or "") != match.group(1)
        or oldest_at >= SNOWFLAKE_EPOCH
    ):
        return default
    profile_wrapper = archive_x.load_json(
        user_dir / "dataset" / "profile.json", None
    )
    profile = (
        profile_wrapper.get("profile")
        if isinstance(profile_wrapper, dict)
        else None
    )
    if (
        not isinstance(profile, dict)
        or str(profile.get("id") or "") != requested_user_id
    ):
        return default
    return {
        "cycles": 1,
        "reason": "verified_pre_snowflake_floor",
        "cursor": cursor,
        "oldest_post_id": match.group(1),
        "oldest_post_at": second_utc(oldest_at),
    }


def matching_source_manifest(user_dir: Path, cursor: str) -> Path:
    matches: list[tuple[str, Path]] = []
    for path in (user_dir / "runs").glob("*/manifest.json"):
        manifest = archive_x.load_json(path, None)
        if not isinstance(manifest, dict):
            continue
        timeline = next(
            (
                item
                for item in manifest.get("endpoints", ())
                if isinstance(item, dict)
                and item.get("endpoint") == "timeline"
                and item.get("resume_cursor") == cursor
                and item.get("status") in {"stalled", "failed", "interrupted"}
            ),
            None,
        )
        if timeline is not None:
            matches.append((str(manifest.get("completed_at") or ""), path))
    if not matches:
        raise archive_x.ArchiveError(
            "no stopped timeline manifest matches the saved cursor"
        )
    return max(matches)[1]


def initialization_plan(user_dir: Path) -> dict[str, Any]:
    state_path = user_dir / "_state" / "state.json"
    state = archive_x.load_json(state_path, None)
    if not isinstance(state, dict):
        raise archive_x.ArchiveError("archive state is missing or invalid")
    if state.get("legacy_backfill") is not None:
        legacy = validate_legacy_state(
            state["legacy_backfill"],
            expected_user_id=str(state.get("requested_user_id") or ""),
        )
        return {"already_initialized": True, "legacy_backfill": legacy}
    if state.get("schema") != archive_x.SCHEMA_NAME or state.get(
        "schema_version"
    ) != archive_x.SCHEMA_VERSION:
        raise archive_x.ArchiveError("archive state schema is not supported")
    user_id = str(state.get("requested_user_id") or "")
    if not user_id.isdecimal():
        raise archive_x.ArchiveError("archive state lacks a numeric account ID")
    resume = state.get("resume")
    cursor = str(resume.get("cursor") or "") if isinstance(resume, dict) else ""
    cursor_match = CURSOR_RE.fullmatch(cursor)
    if not cursor_match:
        raise archive_x.ArchiveError(
            "archive state lacks a stage-3 boundary cursor"
        )
    oldest, dataset_count = oldest_dataset_record(user_dir)
    oldest_id = str(oldest.get("post_id") or "")
    if oldest_id != cursor_match.group(1):
        raise archive_x.ArchiveError(
            "saved cursor does not match the oldest dataset post"
        )
    try:
        oldest_at = archive_x.parse_datetime(str(oldest.get("posted_at") or ""))
    except argparse.ArgumentTypeError as exc:
        raise archive_x.ArchiveError("oldest dataset timestamp is invalid") from exc
    profile_wrapper = archive_x.load_json(
        user_dir / "dataset" / "profile.json", None
    )
    profile = (
        profile_wrapper.get("profile")
        if isinstance(profile_wrapper, dict)
        else None
    )
    if not isinstance(profile, dict) or str(profile.get("id") or "") != user_id:
        raise archive_x.ArchiveError("profile identity does not match archive state")
    try:
        floor = archive_x.parse_datetime(str(profile.get("date") or ""))
    except argparse.ArgumentTypeError as exc:
        raise archive_x.ArchiveError("profile creation timestamp is invalid") from exc
    floor = floor.replace(microsecond=0)
    initial_until = oldest_at.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + timedelta(days=1)
    manifest_path = matching_source_manifest(user_dir, cursor)
    manifest = archive_x.load_json(manifest_path, {})
    evidence = {
        "run_id": str(manifest.get("run_id") or manifest_path.parent.name),
        "manifest_sha256": archive_x.sha256_file(manifest_path),
        "state_sha256_before_init": archive_x.sha256_file(state_path),
        "cursor": cursor,
        "oldest_post_id": oldest_id,
        "oldest_post_at": second_utc(oldest_at),
        "dataset_post_count": dataset_count,
        "reposts_included": bool(manifest.get("reposts_included")),
    }
    confirmations = transition_confirmations(
        user_dir,
        cursor=cursor,
        oldest_post_id=oldest_id,
        oldest_post_at=second_utc(oldest_at),
        requested_user_id=user_id,
    )
    if confirmations:
        evidence["transition_confirmations"] = confirmations
    proposed = {
        "requested_user_id": user_id,
        "initial_until": second_utc(initial_until),
        "next_until": second_utc(initial_until),
        "floor_since": second_utc(floor),
    }
    token = canonical_sha256({"source": evidence, "proposed": proposed})
    evidence["confirmation_token"] = token
    return {
        "already_initialized": False,
        "confirmation_token": token,
        "initialization_command": (
            "scripts/archive-x-legacy --user "
            f"{state.get('canonical_handle') or state.get('requested_handle')} "
            f"init --token {token}"
        ),
        "source": evidence,
        "proposed": proposed,
    }


def initialize_state(
    state: dict[str, Any], plan: dict[str, Any], token: str, initialized_at: str
) -> tuple[dict[str, Any], bool]:
    current = state.get("legacy_backfill")
    if current is not None:
        validate_legacy_state(
            current, expected_user_id=str(state.get("requested_user_id") or "")
        )
        if current["source"]["confirmation_token"] != token:
            raise archive_x.ArchiveError(
                "legacy backfill is already initialized with different evidence"
            )
        return copy.deepcopy(state), False
    if not TOKEN_RE.fullmatch(token) or token != plan.get("confirmation_token"):
        raise archive_x.ArchiveError(
            "legacy initialization token is stale or incorrect"
        )
    proposed = plan["proposed"]
    legacy = {
        "schema_version": LEGACY_SCHEMA_VERSION,
        "status": "pending",
        "requested_user_id": proposed["requested_user_id"],
        "source": copy.deepcopy(plan["source"]),
        "initialized_at": second_utc(parse_utc(initialized_at, "initialized_at")),
        "initial_until": proposed["initial_until"],
        "next_until": proposed["next_until"],
        "floor_since": proposed["floor_since"],
        "active_window": None,
        "last_completed_window": None,
        "coverage_conclusion": "in_progress",
        "manual_review": None,
    }
    validate_legacy_state(
        legacy, expected_user_id=str(state.get("requested_user_id") or "")
    )
    updated = copy.deepcopy(state)
    updated["legacy_backfill"] = legacy
    return updated, True


def _legacy_source_manifest(
    user_dir: Path, source: dict[str, Any]
) -> tuple[Path, dict[str, Any]]:
    run_id_value = str(source.get("run_id") or "")
    if not run_id_value or Path(run_id_value).name != run_id_value:
        raise archive_x.ArchiveError("legacy source run ID is invalid")
    path = user_dir / "runs" / run_id_value / "manifest.json"
    runs_dir = (user_dir / "runs").resolve()
    resolved = path.resolve()
    if not resolved.is_file() or runs_dir not in resolved.parents:
        raise archive_x.ArchiveError("legacy source manifest is missing")
    if archive_x.sha256_file(resolved) != source.get("manifest_sha256"):
        raise archive_x.ArchiveError("legacy source manifest hash changed")
    manifest = archive_x.load_json(resolved, None)
    if not isinstance(manifest, dict):
        raise archive_x.ArchiveError("legacy source manifest is invalid")
    if str(manifest.get("run_id") or resolved.parent.name) != run_id_value:
        raise archive_x.ArchiveError("legacy source manifest run ID changed")
    for confirmation in source.get("transition_confirmations", ()):
        confirmation_run = str(confirmation["run_id"])
        confirmation_manifest = (
            user_dir / "runs" / confirmation_run / "manifest.json"
        ).resolve()
        if (
            not confirmation_manifest.is_file()
            or runs_dir not in confirmation_manifest.parents
            or archive_x.sha256_file(confirmation_manifest)
            != confirmation["manifest_sha256"]
        ):
            raise archive_x.ArchiveError(
                "legacy transition confirmation manifest changed"
            )
        confirmation_value = archive_x.load_json(confirmation_manifest, None)
        if not isinstance(confirmation_value, dict):
            raise archive_x.ArchiveError(
                "legacy transition confirmation manifest is invalid"
            )
        timeline = _matching_timeline(confirmation_value, source["cursor"])
        raw_relative = str(timeline.get("raw_path") or "") if timeline else ""
        raw_path = (user_dir / raw_relative).resolve()
        if (
            not raw_relative
            or not raw_path.is_file()
            or runs_dir not in raw_path.parents
            or archive_x.sha256_file(raw_path) != confirmation["raw_sha256"]
        ):
            raise archive_x.ArchiveError(
                "legacy transition confirmation raw evidence changed"
            )
    return resolved, manifest


def classify_legacy_transition(
    user_dir: Path,
    *,
    expected_run_id: str | None = None,
    minimum_stalled_cycles: int = 3,
) -> dict[str, Any]:
    """Classify durable modern evidence without mutating state or contacting X."""
    state_path = user_dir / "_state" / "state.json"
    state = archive_x.load_json(state_path, None)
    if not isinstance(state, dict):
        return {"decision": "ambiguous", "reason": "invalid_archive_state"}
    existing = state.get("legacy_backfill")
    if existing is not None:
        try:
            validate_legacy_state(
                existing,
                expected_user_id=str(state.get("requested_user_id") or ""),
            )
        except archive_x.ArchiveError:
            return {"decision": "ambiguous", "reason": "invalid_legacy_state"}
        return {
            "decision": "not_applicable",
            "reason": "legacy_already_initialized",
            "source_run_id": existing["source"]["run_id"],
        }
    if minimum_stalled_cycles < 1:
        raise archive_x.ArchiveError("transition stalled-cycle minimum is invalid")
    try:
        plan = initialization_plan(user_dir)
        source = plan["source"]
        manifest_path, manifest = _legacy_source_manifest(user_dir, source)
    except (archive_x.ArchiveError, KeyError, TypeError):
        return {"decision": "ambiguous", "reason": "initialization_evidence_incomplete"}
    run_id_value = str(source["run_id"])
    if expected_run_id is not None and run_id_value != expected_run_id:
        return {"decision": "ambiguous", "reason": "source_run_mismatch"}
    requested_user_id = str(state.get("requested_user_id") or "")
    source_evidence = _transition_run_evidence(
        user_dir,
        manifest_path,
        cursor=source["cursor"],
        oldest_post_id=source["oldest_post_id"],
        oldest_post_at=source["oldest_post_at"],
        requested_user_id=requested_user_id,
        require_unchanged_window=False,
    )
    if source_evidence is None:
        return {"decision": "ambiguous", "reason": "not_exact_stopped_boundary"}
    try:
        boundary_at = parse_utc(source["oldest_post_at"], "source oldest_post_at")
    except archive_x.ArchiveError:
        return {"decision": "ambiguous", "reason": "boundary_timestamp_invalid"}
    if boundary_at >= SNOWFLAKE_EPOCH:
        return {"decision": "not_applicable", "reason": "snowflake_domain"}
    timeline = _matching_timeline(manifest, source["cursor"])
    assert isinstance(timeline, dict)
    strict_watchdog = bool(
        manifest.get("status") == "stalled"
        and manifest.get("failure_stage") == "timeline_no_progress_watchdog"
        and timeline.get("status") == "stalled"
        and timeline.get("stalled") is True
        and timeline.get("interrupted") is False
        and isinstance(timeline.get("stalled_rate_limit_cycles"), int)
        and timeline.get("stalled_rate_limit_cycles") >= minimum_stalled_cycles
    )
    confirmations = source.get("transition_confirmations", [])
    if not confirmations:
        return {
            "decision": "ambiguous",
            "reason": "no_clean_unchanged_window",
        }
    confirmation_runs = [item["run_id"] for item in confirmations]
    if strict_watchdog:
        reason = "exact_pre_snowflake_watchdog_boundary"
    elif any(run_id != run_id_value for run_id in confirmation_runs):
        reason = "exact_pre_snowflake_historical_boundary"
    else:
        reason = "exact_pre_snowflake_single_window_boundary"
    return {
        "decision": "proven",
        "reason": reason,
        "source_run_id": run_id_value,
        "source_manifest_sha256": archive_x.sha256_file(manifest_path),
        "source_raw_sha256": source_evidence["raw_sha256"],
        "oldest_post_id": source["oldest_post_id"],
        "oldest_post_at": source["oldest_post_at"],
        "confirmation_run_ids": confirmation_runs,
        "confirmation_cycles": sum(
            item["stalled_rate_limit_cycles"] for item in confirmations
        ),
        "confirmation_token": plan["confirmation_token"],
    }


def validate_modern_head(
    modern_head: Any, *, source: dict[str, Any]
) -> dict[str, Any]:
    if not isinstance(modern_head, dict):
        raise archive_x.ArchiveError("modern_head must be an object")
    if modern_head.get("schema_version") != MODERN_HEAD_SCHEMA_VERSION:
        raise archive_x.ArchiveError("unsupported modern_head schema version")
    if modern_head.get("source_run_id") != source.get("run_id"):
        raise archive_x.ArchiveError("modern_head source run changed")
    if modern_head.get("source_manifest_sha256") != source.get("manifest_sha256"):
        raise archive_x.ArchiveError("modern_head source manifest changed")
    baseline = parse_modern_timestamp(
        modern_head.get("baseline_started_at"), "baseline"
    )
    successful = parse_modern_timestamp(
        modern_head.get("last_successful_started_at"),
        "last_successful_started_at",
    )
    completed = parse_modern_timestamp(
        modern_head.get("last_successful_completed_at"),
        "last_successful_completed_at",
    )
    if successful < baseline or completed < successful:
        raise archive_x.ArchiveError("modern_head timestamp order is invalid")
    active = modern_head.get("active")
    if active is not None:
        if not isinstance(active, dict):
            raise archive_x.ArchiveError("modern_head active state is invalid")
        cursor = str(active.get("cursor") or "")
        if not CURSOR_RE.fullmatch(cursor):
            raise archive_x.ArchiveError("modern_head active cursor is invalid")
        started = parse_modern_timestamp(active.get("started_at"), "active started_at")
        cutoff = parse_modern_timestamp(active.get("date_after"), "active date_after")
        parse_modern_timestamp(active.get("saved_at"), "active saved_at")
        if cutoff > started:
            raise archive_x.ArchiveError("modern_head active cutoff is invalid")
    return modern_head


def derive_modern_head(
    user_dir: Path, state: dict[str, Any]
) -> tuple[dict[str, Any], bool]:
    legacy = validate_legacy_state(
        state.get("legacy_backfill"),
        expected_user_id=str(state.get("requested_user_id") or ""),
    )
    source = legacy["source"]
    _path, manifest = _legacy_source_manifest(user_dir, source)
    try:
        started = archive_x.parse_datetime(str(manifest.get("started_at") or ""))
        completed = archive_x.parse_datetime(str(manifest.get("completed_at") or ""))
    except argparse.ArgumentTypeError as exc:
        raise archive_x.ArchiveError(
            "legacy source manifest lacks valid modern-head timestamps"
        ) from exc
    existing = state.get("modern_head")
    if existing is not None:
        validate_modern_head(existing, source=source)
        return copy.deepcopy(existing), False
    baseline = second_utc(started)
    modern_head = {
        "schema_version": MODERN_HEAD_SCHEMA_VERSION,
        "source_run_id": source["run_id"],
        "source_manifest_sha256": source["manifest_sha256"],
        "baseline_started_at": baseline,
        "last_successful_started_at": baseline,
        "last_successful_completed_at": second_utc(completed),
        "active": None,
    }
    validate_modern_head(modern_head, source=source)
    return modern_head, True


def legacy_backup_path(user_dir: Path, source: dict[str, Any]) -> Path:
    token = require_sha256(source.get("confirmation_token"), "confirmation token")
    return user_dir / "_state" / "backups" / f"state.pre-legacy-init-{token[:12]}.json"


def automatic_initialize_legacy(
    user_dir: Path,
    *,
    initialized_at: str,
    expected_run_id: str | None = None,
    writer: Any = archive_x.atomic_write_json,
) -> dict[str, Any]:
    """Initialize/migrate under the caller's archive locks; never contacts X."""
    state_path = user_dir / "_state" / "state.json"
    state = archive_x.load_json(state_path, None)
    if not isinstance(state, dict):
        raise archive_x.ArchiveError("archive state is missing or invalid")
    changed = False
    if state.get("legacy_backfill") is None:
        classification = classify_legacy_transition(
            user_dir, expected_run_id=expected_run_id
        )
        if classification.get("decision") != "proven":
            raise archive_x.ArchiveError(
                "automatic legacy transition is not proven: "
                + str(classification.get("reason") or "ambiguous")
            )
        plan = initialization_plan(user_dir)
        updated, changed = initialize_state(
            state, plan, plan["confirmation_token"], initialized_at
        )
    else:
        updated = copy.deepcopy(state)
        validate_legacy_state(
            updated["legacy_backfill"],
            expected_user_id=str(updated.get("requested_user_id") or ""),
        )
    source = updated["legacy_backfill"]["source"]
    backup_path = legacy_backup_path(user_dir, source)
    expected_backup_hash = source["state_sha256_before_init"]
    if backup_path.exists():
        if archive_x.sha256_file(backup_path) != expected_backup_hash:
            raise archive_x.ArchiveError(
                "legacy initialization backup hash does not match source state"
            )
    elif changed:
        if archive_x.sha256_file(state_path) != expected_backup_hash:
            raise archive_x.ArchiveError(
                "archive state changed before automatic legacy backup"
            )
        writer(backup_path, state)
        if archive_x.sha256_file(backup_path) != expected_backup_hash:
            raise archive_x.ArchiveError("automatic legacy backup verification failed")
    else:
        raise archive_x.ArchiveError(
            "existing legacy state is missing its exact pre-init backup"
        )
    modern_head, head_changed = derive_modern_head(user_dir, updated)
    if head_changed:
        updated["modern_head"] = modern_head
    if changed or head_changed:
        writer(state_path, updated)
    return {
        "state": updated,
        "legacy_initialized": changed,
        "modern_head_initialized": head_changed,
        "backup_path": str(backup_path),
    }


def window_id(since: str, until: str) -> str:
    return "legacy-" + canonical_sha256({"since": since, "until": until})[:20]


def legacy_query(
    handle: str,
    since: str,
    until: str,
    *,
    include_reposts: bool,
) -> tuple[str, str]:
    if not archive_x.HANDLE_RE.fullmatch(handle):
        raise archive_x.ArchiveError("legacy query handle is invalid")
    since_at = parse_utc(since, "query since")
    until_at = parse_utc(until, "query until")
    if not since_at < until_at:
        raise archive_x.ArchiveError("legacy query interval is empty or reversed")
    since_epoch = int(since_at.timestamp()) - 1
    until_epoch = int(until_at.timestamp()) + 1
    query = f"from:{handle} since_time:{since_epoch} until_time:{until_epoch}"
    if include_reposts:
        query += " include:retweets include:nativeretweets"
    return query, f"https://x.com/search?q={quote(query, safe='')}&f=live"


def build_legacy_gallery_config(
    *,
    handle: str,
    endpoint: str,
    archive_root: Path,
    user_dir: Path,
    raw_partial: Path,
    cookie_file: Path,
    archive_run_id: str,
    archived_at: str,
    request_delay: str,
    include_reposts: bool,
    empty_tail_pages: int,
    descriptor_artifact: Path | None = None,
    descriptor_operation_id: str | None = None,
) -> dict[str, Any]:
    if empty_tail_pages < 1:
        raise archive_x.ArchiveError(
            "legacy empty-tail page count must be positive"
        )
    config = archive_x.build_gallery_config(
        handle=handle,
        endpoint=endpoint,
        archive_root=archive_root,
        user_dir=user_dir,
        raw_partial=raw_partial,
        cookie_file=cookie_file,
        archive_run_id=archive_run_id,
        archived_at=archived_at,
        request_delay=request_delay,
        download_delay="0",
        extractor_delay="0",
        include_reposts=include_reposts,
        checksums=False,
        cursor=None,
        descriptor_artifact=descriptor_artifact,
        descriptor_operation_id=descriptor_operation_id,
        descriptor_source_kind=(
            "legacy" if descriptor_artifact is not None else None
        ),
        descriptor_source_operation=(
            "legacy" if descriptor_artifact is not None else None
        ),
    )
    twitter = config["extractor"]["twitter"]
    # Enumeration must not depend on the media-download archive. Both walks
    # are metadata-only and independently observe the full search result set.
    twitter.pop("archive", None)
    twitter.update(
        {
            "cookies-update": False,
            "search-pagination": "cursor",
            "search-results": "Latest",
            "search-limit": 20,
            # gallery-dl stops on the next empty response after this counter
            # reaches zero. A value of N-1 therefore records exactly N empty
            # tail pages before successful termination.
            "search-stop": empty_tail_pages - 1,
            "quoted": False,
            "expand": False,
            "showreplies": False,
            "cards": False,
            # --no-download still emits prepare events. Keep video extraction
            # enabled so confirmed walks retain the already returned CDN
            # descriptor without transferring bytes.
            "videos": True,
            "previews": False,
            "articles": False,
        }
    )
    return config


def legacy_gallery_command(
    repo_dir: Path,
    config_path: Path,
    telemetry_path: Path,
    request_telemetry_path: Path,
    *,
    request_limit: int,
    empty_tail_pages: int,
    retries: int,
    http_timeout: int,
    requested_user_id: str,
    url: str,
    scheduler_options: pacing_x.SchedulerOptions | None = None,
) -> list[str]:
    if request_limit < 1:
        raise archive_x.ArchiveError("legacy request limit must be positive")
    if not 1 <= empty_tail_pages < request_limit:
        raise archive_x.ArchiveError(
            "legacy empty-tail page count must be below the request limit"
        )
    if not requested_user_id.isdecimal() or int(requested_user_id) < 1:
        raise archive_x.ArchiveError("legacy bound user ID is invalid")
    command = [
        sys.executable,
        str(repo_dir / "scripts" / "gallery_dl_x_legacy_runner.py"),
        "--archive-x-legacy-telemetry",
        str(telemetry_path),
        "--archive-x-legacy-request-limit",
        str(request_limit),
        "--archive-x-legacy-empty-tail-pages",
        str(empty_tail_pages),
        "--archive-x-legacy-bound-user-id",
        requested_user_id,
        "--archive-x-request-telemetry",
        str(request_telemetry_path),
        "--archive-x-operation",
        "legacy_walk",
    ]
    if scheduler_options is not None:
        command.extend(pacing_x.options_as_runner_args(scheduler_options))
    command.extend([
        "--config-ignore",
        "-c",
        str(repo_dir / "gallery-dl.conf"),
        "--config-json",
        str(config_path),
        "--no-input",
        "--no-colors",
        "--no-download",
        "--http-timeout",
        str(http_timeout),
        "--sleep-retries",
        "0" if scheduler_options is not None else "30-60",
        "--sleep-429",
        "0" if scheduler_options is not None else "300",
        "--retries",
        str(retries),
        url,
    ])
    return command


def verify_legacy_runner(repo_dir: Path, version: str) -> None:
    command = [
        sys.executable,
        str(repo_dir / "scripts" / "gallery_dl_x_legacy_runner.py"),
        "--version",
    ]
    try:
        result = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise archive_x.ArchiveError(
            f"could not verify the gallery-dl X legacy runner: {exc}"
        ) from exc
    if result.returncode != 0 or result.stdout.strip() != version:
        detail = (result.stderr or result.stdout).strip()
        raise archive_x.ArchiveError(
            "gallery-dl X legacy runner compatibility check failed"
            + (f": {detail}" if detail else "")
        )


def validate_walk_records(
    raw_path: Path,
    *,
    since: str,
    until: str,
    requested_user_id: str,
    requested_handle: str,
    include_reposts: bool,
) -> dict[str, Any]:
    since_at = parse_utc(since, "walk since")
    until_at = parse_utc(until, "walk until")
    query_floor = since_at - timedelta(seconds=1)
    query_ceiling = until_at + timedelta(seconds=1)
    accepted: dict[str, dict[str, Any]] = {}
    outside_ids: list[str] = []
    raw_count = 0
    for metadata in archive_x.iter_jsonl(raw_path):
        raw_count += 1
        post_id = archive_x.id_string(metadata.get("tweet_id"))
        if not post_id:
            raise archive_x.ArchiveError("legacy walk returned a record without an ID")
        try:
            observed_at = archive_x.parse_datetime(str(metadata.get("date") or ""))
        except argparse.ArgumentTypeError as exc:
            raise archive_x.ArchiveError(
                f"legacy walk record {post_id} has an invalid returned timestamp"
            ) from exc
        if not query_floor <= observed_at <= query_ceiling:
            raise archive_x.ArchiveError(
                f"legacy walk record {post_id} is outside its query overlap"
            )
        author_id = archive_x.id_string((metadata.get("author") or {}).get("id"))
        user_id = archive_x.id_string((metadata.get("user") or {}).get("id"))
        relation = archive_x.relation_for(metadata, requested_handle)
        authored = relation in {"post", "reply"}
        repost = relation == "repost"
        if repost and not include_reposts:
            raise archive_x.ArchiveError(
                f"legacy walk record {post_id} violates the frozen repost policy"
            )
        if not (
            (authored and author_id == requested_user_id and user_id == requested_user_id)
            or (repost and user_id == requested_user_id)
        ):
            raise archive_x.ArchiveError(
                f"legacy walk record {post_id} failed numeric identity validation"
            )
        if not since_at <= observed_at < until_at:
            outside_ids.append(post_id)
            continue
        current = accepted.get(post_id)
        if current is None or archive_x.record_richness(
            archive_x.normalize_post(metadata, requested_handle, "legacy") or {}
        ) > archive_x.record_richness(
            archive_x.normalize_post(current, requested_handle, "legacy") or {}
        ):
            accepted[post_id] = metadata
    return {
        "raw_count": raw_count,
        "accepted_count": len(accepted),
        "accepted_ids": sorted(accepted, key=int),
        "accepted_records": [accepted[key] for key in sorted(accepted, key=int)],
        "overlap_excluded_ids": sorted(set(outside_ids), key=int),
    }


def validate_walk_telemetry(
    telemetry: Any,
    *,
    expected_query: str,
    request_limit: int,
    empty_tail_pages: int,
    exit_code: int,
    expected_user_id: str,
    require_bound_identity: bool = False,
) -> dict[str, Any]:
    if not isinstance(telemetry, dict) or telemetry.get("schema_version") != 1:
        raise archive_x.ArchiveError("legacy walk telemetry schema is invalid")
    if telemetry.get("opaque_cursor_values_persisted") is not False:
        raise archive_x.ArchiveError("legacy walk telemetry may contain opaque cursors")
    if telemetry.get("request_limit") != request_limit:
        raise archive_x.ArchiveError("legacy walk telemetry request limit changed")
    if telemetry.get("empty_tail_pages") != empty_tail_pages:
        raise archive_x.ArchiveError("legacy walk empty-tail proof changed")
    search_requests = telemetry.get("search_requests")
    if not isinstance(search_requests, int) or not 1 <= search_requests <= request_limit:
        raise archive_x.ArchiveError("legacy walk request count is invalid")
    if telemetry.get("exit_code") != exit_code or exit_code != 0:
        raise archive_x.ArchiveError("legacy walk process did not exit successfully")
    if telemetry.get("profile_user_ids") != [expected_user_id]:
        raise archive_x.ArchiveError(
            "legacy walk profile identity does not match the archive"
        )
    identity_source = telemetry.get("identity_source", "profile_api")
    profile_requests = telemetry.get(
        "profile_requests", 1 if identity_source == "profile_api" else 0
    )
    if identity_source not in {"profile_api", "bound_numeric_id"} or not isinstance(
        profile_requests, int
    ):
        raise archive_x.ArchiveError("legacy walk identity evidence is invalid")
    if (
        identity_source == "bound_numeric_id" and profile_requests != 0
    ) or (
        identity_source == "profile_api" and profile_requests < 1
    ):
        raise archive_x.ArchiveError("legacy walk profile request evidence changed")
    if require_bound_identity and identity_source != "bound_numeric_id":
        raise archive_x.ArchiveError(
            "legacy walk repeated profile resolution instead of bound identity"
        )
    pages = telemetry.get("pages")
    if not isinstance(pages, list) or len(pages) != search_requests:
        raise archive_x.ArchiveError("legacy walk page telemetry is incomplete")
    query_hash = hashlib.sha256(expected_query.encode("utf-8")).hexdigest()
    if any(page.get("query_sha256") != query_hash for page in pages):
        raise archive_x.ArchiveError("legacy walk query changed during pagination")
    if any(page.get("api_error_count") for page in pages):
        raise archive_x.ArchiveError("legacy walk contains an API error")
    if any(page.get("cursor_repeated") for page in pages):
        raise archive_x.ArchiveError("legacy walk repeated an opaque cursor")
    reason = telemetry.get("terminal_reason")
    if reason not in LEGACY_TERMINAL_REASONS:
        raise archive_x.ArchiveError(
            f"legacy walk termination is ambiguous: {reason!r}"
        )
    if telemetry.get("request_cap_reached"):
        raise archive_x.ArchiveError("legacy walk reached its request cap")
    return telemetry


def run_legacy_walk(
    *,
    repo_dir: Path,
    archive_root: Path,
    user_dir: Path,
    run_dir: Path,
    handle: str,
    requested_user_id: str,
    archive_run_id: str,
    window_id_value: str,
    walk_id: str,
    since: str,
    until: str,
    cookie_file: Path,
    request_delay: str,
    include_reposts: bool,
    request_limit: int,
    empty_tail_pages: int,
    retries: int,
    http_timeout: int,
    stalled_rate_limit_cycles: int,
    runner: Any | None = None,
) -> dict[str, Any]:
    endpoint = f"{window_id_value}-{walk_id}"
    raw_partial = run_dir / "raw" / f"{endpoint}.posts.jsonl.partial"
    descriptor_partial = (
        run_dir / "raw" / f"{endpoint}.descriptors.jsonl.partial"
    )
    telemetry_path = run_dir / f"{endpoint}.telemetry.json"
    request_telemetry_path = run_dir / f"{endpoint}.requests.json"
    config_path = run_dir / f"{endpoint}.gallery-dl.json"
    log_path = run_dir / f"{endpoint}.log"
    query, url = legacy_query(
        handle, since, until, include_reposts=include_reposts
    )
    descriptor_operation_id = f"{archive_run_id}:{endpoint}"
    descriptor_x.prepare_artifact(descriptor_partial)
    scheduler_options = archive_x.x_scheduler_options(
        user_dir, requested_user_id, request_delay
    )
    config = build_legacy_gallery_config(
        handle=handle,
        endpoint=endpoint,
        archive_root=archive_root,
        user_dir=user_dir,
        raw_partial=raw_partial,
        cookie_file=cookie_file,
        archive_run_id=archive_run_id,
        archived_at=second_utc(archive_x.utc_now()),
        request_delay="0",
        include_reposts=include_reposts,
        empty_tail_pages=empty_tail_pages,
        descriptor_artifact=descriptor_partial,
        descriptor_operation_id=descriptor_operation_id,
    )
    archive_x.atomic_write_json(config_path, config)
    command = legacy_gallery_command(
        repo_dir,
        config_path,
        telemetry_path,
        request_telemetry_path,
        request_limit=request_limit,
        empty_tail_pages=empty_tail_pages,
        retries=retries,
        http_timeout=http_timeout,
        requested_user_id=requested_user_id,
        url=url,
        scheduler_options=scheduler_options,
    )
    (
        status,
        _ignored_cursor,
        duration,
        interrupted,
        failed_downloads,
        other_error_count,
        stalled,
        stalled_cycles,
    ) = archive_x.run_gallery_dl(
        command,
        log_path,
        f"{handle}:{endpoint}",
        progress_path=raw_partial,
        stalled_rate_limit_cycles=stalled_rate_limit_cycles,
        runner=runner,
    )
    telemetry = archive_x.load_json(telemetry_path, None)
    request_summary, request_error = archive_x.request_telemetry_summary(
        request_telemetry_path, "legacy_walk"
    )
    valid = False
    validation_error = None
    records = None
    try:
        if interrupted:
            raise archive_x.ArchiveError("legacy walk was interrupted")
        if stalled:
            raise archive_x.ArchiveError("legacy walk hit the no-progress watchdog")
        if failed_downloads:
            raise archive_x.ArchiveError(
                "metadata-only legacy walk unexpectedly reported a download failure"
            )
        if other_error_count:
            raise archive_x.ArchiveError("legacy walk log contains an extraction error")
        validate_walk_telemetry(
            telemetry,
            expected_query=query,
            request_limit=request_limit,
            empty_tail_pages=empty_tail_pages,
            exit_code=status,
            expected_user_id=requested_user_id,
            require_bound_identity=True,
        )
        records = validate_walk_records(
            raw_partial,
            since=since,
            until=until,
            requested_user_id=requested_user_id,
            requested_handle=handle,
            include_reposts=include_reposts,
        )
        valid = True
    except archive_x.ArchiveError as exc:
        validation_error = str(exc)
    raw_path = archive_x.finalize_raw_file(raw_partial, valid)
    descriptor_path = descriptor_x.finalize_artifact(
        descriptor_partial, complete=valid
    )
    return {
        "archive_run_id": archive_run_id,
        "walk_id": walk_id,
        "endpoint": endpoint,
        "since": since,
        "until": until,
        "query_sha256": hashlib.sha256(query.encode("utf-8")).hexdigest(),
        "status": "valid" if valid else "ambiguous",
        "exit_code": status,
        "duration_seconds": round(duration, 3),
        "interrupted": interrupted,
        "stalled": stalled,
        "stalled_rate_limit_cycles": stalled_cycles,
        "validation_error": validation_error,
        "terminal_reason": (
            telemetry.get("terminal_reason") if isinstance(telemetry, dict) else None
        ),
        "request_limit": (
            telemetry.get("request_limit") if isinstance(telemetry, dict) else None
        ),
        "empty_tail_pages": (
            telemetry.get("empty_tail_pages")
            if isinstance(telemetry, dict)
            else None
        ),
        "search_requests": (
            telemetry.get("search_requests") if isinstance(telemetry, dict) else None
        ),
        "api_requests": (
            telemetry.get("api_requests") if isinstance(telemetry, dict) else None
        ),
        "profile_requests": (
            telemetry.get("profile_requests")
            if isinstance(telemetry, dict)
            else None
        ),
        "identity_source": (
            telemetry.get("identity_source") if isinstance(telemetry, dict) else None
        ),
        "records": records,
        "raw_path": str(raw_path.relative_to(user_dir)),
        "raw_sha256": archive_x.sha256_file(raw_path),
        "descriptor_artifact_path": str(
            descriptor_path.relative_to(user_dir)
        ),
        "descriptor_artifact_sha256": descriptor_x.sha256_file(
            descriptor_path
        ),
        "descriptor_artifact_bytes": descriptor_path.stat().st_size,
        "descriptor_operation_id": descriptor_operation_id,
        "descriptor_source_kind": "legacy",
        "descriptor_source_operation": "legacy",
        "telemetry_path": str(telemetry_path.relative_to(user_dir))
        if telemetry_path.exists()
        else None,
        "telemetry_sha256": archive_x.sha256_file(telemetry_path)
        if telemetry_path.exists()
        else None,
        "request_telemetry_path": (
            str(request_telemetry_path.relative_to(user_dir))
            if request_telemetry_path.exists()
            else None
        ),
        "request_telemetry_sha256": (
            archive_x.sha256_file(request_telemetry_path)
            if request_telemetry_path.exists()
            else None
        ),
        "request_telemetry": request_summary,
        "request_telemetry_error": request_error,
        "config_path": str(config_path.relative_to(user_dir)),
        "config_sha256": archive_x.sha256_file(config_path),
        "log_path": str(log_path.relative_to(user_dir)),
        "command": command,
    }


def enqueue_legacy_media_posts(
    state: dict[str, Any],
    records: list[dict[str, Any]],
    *,
    source_run_id: str,
    observed_at: str,
) -> None:
    current = state.get("pending_media")
    pending = [
        record.copy()
        for record in (current if isinstance(current, list) else [])
        if isinstance(record, dict)
    ]
    by_key = {
        str(record.get("key")): record
        for record in pending
        if record.get("key")
    }
    for metadata in records:
        post_id = archive_x.id_string(metadata.get("tweet_id"))
        count = metadata.get("count")
        if not post_id or not isinstance(count, int) or count < 1:
            continue
        key = f"post:{post_id}"
        record = by_key.get(key, {})
        record.update(
            {
                "kind": "post",
                "key": key,
                "post_id": post_id,
                "expected_media_count": count,
                "source_url": f"https://x.com/i/web/status/{post_id}",
                "first_failed_at": record.get("first_failed_at") or observed_at,
                "last_failed_at": observed_at,
                "last_source_run_id": source_run_id,
                "attempts": int(record.get("attempts") or 0),
            }
        )
        if key not in by_key:
            pending.append(record)
        by_key[key] = record
    state["pending_media"] = sorted(
        pending,
        key=lambda record: str(record.get("filename") or record.get("key") or ""),
    )


def commit_indexed_legacy_window(
    user_dir: Path,
    *,
    canonical_path: Path,
    canonical_hash: str,
    canonical_records: list[dict[str, Any]],
    handle: str,
    requested_user_id: str,
    run_id: str,
    window_id_value: str,
    since: str,
    until: str,
    observation_ids: list[str],
    observed_at: str,
) -> dict[str, int | bool]:
    relative = str(canonical_path.relative_to(user_dir))
    if archive_x.sha256_file(canonical_path) != require_sha256(
        canonical_hash, "indexed canonical hash"
    ):
        raise archive_x.ArchiveError("legacy canonical file changed before indexing")
    if len(observation_ids) < 2 or len(set(observation_ids)) != len(
        observation_ids
    ):
        raise archive_x.ArchiveError(
            "legacy indexed interval lacks distinct observations"
        )
    for observation_id in observation_ids:
        require_sha256(observation_id, "indexed observation ID")
    normalized_records = []
    for metadata in canonical_records:
        normalized = archive_x.normalize_post(metadata, handle, "legacy")
        if normalized is None:
            raise archive_x.ArchiveError(
                "legacy canonical record could not be normalized"
            )
        if str(normalized.get("requested_user_id") or "") != requested_user_id:
            raise archive_x.ArchiveError(
                "legacy canonical normalized identity changed"
            )
        normalized_records.append(normalized)
    since_at = parse_utc(since, "indexed since")
    until_at = parse_utc(until, "indexed until")
    canonical_sha256_value = canonical_hash
    # Bind the canonical file and every confirming observation without
    # retaining raw IDs or private query material in the interval row.
    evidence_hash = canonical_sha256(
        {
            "canonical_sha256": canonical_sha256_value,
            "observation_ids": sorted(observation_ids),
        }
    )
    stat = canonical_path.stat()
    db_path = user_dir / "_state" / "context.sqlite3"
    try:
        with context_x.ContextDB(db_path) as database:
            database.bind_identity(requested_user_id, handle)
            return database.commit_legacy_interval(
                interval_id=window_id_value,
                root_window_id=window_id_value,
                since_at=since,
                until_at=until,
                since_epoch=int(since_at.timestamp()),
                until_epoch=int(until_at.timestamp()),
                canonical_relative_path=relative,
                canonical_sha256=canonical_sha256_value,
                canonical_stat=stat,
                run_id=run_id,
                evidence_sha256=evidence_hash,
                observation_count=len(observation_ids),
                normalized_records=normalized_records,
                observed_at=observed_at,
            )
    except (context_x.ContextError, sqlite3.Error) as exc:
        raise archive_x.ArchiveError(
            f"legacy indexed commit failed ({exc.__class__.__name__})"
        ) from exc


def enqueue_pending_portable_export(
    legacy: dict[str, Any],
    *,
    window_id_value: str,
    since: str,
    until: str,
    canonical_raw_path: str,
    canonical_raw_sha256: str,
    indexed_generation: int,
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    pending = copy.deepcopy(legacy.get("pending_portable_exports") or [])
    record = {
        "window_id": window_id_value,
        "since": since,
        "until": until,
        "canonical_raw_path": _relative_evidence_path(
            canonical_raw_path, "pending canonical raw"
        ),
        "canonical_raw_sha256": require_sha256(
            canonical_raw_sha256, "pending canonical raw hash"
        ),
        "indexed_generation": _positive_counter(
            indexed_generation, "pending indexed generation", allow_zero=False
        ),
    }
    existing = next(
        (item for item in pending if item["window_id"] == window_id_value), None
    )
    if existing is not None and existing != record:
        raise archive_x.ArchiveError("legacy pending export evidence changed")
    if existing is None:
        pending.append(record)
    updated = copy.deepcopy(legacy)
    updated["pending_portable_exports"] = pending
    validate_legacy_state(updated, expected_user_id=updated["requested_user_id"])
    return updated


def flush_pending_portable_exports(
    user_dir: Path,
    state: dict[str, Any],
    *,
    state_path: Path,
    run_dir: Path,
    handle: str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    legacy = state["legacy_backfill"]
    pending = validate_pending_exports(legacy.get("pending_portable_exports"))
    if not pending:
        return state, None
    del run_dir, handle
    db_path = user_dir / "_state" / "context.sqlite3"
    with context_x.ContextDB(db_path, create=False) as database:
        indexed_generation = int(
            database.connection.execute(
                "SELECT current_generation FROM archive_generation WHERE singleton=1"
            ).fetchone()[0]
        )
        counters = {
            str(row[0]): int(row[1])
            for row in database.connection.execute(
                "SELECT counter_name,value FROM progress_counters"
            )
        }
    updated = copy.deepcopy(state)
    updated_legacy = updated["legacy_backfill"]
    updated_legacy["pending_portable_exports"] = []
    updated_legacy["last_indexed_checkpoint"] = {
        "through_generation": max(item["indexed_generation"] for item in pending),
        "window_count": len(pending),
        "archive_generation": indexed_generation,
        "dataset_posts": counters.get("archive_posts_total", 0),
        "checkpointed_at": second_utc(archive_x.utc_now()),
    }
    last_completed = updated_legacy.get("last_completed_window")
    if (
        isinstance(last_completed, dict)
        and int(last_completed.get("indexed_generation") or 0)
        <= int(updated_legacy["last_indexed_checkpoint"]["through_generation"])
    ):
        last_completed["portable_export_pending"] = True
    validate_legacy_state(
        updated_legacy,
        expected_user_id=str(updated.get("requested_user_id") or ""),
    )
    archive_x.atomic_write_json(state_path, updated)
    return updated, {
        "status": "deferred_to_unified_checkpoint",
        "window_count": len(pending),
        "raw_records": 0,
        "payload_bytes_read": 0,
        "dataset_posts": counters.get("archive_posts_total", 0),
        "archive_generation": indexed_generation,
    }


def record_unified_export_checkpoint(
    user_dir: Path,
    export: dict[str, Any],
) -> bool:
    """Tie a published unified generation back to pending legacy evidence."""
    if export.get("status") not in {"published", "unchanged"}:
        return False
    generation = int(
        export.get("exported_generation") or export.get("generation") or 0
    )
    manifest_relative = str(export.get("manifest_path") or "")
    relative = Path(manifest_relative)
    if (
        generation < 1
        or not relative.parts
        or relative.is_absolute()
        or ".." in relative.parts
    ):
        raise archive_x.ArchiveError("unified legacy export evidence is invalid")
    manifest_path = (user_dir / relative).resolve()
    try:
        manifest_path.relative_to((user_dir / "dataset" / "exports").resolve())
    except ValueError as exc:
        raise archive_x.ArchiveError(
            "unified legacy export manifest escaped its generation root"
        ) from exc
    manifest = archive_x.load_json(manifest_path, None)
    if not isinstance(manifest, dict) or int(manifest.get("generation") or 0) != generation:
        raise archive_x.ArchiveError("unified legacy export manifest is invalid")
    posts = (manifest.get("views") or {}).get("posts")
    if not isinstance(posts, dict):
        raise archive_x.ArchiveError("unified legacy posts export evidence is missing")
    dataset_sha256 = require_sha256(posts.get("sha256"), "unified posts export hash")
    dataset_posts = _positive_counter(
        posts.get("rows"), "unified posts export rows", allow_zero=True
    )
    state_path = user_dir / "_state" / "state.json"
    state = archive_x.load_json(state_path, None)
    if not isinstance(state, dict) or not isinstance(
        state.get("legacy_backfill"), dict
    ):
        return False
    legacy = state["legacy_backfill"]
    checkpoint = legacy.get("last_indexed_checkpoint")
    if not isinstance(checkpoint, dict):
        return False
    if generation < int(checkpoint.get("archive_generation") or 0):
        return False
    updated = copy.deepcopy(state)
    updated_legacy = updated["legacy_backfill"]
    updated_legacy["last_portable_export"] = {
        "through_generation": int(checkpoint["through_generation"]),
        "window_count": int(checkpoint["window_count"]),
        "archive_generation": generation,
        "dataset_sha256": dataset_sha256,
        "dataset_posts": dataset_posts,
        "exported_at": second_utc(archive_x.utc_now()),
    }
    last_completed = updated_legacy.get("last_completed_window")
    if (
        isinstance(last_completed, dict)
        and int(last_completed.get("indexed_generation") or 0)
        <= int(checkpoint["through_generation"])
    ):
        last_completed["portable_export_pending"] = False
        last_completed["dataset_sha256"] = dataset_sha256
    validate_legacy_state(
        updated_legacy,
        expected_user_id=str(updated.get("requested_user_id") or ""),
    )
    archive_x.atomic_write_json(state_path, updated)
    return True


def resume_active_window(
    legacy: dict[str, Any],
    *,
    owner_run_id: str,
    resumed_at: str,
    attempt_limit: int,
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    if legacy["status"] != "active":
        raise archive_x.ArchiveError("legacy state has no active window to resume")
    active = legacy["active_window"]
    locally_confirmable = all(
        len(leaf.get("observations") or []) >= 2
        for leaf in active["leaves"]
    )
    if active["attempt"] >= attempt_limit and not locally_confirmable:
        return mark_manual_review(
            legacy,
            window_id_value=active["window_id"],
            reason=f"window attempt limit ({attempt_limit}) reached",
            observed_at=resumed_at,
        )
    updated = copy.deepcopy(legacy)
    updated["active_window"]["owner_run_id"] = owner_run_id
    if active["attempt"] < attempt_limit:
        updated["active_window"]["attempt"] += 1
    else:
        updated["active_window"]["local_evidence_recovery"] = True
    updated["active_window"]["claimed_at"] = second_utc(
        parse_utc(resumed_at, "resumed_at")
    )
    validate_legacy_state(updated, expected_user_id=updated["requested_user_id"])
    return updated


def split_active_leaf(
    legacy: dict[str, Any],
    *,
    leaf_since: str,
    leaf_until: str,
    max_leaves: int,
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    if legacy["status"] != "active":
        raise archive_x.ArchiveError("legacy state has no active window to split")
    leaves = legacy["active_window"]["leaves"]
    if len(leaves) >= max_leaves:
        raise archive_x.ArchiveError(
            f"legacy active window reached its leaf limit ({max_leaves})"
        )
    index = next(
        (
            index
            for index, leaf in enumerate(leaves)
            if leaf["since"] == leaf_since
            and leaf["until"] == leaf_until
            and leaf["status"] == "pending"
        ),
        None,
    )
    if index is None:
        raise archive_x.ArchiveError("legacy split leaf guard failed")
    since_at = parse_utc(leaf_since, "split leaf since")
    until_at = parse_utc(leaf_until, "split leaf until")
    seconds = int((until_at - since_at).total_seconds())
    if seconds <= 1:
        raise archive_x.ArchiveError("legacy saturated one-second leaf")
    midpoint = since_at + timedelta(seconds=seconds // 2)
    midpoint_text = second_utc(midpoint)
    children = [
        {
            "since": leaf_since,
            "until": midpoint_text,
            "status": "pending",
            "observations": [],
        },
        {
            "since": midpoint_text,
            "until": leaf_until,
            "status": "pending",
            "observations": [],
        },
    ]
    updated = copy.deepcopy(legacy)
    updated["active_window"]["leaves"][index : index + 1] = children
    validate_legacy_state(updated, expected_user_id=updated["requested_user_id"])
    return updated


def compatible_walk_records(
    first: dict[str, Any], second: dict[str, Any]
) -> list[dict[str, Any]]:
    first_records = first.get("records") or {}
    second_records = second.get("records") or {}
    first_ids = first_records.get("accepted_ids")
    second_ids = second_records.get("accepted_ids")
    if first_ids != second_ids:
        raise archive_x.ArchiveError("independent legacy walks returned different IDs")
    by_first = {
        archive_x.id_string(record.get("tweet_id")): record
        for record in first_records.get("accepted_records", ())
    }
    by_second = {
        archive_x.id_string(record.get("tweet_id")): record
        for record in second_records.get("accepted_records", ())
    }
    chosen = []
    for post_id in first_ids or ():
        old, new = by_first.get(post_id), by_second.get(post_id)
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise archive_x.ArchiveError("legacy walk record evidence is incomplete")
        stable_old = (
            str(old.get("date") or ""),
            archive_x.id_string((old.get("user") or {}).get("id")),
            archive_x.id_string((old.get("author") or {}).get("id")),
            archive_x.id_string(old.get("retweet_id")),
            archive_x.id_string(old.get("reply_id")),
        )
        stable_new = (
            str(new.get("date") or ""),
            archive_x.id_string((new.get("user") or {}).get("id")),
            archive_x.id_string((new.get("author") or {}).get("id")),
            archive_x.id_string(new.get("retweet_id")),
            archive_x.id_string(new.get("reply_id")),
        )
        if stable_old != stable_new:
            raise archive_x.ArchiveError(
                f"legacy walk record {post_id} has incompatible stable metadata"
            )
        old_richness = sum(value not in (None, "", [], {}) for value in old.values())
        new_richness = sum(value not in (None, "", [], {}) for value in new.values())
        chosen.append(new if new_richness >= old_richness else old)
    return chosen


def walk_compatibility_sha256(result: dict[str, Any]) -> str:
    records = result.get("records")
    if not isinstance(records, dict):
        raise archive_x.ArchiveError("legacy valid observation lacks records")
    accepted_ids = records.get("accepted_ids")
    accepted_records = records.get("accepted_records")
    if not isinstance(accepted_ids, list) or not isinstance(accepted_records, list):
        raise archive_x.ArchiveError("legacy valid observation records are invalid")
    by_id = {
        archive_x.id_string(record.get("tweet_id")): record
        for record in accepted_records
        if isinstance(record, dict)
    }
    stable = []
    for post_id in accepted_ids:
        post_id = archive_x.id_string(post_id)
        record = by_id.get(post_id)
        if post_id is None or not isinstance(record, dict):
            raise archive_x.ArchiveError(
                "legacy valid observation record evidence is incomplete"
            )
        stable.append(
            [
                post_id,
                str(record.get("date") or ""),
                archive_x.id_string((record.get("user") or {}).get("id")),
                archive_x.id_string((record.get("author") or {}).get("id")),
                archive_x.id_string(record.get("retweet_id")),
                archive_x.id_string(record.get("reply_id")),
            ]
        )
    return canonical_sha256(stable)


def _verified_user_file(
    user_dir: Path, relative: Any, expected_sha256: Any, field: str
) -> Path:
    relative_text = _relative_evidence_path(relative, field)
    path = (user_dir / relative_text).resolve()
    root = user_dir.resolve()
    if root not in path.parents or not path.is_file():
        raise archive_x.ArchiveError(f"legacy retained {field} file is missing")
    expected = require_sha256(expected_sha256, f"retained {field} hash")
    if archive_x.sha256_file(path) != expected:
        raise archive_x.ArchiveError(f"legacy retained {field} hash changed")
    return path


def retained_observation(
    user_dir: Path,
    result: dict[str, Any],
) -> dict[str, Any]:
    if result.get("status") != "valid":
        raise archive_x.ArchiveError("only a valid legacy walk can be retained")
    since = str(result.get("since") or "")
    until = str(result.get("until") or "")
    parse_utc(since, "retained since")
    parse_utc(until, "retained until")
    raw_path = _verified_user_file(
        user_dir, result.get("raw_path"), result.get("raw_sha256"), "raw"
    )
    telemetry_path = _verified_user_file(
        user_dir,
        result.get("telemetry_path"),
        result.get("telemetry_sha256"),
        "telemetry",
    )
    descriptor_relative = result.get("descriptor_artifact_path")
    descriptor_hash = result.get("descriptor_artifact_sha256")
    if descriptor_relative is not None or descriptor_hash is not None:
        _verified_user_file(
            user_dir, descriptor_relative, descriptor_hash, "descriptor"
        )
    compatibility = walk_compatibility_sha256(result)
    request_limit = _positive_counter(
        result.get("request_limit"), "retained request limit", allow_zero=False
    )
    empty_tail = _positive_counter(
        result.get("empty_tail_pages"),
        "retained empty tail",
        allow_zero=False,
    )
    search_requests = _positive_counter(
        result.get("search_requests"), "retained search requests", allow_zero=False
    )
    api_requests = _positive_counter(
        result.get("api_requests"), "retained API requests", allow_zero=False
    )
    accepted_count = _positive_counter(
        (result.get("records") or {}).get("accepted_count"),
        "retained accepted records",
    )
    evidence_identity = {
        "archive_run_id": str(result.get("archive_run_id") or ""),
        "walk_id": str(result.get("walk_id") or ""),
        "raw_path": str(raw_path.relative_to(user_dir)),
        "raw_sha256": str(result.get("raw_sha256")),
        "telemetry_path": str(telemetry_path.relative_to(user_dir)),
        "telemetry_sha256": str(result.get("telemetry_sha256")),
        "query_sha256": str(result.get("query_sha256")),
    }
    observation = {
        "observation_id": canonical_sha256(evidence_identity),
        **evidence_identity,
        "since": since,
        "until": until,
        "compatibility_sha256": compatibility,
        "request_limit": request_limit,
        "empty_tail_pages": empty_tail,
        "search_requests": search_requests,
        "api_requests": api_requests,
        "accepted_count": accepted_count,
    }
    if descriptor_relative is not None:
        observation.update(
            {
                "descriptor_artifact_path": str(descriptor_relative),
                "descriptor_artifact_sha256": str(descriptor_hash),
                "descriptor_operation_id": str(
                    result.get("descriptor_operation_id") or ""
                ),
                "descriptor_source_kind": "legacy",
                "descriptor_source_operation": "legacy",
            }
        )
    validate_retained_observation(
        observation, leaf_since=since, leaf_until=until
    )
    return observation


def append_retained_observation(
    legacy: dict[str, Any],
    *,
    leaf_since: str,
    leaf_until: str,
    observation: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    if legacy["status"] != "active":
        raise archive_x.ArchiveError("legacy observation has no active window")
    updated = copy.deepcopy(legacy)
    leaf = next(
        (
            item
            for item in updated["active_window"]["leaves"]
            if item["since"] == leaf_since and item["until"] == leaf_until
        ),
        None,
    )
    if leaf is None or leaf["status"] != "pending":
        raise archive_x.ArchiveError("legacy observation leaf guard failed")
    validate_retained_observation(
        observation, leaf_since=leaf_since, leaf_until=leaf_until
    )
    retained = leaf.setdefault("observations", [])
    for existing in retained:
        if existing["observation_id"] == observation["observation_id"]:
            if existing != observation:
                raise archive_x.ArchiveError(
                    "legacy retained observation identity changed"
                )
            return updated, False
        if existing["raw_path"] == observation["raw_path"]:
            raise archive_x.ArchiveError(
                "legacy source artifact cannot count as two observations"
            )
    if len(retained) >= MAX_RETAINED_OBSERVATIONS_PER_LEAF:
        raise archive_x.ArchiveError("legacy retained observation limit reached")
    retained.append(copy.deepcopy(observation))
    validate_legacy_state(updated, expected_user_id=updated["requested_user_id"])
    return updated, True


def restore_retained_observation(
    user_dir: Path,
    observation: dict[str, Any],
    *,
    handle: str,
    requested_user_id: str,
    include_reposts: bool,
) -> dict[str, Any]:
    since = str(observation.get("since") or "")
    until = str(observation.get("until") or "")
    validate_retained_observation(
        observation, leaf_since=since, leaf_until=until
    )
    raw_path = _verified_user_file(
        user_dir, observation["raw_path"], observation["raw_sha256"], "raw"
    )
    telemetry_path = _verified_user_file(
        user_dir,
        observation["telemetry_path"],
        observation["telemetry_sha256"],
        "telemetry",
    )
    query, _url = legacy_query(
        handle, since, until, include_reposts=include_reposts
    )
    if hashlib.sha256(query.encode("utf-8")).hexdigest() != observation[
        "query_sha256"
    ]:
        raise archive_x.ArchiveError("legacy retained query evidence changed")
    telemetry = archive_x.load_json(telemetry_path, None)
    validate_walk_telemetry(
        telemetry,
        expected_query=query,
        request_limit=observation["request_limit"],
        empty_tail_pages=observation["empty_tail_pages"],
        exit_code=0,
        expected_user_id=requested_user_id,
        require_bound_identity=True,
    )
    records = validate_walk_records(
        raw_path,
        since=since,
        until=until,
        requested_user_id=requested_user_id,
        requested_handle=handle,
        include_reposts=include_reposts,
    )
    restored = {
        "archive_run_id": observation["archive_run_id"],
        "walk_id": observation["walk_id"],
        "since": since,
        "until": until,
        "query_sha256": observation["query_sha256"],
        "status": "valid",
        "records": records,
        "raw_path": observation["raw_path"],
        "raw_sha256": observation["raw_sha256"],
        "telemetry_path": observation["telemetry_path"],
        "telemetry_sha256": observation["telemetry_sha256"],
        "request_limit": observation["request_limit"],
        "empty_tail_pages": observation["empty_tail_pages"],
        "search_requests": observation["search_requests"],
        "api_requests": observation["api_requests"],
        "terminal_reason": telemetry.get("terminal_reason"),
    }
    descriptor_relative = observation.get("descriptor_artifact_path")
    if descriptor_relative is not None:
        _verified_user_file(
            user_dir,
            descriptor_relative,
            observation["descriptor_artifact_sha256"],
            "descriptor",
        )
        restored.update(
            {
                "descriptor_artifact_path": descriptor_relative,
                "descriptor_artifact_sha256": observation[
                    "descriptor_artifact_sha256"
                ],
                "descriptor_operation_id": observation.get(
                    "descriptor_operation_id"
                ),
                "descriptor_source_kind": "legacy",
                "descriptor_source_operation": "legacy",
            }
        )
    if walk_compatibility_sha256(restored) != observation["compatibility_sha256"]:
        raise archive_x.ArchiveError(
            "legacy retained observation compatibility changed"
        )
    return restored


def confirmation_from_retained(
    user_dir: Path,
    observations: list[dict[str, Any]],
    *,
    handle: str,
    requested_user_id: str,
    include_reposts: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    restored = [
        restore_retained_observation(
            user_dir,
            observation,
            handle=handle,
            requested_user_id=requested_user_id,
            include_reposts=include_reposts,
        )
        for observation in observations
    ]
    digests = {walk_compatibility_sha256(result) for result in restored}
    if len(digests) > 1:
        raise archive_x.ArchiveError(
            "independent legacy observations returned incompatible evidence"
        )
    if len(restored) < 2:
        return None
    return compatible_walk_records(restored[0], restored[1]), restored[:2]


def update_adaptive_window_policy(
    legacy: dict[str, Any],
    *,
    confirmed_observations: list[dict[str, Any]],
    request_limit: int,
    empty_tail_pages: int,
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    if legacy["status"] != "active" or not confirmed_observations:
        raise archive_x.ArchiveError("legacy adaptive policy lacks confirmed work")
    active = legacy["active_window"]
    policy = copy.deepcopy(
        legacy.get("window_policy")
        or new_window_policy(
            int(
                (
                    parse_utc(active["until"], "active until")
                    - parse_utc(active["since"], "active since")
                ).total_seconds()
            )
        )
    )
    validate_window_policy(policy)
    search_requests = max(
        _positive_counter(
            item["search_requests"], "adaptive search requests", allow_zero=False
        )
        for item in confirmed_observations
    )
    api_requests = max(
        _positive_counter(
            item["api_requests"], "adaptive API requests", allow_zero=False
        )
        for item in confirmed_observations
    )
    post_count = sum(
        _positive_counter(item["accepted_count"], "adaptive accepted records")
        for item in confirmed_observations[::2]
    )
    leaf_count = len(active["leaves"])
    current = int(policy["next_window_seconds"])
    if leaf_count > 1 or search_requests >= request_limit - 1:
        next_seconds = max(int(policy["minimum_seconds"]), current // 2)
        decision = "dense_shrink"
    elif search_requests <= empty_tail_pages + 1 and post_count <= 20:
        next_seconds = min(int(policy["maximum_seconds"]), current * 2)
        decision = "sparse_grow" if next_seconds > current else "steady"
    else:
        next_seconds = current
        decision = "steady"
    actual_seconds = int(
        (
            parse_utc(active["until"], "active until")
            - parse_utc(active["since"], "active since")
        ).total_seconds()
    )
    if actual_seconds < current and active["since"] == legacy["floor_since"]:
        decision = "floor_clip"
    policy.update(
        {
            "next_window_seconds": next_seconds,
            "last_decision": decision,
            "last_search_requests": search_requests,
            "last_api_requests": api_requests,
            "last_post_count": post_count,
            "last_leaf_count": leaf_count,
            "last_window_id": active["window_id"],
        }
    )
    validate_window_policy(policy)
    updated = copy.deepcopy(legacy)
    updated["window_policy"] = policy
    validate_legacy_state(updated, expected_user_id=updated["requested_user_id"])
    return updated


def public_walk_result(result: dict[str, Any]) -> dict[str, Any]:
    value = {
        key: item
        for key, item in result.items()
        if key not in {"records", "command"}
    }
    records = result.get("records")
    if isinstance(records, dict):
        value["records"] = {
            "raw_count": records.get("raw_count"),
            "accepted_count": records.get("accepted_count"),
            "accepted_ids": records.get("accepted_ids"),
            "overlap_excluded_ids": records.get("overlap_excluded_ids"),
        }
    return value


def retry_manual_review(
    legacy: dict[str, Any],
    *,
    window_id_value: str,
    operator_reason: str,
    retried_at: str,
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    review = legacy.get("manual_review")
    if legacy["status"] != "manual_review" or not isinstance(review, dict):
        raise archive_x.ArchiveError("legacy backfill is not in manual review")
    if review.get("window_id") != window_id_value:
        raise archive_x.ArchiveError("legacy manual-review retry window guard failed")
    if review.get("until") != legacy["next_until"] or window_id(
        str(review.get("since")), str(review.get("until"))
    ) != window_id_value:
        raise archive_x.ArchiveError("legacy manual-review retry frontier is stale")
    if not operator_reason or len(operator_reason) > 500:
        raise archive_x.ArchiveError("legacy retry reason is invalid")
    updated = copy.deepcopy(legacy)
    updated["status"] = "pending"
    updated["manual_review"] = None
    updated["last_manual_retry"] = {
        "window_id": window_id_value,
        "prior_reason": review.get("reason"),
        "operator_reason": operator_reason,
        "retried_at": second_utc(parse_utc(retried_at, "retried_at")),
    }
    validate_legacy_state(updated, expected_user_id=updated["requested_user_id"])
    return updated


def recover_legacy_manifests(
    user_dir: Path, state: dict[str, Any], *, recovered_at: str
) -> list[dict[str, str]]:
    legacy = state.get("legacy_backfill")
    if legacy is None:
        return []
    validate_legacy_state(
        legacy, expected_user_id=str(state.get("requested_user_id") or "")
    )
    completed = legacy.get("last_completed_window")
    recovered = []
    for manifest_path in sorted((user_dir / "runs").glob("*/manifest.json")):
        manifest = archive_x.load_json(manifest_path, None)
        if not isinstance(manifest, dict) or manifest.get("status") != "running":
            continue
        if manifest.get("mode") != "legacy_backfill":
            continue
        windows = manifest.get("windows")
        uncertain = windows[-1] if isinstance(windows, list) and windows else None
        committed = False
        if (
            isinstance(uncertain, dict)
            and uncertain.get("metadata_confirmed") is True
            and isinstance(completed, dict)
            and str(manifest.get("requested_user_id") or "")
            == legacy["requested_user_id"]
            and uncertain.get("window_id") == completed.get("window_id")
            and uncertain.get("since") == completed.get("since")
            and uncertain.get("until") == completed.get("until")
            and uncertain.get("canonical_raw_sha256")
            == completed.get("canonical_raw_sha256")
        ):
            relative = uncertain.get("canonical_raw_path")
            canonical = user_dir / str(relative) if relative else None
            if (
                canonical is not None
                and canonical.is_file()
                and archive_x.sha256_file(canonical)
                == completed.get("canonical_raw_sha256")
            ):
                committed = True
        if committed:
            uncertain["state_committed"] = True
            uncertain["dataset_sha256"] = completed.get("dataset_sha256")
            uncertain["status"] = "success"
            uncertain["recovered_after_state_commit"] = True
            manifest["status"] = "recovered_success"
            manifest["next_until"] = legacy["next_until"]
            outcome = "recovered_success"
        else:
            if isinstance(uncertain, dict) and uncertain.get("status") == "running":
                uncertain["status"] = "interrupted"
            manifest["status"] = "interrupted"
            manifest["failure_stage"] = "legacy_process_ended_before_state_commit"
            outcome = "interrupted"
        manifest["completed_at"] = second_utc(
            parse_utc(recovered_at, "recovered_at")
        )
        manifest["finalized_on_later_legacy_startup"] = True
        archive_x.atomic_write_json(manifest_path, manifest)
        recovered.append(
            {
                "run_id": str(manifest.get("run_id") or manifest_path.parent.name),
                "status": outcome,
            }
        )
    return recovered


def run_legacy_archive(
    options: LegacyRunOptions,
    repo_dir: Path,
    archive_root: Path,
    handle: str,
    version: str,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    runner: Any | None = None,
) -> dict[str, Any]:
    def report(event: str, **details: Any) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback({"event": event, **details})
        except Exception:
            # Observability must never change an archival outcome.
            pass

    options.validate()
    user_dir, state_path = state_paths(archive_root, handle)
    state = archive_x.load_json(state_path, None)
    if not isinstance(state, dict):
        raise archive_x.ArchiveError("archive state is missing or invalid")
    legacy = state.get("legacy_backfill")
    if legacy is None:
        raise archive_x.ArchiveError(
            "legacy backfill is not initialized; run plan and guarded init first"
        )
    validate_legacy_state(
        legacy, expected_user_id=str(state.get("requested_user_id") or "")
    )
    if legacy["status"] == "manual_review":
        raise archive_x.ArchiveError(
            "legacy backfill requires manual review before it can run"
        )
    started = archive_x.utc_now()
    recovered_manifests = recover_legacy_manifests(
        user_dir, state, recovered_at=second_utc(started)
    )
    current_run_id = archive_x.run_id(started)
    run_dir = user_dir / "runs" / current_run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    archive_x.write_dataset_readme(user_dir)
    manifest_path = run_dir / "manifest.json"
    manifest: dict[str, Any] = {
        "schema": archive_x.SCHEMA_NAME,
        "schema_version": archive_x.SCHEMA_VERSION,
        "legacy_schema_version": LEGACY_SCHEMA_VERSION,
        "run_id": current_run_id,
        "mode": "legacy_backfill",
        "requested_handle": handle,
        "requested_user_id": legacy["requested_user_id"],
        "started_at": second_utc(started),
        "status": "running",
        "gallery_dl_version": version,
        "window_limit": options.max_root_windows,
        "root_window_days": options.root_window_days,
        "request_limit": options.request_limit,
        "empty_tail_pages": options.empty_tail_pages,
        "walk_attempt_limit": options.walk_attempts,
        "window_attempt_limit": options.window_attempts,
        "max_leaves": options.max_leaves,
        "recovered_manifests": recovered_manifests,
        "windows": [],
    }
    archive_x.atomic_write_json(manifest_path, manifest)

    def flush_portable_export() -> None:
        nonlocal state, legacy
        state, export_result = flush_pending_portable_exports(
            user_dir,
            state,
            state_path=state_path,
            run_dir=run_dir,
            handle=handle,
        )
        legacy = state["legacy_backfill"]
        if export_result is not None:
            manifest["portable_export"] = export_result
            for result in manifest["windows"]:
                if result.get("state_committed"):
                    result["portable_export_pending"] = False
            archive_x.atomic_write_json(manifest_path, manifest)

    if legacy["status"] == "complete":
        flush_portable_export()
        manifest["status"] = "complete"
        manifest["completed_at"] = second_utc(archive_x.utc_now())
        archive_x.atomic_write_json(manifest_path, manifest)
        return manifest
    if legacy["status"] == "active":
        legacy = resume_active_window(
            legacy,
            owner_run_id=current_run_id,
            resumed_at=second_utc(started),
            attempt_limit=options.window_attempts,
        )
        state["legacy_backfill"] = legacy
        archive_x.atomic_write_json(state_path, state)
        if legacy["status"] == "manual_review":
            flush_portable_export()
            manifest["status"] = "manual_review"
            manifest["completed_at"] = second_utc(archive_x.utc_now())
            archive_x.atomic_write_json(manifest_path, manifest)
            return manifest

    completed_count = 0
    while (
        options.max_root_windows is None
        or completed_count < options.max_root_windows
    ):
        legacy = state["legacy_backfill"]
        if legacy["status"] == "complete":
            break
        if legacy["status"] == "pending":
            legacy = claim_window(
                legacy,
                owner_run_id=current_run_id,
                claimed_at=second_utc(archive_x.utc_now()),
                root_window_days=options.root_window_days,
            )
            state["legacy_backfill"] = legacy
            archive_x.atomic_write_json(state_path, state)
        active = legacy["active_window"]
        window_result: dict[str, Any] = {
            "window_id": active["window_id"],
            "since": active["since"],
            "until": active["until"],
            "status": "running",
            "walks": [],
            "splits": [],
        }
        manifest["windows"].append(window_result)
        archive_x.atomic_write_json(manifest_path, manifest)
        report(
            "window_started",
            since=active["since"],
            until=active["until"],
            committed_windows=completed_count,
        )
        confirmed_leaf_keys: set[tuple[str, str]] = set()
        canonical_by_id: dict[str, dict[str, Any]] = {}
        confirmed_walk_ids: list[str] = []
        confirmed_descriptor_results: list[dict[str, Any]] = []
        confirmed_policy_observations: list[dict[str, Any]] = []

        while True:
            legacy = state["legacy_backfill"]
            leaves = legacy["active_window"]["leaves"]
            pending = [
                leaf
                for leaf in reversed(leaves)
                if (leaf["since"], leaf["until"]) not in confirmed_leaf_keys
            ]
            if not pending:
                break
            leaf = pending[0]
            confirmed_records = None
            confirmed_results: list[dict[str, Any]] = []
            evidence_error = None
            split = False
            canonical_handle = str(state.get("canonical_handle") or handle)
            try:
                existing_confirmation = confirmation_from_retained(
                    user_dir,
                    list(leaf.get("observations") or []),
                    handle=canonical_handle,
                    requested_user_id=legacy["requested_user_id"],
                    include_reposts=legacy["source"]["reposts_included"],
                )
            except archive_x.ArchiveError as exc:
                evidence_error = str(exc)
                existing_confirmation = None
            if existing_confirmation is not None:
                confirmed_records, confirmed_results = existing_confirmation
                window_result["retained_observations_reused"] = int(
                    window_result.get("retained_observations_reused") or 0
                ) + 2
            for attempt in range(1, options.walk_attempts + 1):
                if confirmed_records is not None or evidence_error is not None:
                    break
                leaf_token = canonical_sha256(
                    {"since": leaf["since"], "until": leaf["until"]}
                )[:12]
                run_token = canonical_sha256(current_run_id)[:8]
                result = run_legacy_walk(
                    repo_dir=repo_dir,
                    archive_root=archive_root,
                    user_dir=user_dir,
                    run_dir=run_dir,
                    handle=canonical_handle,
                    requested_user_id=legacy["requested_user_id"],
                    archive_run_id=current_run_id,
                    window_id_value=active["window_id"],
                    walk_id=f"{leaf_token}-{run_token}-walk-{attempt}",
                    since=leaf["since"],
                    until=leaf["until"],
                    cookie_file=options.cookies,
                    request_delay=options.request_delay,
                    include_reposts=legacy["source"]["reposts_included"],
                    request_limit=options.request_limit,
                    empty_tail_pages=options.empty_tail_pages,
                    retries=options.retries,
                    http_timeout=options.http_timeout,
                    stalled_rate_limit_cycles=options.stalled_rate_limit_cycles,
                    runner=runner,
                )
                window_result["walks"].append(public_walk_result(result))
                archive_x.atomic_write_json(manifest_path, manifest)
                report(
                    "walk_completed",
                    since=active["since"],
                    until=active["until"],
                    attempt=attempt,
                    committed_windows=completed_count,
                )
                if result["interrupted"]:
                    manifest["status"] = "interrupted"
                    manifest["completed_at"] = second_utc(archive_x.utc_now())
                    archive_x.atomic_write_json(manifest_path, manifest)
                    raise KeyboardInterrupt
                if result["terminal_reason"] == "request_cap":
                    try:
                        updated = split_active_leaf(
                            legacy,
                            leaf_since=leaf["since"],
                            leaf_until=leaf["until"],
                            max_leaves=options.max_leaves,
                        )
                    except archive_x.ArchiveError as exc:
                        updated = mark_manual_review(
                            legacy,
                            window_id_value=active["window_id"],
                            reason=str(exc),
                            observed_at=second_utc(archive_x.utc_now()),
                        )
                        state["legacy_backfill"] = updated
                        archive_x.atomic_write_json(state_path, state)
                        flush_portable_export()
                        window_result["status"] = "manual_review"
                        window_result["reason"] = str(exc)
                        manifest["status"] = "manual_review"
                        manifest["completed_at"] = second_utc(archive_x.utc_now())
                        archive_x.atomic_write_json(manifest_path, manifest)
                        return manifest
                    state["legacy_backfill"] = updated
                    archive_x.atomic_write_json(state_path, state)
                    window_result["splits"].append(
                        {"since": leaf["since"], "until": leaf["until"]}
                    )
                    archive_x.atomic_write_json(manifest_path, manifest)
                    split = True
                    break
                if result["status"] != "valid":
                    continue
                observation = retained_observation(user_dir, result)
                updated, _inserted = append_retained_observation(
                    state["legacy_backfill"],
                    leaf_since=leaf["since"],
                    leaf_until=leaf["until"],
                    observation=observation,
                )
                state["legacy_backfill"] = updated
                archive_x.atomic_write_json(state_path, state)
                legacy = updated
                leaf = next(
                    item
                    for item in legacy["active_window"]["leaves"]
                    if item["since"] == leaf["since"]
                    and item["until"] == leaf["until"]
                )
                try:
                    confirmation = confirmation_from_retained(
                        user_dir,
                        list(leaf.get("observations") or []),
                        handle=canonical_handle,
                        requested_user_id=legacy["requested_user_id"],
                        include_reposts=legacy["source"]["reposts_included"],
                    )
                except archive_x.ArchiveError as exc:
                    evidence_error = str(exc)
                    break
                if confirmation is not None:
                    confirmed_records, confirmed_results = confirmation
                    break
            if split:
                continue
            if evidence_error is not None:
                reason = evidence_error
                updated = mark_manual_review(
                    state["legacy_backfill"],
                    window_id_value=active["window_id"],
                    reason=reason,
                    observed_at=second_utc(archive_x.utc_now()),
                )
                state["legacy_backfill"] = updated
                archive_x.atomic_write_json(state_path, state)
                flush_portable_export()
                window_result["status"] = "manual_review"
                window_result["reason"] = reason
                manifest["status"] = "manual_review"
                manifest["completed_at"] = second_utc(archive_x.utc_now())
                archive_x.atomic_write_json(manifest_path, manifest)
                return manifest
            if confirmed_records is None:
                reason = (
                    f"no two matching valid legacy observations after "
                    f"{options.walk_attempts} attempts"
                )
                updated = mark_manual_review(
                    state["legacy_backfill"],
                    window_id_value=active["window_id"],
                    reason=reason,
                    observed_at=second_utc(archive_x.utc_now()),
                )
                state["legacy_backfill"] = updated
                archive_x.atomic_write_json(state_path, state)
                flush_portable_export()
                window_result["status"] = "manual_review"
                window_result["reason"] = reason
                manifest["status"] = "manual_review"
                manifest["completed_at"] = second_utc(archive_x.utc_now())
                archive_x.atomic_write_json(manifest_path, manifest)
                return manifest
            selected_observations = list(leaf.get("observations") or [])[:2]
            confirmed_walk_ids.extend(
                observation["observation_id"]
                for observation in selected_observations
            )
            confirmed_policy_observations.extend(selected_observations)
            confirmed_descriptor_results.extend(confirmed_results)
            for metadata in confirmed_records:
                post_id = archive_x.id_string(metadata.get("tweet_id"))
                if post_id in canonical_by_id:
                    raise archive_x.ArchiveError(
                        f"legacy subdivision returned duplicate post {post_id}"
                    )
                canonical_by_id[post_id] = metadata
            confirmed_leaf_keys.add((leaf["since"], leaf["until"]))

        canonical_path = run_dir / "raw" / f"{active['window_id']}.posts.jsonl"
        canonical_records = sorted(
            canonical_by_id.values(),
            key=lambda record: (
                str(record.get("date") or ""),
                int(archive_x.id_string(record.get("tweet_id")) or 0),
            ),
        )
        archive_x.atomic_write_jsonl(canonical_path, canonical_records)
        canonical_hash = archive_x.sha256_file(canonical_path)
        window_result["canonical_raw_path"] = str(canonical_path.relative_to(user_dir))
        window_result["canonical_raw_sha256"] = canonical_hash
        window_result["canonical_post_count"] = len(canonical_records)
        window_result["metadata_confirmed"] = True
        archive_x.atomic_write_json(manifest_path, manifest)

        policy_legacy = update_adaptive_window_policy(
            state["legacy_backfill"],
            confirmed_observations=confirmed_policy_observations,
            request_limit=options.request_limit,
            empty_tail_pages=options.empty_tail_pages,
        )
        observed_at = second_utc(archive_x.utc_now())
        indexed = commit_indexed_legacy_window(
            user_dir,
            canonical_path=canonical_path,
            canonical_hash=canonical_hash,
            canonical_records=canonical_records,
            handle=str(state.get("canonical_handle") or handle),
            requested_user_id=legacy["requested_user_id"],
            run_id=current_run_id,
            window_id_value=active["window_id"],
            since=active["since"],
            until=active["until"],
            observation_ids=confirmed_walk_ids,
            observed_at=observed_at,
        )
        updated_state = copy.deepcopy(state)
        policy_legacy = enqueue_pending_portable_export(
            policy_legacy,
            window_id_value=active["window_id"],
            since=active["since"],
            until=active["until"],
            canonical_raw_path=str(canonical_path.relative_to(user_dir)),
            canonical_raw_sha256=canonical_hash,
            indexed_generation=int(indexed["generation"]),
        )
        updated_state["legacy_backfill"] = complete_window(
            policy_legacy,
            window_id_value=active["window_id"],
            completed_at=observed_at,
            canonical_raw_sha256=canonical_hash,
            dataset_sha256=None,
            walk_ids=confirmed_walk_ids,
            indexed_generation=int(indexed["generation"]),
        )
        archive_x.atomic_write_json(state_path, updated_state)
        state = updated_state
        window_result["descriptor_commit"] = archive_x.persist_descriptor_evidence(
            user_dir,
            target_user_id=legacy["requested_user_id"],
            canonical_handle=str(state.get("canonical_handle") or handle),
            accepted_records=canonical_records,
            endpoint_results=confirmed_descriptor_results,
        )
        window_result["indexed_commit"] = indexed
        window_result["portable_export_pending"] = True
        window_result["state_committed"] = True
        window_result["status"] = "success"
        archive_x.atomic_write_json(manifest_path, manifest)
        completed_count += 1
        report(
            "window_committed",
            since=active["since"],
            until=active["until"],
            committed_windows=completed_count,
            new_posts=int(indexed.get("new_posts") or 0),
            dataset_posts=0,
        )
    flush_portable_export()
    manifest["status"] = (
        "complete"
        if state["legacy_backfill"]["status"] == "complete"
        else "limited"
    )
    manifest["completed_at"] = second_utc(archive_x.utc_now())
    manifest["next_until"] = state["legacy_backfill"]["next_until"]
    archive_x.atomic_write_json(manifest_path, manifest)
    return manifest


def claim_window(
    legacy: dict[str, Any],
    *,
    owner_run_id: str,
    claimed_at: str,
    root_window_days: int = 1,
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    if legacy["status"] != "pending":
        raise archive_x.ArchiveError("legacy frontier is not ready to claim")
    if not isinstance(root_window_days, int) or root_window_days < 1:
        raise archive_x.ArchiveError("legacy root-window day count must be positive")
    until = parse_utc(legacy["next_until"], "next_until")
    floor = parse_utc(legacy["floor_since"], "floor_since")
    if until == floor:
        raise archive_x.ArchiveError("legacy frontier is already at its floor")
    policy = legacy.get("window_policy")
    if policy is None:
        policy = new_window_policy(root_window_days * 24 * 60 * 60)
    else:
        validate_window_policy(policy)
    window_seconds = int(policy["next_window_seconds"])
    since = max(floor, until - timedelta(seconds=window_seconds))
    since_text, until_text = second_utc(since), second_utc(until)
    updated = copy.deepcopy(legacy)
    updated["status"] = "active"
    updated["window_policy"] = copy.deepcopy(policy)
    updated["active_window"] = {
        "window_id": window_id(since_text, until_text),
        "since": since_text,
        "until": until_text,
        "owner_run_id": owner_run_id,
        "attempt": 1,
        "claimed_at": second_utc(parse_utc(claimed_at, "claimed_at")),
        "leaves": [
            {
                "since": since_text,
                "until": until_text,
                "status": "pending",
                "observations": [],
            }
        ],
    }
    validate_legacy_state(updated, expected_user_id=updated["requested_user_id"])
    return updated


def mark_manual_review(
    legacy: dict[str, Any], *, window_id_value: str, reason: str, observed_at: str
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    active = legacy.get("active_window")
    if legacy["status"] != "active" or active["window_id"] != window_id_value:
        raise archive_x.ArchiveError("legacy manual-review window guard failed")
    if not reason or len(reason) > 500:
        raise archive_x.ArchiveError("legacy manual-review reason is invalid")
    updated = copy.deepcopy(legacy)
    updated["status"] = "manual_review"
    updated["manual_review"] = {
        "window_id": window_id_value,
        "since": active["since"],
        "until": active["until"],
        "reason": reason,
        "observed_at": second_utc(parse_utc(observed_at, "observed_at")),
    }
    updated["active_window"] = None
    validate_legacy_state(updated, expected_user_id=updated["requested_user_id"])
    return updated


def complete_window(
    legacy: dict[str, Any],
    *,
    window_id_value: str,
    completed_at: str,
    canonical_raw_sha256: str,
    dataset_sha256: str | None,
    walk_ids: list[str],
    indexed_generation: int | None = None,
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    active = legacy.get("active_window")
    if legacy["status"] != "active" or active["window_id"] != window_id_value:
        raise archive_x.ArchiveError("legacy completion window guard failed")
    require_sha256(canonical_raw_sha256, "canonical raw hash")
    if dataset_sha256 is not None:
        require_sha256(dataset_sha256, "dataset hash")
    if indexed_generation is not None:
        _positive_counter(
            indexed_generation, "indexed generation", allow_zero=False
        )
    if dataset_sha256 is None and indexed_generation is None:
        raise archive_x.ArchiveError(
            "legacy completion lacks indexed or portable dataset authority"
        )
    if (
        len(walk_ids) < 2
        or len(walk_ids) % 2
        or len(set(walk_ids)) != len(walk_ids)
        or not all(walk_ids)
    ):
        raise archive_x.ArchiveError(
            "legacy completion requires distinct confirmed walk pairs"
        )
    updated = copy.deepcopy(legacy)
    updated["next_until"] = active["since"]
    updated["active_window"] = None
    completed = {
        "window_id": window_id_value,
        "since": active["since"],
        "until": active["until"],
        "completed_at": second_utc(parse_utc(completed_at, "completed_at")),
        "canonical_raw_sha256": canonical_raw_sha256,
        "walk_ids": sorted(walk_ids),
    }
    if dataset_sha256 is not None:
        completed["dataset_sha256"] = dataset_sha256
    if indexed_generation is not None:
        completed["indexed_generation"] = indexed_generation
        completed["portable_export_pending"] = True
    updated["last_completed_window"] = completed
    if updated["next_until"] == updated["floor_since"]:
        updated["status"] = "complete"
        updated["coverage_conclusion"] = "source_visible_to_account_creation"
    else:
        updated["status"] = "pending"
    validate_legacy_state(updated, expected_user_id=updated["requested_user_id"])
    return updated


def state_paths(archive_root: Path, handle: str) -> tuple[Path, Path]:
    user_dir = archive_root / "users" / handle
    return user_dir, user_dir / "_state" / "state.json"


def print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def legacy_status_summary(state: dict[str, Any], handle: str) -> dict[str, Any]:
    legacy = state.get("legacy_backfill")
    if legacy is None:
        return {
            "handle": handle,
            "status": "not_initialized",
            "network_requests": 0,
            "writes": 0,
            "next_command": f"scripts/archive-x-legacy --user {handle} plan",
        }
    validate_legacy_state(
        legacy, expected_user_id=str(state.get("requested_user_id") or "")
    )
    frontier = parse_utc(legacy["next_until"], "next_until")
    floor = parse_utc(legacy["floor_since"], "floor_since")
    next_window = None
    if frontier > floor:
        policy = legacy.get("window_policy")
        next_seconds = (
            int(validate_window_policy(policy)["next_window_seconds"])
            if policy is not None
            else DEFAULT_ROOT_WINDOW_DAYS * 24 * 60 * 60
        )
        next_since = max(
            floor, frontier - timedelta(seconds=next_seconds)
        )
        next_window = {
            "since": second_utc(next_since),
            "until": second_utc(frontier),
        }
    status = legacy["status"]
    if status == "manual_review":
        review = legacy["manual_review"]
        next_command = (
            f"scripts/archive-x-legacy --user {handle} retry "
            f"--window-id {review['window_id']} --reason REASON"
        )
    elif status == "complete":
        next_command = None
    else:
        next_command = f"scripts/archive-x-legacy --user {handle} run"
    source = legacy["source"]
    return {
        "handle": handle,
        "requested_user_id": legacy["requested_user_id"],
        "status": status,
        "coverage_conclusion": legacy["coverage_conclusion"],
        "coverage": {
            "source_visible_since": legacy["next_until"],
            "through_exclusive": legacy["initial_until"],
            "account_creation_floor": legacy["floor_since"],
            "meaning": (
                "source-visible, repeat-confirmed posts returned by X for "
                "contiguous UTC windows; "
                "not proof of deleted, private, withheld, or unindexed posts"
            ),
        },
        "source_boundary": {
            "run_id": source["run_id"],
            "cursor": source["cursor"],
            "oldest_post_id": source["oldest_post_id"],
            "oldest_post_at": source["oldest_post_at"],
        },
        "next_window": next_window,
        "active_window": legacy.get("active_window"),
        "last_completed_window": legacy.get("last_completed_window"),
        "window_policy": legacy.get("window_policy"),
        "pending_portable_export_count": len(
            legacy.get("pending_portable_exports") or []
        ),
        "manual_review": legacy.get("manual_review"),
        "pending_media_count": len(
            state.get("pending_media")
            if isinstance(state.get("pending_media"), list)
            else []
        ),
        "modern_cursor_preserved": str(
            (state.get("resume") or {}).get("cursor") or ""
        ),
        "network_requests": 0,
        "writes": 0,
        "next_command": next_command,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scripts/archive-x-legacy")
    parser.add_argument("--user", required=True, help="one X handle or profile URL")
    parser.add_argument("--output-root", type=Path)
    parser.add_argument(
        "--cookies",
        type=Path,
        default=Path(__file__).resolve().parent.parent
        / "state"
        / "cookies"
        / "x.cookies.txt",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="show local legacy state; no writes/network")
    commands.add_parser("plan", help="derive guarded initialization; no writes/network")
    init = commands.add_parser("init", help="atomically initialize from a plan token")
    init.add_argument("--token", required=True)
    retry = commands.add_parser(
        "retry", help="return one exact manual-review window to pending"
    )
    retry.add_argument("--window-id", required=True)
    retry.add_argument("--reason", required=True)
    run = commands.add_parser(
        "run", help="resume initialized legacy history; optional diagnostic bound"
    )
    run.add_argument(
        "--windows",
        type=archive_x.positive_int,
        help="advanced: stop after this many committed root UTC windows",
    )
    run.add_argument("--request-limit", type=archive_x.positive_int, default=6)
    run.add_argument(
        "--root-window-days",
        type=archive_x.positive_int,
        default=DEFAULT_ROOT_WINDOW_DAYS,
        help="advanced: target root-window width; saturated windows split safely",
    )
    run.add_argument(
        "--empty-tail-pages",
        type=archive_x.positive_int,
        default=DEFAULT_EMPTY_TAIL_PAGES,
        help="advanced: distinct empty pages required per independent walk",
    )
    run.add_argument("--walk-attempts", type=archive_x.positive_int, default=3)
    run.add_argument("--window-attempts", type=archive_x.positive_int, default=3)
    run.add_argument("--max-leaves", type=archive_x.positive_int, default=64)
    run.add_argument(
        "--request-delay", type=archive_x.duration_arg, default="4-8"
    )
    run.add_argument("--walk-delay", type=archive_x.duration_arg, default="10-20")
    run.add_argument(
        "--window-delay", type=archive_x.duration_arg, default="5-15"
    )
    run.add_argument("--retries", type=archive_x.positive_int, default=1)
    run.add_argument("--http-timeout", type=archive_x.positive_int, default=60)
    run.add_argument(
        "--stalled-rate-limit-cycles",
        type=archive_x.positive_int,
        default=3,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "run" and args.walk_attempts < 2:
        parser.error("legacy run requires at least two walk attempts")
    if (
        args.command == "run"
        and args.empty_tail_pages >= args.request_limit
    ):
        parser.error("legacy empty-tail pages must be below the request limit")
    args.cookies = args.cookies.expanduser().resolve()
    try:
        handle = archive_x.normalize_handle(args.user)
        archive_root = archive_x.resolve_output_root(args.output_root, plan_only=True)
        user_dir, state_path = state_paths(archive_root, handle)
        if args.command == "status":
            state = archive_x.load_json(state_path, None)
            if not isinstance(state, dict):
                raise archive_x.ArchiveError("archive state is missing or invalid")
            print_json(legacy_status_summary(state, handle))
            return 0
        if args.command == "plan":
            print_json(initialization_plan(user_dir))
            return 0
        if args.command == "run":
            archive_root = archive_x.resolve_output_root(
                args.output_root, plan_only=False
            )
            _, preflight_state_path = state_paths(archive_root, handle)
            preflight_state = archive_x.load_json(preflight_state_path, None)
            if not isinstance(preflight_state, dict) or preflight_state.get(
                "legacy_backfill"
            ) is None:
                raise archive_x.ArchiveError(
                    "legacy backfill is not initialized; run plan and guarded init first"
                )
            validate_legacy_state(
                preflight_state["legacy_backfill"],
                expected_user_id=str(
                    preflight_state.get("requested_user_id") or ""
                ),
            )
            archive_x.validate_cookie_file(args.cookies)
            version = archive_x.gallery_dl_version()
            verify_legacy_runner(Path(__file__).resolve().parent.parent, version)
            repo_dir = Path(__file__).resolve().parent.parent
            with archive_x.exclusive_lock(
                repo_dir / "state" / "locks" / "archive-x.lock"
            ), archive_x.exclusive_lock(
                archive_root / "_state" / "archive-x.lock"
            ):
                try:
                    result = run_legacy_archive(
                        LegacyRunOptions.from_namespace(args),
                        repo_dir,
                        archive_root,
                        handle,
                        version,
                    )
                except KeyboardInterrupt:
                    print(
                        "Interrupted; legacy window remains active and will replay safely."
                    )
                    return 130
            print_json(
                {
                    "handle": handle,
                    "run_id": result["run_id"],
                    "status": result["status"],
                    "next_until": result.get("next_until"),
                }
            )
            return 0 if result["status"] in {"limited", "complete"} else 1

        repo_dir = Path(__file__).resolve().parent.parent
        archive_root = archive_x.resolve_output_root(args.output_root, plan_only=False)
        with archive_x.exclusive_lock(
            repo_dir / "state" / "locks" / "archive-x.lock"
        ), archive_x.exclusive_lock(archive_root / "_state" / "archive-x.lock"):
            state = archive_x.load_json(state_path, None)
            if not isinstance(state, dict):
                raise archive_x.ArchiveError("archive state is missing or invalid")
            if args.command == "retry":
                legacy = state.get("legacy_backfill")
                if legacy is None:
                    raise archive_x.ArchiveError("legacy backfill is not initialized")
                state["legacy_backfill"] = retry_manual_review(
                    legacy,
                    window_id_value=args.window_id,
                    operator_reason=args.reason,
                    retried_at=second_utc(archive_x.utc_now()),
                )
                archive_x.atomic_write_json(state_path, state)
                print_json(
                    {
                        "handle": handle,
                        "retried": True,
                        "legacy_backfill": state["legacy_backfill"],
                    }
                )
                return 0
            if state.get("legacy_backfill") is not None:
                updated, changed = initialize_state(
                    state, {}, args.token, second_utc(archive_x.utc_now())
                )
            else:
                plan = initialization_plan(user_dir)
                updated, changed = initialize_state(
                    state, plan, args.token, second_utc(archive_x.utc_now())
                )
            if changed:
                backup_path = (
                    user_dir
                    / "_state"
                    / "backups"
                    / f"state.pre-legacy-init-{args.token[:12]}.json"
                )
                if backup_path.exists():
                    backup = archive_x.load_json(backup_path, None)
                    if backup != state:
                        raise archive_x.ArchiveError(
                            "legacy initialization backup already exists with "
                            "different content"
                        )
                else:
                    archive_x.atomic_write_json(backup_path, state)
                archive_x.atomic_write_json(state_path, updated)
            print_json(
                {
                    "handle": handle,
                    "initialized": changed,
                    "idempotent": not changed,
                    "legacy_backfill": updated["legacy_backfill"],
                }
            )
        return 0
    except archive_x.ArchiveError as exc:
        parser.exit(2, f"archive-x-legacy: {exc}\n")
    except OSError as exc:
        parser.exit(2, f"archive-x-legacy: filesystem or process error: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
