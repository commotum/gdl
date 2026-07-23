#!/usr/bin/env python3
"""Fail-closed, date-windowed backfill for pre-Snowflake X history."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import archive_x


LEGACY_SCHEMA_VERSION = 1
LEGACY_STATUSES = {"pending", "active", "manual_review", "complete"}
TOKEN_RE = re.compile(r"[0-9a-f]{64}\Z")
SHA256_RE = TOKEN_RE
CURSOR_RE = re.compile(r"3_(\d+)/\Z")
LEGACY_TERMINAL_REASONS = {"no_cursor", "distinct_empty_tail"}


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
    if expected_user_id is not None and not expected_user_id.isdecimal():
        raise archive_x.ArchiveError("legacy expected numeric account ID is invalid")


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
    for leaf in leaves:
        if not isinstance(leaf, dict) or set(leaf) != {"since", "until", "status"}:
            raise archive_x.ArchiveError("legacy active leaf is malformed")
        leaf_since = parse_utc(leaf["since"], "active leaf since")
        leaf_until = parse_utc(leaf["until"], "active leaf until")
        if leaf_since != expected or not leaf_since < leaf_until:
            raise archive_x.ArchiveError("legacy active leaves are not contiguous")
        if leaf["status"] not in {"pending", "confirmed"}:
            raise archive_x.ArchiveError("legacy active leaf status is invalid")
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
) -> dict[str, Any]:
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
            "search-stop": 3,
            "quoted": False,
            "expand": False,
            "showreplies": False,
            "cards": False,
            "videos": False,
            "previews": False,
            "articles": False,
        }
    )
    return config


def legacy_gallery_command(
    repo_dir: Path,
    config_path: Path,
    telemetry_path: Path,
    *,
    request_limit: int,
    retries: int,
    http_timeout: int,
    url: str,
) -> list[str]:
    if request_limit < 1:
        raise archive_x.ArchiveError("legacy request limit must be positive")
    return [
        sys.executable,
        str(repo_dir / "scripts" / "gallery_dl_x_legacy_runner.py"),
        "--archive-x-legacy-telemetry",
        str(telemetry_path),
        "--archive-x-legacy-request-limit",
        str(request_limit),
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
        "30-60",
        "--sleep-429",
        "300",
        "--retries",
        str(retries),
        url,
    ]


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
    exit_code: int,
    expected_user_id: str,
) -> dict[str, Any]:
    if not isinstance(telemetry, dict) or telemetry.get("schema_version") != 1:
        raise archive_x.ArchiveError("legacy walk telemetry schema is invalid")
    if telemetry.get("opaque_cursor_values_persisted") is not False:
        raise archive_x.ArchiveError("legacy walk telemetry may contain opaque cursors")
    if telemetry.get("request_limit") != request_limit:
        raise archive_x.ArchiveError("legacy walk telemetry request limit changed")
    search_requests = telemetry.get("search_requests")
    if not isinstance(search_requests, int) or not 1 <= search_requests <= request_limit:
        raise archive_x.ArchiveError("legacy walk request count is invalid")
    if telemetry.get("exit_code") != exit_code or exit_code != 0:
        raise archive_x.ArchiveError("legacy walk process did not exit successfully")
    if telemetry.get("profile_user_ids") != [expected_user_id]:
        raise archive_x.ArchiveError(
            "legacy walk profile identity does not match the archive"
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
    retries: int,
    http_timeout: int,
    stalled_rate_limit_cycles: int,
) -> dict[str, Any]:
    endpoint = f"{window_id_value}-{walk_id}"
    raw_partial = run_dir / "raw" / f"{endpoint}.posts.jsonl.partial"
    telemetry_path = run_dir / f"{endpoint}.telemetry.json"
    config_path = run_dir / f"{endpoint}.gallery-dl.json"
    log_path = run_dir / f"{endpoint}.log"
    query, url = legacy_query(
        handle, since, until, include_reposts=include_reposts
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
        request_delay=request_delay,
        include_reposts=include_reposts,
    )
    archive_x.atomic_write_json(config_path, config)
    command = legacy_gallery_command(
        repo_dir,
        config_path,
        telemetry_path,
        request_limit=request_limit,
        retries=retries,
        http_timeout=http_timeout,
        url=url,
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
    )
    telemetry = archive_x.load_json(telemetry_path, None)
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
            exit_code=status,
            expected_user_id=requested_user_id,
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
    return {
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
        "records": records,
        "raw_path": str(raw_path.relative_to(user_dir)),
        "raw_sha256": archive_x.sha256_file(raw_path),
        "telemetry_path": str(telemetry_path.relative_to(user_dir))
        if telemetry_path.exists()
        else None,
        "telemetry_sha256": archive_x.sha256_file(telemetry_path)
        if telemetry_path.exists()
        else None,
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
    if active["attempt"] >= attempt_limit:
        return mark_manual_review(
            legacy,
            window_id_value=active["window_id"],
            reason=f"window attempt limit ({attempt_limit}) reached",
            observed_at=resumed_at,
        )
    updated = copy.deepcopy(legacy)
    updated["active_window"]["owner_run_id"] = owner_run_id
    updated["active_window"]["attempt"] += 1
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
        {"since": leaf_since, "until": midpoint_text, "status": "pending"},
        {"since": midpoint_text, "until": leaf_until, "status": "pending"},
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


def public_walk_result(result: dict[str, Any]) -> dict[str, Any]:
    value = {key: item for key, item in result.items() if key != "records"}
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
    args: argparse.Namespace,
    repo_dir: Path,
    archive_root: Path,
    handle: str,
    version: str,
) -> dict[str, Any]:
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
        "window_limit": args.windows,
        "request_limit": args.request_limit,
        "walk_attempt_limit": args.walk_attempts,
        "window_attempt_limit": args.window_attempts,
        "max_leaves": args.max_leaves,
        "recovered_manifests": recovered_manifests,
        "windows": [],
    }
    archive_x.atomic_write_json(manifest_path, manifest)

    if legacy["status"] == "complete":
        manifest["status"] = "complete"
        manifest["completed_at"] = second_utc(archive_x.utc_now())
        archive_x.atomic_write_json(manifest_path, manifest)
        return manifest
    if legacy["status"] == "active":
        legacy = resume_active_window(
            legacy,
            owner_run_id=current_run_id,
            resumed_at=second_utc(started),
            attempt_limit=args.window_attempts,
        )
        state["legacy_backfill"] = legacy
        archive_x.atomic_write_json(state_path, state)
        if legacy["status"] == "manual_review":
            manifest["status"] = "manual_review"
            manifest["completed_at"] = second_utc(archive_x.utc_now())
            archive_x.atomic_write_json(manifest_path, manifest)
            return manifest

    completed_count = 0
    while completed_count < args.windows:
        legacy = state["legacy_backfill"]
        if legacy["status"] == "complete":
            break
        if legacy["status"] == "pending":
            legacy = claim_window(
                legacy,
                owner_run_id=current_run_id,
                claimed_at=second_utc(archive_x.utc_now()),
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
        confirmed_leaf_keys: set[tuple[str, str]] = set()
        canonical_by_id: dict[str, dict[str, Any]] = {}
        confirmed_walk_ids: list[str] = []

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
            previous_valid = None
            confirmed_records = None
            split = False
            for attempt in range(1, args.walk_attempts + 1):
                if attempt > 1:
                    archive_x.sleep_random(
                        args.walk_delay,
                        f"before independent legacy confirmation {attempt}",
                    )
                leaf_token = canonical_sha256(
                    {"since": leaf["since"], "until": leaf["until"]}
                )[:12]
                result = run_legacy_walk(
                    repo_dir=repo_dir,
                    archive_root=archive_root,
                    user_dir=user_dir,
                    run_dir=run_dir,
                    handle=str(state.get("canonical_handle") or handle),
                    requested_user_id=legacy["requested_user_id"],
                    archive_run_id=current_run_id,
                    window_id_value=active["window_id"],
                    walk_id=f"{leaf_token}-walk-{attempt}",
                    since=leaf["since"],
                    until=leaf["until"],
                    cookie_file=args.cookies,
                    request_delay=args.request_delay,
                    include_reposts=legacy["source"]["reposts_included"],
                    request_limit=args.request_limit,
                    retries=args.retries,
                    http_timeout=args.http_timeout,
                    stalled_rate_limit_cycles=args.stalled_rate_limit_cycles,
                )
                window_result["walks"].append(public_walk_result(result))
                archive_x.atomic_write_json(manifest_path, manifest)
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
                            max_leaves=args.max_leaves,
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
                    previous_valid = None
                    continue
                if previous_valid is not None:
                    try:
                        confirmed_records = compatible_walk_records(
                            previous_valid, result
                        )
                    except archive_x.ArchiveError:
                        previous_valid = result
                        continue
                    confirmed_walk_ids.extend(
                        [previous_valid["walk_id"], result["walk_id"]]
                    )
                    break
                previous_valid = result
            if split:
                continue
            if confirmed_records is None:
                reason = (
                    f"no two consecutive matching valid walks after "
                    f"{args.walk_attempts} attempts"
                )
                updated = mark_manual_review(
                    state["legacy_backfill"],
                    window_id_value=active["window_id"],
                    reason=reason,
                    observed_at=second_utc(archive_x.utc_now()),
                )
                state["legacy_backfill"] = updated
                archive_x.atomic_write_json(state_path, state)
                window_result["status"] = "manual_review"
                window_result["reason"] = reason
                manifest["status"] = "manual_review"
                manifest["completed_at"] = second_utc(archive_x.utc_now())
                archive_x.atomic_write_json(manifest_path, manifest)
                return manifest
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

        dataset_result = archive_x.update_post_dataset(
            user_dir, handle, canonical_path, "legacy"
        )
        dataset_path = user_dir / "dataset" / "posts.jsonl"
        dataset_hash = archive_x.sha256_file(dataset_path)
        updated_state = copy.deepcopy(state)
        enqueue_legacy_media_posts(
            updated_state,
            canonical_records,
            source_run_id=current_run_id,
            observed_at=second_utc(archive_x.utc_now()),
        )
        updated_state["legacy_backfill"] = complete_window(
            updated_state["legacy_backfill"],
            window_id_value=active["window_id"],
            completed_at=second_utc(archive_x.utc_now()),
            canonical_raw_sha256=canonical_hash,
            dataset_sha256=dataset_hash,
            walk_ids=confirmed_walk_ids,
        )
        archive_x.atomic_write_json(state_path, updated_state)
        state = updated_state
        window_result["dataset"] = dataset_result
        window_result["dataset_sha256"] = dataset_hash
        window_result["state_committed"] = True
        window_result["status"] = "success"
        archive_x.atomic_write_json(manifest_path, manifest)
        completed_count += 1
        if completed_count < args.windows and state["legacy_backfill"][
            "status"
        ] != "complete":
            archive_x.sleep_random(args.window_delay, "before next legacy window")

    manifest["status"] = (
        "complete"
        if state["legacy_backfill"]["status"] == "complete"
        else "success"
    )
    manifest["completed_at"] = second_utc(archive_x.utc_now())
    manifest["next_until"] = state["legacy_backfill"]["next_until"]
    archive_x.atomic_write_json(manifest_path, manifest)
    return manifest


def claim_window(
    legacy: dict[str, Any], *, owner_run_id: str, claimed_at: str
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    if legacy["status"] != "pending":
        raise archive_x.ArchiveError("legacy frontier is not ready to claim")
    until = parse_utc(legacy["next_until"], "next_until")
    floor = parse_utc(legacy["floor_since"], "floor_since")
    if until == floor:
        raise archive_x.ArchiveError("legacy frontier is already at its floor")
    since = max(floor, until - timedelta(days=1))
    since_text, until_text = second_utc(since), second_utc(until)
    updated = copy.deepcopy(legacy)
    updated["status"] = "active"
    updated["active_window"] = {
        "window_id": window_id(since_text, until_text),
        "since": since_text,
        "until": until_text,
        "owner_run_id": owner_run_id,
        "attempt": 1,
        "claimed_at": second_utc(parse_utc(claimed_at, "claimed_at")),
        "leaves": [
            {"since": since_text, "until": until_text, "status": "pending"}
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
    dataset_sha256: str,
    walk_ids: list[str],
) -> dict[str, Any]:
    validate_legacy_state(legacy, expected_user_id=legacy.get("requested_user_id"))
    active = legacy.get("active_window")
    if legacy["status"] != "active" or active["window_id"] != window_id_value:
        raise archive_x.ArchiveError("legacy completion window guard failed")
    require_sha256(canonical_raw_sha256, "canonical raw hash")
    require_sha256(dataset_sha256, "dataset hash")
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
    updated["last_completed_window"] = {
        "window_id": window_id_value,
        "since": active["since"],
        "until": active["until"],
        "completed_at": second_utc(parse_utc(completed_at, "completed_at")),
        "canonical_raw_sha256": canonical_raw_sha256,
        "dataset_sha256": dataset_sha256,
        "walk_ids": sorted(walk_ids),
    }
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
        next_since = max(floor, frontier - timedelta(days=1))
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
        next_command = f"scripts/archive-x-legacy --user {handle} run --windows 1"
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
    run = commands.add_parser("run", help="run bounded initialized legacy windows")
    run.add_argument("--windows", type=archive_x.positive_int, required=True)
    run.add_argument("--request-limit", type=archive_x.positive_int, default=6)
    run.add_argument("--walk-attempts", type=archive_x.positive_int, default=3)
    run.add_argument("--window-attempts", type=archive_x.positive_int, default=3)
    run.add_argument("--max-leaves", type=archive_x.positive_int, default=64)
    run.add_argument(
        "--request-delay", type=archive_x.duration_arg, default="4-8"
    )
    run.add_argument("--walk-delay", type=archive_x.duration_arg, default="10-20")
    run.add_argument(
        "--window-delay", type=archive_x.duration_arg, default="30-60"
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
                        args, repo_dir, archive_root, handle, version
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
            return 0 if result["status"] in {"success", "complete"} else 1

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
