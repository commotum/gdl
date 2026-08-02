#!/usr/bin/env python3
"""Read-only, sanitized baseline inspection for Goal 5.

This utility never opens an archive database read/write, hashes payloads, or
persists output.  It reports fixed-path byte totals, manifest-derived process
and request counts, and SQLite query plans without exposing URLs, handles from
records, opaque cursors, or request parameters.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "gdl-x-goal5-baseline"
SCHEMA_VERSION = 1
HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}\Z")

DATASET_PATHS = {
    "posts": "dataset/posts.jsonl",
    "authored_posts": "dataset/authored-posts.jsonl",
    "reposts": "dataset/reposts.jsonl",
    "media": "dataset/media.jsonl",
    "context_posts": "dataset/context-posts.jsonl",
    "reply_edges": "dataset/reply-edges.jsonl",
}

METADATA_CLAIM_SQL = """SELECT t.* FROM targets t
WHERE t.state IN ('pending','retryable')
  AND t.next_attempt_at <= ?
ORDER BY
  (SELECT COUNT(*) FROM reply_edges e WHERE e.parent_id=t.post_id) DESC,
  t.depth_min ASC,
  t.post_id DESC
LIMIT 1"""

MEDIA_CLAIM_SQL = """SELECT * FROM targets
WHERE state='captured'
  AND media_state IN ('pending','retryable')
  AND media_next_attempt_at <= ?
ORDER BY depth_min, post_id DESC LIMIT 1"""

STATUS_QUERIES = (
    "SELECT state,COUNT(*) FROM targets GROUP BY state",
    "SELECT media_state,COUNT(*) FROM targets GROUP BY media_state",
    "SELECT COUNT(*) FROM reply_edges",
    "SELECT COUNT(*) FROM observations",
)


class MeasurementError(ValueError):
    pass


def _object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _size(path: Path) -> int:
    try:
        return path.stat().st_size if path.is_file() else 0
    except OSError:
        return 0


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _manifest_sources(
    user_dir: Path, manifests: Iterable[tuple[Path, dict[str, Any]]]
) -> set[Path]:
    runs = (user_dir / "runs").resolve()
    result: set[Path] = set()
    for _path, manifest in manifests:
        candidates: list[Any] = []
        for endpoint in manifest.get("endpoints") or ():
            if isinstance(endpoint, dict) and endpoint.get("endpoint") == "timeline":
                candidates.append(endpoint.get("raw_path"))
        for window in manifest.get("windows") or ():
            if not isinstance(window, dict) or not window.get("state_committed"):
                continue
            candidates.append(window.get("canonical_raw_path"))
        for value in candidates:
            if not isinstance(value, str) or not value:
                continue
            candidate = (user_dir / value).resolve()
            if _inside(candidate, runs) and candidate.is_file():
                result.add(candidate)
    return result


def _legacy_counts(manifests: Iterable[tuple[Path, dict[str, Any]]]) -> dict[str, int]:
    counts = {
        "runs": 0,
        "windows": 0,
        "committed_windows": 0,
        "walks": 0,
        "valid_walks": 0,
        "search_requests": 0,
        "api_requests": 0,
        "canonical_posts": 0,
    }
    for _path, manifest in manifests:
        if manifest.get("mode") != "legacy_backfill":
            continue
        counts["runs"] += 1
        for window in manifest.get("windows") or ():
            if not isinstance(window, dict):
                continue
            counts["windows"] += 1
            if window.get("state_committed"):
                counts["committed_windows"] += 1
                counts["canonical_posts"] += int(
                    window.get("canonical_post_count") or 0
                )
            for walk in window.get("walks") or ():
                if not isinstance(walk, dict):
                    continue
                counts["walks"] += 1
                if walk.get("status") == "valid":
                    counts["valid_walks"] += 1
                counts["search_requests"] += int(
                    walk.get("search_requests") or 0
                )
                counts["api_requests"] += int(walk.get("api_requests") or 0)
    return counts


def _runner_counts(manifests: Iterable[tuple[Path, dict[str, Any]]]) -> dict[str, int]:
    endpoint_starts = 0
    legacy_walk_starts = 0
    for _path, manifest in manifests:
        endpoint_starts += sum(
            1 for value in manifest.get("endpoints") or () if isinstance(value, dict)
        )
        for window in manifest.get("windows") or ():
            if isinstance(window, dict):
                legacy_walk_starts += sum(
                    1 for value in window.get("walks") or () if isinstance(value, dict)
                )
    return {
        "endpoint_starts": endpoint_starts,
        "legacy_walk_starts": legacy_walk_starts,
        "total_known_starts": endpoint_starts + legacy_walk_starts,
    }


def _plan(connection: sqlite3.Connection, sql: str) -> list[str]:
    return [
        str(row[3])
        for row in connection.execute("EXPLAIN QUERY PLAN " + sql, (0,))
    ]


def _context_measurement(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"present": False, "bytes": 0}
    started = time.perf_counter_ns()
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.execute("PRAGMA query_only=ON")
        try:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            required = {"targets", "reply_edges", "observations"}
            if not required <= tables:
                raise MeasurementError("context database schema is unrecognized")
            counts = {
                name: int(connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
                for name in sorted(required)
            }
            metadata_plan = _plan(connection, METADATA_CLAIM_SQL)
            media_plan = _plan(connection, MEDIA_CLAIM_SQL)
            status_started = time.perf_counter_ns()
            for query in STATUS_QUERIES:
                list(connection.execute(query))
            status_elapsed = time.perf_counter_ns() - status_started
            indexes = sorted(
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
                )
            )
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise MeasurementError("context database cannot be inspected read-only") from exc
    return {
        "present": True,
        "bytes": _size(path),
        "rows": counts,
        "indexes": indexes,
        "metadata_claim_plan": metadata_plan,
        "media_claim_plan": media_plan,
        "status_query_elapsed_us": max(0, status_elapsed // 1_000),
        "inspection_elapsed_us": max(0, (time.perf_counter_ns() - started) // 1_000),
    }


def inspect_user(archive_root: Path, handle: str) -> dict[str, Any]:
    if not HANDLE_RE.fullmatch(handle):
        raise MeasurementError("user must be an X handle without @")
    root = archive_root.resolve()
    user_dir = (root / "users" / handle.lower()).resolve()
    users_dir = (root / "users").resolve()
    if not _inside(user_dir, users_dir) or not user_dir.is_dir():
        raise MeasurementError("archive user directory does not exist")

    manifest_started = time.perf_counter_ns()
    manifest_values = [
        (path, _object(path))
        for path in sorted((user_dir / "runs").glob("*/manifest.json"))
    ]
    manifests = [(path, value) for path, value in manifest_values if value]
    manifest_elapsed_us = max(
        0, (time.perf_counter_ns() - manifest_started) // 1_000
    )
    sources = _manifest_sources(user_dir, manifests)
    dataset_bytes = {
        name: _size(user_dir / relative)
        for name, relative in DATASET_PATHS.items()
    }
    legacy = _legacy_counts(manifests)
    posts_views = (
        dataset_bytes["posts"]
        + dataset_bytes["authored_posts"]
        + dataset_bytes["reposts"]
    )
    repeated_window_io = legacy["committed_windows"] * (
        dataset_bytes["posts"] + posts_views
    )
    return {
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "requested_handle": handle.lower(),
        "read_only": True,
        "dataset_bytes": dataset_bytes,
        "raw_sources": {
            "files": len(sources),
            "bytes": sum(_size(path) for path in sources),
        },
        "manifests": {
            "files": len(manifests),
            "bytes": sum(_size(path) for path, _value in manifests),
            "scan_elapsed_us": manifest_elapsed_us,
        },
        "legacy": legacy,
        "runner_processes": _runner_counts(manifests),
        "baseline_logical_io": {
            "one_post_merge_read_bytes": dataset_bytes["posts"],
            "one_post_merge_write_bytes": posts_views,
            "legacy_window_merge_read_write_bytes": repeated_window_io,
            "one_context_export_write_bytes": (
                dataset_bytes["context_posts"] + dataset_bytes["reply_edges"]
            ),
            "unchanged_seed_payload_bytes_revisited": sum(
                _size(path) for path in sources
            ),
        },
        "context": _context_measurement(user_dir / "_state" / "context.sqlite3"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="read-only sanitized Goal 5 archive baseline"
    )
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--user", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = inspect_user(args.archive_root, args.user)
    except MeasurementError as exc:
        print(f"archive-x measure: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
