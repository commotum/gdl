#!/usr/bin/env python3
"""Incremental local truth, source caching, and generation exports for X archives."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import secrets
import shutil
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

import archive_x
import archive_x_context as context_x
import archive_x_descriptors as descriptor_x


EXPORT_SCHEMA = "gdl-x-portable-export"
EXPORT_SCHEMA_VERSION = 1
SOURCE_BATCH_SIZE = 1_000
LOCAL_MIN_HEADROOM = 512 * 1024 * 1024
EXPORT_MIN_HEADROOM = 256 * 1024 * 1024
EXPORT_VIEW_FILES = {
    "posts": "posts.jsonl",
    "authored_posts": "authored-posts.jsonl",
    "reposts": "reposts.jsonl",
    "media": "media.jsonl",
    "context_posts": "context-posts.jsonl",
    "reply_edges": "reply-edges.jsonl",
    "context_status": "context-status.json",
}
RUN_STATUSES = {
    "running",
    "success",
    "partial",
    "limited",
    "failed",
    "interrupted",
    "stalled",
    "manual_review",
    "complete",
}
RUN_MODES = {"modern", "legacy_backfill", "context", "migration"}


class LocalStateError(archive_x.ArchiveError):
    """A sanitized local indexing, registry, audit, or export failure."""


@dataclass(frozen=True)
class SourceSpec:
    path: Path
    source_kind: str
    run_id: str
    operation_id: str
    endpoint: str
    expected_sha256: str | None = None


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _safe_relative(user_dir: Path, path: Path, *, parent: str) -> str:
    try:
        relative = path.resolve().relative_to(user_dir.resolve())
    except (OSError, ValueError) as exc:
        raise LocalStateError("local archive path is outside its user root") from exc
    if not relative.parts or relative.parts[0] != parent or ".." in relative.parts:
        raise LocalStateError("local archive path has an invalid scope")
    return relative.as_posix()


def _stat_identity(stat: os.stat_result) -> tuple[int, int, int, int]:
    return (
        int(stat.st_dev),
        int(stat.st_ino),
        int(stat.st_size),
        int(stat.st_mtime_ns),
    )


def _row_stat_identity(row: sqlite3.Row) -> tuple[int, int, int, int] | None:
    values = (
        row["stat_device"],
        row["stat_inode"],
        row["stat_size"],
        row["stat_mtime_ns"],
    )
    if any(value is None for value in values):
        return None
    return tuple(int(value) for value in values)  # type: ignore[return-value]


def _require_digest(value: str | None, field: str) -> str | None:
    if value is None:
        return None
    if not descriptor_x.SHA256_RE.fullmatch(value):
        raise LocalStateError(f"{field} is not a SHA-256 digest")
    return value


def _next_generation(
    database: context_x.ContextDB,
    *,
    observed_at: str,
    dirty_views: Iterable[str],
) -> int:
    views = tuple(sorted(set(dirty_views)))
    unknown = set(views) - set(EXPORT_VIEW_FILES)
    if unknown:
        raise LocalStateError("unknown local export view")
    current = int(
        database.connection.execute(
            "SELECT current_generation FROM archive_generation WHERE singleton=1"
        ).fetchone()[0]
    )
    generation = current + 1
    database.connection.execute(
        "UPDATE archive_generation SET current_generation=?,updated_at=? "
        "WHERE singleton=1",
        (generation, observed_at),
    )
    if views:
        placeholders = ",".join("?" for _ in views)
        database.connection.execute(
            f"""UPDATE export_views SET durable_generation=?,status='dirty',
                       updated_at=? WHERE view_name IN ({placeholders})""",
            (generation, observed_at, *views),
        )
    return generation


def counter_snapshot(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        str(row[0]): int(row[1])
        for row in connection.execute(
            "SELECT counter_name,value FROM progress_counters"
        )
    }


def fast_context_status(database: context_x.ContextDB) -> dict[str, Any]:
    counters = counter_snapshot(database.connection)
    states = {
        state: counters.get(f"targets_state_{state}", 0)
        for state in context_x.VALID_STATES
    }
    media_states = (
        "none",
        "pending",
        "leased",
        "captured",
        "retryable",
        "unavailable",
        "manual_review",
    )
    media = {
        state: counters.get(f"targets_media_{state}", 0)
        for state in media_states
    }
    if counters.get("asset_jobs_total", 0):
        media = {
            state: counters.get(f"asset_jobs_state_{state}", 0)
            for state in (
                "pending",
                "leased",
                "captured",
                "retryable",
                "needs_refresh",
                "unavailable",
                "manual_review",
            )
        }
    closure_states = (
        "fully_captured",
        "unavailable_boundary",
        "retry_delayed",
        "pending",
        "manual_review",
    )
    closure = {
        state: counters.get(f"conversations_state_{state}", 0)
        for state in closure_states
    }
    pacing_row = database.connection.execute(
        "SELECT * FROM pacing WHERE singleton=1"
    ).fetchone()
    return {
        "schema_version": context_x.SCHEMA_VERSION,
        "targets": counters.get("targets_total", 0),
        "states": states,
        "edges": counters.get("reply_edges_total", 0),
        "conversations": sum(closure.values()),
        "conversation_closure": closure,
        "media": media,
        "pacing": dict(pacing_row) if pacing_row is not None else {},
        "archive_posts": counters.get("archive_posts_total", 0),
        "archive_media_files": counters.get("archive_media_files", 0),
        "archive_media_bytes": counters.get("archive_media_bytes", 0),
    }


def _source_row(
    database: context_x.ContextDB, relative_path: str
) -> sqlite3.Row | None:
    return database.connection.execute(
        "SELECT * FROM archive_sources WHERE relative_path=?",
        (relative_path,),
    ).fetchone()


def _validate_source_spec(user_dir: Path, spec: SourceSpec) -> tuple[str, os.stat_result]:
    if spec.source_kind not in {"modern", "legacy"}:
        raise LocalStateError("incremental source kind is invalid")
    if not spec.run_id or Path(spec.run_id).name != spec.run_id:
        raise LocalStateError("incremental source run identity is invalid")
    if not spec.operation_id or len(spec.operation_id) > 256:
        raise LocalStateError("incremental source operation is invalid")
    if not spec.endpoint or len(spec.endpoint) > 128:
        raise LocalStateError("incremental source endpoint is invalid")
    if spec.path.name.endswith((".tmp", ".partial")) or not spec.path.is_file():
        raise LocalStateError("incremental source is missing or incomplete")
    relative = _safe_relative(user_dir, spec.path, parent="runs")
    expected = _require_digest(spec.expected_sha256, "source expected digest")
    if expected != spec.expected_sha256:
        raise LocalStateError("incremental source expected digest changed")
    return relative, spec.path.stat()


def _register_ingesting_source(
    database: context_x.ContextDB,
    *,
    relative_path: str,
    spec: SourceSpec,
    stat: os.stat_result,
    registered_at: str,
) -> int:
    with context_x.transaction(database.connection):
        row = _source_row(database, relative_path)
        if row is not None and row["status"] in {"changed", "invalid"}:
            raise LocalStateError("incremental source is blocked by prior mutation")
        if row is None:
            database.connection.execute(
                """INSERT INTO archive_sources(
                       relative_path,source_kind,run_id,operation_id,
                       expected_sha256,stat_device,stat_inode,stat_size,
                       stat_mtime_ns,status,ingest_generation,registered_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,'ingesting',0,?)""",
                (
                    relative_path,
                    spec.source_kind,
                    spec.run_id,
                    spec.operation_id,
                    spec.expected_sha256,
                    int(stat.st_dev),
                    int(stat.st_ino),
                    int(stat.st_size),
                    int(stat.st_mtime_ns),
                    registered_at,
                ),
            )
        else:
            if (
                row["source_kind"] != spec.source_kind
                or row["run_id"] != spec.run_id
                or row["operation_id"] != spec.operation_id
            ):
                raise LocalStateError("incremental source provenance changed")
            database.connection.execute(
                """UPDATE archive_sources SET status='ingesting',
                       stat_device=?,stat_inode=?,stat_size=?,stat_mtime_ns=?,
                       processed_at=NULL,record_count=NULL,edge_count=NULL
                     WHERE source_id=?""",
                (
                    int(stat.st_dev),
                    int(stat.st_ino),
                    int(stat.st_size),
                    int(stat.st_mtime_ns),
                    int(row["source_id"]),
                ),
            )
        return int(_source_row(database, relative_path)["source_id"])


def _prepare_stage(database: context_x.ContextDB) -> None:
    database.connection.execute("DROP TABLE IF EXISTS temp.goal5_source_stage")
    database.connection.execute(
        """CREATE TEMP TABLE goal5_source_stage (
               post_id TEXT PRIMARY KEY,
               normalized_json TEXT NOT NULL,
               normalized_sha256 TEXT NOT NULL,
               raw_json TEXT NOT NULL,
               raw_sha256 TEXT NOT NULL,
               observed_at TEXT NOT NULL,
               author_id TEXT,
               relationship TEXT NOT NULL,
               reply_id TEXT,
               conversation_id TEXT,
               richness_primary INTEGER NOT NULL,
               richness_secondary INTEGER NOT NULL
           ) WITHOUT ROWID"""
    )


def _stage_batch(
    database: context_x.ContextDB,
    rows: list[tuple[Any, ...]],
) -> None:
    if not rows:
        return
    with context_x.transaction(database.connection):
        database.connection.executemany(
            """INSERT INTO goal5_source_stage(
                   post_id,normalized_json,normalized_sha256,raw_json,raw_sha256,
                   observed_at,author_id,relationship,reply_id,conversation_id,
                   richness_primary,richness_secondary
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(post_id) DO UPDATE SET
                   normalized_json=excluded.normalized_json,
                   normalized_sha256=excluded.normalized_sha256,
                   raw_json=excluded.raw_json,
                   raw_sha256=excluded.raw_sha256,
                   observed_at=excluded.observed_at,
                   author_id=excluded.author_id,
                   relationship=excluded.relationship,
                   reply_id=excluded.reply_id,
                   conversation_id=excluded.conversation_id,
                   richness_primary=excluded.richness_primary,
                   richness_secondary=excluded.richness_secondary
               WHERE excluded.richness_primary>goal5_source_stage.richness_primary
                  OR (excluded.richness_primary=goal5_source_stage.richness_primary
                      AND excluded.richness_secondary>
                          goal5_source_stage.richness_secondary)""",
            rows,
        )


def _stream_source_to_stage(
    database: context_x.ContextDB,
    *,
    spec: SourceSpec,
    requested_handle: str,
    target_user_id: str,
) -> dict[str, Any]:
    _prepare_stage(database)
    hasher = hashlib.sha256()
    byte_count = 0
    raw_count = 0
    normalized_count = 0
    staged: list[tuple[Any, ...]] = []
    try:
        with spec.path.open("rb") as stream:
            for line_number, raw_line in enumerate(stream, 1):
                hasher.update(raw_line)
                byte_count += len(raw_line)
                if not raw_line.strip():
                    continue
                raw_count += 1
                try:
                    metadata = json.loads(raw_line.decode("utf-8", errors="strict"))
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise LocalStateError(
                        f"incremental source contains invalid JSON at line {line_number}"
                    ) from exc
                if not isinstance(metadata, dict):
                    raise LocalStateError("incremental source record is not an object")
                normalized = archive_x.normalize_post(
                    metadata, requested_handle, spec.endpoint
                )
                if normalized is None:
                    raise LocalStateError("incremental source record has no post ID")
                post_id = str(normalized["post_id"])
                relationship = str(normalized.get("relationship") or "")
                if (
                    relationship not in {"post", "reply", "repost"}
                    or str(normalized.get("requested_user_id") or "")
                    != target_user_id
                ):
                    raise LocalStateError(
                        "incremental source record failed numeric archive scope"
                    )
                raw_json = _canonical_json(metadata)
                normalized_json = _canonical_json(normalized)
                richness = archive_x.record_richness(normalized)
                staged.append(
                    (
                        post_id,
                        normalized_json,
                        _sha256(normalized_json.encode("utf-8")),
                        raw_json,
                        _sha256(raw_json.encode("utf-8")),
                        str(metadata.get("archived_at") or context_x.iso_now()),
                        archive_x.id_string((metadata.get("author") or {}).get("id")),
                        relationship,
                        archive_x.id_string(metadata.get("reply_id")),
                        archive_x.id_string(metadata.get("conversation_id")),
                        int(richness[0]),
                        int(richness[1]),
                    )
                )
                normalized_count += 1
                if len(staged) >= SOURCE_BATCH_SIZE:
                    _stage_batch(database, staged)
                    staged.clear()
    except OSError as exc:
        raise LocalStateError("incremental source could not be read") from exc
    _stage_batch(database, staged)
    return {
        "sha256": hasher.hexdigest(),
        "bytes_read": byte_count,
        "raw_records": raw_count,
        "normalized_records": normalized_count,
        "unique_posts": int(
            database.connection.execute(
                "SELECT COUNT(*) FROM goal5_source_stage"
            ).fetchone()[0]
        ),
    }


def _merge_staged_source(
    database: context_x.ContextDB,
    *,
    source_id: int,
    spec: SourceSpec,
    relative_path: str,
    digest: str,
    stat: os.stat_result,
    target_user_id: str,
    max_depth: int,
    streamed: dict[str, Any],
    observed_at: str,
    descriptor_batches: tuple[descriptor_x.DescriptorBatch, ...] = (),
) -> dict[str, Any]:
    new_posts = 0
    updated_posts = 0
    new_edges = 0
    local_parents = 0
    with context_x.transaction(database.connection):
        relationships = {
            str(row[0])
            for row in database.connection.execute(
                "SELECT DISTINCT relationship FROM goal5_source_stage"
            )
        }
        dirty_views: set[str] = set()
        if relationships:
            dirty_views.add("posts")
        if relationships & {"post", "reply"}:
            dirty_views.add("authored_posts")
        if "repost" in relationships:
            dirty_views.add("reposts")
        generation = _next_generation(
            database,
            observed_at=observed_at,
            dirty_views=dirty_views,
        )
        for row in database.connection.execute(
            "SELECT * FROM goal5_source_stage ORDER BY post_id"
        ):
            record = json.loads(str(row["normalized_json"]))
            post_id = str(row["post_id"])
            previous = database.connection.execute(
                "SELECT normalized_json FROM archive_posts WHERE post_id=?",
                (post_id,),
            ).fetchone()
            if previous is None:
                merged = record
                new_posts += 1
            else:
                merged = archive_x.merge_post_records(
                    json.loads(str(previous[0])), record
                )
                updated_posts += 1
            normalized_json = _canonical_json(merged)
            database.connection.execute(
                """INSERT INTO archive_posts(
                       post_id,requested_user_id,author_id,relationship,
                       posted_at,normalized_json,normalized_sha256,
                       first_captured_at,last_captured_at,capture_count,
                       durable_generation
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(post_id) DO UPDATE SET
                       author_id=excluded.author_id,
                       relationship=excluded.relationship,
                       posted_at=excluded.posted_at,
                       normalized_json=excluded.normalized_json,
                       normalized_sha256=excluded.normalized_sha256,
                       first_captured_at=excluded.first_captured_at,
                       last_captured_at=excluded.last_captured_at,
                       capture_count=excluded.capture_count,
                       durable_generation=excluded.durable_generation""",
                (
                    post_id,
                    target_user_id,
                    archive_x.id_string(merged.get("author_id")),
                    str(merged["relationship"]),
                    str(merged.get("posted_at") or "") or None,
                    normalized_json,
                    _sha256(normalized_json.encode("utf-8")),
                    str(merged.get("first_captured_at") or observed_at),
                    str(merged.get("last_captured_at") or observed_at),
                    int(merged.get("capture_count") or 1),
                    generation,
                ),
            )
            database.connection.execute(
                """INSERT INTO post_provenance(
                       post_id,source_id,record_sha256,source_endpoint,observed_at
                   ) VALUES (?,?,?,?,?)""",
                (
                    post_id,
                    source_id,
                    str(row["raw_sha256"]),
                    spec.endpoint,
                    str(row["observed_at"]),
                ),
            )
            if str(row["author_id"] or "") == target_user_id:
                database.connection.execute(
                    """INSERT INTO local_posts(
                           post_id,raw_json,sha256,relative_path,source_kind,
                           run_id,observed_at
                       ) VALUES (?,?,?,?,?,?,?)
                       ON CONFLICT(post_id) DO UPDATE SET
                           raw_json=excluded.raw_json,sha256=excluded.sha256,
                           relative_path=excluded.relative_path,
                           source_kind=excluded.source_kind,run_id=excluded.run_id,
                           observed_at=excluded.observed_at
                       WHERE excluded.observed_at>=local_posts.observed_at""",
                    (
                        post_id,
                        str(row["raw_json"]),
                        str(row["raw_sha256"]),
                        relative_path,
                        spec.source_kind,
                        spec.run_id,
                        str(row["observed_at"]),
                    ),
                )
            if row["relationship"] == "reply" and row["reply_id"]:
                new_edges += int(
                    database.add_edge(
                        post_id,
                        str(row["reply_id"]),
                        conversation_id=(
                            str(row["conversation_id"])
                            if row["conversation_id"]
                            else None
                        ),
                        depth=0,
                        run_id=spec.run_id,
                        observed_at=str(row["observed_at"]),
                        max_depth=max_depth,
                    )
                )
        descriptor_commit = database.persist_descriptor_batches(
            descriptor_batches,
            (
                json.loads(str(row[0]))
                for row in database.connection.execute(
                    "SELECT raw_json FROM goal5_source_stage ORDER BY post_id"
                )
            ),
        )
        local_candidates = list(
            database.connection.execute(
                """SELECT t.post_id,l.raw_json,l.source_kind,l.run_id
                     FROM targets t JOIN local_posts l ON l.post_id=t.post_id
                    WHERE t.state<>'captured' AND t.post_id IN (
                          SELECT post_id FROM goal5_source_stage
                          UNION
                          SELECT reply_id FROM goal5_source_stage
                           WHERE reply_id IS NOT NULL)
                    ORDER BY t.depth_min,t.post_id"""
            )
        )
        for row in local_candidates:
            database._capture_record(
                str(row["post_id"]),
                json.loads(str(row["raw_json"])),
                source_kind=f"timeline:{row['source_kind']}:{row['run_id']}",
                target_user_id=target_user_id,
                max_depth=max_depth,
            )
            local_parents += 1
        if new_edges or local_parents:
            placeholders = ("context_posts", "reply_edges", "context_status")
            marks = ",".join("?" for _ in placeholders)
            database.connection.execute(
                f"""UPDATE export_views SET durable_generation=?,status='dirty',
                           updated_at=? WHERE view_name IN ({marks})""",
                (generation, observed_at, *placeholders),
            )
        seeded = database.connection.execute(
            "SELECT sha256,source_kind,run_id FROM seed_sources "
            "WHERE relative_path=?",
            (relative_path,),
        ).fetchone()
        if seeded is not None and tuple(seeded) != (
            digest,
            spec.source_kind,
            spec.run_id,
        ):
            raise LocalStateError("canonical seed source evidence changed")
        database.connection.execute(
            """INSERT INTO seed_sources(
                   relative_path,sha256,source_kind,run_id,processed_at,
                   record_count,edge_count
               ) VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(relative_path) DO UPDATE SET
                   sha256=excluded.sha256,processed_at=excluded.processed_at,
                   record_count=excluded.record_count,
                   edge_count=excluded.edge_count
               WHERE seed_sources.sha256=excluded.sha256""",
            (
                relative_path,
                digest,
                spec.source_kind,
                spec.run_id,
                observed_at,
                int(streamed["raw_records"]),
                new_edges,
            ),
        )
        cursor = database.connection.execute(
            """UPDATE archive_sources SET expected_sha256=?,stat_device=?,
                   stat_inode=?,stat_size=?,stat_mtime_ns=?,status='committed',
                   ingest_generation=?,processed_at=?,record_count=?,edge_count=?
                 WHERE source_id=? AND status='ingesting'""",
            (
                digest,
                int(stat.st_dev),
                int(stat.st_ino),
                int(stat.st_size),
                int(stat.st_mtime_ns),
                generation,
                observed_at,
                int(streamed["raw_records"]),
                new_edges,
                source_id,
            ),
        )
        if cursor.rowcount != 1:
            raise LocalStateError("incremental source commit state changed")
    return {
        "status": "committed",
        "generation": generation,
        "new_posts": new_posts,
        "updated_posts": updated_posts,
        "new_edges": new_edges,
        "local_parents": local_parents,
        "descriptor_commit": descriptor_commit,
        **streamed,
    }


def ingest_source_once(
    user_dir: Path,
    db_path: Path,
    *,
    requested_handle: str,
    target_user_id: str,
    spec: SourceSpec,
    max_depth: int = 1_000,
    descriptor_batches: Iterable[descriptor_x.DescriptorBatch] = (),
    disk_free: Callable[[Path], int] = lambda path: shutil.disk_usage(path).free,
) -> dict[str, Any]:
    """Hash/parse one new source once; exact committed stat hits read no bytes."""
    if not target_user_id.isdecimal() or int(target_user_id) < 1:
        raise LocalStateError("incremental source account identity is invalid")
    relative_path, initial_stat = _validate_source_spec(user_dir, spec)
    selected_batches = tuple(descriptor_batches)
    observed_at = context_x.iso_now()
    with context_x.ContextDB(db_path) as database:
        database.bind_identity(target_user_id, requested_handle)
        existing = _source_row(database, relative_path)
        migration_needs_index = bool(
            existing is not None
            and existing["status"] == "committed"
            and existing["operation_id"] == "seed_context"
            and database.connection.execute(
                "SELECT 1 FROM post_provenance WHERE source_id=? LIMIT 1",
                (int(existing["source_id"]),),
            ).fetchone()
            is None
        )
        if existing is not None and existing["status"] == "committed" and not migration_needs_index:
            if (
                existing["source_kind"] != spec.source_kind
                or existing["run_id"] != spec.run_id
                or existing["operation_id"] != spec.operation_id
            ):
                raise LocalStateError("committed source provenance changed")
            stored_digest = _require_digest(
                str(existing["expected_sha256"] or ""), "committed source digest"
            )
            if spec.expected_sha256 and spec.expected_sha256 != stored_digest:
                raise LocalStateError("committed source manifest digest changed")
            if _row_stat_identity(existing) == _stat_identity(initial_stat):
                return {
                    "status": "unchanged",
                    "generation": int(existing["ingest_generation"]),
                    "bytes_read": 0,
                    "raw_records": 0,
                    "normalized_records": 0,
                    "unique_posts": 0,
                    "new_posts": 0,
                    "updated_posts": 0,
                    "new_edges": 0,
                    "local_parents": 0,
                }
            digest = archive_x.sha256_file(spec.path)
            final_stat = spec.path.stat()
            if _stat_identity(final_stat) != _stat_identity(initial_stat):
                raise LocalStateError("committed source changed during verification")
            if digest != stored_digest:
                with context_x.transaction(database.connection):
                    database.connection.execute(
                        "UPDATE archive_sources SET status='changed' WHERE source_id=?",
                        (int(existing["source_id"]),),
                    )
                raise LocalStateError("committed source content changed")
            with context_x.transaction(database.connection):
                database.connection.execute(
                    """UPDATE archive_sources SET stat_device=?,stat_inode=?,
                           stat_size=?,stat_mtime_ns=? WHERE source_id=?""",
                    (*_stat_identity(final_stat), int(existing["source_id"])),
                )
            return {
                "status": "verified_stat_change",
                "generation": int(existing["ingest_generation"]),
                "bytes_read": int(final_stat.st_size),
                "raw_records": 0,
                "normalized_records": 0,
                "unique_posts": 0,
                "new_posts": 0,
                "updated_posts": 0,
                "new_edges": 0,
                "local_parents": 0,
            }
        required = max(LOCAL_MIN_HEADROOM, int(initial_stat.st_size) * 4)
        if disk_free(db_path.parent) < required:
            raise LocalStateError("insufficient free space for incremental source")
        source_id = _register_ingesting_source(
            database,
            relative_path=relative_path,
            spec=spec,
            stat=initial_stat,
            registered_at=observed_at,
        )
        streamed = _stream_source_to_stage(
            database,
            spec=spec,
            requested_handle=requested_handle,
            target_user_id=target_user_id,
        )
        final_stat = spec.path.stat()
        if _stat_identity(final_stat) != _stat_identity(initial_stat):
            raise LocalStateError("incremental source changed during ingestion")
        digest = str(streamed["sha256"])
        if spec.expected_sha256 and digest != spec.expected_sha256:
            with context_x.transaction(database.connection):
                database.connection.execute(
                    "UPDATE archive_sources SET status='invalid' WHERE source_id=?",
                    (source_id,),
                )
            raise LocalStateError("incremental source digest does not match manifest")
        return _merge_staged_source(
            database,
            source_id=source_id,
            spec=spec,
            relative_path=relative_path,
            digest=digest,
            stat=final_stat,
            target_user_id=target_user_id,
            max_depth=max_depth,
            streamed=streamed,
            observed_at=observed_at,
            descriptor_batches=selected_batches,
        )


def audit_registered_sources(user_dir: Path, db_path: Path) -> dict[str, int]:
    """Explicit full-content audit; unlike ordinary operation this always hashes."""
    checked = 0
    bytes_read = 0
    changed: list[int] = []
    with context_x.ContextDB(db_path, create=False) as database:
        rows = list(
            database.connection.execute(
                """SELECT * FROM archive_sources
                     WHERE status='committed' AND expected_sha256 IS NOT NULL
                     ORDER BY source_id"""
            )
        )
        for row in rows:
            relative = Path(str(row["relative_path"]))
            path = (user_dir / relative).resolve()
            try:
                path.relative_to(user_dir.resolve())
            except ValueError as exc:
                raise LocalStateError("registered source escaped its user root") from exc
            if not path.is_file():
                changed.append(int(row["source_id"]))
                continue
            stat = path.stat()
            digest = archive_x.sha256_file(path)
            checked += 1
            bytes_read += int(stat.st_size)
            if digest != str(row["expected_sha256"]):
                changed.append(int(row["source_id"]))
            else:
                with context_x.transaction(database.connection):
                    database.connection.execute(
                        """UPDATE archive_sources SET stat_device=?,stat_inode=?,
                               stat_size=?,stat_mtime_ns=? WHERE source_id=?""",
                        (*_stat_identity(stat), int(row["source_id"])),
                    )
        if changed:
            with context_x.transaction(database.connection):
                placeholders = ",".join("?" for _ in changed)
                database.connection.execute(
                    f"UPDATE archive_sources SET status='changed' "
                    f"WHERE source_id IN ({placeholders})",
                    tuple(changed),
                )
            raise LocalStateError(
                f"source integrity audit found {len(changed)} changed source(s)"
            )
    return {"sources_checked": checked, "bytes_read": bytes_read, "changed": 0}


def _manifest_mode(value: dict[str, Any]) -> str:
    mode = str(value.get("mode") or "")
    if mode == "legacy_backfill":
        return mode
    if mode == "context":
        return mode
    return "modern"


def _manifest_status(value: dict[str, Any]) -> str:
    status = str(value.get("status") or "failed")
    if status == "complete_with_unavailable_media":
        return "complete"
    return status if status in RUN_STATUSES else "failed"


def _manifest_record_and_value(
    user_dir: Path, path: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    relative = _safe_relative(user_dir, path, parent="runs")
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8", errors="strict"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LocalStateError("run manifest is unreadable") from exc
    if not isinstance(value, dict):
        raise LocalStateError("run manifest is not an object")
    run_id = str(value.get("run_id") or path.parent.name)
    if not run_id or Path(run_id).name != run_id:
        raise LocalStateError("run manifest identity is invalid")
    stat = path.stat()
    record = {
        "run_id": run_id,
        "mode": _manifest_mode(value),
        "manifest_path": relative,
        "manifest_sha256": _sha256(raw),
        "stat_size": int(stat.st_size),
        "stat_mtime_ns": int(stat.st_mtime_ns),
        "status": _manifest_status(value),
        "updated_at": str(
            value.get("completed_at")
            or value.get("updated_at")
            or value.get("started_at")
            or context_x.iso_now()
        ),
    }
    return record, value


def _manifest_record(user_dir: Path, path: Path) -> dict[str, Any]:
    record, _value = _manifest_record_and_value(user_dir, path)
    return record


def _manifest_source_candidates(
    user_dir: Path,
    record: dict[str, Any],
    value: dict[str, Any],
) -> list[dict[str, Any]]:
    """Select only manifest-authoritative raw sources without reading payloads."""
    if record["status"] == "running":
        return []
    candidates: list[dict[str, Any]] = []
    if value.get("mode") == "legacy_backfill":
        for window in value.get("windows") or ():
            if not isinstance(window, dict) or not (
                window.get("status") == "success"
                and window.get("metadata_confirmed") is True
                and window.get("state_committed") is True
            ):
                continue
            relative = str(window.get("canonical_raw_path") or "")
            digest = str(window.get("canonical_raw_sha256") or "")
            operation_id = str(
                window.get("window_id")
                or Path(relative).name.removesuffix(".posts.jsonl")
            )
            candidates.append(
                {
                    "relative_path": relative,
                    "source_kind": "legacy",
                    "run_id": record["run_id"],
                    "operation_id": operation_id,
                    "expected_sha256": _require_digest(
                        digest, "legacy source manifest digest"
                    ),
                }
            )
    else:
        for endpoint in value.get("endpoints") or ():
            if not isinstance(endpoint, dict) or endpoint.get("endpoint") != "timeline":
                continue
            relative = str(endpoint.get("raw_path") or "")
            candidates.append(
                {
                    "relative_path": relative,
                    "source_kind": "modern",
                    "run_id": record["run_id"],
                    "operation_id": str(
                        endpoint.get("descriptor_operation_id")
                        or f"{record['run_id']}:timeline"
                    ),
                    "expected_sha256": None,
                }
            )
    by_path: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        relative = Path(str(candidate["relative_path"] or ""))
        if (
            not relative.parts
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.parts[0] != "runs"
            or not candidate["operation_id"]
        ):
            raise LocalStateError("manifest canonical source path is invalid")
        path = (user_dir / relative).resolve()
        _safe_relative(user_dir, path, parent="runs")
        key = relative.as_posix()
        previous = by_path.get(key)
        normalized = {**candidate, "relative_path": key}
        if previous is not None and previous != normalized:
            raise LocalStateError("manifest canonical source evidence conflicts")
        by_path[key] = normalized
    return [by_path[key] for key in sorted(by_path)]


def _register_manifest_source_candidate(
    database: context_x.ContextDB, candidate: dict[str, Any], observed_at: str
) -> None:
    existing = database.connection.execute(
        "SELECT * FROM archive_sources WHERE relative_path=?",
        (candidate["relative_path"],),
    ).fetchone()
    if existing is not None:
        expected = candidate["expected_sha256"]
        if (
            existing["source_kind"] != candidate["source_kind"]
            or existing["run_id"] != candidate["run_id"]
            or (
                existing["operation_id"]
                not in {candidate["operation_id"], "seed_context"}
            )
            or (
                expected is not None
                and existing["expected_sha256"] not in {None, expected}
            )
        ):
            raise LocalStateError("registered canonical source provenance conflicts")
        if existing["expected_sha256"] is None and expected is not None:
            database.connection.execute(
                "UPDATE archive_sources SET expected_sha256=? WHERE source_id=?",
                (expected, int(existing["source_id"])),
            )
        return
    database.connection.execute(
        """INSERT INTO archive_sources(
               relative_path,source_kind,run_id,operation_id,expected_sha256,
               status,ingest_generation,registered_at
           ) VALUES (?,?,?,?,?,'registered',0,?)""",
        (
            candidate["relative_path"],
            candidate["source_kind"],
            candidate["run_id"],
            candidate["operation_id"],
            candidate["expected_sha256"],
            observed_at,
        ),
    )


def _upsert_manifest_record(
    database: context_x.ContextDB,
    record: dict[str, Any],
    *,
    processed: bool,
) -> None:
    if record["mode"] not in RUN_MODES or record["status"] not in RUN_STATUSES:
        raise LocalStateError("run registry record is invalid")
    database.connection.execute(
        """INSERT INTO run_registry(
               run_id,mode,manifest_path,manifest_sha256,stat_size,
               stat_mtime_ns,status,processed_at,updated_at
           ) VALUES (?,?,?,?,?,?,?,?,?)
           ON CONFLICT(run_id) DO UPDATE SET
               mode=excluded.mode,manifest_path=excluded.manifest_path,
               manifest_sha256=excluded.manifest_sha256,
               stat_size=excluded.stat_size,
               stat_mtime_ns=excluded.stat_mtime_ns,status=excluded.status,
               processed_at=COALESCE(excluded.processed_at,
                                     run_registry.processed_at),
               updated_at=excluded.updated_at""",
        (
            record["run_id"],
            record["mode"],
            record["manifest_path"],
            record["manifest_sha256"],
            record["stat_size"],
            record["stat_mtime_ns"],
            record["status"],
            record["updated_at"] if processed else None,
            record["updated_at"],
        ),
    )


def register_run_manifest(
    user_dir: Path,
    db_path: Path,
    manifest_path: Path,
    *,
    processed: bool = False,
) -> dict[str, Any]:
    """Register the one manifest an ordinary run just changed."""
    record, value = _manifest_record_and_value(user_dir, manifest_path)
    pointer = f"latest_{record['mode']}_manifest".replace("legacy_backfill", "legacy")
    with context_x.ContextDB(db_path) as database, context_x.transaction(
        database.connection
    ):
        _upsert_manifest_record(database, record, processed=processed)
        for candidate in _manifest_source_candidates(user_dir, record, value):
            _register_manifest_source_candidate(
                database, candidate, record["updated_at"]
            )
        database.connection.execute(
            """INSERT INTO current_pointers(
                   pointer_name,run_id,relative_path,generation,updated_at
               ) VALUES (?,?,?,0,?)
               ON CONFLICT(pointer_name) DO UPDATE SET
                   run_id=excluded.run_id,relative_path=excluded.relative_path,
                   updated_at=excluded.updated_at""",
            (
                pointer,
                record["run_id"],
                record["manifest_path"],
                record["updated_at"],
            ),
        )
    return record


def reconcile_manifest_history(user_dir: Path, db_path: Path) -> dict[str, int]:
    """One historical scan followed by exact indexed ordinary lookups."""
    with context_x.ContextDB(db_path) as database:
        marker = database.connection.execute(
            "SELECT 1 FROM current_pointers WHERE pointer_name=?",
            ("manifest_history_reconciled",),
        ).fetchone()
        if marker is not None:
            return {"status": 0, "manifests_loaded": 0, "bytes_read": 0}
    entries: list[tuple[dict[str, Any], dict[str, Any]]] = []
    bytes_read = 0
    for path in sorted((user_dir / "runs").glob("*/manifest.json")):
        record, value = _manifest_record_and_value(user_dir, path)
        entries.append((record, value))
        bytes_read += int(record["stat_size"])
    records = [record for record, _value in entries]
    with context_x.ContextDB(db_path) as database, context_x.transaction(
        database.connection
    ):
        for record, value in entries:
            _upsert_manifest_record(database, record, processed=False)
            for candidate in _manifest_source_candidates(user_dir, record, value):
                _register_manifest_source_candidate(
                    database, candidate, record["updated_at"]
                )
        for mode, pointer in (
            ("modern", "latest_modern_manifest"),
            ("legacy_backfill", "latest_legacy_manifest"),
            ("context", "latest_context_manifest"),
        ):
            latest = max(
                (record for record in records if record["mode"] == mode),
                key=lambda item: (item["updated_at"], item["run_id"]),
                default=None,
            )
            if latest is not None:
                database.connection.execute(
                    """INSERT INTO current_pointers(
                           pointer_name,run_id,relative_path,generation,updated_at
                       ) VALUES (?,?,?,0,?)
                       ON CONFLICT(pointer_name) DO UPDATE SET
                           run_id=excluded.run_id,
                           relative_path=excluded.relative_path,
                           updated_at=excluded.updated_at""",
                    (
                        pointer,
                        latest["run_id"],
                        latest["manifest_path"],
                        latest["updated_at"],
                    ),
                )
        database.connection.execute(
            """INSERT INTO current_pointers(
                   pointer_name,relative_path,generation,updated_at
               ) VALUES ('manifest_history_reconciled','runs',0,?)
               ON CONFLICT(pointer_name) DO NOTHING""",
            (context_x.iso_now(),),
        )
    return {
        "status": 1,
        "manifests_loaded": len(records),
        "bytes_read": bytes_read,
    }


def _mark_local_history_ready(
    database: context_x.ContextDB, *, observed_at: str
) -> bool:
    ready = int(
        database.connection.execute(
            """SELECT COUNT(*) FROM current_pointers
                 WHERE pointer_name IN
                     ('source_history_reconciled','media_history_reconciled')"""
        ).fetchone()[0]
    ) == 2
    if ready:
        database.connection.execute(
            """INSERT INTO current_pointers(
                   pointer_name,relative_path,generation,updated_at
               ) VALUES ('local_history_reconciled','dataset',0,?)
               ON CONFLICT(pointer_name) DO UPDATE SET
                   updated_at=excluded.updated_at""",
            (observed_at,),
        )
    return ready


def reconcile_source_history(
    user_dir: Path,
    db_path: Path,
    *,
    max_depth: int = 1_000,
) -> dict[str, int]:
    """Index manifest-registered historical sources once, then use stat hits."""
    reconcile_manifest_history(user_dir, db_path)
    with context_x.ContextDB(db_path, create=False) as database:
        marker = database.connection.execute(
            "SELECT 1 FROM current_pointers "
            "WHERE pointer_name='source_history_reconciled'"
        ).fetchone()
        rows = [
            dict(row)
            for row in database.connection.execute(
                """SELECT s.* FROM archive_sources s
                     WHERE s.source_kind IN ('modern','legacy') AND (
                           s.status IN ('registered','ingesting') OR
                           (s.status='committed'
                            AND s.operation_id='seed_context'
                            AND NOT EXISTS (
                                SELECT 1 FROM post_provenance p
                                 WHERE p.source_id=s.source_id)))
                     ORDER BY s.source_id"""
            )
        ]
        if marker is not None and not rows:
            return {
                "sources": 0,
                "bytes_read": 0,
                "new_posts": 0,
                "new_edges": 0,
                "local_parents": 0,
            }
    target_id, handle = context_x.target_identity(user_dir)
    totals = {
        "sources": 0,
        "bytes_read": 0,
        "new_posts": 0,
        "new_edges": 0,
        "local_parents": 0,
    }
    for row in rows:
        path = (user_dir / str(row["relative_path"])).resolve()
        _safe_relative(user_dir, path, parent="runs")
        result = ingest_source_once(
            user_dir,
            db_path,
            requested_handle=handle,
            target_user_id=target_id,
            spec=SourceSpec(
                path=path,
                source_kind=str(row["source_kind"]),
                run_id=str(row["run_id"]),
                operation_id=str(row["operation_id"]),
                endpoint=(
                    "legacy" if row["source_kind"] == "legacy" else "timeline"
                ),
                expected_sha256=(
                    str(row["expected_sha256"])
                    if row["expected_sha256"] is not None
                    else None
                ),
            ),
            max_depth=max_depth,
        )
        totals["sources"] += 1
        totals["bytes_read"] += int(result["bytes_read"])
        totals["new_posts"] += int(result["new_posts"])
        totals["new_edges"] += int(result["new_edges"])
        totals["local_parents"] += int(result["local_parents"])
    observed_at = context_x.iso_now()
    with context_x.ContextDB(db_path, create=False) as database, (
        context_x.transaction(database.connection)
    ):
        unresolved = int(
            database.connection.execute(
                """SELECT COUNT(*) FROM archive_sources
                     WHERE source_kind IN ('modern','legacy')
                       AND status<>'committed'"""
            ).fetchone()[0]
        )
        if unresolved:
            raise LocalStateError("historical source reconciliation is incomplete")
        database.connection.execute(
            """INSERT INTO current_pointers(
                   pointer_name,relative_path,generation,updated_at
               ) VALUES ('source_history_reconciled','runs',0,?)
               ON CONFLICT(pointer_name) DO NOTHING""",
            (observed_at,),
        )
        database.connection.execute(
            """UPDATE run_registry SET processed_at=COALESCE(processed_at,?),
                       updated_at=MAX(updated_at,?) WHERE status<>'running'""",
            (observed_at, observed_at),
        )
        _mark_local_history_ready(database, observed_at=observed_at)
    return totals


def registered_manifest_candidates(
    user_dir: Path,
    db_path: Path,
    *,
    statuses: Iterable[str] | None = None,
    unprocessed_only: bool = False,
) -> list[Path]:
    selected = tuple(sorted(set(statuses or RUN_STATUSES)))
    if not selected or set(selected) - RUN_STATUSES:
        raise LocalStateError("run registry status filter is invalid")
    placeholders = ",".join("?" for _ in selected)
    where = " AND processed_at IS NULL" if unprocessed_only else ""
    with context_x.ContextDB(db_path, create=False) as database:
        rows = list(
            database.connection.execute(
                f"""SELECT manifest_path FROM run_registry
                      WHERE status IN ({placeholders}){where}
                      ORDER BY updated_at,run_id""",
                selected,
            )
        )
    paths = []
    for row in rows:
        path = (user_dir / str(row[0])).resolve()
        _safe_relative(user_dir, path, parent="runs")
        paths.append(path)
    return paths


def indexed_recovery_manifest_candidates(
    user_dir: Path,
    db_path: Path,
) -> list[Path] | None:
    """Read recovery paths without opening/migrating SQLite before identity proof."""
    if not db_path.is_file():
        return None
    try:
        connection = sqlite3.connect(
            f"file:{db_path}?mode=ro", uri=True, timeout=0.0
        )
        try:
            connection.row_factory = sqlite3.Row
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if not {"current_pointers", "run_registry"}.issubset(tables):
                return None
            ready = connection.execute(
                "SELECT 1 FROM current_pointers "
                "WHERE pointer_name='manifest_history_reconciled'"
            ).fetchone()
            if ready is None:
                return None
            relatives = [
                str(row[0])
                for row in connection.execute(
                    """SELECT manifest_path FROM run_registry
                         WHERE processed_at IS NULL
                         ORDER BY updated_at,run_id"""
                )
            ]
        finally:
            connection.close()
    except sqlite3.DatabaseError as exc:
        raise LocalStateError("indexed recovery registry is unreadable") from exc
    paths: list[Path] = []
    for relative_value in relatives:
        relative = Path(relative_value)
        if relative.is_absolute() or ".." in relative.parts:
            raise LocalStateError("indexed recovery manifest path is invalid")
        path = (user_dir / relative).resolve()
        _safe_relative(user_dir, path, parent="runs")
        if not path.is_file():
            raise LocalStateError("indexed recovery manifest is missing")
        paths.append(path)
    return paths


def mark_manifest_processed(
    db_path: Path, run_id: str, *, processed_at: str | None = None
) -> bool:
    with context_x.ContextDB(db_path, create=False) as database, (
        context_x.transaction(database.connection)
    ):
        cursor = database.connection.execute(
            """UPDATE run_registry SET processed_at=?,updated_at=?
                 WHERE run_id=? AND processed_at IS NULL""",
            (
                processed_at or context_x.iso_now(),
                processed_at or context_x.iso_now(),
                run_id,
            ),
        )
    return cursor.rowcount == 1


def reconcile_state_media_queue(
    user_dir: Path,
    db_path: Path,
    state: dict[str, Any],
) -> dict[str, Any]:
    """Move the legacy state-JSON media queue into durable asset jobs once."""
    pending = [
        dict(record)
        for record in state.get("pending_media", ())
        if isinstance(record, dict)
    ]
    unavailable = [
        dict(record)
        for record in state.get("unavailable_media", ())
        if isinstance(record, dict)
    ]
    if not pending and not unavailable:
        return {
            "status": "unchanged",
            "jobs": 0,
            "manual_review": int(
                (state.get("legacy_media_queue_migration") or {}).get(
                    "manual_review", 0
                )
            ),
        }

    payload = {"pending_media": pending, "unavailable_media": unavailable}
    payload_bytes = (_canonical_json(payload) + "\n").encode("utf-8")
    payload_sha256 = _sha256(payload_bytes)
    backup = (
        user_dir
        / "_state"
        / "backups"
        / f"state-media-queue.pre-goal5-{payload_sha256[:12]}.json"
    )
    if backup.is_file():
        existing = archive_x.load_json(backup, None)
        if existing != payload:
            raise LocalStateError("legacy media queue backup changed")
    else:
        archive_x.atomic_write_json(backup, payload)
        if archive_x.load_json(backup, None) != payload:
            raise LocalStateError("legacy media queue backup verification failed")

    def identities(record: dict[str, Any]) -> list[tuple[str, int]]:
        post_id = archive_x.id_string(record.get("post_id"))
        ordinal = record.get("media_number")
        if (not post_id or not isinstance(ordinal, int) or ordinal < 1) and record.get(
            "filename"
        ):
            match = archive_x.MEDIA_FILENAME_RE.match(
                Path(str(record["filename"])).name
            )
            if match:
                post_id = match.group(1)
                ordinal = int(match.group(2))
        if record.get("kind") == "post":
            count = record.get("expected_media_count")
            if not post_id or not isinstance(count, int) or count < 1:
                return []
            return [(post_id, number) for number in range(1, count + 1)]
        if not post_id or not isinstance(ordinal, int) or ordinal < 1:
            return []
        return [(post_id, ordinal)]

    valid: list[tuple[dict[str, Any], bool, list[tuple[str, int]]]] = []
    invalid_pending: list[dict[str, Any]] = []
    invalid_unavailable: list[dict[str, Any]] = []
    for records, terminal, invalid in (
        (pending, False, invalid_pending),
        (unavailable, True, invalid_unavailable),
    ):
        for record in records:
            owners = identities(record)
            if owners:
                valid.append((record, terminal, owners))
            else:
                invalid.append(record)

    observed_at = context_x.iso_now()
    jobs = 0
    with context_x.ContextDB(db_path, create=False) as database, (
        context_x.transaction(database.connection)
    ):
        for record, terminal, owners in valid:
            for post_id, ordinal in owners:
                database._enqueue_missing_descriptor(
                    "post", post_id, ordinal, destination_scope="main"
                )
                row = database.connection.execute(
                    """SELECT asset_id,state,descriptor_id FROM asset_jobs
                         WHERE owner_kind='post' AND owner_id=?
                           AND media_ordinal=?""",
                    (post_id, ordinal),
                ).fetchone()
                if row is None:
                    raise LocalStateError("legacy media queue job was not created")
                if row["state"] == "captured":
                    continue
                next_at = 0.0
                retry_value = record.get("next_retry_at")
                if retry_value and not terminal:
                    try:
                        next_at = archive_x.parse_datetime(str(retry_value)).timestamp()
                    except Exception:
                        next_at = 0.0
                state_value = "unavailable" if terminal else (
                    "pending" if row["descriptor_id"] is not None else "needs_refresh"
                )
                database.connection.execute(
                    """UPDATE asset_jobs SET state=?,compatibility_job=1,
                           destination_scope='main',attempts=MAX(attempts,?),
                           next_attempt_at=?,lease_token=NULL,
                           lease_started_at=NULL,last_error_class=?,
                           last_error_detail=NULL,updated_at=?
                         WHERE asset_id=? AND state<>'captured'""",
                    (
                        state_value,
                        max(0, int(record.get("attempts") or 0)),
                        next_at,
                        (
                            "legacy_media_unavailable"
                            if terminal
                            else "legacy_media_queue_migrated"
                        ),
                        observed_at,
                        int(row["asset_id"]),
                    ),
                )
                jobs += 1
        database.connection.execute(
            """INSERT INTO current_pointers(
                   pointer_name,relative_path,generation,updated_at
               ) VALUES ('state_media_queue_reconciled',?,0,?)
               ON CONFLICT(pointer_name) DO UPDATE SET
                   relative_path=excluded.relative_path,
                   updated_at=excluded.updated_at""",
            (
                f"_state/backups/{backup.name}#sha256={payload_sha256}",
                observed_at,
            ),
        )

    state["pending_media"] = invalid_pending
    state["unavailable_media"] = invalid_unavailable
    state["legacy_media_queue_migration"] = {
        "completed_at": observed_at,
        "backup_path": str(backup.relative_to(user_dir)),
        "sha256": payload_sha256,
        "jobs": jobs,
        "manual_review": len(invalid_pending) + len(invalid_unavailable),
    }
    return {
        "status": "complete",
        "jobs": jobs,
        "manual_review": len(invalid_pending) + len(invalid_unavailable),
        "backup_path": str(backup.relative_to(user_dir)),
    }


def reconcile_context_media_jobs(
    db_path: Path,
) -> dict[str, int]:
    """Bridge the pre-v3 target media lane into descriptor/asset jobs once."""
    with context_x.ContextDB(db_path, create=False) as database:
        marker = database.connection.execute(
            "SELECT 1 FROM current_pointers "
            "WHERE pointer_name='context_media_jobs_reconciled'"
        ).fetchone()
        if marker is not None:
            return {"targets": 0, "jobs": 0}
        rows = [
            dict(row)
            for row in database.connection.execute(
                """SELECT t.post_id,t.media_state,t.media_attempts,
                          t.media_next_attempt_at,o.raw_json
                     FROM targets t
                     LEFT JOIN observations o ON o.post_id=t.post_id
                    WHERE t.media_state<>'none'
                    ORDER BY t.post_id"""
            )
        ]
        prepared: list[tuple[dict[str, Any], int]] = []
        for row in rows:
            try:
                metadata = json.loads(str(row["raw_json"] or ""))
            except json.JSONDecodeError as exc:
                raise LocalStateError(
                    "historical context media metadata is invalid"
                ) from exc
            count = metadata.get("count") if isinstance(metadata, dict) else None
            # The pre-v3 media lane can legitimately mark a context request as
            # captured even when the focal post has no attached assets.  Those
            # observations carry an explicit ``count: 0`` and need no asset
            # jobs; only an absent or otherwise invalid count is unsafe to
            # migrate.
            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise LocalStateError(
                    "historical context media metadata lacks an asset count"
                )
            prepared.append((row, count))

        jobs = 0
        observed_at = context_x.iso_now()
        with context_x.transaction(database.connection):
            for row, count in prepared:
                post_id = str(row["post_id"])
                for ordinal in range(1, count + 1):
                    database._enqueue_missing_descriptor(
                        "post", post_id, ordinal, destination_scope="context"
                    )
                    job = database.connection.execute(
                        """SELECT asset_id,state,descriptor_id FROM asset_jobs
                             WHERE owner_kind='post' AND owner_id=?
                               AND media_ordinal=?""",
                        (post_id, ordinal),
                    ).fetchone()
                    if job is None:
                        raise LocalStateError(
                            "historical context media job was not created"
                        )
                    if job["state"] == "captured":
                        continue
                    old_state = str(row["media_state"])
                    if old_state == "unavailable":
                        state = "unavailable"
                    elif old_state == "manual_review":
                        state = "manual_review"
                    else:
                        state = (
                            "pending"
                            if job["descriptor_id"] is not None
                            else "needs_refresh"
                        )
                    database.connection.execute(
                        """UPDATE asset_jobs SET state=?,compatibility_job=1,
                               destination_scope='context',
                               attempts=MAX(attempts,?),next_attempt_at=?,
                               lease_token=NULL,lease_started_at=NULL,
                               last_error_class='context_media_lane_migrated',
                               last_error_detail=NULL,updated_at=?
                             WHERE asset_id=? AND state<>'captured'""",
                        (
                            state,
                            max(0, int(row["media_attempts"] or 0)),
                            max(0.0, float(row["media_next_attempt_at"] or 0)),
                            observed_at,
                            int(job["asset_id"]),
                        ),
                    )
                    jobs += 1
            database.connection.execute(
                """INSERT INTO current_pointers(
                       pointer_name,relative_path,generation,updated_at
                   ) VALUES ('context_media_jobs_reconciled','targets',0,?)
                   ON CONFLICT(pointer_name) DO NOTHING""",
                (observed_at,),
            )
    return {"targets": len(prepared), "jobs": jobs}


def portable_media_record(
    metadata: dict[str, Any],
    *,
    user_dir: Path,
    requested_handle: str,
    asset_path: Path,
    sidecar_path: Path,
) -> dict[str, Any]:
    relation = archive_x.relation_for(metadata, requested_handle)
    stat = asset_path.stat()
    ordinal = metadata.get("num")
    try:
        if not isinstance(ordinal, bool):
            ordinal = int(ordinal)
    except (TypeError, ValueError):
        pass
    return {
        "schema": archive_x.SCHEMA_NAME,
        "schema_version": archive_x.SCHEMA_VERSION,
        "requested_handle": requested_handle,
        "post_id": archive_x.id_string(metadata.get("tweet_id")),
        "relationship": relation,
        "author_handle": (metadata.get("author") or {}).get("name"),
        "posted_at": metadata.get("date"),
        "original_posted_at": (
            metadata.get("date_original") if relation == "repost" else metadata.get("date")
        ),
        "reposted_at": metadata.get("date") if relation == "repost" else None,
        "media_number": ordinal,
        "asset_path": asset_path.relative_to(user_dir).as_posix(),
        "sidecar_path": sidecar_path.relative_to(user_dir).as_posix(),
        "media_type": metadata.get("type"),
        "mime_type": mimetypes.guess_type(asset_path.name)[0],
        "bytes": int(stat.st_size),
        "sha256": metadata.get("sha256"),
        "alt_text": metadata.get("description"),
        "width": metadata.get("width"),
        "height": metadata.get("height"),
        "duration_seconds": metadata.get("duration"),
        "source_url": metadata.get("media_url"),
        "gallery_dl": metadata,
    }


def _prepare_media_record(user_dir: Path, record: dict[str, Any]) -> dict[str, Any]:
    media_path = Path(str(record.get("asset_path") or ""))
    sidecar_path = Path(str(record.get("sidecar_path") or ""))
    if (
        not media_path.parts
        or not sidecar_path.parts
        or media_path.is_absolute()
        or sidecar_path.is_absolute()
        or ".." in media_path.parts
        or ".." in sidecar_path.parts
    ):
        raise LocalStateError("portable media path is invalid")
    asset = (user_dir / media_path).resolve()
    sidecar = (user_dir / sidecar_path).resolve()
    if not asset.is_file() or not sidecar.is_file():
        raise LocalStateError("portable media evidence is missing")
    try:
        asset.relative_to((user_dir / "media").resolve())
        sidecar.relative_to((user_dir / "media").resolve())
    except ValueError as exc:
        raise LocalStateError("portable media escaped its media root") from exc
    digest = str(record.get("sha256") or "")
    if not descriptor_x.SHA256_RE.fullmatch(digest):
        raise LocalStateError("portable media digest evidence is invalid")
    ordinal_value = record.get("media_number")
    try:
        ordinal = (
            int(ordinal_value) if not isinstance(ordinal_value, bool) else 0
        )
    except (TypeError, ValueError):
        ordinal = 0
    if ordinal < 1:
        raise LocalStateError("portable media ordinal is invalid")
    relation = str(record.get("relationship") or "")
    if relation in {"profile_avatar", "profile_background"}:
        owner_kind = relation
        owner_id = "account"
    else:
        owner_kind = "post"
        owner_id = str(record.get("post_id") or "")
        if not owner_id.isdecimal() or int(owner_id) < 1:
            raise LocalStateError("portable media post identity is invalid")
    stat = asset.stat()
    if int(record.get("bytes") or -1) != stat.st_size:
        raise LocalStateError("portable media size evidence changed")
    sidecar_value = archive_x.load_json(sidecar, None)
    if (
        not isinstance(sidecar_value, dict)
        or sidecar_value.get("sha256") != digest
        or archive_x.id_string(sidecar_value.get("num")) != str(ordinal)
    ):
        raise LocalStateError("portable media sidecar evidence changed")
    normalized_record = dict(record)
    normalized_record["media_number"] = ordinal
    normalized_record["asset_path"] = asset.relative_to(user_dir).as_posix()
    normalized_record["sidecar_path"] = sidecar.relative_to(user_dir).as_posix()
    normalized_json = _canonical_json(normalized_record)
    return {
        "media_path": asset.relative_to(user_dir).as_posix(),
        "sidecar_path": sidecar.relative_to(user_dir).as_posix(),
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "media_ordinal": ordinal,
        "normalized_json": normalized_json,
        "normalized_sha256": _sha256(normalized_json.encode("utf-8")),
        "final_sha256": digest,
        "final_bytes": int(stat.st_size),
        "stat_device": int(stat.st_dev),
        "stat_inode": int(stat.st_ino),
        "stat_size": int(stat.st_size),
        "stat_mtime_ns": int(stat.st_mtime_ns),
    }


def _upsert_media_record(
    database: context_x.ContextDB,
    *,
    prepared: dict[str, Any],
    generation: int,
    asset_id: int | None,
    captured_at: str,
) -> None:
    database.connection.execute(
        """INSERT INTO archive_media(
               media_path,sidecar_path,owner_kind,owner_id,media_ordinal,
               asset_id,normalized_json,normalized_sha256,final_sha256,
               final_bytes,stat_device,stat_inode,stat_size,stat_mtime_ns,
               durable_generation,captured_at
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(media_path) DO UPDATE SET
               sidecar_path=excluded.sidecar_path,
               owner_kind=excluded.owner_kind,owner_id=excluded.owner_id,
               media_ordinal=excluded.media_ordinal,
               asset_id=COALESCE(excluded.asset_id,archive_media.asset_id),
               normalized_json=excluded.normalized_json,
               normalized_sha256=excluded.normalized_sha256,
               final_sha256=excluded.final_sha256,
               final_bytes=excluded.final_bytes,
               stat_device=excluded.stat_device,stat_inode=excluded.stat_inode,
               stat_size=excluded.stat_size,stat_mtime_ns=excluded.stat_mtime_ns,
               durable_generation=excluded.durable_generation,
               captured_at=excluded.captured_at""",
        (
            prepared["media_path"],
            prepared["sidecar_path"],
            prepared["owner_kind"],
            prepared["owner_id"],
            prepared["media_ordinal"],
            asset_id,
            prepared["normalized_json"],
            prepared["normalized_sha256"],
            prepared["final_sha256"],
            prepared["final_bytes"],
            prepared["stat_device"],
            prepared["stat_inode"],
            prepared["stat_size"],
            prepared["stat_mtime_ns"],
            generation,
            captured_at,
        ),
    )


def reconcile_media_index(
    user_dir: Path,
    db_path: Path,
    *,
    requested_handle: str,
    disk_free: Callable[[Path], int] = lambda path: shutil.disk_usage(path).free,
) -> dict[str, int]:
    """Import the existing compatibility view once; future assets upsert directly."""
    with context_x.ContextDB(db_path) as database:
        marker = database.connection.execute(
            "SELECT 1 FROM current_pointers WHERE pointer_name='media_history_reconciled'"
        ).fetchone()
        if marker is not None:
            return {"files": 0, "bytes_read": 0, "generation": 0}
    media_view = user_dir / "dataset" / "media.jsonl"
    bytes_read = media_view.stat().st_size if media_view.is_file() else 0
    required = max(EXPORT_MIN_HEADROOM, int(bytes_read) * 3)
    if disk_free(db_path.parent) < required:
        raise LocalStateError("insufficient free space for media reconciliation")
    observed_at = context_x.iso_now()
    with context_x.ContextDB(db_path) as database:
        database.connection.execute("DROP TABLE IF EXISTS temp.goal5_media_stage")
        database.connection.execute(
            """CREATE TEMP TABLE goal5_media_stage(
                   media_path TEXT PRIMARY KEY,sidecar_path TEXT NOT NULL UNIQUE,
                   owner_kind TEXT NOT NULL,owner_id TEXT NOT NULL,
                   media_ordinal INTEGER NOT NULL,normalized_json TEXT NOT NULL,
                   normalized_sha256 TEXT NOT NULL,final_sha256 TEXT NOT NULL,
                   final_bytes INTEGER NOT NULL,stat_device INTEGER,
                   stat_inode INTEGER,stat_size INTEGER NOT NULL,
                   stat_mtime_ns INTEGER
               ) WITHOUT ROWID"""
        )
        count = 0
        for record in (
            archive_x.iter_jsonl(media_view) if media_view.is_file() else ()
        ):
            if not isinstance(record, dict):
                raise LocalStateError("portable media record is invalid")
            prepared = _prepare_media_record(user_dir, record)
            with context_x.transaction(database.connection):
                database.connection.execute(
                    """INSERT INTO goal5_media_stage VALUES (
                           :media_path,:sidecar_path,:owner_kind,:owner_id,
                           :media_ordinal,:normalized_json,:normalized_sha256,
                           :final_sha256,:final_bytes,:stat_device,:stat_inode,
                           :stat_size,:stat_mtime_ns)""",
                    prepared,
                )
            count += 1
        with context_x.transaction(database.connection):
            generation = _next_generation(
                database,
                observed_at=observed_at,
                dirty_views=("media",) if count else (),
            )
            for row in database.connection.execute(
                "SELECT * FROM goal5_media_stage ORDER BY media_path"
            ):
                prepared = dict(row)
                _upsert_media_record(
                    database,
                    prepared=prepared,
                    generation=generation,
                    asset_id=None,
                    captured_at=observed_at,
                )
                if prepared["owner_kind"] != "post":
                    continue
                job = database.connection.execute(
                    """SELECT asset_id,descriptor_id,state FROM asset_jobs
                         WHERE owner_kind='post' AND owner_id=?
                           AND media_ordinal=?""",
                    (prepared["owner_id"], prepared["media_ordinal"]),
                ).fetchone()
                scope = (
                    "context"
                    if Path(prepared["media_path"]).parts[:2]
                    == ("media", "context")
                    else "main"
                )
                if job is None:
                    cursor = database.connection.execute(
                        """INSERT INTO asset_jobs(
                               owner_kind,owner_id,media_ordinal,state,
                               compatibility_job,destination_scope,
                               expected_relative_path,final_relative_path,
                               final_sha256,final_bytes,verified_device,
                               verified_inode,verified_size,verified_mtime_ns,
                               created_at,updated_at,completed_at
                           ) VALUES ('post',?,?,'captured',1,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            prepared["owner_id"],
                            prepared["media_ordinal"],
                            scope,
                            prepared["media_path"],
                            prepared["media_path"],
                            prepared["final_sha256"],
                            prepared["final_bytes"],
                            prepared["stat_device"],
                            prepared["stat_inode"],
                            prepared["stat_size"],
                            prepared["stat_mtime_ns"],
                            observed_at,
                            observed_at,
                            observed_at,
                        ),
                    )
                    asset_id = int(cursor.lastrowid)
                else:
                    asset_id = int(job["asset_id"])
                    database.connection.execute(
                        """UPDATE asset_jobs SET state='captured',
                               compatibility_job=CASE WHEN descriptor_id IS NULL
                                                      THEN 1 ELSE compatibility_job END,
                               destination_scope=?,expected_relative_path=?,
                               final_relative_path=?,final_sha256=?,final_bytes=?,
                               verified_device=?,verified_inode=?,verified_size=?,
                               verified_mtime_ns=?,next_attempt_at=0,
                               lease_token=NULL,lease_started_at=NULL,
                               last_error_class=NULL,last_error_detail=NULL,
                               completed_at=?,updated_at=? WHERE asset_id=?""",
                        (
                            scope,
                            prepared["media_path"],
                            prepared["media_path"],
                            prepared["final_sha256"],
                            prepared["final_bytes"],
                            prepared["stat_device"],
                            prepared["stat_inode"],
                            prepared["stat_size"],
                            prepared["stat_mtime_ns"],
                            observed_at,
                            observed_at,
                            asset_id,
                        ),
                    )
                database.connection.execute(
                    "UPDATE archive_media SET asset_id=? WHERE media_path=?",
                    (asset_id, prepared["media_path"]),
                )
                database._update_post_asset_rollup(
                    "post", str(prepared["owner_id"])
                )
            database.connection.execute(
                """INSERT INTO current_pointers(
                       pointer_name,relative_path,generation,updated_at
                   ) VALUES ('media_history_reconciled','dataset/media.jsonl',?,?)
                   ON CONFLICT(pointer_name) DO NOTHING""",
                (generation, observed_at),
            )
            _mark_local_history_ready(database, observed_at=observed_at)
    return {
        "files": count,
        "bytes_read": int(bytes_read),
        "generation": generation,
    }


def audit_registered_media(user_dir: Path, db_path: Path) -> dict[str, int]:
    """Explicitly hash every indexed asset and verify its sidecar evidence."""
    with context_x.ContextDB(db_path, create=False) as database:
        rows = [dict(row) for row in database.connection.execute(
            "SELECT * FROM archive_media ORDER BY media_path"
        )]
    checked = 0
    bytes_read = 0
    updates: list[tuple[int, int, int, int, str]] = []
    for row in rows:
        media_path = Path(str(row["media_path"]))
        sidecar_path = Path(str(row["sidecar_path"]))
        if (
            media_path.is_absolute()
            or sidecar_path.is_absolute()
            or ".." in media_path.parts
            or ".." in sidecar_path.parts
        ):
            raise LocalStateError("indexed media path is invalid")
        asset = (user_dir / media_path).resolve()
        sidecar = (user_dir / sidecar_path).resolve()
        try:
            asset.relative_to((user_dir / "media").resolve())
            sidecar.relative_to((user_dir / "media").resolve())
        except ValueError as exc:
            raise LocalStateError("indexed media escaped its media root") from exc
        if not asset.is_file() or not sidecar.is_file():
            raise LocalStateError("indexed media evidence is missing")
        stat = asset.stat()
        digest = archive_x.sha256_file(asset)
        bytes_read += int(stat.st_size)
        checked += 1
        sidecar_value = archive_x.load_json(sidecar, None)
        if (
            digest != row["final_sha256"]
            or stat.st_size != int(row["final_bytes"])
            or not isinstance(sidecar_value, dict)
            or sidecar_value.get("sha256") != digest
        ):
            raise LocalStateError("media integrity audit found changed content")
        updates.append(
            (
                int(stat.st_dev),
                int(stat.st_ino),
                int(stat.st_size),
                int(stat.st_mtime_ns),
                str(row["media_path"]),
            )
        )
    with context_x.ContextDB(db_path, create=False) as database, (
        context_x.transaction(database.connection)
    ):
        database.connection.executemany(
            """UPDATE archive_media SET stat_device=?,stat_inode=?,stat_size=?,
                       stat_mtime_ns=? WHERE media_path=?""",
            updates,
        )
    return {"files_checked": checked, "bytes_read": bytes_read, "changed": 0}


def _post_rows(
    connection: sqlite3.Connection, *, relationship: str | None = None
) -> Iterator[dict[str, Any]]:
    where = ""
    parameters: tuple[Any, ...] = ()
    if relationship == "authored":
        where = "WHERE relationship IN ('post','reply')"
    elif relationship is not None:
        where = "WHERE relationship=?"
        parameters = (relationship,)
    for row in connection.execute(
        f"""SELECT normalized_json FROM archive_posts {where}
             ORDER BY COALESCE(posted_at,''),length(post_id),post_id""",
        parameters,
    ):
        yield json.loads(str(row[0]))


def _media_rows(connection: sqlite3.Connection) -> Iterator[dict[str, Any]]:
    for row in connection.execute(
        """SELECT normalized_json FROM archive_media
             ORDER BY captured_at,media_path"""
    ):
        yield json.loads(str(row[0]))


def _context_post_rows(
    connection: sqlite3.Connection,
    *,
    handle: str,
    target_user_id: str,
) -> Iterator[dict[str, Any]]:
    for row in connection.execute(
        "SELECT raw_json FROM observations ORDER BY length(post_id),post_id"
    ):
        yield context_x.normalize_context(
            json.loads(str(row[0])), handle, target_user_id
        )


def _edge_rows(connection: sqlite3.Connection, *, handle: str, target_id: str):
    for row in connection.execute(
        """SELECT e.*,t.state,t.last_error_class,t.unavailable_at
             FROM reply_edges e JOIN targets t ON t.post_id=e.parent_id
            ORDER BY length(e.child_id),e.child_id"""
    ):
        yield {
            "schema": "gdl-x-reply-edge",
            "schema_version": context_x.SCHEMA_VERSION,
            "requested_handle": handle,
            "requested_user_id": target_id,
            "child_post_id": row["child_id"],
            "parent_post_id": row["parent_id"],
            "conversation_id": row["conversation_id"],
            "depth": row["depth"],
            "parent_state": row["state"],
            "unavailable_reason": row["last_error_class"],
            "unavailable_at": row["unavailable_at"],
            "cycle_detected": bool(row["cycle_detected"]),
            "discovered_run_id": row["discovered_run_id"],
            "discovered_at": row["discovered_at"],
        }


def _write_export_view(
    database: context_x.ContextDB,
    *,
    view_name: str,
    path: Path,
    handle: str,
    target_id: str,
) -> dict[str, Any]:
    if view_name == "posts":
        count = archive_x.atomic_write_jsonl(path, _post_rows(database.connection))
    elif view_name == "authored_posts":
        count = archive_x.atomic_write_jsonl(
            path, _post_rows(database.connection, relationship="authored")
        )
    elif view_name == "reposts":
        count = archive_x.atomic_write_jsonl(
            path, _post_rows(database.connection, relationship="repost")
        )
    elif view_name == "media":
        count = archive_x.atomic_write_jsonl(path, _media_rows(database.connection))
    elif view_name == "context_posts":
        count = archive_x.atomic_write_jsonl(
            path,
            _context_post_rows(
                database.connection, handle=handle, target_user_id=target_id
            ),
        )
    elif view_name == "reply_edges":
        count = archive_x.atomic_write_jsonl(
            path,
            _edge_rows(database.connection, handle=handle, target_id=target_id),
        )
    elif view_name == "context_status":
        archive_x.atomic_write_json(path, fast_context_status(database))
        count = 1
    else:
        raise LocalStateError("unknown export view")
    return {
        "relative_path": "",
        "sha256": archive_x.sha256_file(path),
        "bytes": int(path.stat().st_size),
        "rows": count,
    }


def _export_pointer_path(user_dir: Path) -> Path:
    return user_dir / "dataset" / "current-export.json"


def _database_export_pointer(
    database: context_x.ContextDB,
) -> dict[str, Any] | None:
    row = database.connection.execute(
        """SELECT p.relative_path,p.generation,b.manifest_sha256
             FROM current_pointers p JOIN export_batches b
               ON b.generation=p.generation
            WHERE p.pointer_name='portable_export' AND b.state='published'"""
    ).fetchone()
    if (
        row is None
        or not row["relative_path"]
        or not descriptor_x.SHA256_RE.fullmatch(str(row["manifest_sha256"] or ""))
    ):
        return None
    return {
        "schema": EXPORT_SCHEMA,
        "schema_version": EXPORT_SCHEMA_VERSION,
        "generation": int(row["generation"]),
        "manifest_path": str(row["relative_path"]),
        "manifest_sha256": str(row["manifest_sha256"]),
        "published_at": context_x.iso_now(),
    }


def _restore_database_export_pointer(
    user_dir: Path, database: context_x.ContextDB
) -> bool:
    pointer = _database_export_pointer(database)
    path = _export_pointer_path(user_dir)
    if pointer is None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return False
    archive_x.atomic_write_json(path, pointer)
    return True


def _cleanup_export_temporaries(user_dir: Path, generation: int) -> int:
    exports_root = (user_dir / "dataset" / "exports").resolve()
    if not exports_root.is_dir() or generation < 1:
        return 0
    prefix = f".g{generation:020d}.tmp-"
    removed = 0
    for path in exports_root.glob(prefix + "*"):
        resolved = path.resolve()
        if (
            resolved.parent != exports_root
            or not resolved.name.startswith(prefix)
            or not resolved.is_dir()
        ):
            raise LocalStateError("portable export temporary path is invalid")
        shutil.rmtree(resolved)
        removed += 1
    return removed


def _load_export_manifest(
    user_dir: Path, relative_path: str, expected_sha256: str
) -> tuple[dict[str, Any], Path]:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise LocalStateError("portable export manifest path is invalid")
    path = (user_dir / relative).resolve()
    try:
        path.relative_to((user_dir / "dataset").resolve())
    except ValueError as exc:
        raise LocalStateError("portable export manifest escaped dataset") from exc
    if not path.is_file() or archive_x.sha256_file(path) != expected_sha256:
        raise LocalStateError("portable export manifest evidence changed")
    value = archive_x.load_json(path, None)
    if (
        not isinstance(value, dict)
        or value.get("schema") != EXPORT_SCHEMA
        or value.get("schema_version") != EXPORT_SCHEMA_VERSION
        or not isinstance(value.get("generation"), int)
        or value["generation"] < 1
        or set(value.get("views") or {}) != set(EXPORT_VIEW_FILES)
    ):
        raise LocalStateError("portable export manifest is invalid")
    return value, path


def _verify_export_manifest(user_dir: Path, manifest: dict[str, Any]) -> int:
    bytes_read = 0
    for view_name, evidence in manifest["views"].items():
        if not isinstance(evidence, dict):
            raise LocalStateError("portable export view evidence is invalid")
        relative = Path(str(evidence.get("relative_path") or ""))
        digest = str(evidence.get("sha256") or "")
        size = evidence.get("bytes")
        rows = evidence.get("rows")
        durable = evidence.get("durable_generation")
        if (
            view_name not in EXPORT_VIEW_FILES
            or relative.is_absolute()
            or ".." in relative.parts
            or not descriptor_x.SHA256_RE.fullmatch(digest)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(rows, int)
            or rows < 0
            or not isinstance(durable, int)
            or durable < 0
        ):
            raise LocalStateError("portable export view evidence is invalid")
        path = (user_dir / relative).resolve()
        try:
            path.relative_to((user_dir / "dataset").resolve())
        except ValueError as exc:
            raise LocalStateError("portable export view escaped dataset") from exc
        if not path.is_file() or path.stat().st_size != size:
            raise LocalStateError("portable export view file is missing")
        bytes_read += size
        if archive_x.sha256_file(path) != digest:
            raise LocalStateError("portable export view digest changed")
    return bytes_read


def _finalize_export_database(
    database: context_x.ContextDB,
    *,
    manifest: dict[str, Any],
    manifest_relative: str,
    manifest_sha256: str,
    completed_at: str,
) -> None:
    generation = int(manifest["generation"])
    with context_x.transaction(database.connection):
        database.connection.execute(
            """INSERT INTO export_batches(
                   generation,state,started_at,completed_at,manifest_sha256
               ) VALUES (?,'published',?,?,?)
               ON CONFLICT(generation) DO UPDATE SET
                   state='published',completed_at=excluded.completed_at,
                   manifest_sha256=excluded.manifest_sha256,error_class=NULL""",
            (
                generation,
                str(manifest.get("started_at") or completed_at),
                completed_at,
                manifest_sha256,
            ),
        )
        for view_name, evidence in manifest["views"].items():
            cursor = database.connection.execute(
                """UPDATE export_views SET exported_generation=?,status='current',
                       relative_path=?,export_sha256=?,export_bytes=?,row_count=?,
                       updated_at=?
                     WHERE view_name=? AND durable_generation=?""",
                (
                    int(evidence["durable_generation"]),
                    str(evidence["relative_path"]),
                    str(evidence["sha256"]),
                    int(evidence["bytes"]),
                    int(evidence["rows"]),
                    completed_at,
                    view_name,
                    int(evidence["durable_generation"]),
                ),
            )
            if cursor.rowcount != 1:
                raise LocalStateError(
                    "portable export durable generation changed during publication"
                )
        database.connection.execute(
            """INSERT INTO current_pointers(
                   pointer_name,relative_path,generation,updated_at
               ) VALUES ('portable_export',?,?,?)
               ON CONFLICT(pointer_name) DO UPDATE SET
                   relative_path=excluded.relative_path,
                   generation=excluded.generation,updated_at=excluded.updated_at""",
            (manifest_relative, generation, completed_at),
        )


def recover_export_publication(user_dir: Path, db_path: Path) -> dict[str, int]:
    """Finish or roll back the exceptional pointer/database crash window."""
    with context_x.ContextDB(db_path, create=False) as database:
        pending = database.connection.execute(
            """SELECT generation FROM export_batches
                 WHERE state='preparing' ORDER BY generation DESC LIMIT 1"""
        ).fetchone()
        writing = database.connection.execute(
            "SELECT 1 FROM export_views WHERE status='writing' LIMIT 1"
        ).fetchone()
        if pending is None and writing is None:
            return {"recovered": 0, "bytes_read": 0}
        pending_generation = int(pending[0]) if pending is not None else None
        pointer = archive_x.load_json(_export_pointer_path(user_dir), None)
        pointer_matches = bool(
            isinstance(pointer, dict)
            and pending_generation is not None
            and pointer.get("generation") == pending_generation
        )
        if not pointer_matches and pending_generation is not None:
            placed_manifest = (
                user_dir
                / "dataset"
                / "exports"
                / f"g{pending_generation:020d}"
                / "manifest.json"
            )
            if placed_manifest.is_file():
                manifest_sha = archive_x.sha256_file(placed_manifest)
                manifest_relative = placed_manifest.relative_to(user_dir).as_posix()
                manifest, _path = _load_export_manifest(
                    user_dir, manifest_relative, manifest_sha
                )
                _verify_export_manifest(user_dir, manifest)
                pointer = {
                    "schema": EXPORT_SCHEMA,
                    "schema_version": EXPORT_SCHEMA_VERSION,
                    "generation": pending_generation,
                    "manifest_path": manifest_relative,
                    "manifest_sha256": manifest_sha,
                    "published_at": context_x.iso_now(),
                }
                archive_x.atomic_write_json(_export_pointer_path(user_dir), pointer)
                pointer_matches = True
        if not pointer_matches:
            with context_x.transaction(database.connection):
                database.connection.execute(
                    """UPDATE export_batches SET state='failed',
                           error_class='publication_incomplete'
                         WHERE state='preparing'"""
                )
                database.connection.execute(
                    "UPDATE export_views SET status='dirty' WHERE status='writing'"
                )
            cleaned = (
                _cleanup_export_temporaries(user_dir, pending_generation)
                if pending_generation is not None
                else 0
            )
            return {"recovered": 0, "bytes_read": 0, "temporaries_removed": cleaned}
        assert isinstance(pointer, dict)
        manifest_relative = str(pointer.get("manifest_path") or "")
        manifest_sha = str(pointer.get("manifest_sha256") or "")
        if not descriptor_x.SHA256_RE.fullmatch(manifest_sha):
            raise LocalStateError("portable export pointer is invalid")
        manifest, _path = _load_export_manifest(
            user_dir, manifest_relative, manifest_sha
        )
        bytes_read = _verify_export_manifest(user_dir, manifest)
        try:
            _finalize_export_database(
                database,
                manifest=manifest,
                manifest_relative=manifest_relative,
                manifest_sha256=manifest_sha,
                completed_at=context_x.iso_now(),
            )
        except LocalStateError:
            with context_x.transaction(database.connection):
                database.connection.execute(
                    """UPDATE export_batches SET state='failed',
                               error_class='generation_changed'
                         WHERE generation=? AND state='preparing'""",
                    (int(manifest["generation"]),),
                )
                database.connection.execute(
                    "UPDATE export_views SET status='dirty' WHERE status='writing'"
                )
            _restore_database_export_pointer(user_dir, database)
            raise
    return {"recovered": 1, "bytes_read": bytes_read}


def _repair_compatibility_links(
    user_dir: Path, manifest: dict[str, Any]
) -> int:
    dataset = user_dir / "dataset"
    replaced = 0
    for view_name, filename in EXPORT_VIEW_FILES.items():
        source = (user_dir / manifest["views"][view_name]["relative_path"]).resolve()
        destination = dataset / filename
        temporary = dataset / f".{filename}.link-{secrets.token_hex(6)}"
        try:
            try:
                os.link(source, temporary)
            except OSError:
                shutil.copyfile(source, temporary)
            os.chmod(temporary, 0o600)
            os.replace(temporary, destination)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        replaced += 1
    return replaced


def _current_export_without_payload_read(
    database: context_x.ContextDB,
) -> tuple[bool, str | None, int]:
    dirty = int(
        database.connection.execute(
            """SELECT COUNT(*) FROM export_views
                 WHERE status<>'current'
                    OR exported_generation<>durable_generation"""
        ).fetchone()[0]
    )
    pointer = database.connection.execute(
        """SELECT relative_path,generation FROM current_pointers
             WHERE pointer_name='portable_export'"""
    ).fetchone()
    return (
        dirty == 0 and pointer is not None,
        str(pointer[0]) if pointer is not None else None,
        int(pointer[1]) if pointer is not None else 0,
    )


def materialize_exports(
    user_dir: Path,
    db_path: Path,
    *,
    fault: Callable[[str], None] | None = None,
    disk_free: Callable[[Path], int] = lambda path: shutil.disk_usage(path).free,
) -> dict[str, Any]:
    """Publish changed views once; unchanged state reads/writes no view payload."""
    recover_export_publication(user_dir, db_path)
    with context_x.ContextDB(db_path, create=False) as database:
        current, manifest_relative, generation = _current_export_without_payload_read(
            database
        )
        if current:
            authoritative_pointer = _database_export_pointer(database)
            filesystem_pointer = archive_x.load_json(
                _export_pointer_path(user_dir), None
            )
            if not (
                authoritative_pointer is not None
                and isinstance(filesystem_pointer, dict)
                and filesystem_pointer.get("generation") == generation
                and filesystem_pointer.get("manifest_path") == manifest_relative
                and filesystem_pointer.get("manifest_sha256")
                == authoritative_pointer["manifest_sha256"]
            ):
                if not _restore_database_export_pointer(user_dir, database):
                    raise LocalStateError("portable export database pointer is invalid")
            compatibility = database.connection.execute(
                """SELECT generation FROM current_pointers
                     WHERE pointer_name='compatibility_export'"""
            ).fetchone()
            compatibility_generation = (
                int(compatibility[0]) if compatibility is not None else -1
            )
            repaired = 0
            if compatibility_generation != generation:
                pointer_path = _export_pointer_path(user_dir)
                pointer = archive_x.load_json(pointer_path, None)
                if not isinstance(pointer, dict):
                    raise LocalStateError("portable export pointer is missing")
                manifest_sha = str(pointer.get("manifest_sha256") or "")
                if not descriptor_x.SHA256_RE.fullmatch(manifest_sha):
                    raise LocalStateError("portable export pointer is invalid")
                manifest, _path = _load_export_manifest(
                    user_dir, str(pointer.get("manifest_path") or ""), manifest_sha
                )
                repaired = _repair_compatibility_links(user_dir, manifest)
                with context_x.transaction(database.connection):
                    database.connection.execute(
                        """INSERT INTO current_pointers(
                               pointer_name,relative_path,generation,updated_at
                           ) VALUES ('compatibility_export','dataset',?,?)
                           ON CONFLICT(pointer_name) DO UPDATE SET
                               generation=excluded.generation,
                               updated_at=excluded.updated_at""",
                        (generation, context_x.iso_now()),
                    )
            return {
                "status": "unchanged",
                "generation": generation,
                "views_written": 0,
                "bytes_written": 0,
                "payload_bytes_read": 0,
                "manifest_path": manifest_relative,
                "compatibility_links": repaired,
            }
        account = database.connection.execute(
            "SELECT user_id,canonical_handle FROM archive_account WHERE singleton=1"
        ).fetchone()
        if account is None:
            raise LocalStateError("portable export archive identity is missing")
        target_id, handle = str(account[0]), str(account[1])
        generation = int(
            database.connection.execute(
                "SELECT current_generation FROM archive_generation WHERE singleton=1"
            ).fetchone()[0]
        )
        if generation < 1:
            raise LocalStateError("portable export has no durable generation")
        view_rows = {
            str(row["view_name"]): dict(row)
            for row in database.connection.execute("SELECT * FROM export_views")
        }
        dirty_views = [
            name
            for name, row in view_rows.items()
            if row["status"] != "current"
            or int(row["exported_generation"]) != int(row["durable_generation"])
        ]
        estimated_bytes = 0
        for view_name in dirty_views:
            row = view_rows[view_name]
            if row["export_bytes"] is not None:
                estimated_bytes += int(row["export_bytes"])
                continue
            compatibility = user_dir / "dataset" / EXPORT_VIEW_FILES[view_name]
            if compatibility.is_file():
                estimated_bytes += int(compatibility.stat().st_size)
        required = max(EXPORT_MIN_HEADROOM, estimated_bytes * 2)
        if disk_free(user_dir) < required:
            raise LocalStateError("insufficient free space for portable export")
        started_at = context_x.iso_now()
        with context_x.transaction(database.connection):
            database.connection.execute(
                """INSERT INTO export_batches(generation,state,started_at)
                   VALUES (?,'preparing',?)
                   ON CONFLICT(generation) DO UPDATE SET
                       state='preparing',started_at=excluded.started_at,
                       completed_at=NULL,manifest_sha256=NULL,error_class=NULL""",
                (generation, started_at),
            )
            placeholders = ",".join("?" for _ in dirty_views)
            database.connection.execute(
                f"UPDATE export_views SET status='writing',updated_at=? "
                f"WHERE view_name IN ({placeholders})",
                (started_at, *dirty_views),
            )
        if fault:
            fault("after_database_prepare")

        exports_root = user_dir / "dataset" / "exports"
        exports_root.mkdir(parents=True, exist_ok=True)
        final_dir = exports_root / f"g{generation:020d}"
        if final_dir.exists():
            raise LocalStateError("portable export generation path already exists")
        temporary = exports_root / (
            f".g{generation:020d}.tmp-{secrets.token_hex(8)}"
        )
        temporary.mkdir(mode=0o700)
        evidence: dict[str, dict[str, Any]] = {}
        bytes_written = 0
        views_written = 0
        try:
            for view_name in EXPORT_VIEW_FILES:
                row = view_rows[view_name]
                if view_name not in dirty_views:
                    evidence[view_name] = {
                        "relative_path": str(row["relative_path"]),
                        "sha256": str(row["export_sha256"]),
                        "bytes": int(row["export_bytes"]),
                        "rows": int(row["row_count"]),
                        "durable_generation": int(row["durable_generation"]),
                    }
                    continue
                path = temporary / EXPORT_VIEW_FILES[view_name]
                result = _write_export_view(
                    database,
                    view_name=view_name,
                    path=path,
                    handle=handle,
                    target_id=target_id,
                )
                result["relative_path"] = (
                    Path("dataset")
                    / "exports"
                    / final_dir.name
                    / EXPORT_VIEW_FILES[view_name]
                ).as_posix()
                result["durable_generation"] = int(row["durable_generation"])
                evidence[view_name] = result
                bytes_written += int(result["bytes"])
                views_written += 1
            manifest = {
                "schema": EXPORT_SCHEMA,
                "schema_version": EXPORT_SCHEMA_VERSION,
                "generation": generation,
                "started_at": started_at,
                "completed_at": context_x.iso_now(),
                "views": evidence,
            }
            archive_x.atomic_write_json(temporary / "manifest.json", manifest)
            if fault:
                fault("after_view_placement")
            os.replace(temporary, final_dir)
            if fault:
                fault("after_generation_placement")
            manifest_path = final_dir / "manifest.json"
            manifest_relative = manifest_path.relative_to(user_dir).as_posix()
            manifest_sha = archive_x.sha256_file(manifest_path)
            pointer = {
                "schema": EXPORT_SCHEMA,
                "schema_version": EXPORT_SCHEMA_VERSION,
                "generation": generation,
                "manifest_path": manifest_relative,
                "manifest_sha256": manifest_sha,
                "published_at": context_x.iso_now(),
            }
            archive_x.atomic_write_json(_export_pointer_path(user_dir), pointer)
            if fault:
                fault("after_pointer_publication")
            _finalize_export_database(
                database,
                manifest=manifest,
                manifest_relative=manifest_relative,
                manifest_sha256=manifest_sha,
                completed_at=str(manifest["completed_at"]),
            )
            if fault:
                fault("after_database_finalization")
            linked = _repair_compatibility_links(user_dir, manifest)
            with context_x.transaction(database.connection):
                database.connection.execute(
                    """INSERT INTO current_pointers(
                           pointer_name,relative_path,generation,updated_at
                       ) VALUES ('compatibility_export','dataset',?,?)
                       ON CONFLICT(pointer_name) DO UPDATE SET
                           generation=excluded.generation,
                           updated_at=excluded.updated_at""",
                    (generation, context_x.iso_now()),
                )
        except BaseException:
            # The directory/pointer/database ordering is recovered explicitly;
            # never claim current merely because some files were written.
            raise
    return {
        "status": "published",
        "generation": generation,
        "views_written": views_written,
        "bytes_written": bytes_written,
        "payload_bytes_read": 0,
        "compatibility_links": linked,
        "manifest_path": manifest_relative,
    }


def checkpoint_exports(
    user_dir: Path,
    db_path: Path,
    *,
    force: bool = False,
    generation_threshold: int = 1_000,
    maximum_dirty_age: float = 86_400.0,
    now: Callable[[], float] = time.time,
) -> dict[str, Any]:
    """Publish at a bounded checkpoint, or report durable truth as dirty."""
    if generation_threshold < 1 or maximum_dirty_age < 0:
        raise LocalStateError("portable export checkpoint policy is invalid")
    recover_export_publication(user_dir, db_path)
    with context_x.ContextDB(db_path, create=False) as database:
        durable_generation = int(
            database.connection.execute(
                "SELECT current_generation FROM archive_generation WHERE singleton=1"
            ).fetchone()[0]
        )
        pointer = database.connection.execute(
            """SELECT generation FROM current_pointers
                 WHERE pointer_name='portable_export'"""
        ).fetchone()
        exported_generation = int(pointer[0]) if pointer is not None else 0
        dirty = [
            dict(row)
            for row in database.connection.execute(
                """SELECT view_name,durable_generation,exported_generation,
                          updated_at FROM export_views
                     WHERE status<>'current'
                        OR durable_generation<>exported_generation
                     ORDER BY view_name"""
            )
        ]
    if durable_generation < 1:
        return {
            "status": "empty",
            "durable_generation": durable_generation,
            "exported_generation": exported_generation,
            "dirty_views": len(dirty),
        }
    if not dirty:
        result = materialize_exports(user_dir, db_path)
        return {
            **result,
            "durable_generation": durable_generation,
            "exported_generation": durable_generation,
            "policy_reason": "repair_or_unchanged",
        }

    dirty_since = min(str(row["updated_at"]) for row in dirty)
    try:
        dirty_age = max(
            0.0, now() - archive_x.parse_datetime(dirty_since).timestamp()
        )
    except Exception as exc:
        raise LocalStateError("portable export dirty timestamp is invalid") from exc
    generation_delta = max(0, durable_generation - exported_generation)
    if force:
        reason = "forced"
    elif exported_generation == 0:
        reason = "initial_generation"
    elif generation_delta >= generation_threshold:
        reason = "generation_threshold"
    elif dirty_age >= maximum_dirty_age:
        reason = "maximum_dirty_age"
    else:
        return {
            "status": "deferred",
            "durable_generation": durable_generation,
            "exported_generation": exported_generation,
            "generation_delta": generation_delta,
            "dirty_views": len(dirty),
            "dirty_view_names": [str(row["view_name"]) for row in dirty],
            "dirty_age_seconds": round(dirty_age, 3),
            "generation_threshold": generation_threshold,
            "maximum_dirty_age_seconds": maximum_dirty_age,
        }
    result = materialize_exports(user_dir, db_path)
    return {
        **result,
        "durable_generation": durable_generation,
        "exported_generation": durable_generation,
        "policy_reason": reason,
    }
