#!/usr/bin/env python3
"""Durable, ancestor-only reply context for the conservative X archive."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import random
import re
import secrets
import shutil
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

import archive_x
import archive_x_descriptors as descriptor_x
import archive_x_pacing as pacing_x


BASE_SCHEMA_VERSION = 2
SCHEMA_VERSION = 3
V3_LOCAL_ADDENDUM_VERSION = 3009
MIN_CONTEXT_MEDIA_FREE_BYTES = 5 * 1024 * 1024 * 1024
VALID_STATES = (
    "pending",
    "leased",
    "retryable",
    "captured",
    "unavailable",
    "manual_review",
)
RATE_RESET_RE = re.compile(
    r"Archive rate-limit reset=(\d+) remaining=([^\s]+)"
)
TERMINAL_PATTERNS = {
    "deleted": ("Tweet unavailable ('Deleted')", "Tweet unavailable ('NotFound')"),
    "private": (
        "Tweet unavailable ('Protected')",
        "Tweets are protected",
        "AuthRequired: Protected Tweet",
    ),
    "suspended": (
        "User has been suspended",
        "Account suspended",
        "Tweet unavailable ('Suspended')",
    ),
    "withheld": ("Tweet unavailable ('Withheld')", "withheld in your country"),
}
AUTH_PATTERNS = (
    "Could not authenticate you",
    "Login Required",
    "Account temporarily locked",
    "AuthorizationError",
    "PacingAuthenticationError",
    "durable authentication evidence blocks X requests",
)
TRANSIENT_PATTERNS = (
    "429",
    "Rate limit",
    "Dependency: Unspecified",
    "Internal Server Error",
    "timed out",
    "Timeout",
    "RemoteDisconnected",
    "Connection aborted",
    "Unable to retrieve",
)
AMBIGUOUS_RESPONSE_PATTERNS = (
    "KeyError - 'result'",
    'KeyError - "result"',
)
SENSITIVE_LOG_RE = re.compile(
    r"(?i)(authorization|proxy-authorization|cookie:|set-cookie:|auth_token|ct0)"
)
METADATA_CLAIM_SQL = """SELECT t.* FROM targets t
   WHERE t.state IN ('pending','retryable')
     AND t.next_attempt_at <= ?
   ORDER BY t.parent_demand DESC,t.depth_min ASC,t.post_id DESC
   LIMIT 1"""
MEDIA_CLAIM_SQL = """SELECT * FROM targets
   WHERE state='captured'
     AND media_state IN ('pending','retryable')
     AND media_next_attempt_at <= ?
   ORDER BY depth_min ASC,post_id DESC LIMIT 1"""
METADATA_RECLAIM_SQL = """UPDATE targets SET state='retryable',
   lease_started_at=NULL,lease_token=NULL,next_attempt_at=?,
   last_error_class='stale_lease',updated_at=?
   WHERE state='leased' AND lease_started_at < ?"""
MEDIA_RECLAIM_SQL = """UPDATE targets SET media_state='retryable',
   media_lease_started_at=NULL,media_lease_token=NULL,media_next_attempt_at=?,
   last_error_class='stale_media_lease',updated_at=?
   WHERE media_state='leased' AND media_lease_started_at < ?"""
ASSET_CLAIM_SQL = """SELECT a.*,d.source_operation,d.media_type,d.extension,
   d.private_url,d.url_sha256,d.url_host,d.descriptor_sha256,d.filename,
   d.relative_directory,d.width,d.height,d.duration_seconds,d.bitrate,
   d.alt_text,d.variant_json,d.posted_at,d.original_posted_at,d.author_id,
   d.author_handle,d.conversation_id,d.reply_id,d.retweet_id,d.captured_at
  FROM asset_jobs a
  JOIN descriptor_generations d ON d.descriptor_id=a.descriptor_id
 WHERE a.state IN ('pending','retryable') AND a.next_attempt_at <= ?
   AND d.state='active'
 ORDER BY a.transfer_priority,a.next_attempt_at,a.asset_id LIMIT 1"""
REFRESH_CLAIM_SQL = """SELECT * FROM descriptor_refresh_jobs
 WHERE owner_kind='post' AND state IN ('pending','retryable')
   AND next_attempt_at <= ?
 ORDER BY next_attempt_at,refresh_id LIMIT 1"""

V3_REQUIRED_TABLES = {
    "archive_account",
    "archive_generation",
    "archive_media",
    "archive_posts",
    "archive_sources",
    "asset_jobs",
    "conversation_rollups",
    "current_pointers",
    "descriptor_generations",
    "descriptor_observations",
    "descriptor_refresh_jobs",
    "export_batches",
    "export_views",
    "legacy_intervals",
    "post_provenance",
    "progress_counters",
    "request_aggregates",
    "run_registry",
    "schema_migrations",
}
V3_REQUIRED_TARGET_COLUMNS = {
    "parent_demand",
    "lease_token",
    "media_lease_started_at",
    "media_lease_token",
}
V3_REQUIRED_PACING_COLUMNS = {
    "reservation_token",
    "reservation_started_at",
    "auth_stop_class",
    "auth_stop_at",
    "updated_at",
    "not_before_reason",
    "request_sequence",
    "reservation_recoveries",
    "last_request_operation",
    "last_request_category",
}
V3_REQUIRED_DESCRIPTOR_COLUMNS = {
    "posted_at",
    "original_posted_at",
    "author_id",
    "author_handle",
    "conversation_id",
    "reply_id",
    "retweet_id",
}
V3_REQUIRED_ASSET_COLUMNS = {"transfer_priority", "destination_scope"}
V3_REQUIRED_INDEXES = {
    "archive_media_export",
    "archive_posts_export",
    "targets_metadata_priority",
    "targets_media_priority",
    "targets_metadata_lease_expiry",
    "targets_media_lease_expiry",
    "descriptor_active_owner",
    "descriptor_observations_source",
    "asset_jobs_ready",
    "asset_jobs_lease_expiry",
    "refresh_jobs_ready",
    "refresh_jobs_lease_expiry",
    "legacy_intervals_bounds",
    "reply_edges_chain",
}
V3_REQUIRED_TRIGGERS = {
    "archive_media_counter_insert",
    "archive_posts_counter_insert",
    "conversation_counter_insert",
    "conversation_counter_update",
    "targets_metadata_lease_insert",
    "targets_metadata_lease_update",
    "targets_media_lease_insert",
    "targets_media_lease_update",
    "pacing_request_lease_insert",
    "pacing_request_lease_update",
    "captured_requires_observation_insert",
    "captured_requires_observation",
    "preserve_captured_observation",
    "reply_edges_parent_demand_insert",
    "reply_edges_parent_demand_delete",
    "reply_edges_parent_demand_update",
    "reply_edges_rollup_delete",
    "reply_edges_rollup_insert",
    "reply_edges_rollup_update",
    "reply_edges_cycle_counter_insert",
    "reply_edges_cycle_counter_delete",
    "reply_edges_cycle_counter_update",
    "targets_state_rollup_update",
    "targets_unavailable_counter_update",
    "descriptor_terminal_state",
    "asset_active_descriptor_insert",
    "asset_active_descriptor_update",
    "asset_jobs_counter_insert",
    "asset_jobs_counter_delete",
    "asset_jobs_state_counter_update",
    "asset_terminal_transition",
    "export_generation_monotonic",
}


class ContextError(archive_x.ArchiveError):
    """A fail-closed context archive error."""


class ContextAuthenticationError(ContextError):
    """A credential/account failure that must stop all network workers."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def interrupt_handler(_signum: int, _frame: Any) -> None:
    raise KeyboardInterrupt


def id_string(value: Any) -> str | None:
    value = archive_x.id_string(value)
    if not value or not value.isdigit() or int(value) < 1:
        return None
    return value


def asset_transfer_priority(owner_kind: str, media_type: str) -> int:
    if owner_kind in {"profile_avatar", "profile_background"}:
        return 0
    return {
        "photo": 10,
        "preview": 20,
        "card": 20,
        "article": 20,
        "animated_gif": 30,
        "video": 40,
        "unknown": 50,
    }.get(media_type, 50)


def descriptor_destination_scope(row: dict[str, Any]) -> str:
    if row["owner_kind"] in {"profile_avatar", "profile_background"}:
        return "profile"
    parts = Path(str(row.get("relative_path") or "")).parts
    try:
        media_index = parts.index("media")
    except ValueError:
        return "unknown"
    return "context" if "context" in parts[media_index + 1 :] else "main"


def batch_destination_scope(
    batches: Iterable[descriptor_x.DescriptorBatch],
) -> str:
    operations = {batch.source_operation for batch in batches}
    if operations and operations <= {"context", "exact_refresh"}:
        return "context"
    if operations & {"modern", "legacy", "retry"}:
        return "main"
    return "unknown"


def _prepare_portable_asset_record(
    portable_record: dict[str, Any] | None,
    *,
    final_relative_path: str,
    sha256: str,
    byte_count: int,
    stat_result: os.stat_result,
) -> dict[str, Any] | None:
    """Validate already-read portable evidence before a SQLite transaction."""
    if portable_record is None:
        return None
    if not isinstance(portable_record, dict):
        raise ContextError("portable asset record is invalid")
    archive_path = Path(final_relative_path)
    media_path = Path(str(portable_record.get("asset_path") or ""))
    sidecar_path = Path(str(portable_record.get("sidecar_path") or ""))
    if (
        len(archive_path.parts) < 4
        or archive_path.parts[0] != "users"
        or archive_path.parts[2] != "media"
        or archive_path.is_absolute()
        or ".." in archive_path.parts
        or not media_path.parts
        or media_path.parts[0] != "media"
        or media_path.is_absolute()
        or ".." in media_path.parts
        or not sidecar_path.parts
        or sidecar_path.parts[0] != "media"
        or sidecar_path.is_absolute()
        or ".." in sidecar_path.parts
        or media_path.as_posix()
        != Path(*archive_path.parts[2:]).as_posix()
        or sidecar_path.as_posix() != media_path.as_posix() + ".json"
        or portable_record.get("sha256") != sha256
        or portable_record.get("bytes") != byte_count
    ):
        raise ContextError("portable asset path or file evidence changed")
    ordinal = portable_record.get("media_number")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 1:
        raise ContextError("portable asset ordinal is invalid")
    relationship = str(portable_record.get("relationship") or "")
    if relationship in {"profile_avatar", "profile_background"}:
        owner_kind = relationship
        owner_id = "account"
    else:
        owner_kind = "post"
        owner_id = str(portable_record.get("post_id") or "")
        if not owner_id.isdecimal() or int(owner_id) < 1:
            raise ContextError("portable asset owner identity is invalid")
    raw = portable_record.get("gallery_dl")
    if (
        not isinstance(raw, dict)
        or raw.get("sha256") != sha256
        or raw.get("bytes") not in (None, byte_count)
        or archive_x.id_string(raw.get("num")) != str(ordinal)
        or stat_result.st_size != byte_count
    ):
        raise ContextError("portable asset sidecar evidence changed")
    normalized_json = json.dumps(
        portable_record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "media_path": media_path.as_posix(),
        "sidecar_path": sidecar_path.as_posix(),
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "media_ordinal": ordinal,
        "normalized_json": normalized_json,
        "normalized_sha256": hashlib.sha256(
            normalized_json.encode("utf-8")
        ).hexdigest(),
        "final_sha256": sha256,
        "final_bytes": byte_count,
        "stat_device": int(stat_result.st_dev),
        "stat_inode": int(stat_result.st_ino),
        "stat_size": int(stat_result.st_size),
        "stat_mtime_ns": int(stat_result.st_mtime_ns),
    }


def _upsert_portable_asset(
    connection: sqlite3.Connection,
    *,
    prepared: dict[str, Any],
    asset_id: int,
    generation: int,
    captured_at: str,
) -> None:
    cursor = connection.execute(
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
               asset_id=excluded.asset_id,
               normalized_json=excluded.normalized_json,
               normalized_sha256=excluded.normalized_sha256,
               final_sha256=excluded.final_sha256,
               final_bytes=excluded.final_bytes,
               stat_device=excluded.stat_device,stat_inode=excluded.stat_inode,
               stat_size=excluded.stat_size,stat_mtime_ns=excluded.stat_mtime_ns,
               durable_generation=excluded.durable_generation,
               captured_at=excluded.captured_at
           WHERE archive_media.asset_id IS NULL
              OR archive_media.asset_id=excluded.asset_id""",
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
    if cursor.rowcount != 1:
        raise ContextError("portable asset identity conflicts with indexed media")


def positive_float(value: str) -> float:
    number = archive_x.nonnegative_float(value)
    if number == 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def safe_detail(value: str, limit: int = 2000) -> str:
    value = "\n".join(
        "[redacted sensitive log line]" if SENSITIVE_LOG_RE.search(line) else line
        for line in value.replace("\x00", "").splitlines()
    )
    return value[-limit:]


def existing_schema_version(path: Path) -> int:
    if not path.is_file():
        return 0
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = connection.execute(
                "SELECT value FROM context_meta WHERE key='schema_version'"
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error:
        return 0
    try:
        return int(row[0]) if row else 0
    except (TypeError, ValueError):
        return 0


def readonly_context_summary(path: Path) -> dict[str, Any]:
    """Inspect queue truth without creating, migrating, or journaling a DB."""
    if not path.is_file():
        return {"status": "absent"}
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only=ON")
            version_row = connection.execute(
                "SELECT value FROM context_meta WHERE key='schema_version'"
            ).fetchone()
            version = int(version_row[0]) if version_row else 0
            counter_table = connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='progress_counters'"
            ).fetchone()
            counters = (
                {
                    str(row[0]): int(row[1])
                    for row in connection.execute(
                        "SELECT counter_name,value FROM progress_counters"
                    )
                }
                if version >= 3 and counter_table is not None
                else None
            )
            if counters is not None:
                states = {
                    state: counters.get(f"targets_state_{state}", 0)
                    for state in VALID_STATES
                }
                media = {
                    state: counters.get(f"targets_media_{state}", 0)
                    for state in (
                        "none",
                        "pending",
                        "leased",
                        "captured",
                        "retryable",
                        "unavailable",
                        "manual_review",
                    )
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
                edges = counters.get("reply_edges_total", 0)
                integrity_ok: bool | None = None
            else:
                states = {
                    row[0]: row[1]
                    for row in connection.execute(
                        "SELECT state,COUNT(*) FROM targets GROUP BY state"
                    )
                }
                media = {
                    row[0]: row[1]
                    for row in connection.execute(
                        "SELECT media_state,COUNT(*) FROM targets GROUP BY media_state"
                    )
                }
                edges = int(
                    connection.execute("SELECT COUNT(*) FROM reply_edges").fetchone()[0]
                )
                quick = connection.execute("PRAGMA quick_check").fetchone()
                foreign = connection.execute("PRAGMA foreign_key_check").fetchone()
                integrity_ok = bool(quick and quick[0] == "ok" and foreign is None)
        finally:
            connection.close()
    except (sqlite3.Error, TypeError, ValueError) as exc:
        raise ContextError(f"cannot inspect context database read-only: {exc}") from exc
    pending = sum(
        int(states.get(name, 0)) for name in ("pending", "retryable", "leased")
    )
    manual = int(states.get("manual_review", 0))
    media_pending = sum(
        int(media.get(name, 0))
        for name in (
            "pending", "retryable", "leased", "needs_refresh",
            "manual_review",
        )
    )
    return {
        "status": "present",
        "schema_version": version,
        "targets": sum(states.values()),
        "edges": edges,
        "metadata_pending": pending,
        "manual_review": manual,
        "media_pending": media_pending,
        "integrity_ok": integrity_ok,
        "integrity_checked": integrity_ok is not None,
    }


def backup_context_before_v2(path: Path) -> Path:
    digest = archive_x.sha256_file(path)
    backup = path.parent / "backups" / f"context.pre-v2-{digest[:12]}.sqlite3"
    backup.parent.mkdir(parents=True, exist_ok=True)
    if backup.exists():
        if archive_x.sha256_file(backup) != digest:
            raise ContextError("context migration backup exists with changed bytes")
        return backup
    temporary = backup.with_name(f".{backup.name}.tmp-{os.getpid()}")
    try:
        shutil.copyfile(path, temporary)
        os.chmod(temporary, 0o600)
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        if archive_x.sha256_file(temporary) != digest:
            raise ContextError("context migration backup verification failed")
        os.replace(temporary, backup)
        directory_fd = os.open(backup.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    return backup


@contextlib.contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


@contextlib.contextmanager
def savepoint(
    connection: sqlite3.Connection, name: str
) -> Iterator[sqlite3.Connection]:
    connection.execute(f"SAVEPOINT {name}")
    try:
        yield connection
    except BaseException:
        connection.execute(f"ROLLBACK TO {name}")
        connection.execute(f"RELEASE {name}")
        raise
    else:
        connection.execute(f"RELEASE {name}")


class ContextDB:
    """Single-writer context graph, observations, queue, and pacing state."""

    def __init__(self, path: Path, *, create: bool = True):
        self.path = path
        if not create and not path.is_file():
            raise ContextError(f"context database does not exist: {path}")
        if create:
            path.parent.mkdir(parents=True, exist_ok=True)
        self.migration_backup: Path | None = None
        if existing_schema_version(path) == 1:
            self.migration_backup = backup_context_before_v2(path)
        self.connection = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=DELETE")
        self.connection.execute("PRAGMA synchronous=FULL")
        if os.name == "posix":
            os.chmod(path, 0o600)
        try:
            self._ensure_schema(create=create)
        except BaseException:
            self.connection.close()
            raise

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "ContextDB":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def _ensure_schema(self, *, create: bool) -> None:
        tables = {
            row[0]
            for row in self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not tables:
            if not create:
                raise ContextError(f"context database is empty: {self.path}")
            self._create_schema()
            self._validate_v3_schema()
            return
        if "context_meta" not in tables:
            raise ContextError(
                f"refusing unrecognized SQLite schema at {self.path}"
            )
        row = self.connection.execute(
            "SELECT value FROM context_meta WHERE key='schema_version'"
        ).fetchone()
        try:
            version = int(row[0]) if row else 0
        except (TypeError, ValueError):
            version = 0
        if version == 1:
            self._migrate_v1_to_v2()
            version = BASE_SCHEMA_VERSION
        if version == BASE_SCHEMA_VERSION:
            self._migrate_v2_to_v3()
            version = SCHEMA_VERSION
        if version != SCHEMA_VERSION:
            raise ContextError(
                f"unsupported context schema {version}; expected {SCHEMA_VERSION}"
            )
        self._migrate_v3_local_addendum()
        self._validate_v3_schema()

    def _validate_v3_schema(self) -> None:
        catalog = list(
            self.connection.execute(
                "SELECT type,name FROM sqlite_master "
                "WHERE type IN ('table','index','trigger')"
            )
        )
        names = {
            kind: {str(row[1]) for row in catalog if row[0] == kind}
            for kind in ("table", "index", "trigger")
        }
        missing: list[str] = []
        for kind, required in (
            ("table", V3_REQUIRED_TABLES),
            ("index", V3_REQUIRED_INDEXES),
            ("trigger", V3_REQUIRED_TRIGGERS),
        ):
            missing.extend(
                f"{kind}:{name}" for name in sorted(required - names[kind])
            )
        missing.extend(
            f"targets.column:{name}"
            for name in sorted(
                V3_REQUIRED_TARGET_COLUMNS - self._table_columns("targets")
            )
        )
        missing.extend(
            f"pacing.column:{name}"
            for name in sorted(
                V3_REQUIRED_PACING_COLUMNS - self._table_columns("pacing")
            )
        )
        missing.extend(
            f"descriptor_generations.column:{name}"
            for name in sorted(
                V3_REQUIRED_DESCRIPTOR_COLUMNS
                - self._table_columns("descriptor_generations")
            )
        )
        missing.extend(
            f"asset_jobs.column:{name}"
            for name in sorted(
                V3_REQUIRED_ASSET_COLUMNS - self._table_columns("asset_jobs")
            )
        )
        migration = self.connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
        ).fetchone() if "schema_migrations" in names["table"] else None
        if migration is None:
            missing.append(f"migration:{SCHEMA_VERSION}")
        addendum = self.connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version=?",
            (V3_LOCAL_ADDENDUM_VERSION,),
        ).fetchone() if "schema_migrations" in names["table"] else None
        if addendum is None:
            missing.append(f"migration:{V3_LOCAL_ADDENDUM_VERSION}")
        if missing:
            raise ContextError(
                "context schema v3 is incomplete: " + ", ".join(missing)
            )

    def _migrate_v1_to_v2(self) -> None:
        try:
            self.connection.executescript(
                """BEGIN IMMEDIATE;
                CREATE TABLE seed_sources (
                    relative_path TEXT PRIMARY KEY,
                    sha256 TEXT NOT NULL
                        CHECK(length(sha256)=64
                              AND sha256 NOT GLOB '*[^0-9a-f]*'),
                    source_kind TEXT NOT NULL
                        CHECK(source_kind IN ('modern','legacy')),
                    run_id TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    record_count INTEGER NOT NULL CHECK(record_count >= 0),
                    edge_count INTEGER NOT NULL CHECK(edge_count >= 0)
                );
                CREATE TABLE local_posts (
                    post_id TEXT PRIMARY KEY
                        CHECK(post_id <> '' AND post_id NOT GLOB '*[^0-9]*'),
                    raw_json TEXT NOT NULL,
                    sha256 TEXT NOT NULL
                        CHECK(length(sha256)=64
                              AND sha256 NOT GLOB '*[^0-9a-f]*'),
                    relative_path TEXT NOT NULL,
                    source_kind TEXT NOT NULL
                        CHECK(source_kind IN ('modern','legacy')),
                    run_id TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE INDEX local_posts_source ON local_posts(relative_path);
                UPDATE context_meta SET value='2' WHERE key='schema_version';
                COMMIT;"""
            )
        except BaseException:
            self.connection.rollback()
            raise

    def _table_columns(self, table: str) -> set[str]:
        return {
            str(row[1])
            for row in self.connection.execute(f"PRAGMA table_info({table})")
        }

    def _add_column(self, table: str, definition: str) -> None:
        name = definition.partition(" ")[0]
        if name not in self._table_columns(table):
            self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")

    def _create_v3_objects(self) -> None:
        self._add_column(
            "targets", "parent_demand INTEGER NOT NULL DEFAULT 0 CHECK(parent_demand >= 0)"
        )
        self._add_column("targets", "lease_token TEXT")
        self._add_column("targets", "media_lease_started_at REAL")
        self._add_column("targets", "media_lease_token TEXT")
        self._add_column("pacing", "reservation_token TEXT")
        self._add_column("pacing", "reservation_started_at REAL")
        self._add_column("pacing", "auth_stop_class TEXT")
        self._add_column("pacing", "auth_stop_at REAL")
        self._add_column("pacing", "updated_at TEXT")
        self._add_column(
            "pacing",
            "not_before_reason TEXT NOT NULL DEFAULT 'initial' "
            "CHECK(not_before_reason IN "
            "('initial','spacing','rate_limit','http_429'))",
        )
        self._add_column(
            "pacing",
            "request_sequence INTEGER NOT NULL DEFAULT 0 "
            "CHECK(request_sequence >= 0)",
        )
        self._add_column(
            "pacing",
            "reservation_recoveries INTEGER NOT NULL DEFAULT 0 "
            "CHECK(reservation_recoveries >= 0)",
        )
        self._add_column(
            "pacing",
            "last_request_operation TEXT "
            "CHECK(last_request_operation IS NULL OR "
            "(last_request_operation <> '' AND "
            "last_request_operation NOT GLOB '*[^a-z0-9_]*'))",
        )
        self._add_column(
            "pacing",
            "last_request_category TEXT "
            "CHECK(last_request_category IS NULL OR "
            "last_request_category IN ('x_api','x_support','x_redirect'))",
        )

        statements = (
            """CREATE TABLE IF NOT EXISTS schema_migrations (
                   version INTEGER PRIMARY KEY CHECK(version >= 1),
                   applied_at TEXT NOT NULL,
                   description TEXT NOT NULL
               )""",
            """CREATE TABLE IF NOT EXISTS archive_account (
                   singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                   user_id TEXT NOT NULL
                       CHECK(user_id <> '' AND user_id NOT GLOB '*[^0-9]*'),
                   requested_handle TEXT NOT NULL CHECK(requested_handle <> ''),
                   canonical_handle TEXT NOT NULL CHECK(canonical_handle <> ''),
                   bound_at TEXT NOT NULL,
                   updated_at TEXT NOT NULL
               )""",
            """CREATE TABLE IF NOT EXISTS archive_sources (
                   source_id INTEGER PRIMARY KEY,
                   relative_path TEXT NOT NULL UNIQUE
                       CHECK(relative_path <> '' AND substr(relative_path,1,1) <> '/'),
                   source_kind TEXT NOT NULL CHECK(source_kind IN
                       ('modern','legacy','context','info','retry','profile','migration')),
                   run_id TEXT NOT NULL CHECK(run_id <> ''),
                   operation_id TEXT NOT NULL DEFAULT '',
                   expected_sha256 TEXT CHECK(expected_sha256 IS NULL OR
                       (length(expected_sha256)=64
                        AND expected_sha256 NOT GLOB '*[^0-9a-f]*')),
                   stat_device INTEGER,
                   stat_inode INTEGER,
                   stat_size INTEGER CHECK(stat_size IS NULL OR stat_size >= 0),
                   stat_mtime_ns INTEGER,
                   status TEXT NOT NULL CHECK(status IN
                       ('registered','ingesting','committed','changed','invalid')),
                   ingest_generation INTEGER NOT NULL DEFAULT 0
                       CHECK(ingest_generation >= 0),
                   registered_at TEXT NOT NULL,
                   processed_at TEXT,
                   record_count INTEGER CHECK(record_count IS NULL OR record_count >= 0),
                   edge_count INTEGER CHECK(edge_count IS NULL OR edge_count >= 0),
                   UNIQUE(run_id,relative_path)
               )""",
            """CREATE TABLE IF NOT EXISTS archive_posts (
                   post_id TEXT PRIMARY KEY
                       CHECK(post_id <> '' AND post_id NOT GLOB '*[^0-9]*'),
                   requested_user_id TEXT NOT NULL
                       CHECK(requested_user_id <> ''
                             AND requested_user_id NOT GLOB '*[^0-9]*'),
                   author_id TEXT CHECK(author_id IS NULL OR
                       (author_id <> '' AND author_id NOT GLOB '*[^0-9]*')),
                   relationship TEXT NOT NULL CHECK(relationship IN
                       ('post','reply','repost','context')),
                   posted_at TEXT,
                   normalized_json TEXT NOT NULL CHECK(normalized_json <> ''),
                   normalized_sha256 TEXT NOT NULL
                       CHECK(length(normalized_sha256)=64
                             AND normalized_sha256 NOT GLOB '*[^0-9a-f]*'),
                   first_captured_at TEXT NOT NULL,
                   last_captured_at TEXT NOT NULL,
                   capture_count INTEGER NOT NULL DEFAULT 1 CHECK(capture_count >= 1),
                   durable_generation INTEGER NOT NULL DEFAULT 0
                       CHECK(durable_generation >= 0)
               )""",
            """CREATE TABLE IF NOT EXISTS archive_media (
                   media_path TEXT PRIMARY KEY CHECK(
                       media_path <> '' AND substr(media_path,1,1) <> '/'),
                   sidecar_path TEXT NOT NULL UNIQUE CHECK(
                       sidecar_path <> '' AND substr(sidecar_path,1,1) <> '/'),
                   owner_kind TEXT NOT NULL CHECK(owner_kind IN
                       ('post','profile_avatar','profile_background')),
                   owner_id TEXT NOT NULL CHECK(
                       (owner_kind='post' AND owner_id <> ''
                        AND owner_id NOT GLOB '*[^0-9]*') OR
                       (owner_kind IN ('profile_avatar','profile_background')
                        AND owner_id='account')),
                   media_ordinal INTEGER NOT NULL CHECK(media_ordinal >= 1),
                   asset_id INTEGER UNIQUE REFERENCES asset_jobs(asset_id)
                       ON DELETE RESTRICT,
                   normalized_json TEXT NOT NULL CHECK(normalized_json <> ''),
                   normalized_sha256 TEXT NOT NULL CHECK(
                       length(normalized_sha256)=64
                       AND normalized_sha256 NOT GLOB '*[^0-9a-f]*'),
                   final_sha256 TEXT NOT NULL CHECK(
                       length(final_sha256)=64
                       AND final_sha256 NOT GLOB '*[^0-9a-f]*'),
                   final_bytes INTEGER NOT NULL CHECK(final_bytes >= 1),
                   stat_device INTEGER,
                   stat_inode INTEGER,
                   stat_size INTEGER NOT NULL CHECK(stat_size >= 1),
                   stat_mtime_ns INTEGER,
                   durable_generation INTEGER NOT NULL CHECK(
                       durable_generation >= 1),
                   captured_at TEXT NOT NULL
               )""",
            """CREATE TABLE IF NOT EXISTS post_provenance (
                   post_id TEXT NOT NULL REFERENCES archive_posts(post_id)
                       ON DELETE RESTRICT,
                   source_id INTEGER NOT NULL REFERENCES archive_sources(source_id)
                       ON DELETE RESTRICT,
                   record_sha256 TEXT NOT NULL
                       CHECK(length(record_sha256)=64
                             AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
                   source_endpoint TEXT NOT NULL CHECK(source_endpoint <> ''),
                   observed_at TEXT NOT NULL,
                   PRIMARY KEY(post_id,source_id)
               )""",
            """CREATE TABLE IF NOT EXISTS legacy_intervals (
                   interval_id TEXT PRIMARY KEY CHECK(
                       interval_id <> '' AND length(interval_id) <= 80),
                   root_window_id TEXT NOT NULL CHECK(
                       root_window_id <> '' AND length(root_window_id) <= 80),
                   since_at TEXT NOT NULL,
                   until_at TEXT NOT NULL,
                   since_epoch INTEGER NOT NULL,
                   until_epoch INTEGER NOT NULL CHECK(until_epoch > since_epoch),
                   canonical_source_id INTEGER NOT NULL
                       REFERENCES archive_sources(source_id) ON DELETE RESTRICT,
                   canonical_sha256 TEXT NOT NULL CHECK(
                       length(canonical_sha256)=64
                       AND canonical_sha256 NOT GLOB '*[^0-9a-f]*'),
                   canonical_post_count INTEGER NOT NULL
                       CHECK(canonical_post_count >= 0),
                   evidence_sha256 TEXT NOT NULL CHECK(
                       length(evidence_sha256)=64
                       AND evidence_sha256 NOT GLOB '*[^0-9a-f]*'),
                   observation_count INTEGER NOT NULL
                       CHECK(observation_count >= 2),
                   durable_generation INTEGER NOT NULL
                       CHECK(durable_generation >= 1),
                   committed_at TEXT NOT NULL,
                   UNIQUE(since_epoch,until_epoch)
               )""",
            """CREATE TABLE IF NOT EXISTS conversation_rollups (
                   chain_id TEXT PRIMARY KEY CHECK(chain_id <> ''),
                   state TEXT NOT NULL CHECK(state IN
                       ('fully_captured','unavailable_boundary',
                        'retry_delayed','pending','manual_review')),
                   captured_count INTEGER NOT NULL DEFAULT 0
                       CHECK(captured_count >= 0),
                   unavailable_count INTEGER NOT NULL DEFAULT 0
                       CHECK(unavailable_count >= 0),
                   retryable_count INTEGER NOT NULL DEFAULT 0
                       CHECK(retryable_count >= 0),
                   pending_count INTEGER NOT NULL DEFAULT 0
                       CHECK(pending_count >= 0),
                   manual_count INTEGER NOT NULL DEFAULT 0
                       CHECK(manual_count >= 0),
                   edge_count INTEGER NOT NULL CHECK(edge_count >= 0),
                   updated_at TEXT NOT NULL,
                   CHECK(captured_count+unavailable_count+retryable_count+
                         pending_count+manual_count=edge_count)
               )""",
            """CREATE TABLE IF NOT EXISTS descriptor_generations (
                   descriptor_id INTEGER PRIMARY KEY,
                   owner_kind TEXT NOT NULL CHECK(owner_kind IN
                       ('post','profile_avatar','profile_background')),
                   owner_id TEXT NOT NULL CHECK(
                       (owner_kind='post' AND owner_id <> ''
                        AND owner_id NOT GLOB '*[^0-9]*') OR
                       (owner_kind IN ('profile_avatar','profile_background')
                        AND owner_id='account')),
                   media_ordinal INTEGER NOT NULL CHECK(media_ordinal >= 1),
                   generation INTEGER NOT NULL CHECK(generation >= 1),
                   source_id INTEGER REFERENCES archive_sources(source_id)
                       ON DELETE RESTRICT,
                   source_operation TEXT NOT NULL CHECK(source_operation IN
                       ('modern','legacy','context','retry','info','exact_refresh')),
                   media_type TEXT NOT NULL CHECK(media_type IN
                       ('photo','video','animated_gif','preview','card','article','unknown')),
                   extension TEXT NOT NULL CHECK(
                       extension <> '' AND length(extension) <= 16
                       AND extension NOT GLOB '*[^A-Za-z0-9]*'),
                   private_url TEXT NOT NULL CHECK(private_url <> ''),
                   url_sha256 TEXT NOT NULL
                       CHECK(length(url_sha256)=64
                             AND url_sha256 NOT GLOB '*[^0-9a-f]*'),
                   url_host TEXT NOT NULL CHECK(url_host IN
                       ('pbs.twimg.com','video.twimg.com','ton.twimg.com')),
                   descriptor_sha256 TEXT NOT NULL
                       CHECK(length(descriptor_sha256)=64
                             AND descriptor_sha256 NOT GLOB '*[^0-9a-f]*'),
                   filename TEXT NOT NULL CHECK(filename <> ''),
                   relative_directory TEXT NOT NULL,
                   width INTEGER CHECK(width IS NULL OR width >= 0),
                   height INTEGER CHECK(height IS NULL OR height >= 0),
                   duration_seconds REAL CHECK(
                       duration_seconds IS NULL OR duration_seconds >= 0),
                   bitrate INTEGER CHECK(bitrate IS NULL OR bitrate >= 0),
                   alt_text TEXT,
                   variant_json TEXT,
                   posted_at TEXT,
                   original_posted_at TEXT,
                   author_id TEXT CHECK(author_id IS NULL OR
                       (author_id <> '' AND author_id NOT GLOB '*[^0-9]*')),
                   author_handle TEXT,
                   conversation_id TEXT CHECK(conversation_id IS NULL OR
                       (conversation_id <> ''
                        AND conversation_id NOT GLOB '*[^0-9]*')),
                   reply_id TEXT CHECK(reply_id IS NULL OR
                       (reply_id <> '' AND reply_id NOT GLOB '*[^0-9]*')),
                   retweet_id TEXT CHECK(retweet_id IS NULL OR
                       (retweet_id <> '' AND retweet_id NOT GLOB '*[^0-9]*')),
                   state TEXT NOT NULL CHECK(state IN
                       ('active','superseded','invalid')),
                   captured_at TEXT NOT NULL,
                   superseded_at TEXT,
                   UNIQUE(owner_kind,owner_id,media_ordinal,generation),
                   UNIQUE(descriptor_id,owner_kind,owner_id,media_ordinal)
               )""",
            """CREATE TABLE IF NOT EXISTS descriptor_observations (
                   descriptor_id INTEGER NOT NULL
                       REFERENCES descriptor_generations(descriptor_id)
                       ON DELETE RESTRICT,
                   operation_id TEXT NOT NULL
                       CHECK(operation_id <> '' AND length(operation_id) <= 256),
                   source_id INTEGER REFERENCES archive_sources(source_id)
                       ON DELETE RESTRICT,
                   artifact_sha256 TEXT NOT NULL
                       CHECK(length(artifact_sha256)=64
                             AND artifact_sha256 NOT GLOB '*[^0-9a-f]*'),
                   record_sha256 TEXT NOT NULL
                       CHECK(length(record_sha256)=64
                             AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
                   observed_at TEXT NOT NULL,
                   PRIMARY KEY(descriptor_id,operation_id)
               )""",
            """CREATE TABLE IF NOT EXISTS asset_jobs (
                   asset_id INTEGER PRIMARY KEY,
                   owner_kind TEXT NOT NULL CHECK(owner_kind IN
                       ('post','profile_avatar','profile_background')),
                   owner_id TEXT NOT NULL CHECK(
                       (owner_kind='post' AND owner_id <> ''
                        AND owner_id NOT GLOB '*[^0-9]*') OR
                       (owner_kind IN ('profile_avatar','profile_background')
                        AND owner_id='account')),
                   media_ordinal INTEGER NOT NULL CHECK(media_ordinal >= 1),
                   descriptor_id INTEGER,
                   state TEXT NOT NULL CHECK(state IN
                       ('pending','leased','captured','retryable','needs_refresh',
                        'unavailable','manual_review')),
                   compatibility_job INTEGER NOT NULL DEFAULT 0
                       CHECK(compatibility_job IN (0,1)),
                   destination_scope TEXT NOT NULL DEFAULT 'unknown'
                       CHECK(destination_scope IN
                           ('main','context','profile','unknown')),
                   transfer_priority INTEGER NOT NULL DEFAULT 100
                       CHECK(transfer_priority >= 0),
                   attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                   next_attempt_at REAL NOT NULL DEFAULT 0,
                   lease_token TEXT,
                   lease_started_at REAL,
                   expected_relative_path TEXT,
                   final_relative_path TEXT,
                   final_sha256 TEXT CHECK(final_sha256 IS NULL OR
                       (length(final_sha256)=64
                        AND final_sha256 NOT GLOB '*[^0-9a-f]*')),
                   final_bytes INTEGER CHECK(final_bytes IS NULL OR final_bytes >= 0),
                   verified_device INTEGER,
                   verified_inode INTEGER,
                   verified_size INTEGER CHECK(
                       verified_size IS NULL OR verified_size >= 0),
                   verified_mtime_ns INTEGER,
                   last_error_class TEXT,
                   last_error_detail TEXT,
                   created_at TEXT NOT NULL,
                   updated_at TEXT NOT NULL,
                   completed_at TEXT,
                   UNIQUE(owner_kind,owner_id,media_ordinal),
                   FOREIGN KEY(descriptor_id,owner_kind,owner_id,media_ordinal)
                       REFERENCES descriptor_generations(
                           descriptor_id,owner_kind,owner_id,media_ordinal)
                       ON DELETE RESTRICT,
                   CHECK(descriptor_id IS NOT NULL OR compatibility_job=1 OR
                         state IN ('needs_refresh','unavailable','manual_review')),
                   CHECK((state='leased' AND lease_token IS NOT NULL
                          AND lease_started_at IS NOT NULL) OR
                         (state<>'leased' AND lease_token IS NULL
                          AND lease_started_at IS NULL)),
                   CHECK(state<>'captured' OR
                         (final_relative_path IS NOT NULL
                          AND final_sha256 IS NOT NULL
                          AND final_bytes IS NOT NULL
                          AND completed_at IS NOT NULL))
               )""",
            """CREATE TABLE IF NOT EXISTS descriptor_refresh_jobs (
                   refresh_id INTEGER PRIMARY KEY,
                   owner_kind TEXT NOT NULL CHECK(owner_kind IN
                       ('post','profile_avatar','profile_background')),
                   owner_id TEXT NOT NULL CHECK(
                       (owner_kind='post' AND owner_id <> ''
                        AND owner_id NOT GLOB '*[^0-9]*') OR
                       (owner_kind IN ('profile_avatar','profile_background')
                        AND owner_id='account')),
                   generation INTEGER NOT NULL CHECK(generation >= 1),
                   state TEXT NOT NULL CHECK(state IN
                       ('pending','leased','complete','retryable',
                        'unavailable','manual_review')),
                   reason TEXT NOT NULL CHECK(reason IN
                       ('descriptor_missing','descriptor_stale','compatibility',
                        'operator_repair')),
                   attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                   next_attempt_at REAL NOT NULL DEFAULT 0,
                   lease_token TEXT,
                   lease_started_at REAL,
                   created_at TEXT NOT NULL,
                   updated_at TEXT NOT NULL,
                   completed_at TEXT,
                   last_error_class TEXT,
                   last_error_detail TEXT,
                   UNIQUE(owner_kind,owner_id,generation),
                   CHECK((state='leased' AND lease_token IS NOT NULL
                          AND lease_started_at IS NOT NULL) OR
                         (state<>'leased' AND lease_token IS NULL
                          AND lease_started_at IS NULL)),
                   CHECK(state<>'complete' OR completed_at IS NOT NULL)
               )""",
            """CREATE TABLE IF NOT EXISTS request_aggregates (
                   scope_id TEXT NOT NULL CHECK(scope_id <> ''),
                   operation TEXT NOT NULL CHECK(operation IN
                       ('info','timeline','retry_media','profile_avatar',
                        'profile_background','context_metadata','context_exact',
                        'context_media','direct_media','descriptor_refresh',
                        'legacy_walk')),
                   category TEXT NOT NULL CHECK(category IN
                       ('x_api','x_support','media_cdn','x_redirect','external')),
                   endpoint TEXT NOT NULL CHECK(endpoint IN
                       ('user_profile','tweet_result','tweet_detail','user_tweets',
                        'user_tweets_replies','search_timeline','x_api_other',
                        'media_asset','client_bootstrap','x_web','x_redirect',
                        'external_http')),
                   status_label TEXT NOT NULL CHECK(
                       status_label='error' OR
                       (status_label GLOB '[1-5][0-9][0-9]'
                        AND length(status_label)=3)),
                   request_count INTEGER NOT NULL CHECK(request_count >= 0),
                   failure_count INTEGER NOT NULL DEFAULT 0
                       CHECK(failure_count >= 0 AND failure_count <= request_count),
                   redirect_count INTEGER NOT NULL DEFAULT 0
                       CHECK(redirect_count >= 0 AND redirect_count <= request_count),
                   advertised_bytes INTEGER NOT NULL DEFAULT 0
                       CHECK(advertised_bytes >= 0),
                   first_request_at REAL,
                   last_request_at REAL,
                   runner_starts INTEGER NOT NULL DEFAULT 0
                       CHECK(runner_starts >= 0),
                   PRIMARY KEY(scope_id,operation,category,endpoint,status_label),
                   CHECK(first_request_at IS NULL OR last_request_at IS NULL
                         OR last_request_at >= first_request_at)
               )""",
            """CREATE TABLE IF NOT EXISTS archive_generation (
                   singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                   current_generation INTEGER NOT NULL DEFAULT 0
                       CHECK(current_generation >= 0),
                   updated_at TEXT
               )""",
            """CREATE TABLE IF NOT EXISTS export_views (
                   view_name TEXT PRIMARY KEY CHECK(view_name IN
                       ('posts','authored_posts','reposts','media',
                        'context_posts','reply_edges','context_status')),
                   durable_generation INTEGER NOT NULL DEFAULT 0
                       CHECK(durable_generation >= 0),
                   exported_generation INTEGER NOT NULL DEFAULT 0
                       CHECK(exported_generation >= 0
                             AND exported_generation <= durable_generation),
                   status TEXT NOT NULL CHECK(status IN
                       ('unknown','dirty','writing','current','failed')),
                   relative_path TEXT,
                   export_sha256 TEXT CHECK(export_sha256 IS NULL OR
                       (length(export_sha256)=64
                        AND export_sha256 NOT GLOB '*[^0-9a-f]*')),
                   export_bytes INTEGER CHECK(export_bytes IS NULL OR export_bytes >= 0),
                   row_count INTEGER CHECK(row_count IS NULL OR row_count >= 0),
                   updated_at TEXT NOT NULL,
                   CHECK(status<>'current' OR
                         (exported_generation=durable_generation
                          AND relative_path IS NOT NULL
                          AND export_sha256 IS NOT NULL
                          AND export_bytes IS NOT NULL))
               )""",
            """CREATE TABLE IF NOT EXISTS export_batches (
                   generation INTEGER PRIMARY KEY CHECK(generation >= 1),
                   state TEXT NOT NULL CHECK(state IN
                       ('preparing','published','failed')),
                   started_at TEXT NOT NULL,
                   completed_at TEXT,
                   manifest_sha256 TEXT CHECK(manifest_sha256 IS NULL OR
                       (length(manifest_sha256)=64
                        AND manifest_sha256 NOT GLOB '*[^0-9a-f]*')),
                   error_class TEXT,
                   CHECK(state<>'published' OR
                         (completed_at IS NOT NULL AND manifest_sha256 IS NOT NULL))
               )""",
            """CREATE TABLE IF NOT EXISTS progress_counters (
                   counter_name TEXT PRIMARY KEY CHECK(
                       counter_name <> ''
                       AND counter_name NOT GLOB '*[^a-z0-9_]*'),
                   value INTEGER NOT NULL CHECK(value >= 0),
                   generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
                   updated_at TEXT NOT NULL
               )""",
            """CREATE TABLE IF NOT EXISTS run_registry (
                   run_id TEXT PRIMARY KEY CHECK(run_id <> ''),
                   mode TEXT NOT NULL CHECK(mode IN
                       ('modern','legacy_backfill','context','migration')),
                   manifest_path TEXT NOT NULL UNIQUE CHECK(manifest_path <> ''),
                   manifest_sha256 TEXT CHECK(manifest_sha256 IS NULL OR
                       (length(manifest_sha256)=64
                        AND manifest_sha256 NOT GLOB '*[^0-9a-f]*')),
                   stat_size INTEGER CHECK(stat_size IS NULL OR stat_size >= 0),
                   stat_mtime_ns INTEGER,
                   status TEXT NOT NULL CHECK(status IN
                       ('running','success','partial','limited','failed',
                        'interrupted','stalled','manual_review','complete')),
                   processed_at TEXT,
                   updated_at TEXT NOT NULL
               )""",
            """CREATE TABLE IF NOT EXISTS current_pointers (
                   pointer_name TEXT PRIMARY KEY CHECK(
                       pointer_name <> ''
                       AND pointer_name NOT GLOB '*[^a-z0-9_]*'),
                   run_id TEXT REFERENCES run_registry(run_id) ON DELETE SET NULL,
                   relative_path TEXT,
                   generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
                   updated_at TEXT NOT NULL
               )""",
        )
        for statement in statements:
            self.connection.execute(statement)

        self.connection.execute(
            "INSERT OR IGNORE INTO archive_generation(singleton) VALUES (1)"
        )
        now = iso_now()
        for view in (
            "posts",
            "authored_posts",
            "reposts",
            "media",
            "context_posts",
            "reply_edges",
            "context_status",
        ):
            self.connection.execute(
                """INSERT OR IGNORE INTO export_views(
                       view_name,status,updated_at
                   ) VALUES (?,'unknown',?)""",
                (view, now),
            )

        self.connection.execute(
            """INSERT OR IGNORE INTO archive_account(
                   singleton,user_id,requested_handle,canonical_handle,
                   bound_at,updated_at
               )
               SELECT 1,user.value,handle.value,handle.value,?,?
                 FROM context_meta user,context_meta handle
                WHERE user.key='target_user_id'
                  AND handle.key='canonical_handle'""",
            (now, now),
        )
        self.connection.execute(
            """INSERT OR IGNORE INTO archive_sources(
                   relative_path,source_kind,run_id,operation_id,
                   expected_sha256,status,ingest_generation,
                   registered_at,processed_at,record_count,edge_count
               )
               SELECT relative_path,source_kind,run_id,'seed_context',sha256,
                      'committed',1,processed_at,processed_at,
                      record_count,edge_count
                 FROM seed_sources"""
        )
        self.connection.execute(
            """UPDATE targets SET parent_demand=(
                   SELECT COUNT(*) FROM reply_edges e
                    WHERE e.parent_id=targets.post_id
               )"""
        )
        self.connection.execute("DELETE FROM conversation_rollups")
        self.connection.execute(
            """INSERT INTO conversation_rollups(
                   chain_id,state,captured_count,unavailable_count,
                   retryable_count,pending_count,manual_count,edge_count,
                   updated_at
               )
               SELECT COALESCE(e.conversation_id,e.child_id),
                      CASE
                        WHEN SUM(t.state='manual_review')>0 THEN 'manual_review'
                        WHEN SUM(t.state IN ('pending','leased'))>0 THEN 'pending'
                        WHEN SUM(t.state='retryable')>0 THEN 'retry_delayed'
                        WHEN SUM(t.state='unavailable')>0
                          THEN 'unavailable_boundary'
                        ELSE 'fully_captured'
                      END,
                      SUM(t.state='captured'),SUM(t.state='unavailable'),
                      SUM(t.state='retryable'),
                      SUM(t.state IN ('pending','leased')),
                      SUM(t.state='manual_review'),COUNT(*),MAX(e.discovered_at)
                 FROM reply_edges e JOIN targets t ON t.post_id=e.parent_id
                GROUP BY COALESCE(e.conversation_id,e.child_id)"""
        )
        self.connection.execute(
            """UPDATE targets SET media_lease_started_at=lease_started_at
                 WHERE media_state='leased'
                   AND media_lease_started_at IS NULL"""
        )
        # Schema v2 used one timestamp for both independent queues and had no
        # ownership token.  Preserve every active lease in its proper lane,
        # give it a durable owner, and clear the obsolete cross-lane value.
        self.connection.execute(
            """UPDATE targets SET
                   lease_started_at=COALESCE(lease_started_at,0),
                   lease_token=COALESCE(
                       lease_token,'migration-'||lower(hex(randomblob(16)))
                   )
                 WHERE state='leased'"""
        )
        self.connection.execute(
            """UPDATE targets SET lease_started_at=NULL,lease_token=NULL
                 WHERE state<>'leased'"""
        )
        self.connection.execute(
            """UPDATE targets SET
                   media_lease_started_at=COALESCE(media_lease_started_at,0),
                   media_lease_token=COALESCE(
                       media_lease_token,'migration-'||lower(hex(randomblob(16)))
                   )
                 WHERE media_state='leased'"""
        )
        self.connection.execute(
            """UPDATE targets SET
                   media_lease_started_at=NULL,media_lease_token=NULL
                 WHERE media_state<>'leased'"""
        )

        states = (*VALID_STATES,)
        media_states = (
            "none",
            "pending",
            "leased",
            "captured",
            "retryable",
            "unavailable",
            "manual_review",
        )
        asset_states = (
            "pending",
            "leased",
            "captured",
            "retryable",
            "needs_refresh",
            "unavailable",
            "manual_review",
        )
        self.connection.execute(
            """INSERT INTO progress_counters(
                   counter_name,value,generation,updated_at
               ) VALUES ('targets_total',(SELECT COUNT(*) FROM targets),0,?)
               ON CONFLICT(counter_name) DO UPDATE SET
                   value=excluded.value,updated_at=excluded.updated_at""",
            (now,),
        )
        for state in states:
            self.connection.execute(
                """INSERT INTO progress_counters(
                       counter_name,value,generation,updated_at
                   ) VALUES (?,(SELECT COUNT(*) FROM targets WHERE state=?),0,?)
                   ON CONFLICT(counter_name) DO UPDATE SET
                       value=excluded.value,updated_at=excluded.updated_at""",
                (f"targets_state_{state}", state, now),
            )
        for state in media_states:
            self.connection.execute(
                """INSERT INTO progress_counters(
                       counter_name,value,generation,updated_at
                   ) VALUES (?,(SELECT COUNT(*) FROM targets WHERE media_state=?),0,?)
                   ON CONFLICT(counter_name) DO UPDATE SET
                       value=excluded.value,updated_at=excluded.updated_at""",
                (f"targets_media_{state}", state, now),
            )
        self.connection.execute(
            """INSERT INTO progress_counters(
                   counter_name,value,generation,updated_at
               ) VALUES ('asset_jobs_total',(SELECT COUNT(*) FROM asset_jobs),0,?)
               ON CONFLICT(counter_name) DO UPDATE SET
                   value=excluded.value,updated_at=excluded.updated_at""",
            (now,),
        )
        for state in asset_states:
            self.connection.execute(
                """INSERT INTO progress_counters(
                       counter_name,value,generation,updated_at
                   ) VALUES (?,(SELECT COUNT(*) FROM asset_jobs WHERE state=?),0,?)
                   ON CONFLICT(counter_name) DO UPDATE SET
                       value=excluded.value,updated_at=excluded.updated_at""",
                (f"asset_jobs_state_{state}", state, now),
            )
        for name, table in (
            ("reply_edges_total", "reply_edges"),
            ("observations_total", "observations"),
        ):
            self.connection.execute(
                f"""INSERT INTO progress_counters(
                        counter_name,value,generation,updated_at
                    ) VALUES (?,(SELECT COUNT(*) FROM {table}),0,?)
                    ON CONFLICT(counter_name) DO UPDATE SET
                        value=excluded.value,updated_at=excluded.updated_at""",
                (name, now),
            )
        scalar_counters = {
            "archive_posts_total": "SELECT COUNT(*) FROM archive_posts",
            "archive_media_files": "SELECT COUNT(*) FROM archive_media",
            "archive_media_bytes": (
                "SELECT COALESCE(SUM(final_bytes),0) FROM archive_media"
            ),
            "observations_focal": (
                "SELECT COUNT(*) FROM observations WHERE source_kind='x:focal'"
            ),
            "reply_edges_cycles": (
                "SELECT COUNT(*) FROM reply_edges WHERE cycle_detected=1"
            ),
            "targets_unavailable_private": (
                "SELECT COUNT(*) FROM targets WHERE state='unavailable' AND "
                "last_error_class IN ('private','protected','auth_required')"
            ),
            "targets_unavailable_deleted": (
                "SELECT COUNT(*) FROM targets WHERE state='unavailable' AND "
                "instr(lower(COALESCE(last_error_class,'')),'deleted')>0"
            ),
            "targets_unavailable_suspended": (
                "SELECT COUNT(*) FROM targets WHERE state='unavailable' AND "
                "instr(lower(COALESCE(last_error_class,'')),'suspend')>0"
            ),
            "targets_unavailable_other": (
                "SELECT COUNT(*) FROM targets WHERE state='unavailable' AND "
                "COALESCE(last_error_class,'') NOT IN "
                "('private','protected','auth_required') "
                "AND instr(lower(COALESCE(last_error_class,'')),'deleted')=0 "
                "AND instr(lower(COALESCE(last_error_class,'')),'suspend')=0"
            ),
        }
        for name, query in scalar_counters.items():
            self.connection.execute(
                f"""INSERT INTO progress_counters(
                        counter_name,value,generation,updated_at
                    ) VALUES (?,({query}),0,?)
                    ON CONFLICT(counter_name) DO UPDATE SET
                        value=excluded.value,updated_at=excluded.updated_at""",
                (name, now),
            )
        for state in (
            "fully_captured",
            "unavailable_boundary",
            "retry_delayed",
            "pending",
            "manual_review",
        ):
            self.connection.execute(
                """INSERT INTO progress_counters(
                       counter_name,value,generation,updated_at
                   ) VALUES (?,(SELECT COUNT(*) FROM conversation_rollups
                                 WHERE state=?),0,?)
                   ON CONFLICT(counter_name) DO UPDATE SET
                       value=excluded.value,updated_at=excluded.updated_at""",
                (f"conversations_state_{state}", state, now),
            )

        indexes = (
            """CREATE INDEX IF NOT EXISTS archive_posts_export
               ON archive_posts(posted_at,post_id)""",
            """CREATE INDEX IF NOT EXISTS archive_media_export
               ON archive_media(captured_at,media_path)""",
            """CREATE INDEX IF NOT EXISTS targets_metadata_priority
               ON targets(parent_demand DESC,depth_min ASC,post_id DESC,next_attempt_at)
               WHERE state IN ('pending','retryable')""",
            """CREATE INDEX IF NOT EXISTS targets_media_priority
               ON targets(depth_min ASC,post_id DESC,media_next_attempt_at)
               WHERE state='captured'
                 AND media_state IN ('pending','retryable')""",
            """CREATE INDEX IF NOT EXISTS targets_metadata_lease_expiry
               ON targets(lease_started_at)
               WHERE state='leased'""",
            """CREATE INDEX IF NOT EXISTS targets_media_lease_expiry
               ON targets(media_lease_started_at)
               WHERE media_state='leased'""",
            """CREATE INDEX IF NOT EXISTS archive_sources_status
               ON archive_sources(status,source_id)""",
            """CREATE INDEX IF NOT EXISTS legacy_intervals_bounds
               ON legacy_intervals(until_epoch DESC,since_epoch,interval_id)""",
            """CREATE INDEX IF NOT EXISTS reply_edges_chain
               ON reply_edges(COALESCE(conversation_id,child_id),parent_id)""",
            """CREATE INDEX IF NOT EXISTS post_provenance_source
               ON post_provenance(source_id,post_id)""",
            """CREATE INDEX IF NOT EXISTS descriptor_observations_source
               ON descriptor_observations(source_id,descriptor_id)""",
            """CREATE UNIQUE INDEX IF NOT EXISTS descriptor_active_owner
               ON descriptor_generations(owner_kind,owner_id,media_ordinal)
               WHERE state='active'""",
            """CREATE INDEX IF NOT EXISTS asset_jobs_ready
               ON asset_jobs(transfer_priority,next_attempt_at,asset_id)
               WHERE state IN ('pending','retryable')""",
            """CREATE INDEX IF NOT EXISTS asset_jobs_lease_expiry
               ON asset_jobs(lease_started_at)
               WHERE state='leased'""",
            """CREATE INDEX IF NOT EXISTS refresh_jobs_ready
               ON descriptor_refresh_jobs(owner_kind,next_attempt_at,refresh_id)
               WHERE state IN ('pending','retryable')""",
            """CREATE INDEX IF NOT EXISTS refresh_jobs_lease_expiry
               ON descriptor_refresh_jobs(lease_started_at)
               WHERE state='leased'""",
            """CREATE INDEX IF NOT EXISTS request_aggregates_scope
               ON request_aggregates(scope_id,operation)""",
            """CREATE INDEX IF NOT EXISTS run_registry_status
               ON run_registry(status,updated_at)""",
        )
        for statement in indexes:
            self.connection.execute(statement)

        triggers = (
            """CREATE TRIGGER IF NOT EXISTS targets_metadata_lease_insert
               BEFORE INSERT ON targets
               WHEN (NEW.state='leased' AND
                     (NEW.lease_token IS NULL OR NEW.lease_started_at IS NULL))
                    OR (NEW.state<>'leased' AND
                        (NEW.lease_token IS NOT NULL OR
                         NEW.lease_started_at IS NOT NULL))
               BEGIN SELECT RAISE(ABORT,'invalid metadata lease'); END""",
            """CREATE TRIGGER IF NOT EXISTS targets_metadata_lease_update
               BEFORE UPDATE OF state,lease_token,lease_started_at ON targets
               WHEN (NEW.state='leased' AND
                     (NEW.lease_token IS NULL OR NEW.lease_started_at IS NULL))
                    OR (NEW.state<>'leased' AND
                        (NEW.lease_token IS NOT NULL OR
                         NEW.lease_started_at IS NOT NULL))
               BEGIN SELECT RAISE(ABORT,'invalid metadata lease'); END""",
            """CREATE TRIGGER IF NOT EXISTS targets_media_lease_insert
               BEFORE INSERT ON targets
               WHEN (NEW.media_state='leased' AND
                     (NEW.media_lease_token IS NULL OR
                      NEW.media_lease_started_at IS NULL))
                    OR (NEW.media_state<>'leased' AND
                        (NEW.media_lease_token IS NOT NULL OR
                         NEW.media_lease_started_at IS NOT NULL))
               BEGIN SELECT RAISE(ABORT,'invalid media lease'); END""",
            """CREATE TRIGGER IF NOT EXISTS targets_media_lease_update
               BEFORE UPDATE OF media_state,media_lease_token,
                                media_lease_started_at ON targets
               WHEN (NEW.media_state='leased' AND
                     (NEW.media_lease_token IS NULL OR
                      NEW.media_lease_started_at IS NULL))
                    OR (NEW.media_state<>'leased' AND
                        (NEW.media_lease_token IS NOT NULL OR
                         NEW.media_lease_started_at IS NOT NULL))
               BEGIN SELECT RAISE(ABORT,'invalid media lease'); END""",
            """CREATE TRIGGER IF NOT EXISTS pacing_request_lease_insert
               BEFORE INSERT ON pacing
               WHEN (NEW.reservation_token IS NULL) <>
                    (NEW.reservation_started_at IS NULL)
               BEGIN SELECT RAISE(ABORT,'invalid request lease'); END""",
            """CREATE TRIGGER IF NOT EXISTS pacing_request_lease_update
               BEFORE UPDATE OF reservation_token,reservation_started_at
               ON pacing
               WHEN (NEW.reservation_token IS NULL) <>
                    (NEW.reservation_started_at IS NULL)
               BEGIN SELECT RAISE(ABORT,'invalid request lease'); END""",
            """CREATE TRIGGER IF NOT EXISTS captured_requires_observation_insert
               BEFORE INSERT ON targets
               WHEN NEW.state='captured' AND NOT EXISTS (
                   SELECT 1 FROM observations WHERE post_id=NEW.post_id
               )
               BEGIN
                   SELECT RAISE(ABORT,'captured target requires observation');
               END""",
            """CREATE TRIGGER IF NOT EXISTS captured_requires_observation
               BEFORE UPDATE OF state ON targets
               WHEN NEW.state='captured' AND NOT EXISTS (
                   SELECT 1 FROM observations WHERE post_id=NEW.post_id
               )
               BEGIN
                   SELECT RAISE(ABORT,'captured target requires observation');
               END""",
            """CREATE TRIGGER IF NOT EXISTS preserve_captured_observation
               BEFORE DELETE ON observations
               WHEN EXISTS (
                   SELECT 1 FROM targets
                    WHERE post_id=OLD.post_id AND state='captured'
               )
               BEGIN
                   SELECT RAISE(ABORT,'cannot delete captured observation');
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_parent_demand_insert
               AFTER INSERT ON reply_edges BEGIN
                   UPDATE targets SET parent_demand=parent_demand+1
                    WHERE post_id=NEW.parent_id;
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_parent_demand_delete
               AFTER DELETE ON reply_edges BEGIN
                   UPDATE targets SET parent_demand=parent_demand-1
                    WHERE post_id=OLD.parent_id;
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_parent_demand_update
               AFTER UPDATE OF parent_id ON reply_edges
               WHEN OLD.parent_id<>NEW.parent_id BEGIN
                   UPDATE targets SET parent_demand=parent_demand-1
                    WHERE post_id=OLD.parent_id;
                   UPDATE targets SET parent_demand=parent_demand+1
                    WHERE post_id=NEW.parent_id;
               END""",
            """CREATE TRIGGER IF NOT EXISTS targets_counter_insert
               AFTER INSERT ON targets BEGIN
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_total';
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_state_'||NEW.state;
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_media_'||NEW.media_state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS targets_counter_delete
               AFTER DELETE ON targets BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='targets_total';
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='targets_state_'||OLD.state;
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='targets_media_'||OLD.media_state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS targets_state_counter_update
               AFTER UPDATE OF state ON targets WHEN OLD.state<>NEW.state BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_state_'||OLD.state;
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_state_'||NEW.state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS targets_media_counter_update
               AFTER UPDATE OF media_state ON targets
               WHEN OLD.media_state<>NEW.media_state BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_media_'||OLD.media_state;
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_media_'||NEW.media_state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS asset_jobs_counter_insert
               AFTER INSERT ON asset_jobs BEGIN
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='asset_jobs_total';
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='asset_jobs_state_'||NEW.state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS asset_jobs_counter_delete
               AFTER DELETE ON asset_jobs BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='asset_jobs_total';
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='asset_jobs_state_'||OLD.state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS asset_jobs_state_counter_update
               AFTER UPDATE OF state ON asset_jobs WHEN OLD.state<>NEW.state BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=NEW.updated_at
                    WHERE counter_name='asset_jobs_state_'||OLD.state;
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='asset_jobs_state_'||NEW.state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_counter_insert
               AFTER INSERT ON reply_edges BEGIN
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.discovered_at
                    WHERE counter_name='reply_edges_total';
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_counter_delete
               AFTER DELETE ON reply_edges BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.discovered_at
                    WHERE counter_name='reply_edges_total';
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_cycle_counter_insert
               AFTER INSERT ON reply_edges WHEN NEW.cycle_detected=1 BEGIN
                   UPDATE progress_counters SET value=value+1,
                          updated_at=NEW.discovered_at
                    WHERE counter_name='reply_edges_cycles';
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_cycle_counter_delete
               AFTER DELETE ON reply_edges WHEN OLD.cycle_detected=1 BEGIN
                   UPDATE progress_counters SET value=value-1,
                          updated_at=OLD.discovered_at
                    WHERE counter_name='reply_edges_cycles';
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_cycle_counter_update
               AFTER UPDATE OF cycle_detected ON reply_edges
               WHEN OLD.cycle_detected<>NEW.cycle_detected BEGIN
                   UPDATE progress_counters SET
                          value=value+NEW.cycle_detected-OLD.cycle_detected,
                          updated_at=NEW.discovered_at
                    WHERE counter_name='reply_edges_cycles';
               END""",
            """CREATE TRIGGER IF NOT EXISTS observations_counter_insert
               AFTER INSERT ON observations BEGIN
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.captured_at
                    WHERE counter_name='observations_total';
               END""",
            """CREATE TRIGGER IF NOT EXISTS observations_counter_delete
               AFTER DELETE ON observations BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.captured_at
                    WHERE counter_name='observations_total';
               END""",
            """CREATE TRIGGER IF NOT EXISTS observations_focal_counter_insert
               AFTER INSERT ON observations WHEN NEW.source_kind='x:focal' BEGIN
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.captured_at
                    WHERE counter_name='observations_focal';
               END""",
            """CREATE TRIGGER IF NOT EXISTS observations_focal_counter_delete
               AFTER DELETE ON observations WHEN OLD.source_kind='x:focal' BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.captured_at
                    WHERE counter_name='observations_focal';
               END""",
            """CREATE TRIGGER IF NOT EXISTS observations_focal_counter_update
               AFTER UPDATE OF source_kind ON observations
               WHEN OLD.source_kind<>NEW.source_kind BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=NEW.captured_at
                    WHERE counter_name='observations_focal'
                      AND OLD.source_kind='x:focal';
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.captured_at
                    WHERE counter_name='observations_focal'
                      AND NEW.source_kind='x:focal';
               END""",
            """CREATE TRIGGER IF NOT EXISTS archive_posts_counter_insert
               AFTER INSERT ON archive_posts BEGIN
                   UPDATE progress_counters SET value=value+1,
                          generation=MAX(generation,NEW.durable_generation),
                          updated_at=NEW.last_captured_at
                    WHERE counter_name='archive_posts_total';
               END""",
            """CREATE TRIGGER IF NOT EXISTS archive_posts_counter_delete
               AFTER DELETE ON archive_posts BEGIN
                   UPDATE progress_counters SET value=value-1,
                          updated_at=OLD.last_captured_at
                    WHERE counter_name='archive_posts_total';
               END""",
            """CREATE TRIGGER IF NOT EXISTS archive_media_counter_insert
               AFTER INSERT ON archive_media BEGIN
                   UPDATE progress_counters SET value=value+1,
                          generation=MAX(generation,NEW.durable_generation),
                          updated_at=NEW.captured_at
                    WHERE counter_name='archive_media_files';
                   UPDATE progress_counters SET value=value+NEW.final_bytes,
                          generation=MAX(generation,NEW.durable_generation),
                          updated_at=NEW.captured_at
                    WHERE counter_name='archive_media_bytes';
               END""",
            """CREATE TRIGGER IF NOT EXISTS archive_media_counter_delete
               AFTER DELETE ON archive_media BEGIN
                   UPDATE progress_counters SET value=value-1,
                          updated_at=OLD.captured_at
                    WHERE counter_name='archive_media_files';
                   UPDATE progress_counters SET value=value-OLD.final_bytes,
                          updated_at=OLD.captured_at
                    WHERE counter_name='archive_media_bytes';
               END""",
            """CREATE TRIGGER IF NOT EXISTS archive_media_counter_update
               AFTER UPDATE OF final_bytes,durable_generation ON archive_media BEGIN
                   UPDATE progress_counters SET
                          value=value+NEW.final_bytes-OLD.final_bytes,
                          generation=MAX(generation,NEW.durable_generation),
                          updated_at=NEW.captured_at
                    WHERE counter_name='archive_media_bytes';
                   UPDATE progress_counters SET
                          generation=MAX(generation,NEW.durable_generation),
                          updated_at=NEW.captured_at
                    WHERE counter_name='archive_media_files';
               END""",
            """CREATE TRIGGER IF NOT EXISTS targets_unavailable_counter_insert
               AFTER INSERT ON targets WHEN NEW.state='unavailable' BEGIN
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_unavailable_private'
                      AND COALESCE(NEW.last_error_class,'') IN
                          ('private','protected','auth_required');
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_unavailable_deleted'
                      AND instr(lower(COALESCE(NEW.last_error_class,'')),'deleted')>0;
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_unavailable_suspended'
                      AND instr(lower(COALESCE(NEW.last_error_class,'')),'suspend')>0;
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='targets_unavailable_other'
                      AND COALESCE(NEW.last_error_class,'') NOT IN
                          ('private','protected','auth_required')
                      AND instr(lower(COALESCE(NEW.last_error_class,'')),'deleted')=0
                      AND instr(lower(COALESCE(NEW.last_error_class,'')),'suspend')=0;
               END""",
            """CREATE TRIGGER IF NOT EXISTS targets_unavailable_counter_delete
               AFTER DELETE ON targets WHEN OLD.state='unavailable' BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='targets_unavailable_private'
                      AND COALESCE(OLD.last_error_class,'') IN
                          ('private','protected','auth_required');
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='targets_unavailable_deleted'
                      AND instr(lower(COALESCE(OLD.last_error_class,'')),'deleted')>0;
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='targets_unavailable_suspended'
                      AND instr(lower(COALESCE(OLD.last_error_class,'')),'suspend')>0;
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='targets_unavailable_other'
                      AND COALESCE(OLD.last_error_class,'') NOT IN
                          ('private','protected','auth_required')
                      AND instr(lower(COALESCE(OLD.last_error_class,'')),'deleted')=0
                      AND instr(lower(COALESCE(OLD.last_error_class,'')),'suspend')=0;
               END""",
            """CREATE TRIGGER IF NOT EXISTS targets_unavailable_counter_update
               AFTER UPDATE OF state,last_error_class ON targets
               WHEN OLD.state<>NEW.state OR
                    COALESCE(OLD.last_error_class,'')<>
                    COALESCE(NEW.last_error_class,'') BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=NEW.updated_at
                    WHERE OLD.state='unavailable' AND counter_name=
                      CASE
                        WHEN COALESCE(OLD.last_error_class,'') IN
                          ('private','protected','auth_required')
                          THEN 'targets_unavailable_private'
                        WHEN instr(lower(COALESCE(OLD.last_error_class,'')),
                                   'deleted')>0
                          THEN 'targets_unavailable_deleted'
                        WHEN instr(lower(COALESCE(OLD.last_error_class,'')),
                                   'suspend')>0
                          THEN 'targets_unavailable_suspended'
                        ELSE 'targets_unavailable_other'
                      END;
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE NEW.state='unavailable' AND counter_name=
                      CASE
                        WHEN COALESCE(NEW.last_error_class,'') IN
                          ('private','protected','auth_required')
                          THEN 'targets_unavailable_private'
                        WHEN instr(lower(COALESCE(NEW.last_error_class,'')),
                                   'deleted')>0
                          THEN 'targets_unavailable_deleted'
                        WHEN instr(lower(COALESCE(NEW.last_error_class,'')),
                                   'suspend')>0
                          THEN 'targets_unavailable_suspended'
                        ELSE 'targets_unavailable_other'
                      END;
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_rollup_insert
               AFTER INSERT ON reply_edges BEGIN
                   INSERT INTO conversation_rollups(
                       chain_id,state,captured_count,unavailable_count,
                       retryable_count,pending_count,manual_count,edge_count,
                       updated_at)
                   SELECT COALESCE(NEW.conversation_id,NEW.child_id),
                          CASE
                            WHEN t.state='manual_review' THEN 'manual_review'
                            WHEN t.state IN ('pending','leased') THEN 'pending'
                            WHEN t.state='retryable' THEN 'retry_delayed'
                            WHEN t.state='unavailable' THEN 'unavailable_boundary'
                            ELSE 'fully_captured'
                          END,
                          t.state='captured',t.state='unavailable',
                          t.state='retryable',t.state IN ('pending','leased'),
                          t.state='manual_review',1,NEW.discovered_at
                     FROM targets t WHERE t.post_id=NEW.parent_id
                   ON CONFLICT(chain_id) DO UPDATE SET
                     state=CASE
                       WHEN conversation_rollups.manual_count+excluded.manual_count>0
                         THEN 'manual_review'
                       WHEN conversation_rollups.pending_count+excluded.pending_count>0
                         THEN 'pending'
                       WHEN conversation_rollups.retryable_count+excluded.retryable_count>0
                         THEN 'retry_delayed'
                       WHEN conversation_rollups.unavailable_count+
                            excluded.unavailable_count>0
                         THEN 'unavailable_boundary'
                       ELSE 'fully_captured'
                     END,
                     captured_count=captured_count+excluded.captured_count,
                     unavailable_count=unavailable_count+excluded.unavailable_count,
                     retryable_count=retryable_count+excluded.retryable_count,
                     pending_count=pending_count+excluded.pending_count,
                     manual_count=manual_count+excluded.manual_count,
                     edge_count=edge_count+1,updated_at=excluded.updated_at;
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_rollup_delete
               AFTER DELETE ON reply_edges BEGIN
                   UPDATE conversation_rollups SET
                     captured_count=captured_count-(SELECT t.state='captured'
                         FROM targets t WHERE t.post_id=OLD.parent_id),
                     unavailable_count=unavailable_count-(SELECT t.state='unavailable'
                         FROM targets t WHERE t.post_id=OLD.parent_id),
                     retryable_count=retryable_count-(SELECT t.state='retryable'
                         FROM targets t WHERE t.post_id=OLD.parent_id),
                     pending_count=pending_count-(SELECT t.state IN ('pending','leased')
                         FROM targets t WHERE t.post_id=OLD.parent_id),
                     manual_count=manual_count-(SELECT t.state='manual_review'
                         FROM targets t WHERE t.post_id=OLD.parent_id),
                     edge_count=edge_count-1,updated_at=OLD.discovered_at
                    WHERE chain_id=COALESCE(OLD.conversation_id,OLD.child_id);
                   UPDATE conversation_rollups SET state=CASE
                     WHEN manual_count>0 THEN 'manual_review'
                     WHEN pending_count>0 THEN 'pending'
                     WHEN retryable_count>0 THEN 'retry_delayed'
                     WHEN unavailable_count>0 THEN 'unavailable_boundary'
                     ELSE 'fully_captured' END
                    WHERE chain_id=COALESCE(OLD.conversation_id,OLD.child_id)
                      AND edge_count>0;
                   DELETE FROM conversation_rollups
                    WHERE chain_id=COALESCE(OLD.conversation_id,OLD.child_id)
                      AND edge_count=0;
               END""",
            """CREATE TRIGGER IF NOT EXISTS reply_edges_rollup_update
               AFTER UPDATE OF parent_id,conversation_id ON reply_edges
               WHEN OLD.parent_id<>NEW.parent_id OR
                    COALESCE(OLD.conversation_id,OLD.child_id)<>
                    COALESCE(NEW.conversation_id,NEW.child_id) BEGIN
                   DELETE FROM conversation_rollups
                    WHERE chain_id IN (
                      COALESCE(OLD.conversation_id,OLD.child_id),
                      COALESCE(NEW.conversation_id,NEW.child_id));
                   INSERT INTO conversation_rollups(
                       chain_id,state,captured_count,unavailable_count,
                       retryable_count,pending_count,manual_count,edge_count,
                       updated_at)
                   SELECT COALESCE(e.conversation_id,e.child_id),
                          CASE
                            WHEN SUM(t.state='manual_review')>0
                              THEN 'manual_review'
                            WHEN SUM(t.state IN ('pending','leased'))>0
                              THEN 'pending'
                            WHEN SUM(t.state='retryable')>0
                              THEN 'retry_delayed'
                            WHEN SUM(t.state='unavailable')>0
                              THEN 'unavailable_boundary'
                            ELSE 'fully_captured'
                          END,
                          SUM(t.state='captured'),SUM(t.state='unavailable'),
                          SUM(t.state='retryable'),
                          SUM(t.state IN ('pending','leased')),
                          SUM(t.state='manual_review'),COUNT(*),MAX(e.discovered_at)
                     FROM reply_edges e
                     JOIN targets t ON t.post_id=e.parent_id
                    WHERE COALESCE(e.conversation_id,e.child_id) IN (
                      COALESCE(OLD.conversation_id,OLD.child_id),
                      COALESCE(NEW.conversation_id,NEW.child_id))
                    GROUP BY COALESCE(e.conversation_id,e.child_id);
               END""",
            """CREATE TRIGGER IF NOT EXISTS targets_state_rollup_update
               AFTER UPDATE OF state ON targets WHEN OLD.state<>NEW.state BEGIN
                   UPDATE conversation_rollups SET
                     captured_count=captured_count+
                       ((NEW.state='captured')-(OLD.state='captured'))*(
                         SELECT COUNT(*) FROM reply_edges e
                          WHERE e.parent_id=NEW.post_id AND
                            COALESCE(e.conversation_id,e.child_id)=
                            conversation_rollups.chain_id),
                     unavailable_count=unavailable_count+
                       ((NEW.state='unavailable')-(OLD.state='unavailable'))*(
                         SELECT COUNT(*) FROM reply_edges e
                          WHERE e.parent_id=NEW.post_id AND
                            COALESCE(e.conversation_id,e.child_id)=
                            conversation_rollups.chain_id),
                     retryable_count=retryable_count+
                       ((NEW.state='retryable')-(OLD.state='retryable'))*(
                         SELECT COUNT(*) FROM reply_edges e
                          WHERE e.parent_id=NEW.post_id AND
                            COALESCE(e.conversation_id,e.child_id)=
                            conversation_rollups.chain_id),
                     pending_count=pending_count+
                       ((NEW.state IN ('pending','leased'))-
                        (OLD.state IN ('pending','leased')))*(
                         SELECT COUNT(*) FROM reply_edges e
                          WHERE e.parent_id=NEW.post_id AND
                            COALESCE(e.conversation_id,e.child_id)=
                            conversation_rollups.chain_id),
                     manual_count=manual_count+
                       ((NEW.state='manual_review')-
                        (OLD.state='manual_review'))*(
                         SELECT COUNT(*) FROM reply_edges e
                          WHERE e.parent_id=NEW.post_id AND
                            COALESCE(e.conversation_id,e.child_id)=
                            conversation_rollups.chain_id),
                     updated_at=NEW.updated_at
                    WHERE chain_id IN (
                      SELECT COALESCE(conversation_id,child_id)
                        FROM reply_edges WHERE parent_id=NEW.post_id);
                   UPDATE conversation_rollups SET state=CASE
                     WHEN manual_count>0 THEN 'manual_review'
                     WHEN pending_count>0 THEN 'pending'
                     WHEN retryable_count>0 THEN 'retry_delayed'
                     WHEN unavailable_count>0 THEN 'unavailable_boundary'
                     ELSE 'fully_captured' END
                    WHERE chain_id IN (
                      SELECT COALESCE(conversation_id,child_id)
                        FROM reply_edges WHERE parent_id=NEW.post_id);
               END""",
            """CREATE TRIGGER IF NOT EXISTS conversation_counter_insert
               AFTER INSERT ON conversation_rollups BEGIN
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='conversations_state_'||NEW.state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS conversation_counter_delete
               AFTER DELETE ON conversation_rollups BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=OLD.updated_at
                    WHERE counter_name='conversations_state_'||OLD.state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS conversation_counter_update
               AFTER UPDATE OF state ON conversation_rollups
               WHEN OLD.state<>NEW.state BEGIN
                   UPDATE progress_counters SET value=value-1,updated_at=NEW.updated_at
                    WHERE counter_name='conversations_state_'||OLD.state;
                   UPDATE progress_counters SET value=value+1,updated_at=NEW.updated_at
                    WHERE counter_name='conversations_state_'||NEW.state;
               END""",
            """CREATE TRIGGER IF NOT EXISTS descriptor_terminal_state
               BEFORE UPDATE OF state ON descriptor_generations
               WHEN OLD.state IN ('superseded','invalid') AND NEW.state='active'
               BEGIN SELECT RAISE(ABORT,'terminal descriptor cannot reactivate'); END""",
            """CREATE TRIGGER IF NOT EXISTS asset_active_descriptor_insert
               BEFORE INSERT ON asset_jobs
               WHEN NEW.descriptor_id IS NOT NULL AND NOT EXISTS (
                   SELECT 1 FROM descriptor_generations d
                    WHERE d.descriptor_id=NEW.descriptor_id
                      AND d.owner_kind=NEW.owner_kind
                      AND d.owner_id=NEW.owner_id
                      AND d.media_ordinal=NEW.media_ordinal
                      AND d.state='active'
               )
               BEGIN SELECT RAISE(ABORT,'asset requires active owned descriptor'); END""",
            """CREATE TRIGGER IF NOT EXISTS asset_active_descriptor_update
               BEFORE UPDATE OF descriptor_id,owner_kind,owner_id,media_ordinal
               ON asset_jobs
               WHEN NEW.descriptor_id IS NOT NULL AND NOT EXISTS (
                   SELECT 1 FROM descriptor_generations d
                    WHERE d.descriptor_id=NEW.descriptor_id
                      AND d.owner_kind=NEW.owner_kind
                      AND d.owner_id=NEW.owner_id
                      AND d.media_ordinal=NEW.media_ordinal
                      AND d.state='active'
               )
               BEGIN SELECT RAISE(ABORT,'asset requires active owned descriptor'); END""",
            """CREATE TRIGGER IF NOT EXISTS asset_terminal_transition
               BEFORE UPDATE OF state ON asset_jobs
               WHEN OLD.state IN ('unavailable','manual_review')
                    AND NEW.state NOT IN (OLD.state,'needs_refresh')
               BEGIN SELECT RAISE(ABORT,'terminal asset requires explicit refresh'); END""",
            """CREATE TRIGGER IF NOT EXISTS export_generation_monotonic
               BEFORE UPDATE ON export_views
               WHEN NEW.durable_generation<OLD.durable_generation
                    OR NEW.exported_generation<OLD.exported_generation
               BEGIN SELECT RAISE(ABORT,'export generation cannot move backward'); END""",
        )
        for statement in triggers:
            self.connection.execute(statement)

    def _migrate_v2_to_v3(self) -> None:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._create_v3_objects()
            self.connection.execute(
                """INSERT INTO schema_migrations(
                       version,applied_at,description
                   ) VALUES (?,?,?)
                   ON CONFLICT(version) DO UPDATE SET
                       applied_at=excluded.applied_at,
                       description=excluded.description""",
                (SCHEMA_VERSION, iso_now(), "goal-5 coherent archive state"),
            )
            self.connection.execute(
                """INSERT INTO schema_migrations(
                       version,applied_at,description
                   ) VALUES (?,?,?)
                   ON CONFLICT(version) DO UPDATE SET
                       applied_at=excluded.applied_at,
                       description=excluded.description""",
                (
                    V3_LOCAL_ADDENDUM_VERSION,
                    iso_now(),
                    "goal-5 incremental local truth addendum",
                ),
            )
            cursor = self.connection.execute(
                """UPDATE context_meta SET value=?
                     WHERE key='schema_version' AND value=?""",
                (str(SCHEMA_VERSION), str(BASE_SCHEMA_VERSION)),
            )
            if cursor.rowcount != 1:
                raise ContextError("context v3 migration version guard failed")
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def _migrate_v3_local_addendum(self) -> None:
        applied = self.connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version=?",
            (V3_LOCAL_ADDENDUM_VERSION,),
        ).fetchone()
        if applied is not None:
            return
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._create_v3_objects()
            self.connection.execute(
                """INSERT INTO schema_migrations(
                       version,applied_at,description
                   ) VALUES (?,?,?)""",
                (
                    V3_LOCAL_ADDENDUM_VERSION,
                    iso_now(),
                    "goal-5 incremental local truth addendum",
                ),
            )
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def _create_schema(self) -> None:
        states = ",".join(f"'{state}'" for state in VALID_STATES)
        try:
            self.connection.executescript(
                f"""BEGIN IMMEDIATE;
                CREATE TABLE context_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                INSERT INTO context_meta(key, value)
                    VALUES ('schema_version', '{BASE_SCHEMA_VERSION}');

                CREATE TABLE targets (
                    post_id TEXT PRIMARY KEY
                        CHECK(post_id <> '' AND post_id NOT GLOB '*[^0-9]*'),
                    conversation_id TEXT,
                    depth_min INTEGER NOT NULL DEFAULT 0 CHECK(depth_min >= 0),
                    state TEXT NOT NULL DEFAULT 'pending'
                        CHECK(state IN ({states})),
                    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                    next_attempt_at REAL NOT NULL DEFAULT 0,
                    lease_started_at REAL,
                    discovered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_error_class TEXT,
                    last_error_detail TEXT,
                    unavailable_at TEXT,
                    author_id TEXT,
                    media_state TEXT NOT NULL DEFAULT 'none'
                        CHECK(media_state IN
                            ('none','pending','leased','captured','retryable',
                             'unavailable','manual_review')),
                    media_attempts INTEGER NOT NULL DEFAULT 0
                        CHECK(media_attempts >= 0),
                    media_next_attempt_at REAL NOT NULL DEFAULT 0
                );

                CREATE TABLE reply_edges (
                    child_id TEXT PRIMARY KEY,
                    parent_id TEXT NOT NULL REFERENCES targets(post_id),
                    conversation_id TEXT,
                    depth INTEGER NOT NULL CHECK(depth >= 0),
                    discovered_run_id TEXT,
                    discovered_at TEXT NOT NULL,
                    cycle_detected INTEGER NOT NULL DEFAULT 0
                        CHECK(cycle_detected IN (0,1))
                );
                CREATE INDEX reply_edges_parent ON reply_edges(parent_id);
                CREATE INDEX reply_edges_conversation
                    ON reply_edges(conversation_id);

                CREATE TABLE observations (
                    post_id TEXT PRIMARY KEY REFERENCES targets(post_id),
                    captured_at TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    capture_count INTEGER NOT NULL DEFAULT 1
                        CHECK(capture_count >= 1)
                );

                CREATE TABLE seed_sources (
                    relative_path TEXT PRIMARY KEY,
                    sha256 TEXT NOT NULL
                        CHECK(length(sha256)=64
                              AND sha256 NOT GLOB '*[^0-9a-f]*'),
                    source_kind TEXT NOT NULL
                        CHECK(source_kind IN ('modern','legacy')),
                    run_id TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    record_count INTEGER NOT NULL CHECK(record_count >= 0),
                    edge_count INTEGER NOT NULL CHECK(edge_count >= 0)
                );

                CREATE TABLE local_posts (
                    post_id TEXT PRIMARY KEY
                        CHECK(post_id <> '' AND post_id NOT GLOB '*[^0-9]*'),
                    raw_json TEXT NOT NULL,
                    sha256 TEXT NOT NULL
                        CHECK(length(sha256)=64
                              AND sha256 NOT GLOB '*[^0-9a-f]*'),
                    relative_path TEXT NOT NULL,
                    source_kind TEXT NOT NULL
                        CHECK(source_kind IN ('modern','legacy')),
                    run_id TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE INDEX local_posts_source ON local_posts(relative_path);

                CREATE TABLE pacing (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    next_request_at REAL NOT NULL DEFAULT 0,
                    last_request_at REAL,
                    last_rate_limit_at REAL,
                    last_progress_at TEXT
                );
                INSERT INTO pacing(singleton) VALUES (1);

                CREATE TRIGGER captured_requires_observation
                BEFORE UPDATE OF state ON targets
                WHEN NEW.state = 'captured'
                     AND NOT EXISTS (
                         SELECT 1 FROM observations WHERE post_id = NEW.post_id
                     )
                BEGIN
                    SELECT RAISE(ABORT, 'captured target requires observation');
                END;

                CREATE TRIGGER preserve_captured_observation
                BEFORE DELETE ON observations
                WHEN EXISTS (
                    SELECT 1 FROM targets
                    WHERE post_id = OLD.post_id AND state = 'captured'
                )
                BEGIN
                    SELECT RAISE(ABORT, 'cannot delete captured observation');
                END;
                COMMIT;
                """
            )
        except BaseException:
            self.connection.rollback()
            raise
        self._migrate_v2_to_v3()

    def integrity_errors(self) -> list[str]:
        errors: list[str] = []
        result = self.connection.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            errors.append(str(result))
        foreign = list(self.connection.execute("PRAGMA foreign_key_check"))
        if foreign:
            errors.append(f"foreign key violations: {len(foreign)}")
        missing = self.connection.execute(
            """SELECT COUNT(*) FROM targets t
               WHERE t.state='captured' AND NOT EXISTS
                   (SELECT 1 FROM observations o WHERE o.post_id=t.post_id)"""
        ).fetchone()[0]
        if missing:
            errors.append(f"captured targets without observations: {missing}")
        missing_targets = self.connection.execute(
            """SELECT COUNT(*) FROM reply_edges e
               WHERE NOT EXISTS
                   (SELECT 1 FROM targets t WHERE t.post_id=e.parent_id)"""
        ).fetchone()[0]
        if missing_targets:
            errors.append(f"edges without targets: {missing_targets}")
        return errors

    def bind_identity(self, target_user_id: str, handle: str) -> None:
        previous = self.connection.execute(
            "SELECT value FROM context_meta WHERE key='target_user_id'"
        ).fetchone()
        account = self.connection.execute(
            "SELECT user_id FROM archive_account WHERE singleton=1"
        ).fetchone()
        if (
            previous and previous[0] != target_user_id
        ) or (
            account and account[0] != target_user_id
        ):
            bound = previous[0] if previous else account[0]
            raise ContextError(
                "context database identity does not match archive state: "
                f"{bound} != {target_user_id}"
            )
        observed = iso_now()
        with transaction(self.connection):
            self._set_meta("target_user_id", target_user_id)
            self._set_meta("canonical_handle", handle)
            self.connection.execute(
                """INSERT INTO archive_account(
                       singleton,user_id,requested_handle,canonical_handle,
                       bound_at,updated_at
                   ) VALUES (1,?,?,?,?,?)
                   ON CONFLICT(singleton) DO UPDATE SET
                       canonical_handle=excluded.canonical_handle,
                       updated_at=excluded.updated_at
                   WHERE archive_account.user_id=excluded.user_id""",
                (target_user_id, handle, handle, observed, observed),
            )

    def advance_archive_generation(
        self, view_names: Iterable[str], *, observed_at: str | None = None
    ) -> int:
        """Dirty a bounded set of materialized views in the caller's transaction."""
        views = tuple(sorted(set(view_names)))
        allowed = {
            "posts",
            "authored_posts",
            "reposts",
            "media",
            "context_posts",
            "reply_edges",
            "context_status",
        }
        if set(views) - allowed:
            raise ContextError("archive generation view set is invalid")
        changed_at = observed_at or iso_now()
        current = int(
            self.connection.execute(
                """SELECT current_generation FROM archive_generation
                     WHERE singleton=1"""
            ).fetchone()[0]
        )
        generation = current + 1
        self.connection.execute(
            """UPDATE archive_generation SET current_generation=?,updated_at=?
                 WHERE singleton=1""",
            (generation, changed_at),
        )
        if views:
            placeholders = ",".join("?" for _ in views)
            self.connection.execute(
                f"""UPDATE export_views SET durable_generation=?,status='dirty',
                           updated_at=? WHERE view_name IN ({placeholders})""",
                (generation, changed_at, *views),
            )
        return generation

    def commit_legacy_interval(
        self,
        *,
        interval_id: str,
        root_window_id: str,
        since_at: str,
        until_at: str,
        since_epoch: int,
        until_epoch: int,
        canonical_relative_path: str,
        canonical_sha256: str,
        canonical_stat: os.stat_result,
        run_id: str,
        evidence_sha256: str,
        observation_count: int,
        normalized_records: list[dict[str, Any]],
        observed_at: str,
    ) -> dict[str, int | bool]:
        """Commit one confirmed legacy interval without reading archive files."""
        if (
            not interval_id
            or len(interval_id) > 80
            or not root_window_id
            or len(root_window_id) > 80
            or not isinstance(since_epoch, int)
            or not isinstance(until_epoch, int)
            or until_epoch <= since_epoch
            or not descriptor_x.SHA256_RE.fullmatch(canonical_sha256)
            or not descriptor_x.SHA256_RE.fullmatch(evidence_sha256)
            or not isinstance(observation_count, int)
            or observation_count < 2
            or not run_id
        ):
            raise ContextError("legacy interval commit evidence is invalid")
        relative = Path(canonical_relative_path)
        if (
            not canonical_relative_path
            or relative.is_absolute()
            or ".." in relative.parts
            or canonical_stat.st_size < 0
        ):
            raise ContextError("legacy canonical source evidence is invalid")
        account = self.connection.execute(
            "SELECT user_id FROM archive_account WHERE singleton=1"
        ).fetchone()
        if account is None:
            raise ContextError("legacy interval database identity is missing")
        requested_user_id = str(account[0])
        prepared: list[dict[str, Any]] = []
        seen: set[str] = set()
        for record in normalized_records:
            if not isinstance(record, dict):
                raise ContextError("legacy normalized record is invalid")
            post_id = id_string(record.get("post_id"))
            relationship = str(record.get("relationship") or "")
            if (
                post_id is None
                or post_id in seen
                or relationship not in {"post", "reply", "repost"}
                or id_string(record.get("requested_user_id"))
                != requested_user_id
            ):
                raise ContextError("legacy normalized record identity is invalid")
            raw = record.get("gallery_dl")
            if (
                not isinstance(raw, dict)
                or id_string(raw.get("tweet_id")) != post_id
            ):
                raise ContextError("legacy normalized raw provenance is invalid")
            raw_json = json.dumps(
                raw,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            seen.add(post_id)
            prepared.append(
                {
                    "record": dict(record),
                    "raw_json": raw_json,
                    "raw_sha256": hashlib.sha256(
                        raw_json.encode("utf-8")
                    ).hexdigest(),
                }
            )

        with transaction(self.connection):
            existing_interval = self.connection.execute(
                "SELECT * FROM legacy_intervals WHERE interval_id=?",
                (interval_id,),
            ).fetchone()
            if existing_interval is not None:
                expected = (
                    root_window_id,
                    since_at,
                    until_at,
                    since_epoch,
                    until_epoch,
                    canonical_sha256,
                    len(prepared),
                    evidence_sha256,
                    observation_count,
                )
                actual = (
                    existing_interval["root_window_id"],
                    existing_interval["since_at"],
                    existing_interval["until_at"],
                    existing_interval["since_epoch"],
                    existing_interval["until_epoch"],
                    existing_interval["canonical_sha256"],
                    existing_interval["canonical_post_count"],
                    existing_interval["evidence_sha256"],
                    existing_interval["observation_count"],
                )
                if actual != expected:
                    raise ContextError("committed legacy interval evidence changed")
                return {
                    "generation": int(existing_interval["durable_generation"]),
                    "new_posts": 0,
                    "updated_posts": 0,
                    "idempotent": True,
                }

            relationships = {
                str(item["record"]["relationship"]) for item in prepared
            }
            dirty_views: set[str] = set()
            if relationships:
                dirty_views.add("posts")
            if relationships & {"post", "reply"}:
                dirty_views.add("authored_posts")
            if "repost" in relationships:
                dirty_views.add("reposts")
            generation = self.advance_archive_generation(
                dirty_views, observed_at=observed_at
            )
            self.connection.execute(
                """INSERT INTO archive_sources(
                       relative_path,source_kind,run_id,operation_id,
                       expected_sha256,stat_device,stat_inode,stat_size,
                       stat_mtime_ns,status,ingest_generation,registered_at,
                       processed_at,record_count,edge_count
                   ) VALUES (?,?,?,?,?,?,?,?,?,'committed',?,?,?,?,0)
                   ON CONFLICT(relative_path) DO NOTHING""",
                (
                    canonical_relative_path,
                    "legacy",
                    run_id,
                    interval_id,
                    canonical_sha256,
                    int(canonical_stat.st_dev),
                    int(canonical_stat.st_ino),
                    int(canonical_stat.st_size),
                    int(canonical_stat.st_mtime_ns),
                    generation,
                    observed_at,
                    observed_at,
                    len(prepared),
                ),
            )
            source = self.connection.execute(
                """SELECT * FROM archive_sources WHERE relative_path=?""",
                (canonical_relative_path,),
            ).fetchone()
            if source is None or (
                source["source_kind"],
                source["run_id"],
                source["operation_id"],
                source["expected_sha256"],
                source["status"],
            ) != (
                "legacy",
                run_id,
                interval_id,
                canonical_sha256,
                "committed",
            ):
                raise ContextError("legacy canonical source provenance changed")
            source_id = int(source["source_id"])
            new_posts = 0
            updated_posts = 0
            new_edges = 0
            local_parents = 0
            candidate_ids: set[str] = set()
            for item in prepared:
                record = item["record"]
                post_id = str(record["post_id"])
                candidate_ids.add(post_id)
                previous = self.connection.execute(
                    "SELECT normalized_json FROM archive_posts WHERE post_id=?",
                    (post_id,),
                ).fetchone()
                if previous is None:
                    merged = record
                    new_posts += 1
                else:
                    try:
                        prior_record = json.loads(str(previous[0]))
                    except (TypeError, json.JSONDecodeError) as exc:
                        raise ContextError(
                            "indexed archive post JSON is invalid"
                        ) from exc
                    merged = archive_x.merge_post_records(prior_record, record)
                    updated_posts += 1
                normalized_json = json.dumps(
                    merged,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                normalized_hash = hashlib.sha256(
                    normalized_json.encode("utf-8")
                ).hexdigest()
                first_captured = str(merged.get("first_captured_at") or observed_at)
                last_captured = str(merged.get("last_captured_at") or observed_at)
                capture_count = int(merged.get("capture_count") or 1)
                self.connection.execute(
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
                        requested_user_id,
                        id_string(merged.get("author_id")),
                        str(merged["relationship"]),
                        str(merged.get("posted_at") or "") or None,
                        normalized_json,
                        normalized_hash,
                        first_captured,
                        last_captured,
                        capture_count,
                        generation,
                    ),
                )
                self.connection.execute(
                    """INSERT INTO post_provenance(
                           post_id,source_id,record_sha256,source_endpoint,
                           observed_at
                       ) VALUES (?,?,?,?,?)""",
                    (
                        post_id,
                        source_id,
                        item["raw_sha256"],
                        interval_id,
                        observed_at,
                    ),
                )
                if id_string(record.get("author_id")) == requested_user_id:
                    self.connection.execute(
                        """INSERT INTO local_posts(
                               post_id,raw_json,sha256,relative_path,source_kind,
                               run_id,observed_at
                           ) VALUES (?,?,?,?,?,?,?)
                           ON CONFLICT(post_id) DO UPDATE SET
                               raw_json=excluded.raw_json,sha256=excluded.sha256,
                               relative_path=excluded.relative_path,
                               source_kind=excluded.source_kind,
                               run_id=excluded.run_id,
                               observed_at=excluded.observed_at
                           WHERE excluded.observed_at>=local_posts.observed_at""",
                        (
                            post_id,
                            item["raw_json"],
                            item["raw_sha256"],
                            canonical_relative_path,
                            "legacy",
                            run_id,
                            observed_at,
                        ),
                    )
                parent_id = id_string(record.get("reply_to_post_id"))
                if record["relationship"] == "reply" and parent_id:
                    candidate_ids.add(parent_id)
                    new_edges += int(
                        self.add_edge(
                            post_id,
                            parent_id,
                            conversation_id=id_string(record.get("conversation_id")),
                            depth=0,
                            run_id=run_id,
                            observed_at=observed_at,
                            max_depth=1_000,
                        )
                    )
            if candidate_ids:
                placeholders = ",".join("?" for _ in candidate_ids)
                local_candidates = list(
                    self.connection.execute(
                        f"""SELECT t.post_id,l.raw_json,l.source_kind,l.run_id
                              FROM targets t JOIN local_posts l
                                ON l.post_id=t.post_id
                             WHERE t.state<>'captured'
                               AND t.post_id IN ({placeholders})
                             ORDER BY t.depth_min,t.post_id""",
                        tuple(sorted(candidate_ids)),
                    )
                )
                for local in local_candidates:
                    self._capture_record(
                        str(local["post_id"]),
                        json.loads(str(local["raw_json"])),
                        source_kind=(
                            f"timeline:{local['source_kind']}:{local['run_id']}"
                        ),
                        target_user_id=requested_user_id,
                        max_depth=1_000,
                    )
                    local_parents += 1
            if new_edges or local_parents:
                self.connection.execute(
                    """UPDATE export_views SET durable_generation=?,status='dirty',
                               updated_at=? WHERE view_name IN
                               ('context_posts','reply_edges','context_status')""",
                    (generation, observed_at),
                )
            seeded = self.connection.execute(
                "SELECT sha256,source_kind,run_id FROM seed_sources "
                "WHERE relative_path=?",
                (canonical_relative_path,),
            ).fetchone()
            if seeded is not None and tuple(seeded) != (
                canonical_sha256,
                "legacy",
                run_id,
            ):
                raise ContextError("legacy seed source evidence changed")
            self.connection.execute(
                """INSERT INTO seed_sources(
                       relative_path,sha256,source_kind,run_id,processed_at,
                       record_count,edge_count
                   ) VALUES (?,?,'legacy',?,?,?,?)
                   ON CONFLICT(relative_path) DO UPDATE SET
                       processed_at=excluded.processed_at,
                       record_count=excluded.record_count,
                       edge_count=excluded.edge_count
                   WHERE seed_sources.sha256=excluded.sha256""",
                (
                    canonical_relative_path,
                    canonical_sha256,
                    run_id,
                    observed_at,
                    len(prepared),
                    new_edges,
                ),
            )
            self.connection.execute(
                "UPDATE archive_sources SET edge_count=? WHERE source_id=?",
                (new_edges, source_id),
            )
            self.connection.execute(
                """INSERT INTO legacy_intervals(
                       interval_id,root_window_id,since_at,until_at,
                       since_epoch,until_epoch,canonical_source_id,
                       canonical_sha256,canonical_post_count,evidence_sha256,
                       observation_count,durable_generation,committed_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    interval_id,
                    root_window_id,
                    since_at,
                    until_at,
                    since_epoch,
                    until_epoch,
                    source_id,
                    canonical_sha256,
                    len(prepared),
                    evidence_sha256,
                    observation_count,
                    generation,
                    observed_at,
                ),
            )
            return {
                "generation": generation,
                "new_posts": new_posts,
                "updated_posts": updated_posts,
                "new_edges": new_edges,
                "local_parents": local_parents,
                "idempotent": False,
            }

    def _register_descriptor_source(
        self, batch: descriptor_x.DescriptorBatch
    ) -> int | None:
        relative = batch.source_relative_path
        digest = batch.source_sha256
        if relative is None:
            return None
        path = Path(relative)
        if (
            path.is_absolute()
            or ".." in path.parts
            or not relative
            or digest is None
            or not descriptor_x.SHA256_RE.fullmatch(digest)
        ):
            raise ContextError("descriptor source evidence is invalid")
        now = iso_now()
        source_record_count = batch.source_record_count
        if source_record_count is None:
            source_record_count = len(batch.rows) + len(batch.errors)
        if source_record_count < 0:
            raise ContextError("descriptor source record count is invalid")
        self.connection.execute(
            """INSERT INTO archive_sources(
                   relative_path,source_kind,run_id,operation_id,
                   expected_sha256,status,ingest_generation,registered_at,
                   processed_at,record_count,edge_count
               ) VALUES (?,?,?,?,?,'committed',0,?,?,?,0)
               ON CONFLICT(relative_path) DO NOTHING""",
            (
                relative,
                batch.source_kind,
                batch.run_id,
                batch.operation_id,
                digest,
                now,
                now,
                source_record_count,
            ),
        )
        row = self.connection.execute(
            """SELECT source_id,source_kind,run_id,operation_id,
                      expected_sha256,status
                 FROM archive_sources WHERE relative_path=?""",
            (relative,),
        ).fetchone()
        if row is None or tuple(row)[1:] != (
            batch.source_kind,
            batch.run_id,
            batch.operation_id,
            digest,
            "committed",
        ):
            raise ContextError("descriptor source provenance changed")
        return int(row["source_id"])

    def _active_descriptor(
        self, owner_kind: str, owner_id: str, ordinal: int
    ) -> sqlite3.Row | None:
        return self.connection.execute(
            """SELECT * FROM descriptor_generations
                 WHERE owner_kind=? AND owner_id=? AND media_ordinal=?
                   AND state='active'""",
            (owner_kind, owner_id, ordinal),
        ).fetchone()

    def _upsert_asset_for_descriptor(
        self, row: dict[str, Any], descriptor_id: int
    ) -> tuple[bool, bool]:
        now = iso_now()
        owner = (row["owner_kind"], row["owner_id"], row["media_ordinal"])
        current = self.connection.execute(
            """SELECT * FROM asset_jobs
                 WHERE owner_kind=? AND owner_id=? AND media_ordinal=?""",
            owner,
        ).fetchone()
        if current is None:
            self.connection.execute(
                """INSERT INTO asset_jobs(
                       owner_kind,owner_id,media_ordinal,descriptor_id,state,
                       destination_scope,transfer_priority,expected_relative_path,
                       created_at,updated_at
                   ) VALUES (?,?,?,?,'pending',?,?,?,?,?)""",
                (
                    *owner,
                    descriptor_id,
                    descriptor_destination_scope(row),
                    asset_transfer_priority(
                        row["owner_kind"], row["media_type"]
                    ),
                    row["relative_path"],
                    now,
                    now,
                ),
            )
            return True, False

        descriptor_changed = current["descriptor_id"] != descriptor_id
        path_changed = (
            current["expected_relative_path"] not in (None, row["relative_path"])
        )
        state = str(current["state"])
        if state in {"unavailable", "manual_review"}:
            profile_changed = (
                row["owner_kind"] in {"profile_avatar", "profile_background"}
                and (descriptor_changed or path_changed)
            )
            self.connection.execute(
                """UPDATE asset_jobs SET descriptor_id=?,
                       destination_scope=?,transfer_priority=?,
                       expected_relative_path=?,state=CASE WHEN ?
                           THEN 'needs_refresh' ELSE state END,updated_at=?
                     WHERE asset_id=?""",
                (
                    descriptor_id,
                    descriptor_destination_scope(row),
                    asset_transfer_priority(
                        row["owner_kind"], row["media_type"]
                    ),
                    row["relative_path"],
                    int(profile_changed),
                    now,
                    current["asset_id"],
                ),
            )
            if not profile_changed:
                return False, False
            state = "needs_refresh"
        if state == "captured" and not path_changed:
            self.connection.execute(
                """UPDATE asset_jobs SET descriptor_id=?,
                       destination_scope=?,transfer_priority=?,
                       expected_relative_path=?,updated_at=?
                     WHERE asset_id=?""",
                (
                    descriptor_id,
                    descriptor_destination_scope(row),
                    asset_transfer_priority(
                        row["owner_kind"], row["media_type"]
                    ),
                    row["relative_path"],
                    now,
                    current["asset_id"],
                ),
            )
            return False, False

        self.connection.execute(
            """UPDATE asset_jobs SET descriptor_id=?,state='pending',
                   destination_scope=?,transfer_priority=?,
                   compatibility_job=0,attempts=CASE WHEN ? THEN 0 ELSE attempts END,
                   next_attempt_at=0,lease_token=NULL,lease_started_at=NULL,
                   expected_relative_path=?,final_relative_path=NULL,
                   final_sha256=NULL,final_bytes=NULL,verified_device=NULL,
                   verified_inode=NULL,verified_size=NULL,verified_mtime_ns=NULL,
                   last_error_class=NULL,last_error_detail=NULL,
                   completed_at=NULL,updated_at=?
                 WHERE asset_id=?""",
            (
                descriptor_id,
                descriptor_destination_scope(row),
                asset_transfer_priority(row["owner_kind"], row["media_type"]),
                int(descriptor_changed),
                row["relative_path"],
                now,
                current["asset_id"],
            ),
        )
        return False, state == "captured" and path_changed

    def _persist_descriptor_row(
        self,
        row: dict[str, Any],
        *,
        source_id: int | None,
        artifact_sha256: str,
    ) -> tuple[int, bool, bool, bool]:
        owner = (row["owner_kind"], row["owner_id"], row["media_ordinal"])
        active = self._active_descriptor(*owner)
        created_generation = False
        if active is not None and (
            active["descriptor_sha256"] == row["descriptor_sha256"]
        ):
            descriptor_id = int(active["descriptor_id"])
        else:
            if active is not None:
                self.connection.execute(
                    """UPDATE descriptor_generations
                          SET state='superseded',superseded_at=?
                        WHERE descriptor_id=? AND state='active'""",
                    (iso_now(), active["descriptor_id"]),
                )
            generation = int(
                self.connection.execute(
                    """SELECT COALESCE(MAX(generation),0)+1
                         FROM descriptor_generations
                        WHERE owner_kind=? AND owner_id=? AND media_ordinal=?""",
                    owner,
                ).fetchone()[0]
            )
            cursor = self.connection.execute(
                """INSERT INTO descriptor_generations(
                       owner_kind,owner_id,media_ordinal,generation,source_id,
                       source_operation,media_type,extension,private_url,
                       url_sha256,url_host,descriptor_sha256,filename,
                       relative_directory,width,height,duration_seconds,bitrate,
                       alt_text,variant_json,posted_at,original_posted_at,
                       author_id,author_handle,conversation_id,reply_id,
                       retweet_id,state,captured_at
                   ) VALUES (
                       :owner_kind,:owner_id,:media_ordinal,:generation,
                       :source_id,:source_operation,:media_type,:extension,
                       :private_url,:url_sha256,:url_host,:descriptor_sha256,
                       :filename,:relative_directory,:width,:height,
                       :duration_seconds,:bitrate,:alt_text,:variant_json,
                       :posted_at,:original_posted_at,:author_id,:author_handle,
                       :conversation_id,:reply_id,:retweet_id,'active',
                       :captured_at
                   )""",
                {
                    **row,
                    "generation": generation,
                    "source_id": source_id,
                    "variant_json": (
                        descriptor_x.canonical_json(row["variant"])
                        if row["variant"] is not None
                        else None
                    ),
                },
            )
            descriptor_id = int(cursor.lastrowid)
            created_generation = True

        record_sha256 = hashlib.sha256(
            descriptor_x.canonical_json(row).encode("utf-8")
        ).hexdigest()
        existing_observation = self.connection.execute(
            """SELECT artifact_sha256,record_sha256,source_id
                 FROM descriptor_observations
                WHERE descriptor_id=? AND operation_id=?""",
            (descriptor_id, row["operation_id"]),
        ).fetchone()
        if existing_observation is not None and tuple(existing_observation) != (
            artifact_sha256,
            record_sha256,
            source_id,
        ):
            raise ContextError("descriptor replay provenance changed")
        self.connection.execute(
            """INSERT OR IGNORE INTO descriptor_observations(
                   descriptor_id,operation_id,source_id,artifact_sha256,
                   record_sha256,observed_at
               ) VALUES (?,?,?,?,?,?)""",
            (
                descriptor_id,
                row["operation_id"],
                source_id,
                artifact_sha256,
                record_sha256,
                row["captured_at"],
            ),
        )
        created_job, reopened_job = self._upsert_asset_for_descriptor(
            row, descriptor_id
        )
        return descriptor_id, created_generation, created_job, reopened_job

    def _enqueue_missing_descriptor(
        self,
        owner_kind: str,
        owner_id: str,
        ordinal: int,
        *,
        destination_scope: str = "unknown",
    ) -> bool:
        if destination_scope not in {"main", "context", "profile", "unknown"}:
            raise ContextError("missing descriptor destination scope is invalid")
        if self._active_descriptor(owner_kind, owner_id, ordinal) is not None:
            return False
        now = iso_now()
        current = self.connection.execute(
            """SELECT asset_id,state FROM asset_jobs
                 WHERE owner_kind=? AND owner_id=? AND media_ordinal=?""",
            (owner_kind, owner_id, ordinal),
        ).fetchone()
        if current is None:
            self.connection.execute(
                """INSERT INTO asset_jobs(
                       owner_kind,owner_id,media_ordinal,state,destination_scope,
                       compatibility_job,created_at,updated_at
                   ) VALUES (?,?,?,'needs_refresh',?,0,?,?)""",
                (owner_kind, owner_id, ordinal, destination_scope, now, now),
            )
            return True
        if current["state"] in {"pending", "retryable", "needs_refresh"}:
            self.connection.execute(
                """UPDATE asset_jobs SET state='needs_refresh',
                       descriptor_id=NULL,next_attempt_at=0,
                       destination_scope=CASE
                           WHEN destination_scope='unknown' THEN ?
                           ELSE destination_scope END,
                       lease_token=NULL,lease_started_at=NULL,
                       updated_at=? WHERE asset_id=?""",
                (destination_scope, now, current["asset_id"]),
            )
        return False

    def _persist_descriptor_batches(
        self,
        batches: tuple[descriptor_x.DescriptorBatch, ...],
        records: dict[str, dict[str, Any]],
        *,
        allow_profile: bool,
    ) -> dict[str, Any]:
        missing_scope = batch_destination_scope(batches)
        summary = {
            "batches": len(batches),
            "artifact_errors": sum(len(batch.errors) for batch in batches),
            "rows_seen": sum(len(batch.rows) for batch in batches),
            "rows_accepted": 0,
            "rows_rejected": 0,
            "conflicting_rows": 0,
            "generations_created": 0,
            "jobs_created": 0,
            "jobs_reopened": 0,
            "needs_refresh_created": 0,
            "status": "complete",
        }
        expected: set[tuple[str, str, int]] = set()
        for post_id, metadata in records.items():
            count = metadata.get("count")
            if isinstance(count, int) and not isinstance(count, bool) and count > 0:
                expected.update(("post", post_id, ordinal) for ordinal in range(1, count + 1))

        covered: set[tuple[str, str, int]] = set()
        for batch in batches:
            source_id = self._register_descriptor_source(batch)
            artifact_hash = batch.source_sha256 or hashlib.sha256(
                descriptor_x.canonical_json(batch.rows).encode("utf-8")
            ).hexdigest()
            candidates: dict[tuple[str, str, int], dict[str, Any]] = {}
            conflicts: set[tuple[str, str, int]] = set()
            for row in batch.rows:
                key = (row["owner_kind"], row["owner_id"], row["media_ordinal"])
                if row["owner_kind"] == "post":
                    metadata = records.get(str(row["post_id"]))
                    if metadata is None:
                        summary["rows_rejected"] += 1
                        continue
                    count = metadata.get("count")
                    if (
                        isinstance(count, int)
                        and not isinstance(count, bool)
                        and count >= 0
                        and row["media_ordinal"] > count
                    ):
                        summary["rows_rejected"] += 1
                        continue
                elif not allow_profile:
                    summary["rows_rejected"] += 1
                    continue
                previous = candidates.get(key)
                if previous is not None and (
                    previous["descriptor_sha256"] != row["descriptor_sha256"]
                ):
                    conflicts.add(key)
                else:
                    candidates[key] = row
            for key in conflicts:
                candidates.pop(key, None)
            summary["conflicting_rows"] += len(conflicts)

            for key, row in candidates.items():
                (
                    _descriptor_id,
                    created_generation,
                    created_job,
                    reopened_job,
                ) = self._persist_descriptor_row(
                    row,
                    source_id=source_id,
                    artifact_sha256=artifact_hash,
                )
                covered.add(key)
                summary["rows_accepted"] += 1
                summary["generations_created"] += int(created_generation)
                summary["jobs_created"] += int(created_job)
                summary["jobs_reopened"] += int(reopened_job)

        for key in sorted(expected - covered):
            summary["needs_refresh_created"] += int(
                self._enqueue_missing_descriptor(
                    *key, destination_scope=missing_scope
                )
            )
        return summary

    def persist_descriptor_batches(
        self,
        batches: Iterable[descriptor_x.DescriptorBatch],
        accepted_records: Iterable[dict[str, Any]],
        *,
        allow_profile: bool = False,
    ) -> dict[str, Any]:
        selected_batches = tuple(batches)
        missing_scope = batch_destination_scope(selected_batches)
        records = {
            post_id: metadata
            for metadata in accepted_records
            if (post_id := id_string(metadata.get("tweet_id"))) is not None
        }
        try:
            with savepoint(self.connection, "descriptor_capture"):
                summary = self._persist_descriptor_batches(
                    selected_batches,
                    records,
                    allow_profile=allow_profile,
                )
        except Exception as exc:
            summary = {
                "status": "degraded",
                "batches": len(selected_batches),
                "rows_seen": sum(len(batch.rows) for batch in selected_batches),
                "rows_accepted": 0,
                "rows_rejected": 0,
                "artifact_errors": sum(
                    len(batch.errors) for batch in selected_batches
                ),
                "generations_created": 0,
                "jobs_created": 0,
                "jobs_reopened": 0,
                "needs_refresh_created": 0,
                "error_class": exc.__class__.__name__,
            }
            try:
                with savepoint(self.connection, "descriptor_degraded_queue"):
                    for post_id, metadata in records.items():
                        count = metadata.get("count")
                        if not isinstance(count, int) or isinstance(count, bool):
                            continue
                        for ordinal in range(1, max(0, count) + 1):
                            summary["needs_refresh_created"] += int(
                                self._enqueue_missing_descriptor(
                                    "post",
                                    post_id,
                                    ordinal,
                                    destination_scope=missing_scope,
                                )
                            )
            except Exception as queue_exc:
                summary["queue_error_class"] = queue_exc.__class__.__name__
        for batch in selected_batches:
            batch.persistence = dict(summary)
        return summary

    def reclaim_stale_asset_leases(self, now: float, lease_seconds: float) -> int:
        if lease_seconds <= 0:
            raise ContextError("asset lease duration must be positive")
        cutoff = now - lease_seconds
        with transaction(self.connection):
            cursor = self.connection.execute(
                """UPDATE asset_jobs SET state='retryable',
                       lease_token=NULL,lease_started_at=NULL,
                       next_attempt_at=?,last_error_class='stale_asset_lease',
                       last_error_detail=NULL,updated_at=?
                     WHERE state='leased' AND lease_started_at < ?""",
                (now, iso_now(), cutoff),
            )
        return cursor.rowcount

    def claim_asset(
        self, *, now: float, lease_seconds: float
    ) -> dict[str, Any] | None:
        self.reclaim_stale_asset_leases(now, lease_seconds)
        with transaction(self.connection):
            row = self.connection.execute(ASSET_CLAIM_SQL, (now,)).fetchone()
            if row is None:
                return None
            token = secrets.token_hex(16)
            cursor = self.connection.execute(
                """UPDATE asset_jobs SET state='leased',lease_token=?,
                       lease_started_at=?,attempts=attempts+1,updated_at=?
                     WHERE asset_id=? AND descriptor_id=?
                       AND state IN ('pending','retryable')""",
                (
                    token,
                    now,
                    iso_now(),
                    row["asset_id"],
                    row["descriptor_id"],
                ),
            )
            if cursor.rowcount != 1:
                raise ContextError("asset claim changed during guarded update")
            claimed = dict(row)
            claimed["lease_token"] = token
            claimed["lease_started_at"] = now
            claimed["attempts"] = int(row["attempts"]) + 1
            return claimed

    def _update_post_asset_rollup(self, owner_kind: str, owner_id: str) -> None:
        if owner_kind != "post":
            return
        states = [
            str(row[0])
            for row in self.connection.execute(
                """SELECT state FROM asset_jobs
                     WHERE owner_kind='post' AND owner_id=?""",
                (owner_id,),
            )
        ]
        if not states:
            return
        if all(state == "captured" for state in states):
            rollup = "captured"
        elif "manual_review" in states:
            rollup = "manual_review"
        elif all(state in {"captured", "unavailable"} for state in states):
            rollup = "unavailable"
        elif "retryable" in states:
            rollup = "retryable"
        else:
            rollup = "pending"
        self.connection.execute(
            """UPDATE targets SET media_state=?,media_lease_started_at=NULL,
                   media_lease_token=NULL,media_next_attempt_at=0,updated_at=?
                 WHERE post_id=? AND state='captured'""",
            (rollup, iso_now(), owner_id),
        )

    def asset_succeeded(
        self,
        *,
        asset_id: int,
        lease_token: str,
        descriptor_id: int,
        final_relative_path: str,
        sha256: str,
        byte_count: int,
        stat_result: os.stat_result,
        portable_record: dict[str, Any] | None = None,
    ) -> None:
        path = Path(final_relative_path)
        if (
            asset_id < 1
            or descriptor_id < 1
            or not lease_token
            or path.is_absolute()
            or ".." in path.parts
            or not descriptor_x.SHA256_RE.fullmatch(sha256)
            or byte_count < 1
            or stat_result.st_size != byte_count
        ):
            raise ContextError("asset completion evidence is invalid")
        prepared = _prepare_portable_asset_record(
            portable_record,
            final_relative_path=final_relative_path,
            sha256=sha256,
            byte_count=byte_count,
            stat_result=stat_result,
        )
        completed_at = iso_now()
        with transaction(self.connection):
            row = self.connection.execute(
                """SELECT owner_kind,owner_id,media_ordinal FROM asset_jobs
                     WHERE asset_id=? AND descriptor_id=? AND state='leased'
                       AND lease_token=?""",
                (asset_id, descriptor_id, lease_token),
            ).fetchone()
            if row is None:
                raise ContextError("asset completion lease is stale")
            if prepared is not None and (
                prepared["owner_kind"] != row["owner_kind"]
                or prepared["owner_id"] != row["owner_id"]
                or prepared["media_ordinal"] != row["media_ordinal"]
            ):
                raise ContextError("portable asset queue identity changed")
            cursor = self.connection.execute(
                """UPDATE asset_jobs SET state='captured',lease_token=NULL,
                       lease_started_at=NULL,next_attempt_at=0,
                       final_relative_path=?,final_sha256=?,final_bytes=?,
                       verified_device=?,verified_inode=?,verified_size=?,
                       verified_mtime_ns=?,last_error_class=NULL,
                       last_error_detail=NULL,completed_at=?,updated_at=?
                     WHERE asset_id=? AND descriptor_id=? AND state='leased'
                       AND lease_token=? AND EXISTS (
                           SELECT 1 FROM descriptor_generations d
                            WHERE d.descriptor_id=asset_jobs.descriptor_id
                              AND d.state='active'
                       )""",
                (
                    final_relative_path,
                    sha256,
                    byte_count,
                    stat_result.st_dev,
                    stat_result.st_ino,
                    stat_result.st_size,
                    stat_result.st_mtime_ns,
                    completed_at,
                    completed_at,
                    asset_id,
                    descriptor_id,
                    lease_token,
                ),
            )
            if cursor.rowcount != 1:
                raise ContextError("asset completion descriptor changed")
            if prepared is not None:
                generation = self.advance_archive_generation(
                    ("media", "context_status"), observed_at=completed_at
                )
                _upsert_portable_asset(
                    self.connection,
                    prepared=prepared,
                    asset_id=asset_id,
                    generation=generation,
                    captured_at=completed_at,
                )
            self._update_post_asset_rollup(row["owner_kind"], row["owner_id"])

    def asset_failed(
        self,
        *,
        asset_id: int,
        lease_token: str,
        descriptor_id: int,
        state: str,
        error_class: str,
        detail: str,
        next_attempt_at: float = 0,
        count_attempt: bool = True,
    ) -> None:
        if state not in {
            "retryable",
            "needs_refresh",
            "unavailable",
            "manual_review",
        }:
            raise ContextError("asset failure state is invalid")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", error_class):
            raise ContextError("asset error class is invalid")
        observed_at = iso_now()
        with transaction(self.connection):
            row = self.connection.execute(
                """SELECT owner_kind,owner_id FROM asset_jobs
                     WHERE asset_id=? AND descriptor_id=? AND state='leased'
                       AND lease_token=?""",
                (asset_id, descriptor_id, lease_token),
            ).fetchone()
            if row is None:
                raise ContextError("asset failure lease is stale")
            cursor = self.connection.execute(
                """UPDATE asset_jobs SET state=?,lease_token=NULL,
                       lease_started_at=NULL,next_attempt_at=?,
                       attempts=MAX(0,attempts-?),last_error_class=?,
                       last_error_detail=?,updated_at=?
                     WHERE asset_id=? AND descriptor_id=? AND state='leased'
                       AND lease_token=?""",
                (
                    state,
                    max(0.0, next_attempt_at),
                    int(not count_attempt),
                    error_class,
                    safe_detail(detail),
                    observed_at,
                    asset_id,
                    descriptor_id,
                    lease_token,
                ),
            )
            if cursor.rowcount != 1:
                raise ContextError("asset failure changed during guarded update")
            self._update_post_asset_rollup(row["owner_kind"], row["owner_id"])

    def asset_availability(self, *, now: float) -> dict[str, Any]:
        row = self.connection.execute(
            """SELECT COUNT(*) AS total,
                      SUM(state IN ('pending','retryable')
                          AND next_attempt_at <= ?) AS ready,
                      MIN(CASE WHEN state='retryable' THEN next_attempt_at END)
                          AS next_eligible_at,
                      SUM(state='needs_refresh') AS needs_refresh,
                      SUM(state='manual_review') AS manual_review
                 FROM asset_jobs
                WHERE state NOT IN ('captured','unavailable')""",
            (now,),
        ).fetchone()
        return {
            "total": int(row["total"] or 0),
            "ready": int(row["ready"] or 0),
            "next_eligible_at": (
                float(row["next_eligible_at"])
                if row["next_eligible_at"] is not None
                else None
            ),
            "needs_refresh": int(row["needs_refresh"] or 0),
            "manual_review": int(row["manual_review"] or 0),
        }

    def authentication_stop(self) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT auth_stop_class,auth_stop_at FROM pacing WHERE singleton=1"
        ).fetchone()
        if row is None or row["auth_stop_class"] is None:
            return None
        return {
            "error_class": str(row["auth_stop_class"]),
            "stopped_at": float(row["auth_stop_at"] or 0),
        }

    def require_authentication_clear(self) -> None:
        stopped = self.authentication_stop()
        if stopped is not None:
            raise ContextAuthenticationError(
                "X network work is stopped by durable authentication evidence; "
                "credentials require explicit operator inspection"
            )

    def record_authentication_stop(self, error_class: str, *, now: float) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", error_class) or now < 0:
            raise ContextError("authentication stop evidence is invalid")
        with transaction(self.connection):
            self.connection.execute(
                """UPDATE pacing SET auth_stop_class=?,auth_stop_at=?,updated_at=?
                     WHERE singleton=1""",
                (error_class, now, iso_now()),
            )

    def clear_authentication_stop(self) -> bool:
        """Explicit operator action; normal workers never clear auth evidence."""
        with transaction(self.connection):
            cursor = self.connection.execute(
                """UPDATE pacing SET auth_stop_class=NULL,auth_stop_at=NULL,
                       updated_at=? WHERE singleton=1
                       AND auth_stop_class IS NOT NULL""",
                (iso_now(),),
            )
        return cursor.rowcount == 1

    def assets_needing_refresh(self) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """SELECT a.*,d.source_operation,d.media_type,d.extension,
                          d.private_url,d.descriptor_sha256,d.captured_at,
                          d.posted_at,d.original_posted_at,d.author_id,
                          d.author_handle,d.conversation_id,d.reply_id,d.retweet_id
                     FROM asset_jobs a
                     LEFT JOIN descriptor_generations d
                       ON d.descriptor_id=a.descriptor_id
                    WHERE a.state='needs_refresh'
                    ORDER BY a.owner_kind,a.owner_id,a.media_ordinal"""
            )
        ]

    def reconcile_asset_succeeded(
        self,
        *,
        asset_id: int,
        final_relative_path: str,
        sha256: str,
        byte_count: int,
        stat_result: os.stat_result,
        portable_record: dict[str, Any] | None = None,
    ) -> None:
        path = Path(final_relative_path)
        if (
            asset_id < 1
            or path.is_absolute()
            or ".." in path.parts
            or not descriptor_x.SHA256_RE.fullmatch(sha256)
            or byte_count < 1
            or stat_result.st_size != byte_count
        ):
            raise ContextError("local asset reconciliation evidence is invalid")
        prepared = _prepare_portable_asset_record(
            portable_record,
            final_relative_path=final_relative_path,
            sha256=sha256,
            byte_count=byte_count,
            stat_result=stat_result,
        )
        completed_at = iso_now()
        with transaction(self.connection):
            row = self.connection.execute(
                """SELECT owner_kind,owner_id,media_ordinal,descriptor_id
                     FROM asset_jobs WHERE asset_id=? AND state='needs_refresh'""",
                (asset_id,),
            ).fetchone()
            if row is None:
                raise ContextError("local asset reconciliation state changed")
            if prepared is not None and (
                prepared["owner_kind"] != row["owner_kind"]
                or prepared["owner_id"] != row["owner_id"]
                or prepared["media_ordinal"] != row["media_ordinal"]
            ):
                raise ContextError("portable asset queue identity changed")
            cursor = self.connection.execute(
                """UPDATE asset_jobs SET state='captured',
                       compatibility_job=CASE WHEN descriptor_id IS NULL THEN 1
                                              ELSE compatibility_job END,
                       expected_relative_path=?,final_relative_path=?,
                       final_sha256=?,final_bytes=?,verified_device=?,
                       verified_inode=?,verified_size=?,verified_mtime_ns=?,
                       next_attempt_at=0,last_error_class=NULL,
                       last_error_detail=NULL,completed_at=?,updated_at=?
                     WHERE asset_id=? AND state='needs_refresh'""",
                (
                    final_relative_path,
                    final_relative_path,
                    sha256,
                    byte_count,
                    stat_result.st_dev,
                    stat_result.st_ino,
                    stat_result.st_size,
                    stat_result.st_mtime_ns,
                    completed_at,
                    completed_at,
                    asset_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ContextError("local asset reconciliation commit changed")
            if prepared is not None:
                generation = self.advance_archive_generation(
                    ("media", "context_status"), observed_at=completed_at
                )
                _upsert_portable_asset(
                    self.connection,
                    prepared=prepared,
                    asset_id=asset_id,
                    generation=generation,
                    captured_at=completed_at,
                )
            self._update_post_asset_rollup(row["owner_kind"], row["owner_id"])

    @staticmethod
    def _rejected_descriptor_state(detail: str | None) -> str:
        return (
            "unavailable"
            if detail and re.search(r"status=(?:404|410)(?:\D|$)", detail)
            else "manual_review"
        )

    def prepare_descriptor_refreshes(self) -> dict[str, int]:
        counts = {
            "created": 0,
            "already_queued": 0,
            "terminalized": 0,
            "profile_terminalized": 0,
        }
        with transaction(self.connection):
            rows = list(
                self.connection.execute(
                    """SELECT asset_id,owner_kind,owner_id,descriptor_id,
                              compatibility_job,last_error_detail,next_attempt_at
                         FROM asset_jobs WHERE state='needs_refresh'
                        ORDER BY owner_kind,owner_id,media_ordinal"""
                )
            )
            grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}
            for row in rows:
                grouped.setdefault(
                    (str(row["owner_kind"]), str(row["owner_id"])), []
                ).append(row)
            for (owner_kind, owner_id), assets in grouped.items():
                if owner_kind != "post":
                    for asset in assets:
                        state = self._rejected_descriptor_state(
                            asset["last_error_detail"]
                        )
                        self.connection.execute(
                            """UPDATE asset_jobs SET state=?,next_attempt_at=0,
                                   last_error_class='profile_descriptor_rejected',
                                   last_error_detail=NULL,updated_at=?
                                 WHERE asset_id=? AND state='needs_refresh'""",
                            (state, iso_now(), asset["asset_id"]),
                        )
                        counts["profile_terminalized"] += 1
                    continue
                previous = self.connection.execute(
                    """SELECT * FROM descriptor_refresh_jobs
                         WHERE owner_kind='post' AND owner_id=?
                         ORDER BY generation DESC LIMIT 1""",
                    (owner_id,),
                ).fetchone()
                if previous is None:
                    reason = (
                        "compatibility"
                        if any(asset["compatibility_job"] for asset in assets)
                        else (
                            "descriptor_missing"
                            if any(asset["descriptor_id"] is None for asset in assets)
                            else "descriptor_stale"
                        )
                    )
                    observed_at = iso_now()
                    next_attempt_at = min(
                        float(asset["next_attempt_at"] or 0) for asset in assets
                    )
                    self.connection.execute(
                        """INSERT INTO descriptor_refresh_jobs(
                               owner_kind,owner_id,generation,state,reason,
                               next_attempt_at,created_at,updated_at
                           ) VALUES ('post',?,1,'pending',?,?,?,?)""",
                        (
                            owner_id,
                            reason,
                            max(0.0, next_attempt_at),
                            observed_at,
                            observed_at,
                        ),
                    )
                    counts["created"] += 1
                    continue
                if previous["state"] in {"pending", "retryable", "leased"}:
                    counts["already_queued"] += 1
                    continue
                for asset in assets:
                    if previous["state"] == "complete":
                        state = self._rejected_descriptor_state(
                            asset["last_error_detail"]
                        )
                        error_class = "descriptor_rejected_after_refresh"
                    else:
                        state = str(previous["state"])
                        error_class = str(
                            previous["last_error_class"] or "refresh_terminal"
                        )
                    self.connection.execute(
                        """UPDATE asset_jobs SET state=?,next_attempt_at=0,
                               last_error_class=?,last_error_detail=NULL,updated_at=?
                             WHERE asset_id=? AND state='needs_refresh'""",
                        (state, error_class, iso_now(), asset["asset_id"]),
                    )
                    counts["terminalized"] += 1
                self._update_post_asset_rollup(owner_kind, owner_id)
        return counts

    def reclaim_stale_refresh_leases(
        self, *, now: float, lease_seconds: float
    ) -> int:
        if lease_seconds <= 0:
            raise ContextError("refresh lease duration must be positive")
        with transaction(self.connection):
            cursor = self.connection.execute(
                """UPDATE descriptor_refresh_jobs SET state='retryable',
                       lease_token=NULL,lease_started_at=NULL,next_attempt_at=?,
                       last_error_class='stale_refresh_lease',
                       last_error_detail=NULL,updated_at=?
                     WHERE state='leased' AND lease_started_at < ?""",
                (now, iso_now(), now - lease_seconds),
            )
        return cursor.rowcount

    def claim_descriptor_refresh(
        self, *, now: float, lease_seconds: float
    ) -> dict[str, Any] | None:
        self.reclaim_stale_refresh_leases(now=now, lease_seconds=lease_seconds)
        with transaction(self.connection):
            row = self.connection.execute(REFRESH_CLAIM_SQL, (now,)).fetchone()
            if row is None:
                return None
            self.require_authentication_clear()
            token = secrets.token_hex(16)
            cursor = self.connection.execute(
                """UPDATE descriptor_refresh_jobs SET state='leased',
                       lease_token=?,lease_started_at=?,attempts=attempts+1,
                       updated_at=? WHERE refresh_id=?
                       AND state IN ('pending','retryable')""",
                (token, now, iso_now(), row["refresh_id"]),
            )
            if cursor.rowcount != 1:
                raise ContextError("refresh claim changed during guarded update")
            claimed = dict(row)
            claimed["lease_token"] = token
            claimed["lease_started_at"] = now
            claimed["attempts"] = int(row["attempts"]) + 1
            return claimed

    def refresh_destination_scope(self, owner_id: str) -> str:
        rows = list(
            self.connection.execute(
                """SELECT destination_scope,expected_relative_path
                     FROM asset_jobs WHERE owner_kind='post' AND owner_id=?
                       AND state='needs_refresh'""",
                (owner_id,),
            )
        )
        scopes: set[str] = set()
        for row in rows:
            scope = str(row["destination_scope"] or "unknown")
            if scope == "unknown" and row["expected_relative_path"]:
                parts = Path(str(row["expected_relative_path"])).parts
                try:
                    media_index = parts.index("media")
                except ValueError:
                    scope = "unknown"
                else:
                    scope = (
                        "context"
                        if "context" in parts[media_index + 1 :]
                        else "main"
                    )
            if scope in {"main", "context"}:
                scopes.add(scope)
        return scopes.pop() if len(scopes) == 1 else "unknown"

    def descriptor_refresh_failed(
        self,
        *,
        refresh_id: int,
        lease_token: str,
        state: str,
        error_class: str,
        next_attempt_at: float = 0,
        count_attempt: bool = True,
    ) -> None:
        if state not in {"retryable", "unavailable", "manual_review"}:
            raise ContextError("refresh failure state is invalid")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", error_class):
            raise ContextError("refresh error class is invalid")
        observed_at = iso_now()
        with transaction(self.connection):
            row = self.connection.execute(
                """SELECT owner_kind,owner_id FROM descriptor_refresh_jobs
                     WHERE refresh_id=? AND state='leased' AND lease_token=?""",
                (refresh_id, lease_token),
            ).fetchone()
            if row is None:
                raise ContextError("refresh failure lease is stale")
            cursor = self.connection.execute(
                """UPDATE descriptor_refresh_jobs SET state=?,lease_token=NULL,
                       lease_started_at=NULL,next_attempt_at=?,
                       attempts=MAX(0,attempts-?),last_error_class=?,
                       last_error_detail=NULL,completed_at=?,updated_at=?
                     WHERE refresh_id=? AND state='leased' AND lease_token=?""",
                (
                    state,
                    max(0.0, next_attempt_at),
                    int(not count_attempt),
                    error_class,
                    observed_at if state != "retryable" else None,
                    observed_at,
                    refresh_id,
                    lease_token,
                ),
            )
            if cursor.rowcount != 1:
                raise ContextError("refresh failure changed during guarded update")
            if state in {"unavailable", "manual_review"}:
                self.connection.execute(
                    """UPDATE asset_jobs SET state=?,next_attempt_at=0,
                           last_error_class=?,last_error_detail=NULL,updated_at=?
                         WHERE owner_kind=? AND owner_id=?
                           AND state='needs_refresh'""",
                    (
                        state,
                        error_class,
                        observed_at,
                        row["owner_kind"],
                        row["owner_id"],
                    ),
                )
                self._update_post_asset_rollup(
                    row["owner_kind"], row["owner_id"]
                )

    def descriptor_refresh_authentication_stopped(
        self,
        *,
        refresh_id: int,
        lease_token: str,
        error_class: str,
        now: float,
    ) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", error_class) or now < 0:
            raise ContextError("refresh authentication evidence is invalid")
        observed_at = iso_now()
        with transaction(self.connection):
            cursor = self.connection.execute(
                """UPDATE descriptor_refresh_jobs SET state='retryable',
                       lease_token=NULL,lease_started_at=NULL,next_attempt_at=0,
                       attempts=MAX(0,attempts-1),last_error_class=?,
                       last_error_detail=NULL,updated_at=?
                     WHERE refresh_id=? AND state='leased' AND lease_token=?""",
                (error_class, observed_at, refresh_id, lease_token),
            )
            if cursor.rowcount != 1:
                raise ContextError("refresh authentication lease is stale")
            self.connection.execute(
                """UPDATE pacing SET auth_stop_class=?,auth_stop_at=?,updated_at=?
                     WHERE singleton=1""",
                (error_class, now, observed_at),
            )

    def descriptor_refresh_succeeded(
        self,
        *,
        refresh_id: int,
        lease_token: str,
        metadata: dict[str, Any],
        descriptor_batches: Iterable[descriptor_x.DescriptorBatch],
    ) -> dict[str, Any]:
        post_id = id_string(metadata.get("tweet_id"))
        batches = tuple(descriptor_batches)
        if post_id is None or any(
            batch.source_kind != "exact_refresh"
            or batch.source_operation != "exact_refresh"
            for batch in batches
        ):
            raise ContextError("refresh result provenance is invalid")
        with transaction(self.connection):
            refresh = self.connection.execute(
                """SELECT owner_kind,owner_id FROM descriptor_refresh_jobs
                     WHERE refresh_id=? AND state='leased' AND lease_token=?""",
                (refresh_id, lease_token),
            ).fetchone()
            if (
                refresh is None
                or refresh["owner_kind"] != "post"
                or refresh["owner_id"] != post_id
            ):
                raise ContextError("refresh completion lease or owner changed")
            summary = self.persist_descriptor_batches(batches, (metadata,))
            count = metadata.get("count")
            count_valid = (
                isinstance(count, int)
                and not isinstance(count, bool)
                and count >= 0
            )
            if not count_valid:
                self.connection.execute(
                    """UPDATE asset_jobs SET state='manual_review',
                           next_attempt_at=0,
                           last_error_class='invalid_media_count_after_refresh',
                           last_error_detail=NULL,updated_at=?
                         WHERE owner_kind='post' AND owner_id=?
                           AND state IN ('pending','retryable','needs_refresh')""",
                    (iso_now(), post_id),
                )
            unresolved = list(
                self.connection.execute(
                    """SELECT asset_id,media_ordinal FROM asset_jobs
                         WHERE owner_kind='post' AND owner_id=?
                           AND state='needs_refresh'""",
                    (post_id,),
                )
            )
            observed_at = iso_now()
            for asset in unresolved:
                absent = count_valid and int(asset["media_ordinal"]) > int(count)
                state = "unavailable" if absent else "manual_review"
                error_class = (
                    "media_absent_after_refresh"
                    if absent
                    else "descriptor_missing_after_refresh"
                )
                self.connection.execute(
                    """UPDATE asset_jobs SET state=?,next_attempt_at=0,
                           last_error_class=?,last_error_detail=NULL,updated_at=?
                         WHERE asset_id=? AND state='needs_refresh'""",
                    (state, error_class, observed_at, asset["asset_id"]),
                )
            states = [
                str(row[0])
                for row in self.connection.execute(
                    """SELECT state FROM asset_jobs
                         WHERE owner_kind='post' AND owner_id=?""",
                    (post_id,),
                )
            ]
            if not states or "manual_review" in states:
                refresh_state = "manual_review"
                error_class = "descriptor_missing_after_refresh"
            elif all(state == "unavailable" for state in states):
                refresh_state = "unavailable"
                error_class = "media_absent_after_refresh"
            else:
                refresh_state = "complete"
                error_class = None
            cursor = self.connection.execute(
                """UPDATE descriptor_refresh_jobs SET state=?,lease_token=NULL,
                       lease_started_at=NULL,next_attempt_at=0,
                       last_error_class=?,last_error_detail=NULL,
                       completed_at=?,updated_at=?
                     WHERE refresh_id=? AND state='leased' AND lease_token=?""",
                (
                    refresh_state,
                    error_class,
                    observed_at,
                    observed_at,
                    refresh_id,
                    lease_token,
                ),
            )
            if cursor.rowcount != 1:
                raise ContextError("refresh completion changed during commit")
            self._update_post_asset_rollup("post", post_id)
            summary = dict(summary)
            summary["refresh_state"] = refresh_state
            summary["unresolved_terminalized"] = len(unresolved)
            return summary

    def enqueue_operator_refresh(self, post_id: str) -> int:
        normalized = id_string(post_id)
        if normalized is None:
            raise ContextError("operator refresh post ID is invalid")
        with transaction(self.connection):
            active = self.connection.execute(
                """SELECT 1 FROM descriptor_refresh_jobs
                     WHERE owner_kind='post' AND owner_id=?
                       AND state IN ('pending','retryable','leased')""",
                (normalized,),
            ).fetchone()
            if active is not None:
                raise ContextError("post already has actionable refresh work")
            cursor = self.connection.execute(
                """UPDATE asset_jobs SET state='needs_refresh',attempts=0,
                       next_attempt_at=0,lease_token=NULL,lease_started_at=NULL,
                       last_error_class=NULL,last_error_detail=NULL,updated_at=?
                     WHERE owner_kind='post' AND owner_id=?
                       AND state IN ('needs_refresh','unavailable','manual_review')""",
                (iso_now(), normalized),
            )
            if cursor.rowcount < 1:
                raise ContextError("post has no failed media assets to repair")
            generation = int(
                self.connection.execute(
                    """SELECT COALESCE(MAX(generation),0)+1
                         FROM descriptor_refresh_jobs
                        WHERE owner_kind='post' AND owner_id=?""",
                    (normalized,),
                ).fetchone()[0]
            )
            observed_at = iso_now()
            inserted = self.connection.execute(
                """INSERT INTO descriptor_refresh_jobs(
                       owner_kind,owner_id,generation,state,reason,
                       created_at,updated_at
                   ) VALUES ('post',?,?,'pending','operator_repair',?,?)""",
                (normalized, generation, observed_at, observed_at),
            )
            return int(inserted.lastrowid)

    def descriptor_refresh_quality(self) -> dict[str, Any]:
        post_owners = int(
            self.connection.execute(
                """SELECT COUNT(DISTINCT owner_id) FROM asset_jobs
                     WHERE owner_kind='post'"""
            ).fetchone()[0]
        )
        automatic = int(
            self.connection.execute(
                """SELECT COUNT(DISTINCT owner_id)
                     FROM descriptor_refresh_jobs
                    WHERE owner_kind='post' AND reason<>'operator_repair'"""
            ).fetchone()[0]
        )
        ratio = automatic / post_owners if post_owners else 0.0
        return {
            "post_owners": post_owners,
            "automatic_refresh_owners": automatic,
            "ratio": ratio,
            "threshold": 0.02,
            "alert": post_owners >= 100 and ratio > 0.02,
        }

    def upsert_target(
        self,
        post_id: str,
        *,
        conversation_id: str | None,
        depth: int,
        observed_at: str,
    ) -> None:
        self.connection.execute(
            """INSERT INTO targets(
                   post_id, conversation_id, depth_min, discovered_at, updated_at
               ) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(post_id) DO UPDATE SET
                   conversation_id=COALESCE(targets.conversation_id,
                                             excluded.conversation_id),
                   depth_min=MIN(targets.depth_min, excluded.depth_min),
                   updated_at=excluded.updated_at""",
            (post_id, conversation_id, depth, observed_at, observed_at),
        )

    def _would_cycle(self, child_id: str, parent_id: str) -> bool:
        current = parent_id
        seen: set[str] = set()
        while current:
            if current == child_id or current in seen:
                return True
            seen.add(current)
            row = self.connection.execute(
                "SELECT parent_id FROM reply_edges WHERE child_id=?", (current,)
            ).fetchone()
            if not row:
                return False
            current = row[0]
        return False

    def add_edge(
        self,
        child_id: str,
        parent_id: str,
        *,
        conversation_id: str | None,
        depth: int,
        run_id: str | None,
        observed_at: str,
        max_depth: int,
    ) -> bool:
        if not child_id or not parent_id:
            return False
        with savepoint(self.connection, "add_reply_edge"):
            self.upsert_target(
                parent_id,
                conversation_id=conversation_id,
                depth=depth,
                observed_at=observed_at,
            )
            cycle = self._would_cycle(child_id, parent_id)
            depth_exceeded = depth > max_depth
            previous = self.connection.execute(
                "SELECT parent_id FROM reply_edges WHERE child_id=?", (child_id,)
            ).fetchone()
            if previous and previous[0] != parent_id:
                raise ContextError(
                    f"conflicting parents for {child_id}: "
                    f"{previous[0]} and {parent_id}"
                )
            self.connection.execute(
                """INSERT INTO reply_edges(
                       child_id,parent_id,conversation_id,depth,
                       discovered_run_id,discovered_at,cycle_detected
                   ) VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(child_id) DO UPDATE SET
                       conversation_id=COALESCE(reply_edges.conversation_id,
                                                 excluded.conversation_id),
                       depth=MIN(reply_edges.depth, excluded.depth),
                       cycle_detected=MAX(reply_edges.cycle_detected,
                                          excluded.cycle_detected)""",
                (
                    child_id,
                    parent_id,
                    conversation_id,
                    depth,
                    run_id,
                    observed_at,
                    int(cycle),
                ),
            )
            if cycle or depth_exceeded:
                self.connection.execute(
                    """UPDATE targets SET state='manual_review',
                           lease_started_at=NULL,lease_token=NULL,
                           last_error_class=?, updated_at=?
                       WHERE post_id=?
                         AND state IN ('pending','retryable','leased')""",
                    (
                        "cycle" if cycle else "max_depth",
                        observed_at,
                        parent_id,
                    ),
                )
        return previous is None

    def capture(
        self,
        post_id: str,
        metadata: dict[str, Any],
        *,
        source_kind: str,
        target_user_id: str,
        max_depth: int,
        descriptor_batches: Iterable[descriptor_x.DescriptorBatch] | None = None,
    ) -> str | None:
        with transaction(self.connection):
            parent_id = self._capture_record(
                post_id,
                metadata,
                source_kind=source_kind,
                target_user_id=target_user_id,
                max_depth=max_depth,
            )
            if descriptor_batches is not None:
                self.persist_descriptor_batches(
                    descriptor_batches, (metadata,)
                )
            return parent_id

    def _capture_record(
        self,
        post_id: str,
        metadata: dict[str, Any],
        *,
        source_kind: str,
        target_user_id: str,
        max_depth: int,
    ) -> str | None:
        actual = id_string(metadata.get("tweet_id"))
        if actual != post_id:
            raise ContextError(f"expected post {post_id}, received {actual or 'none'}")
        captured_at = str(metadata.get("archived_at") or iso_now())
        raw_json = json.dumps(metadata, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha256(raw_json.encode()).hexdigest()
        author_id = id_string((metadata.get("author") or {}).get("id"))
        parent_id = id_string(metadata.get("reply_id"))
        conversation_id = id_string(metadata.get("conversation_id"))
        media_count = int(metadata.get("count") or 0)
        enqueue_media = media_count > 0 and source_kind.startswith("x:")
        row = self.connection.execute(
            "SELECT depth_min FROM targets WHERE post_id=?", (post_id,)
        ).fetchone()
        depth = int(row[0]) if row else 0
        self.upsert_target(
            post_id,
            conversation_id=conversation_id,
            depth=depth,
            observed_at=captured_at,
        )
        self.connection.execute(
            """INSERT INTO observations(
                   post_id,captured_at,source_kind,raw_json,sha256
               ) VALUES (?,?,?,?,?)
               ON CONFLICT(post_id) DO UPDATE SET
                   captured_at=excluded.captured_at,
                   source_kind=excluded.source_kind,
                   raw_json=excluded.raw_json,
                   sha256=excluded.sha256,
                   capture_count=observations.capture_count+1""",
            (post_id, captured_at, source_kind, raw_json, digest),
        )
        self.connection.execute(
            """UPDATE targets SET state='captured', lease_started_at=NULL,
                   lease_token=NULL,
                   next_attempt_at=0, author_id=?, updated_at=?,
                   last_error_class=NULL, last_error_detail=NULL,
                   media_state=CASE
                       WHEN ? > 0 AND media_state='none' THEN 'pending'
                       ELSE media_state END
               WHERE post_id=?""",
            (author_id, captured_at, int(enqueue_media), post_id),
        )
        if parent_id:
            self.add_edge(
                post_id,
                parent_id,
                conversation_id=conversation_id,
                depth=depth + 1,
                run_id=f"context:{source_kind}",
                observed_at=captured_at,
                max_depth=max_depth,
            )
        return parent_id

    def capture_conversation_response(
        self,
        focal_id: str,
        records: tuple[dict[str, Any], ...],
        *,
        target_user_id: str,
        max_depth: int,
        descriptor_batches: Iterable[descriptor_x.DescriptorBatch] | None = None,
    ) -> tuple[list[str], str | None]:
        """Atomically retain only queued targets and their verified ancestors."""
        by_id: dict[str, dict[str, Any]] = {}
        for record in records:
            post_id = id_string(record.get("tweet_id"))
            if not post_id or post_id in by_id:
                raise ContextError("invalid or duplicate conversation response post")
            by_id[post_id] = record
        focal = by_id.get(focal_id)
        if focal is None:
            return [], None

        row = self.connection.execute(
            "SELECT conversation_id FROM targets WHERE post_id=?", (focal_id,)
        ).fetchone()
        queued_conversation = id_string(row[0]) if row else None
        focal_conversation = id_string(focal.get("conversation_id"))
        conversation_mismatch = bool(
            queued_conversation
            and focal_conversation
            and queued_conversation != focal_conversation
        )
        conversation_id = focal_conversation or queued_conversation

        placeholders = ",".join("?" for _ in by_id)
        queued = {
            str(target["post_id"]): str(target["state"])
            for target in self.connection.execute(
                f"""SELECT post_id,state FROM targets
                    WHERE post_id IN ({placeholders})""",
                tuple(by_id),
            )
        }
        actionable_states = {"pending", "retryable", "leased", "manual_review"}
        seeds = [focal_id]
        if not conversation_mismatch:
            seeds.extend(
                post_id
                for post_id in by_id
                if post_id != focal_id and queued.get(post_id) in actionable_states
            )

        selected: list[str] = []
        selected_set: set[str] = set()
        for seed in seeds:
            current = seed
            while current and current not in selected_set:
                record = by_id.get(current)
                if record is None:
                    break
                record_conversation = id_string(record.get("conversation_id"))
                if (
                    conversation_id
                    and record_conversation
                    and record_conversation != conversation_id
                ):
                    break
                if queued.get(current) == "captured":
                    break
                selected.append(current)
                selected_set.add(current)
                if conversation_mismatch:
                    # Some old X records claim the reply itself as the
                    # conversation root even though its reply_id points to an
                    # older post.  The focal record remains useful, but this
                    # response is not safe evidence for opportunistic
                    # conversation harvesting.
                    break
                current = id_string(record.get("reply_id"))

        parents: dict[str, str | None] = {}
        with transaction(self.connection):
            if conversation_mismatch and focal_conversation:
                self.connection.execute(
                    """UPDATE targets SET conversation_id=?,updated_at=?
                       WHERE post_id=?""",
                    (focal_conversation, iso_now(), focal_id),
                )
            for post_id in selected:
                parents[post_id] = self._capture_record(
                    post_id,
                    by_id[post_id],
                    source_kind=(
                        "x:focal" if post_id == focal_id else "x:conversation"
                    ),
                    target_user_id=target_user_id,
                    max_depth=max_depth,
                )
            if descriptor_batches is not None:
                self.persist_descriptor_batches(
                    descriptor_batches,
                    (by_id[post_id] for post_id in selected),
                )
        continuation = None
        for post_id in reversed(selected):
            parent_id = parents.get(post_id)
            if not parent_id or parent_id in selected_set:
                continue
            parent = self.connection.execute(
                "SELECT state FROM targets WHERE post_id=?", (parent_id,)
            ).fetchone()
            if parent and parent["state"] in {"pending", "retryable"}:
                continuation = parent_id
                break
        return selected, continuation

    def reclaim_stale(
        self, now: float, lease_seconds: float, *, media: bool = False
    ) -> int:
        cutoff = now - lease_seconds
        with transaction(self.connection):
            if media:
                cursor = self.connection.execute(
                    MEDIA_RECLAIM_SQL,
                    (now, iso_now(), cutoff),
                )
            else:
                cursor = self.connection.execute(
                    METADATA_RECLAIM_SQL,
                    (now, iso_now(), cutoff),
                )
        return cursor.rowcount

    def claim(
        self,
        *,
        now: float,
        lease_seconds: float,
        fairness_quantum: int,
        media: bool = False,
    ) -> sqlite3.Row | None:
        self.reclaim_stale(now, lease_seconds, media=media)
        with transaction(self.connection):
            if media:
                row = self.connection.execute(
                    MEDIA_CLAIM_SQL,
                    (now,),
                ).fetchone()
                if row:
                    self.connection.execute(
                        """UPDATE targets SET media_state='leased',
                               media_lease_started_at=?, media_lease_token=?,
                               media_attempts=media_attempts+1
                           WHERE post_id=?""",
                        (now, secrets.token_hex(16), row["post_id"]),
                    )
                return row

            active = self.connection.execute(
                "SELECT value FROM context_meta WHERE key='active_post_id'"
            ).fetchone()
            steps = self.connection.execute(
                "SELECT value FROM context_meta WHERE key='active_steps'"
            ).fetchone()
            active_id = active[0] if active else None
            active_steps = int(steps[0]) if steps else 0
            row = None
            if active_id and active_steps < fairness_quantum:
                row = self.connection.execute(
                    """SELECT * FROM targets WHERE post_id=?
                       AND state IN ('pending','retryable')
                       AND next_attempt_at <= ?""",
                    (active_id, now),
                ).fetchone()
            if row is None:
                row = self.connection.execute(
                    METADATA_CLAIM_SQL,
                    (now,),
                ).fetchone()
                active_steps = 0
            if row:
                self.connection.execute(
                    """UPDATE targets SET state='leased', lease_started_at=?,
                           lease_token=?, attempts=attempts+1, updated_at=?
                       WHERE post_id=?""",
                    (now, secrets.token_hex(16), iso_now(), row["post_id"]),
                )
                self._set_meta("active_post_id", row["post_id"])
                self._set_meta("active_steps", str(active_steps))
            return row

    def work_availability(
        self, *, now: float, lease_seconds: float, media: bool = False
    ) -> dict[str, Any]:
        """Describe remaining work when no target is immediately claimable."""
        ready = 0
        manual_review = 0
        next_at: float | None = None
        if media:
            rows = self.connection.execute(
                """SELECT media_state AS state,media_next_attempt_at AS eligible,
                          media_lease_started_at AS lease_started_at FROM targets
                   WHERE state='captured'
                     AND media_state IN
                         ('pending','retryable','leased','manual_review')"""
            )
        else:
            rows = self.connection.execute(
                """SELECT state,next_attempt_at AS eligible,lease_started_at
                     FROM targets
                    WHERE state IN
                        ('pending','retryable','leased','manual_review')"""
            )
        total = 0
        for row in rows:
            total += 1
            state = row["state"]
            if state == "manual_review":
                manual_review += 1
                continue
            if state == "pending" or (
                state == "retryable" and float(row["eligible"] or 0) <= now
            ):
                ready += 1
                continue
            eligible = (
                float(row["lease_started_at"] or now) + lease_seconds
                if state == "leased"
                else float(row["eligible"] or now)
            )
            next_at = eligible if next_at is None else min(next_at, eligible)
        return {
            "total": total,
            "ready": ready,
            "manual_review": manual_review,
            "next_eligible_at": next_at,
        }

    def _set_meta(self, key: str, value: str | None) -> None:
        if value is None:
            self.connection.execute("DELETE FROM context_meta WHERE key=?", (key,))
        else:
            self.connection.execute(
                """INSERT INTO context_meta(key,value) VALUES (?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (key, value),
            )

    def continue_chain(self, parent_id: str | None, fairness_quantum: int) -> None:
        with transaction(self.connection):
            steps_row = self.connection.execute(
                "SELECT value FROM context_meta WHERE key='active_steps'"
            ).fetchone()
            steps = (int(steps_row[0]) if steps_row else 0) + 1
            if not parent_id or steps >= fairness_quantum:
                self._set_meta("active_post_id", None)
                self._set_meta("active_steps", "0")
                return
            row = self.connection.execute(
                "SELECT state,next_attempt_at FROM targets WHERE post_id=?",
                (parent_id,),
            ).fetchone()
            if row and row["state"] in {"pending", "retryable"}:
                self._set_meta("active_post_id", parent_id)
                self._set_meta("active_steps", str(steps))
            else:
                self._set_meta("active_post_id", None)
                self._set_meta("active_steps", "0")

    def fail(
        self,
        post_id: str,
        *,
        error_class: str,
        detail: str,
        now: float,
        max_attempts: int,
        retry_delay: float,
        terminal: bool = False,
        media: bool = False,
    ) -> str:
        row = self.connection.execute(
            "SELECT attempts,media_attempts FROM targets WHERE post_id=?",
            (post_id,),
        ).fetchone()
        attempts = int(row["media_attempts" if media else "attempts"])
        if terminal:
            state = "unavailable"
        elif error_class == "interrupted":
            state = "retryable"
        elif attempts >= max_attempts:
            state = "manual_review"
        else:
            state = "retryable"
        if state == "retryable" and retry_delay:
            exponent = max(0, attempts - 1)
            backoff = min(retry_delay * (2 ** exponent), 86400.0)
            eligible_at = now + random.uniform(backoff * 0.9, backoff * 1.1)
        else:
            eligible_at = 0
        with transaction(self.connection):
            if media:
                self.connection.execute(
                    """UPDATE targets SET media_state=?,
                           media_lease_started_at=NULL,media_lease_token=NULL,
                           media_next_attempt_at=?, last_error_class=?,
                           last_error_detail=?, updated_at=? WHERE post_id=?""",
                    (
                        state,
                        eligible_at,
                        error_class,
                        safe_detail(detail),
                        iso_now(),
                        post_id,
                    ),
                )
            else:
                self.connection.execute(
                    """UPDATE targets SET state=?, lease_started_at=NULL,
                           lease_token=NULL,
                           next_attempt_at=?, last_error_class=?,
                           last_error_detail=?, unavailable_at=?, updated_at=?
                       WHERE post_id=?""",
                    (
                        state,
                        eligible_at,
                        error_class,
                        safe_detail(detail),
                        iso_now() if terminal else None,
                        iso_now(),
                        post_id,
                    ),
                )
            self._set_meta("active_post_id", None)
            self._set_meta("active_steps", "0")
        return state

    def reconcile_known_failures(self) -> dict[str, dict[str, int]]:
        """Repair classifications produced by older conservative pattern sets."""
        terminal_changes: list[tuple[str, str, str]] = []
        for row in self.connection.execute(
            """SELECT post_id,state,last_error_class,last_error_detail FROM targets
               WHERE state IN ('retryable','manual_review')
                 AND last_error_detail IS NOT NULL"""
        ):
            error_class, terminal, global_stop = classify_log(
                str(row["last_error_detail"])
            )
            if terminal and not global_stop:
                terminal_changes.append(
                    (str(row["post_id"]), str(row["state"]), error_class)
                )
        unavailable_counts: dict[str, int] = {}
        if not terminal_changes:
            return {"unavailable": unavailable_counts}
        changed_at = iso_now()
        with transaction(self.connection):
            for post_id, previous_state, error_class in terminal_changes:
                cursor = self.connection.execute(
                    """UPDATE targets SET state='unavailable',
                           next_attempt_at=0,lease_started_at=NULL,lease_token=NULL,
                           last_error_class=?,unavailable_at=COALESCE(
                               unavailable_at,?
                           ),updated_at=?
                       WHERE post_id=? AND state=?""",
                    (
                        error_class,
                        changed_at,
                        changed_at,
                        post_id,
                        previous_state,
                    ),
                )
                if cursor.rowcount:
                    unavailable_counts[error_class] = (
                        unavailable_counts.get(error_class, 0) + 1
                    )
            self._set_meta("active_post_id", None)
            self._set_meta("active_steps", "0")
        return {"unavailable": unavailable_counts}

    def media_succeeded(self, post_id: str) -> None:
        with transaction(self.connection):
            self.connection.execute(
                """UPDATE targets SET media_state='captured',
                       media_lease_started_at=NULL,media_lease_token=NULL,
                       media_next_attempt_at=0,
                       updated_at=? WHERE post_id=?""",
                (iso_now(), post_id),
            )

    def status(self, *, full: bool = False) -> dict[str, Any]:
        if not full:
            counters = {
                str(row[0]): int(row[1])
                for row in self.connection.execute(
                    "SELECT counter_name,value FROM progress_counters"
                )
            }
            states = {
                state: counters.get(f"targets_state_{state}", 0)
                for state in VALID_STATES
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
            pacing_row = self.connection.execute(
                "SELECT * FROM pacing WHERE singleton=1"
            ).fetchone()
            return {
                "schema_version": SCHEMA_VERSION,
                "targets": counters.get("targets_total", 0),
                "states": states,
                "edges": counters.get("reply_edges_total", 0),
                "conversations": sum(closure.values()),
                "cycles": counters.get("reply_edges_cycles", 0),
                "depth_distribution": {},
                "conversation_closure": closure,
                "media": media,
                "pacing": dict(pacing_row) if pacing_row is not None else {},
                "archive_posts": counters.get("archive_posts_total", 0),
                "archive_media_files": counters.get("archive_media_files", 0),
                "archive_media_bytes": counters.get("archive_media_bytes", 0),
                "integrity_errors": None,
                "integrity_checked": False,
            }
        states = {
            row[0]: row[1]
            for row in self.connection.execute(
                "SELECT state,COUNT(*) FROM targets GROUP BY state"
            )
        }
        media = {
            row[0]: row[1]
            for row in self.connection.execute(
                "SELECT media_state,COUNT(*) FROM targets GROUP BY media_state"
            )
        }
        pacing = dict(self.connection.execute("SELECT * FROM pacing").fetchone())
        edges = self.connection.execute("SELECT COUNT(*) FROM reply_edges").fetchone()[0]
        conversations = self.connection.execute(
            "SELECT COUNT(DISTINCT conversation_id) FROM reply_edges"
        ).fetchone()[0]
        cycles = self.connection.execute(
            "SELECT COUNT(*) FROM reply_edges WHERE cycle_detected=1"
        ).fetchone()[0]
        depth = {
            str(row[0]): row[1]
            for row in self.connection.execute(
                "SELECT depth_min,COUNT(*) FROM targets GROUP BY depth_min "
                "ORDER BY depth_min"
            )
        }
        closure = {
            "fully_captured": 0,
            "unavailable_boundary": 0,
            "retry_delayed": 0,
            "pending": 0,
            "manual_review": 0,
        }
        for row in self.connection.execute(
            """SELECT COALESCE(e.conversation_id,e.child_id) AS chain_id,
                      SUM(t.state='captured') AS captured,
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
            "schema_version": SCHEMA_VERSION,
            "targets": sum(states.values()),
            "states": states,
            "edges": edges,
            "conversations": conversations,
            "cycles": cycles,
            "depth_distribution": depth,
            "conversation_closure": closure,
            "media": media,
            "pacing": pacing,
            "integrity_errors": self.integrity_errors(),
        }


def user_paths(archive_root: Path, handle: str) -> tuple[Path, Path]:
    user_dir = archive_root / "users" / handle
    state_path = user_dir / "_state" / "state.json"
    if not user_dir.is_dir() or not state_path.is_file():
        raise ContextError(f"existing X archive not found for @{handle}: {user_dir}")
    return user_dir, user_dir / "_state" / "context.sqlite3"


def target_identity(user_dir: Path) -> tuple[str, str]:
    state = archive_x.load_json(user_dir / "_state" / "state.json", {})
    target_id = id_string(state.get("requested_user_id"))
    handle = str(state.get("canonical_handle") or state.get("requested_handle") or "")
    if not target_id or not handle:
        raise ContextError("archive state lacks stable target identity")
    return target_id, handle


@dataclass(frozen=True)
class SeedSource:
    path: Path
    relative_path: str
    sha256: str
    source_kind: str
    run_id: str


def _seed_source(
    user_dir: Path, path_value: Any, *, source_kind: str, run_id: str
) -> SeedSource:
    relative = Path(str(path_value or ""))
    if relative.is_absolute() or not relative.parts:
        raise ContextError("context seed source path is invalid")
    path = (user_dir / relative).resolve()
    runs_dir = (user_dir / "runs").resolve()
    if (
        not path.is_file()
        or runs_dir not in path.parents
        or path.name.endswith(".tmp")
    ):
        raise ContextError(f"canonical context seed source is missing: {relative}")
    return SeedSource(
        path=path,
        relative_path=str(path.relative_to(user_dir.resolve())),
        sha256=archive_x.sha256_file(path),
        source_kind=source_kind,
        run_id=run_id,
    )


def canonical_seed_sources(user_dir: Path) -> list[SeedSource]:
    by_path: dict[str, SeedSource] = {}
    for manifest_path in sorted((user_dir / "runs").glob("*/manifest.json")):
        manifest = archive_x.load_json(manifest_path, None)
        if not isinstance(manifest, dict) or manifest.get("status") == "running":
            continue
        run_id = str(manifest.get("run_id") or manifest_path.parent.name)
        candidates: list[SeedSource] = []
        if manifest.get("mode") == "legacy_backfill":
            for window in manifest.get("windows", ()):
                if not isinstance(window, dict) or not (
                    window.get("status") == "success"
                    and window.get("metadata_confirmed") is True
                    and window.get("state_committed") is True
                ):
                    continue
                candidates.append(
                    _seed_source(
                        user_dir,
                        window.get("canonical_raw_path"),
                        source_kind="legacy",
                        run_id=run_id,
                    )
                )
        elif isinstance(manifest.get("post_dataset"), dict):
            for endpoint in manifest.get("endpoints", ()):
                if not isinstance(endpoint, dict) or endpoint.get("endpoint") != "timeline":
                    continue
                candidates.append(
                    _seed_source(
                        user_dir,
                        endpoint.get("raw_path"),
                        source_kind="modern",
                        run_id=run_id,
                    )
                )
        for source in candidates:
            previous = by_path.get(source.relative_path)
            if previous is not None and previous != source:
                raise ContextError(
                    f"conflicting canonical context source: {source.relative_path}"
                )
            by_path[source.relative_path] = source
    return [by_path[key] for key in sorted(by_path)]


def timeline_raw_paths(user_dir: Path) -> list[Path]:
    """Compatibility view of manifest-authoritative modern raw sources."""
    return [
        source.path
        for source in canonical_seed_sources(user_dir)
        if source.source_kind == "modern"
    ]


def is_target_reply_candidate(metadata: dict[str, Any], target_id: str) -> bool:
    author_id = id_string((metadata.get("author") or {}).get("id"))
    raw_reply = metadata.get("reply_id")
    return bool(
        author_id == target_id
        and raw_reply not in (None, False, 0, "0", "")
        and not id_string(metadata.get("retweet_id"))
    )


def seed_context(
    user_dir: Path,
    db_path: Path,
    *,
    dry_run: bool,
    max_depth: int,
    raw_paths: list[Path] | None = None,
) -> dict[str, int]:
    target_id, _handle = target_identity(user_dir)
    if db_path.is_file() and raw_paths is None:
        uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        try:
            ready = connection.execute(
                "SELECT 1 FROM current_pointers "
                "WHERE pointer_name='local_history_reconciled'"
            ).fetchone()
            if ready is not None:
                source_count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM archive_sources "
                        "WHERE source_kind IN ('modern','legacy') "
                        "AND status='committed'"
                    ).fetchone()[0]
                )
                counters = {
                    str(row[0]): int(row[1])
                    for row in connection.execute(
                        "SELECT counter_name,value FROM progress_counters"
                    )
                }
                return {
                    "files": source_count,
                    "files_processed": 0,
                    "files_skipped": source_count,
                    "records": 0,
                    "reply_edges": counters.get("reply_edges_total", 0),
                    "unique_parents": 0,
                    "local_parents": 0,
                    "local_parent_candidates": 0,
                    "malformed": 0,
                }
        except sqlite3.Error:
            pass
        finally:
            connection.close()
    authoritative = canonical_seed_sources(user_dir)
    by_resolved = {source.path: source for source in authoritative}
    if raw_paths is None:
        sources = authoritative
    else:
        sources = []
        for path in sorted(raw_paths):
            resolved = path.resolve()
            source = by_resolved.get(resolved)
            if source is None:
                raise ContextError(
                    f"raw path is not a committed canonical source: {path}"
                )
            sources.append(source)
    stats = {
        "files": len(sources),
        "files_processed": 0,
        "files_skipped": 0,
        "records": 0,
        "reply_edges": 0,
        "unique_parents": 0,
        "local_parents": 0,
        "local_parent_candidates": 0,
        "malformed": 0,
    }
    edges: dict[str, tuple[str, str | None, str | None]] = {}
    parents: set[str] = set()
    local_post_ids: set[str] = set()
    for source in sources:
        for metadata in archive_x.iter_jsonl(source.path):
            stats["records"] += 1
            record_id = id_string(metadata.get("tweet_id"))
            author_id = id_string((metadata.get("author") or {}).get("id"))
            if record_id and author_id == target_id:
                local_post_ids.add(record_id)
            if not is_target_reply_candidate(metadata, target_id):
                continue
            child = id_string(metadata.get("tweet_id"))
            parent = id_string(metadata.get("reply_id"))
            if not child or not parent:
                stats["malformed"] += 1
                continue
            value = (
                parent,
                id_string(metadata.get("conversation_id")),
                source.run_id,
            )
            previous = edges.get(child)
            if previous and previous[0] != parent:
                raise ContextError(
                    f"conflicting timeline parents for {child}: {previous[0]} and {parent}"
                )
            edges[child] = value
            parents.add(parent)
    stats["reply_edges"] = len(edges)
    stats["unique_parents"] = len(parents)
    stats["local_parent_candidates"] = len(parents & local_post_ids)
    if dry_run:
        return stats

    observed_at = iso_now()
    with ContextDB(db_path) as context:
        context.bind_identity(target_id, _handle)
        for source in sources:
            previous = context.connection.execute(
                "SELECT sha256 FROM seed_sources WHERE relative_path=?",
                (source.relative_path,),
            ).fetchone()
            if previous is not None:
                if previous[0] != source.sha256:
                    raise ContextError(
                        "previously seeded canonical source changed: "
                        + source.relative_path
                    )
                stats["files_skipped"] += 1
                continue
            source_records = list(archive_x.iter_jsonl(source.path))
            source_edges = 0
            with transaction(context.connection):
                for metadata in source_records:
                    post_id = id_string(metadata.get("tweet_id"))
                    author_id = id_string((metadata.get("author") or {}).get("id"))
                    if post_id and author_id == target_id:
                        raw_json = json.dumps(
                            metadata, ensure_ascii=False, sort_keys=True
                        )
                        digest = hashlib.sha256(raw_json.encode()).hexdigest()
                        source_observed = str(
                            metadata.get("archived_at") or observed_at
                        )
                        context.connection.execute(
                            """INSERT INTO local_posts(
                                   post_id,raw_json,sha256,relative_path,
                                   source_kind,run_id,observed_at
                               ) VALUES (?,?,?,?,?,?,?)
                               ON CONFLICT(post_id) DO UPDATE SET
                                   raw_json=excluded.raw_json,
                                   sha256=excluded.sha256,
                                   relative_path=excluded.relative_path,
                                   source_kind=excluded.source_kind,
                                   run_id=excluded.run_id,
                                   observed_at=excluded.observed_at
                               WHERE excluded.observed_at >= local_posts.observed_at""",
                            (
                                post_id,
                                raw_json,
                                digest,
                                source.relative_path,
                                source.source_kind,
                                source.run_id,
                                source_observed,
                            ),
                        )
                    if not is_target_reply_candidate(metadata, target_id):
                        continue
                    child = id_string(metadata.get("tweet_id"))
                    parent = id_string(metadata.get("reply_id"))
                    if not child or not parent:
                        continue
                    if context.add_edge(
                        child,
                        parent,
                        conversation_id=id_string(metadata.get("conversation_id")),
                        depth=0,
                        run_id=source.run_id,
                        observed_at=observed_at,
                        max_depth=max_depth,
                    ):
                        source_edges += 1
                context.connection.execute(
                    """INSERT INTO seed_sources(
                           relative_path,sha256,source_kind,run_id,processed_at,
                           record_count,edge_count
                       ) VALUES (?,?,?,?,?,?,?)""",
                    (
                        source.relative_path,
                        source.sha256,
                        source.source_kind,
                        source.run_id,
                        observed_at,
                        len(source_records),
                        source_edges,
                    ),
                )
            stats["files_processed"] += 1

        local_candidates = list(
            context.connection.execute(
                """SELECT t.post_id,l.raw_json,l.source_kind,l.run_id
                     FROM targets t JOIN local_posts l ON l.post_id=t.post_id
                    WHERE t.state != 'captured' ORDER BY t.depth_min,t.post_id"""
            )
        )
        stats["local_parent_candidates"] = len(local_candidates)
        for row in local_candidates:
            context.capture(
                row["post_id"],
                json.loads(row["raw_json"]),
                source_kind=f"timeline:{row['source_kind']}:{row['run_id']}",
                target_user_id=target_id,
                max_depth=max_depth,
            )
            stats["local_parents"] += 1
    return stats


def build_context_config(
    *,
    handle: str,
    post_id: str,
    archive_root: Path,
    user_dir: Path,
    cookie_file: Path,
    work_dir: Path,
    media: bool,
    conversation: bool = True,
    operation_id: str | None = None,
    descriptor_source_kind: str = "context",
    descriptor_source_operation: str = "context",
    destination_scope: str = "context",
) -> tuple[dict[str, Any], Path, Path]:
    if destination_scope not in {"main", "context"}:
        raise ContextError("exact-post destination scope is invalid")
    operation_id = operation_id or f"context-{post_id}"
    raw_path = work_dir / f"{operation_id}.posts.jsonl.partial"
    descriptor_path = work_dir / f"{operation_id}.descriptors.jsonl.partial"
    config = archive_x.build_gallery_config(
        handle=handle,
        endpoint="reply-context",
        archive_root=archive_root,
        user_dir=user_dir,
        raw_partial=raw_path,
        cookie_file=cookie_file,
        archive_run_id=operation_id,
        archived_at=iso_now(),
        # Context pacing is reserved durably in SQLite immediately before
        # each request.  A second in-extractor delay only makes every short
        # process slower without adding safety.
        request_delay="0",
        download_delay="1-3",
        extractor_delay="0",
        include_reposts=True,
        checksums=media,
        cursor=None,
        descriptor_artifact=descriptor_path,
        descriptor_operation_id=operation_id,
        descriptor_source_kind=descriptor_source_kind,
        descriptor_source_operation=descriptor_source_operation,
    )
    twitter = config["extractor"]["twitter"]
    twitter.pop("timeline", None)
    if media:
        # Context paths intentionally duplicate neither the primary archive's
        # location nor its download ledger.  A metadata-only request must not
        # mark a future context-media download as already archived.
        twitter["archive"] = str(
            user_dir / "_state" / "context-downloads.sqlite3"
        )
    else:
        twitter.pop("archive", None)
        twitter.pop("archive-table", None)
    twitter.update(
        {
            "tweet-endpoint": "detail" if conversation and not media else "rest",
            "conversations": bool(conversation and not media),
            "expand": False,
            "showreplies": False,
            "quoted": False,
            "pinned": False,
            # A runner-side cursor guard turns TweetDetail into a single
            # response rather than an unbounded conversation pagination.
            "archive-conversation-pages": 1,
        }
    )
    if conversation and not media:
        twitter.pop("post-filter", None)
    else:
        twitter["post-filter"] = f"tweet_id == {post_id}"
    twitter["directory"] = ["users", handle, "media"]
    if destination_scope == "context":
        twitter["directory"].append("context")
    twitter["directory"].extend(("{date:%Y}", "{date:%m}"))
    if not media:
        twitter["postprocessors"] = [
            processor
            for processor in twitter["postprocessors"]
            if (
                processor.get("event") == "post"
                or processor.get("name") == descriptor_x.POSTPROCESSOR_NAME
            )
        ]
    return config, raw_path, descriptor_path


@dataclass
class FetchResult:
    status: int
    metadata: dict[str, Any] | None
    log: str
    interrupted: bool
    failed_downloads: list[dict[str, Any]]
    rate_reset: float | None
    records: tuple[dict[str, Any], ...] = ()
    descriptor_batches: tuple[descriptor_x.DescriptorBatch, ...] = ()
    request_telemetry: dict[str, Any] | None = None
    request_telemetry_error: str | None = None


def fetch_post(
    *,
    repo_dir: Path,
    archive_root: Path,
    user_dir: Path,
    handle: str,
    post_id: str,
    cookie_file: Path,
    media: bool,
    conversation: bool = True,
    descriptor_source_kind: str = "context",
    descriptor_source_operation: str = "context",
    destination_scope: str = "context",
    request_operation: str | None = None,
    request_delay: str = "4-8",
    runner: Any | None = None,
    control_lease_token: str | None = None,
) -> FetchResult:
    work_dir = user_dir / "_state" / "context-work"
    work_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(work_dir, 0o700)
    operation_id = (
        f"{descriptor_source_operation}-{post_id}-{secrets.token_hex(8)}"
    )
    config_path = work_dir / f"{operation_id}.gallery-dl.json"
    log_path = work_dir / f"{operation_id}.log"
    request_path = work_dir / f"{operation_id}.requests.json"
    config, raw_path, descriptor_partial = build_context_config(
        handle=handle,
        post_id=post_id,
        archive_root=archive_root,
        user_dir=user_dir,
        cookie_file=cookie_file,
        work_dir=work_dir,
        media=media,
        conversation=conversation,
        operation_id=operation_id,
        descriptor_source_kind=descriptor_source_kind,
        descriptor_source_operation=descriptor_source_operation,
        destination_scope=destination_scope,
    )
    for path in (
        config_path,
        log_path,
        raw_path,
        request_path,
        descriptor_partial,
    ):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    descriptor_x.prepare_artifact(descriptor_partial)
    archive_x.atomic_write_json(config_path, config)
    request_operation = request_operation or (
        "context_media"
        if media
        else ("context_metadata" if conversation else "context_exact")
    )
    scheduler_options = archive_x.x_scheduler_options(
        user_dir, target_identity(user_dir)[0], request_delay
    )
    command = [
        sys.executable,
        str(repo_dir / "scripts" / "gallery_dl_x_runner.py"),
        "--archive-x-request-telemetry",
        str(request_path),
        "--archive-x-operation",
        request_operation,
        *pacing_x.options_as_runner_args(scheduler_options),
        "--config-ignore",
        "-c",
        str(repo_dir / "gallery-dl.conf"),
        "--config-json",
        str(config_path),
        "--no-input",
        "--no-colors",
        "--http-timeout",
        "60",
        "--sleep-retries",
        "0",
        "--sleep-429",
        "0",
        "--retries",
        "1",
        "--post-range",
        "1-200" if conversation and not media else "1",
    ]
    if not media:
        command.append("--no-download")
    command.append(f"https://x.com/i/web/status/{post_id}")
    run_kwargs: dict[str, Any] = {}
    if runner is not None:
        run_kwargs.update(
            runner=runner,
            control_lease_token=control_lease_token,
        )
    (
        status,
        _cursor,
        _duration,
        interrupted,
        failed_downloads,
        _errors,
        _stalled,
        _cycles,
    ) = archive_x.run_gallery_dl(
        command,
        log_path,
        f"context:{post_id}",
        **run_kwargs,
    )
    log = log_path.read_text(encoding="utf-8", errors="replace")
    rate_resets = [float(match.group(1)) for match in RATE_RESET_RE.finditer(log)]
    request_summary, request_error = archive_x.request_telemetry_summary(
        request_path, request_operation
    )
    descriptor_path = descriptor_x.finalize_artifact(
        descriptor_partial,
        complete=status == 0 and not interrupted,
    )
    try:
        descriptor_batch = descriptor_x.load_artifact(
            descriptor_path,
            user_dir=user_dir,
            operation_id=operation_id,
            run_id=operation_id,
            source_kind=descriptor_source_kind,
            source_operation=descriptor_source_operation,
            ephemeral=True,
        )
    except (descriptor_x.DescriptorError, OSError) as exc:
        descriptor_batch = descriptor_x.DescriptorBatch(
            operation_id=operation_id,
            run_id=operation_id,
            source_kind=descriptor_source_kind,
            source_operation=descriptor_source_operation,
            rows=(),
            errors=(exc.__class__.__name__,),
            artifact_path=descriptor_path,
            ephemeral=True,
        )
    records = list(archive_x.iter_jsonl(raw_path))
    matching = [record for record in records if id_string(record.get("tweet_id")) == post_id]
    if not conversation and records and (
        len(records) != 1 or len(matching) != 1
    ):
        raise ContextError(
            f"focal-only invariant failed for {post_id}: "
            f"{len(records)} total, {len(matching)} matching"
        )
    if conversation:
        seen: set[str] = set()
        for record in records:
            record_id = id_string(record.get("tweet_id"))
            if not record_id:
                raise ContextError("conversation response contains a post without an ID")
            if record_id in seen:
                raise ContextError(
                    f"conversation response contains duplicate post {record_id}"
                )
            seen.add(record_id)
    return FetchResult(
        status=status,
        metadata=matching[0] if matching else None,
        log=log,
        interrupted=interrupted,
        failed_downloads=failed_downloads,
        rate_reset=max(rate_resets) if rate_resets else None,
        records=tuple(records),
        descriptor_batches=(descriptor_batch,),
        request_telemetry=request_summary,
        request_telemetry_error=request_error,
    )


def classify_log(log: str) -> tuple[str, bool, bool]:
    for error_class, patterns in TERMINAL_PATTERNS.items():
        if any(pattern.lower() in log.lower() for pattern in patterns):
            return error_class, True, False
    if any(pattern.lower() in log.lower() for pattern in AUTH_PATTERNS):
        return "authentication", False, True
    if any(
        pattern.lower() in log.lower() for pattern in AMBIGUOUS_RESPONSE_PATTERNS
    ):
        return "ambiguous_response_shape", False, False
    if any(pattern.lower() in log.lower() for pattern in TRANSIENT_PATTERNS):
        return "transient", False, False
    return "unknown", False, False


def classify_failure(result: FetchResult) -> tuple[str, bool, bool]:
    return classify_log(result.log)


def reserve_request(
    context: ContextDB,
    delay: str,
    *,
    now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    announce: bool = True,
) -> float:
    low, high = archive_x.parse_duration(delay)
    current = now()
    with transaction(context.connection):
        row = context.connection.execute(
            "SELECT next_request_at FROM pacing WHERE singleton=1"
        ).fetchone()
        base = max(current, float(row[0]))
        chosen = random.uniform(low, high)
        reserved = base + chosen
        context.connection.execute(
            """UPDATE pacing SET next_request_at=?,not_before_reason='spacing',
                   last_request_at=?,updated_at=?
               WHERE singleton=1""",
            (reserved, current, iso_now()),
        )
    wait = max(0.0, reserved - current)
    if wait and announce:
        print(f"Waiting {wait:.1f}s before context request.")
    if wait:
        sleep(wait)
    return reserved


def persist_rate_reset(context: ContextDB, reset: float | None) -> None:
    if reset is None:
        return
    with transaction(context.connection):
        row = context.connection.execute(
            "SELECT next_request_at FROM pacing WHERE singleton=1"
        ).fetchone()
        context.connection.execute(
            """UPDATE pacing SET next_request_at=?,
                   not_before_reason='rate_limit',last_rate_limit_at=?,updated_at=?
               WHERE singleton=1""",
            (max(float(row[0]), reset), reset, iso_now()),
        )


def context_media_complete(user_dir: Path, post_id: str) -> bool:
    root = user_dir / "media" / "context"
    if not root.is_dir():
        return False
    found = False
    for sidecar in root.rglob(f"*_{post_id}_*.json"):
        asset = Path(str(sidecar)[:-5])
        metadata = archive_x.load_json(sidecar, {})
        digest = metadata.get("sha256") if isinstance(metadata, dict) else None
        if not asset.is_file() or not digest:
            return False
        if archive_x.sha256_file(asset) != digest:
            return False
        found = True
    return found


def false_media_archive_skip_paths(
    user_dir: Path, post_id: str, detail: str | None
) -> list[str]:
    """Return exact skipped context paths or nothing when evidence is mixed."""
    if not detail:
        return []
    context_root = (user_dir / "media" / "context").resolve()
    paths: list[str] = []
    for raw_line in detail.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("command: "):
            continue
        if line == "[twitter][info] Initializing client transaction keys":
            continue
        if (
            "[warning]" in line
            and (
                "Unsupported block type" in line
                or "Unsupported entity type" in line
            )
        ):
            continue
        if not line.startswith("# "):
            return []
        path = Path(line[2:]).resolve()
        if context_root not in path.parents or f"_{post_id}_" not in path.name:
            return []
        if path.exists() or Path(str(path) + ".json").exists():
            return []
        paths.append(str(path))
    return paths


def repair_false_media_archive_skips(
    user_dir: Path, db_path: Path, *, apply: bool
) -> dict[str, Any]:
    """Requeue only media reviews proven to be download-archive false skips."""
    with ContextDB(db_path, create=False) as context:
        candidates = []
        for row in context.connection.execute(
            """SELECT post_id,media_state,media_attempts,media_next_attempt_at,
                      last_error_class,last_error_detail,updated_at
                 FROM targets
                WHERE state='captured' AND media_state='manual_review'
                  AND last_error_class='media_download'
                ORDER BY post_id"""
        ):
            paths = false_media_archive_skip_paths(
                user_dir, str(row["post_id"]), row["last_error_detail"]
            )
            if paths and not context_media_complete(
                user_dir, str(row["post_id"])
            ):
                candidates.append({**dict(row), "skipped_paths": paths})

        result: dict[str, Any] = {
            "candidates": len(candidates),
            "requeued": 0,
            "writes": False,
        }
        if not apply or not candidates:
            return result

        canonical = json.dumps(
            candidates, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        backup = (
            user_dir
            / "_state"
            / "backups"
            / f"context-media-archive-skip-{digest[:12]}.json"
        )
        payload = {
            "schema": "gdl-x-context-media-repair",
            "schema_version": 1,
            "created_at": iso_now(),
            "database": str(db_path.relative_to(user_dir)),
            "candidate_sha256": digest,
            "rows": candidates,
        }
        if backup.exists():
            previous = archive_x.load_json(backup, {})
            if (
                previous.get("candidate_sha256") != digest
                or previous.get("rows") != candidates
            ):
                raise ContextError("context media repair backup changed")
        else:
            archive_x.atomic_write_json(backup, payload)
            os.chmod(backup, 0o600)

        repaired_at = iso_now()
        with transaction(context.connection):
            changed = 0
            for row in candidates:
                cursor = context.connection.execute(
                    """UPDATE targets SET media_state='pending',
                           media_attempts=0,media_next_attempt_at=0,
                           last_error_class=NULL,last_error_detail=NULL,
                           media_lease_started_at=NULL,media_lease_token=NULL,
                           updated_at=?
                       WHERE post_id=? AND state='captured'
                         AND media_state='manual_review'
                         AND media_attempts=?
                         AND last_error_class='media_download'
                         AND last_error_detail=?""",
                    (
                        repaired_at,
                        row["post_id"],
                        row["media_attempts"],
                        row["last_error_detail"],
                    ),
                )
                changed += cursor.rowcount
            if changed != len(candidates):
                raise ContextError(
                    "context media repair target changed during guarded update"
                )
        errors = context.integrity_errors()
        if errors:
            raise ContextError("; ".join(errors))
        result.update(
            {
                "requeued": len(candidates),
                "writes": True,
                "backup": str(backup.relative_to(user_dir)),
            }
        )
        return result


def ensure_context_media_space(archive_root: Path) -> None:
    free = shutil.disk_usage(archive_root).free
    if free < MIN_CONTEXT_MEDIA_FREE_BYTES:
        raise ContextError(
            "refusing context media download with less than 5 GiB free at "
            f"{archive_root}"
        )


def run_worker(
    *,
    repo_dir: Path,
    archive_root: Path,
    user_dir: Path,
    db_path: Path,
    handle: str,
    cookie_file: Path,
    max_posts: int | None,
    request_delay: str,
    retry_delay: float,
    max_attempts: int,
    lease_seconds: float,
    fairness_quantum: int,
    max_depth: int,
    media: bool,
    fetcher: Callable[..., FetchResult] = fetch_post,
    clock: Callable[[], float] = time.time,
    idle_sleep: Callable[[float], None] = time.sleep,
    progress: Callable[[str, str, bool], None] | None = None,
    runner: Any | None = None,
) -> dict[str, int]:
    if max_posts is not None and max_posts < 1:
        raise ContextError("context post limit must be positive")
    target_id, canonical_handle = target_identity(user_dir)
    counts = {
        "attempted": 0,
        "requests": 0,
        "actual_requests": 0,
        "x_api_requests": 0,
        "x_support_requests": 0,
        "cdn_requests": 0,
        "external_requests": 0,
        "descriptor_rows": 0,
        "descriptor_generations": 0,
        "descriptor_refresh_needed": 0,
        "descriptor_artifact_errors": 0,
        "captured": 0,
        "conversation_captured": 0,
        "unavailable": 0,
        "retryable": 0,
        "manual_review": 0,
        "reclassified_unavailable": 0,
    }

    def add_request_telemetry(result: FetchResult) -> None:
        summary = result.request_telemetry
        if not isinstance(summary, dict):
            return
        counts["actual_requests"] += int(summary.get("actual_requests") or 0)
        categories = summary.get("by_category")
        if not isinstance(categories, dict):
            return
        counts["x_api_requests"] += int(categories.get("x_api") or 0)
        counts["x_support_requests"] += int(categories.get("x_support") or 0)
        counts["cdn_requests"] += int(categories.get("media_cdn") or 0)
        counts["external_requests"] += int(categories.get("external") or 0)
    actual_boundary_pacing = fetcher is fetch_post or runner is not None
    with ContextDB(db_path) as context:
        context.bind_identity(target_id, canonical_handle)
        errors = context.integrity_errors()
        if errors:
            raise ContextError("; ".join(errors))
        reconciled = context.reconcile_known_failures()
        counts["reclassified_unavailable"] = sum(
            reconciled["unavailable"].values()
        )
        context.require_authentication_clear()
        while max_posts is None or counts["attempted"] < max_posts:
            context.require_authentication_clear()
            current = clock()
            row = context.claim(
                now=current,
                lease_seconds=lease_seconds,
                fairness_quantum=fairness_quantum,
                media=media,
            )
            if row is None:
                if max_posts is not None:
                    break
                availability = context.work_availability(
                    now=current, lease_seconds=lease_seconds, media=media
                )
                next_at = availability["next_eligible_at"]
                if availability["ready"]:
                    raise ContextError(
                        "context queue reported ready work that could not be claimed"
                    )
                if next_at is None:
                    break
                idle_sleep(max(0.01, min(float(next_at) - current, 60.0)))
                continue
            post_id = row["post_id"]
            lease_column = "media_lease_token" if media else "lease_token"
            control_lease = context.connection.execute(
                f"SELECT {lease_column} FROM targets WHERE post_id=?",
                (post_id,),
            ).fetchone()[0]
            if progress is not None:
                progress("fetching", post_id, False)
            counts["attempted"] += 1
            if media and context_media_complete(user_dir, post_id):
                context.media_succeeded(post_id)
                counts["captured"] += 1
                if progress is not None:
                    progress("captured", post_id, True)
                continue
            try:
                if media:
                    ensure_context_media_space(archive_root)
                if not actual_boundary_pacing:
                    reserve_request(
                        context, request_delay, announce=progress is None
                    )
                fetch_kwargs: dict[str, Any] = {}
                if actual_boundary_pacing:
                    fetch_kwargs.update(
                        request_delay=request_delay,
                        runner=runner,
                        control_lease_token=str(control_lease),
                    )
                result = fetcher(
                    repo_dir=repo_dir,
                    archive_root=archive_root,
                    user_dir=user_dir,
                    handle=canonical_handle or handle,
                    post_id=post_id,
                    cookie_file=cookie_file,
                    media=media,
                    conversation=not media,
                    **fetch_kwargs,
                )
                counts["requests"] += 1
                add_request_telemetry(result)
                descriptor_batches = list(result.descriptor_batches)
            except KeyboardInterrupt:
                context.fail(
                    post_id,
                    error_class="interrupted",
                    detail="operator interrupt",
                    now=clock(),
                    max_attempts=max_attempts,
                    retry_delay=0,
                    media=media,
                )
                raise
            if not actual_boundary_pacing:
                persist_rate_reset(context, result.rate_reset)
            if result.interrupted:
                context.fail(
                    post_id,
                    error_class="interrupted",
                    detail=result.log,
                    now=clock(),
                    max_attempts=max_attempts,
                    retry_delay=0,
                    media=media,
                )
                raise KeyboardInterrupt
            if (
                not media
                and result.metadata is None
                and (
                    result.status == 0
                    or classify_failure(result)[0]
                    in {"ambiguous_response_shape", "unknown"}
                )
            ):
                # A conversation response can legitimately contain nearby
                # posts without its focal post.  Never infer unavailability
                # from that shape; spend one paced exact lookup instead.
                try:
                    if not actual_boundary_pacing:
                        reserve_request(
                            context, request_delay, announce=progress is None
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
                        **fetch_kwargs,
                    )
                    counts["requests"] += 1
                    add_request_telemetry(result)
                    descriptor_batches.extend(result.descriptor_batches)
                except KeyboardInterrupt:
                    context.fail(
                        post_id,
                        error_class="interrupted",
                        detail="operator interrupt",
                        now=clock(),
                        max_attempts=max_attempts,
                        retry_delay=0,
                    )
                    raise
                if not actual_boundary_pacing:
                    persist_rate_reset(context, result.rate_reset)
                if result.interrupted:
                    context.fail(
                        post_id,
                        error_class="interrupted",
                        detail=result.log,
                        now=clock(),
                        max_attempts=max_attempts,
                        retry_delay=0,
                    )
                    raise KeyboardInterrupt
            if result.metadata is not None:
                if media:
                    descriptor_summary = context.persist_descriptor_batches(
                        descriptor_batches,
                        (result.metadata,),
                    )
                    counts["descriptor_rows"] += int(
                        descriptor_summary.get("rows_accepted") or 0
                    )
                    counts["descriptor_generations"] += int(
                        descriptor_summary.get("generations_created") or 0
                    )
                    counts["descriptor_refresh_needed"] += int(
                        descriptor_summary.get("needs_refresh_created") or 0
                    )
                    counts["descriptor_artifact_errors"] += int(
                        descriptor_summary.get("artifact_errors") or 0
                    )
                    if (
                        result.failed_downloads
                        or result.status != 0
                        or not context_media_complete(user_dir, post_id)
                    ):
                        state = context.fail(
                            post_id,
                            error_class="media_download",
                            detail=result.log,
                            now=clock(),
                            max_attempts=max_attempts,
                            retry_delay=retry_delay,
                            media=True,
                        )
                        counts[state] = counts.get(state, 0) + 1
                        if progress is not None:
                            progress(state, post_id, True)
                    else:
                        context.media_succeeded(post_id)
                        counts["captured"] += 1
                        if progress is not None:
                            progress("captured", post_id, True)
                    for batch in descriptor_batches:
                        descriptor_x.discard_ephemeral_artifact(batch)
                    continue
                if len(result.records) > 1:
                    captured, continuation = context.capture_conversation_response(
                        post_id,
                        result.records,
                        target_user_id=target_id,
                        max_depth=max_depth,
                        descriptor_batches=descriptor_batches,
                    )
                    if not captured:
                        raise ContextError(
                            f"conversation response lost focal post {post_id}"
                        )
                    context.continue_chain(continuation, fairness_quantum)
                    counts["captured"] += len(captured)
                    counts["conversation_captured"] += max(0, len(captured) - 1)
                else:
                    parent = context.capture(
                        post_id,
                        result.metadata,
                        source_kind="x:focal",
                        target_user_id=target_id,
                        max_depth=max_depth,
                        descriptor_batches=descriptor_batches,
                    )
                    context.continue_chain(parent, fairness_quantum)
                    counts["captured"] += 1
                if progress is not None:
                    progress("captured", post_id, True)
                descriptor_summary = (
                    descriptor_batches[-1].persistence
                    if descriptor_batches
                    else {}
                )
                counts["descriptor_rows"] += int(
                    descriptor_summary.get("rows_accepted") or 0
                )
                counts["descriptor_generations"] += int(
                    descriptor_summary.get("generations_created") or 0
                )
                counts["descriptor_refresh_needed"] += int(
                    descriptor_summary.get("needs_refresh_created") or 0
                )
                counts["descriptor_artifact_errors"] += int(
                    descriptor_summary.get("artifact_errors") or 0
                )
                for batch in descriptor_batches:
                    descriptor_x.discard_ephemeral_artifact(batch)
                continue
            error_class, terminal, global_stop = classify_failure(result)
            state = context.fail(
                post_id,
                error_class=error_class,
                detail=result.log,
                now=clock(),
                max_attempts=max_attempts,
                retry_delay=retry_delay,
                terminal=terminal,
                media=media,
            )
            counts[state] = counts.get(state, 0) + 1
            for batch in descriptor_batches:
                descriptor_x.discard_ephemeral_artifact(batch)
            if progress is not None:
                progress(state, post_id, True)
            if global_stop:
                context.record_authentication_stop(error_class, now=clock())
                raise ContextAuthenticationError(
                    "context worker stopped on authentication/account state; "
                    "credentials require operator inspection"
                )
        with transaction(context.connection):
            context.connection.execute(
                "UPDATE pacing SET last_progress_at=? WHERE singleton=1",
                (iso_now(),),
            )
    return counts


def normalize_context(
    metadata: dict[str, Any], handle: str, target_id: str
) -> dict[str, Any]:
    record = archive_x.normalize_post(metadata, handle, "reply-context")
    if record is None:
        raise ContextError("context observation lacks post ID")
    author_id = id_string((metadata.get("author") or {}).get("id"))
    authored = author_id == target_id
    record["requested_handle"] = handle
    record["requested_user_id"] = target_id
    record["canonical_requested_handle"] = handle
    record["is_authored_by_requested_user"] = authored
    if authored:
        record["relationship"] = "reply" if id_string(metadata.get("reply_id")) else "post"
    else:
        record["relationship"] = "context"
    author = metadata.get("author") or {}
    if author.get("name"):
        record["source_url"] = (
            f"https://x.com/{author['name']}/status/{record['post_id']}"
        )
    return record


def export_datasets(user_dir: Path, db_path: Path) -> dict[str, int]:
    with ContextDB(db_path, create=False) as database:
        local_ready = database.connection.execute(
            "SELECT 1 FROM current_pointers "
            "WHERE pointer_name='local_history_reconciled'"
        ).fetchone()
    if local_ready is not None:
        import archive_x_local as local_x

        exported = local_x.materialize_exports(user_dir, db_path)
        with ContextDB(db_path, create=False) as database:
            counters = {
                str(row[0]): int(row[1])
                for row in database.connection.execute(
                    "SELECT counter_name,value FROM progress_counters"
                )
            }
        return {
            "context_posts": counters.get("observations_total", 0),
            "reply_edges": counters.get("reply_edges_total", 0),
            "generation": int(exported["generation"]),
            "views_written": int(exported["views_written"]),
            "bytes_written": int(exported["bytes_written"]),
        }
    target_id, handle = target_identity(user_dir)
    with ContextDB(db_path, create=False) as context:
        context.bind_identity(target_id, handle)
        posts = []
        for row in context.connection.execute(
            "SELECT raw_json FROM observations ORDER BY post_id"
        ):
            posts.append(normalize_context(json.loads(row[0]), handle, target_id))
        edges = []
        for row in context.connection.execute(
            """SELECT e.*,t.state,t.last_error_class,t.unavailable_at
               FROM reply_edges e JOIN targets t ON t.post_id=e.parent_id
               ORDER BY CAST(e.child_id AS INTEGER),e.child_id"""
        ):
            edges.append(
                {
                    "schema": "gdl-x-reply-edge",
                    "schema_version": SCHEMA_VERSION,
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
            )
        dataset = user_dir / "dataset"
        post_count = archive_x.atomic_write_jsonl(
            dataset / "context-posts.jsonl", posts
        )
        edge_count = archive_x.atomic_write_jsonl(
            dataset / "reply-edges.jsonl", edges
        )
        status = context.status()
        archive_x.atomic_write_json(dataset / "context-status.json", status)
    return {"context_posts": post_count, "reply_edges": edge_count}


def reset_targets(
    db_path: Path, post_ids: list[str] | None, *, media: bool = False
) -> int:
    normalized = [id_string(value) for value in (post_ids or [])]
    if any(value is None for value in normalized):
        raise ContextError("retry post IDs must be positive numeric IDs")
    selected = [value for value in normalized if value is not None]
    with ContextDB(db_path, create=False) as context, transaction(context.connection):
        if media:
            where = ""
            parameters: list[str] = []
            if selected:
                placeholders = ",".join("?" for _ in selected)
                where = f" AND post_id IN ({placeholders})"
                parameters = selected
            cursor = context.connection.execute(
                """UPDATE targets SET media_state='pending',media_attempts=0,
                       media_next_attempt_at=0,last_error_class=NULL,
                       last_error_detail=NULL
                   WHERE state='captured'
                     AND media_state IN
                         ('unavailable','manual_review','retryable')"""
                + where,
                parameters,
            )
        elif selected:
            placeholders = ",".join("?" for _ in selected)
            cursor = context.connection.execute(
                f"""UPDATE targets SET state='pending',attempts=0,
                       next_attempt_at=0,last_error_class=NULL,
                       last_error_detail=NULL,unavailable_at=NULL
                       WHERE post_id IN ({placeholders})
                         AND state IN ('unavailable','manual_review','retryable')""",
                selected,
            )
        else:
            cursor = context.connection.execute(
                """UPDATE targets SET state='pending',attempts=0,
                       next_attempt_at=0,last_error_class=NULL,
                       last_error_detail=NULL,unavailable_at=NULL
                   WHERE state IN ('manual_review','retryable')"""
            )
        return cursor.rowcount


def build_parser(repo_dir: Path) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scripts/archive-x-context",
        description="Safely seed, resolve, inspect, and export X reply ancestors.",
    )
    parser.add_argument("--user", required=True, help="existing archived X handle")
    parser.add_argument("--output-root", type=Path)
    parser.add_argument(
        "--cookies",
        type=Path,
        default=repo_dir / "state" / "cookies" / "x.cookies.txt",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    seed = commands.add_parser("seed", help="discover reply edges without X requests")
    seed.add_argument("--dry-run", action="store_true")
    seed.add_argument(
        "--raw-path",
        type=Path,
        action="append",
        help="seed only this timeline raw file; intended for timeline integration",
    )
    run = commands.add_parser(
        "run", help="resolve parents to closure; optional diagnostic bound"
    )
    run.add_argument(
        "--max-posts",
        type=archive_x.positive_int,
        help="advanced: stop after this many parent attempts",
    )
    media = commands.add_parser(
        "media", help="download captured context media; optional diagnostic bound"
    )
    media.add_argument(
        "--max-posts",
        type=archive_x.positive_int,
        help="advanced: stop after this many media attempts",
    )
    commands.add_parser("status", help="print queue and coverage status")
    commands.add_parser("integrity", help="verify SQLite and graph invariants")
    commands.add_parser("export", help="atomically rebuild context datasets")
    repair_media = commands.add_parser(
        "repair-media-skips",
        help="guardedly requeue false context-media archive skips",
    )
    repair_media.add_argument(
        "--apply",
        action="store_true",
        help="write a recovery manifest and apply the exact repair",
    )
    retry = commands.add_parser("retry", help="explicitly requeue failed targets")
    retry.add_argument("post_ids", nargs="*")
    retry.add_argument(
        "--media", action="store_true", help="requeue context-media state instead"
    )
    repair_descriptor = commands.add_parser(
        "repair-descriptor",
        help="explicitly create a new media descriptor refresh generation",
    )
    repair_descriptor.add_argument("post_id")
    auth_stop = commands.add_parser(
        "auth-stop", help="inspect the durable account authentication stop"
    )
    auth_stop.add_argument(
        "--clear",
        action="store_true",
        help="explicitly clear the stop after credentials were inspected",
    )
    parser.add_argument("--request-delay", type=archive_x.duration_arg, default="4-8")
    parser.add_argument(
        "--retry-delay", type=archive_x.nonnegative_float, default=300.0
    )
    parser.add_argument("--max-attempts", type=archive_x.positive_int, default=3)
    parser.add_argument("--lease-seconds", type=positive_float, default=900.0)
    parser.add_argument("--fairness-quantum", type=archive_x.positive_int, default=50)
    parser.add_argument("--max-depth", type=archive_x.positive_int, default=1000)
    return parser


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, interrupt_handler)
    repo_dir = Path(__file__).resolve().parent.parent
    parser = build_parser(repo_dir)
    args = parser.parse_args(argv)
    try:
        handle = archive_x.normalize_handle(args.user)
        dry = (
            args.command == "seed" and args.dry_run
        ) or (
            args.command == "repair-media-skips" and not args.apply
        ) or (
            args.command == "auth-stop" and not args.clear
        )
        archive_root = archive_x.resolve_output_root(args.output_root, plan_only=dry)
        user_dir, db_path = user_paths(archive_root, handle)
        if args.command == "seed":
            result = seed_context(
                user_dir,
                db_path,
                dry_run=dry,
                max_depth=args.max_depth,
                raw_paths=args.raw_path,
            )
            if dry:
                result = {
                    **result,
                    "archive_user_dir": str(user_dir),
                    "database": str(db_path),
                    "writes": False,
                    "network_requests": 0,
                    "policy": {
                        "scope": "ancestor-only",
                        "worker_count": 1,
                        "max_depth": args.max_depth,
                        "fairness_quantum": args.fairness_quantum,
                        "metadata_before_media": True,
                    },
                    "next_commands": [
                        f"scripts/archive-x-context --user {handle} seed",
                        f"scripts/archive-x-context --user {handle} status",
                        (
                            f"scripts/archive-x-context --user {handle} "
                            "run --max-posts 1"
                        ),
                    ],
                }
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if args.command == "status":
            with ContextDB(db_path, create=False) as context:
                print(json.dumps(context.status(), indent=2, sort_keys=True))
            return 0
        if args.command == "integrity":
            with ContextDB(db_path, create=False) as context:
                errors = context.integrity_errors()
            if errors:
                print("\n".join(errors), file=sys.stderr)
                return 1
            print("context database: ok")
            return 0
        if args.command == "export":
            print(json.dumps(export_datasets(user_dir, db_path), indent=2))
            return 0
        if args.command == "retry":
            print(
                f"requeued: {reset_targets(db_path, args.post_ids, media=args.media)}"
            )
            return 0
        if args.command == "auth-stop" and not args.clear:
            with ContextDB(db_path, create=False) as context:
                print(
                    json.dumps(
                        {"authentication_stop": context.authentication_stop()},
                        indent=2,
                        sort_keys=True,
                    )
                )
            return 0
        if args.command == "repair-media-skips":
            if not args.apply:
                print(
                    json.dumps(
                        repair_false_media_archive_skips(
                            user_dir, db_path, apply=False
                        ),
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0
            if not os.access(archive_root, os.W_OK | os.X_OK):
                raise ContextError(f"archive root is not writable: {archive_root}")
            with archive_x.exclusive_lock(
                repo_dir / "state" / "locks" / "archive-x.lock"
            ), archive_x.exclusive_lock(
                archive_root / "_state" / "archive-x.lock"
            ):
                result = repair_false_media_archive_skips(
                    user_dir, db_path, apply=True
                )
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0

        if args.command in {"repair-descriptor", "auth-stop"}:
            if not os.access(archive_root, os.W_OK | os.X_OK):
                raise ContextError(f"archive root is not writable: {archive_root}")
            with archive_x.exclusive_lock(
                repo_dir / "state" / "locks" / "archive-x.lock"
            ), archive_x.exclusive_lock(
                archive_root / "_state" / "archive-x.lock"
            ), ContextDB(db_path, create=False) as context:
                target_id, canonical_handle = target_identity(user_dir)
                context.bind_identity(target_id, canonical_handle)
                if args.command == "repair-descriptor":
                    refresh_id = context.enqueue_operator_refresh(args.post_id)
                    print(
                        json.dumps(
                            {
                                "post_id": args.post_id,
                                "descriptor_refresh_id": refresh_id,
                            },
                            indent=2,
                            sort_keys=True,
                        )
                    )
                else:
                    print(
                        json.dumps(
                            {
                                "authentication_stop_cleared": (
                                    context.clear_authentication_stop()
                                )
                            },
                            indent=2,
                            sort_keys=True,
                        )
                    )
            return 0

        args.cookies = args.cookies.expanduser().resolve()
        archive_x.validate_cookie_file(args.cookies)
        version = archive_x.gallery_dl_version()
        archive_x.verify_gallery_dl_x_runner(repo_dir, version)
        if not os.access(archive_root, os.W_OK | os.X_OK):
            raise ContextError(f"archive root is not writable: {archive_root}")
        with archive_x.exclusive_lock(
            repo_dir / "state" / "locks" / "archive-x.lock"
        ), archive_x.exclusive_lock(archive_root / "_state" / "archive-x.lock"):
            result = run_worker(
                repo_dir=repo_dir,
                archive_root=archive_root,
                user_dir=user_dir,
                db_path=db_path,
                handle=handle,
                cookie_file=args.cookies,
                max_posts=args.max_posts,
                request_delay=args.request_delay,
                retry_delay=args.retry_delay,
                max_attempts=args.max_attempts,
                lease_seconds=args.lease_seconds,
                fairness_quantum=args.fairness_quantum,
                max_depth=args.max_depth,
                media=args.command == "media",
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (archive_x.ArchiveError, ContextError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"archive-x-context: {exc}\n")
    except KeyboardInterrupt:
        print("Interrupted; context lease is safely retryable.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
