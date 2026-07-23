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
import shutil
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

import archive_x


SCHEMA_VERSION = 2
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
    "private": ("Tweet unavailable ('Protected')", "Tweets are protected"),
    "suspended": ("User has been suspended", "Account suspended"),
    "withheld": ("Tweet unavailable ('Withheld')", "withheld in your country"),
}
AUTH_PATTERNS = (
    "Could not authenticate you",
    "Login Required",
    "Account temporarily locked",
    "AuthorizationError",
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
SENSITIVE_LOG_RE = re.compile(
    r"(?i)(authorization|proxy-authorization|cookie:|set-cookie:|auth_token|ct0)"
)


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
        for name in ("pending", "retryable", "leased", "manual_review")
    )
    return {
        "status": "present",
        "schema_version": version,
        "targets": sum(states.values()),
        "edges": edges,
        "metadata_pending": pending,
        "manual_review": manual,
        "media_pending": media_pending,
        "integrity_ok": bool(quick and quick[0] == "ok" and foreign is None),
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
        if create:
            os.chmod(path, 0o600)
        self._ensure_schema(create=create)

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
            return
        if version != SCHEMA_VERSION:
            raise ContextError(
                f"unsupported context schema {version}; expected {SCHEMA_VERSION}"
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
                    VALUES ('schema_version', '{SCHEMA_VERSION}');

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
        if previous and previous[0] != target_user_id:
            raise ContextError(
                "context database identity does not match archive state: "
                f"{previous[0]} != {target_user_id}"
            )
        with transaction(self.connection):
            self._set_meta("target_user_id", target_user_id)
            self._set_meta("canonical_handle", handle)

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
        enqueue_media = media_count > 0 and source_kind == "x:focal"
        row = self.connection.execute(
            "SELECT depth_min FROM targets WHERE post_id=?", (post_id,)
        ).fetchone()
        depth = int(row[0]) if row else 0
        with transaction(self.connection):
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

    def reclaim_stale(
        self, now: float, lease_seconds: float, *, media: bool = False
    ) -> int:
        cutoff = now - lease_seconds
        with transaction(self.connection):
            if media:
                cursor = self.connection.execute(
                    """UPDATE targets SET media_state='retryable',
                           lease_started_at=NULL, media_next_attempt_at=?,
                           last_error_class='stale_media_lease', updated_at=?
                       WHERE media_state='leased' AND lease_started_at < ?""",
                    (now, iso_now(), cutoff),
                )
            else:
                cursor = self.connection.execute(
                    """UPDATE targets SET state='retryable',
                           lease_started_at=NULL, next_attempt_at=?,
                           last_error_class='stale_lease', updated_at=?
                       WHERE state='leased' AND lease_started_at < ?""",
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
                    """SELECT * FROM targets
                       WHERE state='captured'
                         AND media_state IN ('pending','retryable')
                         AND media_next_attempt_at <= ?
                       ORDER BY depth_min, post_id DESC LIMIT 1""",
                    (now,),
                ).fetchone()
                if row:
                    self.connection.execute(
                        """UPDATE targets SET media_state='leased',
                               lease_started_at=?, media_attempts=media_attempts+1
                           WHERE post_id=?""",
                        (now, row["post_id"]),
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
                    """SELECT t.* FROM targets t
                       WHERE t.state IN ('pending','retryable')
                         AND t.next_attempt_at <= ?
                       ORDER BY
                         (SELECT COUNT(*) FROM reply_edges e
                          WHERE e.parent_id=t.post_id) DESC,
                         t.depth_min ASC,
                         t.post_id DESC
                       LIMIT 1""",
                    (now,),
                ).fetchone()
                active_steps = 0
            if row:
                self.connection.execute(
                    """UPDATE targets SET state='leased', lease_started_at=?,
                           attempts=attempts+1, updated_at=?
                       WHERE post_id=?""",
                    (now, iso_now(), row["post_id"]),
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
                          lease_started_at FROM targets
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
                    """UPDATE targets SET media_state=?, lease_started_at=NULL,
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

    def media_succeeded(self, post_id: str) -> None:
        with transaction(self.connection):
            self.connection.execute(
                """UPDATE targets SET media_state='captured',
                       lease_started_at=NULL, media_next_attempt_at=0,
                       updated_at=? WHERE post_id=?""",
                (iso_now(), post_id),
            )

    def status(self) -> dict[str, Any]:
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
) -> tuple[dict[str, Any], Path]:
    raw_path = work_dir / "current.posts.jsonl.partial"
    config = archive_x.build_gallery_config(
        handle=handle,
        endpoint="reply-context",
        archive_root=archive_root,
        user_dir=user_dir,
        raw_partial=raw_path,
        cookie_file=cookie_file,
        archive_run_id=f"context-{post_id}",
        archived_at=iso_now(),
        request_delay="4-8",
        download_delay="1-3",
        extractor_delay="2-5",
        include_reposts=True,
        checksums=media,
        cursor=None,
    )
    twitter = config["extractor"]["twitter"]
    twitter.pop("timeline", None)
    twitter.update(
        {
            "tweet-endpoint": "rest",
            "conversations": False,
            "expand": False,
            "showreplies": False,
            "quoted": False,
            "pinned": False,
            "post-filter": f"tweet_id == {post_id}",
        }
    )
    twitter["directory"] = [
        "users",
        handle,
        "media",
        "context",
        "{date:%Y}",
        "{date:%m}",
    ]
    if not media:
        twitter["postprocessors"] = [
            processor
            for processor in twitter["postprocessors"]
            if processor.get("event") == "post"
        ]
    return config, raw_path


@dataclass
class FetchResult:
    status: int
    metadata: dict[str, Any] | None
    log: str
    interrupted: bool
    failed_downloads: list[dict[str, Any]]
    rate_reset: float | None


def fetch_post(
    *,
    repo_dir: Path,
    archive_root: Path,
    user_dir: Path,
    handle: str,
    post_id: str,
    cookie_file: Path,
    media: bool,
) -> FetchResult:
    work_dir = user_dir / "_state" / "context-work"
    work_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(work_dir, 0o700)
    config_path = work_dir / "current.gallery-dl.json"
    log_path = work_dir / "current.log"
    config, raw_path = build_context_config(
        handle=handle,
        post_id=post_id,
        archive_root=archive_root,
        user_dir=user_dir,
        cookie_file=cookie_file,
        work_dir=work_dir,
        media=media,
    )
    for path in (config_path, log_path, raw_path):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    archive_x.atomic_write_json(config_path, config)
    command = [
        sys.executable,
        str(repo_dir / "scripts" / "gallery_dl_x_runner.py"),
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
        "30-60",
        "--sleep-429",
        "300",
        "--retries",
        "1",
        "--post-range",
        "1",
    ]
    if not media:
        command.append("--no-download")
    command.append(f"https://x.com/i/web/status/{post_id}")
    (
        status,
        _cursor,
        _duration,
        interrupted,
        failed_downloads,
        _errors,
        _stalled,
        _cycles,
    ) = archive_x.run_gallery_dl(command, log_path, f"context:{post_id}")
    log = log_path.read_text(encoding="utf-8", errors="replace")
    rate_resets = [float(match.group(1)) for match in RATE_RESET_RE.finditer(log)]
    records = list(archive_x.iter_jsonl(raw_path))
    matching = [record for record in records if id_string(record.get("tweet_id")) == post_id]
    if records and (len(records) != 1 or len(matching) != 1):
        raise ContextError(
            f"focal-only invariant failed for {post_id}: "
            f"{len(records)} total, {len(matching)} matching"
        )
    return FetchResult(
        status=status,
        metadata=matching[0] if matching else None,
        log=log,
        interrupted=interrupted,
        failed_downloads=failed_downloads,
        rate_reset=max(rate_resets) if rate_resets else None,
    )


def classify_failure(result: FetchResult) -> tuple[str, bool, bool]:
    for error_class, patterns in TERMINAL_PATTERNS.items():
        if any(pattern.lower() in result.log.lower() for pattern in patterns):
            return error_class, True, False
    if any(pattern.lower() in result.log.lower() for pattern in AUTH_PATTERNS):
        return "authentication", False, True
    if any(pattern.lower() in result.log.lower() for pattern in TRANSIENT_PATTERNS):
        return "transient", False, False
    return "unknown", False, False


def reserve_request(
    context: ContextDB,
    delay: str,
    *,
    now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
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
            """UPDATE pacing SET next_request_at=?, last_request_at=?
               WHERE singleton=1""",
            (reserved, current),
        )
    wait = max(0.0, reserved - current)
    if wait:
        print(f"Waiting {wait:.1f}s before context request.")
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
            """UPDATE pacing SET next_request_at=?, last_rate_limit_at=?
               WHERE singleton=1""",
            (max(float(row[0]), reset), reset),
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
) -> dict[str, int]:
    if max_posts is not None and max_posts < 1:
        raise ContextError("context post limit must be positive")
    target_id, canonical_handle = target_identity(user_dir)
    counts = {"attempted": 0, "captured": 0, "unavailable": 0, "retryable": 0,
              "manual_review": 0}
    with ContextDB(db_path) as context:
        context.bind_identity(target_id, canonical_handle)
        errors = context.integrity_errors()
        if errors:
            raise ContextError("; ".join(errors))
        while max_posts is None or counts["attempted"] < max_posts:
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
            counts["attempted"] += 1
            if media and context_media_complete(user_dir, post_id):
                context.media_succeeded(post_id)
                counts["captured"] += 1
                continue
            try:
                if media:
                    ensure_context_media_space(archive_root)
                reserve_request(context, request_delay)
                result = fetcher(
                    repo_dir=repo_dir,
                    archive_root=archive_root,
                    user_dir=user_dir,
                    handle=canonical_handle or handle,
                    post_id=post_id,
                    cookie_file=cookie_file,
                    media=media,
                )
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
            if result.metadata is not None:
                if media:
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
                    else:
                        context.media_succeeded(post_id)
                        counts["captured"] += 1
                    continue
                parent = context.capture(
                    post_id,
                    result.metadata,
                    source_kind="x:focal",
                    target_user_id=target_id,
                    max_depth=max_depth,
                )
                context.continue_chain(parent, fairness_quantum)
                counts["captured"] += 1
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
            if global_stop:
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
    retry = commands.add_parser("retry", help="explicitly requeue failed targets")
    retry.add_argument("post_ids", nargs="*")
    retry.add_argument(
        "--media", action="store_true", help="requeue context-media state instead"
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
        dry = args.command == "seed" and args.dry_run
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
