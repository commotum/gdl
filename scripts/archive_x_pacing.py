#!/usr/bin/env python3
"""Durable, account-scoped pacing at actual X HTTP boundaries.

The scheduler deliberately stores only allowlisted operation/category labels
and timing state.  It never receives or persists a URL, query, header value,
cookie, post ID, handle, or opaque cursor.
"""

from __future__ import annotations

import dataclasses
import random
import re
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


SCHEDULER_DB_OPTION = "--archive-x-scheduler-db"
SCHEDULER_SCOPE_OPTION = "--archive-x-scheduler-scope"
SCHEDULER_DELAY_OPTION = "--archive-x-scheduler-delay"
SCHEDULER_LEASE_OPTION = "--archive-x-scheduler-lease-seconds"
SCHEDULER_429_OPTION = "--archive-x-scheduler-429-seconds"
DEFAULT_DELAY_LOW_SECONDS = 4.0
DEFAULT_DELAY_HIGH_SECONDS = 8.0
DEFAULT_REQUEST_LEASE_SECONDS = 180.0
DEFAULT_429_BACKOFF_SECONDS = 300.0
SCHEDULER_OPTIONS = frozenset(
    {
        SCHEDULER_DB_OPTION,
        SCHEDULER_SCOPE_OPTION,
        SCHEDULER_DELAY_OPTION,
        SCHEDULER_LEASE_OPTION,
        SCHEDULER_429_OPTION,
    }
)

X_CATEGORIES = frozenset({"x_api", "x_support", "x_redirect"})
SAFE_ENDPOINT_RE = re.compile(r"[a-z][a-z0-9_]{0,79}\Z")
SAFE_OPERATION_RE = re.compile(r"[a-z][a-z0-9_]{0,79}\Z")
DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)(?:-(\d+(?:\.\d+)?))?\Z")
TOKEN_RE = re.compile(r"[0-9a-f]{32}\Z")
VALID_NOT_BEFORE_REASONS = frozenset(
    {"initial", "spacing", "rate_limit", "http_429"}
)
VALID_WAIT_SOURCES = frozenset(
    {
        "none",
        "spacing",
        "rate_limit",
        "http_429",
        "active_lease",
        "multiple",
    }
)


class PacingError(RuntimeError):
    """Fail-closed scheduler configuration or persistence error."""


class PacingAuthenticationError(PacingError):
    """Durable account/authentication evidence blocks X network work."""


@dataclasses.dataclass(frozen=True)
class SchedulerOptions:
    database: Path
    scope_id: str
    delay_low: float
    delay_high: float
    lease_seconds: float
    backoff_429_seconds: float


@dataclasses.dataclass(frozen=True)
class PacingReservation:
    token: str
    sequence: int
    category: str
    endpoint: str
    operation: str
    started_at: float
    waited_seconds: float
    wait_source: str


def _positive_float(value: str, label: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise PacingError(f"{label} must be a number") from exc
    if number <= 0 or number == float("inf") or number != number:
        raise PacingError(f"{label} must be finite and positive")
    return number


def parse_delay(value: str) -> tuple[float, float]:
    match = DURATION_RE.fullmatch(value.strip())
    if not match:
        raise PacingError("scheduler delay must be SECONDS or MIN-MAX")
    low = float(match.group(1))
    high = float(match.group(2) or match.group(1))
    if high < low:
        raise PacingError("scheduler delay maximum is below minimum")
    return low, high


def parse_runner_options(
    argv: list[str],
) -> tuple[SchedulerOptions | None, list[str]]:
    """Remove the private all-or-none scheduler options from runner args."""
    values: dict[str, str] = {}
    remaining: list[str] = []
    index = 0
    while index < len(argv):
        value = argv[index]
        if value in SCHEDULER_OPTIONS:
            if index + 1 >= len(argv):
                raise PacingError(f"{value} requires a value")
            if value in values:
                raise PacingError(f"{value} was provided more than once")
            values[value] = argv[index + 1]
            index += 2
            continue
        remaining.append(value)
        index += 1
    if values and set(values) != SCHEDULER_OPTIONS:
        raise PacingError("all scheduler options are required together")
    if not values:
        return None, remaining
    scope_id = values[SCHEDULER_SCOPE_OPTION]
    if not scope_id.isdecimal() or int(scope_id) < 1:
        raise PacingError("scheduler scope must be a positive numeric account ID")
    low, high = parse_delay(values[SCHEDULER_DELAY_OPTION])
    return (
        SchedulerOptions(
            database=Path(values[SCHEDULER_DB_OPTION]),
            scope_id=scope_id,
            delay_low=low,
            delay_high=high,
            lease_seconds=_positive_float(
                values[SCHEDULER_LEASE_OPTION], "scheduler lease"
            ),
            backoff_429_seconds=_positive_float(
                values[SCHEDULER_429_OPTION], "scheduler 429 backoff"
            ),
        ),
        remaining,
    )


def _iso_epoch(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _header_number(headers: Any, name: str) -> float | None:
    try:
        raw = headers.get(name)
        if raw is None:
            raw = headers.get(name.lower())
        value = float(raw)
    except (AttributeError, TypeError, ValueError):
        return None
    if value < 0 or value == float("inf") or value != value:
        return None
    return value


class DurableRequestScheduler:
    """Serialize actual X attempts through one durable SQLite row."""

    REQUIRED_COLUMNS = frozenset(
        {
            "next_request_at",
            "last_request_at",
            "last_rate_limit_at",
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
    )

    def __init__(
        self,
        options: SchedulerOptions,
        operation: str,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        choose_gap: Callable[[float, float], float] | None = None,
        token_factory: Callable[[], str] | None = None,
        lease_poll_seconds: float = 1.0,
        sqlite_timeout: float = 5.0,
    ) -> None:
        if not SAFE_OPERATION_RE.fullmatch(operation):
            raise PacingError("scheduler operation label is invalid")
        if lease_poll_seconds <= 0:
            raise PacingError("scheduler lease poll interval is invalid")
        if not options.database.is_file():
            raise PacingError("scheduler database does not exist")
        self.options = options
        self.operation = operation
        self.clock = clock
        self.sleep = sleep
        self.choose_gap = choose_gap or random.uniform
        self.token_factory = token_factory or (lambda: secrets.token_hex(16))
        self.lease_poll_seconds = lease_poll_seconds
        self._lock = threading.Lock()
        try:
            self.connection = sqlite3.connect(
                options.database,
                timeout=sqlite_timeout,
                isolation_level=None,
                check_same_thread=False,
            )
            self.connection.row_factory = sqlite3.Row
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("PRAGMA busy_timeout=5000")
            self._validate_catalog()
        except (OSError, sqlite3.Error) as exc:
            try:
                self.connection.close()
            except (AttributeError, sqlite3.Error):
                pass
            raise PacingError("scheduler database could not be opened") from exc

    def _validate_catalog(self) -> None:
        columns = {
            str(row[1])
            for row in self.connection.execute("PRAGMA table_info(pacing)")
        }
        if not self.REQUIRED_COLUMNS <= columns:
            raise PacingError("scheduler database schema is incomplete")
        account = self.connection.execute(
            "SELECT user_id FROM archive_account WHERE singleton=1"
        ).fetchone()
        if account is None or str(account[0]) != self.options.scope_id:
            raise PacingError("scheduler account scope does not match archive state")
        row = self.connection.execute(
            "SELECT COUNT(*) FROM pacing WHERE singleton=1"
        ).fetchone()
        if row is None or int(row[0]) != 1:
            raise PacingError("scheduler pacing row is missing")

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def __enter__(self) -> DurableRequestScheduler:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _begin(self) -> None:
        try:
            self.connection.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as exc:
            raise PacingError("scheduler reservation database is unavailable") from exc

    def _commit(self) -> None:
        try:
            self.connection.commit()
        except sqlite3.Error as exc:
            self.connection.rollback()
            raise PacingError("scheduler reservation could not be committed") from exc

    def _rollback(self) -> None:
        try:
            self.connection.rollback()
        except sqlite3.Error:
            pass

    def _row(self) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM pacing WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise PacingError("scheduler pacing row disappeared")
        return row

    @staticmethod
    def _wait_source(sources: set[str]) -> str:
        sources.discard("none")
        if not sources:
            return "none"
        if len(sources) == 1:
            source = next(iter(sources))
            return source if source in VALID_WAIT_SOURCES else "multiple"
        return "multiple"

    def reserve(self, category: str, endpoint: str) -> PacingReservation | None:
        try:
            return self._reserve(category, endpoint)
        except sqlite3.Error as exc:
            self._rollback()
            raise PacingError("scheduler reservation database failed") from exc

    def _reserve(self, category: str, endpoint: str) -> PacingReservation | None:
        """Wait and claim immediately before one classified HTTP attempt."""
        if category not in X_CATEGORIES:
            return None
        if not SAFE_ENDPOINT_RE.fullmatch(endpoint):
            raise PacingError("scheduler endpoint label is invalid")
        waited = 0.0
        wait_sources: set[str] = set()
        while True:
            current = self.clock()
            wait_for = 0.0
            with self._lock:
                self._begin()
                try:
                    row = self._row()
                    account = self.connection.execute(
                        "SELECT user_id FROM archive_account WHERE singleton=1"
                    ).fetchone()
                    if account is None or str(account[0]) != self.options.scope_id:
                        raise PacingError(
                            "scheduler account scope changed during reservation"
                        )
                    if row["auth_stop_class"] is not None:
                        raise PacingAuthenticationError(
                            "durable authentication evidence blocks X requests"
                        )

                    active_token = row["reservation_token"]
                    active_started = row["reservation_started_at"]
                    if (active_token is None) != (active_started is None):
                        raise PacingError("scheduler request lease is inconsistent")
                    if active_token is not None:
                        stale_at = (
                            float(active_started) + self.options.lease_seconds
                        )
                        if stale_at > current:
                            wait_for = min(
                                stale_at - current, self.lease_poll_seconds
                            )
                            wait_sources.add("active_lease")
                        else:
                            self.connection.execute(
                                """UPDATE pacing
                                      SET reservation_token=NULL,
                                          reservation_started_at=NULL,
                                          reservation_recoveries=
                                              reservation_recoveries+1,
                                          updated_at=?
                                    WHERE singleton=1
                                      AND reservation_token=?""",
                                (_iso_epoch(current), str(active_token)),
                            )
                            row = self._row()

                    if wait_for == 0.0:
                        next_at = float(row["next_request_at"] or 0)
                        if next_at > current:
                            wait_for = next_at - current
                            reason = str(row["not_before_reason"] or "spacing")
                            wait_sources.add(
                                reason
                                if reason in VALID_WAIT_SOURCES
                                else "spacing"
                            )
                        else:
                            gap = float(
                                self.choose_gap(
                                    self.options.delay_low,
                                    self.options.delay_high,
                                )
                            )
                            if not self.options.delay_low <= gap <= self.options.delay_high:
                                raise PacingError(
                                    "scheduler gap chooser returned an invalid value"
                                )
                            token = self.token_factory()
                            if not TOKEN_RE.fullmatch(token):
                                raise PacingError(
                                    "scheduler token factory returned an invalid value"
                                )
                            sequence = int(row["request_sequence"] or 0) + 1
                            cursor = self.connection.execute(
                                """UPDATE pacing
                                      SET reservation_token=?,
                                          reservation_started_at=?,
                                          next_request_at=?,
                                          not_before_reason='spacing',
                                          last_request_at=?,
                                          last_request_operation=?,
                                          last_request_category=?,
                                          request_sequence=?,updated_at=?
                                    WHERE singleton=1
                                      AND reservation_token IS NULL
                                      AND auth_stop_class IS NULL""",
                                (
                                    token,
                                    current,
                                    current + gap,
                                    current,
                                    self.operation,
                                    category,
                                    sequence,
                                    _iso_epoch(current),
                                ),
                            )
                            if cursor.rowcount != 1:
                                raise PacingError(
                                    "scheduler reservation changed concurrently"
                                )
                            self._commit()
                            return PacingReservation(
                                token=token,
                                sequence=sequence,
                                category=category,
                                endpoint=endpoint,
                                operation=self.operation,
                                started_at=current,
                                waited_seconds=waited,
                                wait_source=self._wait_source(wait_sources),
                            )
                    self._commit()
                except BaseException:
                    self._rollback()
                    raise
            if wait_for > 0:
                before = self.clock()
                self.sleep(wait_for)
                after = self.clock()
                waited += max(0.0, after - before)

    def complete(
        self,
        reservation: PacingReservation | None,
        *,
        status: int | None,
        headers: Any,
        error: str | None,
        rate_limit_threshold: int | None = None,
    ) -> None:
        try:
            self._complete(
                reservation,
                status=status,
                headers=headers,
                error=error,
                rate_limit_threshold=rate_limit_threshold,
            )
        except sqlite3.Error as exc:
            self._rollback()
            raise PacingError("scheduler completion database failed") from exc

    def _complete(
        self,
        reservation: PacingReservation | None,
        *,
        status: int | None,
        headers: Any,
        error: str | None,
        rate_limit_threshold: int | None = None,
    ) -> None:
        """Release one actual-call lease and lengthen quota boundaries."""
        if reservation is None:
            return
        if (
            reservation.operation != self.operation
            or reservation.category not in X_CATEGORIES
            or not TOKEN_RE.fullmatch(reservation.token)
        ):
            raise PacingError("scheduler completion reservation is invalid")
        current = self.clock()
        reset = _header_number(headers, "x-rate-limit-reset")
        remaining = _header_number(headers, "x-rate-limit-remaining")
        if rate_limit_threshold is not None and rate_limit_threshold not in {
            1,
            2,
            3,
            4,
            5,
        }:
            raise PacingError("rate-limit threshold evidence is invalid")
        with self._lock:
            self._begin()
            try:
                row = self._row()
                if row["reservation_token"] != reservation.token:
                    raise PacingError("scheduler completion lease is stale")
                boundary = float(row["next_request_at"] or 0)
                reason = str(row["not_before_reason"] or "spacing")
                last_rate_limit = row["last_rate_limit_at"]
                if status == 429:
                    rejected_until = (
                        reset
                        if reset is not None and reset > current
                        else current + self.options.backoff_429_seconds
                    )
                    if rejected_until > boundary:
                        boundary = rejected_until
                        reason = "http_429"
                    last_rate_limit = rejected_until
                elif (
                    status is not None
                    and status < 400
                    and rate_limit_threshold is not None
                    and remaining is not None
                    and remaining < 6
                    and remaining <= rate_limit_threshold
                    and reset is not None
                    and reset > boundary
                ):
                    boundary = reset
                    reason = "rate_limit"
                    last_rate_limit = reset
                cursor = self.connection.execute(
                    """UPDATE pacing
                          SET reservation_token=NULL,
                              reservation_started_at=NULL,
                              next_request_at=?,not_before_reason=?,
                              last_rate_limit_at=?,updated_at=?
                        WHERE singleton=1 AND reservation_token=?""",
                    (
                        boundary,
                        reason,
                        last_rate_limit,
                        _iso_epoch(current),
                        reservation.token,
                    ),
                )
                if cursor.rowcount != 1:
                    raise PacingError("scheduler completion changed concurrently")
                self._commit()
            except BaseException:
                self._rollback()
                raise

    def persist_rate_limit_reset(self, value: Any) -> None:
        try:
            self._persist_rate_limit_reset(value)
        except sqlite3.Error as exc:
            self._rollback()
            raise PacingError("rate-limit reset persistence failed") from exc

    def _persist_rate_limit_reset(self, value: Any) -> None:
        """Lengthen the boundary after the pinned API selects a quota wait."""
        try:
            reset = float(value)
        except (TypeError, ValueError) as exc:
            raise PacingError("rate-limit reset evidence is invalid") from exc
        if reset < 0 or reset == float("inf") or reset != reset:
            raise PacingError("rate-limit reset evidence is invalid")
        current = self.clock()
        with self._lock:
            self._begin()
            try:
                row = self._row()
                boundary = float(row["next_request_at"] or 0)
                if reset > boundary:
                    boundary = reset
                    reason = "rate_limit"
                else:
                    reason = str(row["not_before_reason"] or "spacing")
                self.connection.execute(
                    """UPDATE pacing SET next_request_at=?,
                              not_before_reason=?,last_rate_limit_at=?,updated_at=?
                         WHERE singleton=1""",
                    (boundary, reason, reset, _iso_epoch(current)),
                )
                self._commit()
            except BaseException:
                self._rollback()
                raise


def options_as_runner_args(options: SchedulerOptions) -> list[str]:
    """Return private arguments without exposing them in normal CLI help."""
    delay = (
        str(options.delay_low)
        if options.delay_low == options.delay_high
        else f"{options.delay_low}-{options.delay_high}"
    )
    return [
        SCHEDULER_DB_OPTION,
        str(options.database),
        SCHEDULER_SCOPE_OPTION,
        options.scope_id,
        SCHEDULER_DELAY_OPTION,
        delay,
        SCHEDULER_LEASE_OPTION,
        str(options.lease_seconds),
        SCHEDULER_429_OPTION,
        str(options.backoff_429_seconds),
    ]
