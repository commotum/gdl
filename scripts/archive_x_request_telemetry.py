#!/usr/bin/env python3
"""Private, sanitized actual-request telemetry for pinned X archive runners.

This module observes transport boundaries.  It intentionally never persists a
URL, hostname, query, request/response headers, cookies, body, post ID, handle,
or opaque cursor.  Callers supply one fixed operation label from an allowlist.
"""

from __future__ import annotations

import collections
import contextlib
import contextvars
import http.client
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import urlsplit

import requests
import urllib3.connection
import urllib3.connectionpool


SCHEMA_NAME = "gdl-x-request-telemetry"
SCHEMA_VERSION = 2
MAX_EVENTS = 20_000

TELEMETRY_OPTION = "--archive-x-request-telemetry"
OPERATION_OPTION = "--archive-x-operation"

VALID_OPERATIONS = frozenset(
    {
        "info",
        "timeline",
        "retry_media",
        "profile_avatar",
        "profile_background",
        "context_metadata",
        "context_exact",
        "context_media",
        "direct_media",
        "descriptor_refresh",
        "legacy_walk",
    }
)

X_WEB_HOSTS = frozenset(
    {
        "x.com",
        "www.x.com",
        "mobile.x.com",
        "twitter.com",
        "www.twitter.com",
        "mobile.twitter.com",
    }
)
X_API_HOSTS = frozenset(
    {
        "api.x.com",
        "api.twitter.com",
        "upload.twitter.com",
    }
)
MEDIA_CDN_HOSTS = frozenset(
    {
        "pbs.twimg.com",
        "video.twimg.com",
        "ton.twimg.com",
    }
)
X_SUPPORT_HOSTS = frozenset(
    {
        "abs.twimg.com",
        "platform.twitter.com",
        "syndication.twitter.com",
    }
)
X_REDIRECT_HOSTS = frozenset({"t.co", "www.t.co"})

API_OPERATION_LABELS = {
    "UserByScreenName": "user_profile",
    "UserByRestId": "user_profile",
    "TweetResultByRestId": "tweet_result",
    "TweetDetail": "tweet_detail",
    "UserTweets": "user_tweets",
    "UserTweetsAndReplies": "user_tweets_replies",
    "SearchTimeline": "search_timeline",
}
SAFE_CATEGORIES = frozenset(
    {"x_api", "x_support", "media_cdn", "x_redirect", "external"}
)
SAFE_ENDPOINTS = frozenset(
    set(API_OPERATION_LABELS.values())
    | {
        "x_api_other",
        "media_asset",
        "client_bootstrap",
        "x_web",
        "x_redirect",
        "external_http",
    }
)
SAFE_TRANSPORTS = frozenset({"urllib3", "urllib", "fixture"})
SAFE_SESSIONS = frozenset({"requests", "urllib"})
SAFE_CONNECTION_RE = re.compile(
    r"(?:urllib3_http|urllib3_https|stdlib_http):"
    r"(?:x_api|x_support|media_cdn|x_redirect|external)\Z"
)

SAFE_ERROR_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,79}\Z")
SAFE_METHOD_RE = re.compile(r"[A-Z]{1,12}\Z")
SAFE_WAIT_SOURCES = frozenset(
    {
        "none",
        "spacing",
        "rate_limit",
        "http_429",
        "active_lease",
        "multiple",
    }
)
CURRENT_RATE_LIMIT_THRESHOLD: contextvars.ContextVar[int | None] = (
    contextvars.ContextVar("archive_x_rate_limit_threshold", default=None)
)


class RequestTelemetryError(ValueError):
    """Raised for invalid private runner telemetry options or artifacts."""


@contextlib.contextmanager
def rate_limit_threshold(value: int):
    if value not in {1, 2, 3, 4, 5}:
        raise RequestTelemetryError("rate-limit threshold is invalid")
    token = CURRENT_RATE_LIMIT_THRESHOLD.set(value)
    try:
        yield
    finally:
        CURRENT_RATE_LIMIT_THRESHOLD.reset(token)


def parse_runner_options(
    argv: list[str],
) -> tuple[Path | None, str | None, list[str]]:
    """Remove the two private telemetry flags from gallery-dl arguments."""
    path = None
    operation = None
    remaining: list[str] = []
    index = 0
    while index < len(argv):
        value = argv[index]
        if value in {TELEMETRY_OPTION, OPERATION_OPTION}:
            if index + 1 >= len(argv):
                raise RequestTelemetryError(f"{value} requires a value")
            option = argv[index + 1]
            if value == TELEMETRY_OPTION:
                if path is not None:
                    raise RequestTelemetryError(
                        "request telemetry path was provided more than once"
                    )
                path = Path(option)
            else:
                if operation is not None:
                    raise RequestTelemetryError(
                        "request telemetry operation was provided more than once"
                    )
                operation = option
            index += 2
            continue
        remaining.append(value)
        index += 1

    if (path is None) != (operation is None):
        raise RequestTelemetryError(
            "request telemetry path and operation are required together"
        )
    if operation is not None and operation not in VALID_OPERATIONS:
        raise RequestTelemetryError(
            f"unsupported request telemetry operation: {operation!r}"
        )
    return path, operation, remaining


def _normalized_host(value: str | None) -> str:
    return str(value or "").rstrip(".").lower()


def _host_category(host: str) -> str:
    host = _normalized_host(host)
    if host in MEDIA_CDN_HOSTS:
        return "media_cdn"
    if host in X_REDIRECT_HOSTS:
        return "x_redirect"
    if host in X_API_HOSTS:
        return "x_api"
    if host in X_WEB_HOSTS or host in X_SUPPORT_HOSTS:
        return "x_support"
    if host.endswith(".twimg.com"):
        return "x_support"
    return "external"


def classify_url(value: Any) -> tuple[str | None, str | None]:
    """Return a coarse category and allowlisted endpoint label."""
    try:
        parsed = urlsplit(str(value))
    except (TypeError, ValueError):
        return None, None
    if parsed.scheme.lower() not in {"http", "https"}:
        return None, None

    host = _normalized_host(parsed.hostname)
    category = _host_category(host)
    path = parsed.path or ""
    if category == "x_support" and (
        host in X_API_HOSTS or "/i/api/" in path
    ):
        category = "x_api"

    if category == "x_api":
        leaf = path.rstrip("/").rsplit("/", 1)[-1]
        endpoint = API_OPERATION_LABELS.get(leaf, "x_api_other")
    elif category == "media_cdn":
        endpoint = "media_asset"
    elif category == "x_redirect":
        endpoint = "x_redirect"
    elif category == "x_support":
        endpoint = (
            "client_bootstrap"
            if "/i/js_inst" in path or path.endswith(".js")
            else "x_web"
        )
    else:
        endpoint = "external_http"
    return category, endpoint


def _safe_method(value: Any) -> str:
    method = str(value or "GET").upper()
    return method if SAFE_METHOD_RE.fullmatch(method) else "OTHER"


def _safe_status(value: Any) -> int | None:
    try:
        status = int(value)
    except (TypeError, ValueError):
        return None
    return status if 100 <= status <= 599 else None


def _safe_content_length(headers: Any) -> int | None:
    try:
        value = headers.get("Content-Length")
        if value is None:
            value = headers.get("content-length")
        size = int(value)
    except (AttributeError, TypeError, ValueError):
        return None
    return size if size >= 0 else None


def _response_headers(response: Any) -> Any:
    headers = getattr(response, "headers", None)
    if headers is not None:
        return headers
    info = getattr(response, "info", None)
    return info() if callable(info) else None


def _pool_request_url(pool: Any, value: Any) -> str:
    """Construct a URL only in memory for safe category classification."""
    text = str(value or "")
    if text.startswith(("http://", "https://")):
        return text
    scheme = str(getattr(pool, "scheme", "https") or "https")
    host = str(getattr(pool, "host", "") or "")
    if not text.startswith("/"):
        text = "/" + text
    return f"{scheme}://{host}{text}"


def _safe_error_name(exc: BaseException) -> str:
    name = exc.__class__.__name__
    return name if SAFE_ERROR_RE.fullmatch(name) else "RequestError"


class RequestRecorder:
    """Thread-safe sanitized recorder for one gallery-dl runner process."""

    def __init__(
        self,
        path: Path,
        operation: str,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        max_events: int = MAX_EVENTS,
        request_gate: Any = None,
        runner_starts: int = 1,
    ) -> None:
        if operation not in VALID_OPERATIONS:
            raise RequestTelemetryError(
                f"unsupported request telemetry operation: {operation!r}"
            )
        if max_events < 1:
            raise RequestTelemetryError("request telemetry event limit is invalid")
        if runner_starts not in {0, 1}:
            raise RequestTelemetryError("request telemetry runner-start count is invalid")
        self.path = path
        self.operation = operation
        self.clock = clock
        self.wall_clock = wall_clock
        self.max_events = max_events
        self.request_gate = request_gate
        self.runner_starts = runner_starts
        self.started_monotonic = clock()
        self.started_epoch = wall_clock()
        self._lock = threading.Lock()
        self._sequence = 0
        self._active = 0
        self._peak_concurrency = 0
        self._first_start_ms: int | None = None
        self._last_start_ms: int | None = None
        self._minimum_start_gap_ms: int | None = None
        self._events: list[dict[str, Any]] = []
        self._events_truncated = 0
        self._category_counts: collections.Counter[str] = collections.Counter()
        self._endpoint_counts: collections.Counter[str] = collections.Counter()
        self._transport_counts: collections.Counter[str] = collections.Counter()
        self._status_counts: collections.Counter[str] = collections.Counter()
        self._connection_counts: collections.Counter[str] = collections.Counter()
        self._session_counts: collections.Counter[str] = collections.Counter()
        self._advertised_bytes = 0
        self._advertised_bytes_unknown = 0
        self._failures = 0
        self._redirects = 0
        self._pacing_wait_ms = 0
        self._wait_source_counts: collections.Counter[str] = collections.Counter()

    def _begin(self) -> tuple[int, float, int]:
        started = self.clock()
        with self._lock:
            self._sequence += 1
            sequence = self._sequence
            self._active += 1
            self._peak_concurrency = max(self._peak_concurrency, self._active)
            offset_ms = max(
                0, round((started - self.started_monotonic) * 1000)
            )
            if self._first_start_ms is None:
                self._first_start_ms = offset_ms
            if self._last_start_ms is not None:
                gap = max(0, offset_ms - self._last_start_ms)
                self._minimum_start_gap_ms = (
                    gap
                    if self._minimum_start_gap_ms is None
                    else min(self._minimum_start_gap_ms, gap)
                )
            self._last_start_ms = offset_ms
        return sequence, started, offset_ms

    def _finish(
        self,
        *,
        sequence: int,
        started: float,
        offset_ms: int,
        transport: str,
        category: str,
        endpoint: str,
        method: str,
        status: int | None,
        content_length: int | None,
        error: str | None,
        pacing_wait_ms: int,
        pacing_wait_source: str,
    ) -> None:
        completed = self.clock()
        event = {
            "sequence": sequence,
            "offset_ms": offset_ms,
            "elapsed_ms": max(0, round((completed - started) * 1000)),
            "transport": transport,
            "category": category,
            "endpoint": endpoint,
            "method": method,
            "status": status,
            "advertised_bytes": content_length,
            "redirect": bool(status is not None and 300 <= status <= 399),
            "error": error,
            "pacing_wait_ms": pacing_wait_ms,
            "pacing_wait_source": pacing_wait_source,
        }
        with self._lock:
            self._active = max(0, self._active - 1)
            self._category_counts[category] += 1
            self._endpoint_counts[endpoint] += 1
            self._transport_counts[transport] += 1
            self._status_counts[str(status) if status is not None else "error"] += 1
            if content_length is None:
                self._advertised_bytes_unknown += 1
            else:
                self._advertised_bytes += content_length
            if error is not None:
                self._failures += 1
            if event["redirect"]:
                self._redirects += 1
            self._pacing_wait_ms += pacing_wait_ms
            self._wait_source_counts[pacing_wait_source] += 1
            if len(self._events) < self.max_events:
                self._events.append(event)
            else:
                self._events_truncated += 1

    def observe(
        self,
        *,
        transport: str,
        url: Any,
        method: Any,
        send: Callable[[], Any],
        status_getter: Callable[[Any], Any],
        headers_getter: Callable[[Any], Any],
    ) -> Any:
        category, endpoint = classify_url(url)
        if category is None or endpoint is None:
            return send()
        reservation = None
        if self.request_gate is not None:
            reservation = self.request_gate.reserve(category, endpoint)
        wait_seconds = float(getattr(reservation, "waited_seconds", 0) or 0)
        wait_source = str(getattr(reservation, "wait_source", "none") or "none")
        if wait_seconds < 0 or wait_seconds == float("inf") or wait_seconds != wait_seconds:
            raise RequestTelemetryError("request pacing wait is invalid")
        if wait_source not in SAFE_WAIT_SOURCES:
            raise RequestTelemetryError("request pacing wait source is invalid")
        sequence, started, offset_ms = self._begin()
        response = None
        error = None
        status = None
        headers = None
        completion_error = None
        try:
            response = send()
            return response
        except BaseException as exc:
            error = _safe_error_name(exc)
            status = _safe_status(getattr(exc, "code", None))
            headers = getattr(exc, "headers", None)
            raise
        finally:
            if response is not None:
                status = _safe_status(status_getter(response))
                headers = headers_getter(response)
            elif error is None:
                status = None
                headers = None
            if self.request_gate is not None:
                try:
                    self.request_gate.complete(
                        reservation,
                        status=status,
                        headers=headers,
                        error=error,
                        rate_limit_threshold=CURRENT_RATE_LIMIT_THRESHOLD.get(),
                    )
                except BaseException as exc:
                    completion_error = exc
            self._finish(
                sequence=sequence,
                started=started,
                offset_ms=offset_ms,
                transport=transport,
                category=category,
                endpoint=endpoint,
                method=_safe_method(method),
                status=status,
                content_length=_safe_content_length(headers),
                error=error,
                pacing_wait_ms=max(0, round(wait_seconds * 1000)),
                pacing_wait_source=wait_source,
            )
            if completion_error is not None:
                raise completion_error

    def record_connection(self, host: Any, transport: str) -> None:
        category = _host_category(str(host or ""))
        with self._lock:
            self._connection_counts[f"{transport}:{category}"] += 1

    def record_session(self, transport: str) -> None:
        with self._lock:
            self._session_counts[transport] += 1

    @contextlib.contextmanager
    def capture(self) -> Iterator[None]:
        original_requests_init = requests.sessions.Session.__init__
        original_urllib_init = urllib.request.OpenerDirector.__init__
        original_pool_request = (
            urllib3.connectionpool.HTTPConnectionPool._make_request
        )
        original_urllib_do_open = urllib.request.AbstractHTTPHandler.do_open
        original_urllib3_http_connect = urllib3.connection.HTTPConnection.connect
        original_urllib3_https_connect = urllib3.connection.HTTPSConnection.connect
        original_http_connect = http.client.HTTPConnection.connect

        recorder = self

        def pool_request(
            pool,
            connection,
            method,
            url,
            *args,
            **kwargs,
        ):
            return recorder.observe(
                transport="urllib3",
                url=_pool_request_url(pool, url),
                method=method,
                send=lambda: original_pool_request(
                    pool,
                    connection,
                    method,
                    url,
                    *args,
                    **kwargs,
                ),
                status_getter=lambda response: getattr(response, "status", None),
                headers_getter=_response_headers,
            )

        def requests_init(session, *args, **kwargs):
            original_requests_init(session, *args, **kwargs)
            recorder.record_session("requests")

        def urllib_do_open(handler, http_class, request, **http_conn_args):
            return recorder.observe(
                transport="urllib",
                url=getattr(request, "full_url", None),
                method=(
                    request.get_method()
                    if hasattr(request, "get_method")
                    else "GET"
                ),
                send=lambda: original_urllib_do_open(
                    handler, http_class, request, **http_conn_args
                ),
                status_getter=lambda response: getattr(
                    response, "status", getattr(response, "code", None)
                ),
                headers_getter=_response_headers,
            )

        def urllib_init(opener, *args, **kwargs):
            original_urllib_init(opener, *args, **kwargs)
            recorder.record_session("urllib")

        def urllib3_http_connect(connection, *args, **kwargs):
            recorder.record_connection(connection.host, "urllib3_http")
            return original_urllib3_http_connect(connection, *args, **kwargs)

        def urllib3_https_connect(connection, *args, **kwargs):
            recorder.record_connection(connection.host, "urllib3_https")
            return original_urllib3_https_connect(connection, *args, **kwargs)

        def stdlib_connect(connection, *args, **kwargs):
            recorder.record_connection(connection.host, "stdlib_http")
            return original_http_connect(connection, *args, **kwargs)

        requests.sessions.Session.__init__ = requests_init
        urllib.request.OpenerDirector.__init__ = urllib_init
        urllib3.connectionpool.HTTPConnectionPool._make_request = pool_request
        urllib.request.AbstractHTTPHandler.do_open = urllib_do_open
        urllib3.connection.HTTPConnection.connect = urllib3_http_connect
        urllib3.connection.HTTPSConnection.connect = urllib3_https_connect
        http.client.HTTPConnection.connect = stdlib_connect
        try:
            yield
        finally:
            requests.sessions.Session.__init__ = original_requests_init
            urllib.request.OpenerDirector.__init__ = original_urllib_init
            urllib3.connectionpool.HTTPConnectionPool._make_request = (
                original_pool_request
            )
            urllib.request.AbstractHTTPHandler.do_open = original_urllib_do_open
            urllib3.connection.HTTPConnection.connect = original_urllib3_http_connect
            urllib3.connection.HTTPSConnection.connect = original_urllib3_https_connect
            http.client.HTTPConnection.connect = original_http_connect

    def value(self, exit_code: int) -> dict[str, Any]:
        completed_epoch = self.wall_clock()
        with self._lock:
            events = sorted(self._events, key=lambda event: event["sequence"])
            summary = {
                "operation": self.operation,
                "runner_starts": self.runner_starts,
                "actual_requests": self._sequence,
                "by_category": dict(sorted(self._category_counts.items())),
                "by_endpoint": dict(sorted(self._endpoint_counts.items())),
                "by_transport": dict(sorted(self._transport_counts.items())),
                "by_status": dict(sorted(self._status_counts.items())),
                "connections": dict(sorted(self._connection_counts.items())),
                "sessions": dict(sorted(self._session_counts.items())),
                "peak_concurrency": self._peak_concurrency,
                "redirects": self._redirects,
                "failures": self._failures,
                "pacing_wait_ms": self._pacing_wait_ms,
                "by_wait_source": dict(sorted(self._wait_source_counts.items())),
                "advertised_bytes": self._advertised_bytes,
                "advertised_bytes_unknown": self._advertised_bytes_unknown,
                "events_retained": len(events),
                "events_truncated": self._events_truncated,
                "first_request_offset_ms": self._first_start_ms,
                "last_request_offset_ms": self._last_start_ms,
                "minimum_start_gap_ms": self._minimum_start_gap_ms,
                "request_span_ms": (
                    max(0, self._last_start_ms - self._first_start_ms)
                    if self._first_start_ms is not None
                    and self._last_start_ms is not None
                    else 0
                ),
            }
        return {
            "schema": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
            "operation": self.operation,
            "started_at_epoch": round(self.started_epoch, 3),
            "completed_at_epoch": round(completed_epoch, 3),
            "duration_ms": max(
                0, round((completed_epoch - self.started_epoch) * 1000)
            ),
            "exit_code": int(exit_code),
            "sensitive_values_persisted": False,
            "summary": summary,
            "events": events,
        }

    def write(self, exit_code: int) -> None:
        value = self.value(exit_code)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(value, file, ensure_ascii=True, indent=2, sort_keys=True)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.path)

    def safe_write(self, exit_code: int) -> str | None:
        """Write telemetry without ever changing the runner's archive result."""
        try:
            self.write(exit_code)
        except Exception as exc:  # observability must be outcome-neutral
            return _safe_error_name(exc)
        return None


def read_summary(path: Path, *, expected_operation: str) -> dict[str, Any]:
    """Validate a private artifact and return only its sanitized summary."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RequestTelemetryError("request telemetry is unreadable") from exc
    if not isinstance(value, dict):
        raise RequestTelemetryError("request telemetry must be an object")
    schema_version = value.get("schema_version")
    if value.get("schema") != SCHEMA_NAME or schema_version not in {1, 2}:
        raise RequestTelemetryError("request telemetry schema is invalid")
    if value.get("operation") != expected_operation:
        raise RequestTelemetryError("request telemetry operation is invalid")
    if value.get("sensitive_values_persisted") is not False:
        raise RequestTelemetryError("request telemetry privacy marker is invalid")
    summary = value.get("summary")
    if not isinstance(summary, dict):
        raise RequestTelemetryError("request telemetry summary is invalid")
    if schema_version == 1:
        summary = dict(summary)
        summary["pacing_wait_ms"] = 0
        summary["by_wait_source"] = (
            {"none": int(summary.get("actual_requests") or 0)}
            if summary.get("actual_requests")
            else {}
        )
    required = {
        "operation",
        "runner_starts",
        "actual_requests",
        "by_category",
        "by_endpoint",
        "by_transport",
        "by_status",
        "connections",
        "sessions",
        "peak_concurrency",
        "redirects",
        "failures",
        "pacing_wait_ms",
        "by_wait_source",
        "advertised_bytes",
        "advertised_bytes_unknown",
        "events_retained",
        "events_truncated",
        "first_request_offset_ms",
        "last_request_offset_ms",
        "minimum_start_gap_ms",
        "request_span_ms",
    }
    if set(summary) != required:
        raise RequestTelemetryError("request telemetry summary fields are invalid")
    if summary["operation"] != expected_operation:
        raise RequestTelemetryError("request telemetry summary operation is invalid")
    if not all(
        isinstance(summary[name], int) and summary[name] >= 0
        for name in required
        if name not in {
            "operation",
            "by_category",
            "by_endpoint",
            "by_transport",
            "by_status",
            "connections",
            "sessions",
            "first_request_offset_ms",
            "last_request_offset_ms",
            "minimum_start_gap_ms",
            "by_wait_source",
        }
    ):
        raise RequestTelemetryError("request telemetry counters are invalid")
    actual = summary["actual_requests"]
    first = summary["first_request_offset_ms"]
    last = summary["last_request_offset_ms"]
    gap = summary["minimum_start_gap_ms"]
    if actual == 0:
        timing_valid = first is None and last is None and gap is None
    else:
        timing_valid = (
            isinstance(first, int)
            and first >= 0
            and isinstance(last, int)
            and last >= first
            and (
                gap is None
                if actual == 1
                else isinstance(gap, int) and gap >= 0
            )
        )
    if not timing_valid:
        raise RequestTelemetryError("request telemetry timing is invalid")
    for name in (
        "by_category",
        "by_endpoint",
        "by_transport",
        "by_status",
        "connections",
        "sessions",
        "by_wait_source",
    ):
        counts = summary[name]
        if not isinstance(counts, dict) or not all(
            isinstance(key, str) and isinstance(count, int) and count >= 0
            for key, count in counts.items()
        ):
            raise RequestTelemetryError("request telemetry groups are invalid")
    if not set(summary["by_category"]) <= SAFE_CATEGORIES:
        raise RequestTelemetryError("request telemetry category is invalid")
    if not set(summary["by_endpoint"]) <= SAFE_ENDPOINTS:
        raise RequestTelemetryError("request telemetry endpoint is invalid")
    if not set(summary["by_transport"]) <= SAFE_TRANSPORTS:
        raise RequestTelemetryError("request telemetry transport is invalid")
    if not set(summary["sessions"]) <= SAFE_SESSIONS:
        raise RequestTelemetryError("request telemetry session is invalid")
    if not all(SAFE_CONNECTION_RE.fullmatch(key) for key in summary["connections"]):
        raise RequestTelemetryError("request telemetry connection is invalid")
    if not all(
        key == "error" or key.isdecimal() and 100 <= int(key) <= 599
        for key in summary["by_status"]
    ):
        raise RequestTelemetryError("request telemetry status is invalid")
    if not set(summary["by_wait_source"]) <= SAFE_WAIT_SOURCES:
        raise RequestTelemetryError("request telemetry wait source is invalid")
    actual = summary["actual_requests"]
    if any(
        sum(summary[name].values()) != actual
        for name in ("by_category", "by_endpoint", "by_transport", "by_status")
    ):
        raise RequestTelemetryError("request telemetry group totals are invalid")
    if sum(summary["by_wait_source"].values()) != actual:
        raise RequestTelemetryError("request telemetry wait total is invalid")
    if (
        summary["runner_starts"] not in {0, 1}
        or summary["events_retained"] + summary["events_truncated"] != actual
        or summary["peak_concurrency"] > actual
        or summary["redirects"] > actual
        or summary["failures"] > actual
        or summary["advertised_bytes_unknown"] > actual
        or (
            actual > 0
            and summary["request_span_ms"] != last - first
        )
        or (actual == 0 and summary["request_span_ms"] != 0)
    ):
        raise RequestTelemetryError("request telemetry aggregate is inconsistent")
    return json.loads(json.dumps(summary, sort_keys=True))
