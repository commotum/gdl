#!/usr/bin/env python3
"""Pinned gallery-dl runner for bounded, auditable legacy X search walks."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import sys
import textwrap
from pathlib import Path
from typing import Any

import gallery_dl
from gallery_dl.extractor.twitter import (
    TwitterAPI,
    TwitterSearchExtractor,
)

import gallery_dl_x_runner as base_runner


TELEMETRY_SCHEMA_VERSION = 1
SEARCH_TIMELINE_SUFFIX = "SearchTimeline"
SUPPORTED_SEARCH_TIMELINE_SHA256 = (
    "a6a27d4168ae98bee3ed1608bd8c8acec674d07e5ff4acad9651b20af32a48c3"
)
SUPPORTED_PAGINATION_TWEETS_SHA256 = (
    "6857fde6c5b21099cb52d5503d58f938f19796137fe9e680d34114dc93b5f69c"
)
SUPPORTED_SEARCH_EXTRACTOR_SHA256 = (
    "dbb0ddd1a4d7ad39421407a8865c64e085f7fcb5b7f703e786204df50a0a0dc1"
)


def source_sha256(value: Any) -> str:
    source = textwrap.dedent(inspect.getsource(value)).encode("utf-8")
    return hashlib.sha256(source).hexdigest()


def require_supported_legacy_gallery_dl() -> None:
    base_runner.require_supported_gallery_dl()
    targets = (
        (
            "TwitterAPI.search_timeline",
            TwitterAPI.search_timeline,
            SUPPORTED_SEARCH_TIMELINE_SHA256,
        ),
        (
            "TwitterAPI._pagination_tweets",
            TwitterAPI._pagination_tweets,
            SUPPORTED_PAGINATION_TWEETS_SHA256,
        ),
        (
            "TwitterSearchExtractor",
            TwitterSearchExtractor,
            SUPPORTED_SEARCH_EXTRACTOR_SHA256,
        ),
    )
    for name, value, expected in targets:
        try:
            actual = source_sha256(value)
        except (OSError, TypeError) as exc:
            raise base_runner.ShimCompatibilityError(
                f"cannot verify {name} source"
            ) from exc
        if actual != expected:
            raise base_runner.ShimCompatibilityError(
                f"{name} does not match the supported gallery-dl implementation"
            )


def digest_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def bottom_cursor(value: Any) -> Any:
    found = []

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            if str(item.get("entryId") or "").startswith("cursor-bottom-"):
                content = item.get("content") or {}
                if isinstance(content, dict) and "itemContent" in content:
                    content = content["itemContent"]
                if isinstance(content, dict):
                    found.append(content.get("value"))
            for child in item.values():
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return found[-1] if found else None


def tweet_entry_count(value: Any) -> int:
    entry_ids: set[str] = set()

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            entry_id = str(item.get("entryId") or "")
            if entry_id.startswith(("tweet-", "profile-conversation-")):
                entry_ids.add(entry_id)
            for child in item.values():
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return len(entry_ids)


def profile_user_ids(value: Any) -> list[str]:
    try:
        result = value["data"]["user"]["result"]
        while isinstance(result, dict) and isinstance(result.get("result"), dict):
            result = result["result"]
        rest_id = str(result["rest_id"])
        if rest_id.isdecimal():
            return [rest_id]
    except (KeyError, TypeError):
        pass

    found: set[str] = set()

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            rest_id = item.get("rest_id")
            legacy = item.get("legacy")
            core = item.get("core")
            if (
                rest_id is not None
                and str(rest_id).isdecimal()
                and (
                    (isinstance(legacy, dict) and legacy.get("screen_name"))
                    or (isinstance(core, dict) and core.get("screen_name"))
                )
            ):
                found.add(str(rest_id))
            for child in item.values():
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return sorted(found, key=int)


def terminal_reason(pages: list[dict[str, Any]], status: int, capped: bool) -> str:
    if capped:
        return "request_cap"
    if any(page["api_error_count"] for page in pages):
        return "api_error"
    if any(page["cursor_repeated"] for page in pages):
        return "repeated_cursor"
    if status != 0:
        return "process_error"
    if not pages:
        return "no_search_response"
    if pages[-1]["returned_cursor_sha256"] is None:
        return "no_cursor"
    tail = pages[-4:]
    returned = [page["returned_cursor_sha256"] for page in tail]
    if (
        len(tail) == 4
        and all(page["tweet_entry_count"] == 0 for page in tail)
        and all(page["api_error_count"] == 0 for page in tail)
        and all(returned)
        and len(set(returned)) == 4
    ):
        return "distinct_empty_tail"
    return "ambiguous"


class TelemetryRecorder:
    def __init__(self, path: Path, request_limit: int):
        self.path = path
        self.request_limit = request_limit
        self.api_requests = 0
        self.search_requests = 0
        self.capped = False
        self.pages: list[dict[str, Any]] = []
        self.profile_user_ids: set[str] = set()

    def call(self, original, api, endpoint, params, *args, **kwargs):
        is_search = endpoint.endswith(SEARCH_TIMELINE_SUFFIX)
        if is_search and self.search_requests >= self.request_limit:
            self.capped = True
            raise api.exc.AbortExtraction(
                f"legacy SearchTimeline request cap ({self.request_limit}) reached"
            )
        self.api_requests += 1
        submitted_cursor = None
        query = None
        if is_search:
            self.search_requests += 1
            try:
                variables = json.loads(params.get("variables") or "{}")
                submitted_cursor = variables.get("cursor")
                query = variables.get("rawQuery")
            except (TypeError, ValueError):
                pass
        data = original(api, endpoint, params, *args, **kwargs)
        if is_search:
            returned_cursor = bottom_cursor(data)
            self.pages.append(
                {
                    "request_number": self.search_requests,
                    "query_sha256": digest_text(query),
                    "submitted_cursor_sha256": digest_text(submitted_cursor),
                    "returned_cursor_sha256": digest_text(returned_cursor),
                    "cursor_repeated": bool(
                        submitted_cursor
                        and returned_cursor
                        and submitted_cursor == returned_cursor
                    ),
                    "tweet_entry_count": tweet_entry_count(data),
                    "api_error_count": len(data.get("errors") or []),
                }
            )
        elif endpoint.endswith("UserByScreenName"):
            self.profile_user_ids.update(profile_user_ids(data))
        return data

    def value(self, status: int) -> dict[str, Any]:
        return {
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "request_limit": self.request_limit,
            "api_requests": self.api_requests,
            "search_requests": self.search_requests,
            "request_cap_reached": self.capped,
            "terminal_reason": terminal_reason(self.pages, status, self.capped),
            "exit_code": status,
            "pages": self.pages,
            "profile_user_ids": sorted(self.profile_user_ids, key=int),
            "opaque_cursor_values_persisted": False,
        }

    def write(self, status: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(self.value(status), file, indent=2, sort_keys=True)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.path)


def parse_runner_options(argv: list[str]) -> tuple[Path | None, int | None, list[str]]:
    telemetry = None
    request_limit = None
    remaining = []
    index = 0
    while index < len(argv):
        value = argv[index]
        if value in {"--archive-x-legacy-telemetry", "--archive-x-legacy-request-limit"}:
            if index + 1 >= len(argv):
                raise ValueError(f"{value} requires a value")
            option = argv[index + 1]
            if value.endswith("telemetry"):
                telemetry = Path(option)
            else:
                request_limit = int(option)
            index += 2
            continue
        remaining.append(value)
        index += 1
    if (telemetry is None) != (request_limit is None):
        raise ValueError("legacy telemetry path and request limit are required together")
    if request_limit is not None and request_limit < 1:
        raise ValueError("legacy request limit must be positive")
    return telemetry, request_limit, remaining


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    try:
        telemetry_path, request_limit, gallery_args = parse_runner_options(values)
        require_supported_legacy_gallery_dl()
        base_runner.install_patch()
    except (
        ValueError,
        base_runner.ShimCompatibilityError,
    ) as exc:
        print(f"gallery-dl X legacy runner: {exc}", file=sys.stderr)
        return 32

    if telemetry_path is None:
        original_argv = sys.argv
        try:
            sys.argv = [original_argv[0], *gallery_args]
            return gallery_dl.main()
        finally:
            sys.argv = original_argv

    recorder = TelemetryRecorder(telemetry_path, request_limit)
    original_call = TwitterAPI._call
    original_checkpoint = base_runner._checkpoint_cursor

    def observed_call(api, endpoint, params, *args, **kwargs):
        return recorder.call(original_call, api, endpoint, params, *args, **kwargs)

    # A legacy restart always replays its fixed query. The opaque cursor has no
    # recovery authority, so redact it from the base runner's quota checkpoint.
    base_runner._checkpoint_cursor = lambda _api: "legacy-cursor-redacted"
    TwitterAPI._call = observed_call
    status = 1
    original_argv = sys.argv
    try:
        sys.argv = [original_argv[0], *gallery_args]
        result = gallery_dl.main()
        status = int(result or 0)
    finally:
        sys.argv = original_argv
        TwitterAPI._call = original_call
        base_runner._checkpoint_cursor = original_checkpoint
        recorder.write(status)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
