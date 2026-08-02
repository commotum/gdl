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

from gallery_dl import text
from gallery_dl.extractor.twitter import (
    TwitterAPI,
    TwitterSearchExtractor,
)

import gallery_dl_x_runner as base_runner


TELEMETRY_SCHEMA_VERSION = 1
BOUND_USER_ID_OPTION = "--archive-x-legacy-bound-user-id"
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


def terminal_reason(
    pages: list[dict[str, Any]],
    status: int,
    capped: bool,
    empty_tail_pages: int,
) -> str:
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
    tail = pages[-empty_tail_pages:]
    returned = [page["returned_cursor_sha256"] for page in tail]
    if (
        len(tail) == empty_tail_pages
        and all(page["tweet_entry_count"] == 0 for page in tail)
        and all(page["api_error_count"] == 0 for page in tail)
        and all(returned)
        and len(set(returned)) == empty_tail_pages
    ):
        return "distinct_empty_tail"
    return "ambiguous"


class TelemetryRecorder:
    def __init__(
        self,
        path: Path,
        request_limit: int,
        empty_tail_pages: int,
        bound_user_id: str | None = None,
    ):
        self.path = path
        self.request_limit = request_limit
        self.empty_tail_pages = empty_tail_pages
        self.api_requests = 0
        self.search_requests = 0
        self.capped = False
        self.pages: list[dict[str, Any]] = []
        self.profile_user_ids: set[str] = (
            {bound_user_id} if bound_user_id is not None else set()
        )
        self.profile_requests = 0
        self.identity_source = (
            "bound_numeric_id" if bound_user_id is not None else "profile_api"
        )

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
            self.profile_requests += 1
            self.profile_user_ids.update(profile_user_ids(data))
        return data

    def value(self, status: int) -> dict[str, Any]:
        return {
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "request_limit": self.request_limit,
            "empty_tail_pages": self.empty_tail_pages,
            "api_requests": self.api_requests,
            "search_requests": self.search_requests,
            "request_cap_reached": self.capped,
            "terminal_reason": terminal_reason(
                self.pages,
                status,
                self.capped,
                self.empty_tail_pages,
            ),
            "exit_code": status,
            "pages": self.pages,
            "profile_user_ids": sorted(self.profile_user_ids, key=int),
            "profile_requests": self.profile_requests,
            "identity_source": self.identity_source,
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


def parse_runner_options(
    argv: list[str],
) -> tuple[Path | None, int | None, int | None, str | None, list[str]]:
    telemetry = None
    request_limit = None
    empty_tail_pages = None
    bound_user_id = None
    remaining = []
    index = 0
    while index < len(argv):
        value = argv[index]
        if value in {
            "--archive-x-legacy-telemetry",
            "--archive-x-legacy-request-limit",
            "--archive-x-legacy-empty-tail-pages",
            BOUND_USER_ID_OPTION,
        }:
            if index + 1 >= len(argv):
                raise ValueError(f"{value} requires a value")
            option = argv[index + 1]
            if value.endswith("telemetry"):
                telemetry = Path(option)
            elif value.endswith("empty-tail-pages"):
                empty_tail_pages = int(option)
            elif value == BOUND_USER_ID_OPTION:
                bound_user_id = option
            else:
                request_limit = int(option)
            index += 2
            continue
        remaining.append(value)
        index += 1
    provided = (
        telemetry is not None,
        request_limit is not None,
        empty_tail_pages is not None,
    )
    if any(provided) and not all(provided):
        raise ValueError(
            "legacy telemetry path, request limit, and empty-tail pages "
            "are required together"
        )
    if request_limit is not None and request_limit < 1:
        raise ValueError("legacy request limit must be positive")
    if (
        empty_tail_pages is not None
        and (
            empty_tail_pages < 1
            or request_limit is None
            or empty_tail_pages >= request_limit
        )
    ):
        raise ValueError(
            "legacy empty-tail pages must be positive and below the request limit"
        )
    if bound_user_id is not None and (
        telemetry is None
        or not bound_user_id.isdecimal()
        or int(bound_user_id) < 1
    ):
        raise ValueError(
            "legacy bound user ID requires telemetry and must be numeric"
        )
    return telemetry, request_limit, empty_tail_pages, bound_user_id, remaining


def bound_identity_tweets(extractor: TwitterSearchExtractor, user_id: str):
    """Run one pinned search without repeating UserByScreenName."""
    query = text.unquote(extractor.user.replace("+", " "))
    handle = None
    for item in query.split():
        item = item.strip("()")
        if item.startswith("from:"):
            if handle:
                handle = None
                break
            handle = item[5:]
    if not handle:
        raise base_runner.ShimCompatibilityError(
            "bound legacy search query lacks one account scope"
        )
    extractor._user_obj = {"rest_id": user_id}
    extractor._user = {
        "id": int(user_id),
        "name": handle,
        "nick": handle,
    }
    return extractor.api.search_timeline(query)


def run_once(values: list[str], *, runner_starts: int = 1) -> int:
    try:
        (
            telemetry_path,
            request_limit,
            empty_tail_pages,
            bound_user_id,
            remaining_args,
        ) = parse_runner_options(values)
        request_telemetry_path, operation, remaining_args = (
            base_runner.request_telemetry.parse_runner_options(
                remaining_args
            )
        )
        scheduler_options, gallery_args = base_runner.pacing.parse_runner_options(
            remaining_args
        )
        if scheduler_options is not None and operation is None:
            raise base_runner.pacing.PacingError(
                "request telemetry operation is required with scheduler options"
            )
        require_supported_legacy_gallery_dl()
        base_runner.install_patch()
    except (
        ValueError,
        base_runner.pacing.PacingError,
        base_runner.request_telemetry.RequestTelemetryError,
        base_runner.ShimCompatibilityError,
    ) as exc:
        print(f"gallery-dl X legacy runner: {exc}", file=sys.stderr)
        return 32

    def execute_base() -> int:
        runner_options = {}
        if scheduler_options is not None:
            runner_options["scheduler_options"] = scheduler_options
        if runner_starts != 1:
            runner_options["runner_starts"] = runner_starts
        try:
            return base_runner.run_gallery_args(
                gallery_args,
                request_telemetry_path,
                operation,
                **runner_options,
            )
        except base_runner.pacing.PacingError as exc:
            print(f"gallery-dl X legacy runner: {exc}", file=sys.stderr)
            return 32

    if telemetry_path is None:
        return execute_base()

    recorder = TelemetryRecorder(
        telemetry_path,
        request_limit,
        empty_tail_pages,
        bound_user_id,
    )
    original_call = TwitterAPI._call
    original_checkpoint = base_runner._checkpoint_cursor
    original_search_tweets = TwitterSearchExtractor.tweets

    def observed_call(api, endpoint, params, *args, **kwargs):
        return recorder.call(original_call, api, endpoint, params, *args, **kwargs)

    # A legacy restart always replays its fixed query. The opaque cursor has no
    # recovery authority, so redact it from the base runner's quota checkpoint.
    base_runner._checkpoint_cursor = lambda _api: "legacy-cursor-redacted"
    TwitterAPI._call = observed_call
    if bound_user_id is not None:
        TwitterSearchExtractor.tweets = lambda extractor: bound_identity_tweets(
            extractor, bound_user_id
        )
    status = 1
    try:
        status = execute_base()
    finally:
        TwitterAPI._call = original_call
        TwitterSearchExtractor.tweets = original_search_tweets
        base_runner._checkpoint_cursor = original_checkpoint
        recorder.write(status)
    return status


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    try:
        worker_options, remaining = base_runner.runner_control.parse_worker_options(
            values
        )
        if worker_options is not None and remaining:
            raise base_runner.runner_control.ControlProtocolError(
                "control-worker startup does not accept gallery arguments"
            )
        if worker_options is not None:
            require_supported_legacy_gallery_dl()
            base_runner.install_patch()
    except (
        base_runner.runner_control.ControlProtocolError,
        base_runner.ShimCompatibilityError,
    ) as exc:
        print(f"gallery-dl X legacy runner: {exc}", file=sys.stderr)
        return 32
    if worker_options is not None:
        return base_runner.runner_control.worker_loop(worker_options, run_once)
    return run_once(remaining)


if __name__ == "__main__":
    raise SystemExit(main())
