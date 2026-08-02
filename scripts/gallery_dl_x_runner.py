#!/usr/bin/env python3
"""Run gallery-dl with a narrow X rate-limit fix for version 1.32.4.

gallery-dl 1.32.4 can discard a successful X API response when its rate-limit
headers indicate that the quota is nearly exhausted.  It waits, then repeats
the request instead of processing the response it already received.  For a
timeline near its oldest page, that can become an endless wait/re-fetch loop.

This runner changes only that ordering: a successful low-quota response is
returned to the paginator, and its reset response is remembered.  Immediately
before the next API request, the runner logs the paginator checkpoint and
performs gallery-dl's normal rate-limit handling.  Real HTTP 429 responses keep
their original immediate wait-and-retry behavior.

The patch is deliberately pinned to both gallery-dl's version and the source
fingerprint of the method it replaces.  An upgrade therefore fails closed
instead of applying an old monkey patch to unfamiliar code.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import inspect
import json
import random
import sys
import textwrap
from pathlib import Path
from typing import Any

import gallery_dl
from gallery_dl.extractor.twitter import TwitterAPI, TwitterTweetExtractor


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
import archive_x_request_telemetry as request_telemetry
import archive_x_descriptors as descriptor_capture
import archive_x_pacing as pacing
import archive_x_runner_control as runner_control


SUPPORTED_VERSION = "1.32.4"
SUPPORTED_CALL_SHA256 = (
    "c7c1062eaf240cae86904fad97847c01aeb76b0161ab82671e91686c78a1e7df"
)
SUPPORTED_TWEET_EXTRACTOR_SHA256 = (
    "bea8901624be2021c0dcc4ceaa97d698d3df026e5f0aa06209a678205adfe626"
)
SUPPORTED_TWEET_RESULT_SHA256 = (
    "f97adcfab95ecdd239ad3e1da2e80b980cd30cf472fae7361deb6f5fc7359e1e"
)
DEFERRED_RESPONSE_ATTRIBUTE = "_gdl_x_deferred_ratelimit_response"
RATE_LIMIT_RESET_LOG = "Archive rate-limit reset=%s remaining=%s"
EMPTY_TWEET_RESULT_LOG = (
    "Archive empty TweetResult for %s; confirming once with TweetDetail"
)
RECOVERED_TWEET_RESULT_LOG = "Archive recovered %s through TweetDetail"
BOUNDED_CONVERSATION_CONFIG = "archive-conversation-pages"
BOUNDED_CONVERSATION_LOG = (
    "Archive bounded TweetDetail at one response; skipped continuation cursor"
)
UPSTREAM_TWEET_RESULT_BY_REST_ID = TwitterAPI.tweet_result_by_rest_id


class ShimCompatibilityError(RuntimeError):
    """The installed gallery-dl is not the implementation this shim targets."""


def _source_sha256(function: Any) -> str:
    source = textwrap.dedent(inspect.getsource(function)).encode("utf-8")
    return hashlib.sha256(source).hexdigest()


def require_supported_gallery_dl() -> str:
    """Return the installed version or fail closed when it is not supported."""
    version = importlib.metadata.version("gallery-dl")
    if version != SUPPORTED_VERSION:
        raise ShimCompatibilityError(
            "X rate-limit shim supports gallery-dl "
            f"{SUPPORTED_VERSION} exactly; found {version}"
        )
    try:
        tweet_fingerprint = _source_sha256(TwitterTweetExtractor)
    except (OSError, TypeError) as exc:
        raise ShimCompatibilityError(
            "cannot verify gallery-dl individual Tweet extractor source"
        ) from exc
    if tweet_fingerprint != SUPPORTED_TWEET_EXTRACTOR_SHA256:
        raise ShimCompatibilityError(
            "gallery-dl individual Tweet extractor does not match the "
            f"supported {SUPPORTED_VERSION} implementation"
        )
    current_tweet_result = TwitterAPI.tweet_result_by_rest_id
    if current_tweet_result is not empty_result_safe_tweet_result:
        try:
            tweet_result_fingerprint = _source_sha256(current_tweet_result)
        except (OSError, TypeError) as exc:
            raise ShimCompatibilityError(
                "cannot verify gallery-dl TweetResultByRestId source"
            ) from exc
        if tweet_result_fingerprint != SUPPORTED_TWEET_RESULT_SHA256:
            raise ShimCompatibilityError(
                "gallery-dl TweetResultByRestId does not match the "
                f"supported {SUPPORTED_VERSION} implementation"
            )
    return version


def _remember_deferred_ratelimit(api: TwitterAPI, response: Any) -> None:
    setattr(api, DEFERRED_RESPONSE_ATTRIBUTE, response)
    reset = response.headers.get("x-rate-limit-reset")
    remaining = response.headers.get("x-rate-limit-remaining")
    if reset:
        # Individual-post extractors may exit before another API call applies
        # the deferred wait. Emit only non-secret quota state so an outer
        # sequential worker can persist the same not-before boundary.
        api.log.info(RATE_LIMIT_RESET_LOG, reset, remaining or "unknown")


def _checkpoint_cursor(api: TwitterAPI) -> Any:
    extractor = api.extractor
    cursor = getattr(extractor, "_cursor", None)
    prefix = getattr(extractor, "_cursor_prefix", None)
    if cursor and prefix:
        cursor_boundary = cursor.partition("/")[0]
        prefix_boundary = prefix.partition("/")[0]
        cursor_stage = cursor_boundary.partition("_")[0]
        prefix_stage = prefix_boundary.partition("_")[0]
        if (
            cursor_stage in {"2", "3"}
            and cursor_stage == prefix_stage
            and cursor_boundary != prefix_boundary
        ):
            # Search pagination may advance the max_id boundary without
            # replacing a full cursor that was supplied to resume this run.
            # The updated prefix is then the durable checkpoint; returning
            # the old full cursor would replay the same page indefinitely.
            return prefix
    if cursor and cursor.startswith(("2_", "3_")) and not cursor.partition("/")[2]:
        if prefix and prefix.startswith(cursor.partition("_")[0] + "_"):
            return prefix
    return cursor


def _wait_for_deferred_ratelimit(api: TwitterAPI) -> None:
    """Apply a prior successful response's quota wait before a new request."""
    response = getattr(api, DEFERRED_RESPONSE_ATTRIBUTE, None)
    if response is None:
        return

    # Clear first.  If gallery-dl's handler aborts, a caller that catches the
    # exception must not accidentally apply this same quota event twice.
    delattr(api, DEFERRED_RESPONSE_ATTRIBUTE)
    cursor = _checkpoint_cursor(api)
    if cursor is None:
        api.log.info("Archive checkpoint cursor unavailable")
    else:
        api.log.info("Archive checkpoint cursor=%s", cursor)
    api._handle_ratelimit(response)


def rate_limit_safe_call(
    self: TwitterAPI,
    endpoint: str,
    params: Any,
    method: str = "GET",
    auth: bool = True,
    root: str | None = None,
) -> Any:
    """TwitterAPI._call from 1.32.4 with successful responses preserved."""
    if (
        endpoint.endswith("/TweetDetail")
        and self.extractor.config(BOUNDED_CONVERSATION_CONFIG) == 1
    ):
        try:
            variables = json.loads(params.get("variables") or "{}")
        except (AttributeError, TypeError, ValueError):
            variables = {}
        if variables.get("cursor"):
            # The context resolver deliberately harvests only the response
            # that contains its focal post.  Returning a valid empty terminal
            # page here prevents gallery-dl from spending another request on
            # broad replies/siblings while allowing the first response to be
            # fully processed.
            self.log.info(BOUNDED_CONVERSATION_LOG)
            return {
                "data": {
                    "threaded_conversation_with_injections_v2": {
                        "instructions": []
                    }
                }
            }
    url = (self.root if root is None else root) + endpoint

    while True:
        # A proactive wait belongs here, after the preceding page has been
        # consumed and before another request spends quota.
        _wait_for_deferred_ratelimit(self)

        if auth:
            if self.headers["x-twitter-auth-type"]:
                self._transaction_id(url, method)
            else:
                self._authenticate_guest()

        quota_threshold = random.randrange(1, 6)
        with request_telemetry.rate_limit_threshold(quota_threshold):
            response = self.extractor.request(
                url,
                method=method,
                params=params,
                headers=self.headers,
                fatal=None,
            )

        # Update 'x-csrf-token' header (#1170).
        if csrf_token := response.cookies.get("ct0"):
            self.headers["x-csrf-token"] = csrf_token

        remaining = int(response.headers.get("x-rate-limit-remaining", 6))
        low_quota = (
            response.status_code < 400
            and remaining < 6
            and remaining <= quota_threshold
        )

        try:
            data = response.json()
        except ValueError:
            data = {"errors": ({"message": response.text},)}

        errors = data.get("errors")
        if not errors:
            if low_quota:
                _remember_deferred_ratelimit(self, response)
            return data

        retry = False
        for error in errors:
            msg = error.get("message") or "Unspecified"
            self.log.debug("API error: '%s'", msg)

            if "this account is temporarily locked" in msg:
                msg = "Account temporarily locked"
                if self.extractor.config("locked") != "wait":
                    raise self.exc.AuthorizationError(msg)
                self.log.warning(msg)
                self.extractor.input("Press ENTER to retry.")
                retry = True

            elif "Could not authenticate you" in msg:
                raise self.exc.AbortExtraction(f"'{msg}'")

            elif msg.lower().startswith("timeout"):
                retry = True

        if retry:
            if self.headers["x-twitter-auth-type"]:
                if low_quota:
                    _remember_deferred_ratelimit(self, response)
                self.log.debug("Retrying API request")
                continue
            # Fall through to "Login Required".
            response.status_code = 404

        if response.status_code < 400:
            if low_quota:
                _remember_deferred_ratelimit(self, response)
            return data
        if response.status_code in {403, 404} and not self.headers[
            "x-twitter-auth-type"
        ]:
            raise self.exc.AuthRequired("authenticated cookies", "timeline")
        if response.status_code == 429:
            # A real rejection was not a usable page.  Preserve gallery-dl's
            # immediate wait-and-retry behavior rather than deferring it.
            self._handle_ratelimit(response)
            continue

        try:
            errors = ", ".join(error["message"] for error in errors)
        except Exception:
            pass

        raise self.exc.AbortExtraction(
            f"{response.status_code} {response.reason} ({errors})"
        )


def empty_result_safe_tweet_result(
    self: TwitterAPI, tweet_id: str
) -> dict[str, Any]:
    """Confirm an empty direct result once through the stable detail endpoint."""
    try:
        return UPSTREAM_TWEET_RESULT_BY_REST_ID(self, tweet_id)
    except KeyError as exc:
        if exc.args != ("result",):
            raise

    self.log.info(EMPTY_TWEET_RESULT_LOG, tweet_id)
    for tweet in self.tweet_detail(tweet_id):
        if (
            str(tweet.get("rest_id") or "") == str(tweet_id)
            or str(tweet.get("_retweet_id_str") or "") == str(tweet_id)
        ):
            self.log.info(RECOVERED_TWEET_RESULT_LOG, tweet_id)
            return tweet
    raise self.exc.AbortExtraction("Tweet unavailable ('Deleted')")


def install_patch() -> None:
    """Install the version-checked patch once in this interpreter."""
    require_supported_gallery_dl()
    descriptor_capture.install_postprocessor()
    current = TwitterAPI._call
    current_tweet_result = TwitterAPI.tweet_result_by_rest_id
    if (
        current is rate_limit_safe_call
        and current_tweet_result is empty_result_safe_tweet_result
    ):
        return

    if current is not rate_limit_safe_call:
        try:
            fingerprint = _source_sha256(current)
        except (OSError, TypeError) as exc:
            raise ShimCompatibilityError(
                "cannot verify gallery-dl TwitterAPI._call source"
            ) from exc
        if fingerprint != SUPPORTED_CALL_SHA256:
            raise ShimCompatibilityError(
                "gallery-dl TwitterAPI._call does not match the supported "
                f"{SUPPORTED_VERSION} implementation"
            )
    TwitterAPI._call = rate_limit_safe_call
    TwitterAPI.tweet_result_by_rest_id = empty_result_safe_tweet_result


def run_gallery_args(
    gallery_args: list[str],
    telemetry_path: Path | None,
    operation: str | None,
    *,
    scheduler_options: pacing.SchedulerOptions | None = None,
    runner_starts: int = 1,
) -> int:
    scheduler = (
        pacing.DurableRequestScheduler(scheduler_options, operation)
        if scheduler_options is not None and operation is not None
        else None
    )
    recorder_options: dict[str, Any] = {}
    if scheduler is not None:
        recorder_options["request_gate"] = scheduler
    if runner_starts != 1:
        recorder_options["runner_starts"] = runner_starts
    recorder = (
        request_telemetry.RequestRecorder(
            telemetry_path, operation, **recorder_options
        )
        if telemetry_path is not None and operation is not None
        else None
    )
    capture = None
    if recorder is not None:
        try:
            capture = recorder.capture()
            capture.__enter__()
        except Exception as exc:
            capture = None
            if scheduler is not None:
                scheduler.close()
                raise pacing.PacingError(
                    "actual request gate could not be installed"
                ) from exc
            print(
                "gallery-dl X runner: request telemetry disabled after "
                f"{exc.__class__.__name__}",
                file=sys.stderr,
            )

    original_argv = sys.argv
    status = 1
    capture_shutdown_error: BaseException | None = None
    try:
        sys.argv = [original_argv[0], *gallery_args]
        result = gallery_dl.main()
        status = int(result or 0)
    finally:
        sys.argv = original_argv
        if capture is not None:
            try:
                capture.__exit__(*sys.exc_info())
            except Exception as exc:
                capture_shutdown_error = exc
                if scheduler is None:
                    print(
                        "gallery-dl X runner: request telemetry shutdown failed "
                        f"({exc.__class__.__name__})",
                        file=sys.stderr,
                    )
        if recorder is not None:
            error = recorder.safe_write(status)
            if error:
                print(
                    "gallery-dl X runner: request telemetry write failed "
                    f"({error})",
                    file=sys.stderr,
                )
        if scheduler is not None:
            scheduler.close()
    if capture_shutdown_error is not None and scheduler is not None:
        raise pacing.PacingError("actual request gate could not be removed") from (
            capture_shutdown_error
        )
    return status


def run_once(values: list[str], *, runner_starts: int = 1) -> int:
    try:
        telemetry_path, operation, remaining = (
            request_telemetry.parse_runner_options(values)
        )
        scheduler_options, gallery_args = pacing.parse_runner_options(remaining)
        if scheduler_options is not None and operation is None:
            raise pacing.PacingError(
                "request telemetry operation is required with scheduler options"
            )
        install_patch()
    except (
        importlib.metadata.PackageNotFoundError,
        pacing.PacingError,
        request_telemetry.RequestTelemetryError,
        ShimCompatibilityError,
    ) as exc:
        print(f"gallery-dl X runner: {exc}", file=sys.stderr)
        return 32
    try:
        return run_gallery_args(
            gallery_args,
            telemetry_path,
            operation,
            scheduler_options=scheduler_options,
            runner_starts=runner_starts,
        )
    except pacing.PacingError as exc:
        print(f"gallery-dl X runner: {exc}", file=sys.stderr)
        return 32


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    try:
        worker_options, remaining = runner_control.parse_worker_options(values)
        if worker_options is not None and remaining:
            raise runner_control.ControlProtocolError(
                "control-worker startup does not accept gallery arguments"
            )
        if worker_options is not None:
            install_patch()
    except (
        importlib.metadata.PackageNotFoundError,
        runner_control.ControlProtocolError,
        ShimCompatibilityError,
    ) as exc:
        print(f"gallery-dl X runner: {exc}", file=sys.stderr)
        return 32
    if worker_options is not None:
        return runner_control.worker_loop(worker_options, run_once)
    return run_once(remaining)


if __name__ == "__main__":
    raise SystemExit(main())
