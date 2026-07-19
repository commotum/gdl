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
import random
import sys
import textwrap
from typing import Any

import gallery_dl
from gallery_dl.extractor.twitter import TwitterAPI


SUPPORTED_VERSION = "1.32.4"
SUPPORTED_CALL_SHA256 = (
    "c7c1062eaf240cae86904fad97847c01aeb76b0161ab82671e91686c78a1e7df"
)
DEFERRED_RESPONSE_ATTRIBUTE = "_gdl_x_deferred_ratelimit_response"


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
    return version


def _remember_deferred_ratelimit(api: TwitterAPI, response: Any) -> None:
    setattr(api, DEFERRED_RESPONSE_ATTRIBUTE, response)


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
            and remaining <= random.randrange(1, 6)
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


def install_patch() -> None:
    """Install the version-checked patch once in this interpreter."""
    require_supported_gallery_dl()
    current = TwitterAPI._call
    if current is rate_limit_safe_call:
        return

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


def main() -> int:
    try:
        install_patch()
    except (importlib.metadata.PackageNotFoundError, ShimCompatibilityError) as exc:
        print(f"gallery-dl X runner: {exc}", file=sys.stderr)
        return 32
    return gallery_dl.main()


if __name__ == "__main__":
    raise SystemExit(main())
