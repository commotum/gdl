import importlib.util
import types
import unittest
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location(
    "gallery_dl_x_runner", REPO / "scripts" / "gallery_dl_x_runner.py"
)
runner = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(runner)


class FakeResponse:
    def __init__(
        self,
        status_code,
        data,
        *,
        remaining="6",
        reset="1800000000",
        reason="response",
    ):
        self.status_code = status_code
        self._data = data
        self.headers = {
            "x-rate-limit-remaining": remaining,
            "x-rate-limit-reset": reset,
        }
        self.cookies = {}
        self.reason = reason
        self.text = ""

    def json(self):
        return self._data


class FakeLog:
    def __init__(self, events):
        self.events = events

    def _record(self, level, message, args):
        rendered = message % args if args else message
        self.events.append((level, rendered))

    def info(self, message, *args):
        self._record("info", message, args)

    def debug(self, message, *args):
        self._record("debug", message, args)

    def warning(self, message, *args):
        self._record("warning", message, args)


class FakeExtractor:
    def __init__(self, responses, events):
        self.responses = list(responses)
        self.events = events

    def request(self, url, **kwargs):
        self.events.append(("request", url))
        return self.responses.pop(0)

    def config(self, _key):
        return None

    def input(self, message):
        raise AssertionError(message)


class FakeAPI:
    root = "https://api.x.test"

    def __init__(self, responses):
        self.events = []
        self.extractor = FakeExtractor(responses, self.events)
        self.log = FakeLog(self.events)
        self.headers = {"x-twitter-auth-type": "OAuth2Session"}
        self.exc = types.SimpleNamespace(
            AbortExtraction=RuntimeError,
            AuthRequired=RuntimeError,
            AuthorizationError=RuntimeError,
        )

    def _transaction_id(self, _url, _method):
        self.events.append(("transaction", None))

    def _authenticate_guest(self):
        raise AssertionError("authenticated test unexpectedly used guest auth")

    def _handle_ratelimit(self, response):
        self.events.append(("rate-wait", response.status_code))


class DeferredRateLimitTests(unittest.TestCase):
    def test_successful_low_quota_page_is_returned_without_refetch(self):
        low = FakeResponse(200, {"page": "oldest"}, remaining="0")
        api = FakeAPI([low])

        with mock.patch.object(runner.random, "randrange", return_value=1):
            data = runner.rate_limit_safe_call(api, "/timeline", {})

        self.assertEqual(data, {"page": "oldest"})
        self.assertEqual(
            [event for event in api.events if event[0] == "request"],
            [("request", "https://api.x.test/timeline")],
        )
        self.assertFalse(any(event[0] == "rate-wait" for event in api.events))
        self.assertIs(
            getattr(api, runner.DEFERRED_RESPONSE_ATTRIBUTE),
            low,
        )

    def test_next_call_checkpoints_cursor_then_waits_before_request(self):
        low = FakeResponse(200, {"page": 1}, remaining="0")
        normal = FakeResponse(200, {"page": 2})
        api = FakeAPI([low, normal])

        with mock.patch.object(runner.random, "randrange", return_value=1):
            self.assertEqual(
                runner.rate_limit_safe_call(api, "/timeline", {}),
                {"page": 1},
            )
            api.extractor._cursor = "cursor-for-next-page"
            self.assertEqual(
                runner.rate_limit_safe_call(api, "/timeline", {}),
                {"page": 2},
            )

        checkpoint = (
            "info",
            "Archive checkpoint cursor=cursor-for-next-page",
        )
        self.assertIn(checkpoint, api.events)
        checkpoint_index = api.events.index(checkpoint)
        wait_index = api.events.index(("rate-wait", 200))
        request_indices = [
            index
            for index, event in enumerate(api.events)
            if event[0] == "request"
        ]
        self.assertLess(checkpoint_index, wait_index)
        self.assertLess(wait_index, request_indices[1])
        self.assertFalse(hasattr(api, runner.DEFERRED_RESPONSE_ATTRIBUTE))

    def test_checkpoint_message_is_explicit_when_cursor_is_unavailable(self):
        api = FakeAPI([])
        low = FakeResponse(200, {"page": 1}, remaining="0")
        setattr(api, runner.DEFERRED_RESPONSE_ATTRIBUTE, low)

        runner._wait_for_deferred_ratelimit(api)

        self.assertEqual(
            api.events,
            [
                ("info", "Archive checkpoint cursor unavailable"),
                ("rate-wait", 200),
            ],
        )

    def test_stage_three_checkpoint_prefers_advanced_prefix(self):
        api = FakeAPI([])
        api.extractor._cursor = "3_100/"
        api.extractor._cursor_prefix = "3_50/"
        low = FakeResponse(200, {"page": 1}, remaining="0")
        setattr(api, runner.DEFERRED_RESPONSE_ATTRIBUTE, low)

        runner._wait_for_deferred_ratelimit(api)

        self.assertIn(
            ("info", "Archive checkpoint cursor=3_50/"), api.events
        )

    def test_resumed_full_cursor_does_not_hide_advanced_prefix(self):
        api = FakeAPI([])
        api.extractor._cursor = "3_100/old-page-token"
        api.extractor._cursor_prefix = "3_50/"
        low = FakeResponse(200, {"page": 1}, remaining="0")
        setattr(api, runner.DEFERRED_RESPONSE_ATTRIBUTE, low)

        runner._wait_for_deferred_ratelimit(api)

        self.assertIn(
            ("info", "Archive checkpoint cursor=3_50/"), api.events
        )

    def test_real_429_waits_and_retries_immediately(self):
        rejected = FakeResponse(
            429,
            {"errors": [{"message": "Rate limit exceeded"}]},
            remaining="0",
            reason="Too Many Requests",
        )
        success = FakeResponse(200, {"page": "after-reset"})
        api = FakeAPI([rejected, success])

        with mock.patch.object(runner.random, "randrange", return_value=1):
            data = runner.rate_limit_safe_call(api, "/timeline", {})

        self.assertEqual(data, {"page": "after-reset"})
        wait_index = api.events.index(("rate-wait", 429))
        request_indices = [
            index
            for index, event in enumerate(api.events)
            if event[0] == "request"
        ]
        self.assertEqual(len(request_indices), 2)
        self.assertLess(request_indices[0], wait_index)
        self.assertLess(wait_index, request_indices[1])
        self.assertFalse(hasattr(api, runner.DEFERRED_RESPONSE_ATTRIBUTE))
        self.assertFalse(
            any(
                event[0] == "info"
                and event[1].startswith("Archive checkpoint cursor")
                for event in api.events
            )
        )


class CompatibilityTests(unittest.TestCase):
    def test_rejects_other_gallery_dl_versions(self):
        with mock.patch.object(
            runner.importlib.metadata,
            "version",
            return_value="1.32.5",
        ):
            with self.assertRaisesRegex(
                runner.ShimCompatibilityError,
                "supports gallery-dl 1.32.4 exactly; found 1.32.5",
            ):
                runner.require_supported_gallery_dl()

    def test_installs_only_over_the_known_upstream_method(self):
        original = runner.TwitterAPI._call
        try:
            with mock.patch.object(
                runner.importlib.metadata,
                "version",
                return_value=runner.SUPPORTED_VERSION,
            ):
                runner.install_patch()
                runner.install_patch()
            self.assertIs(runner.TwitterAPI._call, runner.rate_limit_safe_call)
        finally:
            runner.TwitterAPI._call = original

    def test_rejects_an_unknown_same_version_implementation(self):
        original = runner.TwitterAPI._call

        def unknown_call(self):
            return self

        try:
            runner.TwitterAPI._call = unknown_call
            with mock.patch.object(
                runner.importlib.metadata,
                "version",
                return_value=runner.SUPPORTED_VERSION,
            ):
                with self.assertRaisesRegex(
                    runner.ShimCompatibilityError,
                    "does not match the supported",
                ):
                    runner.install_patch()
        finally:
            runner.TwitterAPI._call = original


if __name__ == "__main__":
    unittest.main()
