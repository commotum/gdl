import importlib.util
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import urllib3.connectionpool


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


class FallbackAbort(RuntimeError):
    pass


class FallbackAPI:
    def __init__(self, detail=()):
        self.events = []
        self.log = FakeLog(self.events)
        self.exc = types.SimpleNamespace(AbortExtraction=FallbackAbort)
        self.detail = list(detail)
        self.detail_calls = []

    def tweet_detail(self, tweet_id):
        self.detail_calls.append(tweet_id)
        return iter(self.detail)


class DeferredRateLimitTests(unittest.TestCase):
    def test_selected_low_quota_threshold_reaches_actual_gate(self):
        class Gate:
            def __init__(self):
                self.completions = []

            def reserve(self, _category, _endpoint):
                return types.SimpleNamespace(
                    waited_seconds=0, wait_source="none"
                )

            def complete(self, _reservation, **outcome):
                self.completions.append(outcome)

        gate = Gate()
        low = FakeResponse(200, {"page": "oldest"}, remaining="2")
        api = FakeAPI([low])
        recorder = runner.request_telemetry.RequestRecorder(
            Path("unused.json"), "timeline", request_gate=gate
        )

        def observed_request(_url, **_kwargs):
            return recorder.observe(
                transport="fixture",
                url="https://x.com/i/api/graphql/hash/UserTweetsAndReplies",
                method="GET",
                send=lambda: api.extractor.responses.pop(0),
                status_getter=lambda response: response.status_code,
                headers_getter=lambda response: response.headers,
            )

        api.extractor.request = observed_request
        with mock.patch.object(runner.random, "randrange", return_value=3):
            runner.rate_limit_safe_call(api, "/timeline", {})
        self.assertEqual(len(gate.completions), 1)
        self.assertEqual(gate.completions[0]["rate_limit_threshold"], 3)
        self.assertEqual(
            gate.completions[0]["headers"]["x-rate-limit-remaining"], "2"
        )

    def test_bounded_tweet_detail_stops_before_cursor_request(self):
        api = FakeAPI([])
        with mock.patch.object(
            api.extractor,
            "config",
            side_effect=lambda key: (
                1 if key == runner.BOUNDED_CONVERSATION_CONFIG else None
            ),
        ):
            data = runner.rate_limit_safe_call(
                api,
                "/graphql/hash/TweetDetail",
                {"variables": '{"focalTweetId":"100","cursor":"next-page"}'},
            )

        self.assertEqual(
            data,
            {
                "data": {
                    "threaded_conversation_with_injections_v2": {
                        "instructions": []
                    }
                }
            },
        )
        self.assertFalse(any(event[0] == "request" for event in api.events))
        self.assertIn(("info", runner.BOUNDED_CONVERSATION_LOG), api.events)

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
        self.assertIn(
            (
                "info",
                "Archive rate-limit reset=1800000000 remaining=0",
            ),
            api.events,
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


class EmptyTweetResultTests(unittest.TestCase):
    def test_direct_result_does_not_call_fallback(self):
        api = FallbackAPI()
        direct = {"rest_id": "100"}
        with mock.patch.object(
            runner,
            "UPSTREAM_TWEET_RESULT_BY_REST_ID",
            return_value=direct,
        ):
            result = runner.empty_result_terminal_tweet_result(api, "100")

        self.assertIs(result, direct)
        self.assertEqual(api.detail_calls, [])

    def test_empty_result_is_terminal_without_detail_confirmation(self):
        focal = {"rest_id": "100"}
        api = FallbackAPI(({"rest_id": "other"}, focal))
        with mock.patch.object(
            runner,
            "UPSTREAM_TWEET_RESULT_BY_REST_ID",
            side_effect=KeyError("result"),
        ):
            with self.assertRaisesRegex(FallbackAbort, "EmptyResult"):
                runner.empty_result_terminal_tweet_result(api, "100")

        self.assertEqual(api.detail_calls, [])
        self.assertIn(
            ("info", "Archive empty TweetResult for 100; "
                     "treating as unavailable without confirmation"),
            api.events,
        )

    def test_empty_result_without_focal_tweet_is_explicitly_unavailable(self):
        api = FallbackAPI(({"rest_id": "other"},))
        with mock.patch.object(
            runner,
            "UPSTREAM_TWEET_RESULT_BY_REST_ID",
            side_effect=KeyError("result"),
        ):
            with self.assertRaisesRegex(FallbackAbort, "EmptyResult"):
                runner.empty_result_terminal_tweet_result(api, "100")

        self.assertEqual(api.detail_calls, [])

    def test_unrelated_key_error_is_not_reclassified(self):
        api = FallbackAPI()
        with mock.patch.object(
            runner,
            "UPSTREAM_TWEET_RESULT_BY_REST_ID",
            side_effect=KeyError("different"),
        ):
            with self.assertRaisesRegex(KeyError, "different"):
                runner.empty_result_terminal_tweet_result(api, "100")

        self.assertEqual(api.detail_calls, [])


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


class CompatibilityTestsContinued(unittest.TestCase):
    def test_installs_only_over_the_known_upstream_method(self):
        original = runner.TwitterAPI._call
        original_tweet_result = runner.TwitterAPI.tweet_result_by_rest_id
        try:
            with mock.patch.object(
                runner.importlib.metadata,
                "version",
                return_value=runner.SUPPORTED_VERSION,
            ):
                runner.install_patch()
                runner.install_patch()
            self.assertIs(runner.TwitterAPI._call, runner.rate_limit_safe_call)
            self.assertIs(
                runner.TwitterAPI.tweet_result_by_rest_id,
                runner.empty_result_terminal_tweet_result,
            )
        finally:
            runner.TwitterAPI._call = original
            runner.TwitterAPI.tweet_result_by_rest_id = original_tweet_result

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

    def test_rejects_changed_individual_tweet_extractor(self):
        with mock.patch.object(
            runner.importlib.metadata,
            "version",
            return_value=runner.SUPPORTED_VERSION,
        ), mock.patch.object(
            runner,
            "SUPPORTED_TWEET_EXTRACTOR_SHA256",
            "unreviewed",
        ):
            with self.assertRaisesRegex(
                runner.ShimCompatibilityError,
                "individual Tweet extractor does not match",
            ):
                runner.require_supported_gallery_dl()

    def test_rejects_changed_tweet_result_implementation(self):
        with mock.patch.object(
            runner.importlib.metadata,
            "version",
            return_value=runner.SUPPORTED_VERSION,
        ), mock.patch.object(
            runner,
            "SUPPORTED_TWEET_RESULT_SHA256",
            "unreviewed",
        ):
            with self.assertRaisesRegex(
                runner.ShimCompatibilityError,
                "TweetResultByRestId does not match",
            ):
                runner.require_supported_gallery_dl()


class RequestTelemetryIntegrationTests(unittest.TestCase):
    def test_runner_wires_scheduler_to_each_hidden_actual_boundary(self):
        class PoolResponse:
            status = 200
            headers = {"Content-Length": "0"}

        class Gate:
            def __init__(self):
                self.reserved = []
                self.completed = []
                self.closed = False

            def reserve(self, category, endpoint):
                self.reserved.append((category, endpoint))
                return types.SimpleNamespace(
                    waited_seconds=0, wait_source="none"
                )

            def complete(self, reservation, **outcome):
                self.completed.append((reservation, outcome))

            def close(self):
                self.closed = True

        gate = Gate()

        def fake_pool_request(
            _pool, _connection, _method, _url, *_args, **_kwargs
        ):
            return PoolResponse()

        def fake_gallery_main():
            pool = types.SimpleNamespace(scheme="https", host="x.com")
            for endpoint in ("TweetResultByRestId", "TweetDetail"):
                urllib3.connectionpool.HTTPConnectionPool._make_request(
                    pool, None, "GET", "/i/api/graphql/hash/" + endpoint
                )
            return 0

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.json"
            with mock.patch.object(
                runner.pacing,
                "DurableRequestScheduler",
                return_value=gate,
            ), mock.patch.object(
                urllib3.connectionpool.HTTPConnectionPool,
                "_make_request",
                fake_pool_request,
            ), mock.patch.object(
                runner.gallery_dl, "main", side_effect=fake_gallery_main
            ):
                status = runner.run_gallery_args(
                    [],
                    path,
                    "context_exact",
                    scheduler_options=object(),
                )
        self.assertEqual(status, 0)
        self.assertEqual(
            gate.reserved,
            [("x_api", "tweet_result"), ("x_api", "tweet_detail")],
        )
        self.assertEqual(len(gate.completed), 2)
        self.assertTrue(gate.closed)

    def test_telemetry_shutdown_failure_does_not_change_gallery_status(self):
        class BrokenCapture:
            def __enter__(self):
                return None

            def __exit__(self, *_args):
                raise OSError("private telemetry failure")

        class Recorder:
            def __init__(self, *_args):
                pass

            def capture(self):
                return BrokenCapture()

            def safe_write(self, _status):
                return None

        with mock.patch.object(
            runner.request_telemetry, "RequestRecorder", Recorder
        ), mock.patch.object(runner.gallery_dl, "main", return_value=4):
            status = runner.run_gallery_args(
                [], Path("unused.json"), "timeline"
            )

        self.assertEqual(status, 4)

    def test_scheduler_capture_install_failure_is_fail_closed(self):
        class BrokenCapture:
            def __enter__(self):
                raise OSError("private capture setup detail")

        class Recorder:
            def __init__(self, *_args, **_kwargs):
                pass

            def capture(self):
                return BrokenCapture()

        gate = mock.Mock()
        with mock.patch.object(
            runner.request_telemetry, "RequestRecorder", Recorder
        ), mock.patch.object(
            runner.pacing, "DurableRequestScheduler", return_value=gate
        ), mock.patch.object(runner.gallery_dl, "main") as gallery_main:
            with self.assertRaisesRegex(
                runner.pacing.PacingError, "could not be installed"
            ):
                runner.run_gallery_args(
                    [],
                    Path("unused.json"),
                    "timeline",
                    scheduler_options=object(),
                )

        gallery_main.assert_not_called()
        gate.close.assert_called_once_with()

    def test_scheduler_capture_shutdown_failure_is_fail_closed(self):
        class BrokenCapture:
            def __enter__(self):
                return None

            def __exit__(self, *_args):
                raise OSError("private capture shutdown detail")

        class Recorder:
            def __init__(self, *_args, **_kwargs):
                pass

            def capture(self):
                return BrokenCapture()

            def safe_write(self, _status):
                return None

        gate = mock.Mock()
        with mock.patch.object(
            runner.request_telemetry, "RequestRecorder", Recorder
        ), mock.patch.object(
            runner.pacing, "DurableRequestScheduler", return_value=gate
        ), mock.patch.object(runner.gallery_dl, "main", return_value=0):
            with self.assertRaisesRegex(
                runner.pacing.PacingError, "could not be removed"
            ):
                runner.run_gallery_args(
                    [],
                    Path("unused.json"),
                    "timeline",
                    scheduler_options=object(),
                )

        gate.close.assert_called_once_with()

    def test_one_logical_exact_fetch_records_hidden_detail_fallback(self):
        class PoolResponse:
            status = 200
            headers = {"Content-Length": "2"}

        def fake_pool_request(
            _pool, _connection, _method, _url, *_args, **_kwargs
        ):
            return PoolResponse()

        def fake_gallery_main():
            pool = types.SimpleNamespace(scheme="https", host="x.com")
            for endpoint in ("TweetResultByRestId", "TweetDetail"):
                urllib3.connectionpool.HTTPConnectionPool._make_request(
                    pool,
                    None,
                    "GET",
                    "/i/api/graphql/hash/"
                    + endpoint
                    + "?variables=SECRET_POST_ID",
                )
            return 0

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.json"
            with mock.patch.object(
                urllib3.connectionpool.HTTPConnectionPool,
                "_make_request",
                fake_pool_request,
            ), mock.patch.object(
                runner.gallery_dl,
                "main",
                side_effect=fake_gallery_main,
            ):
                status = runner.run_gallery_args([], path, "context_exact")

            self.assertEqual(status, 0)
            value = json.loads(path.read_text(encoding="utf-8"))
            summary = value["summary"]
            self.assertEqual(summary["actual_requests"], 2)
            self.assertEqual(
                summary["by_endpoint"],
                {"tweet_detail": 1, "tweet_result": 1},
            )
            self.assertEqual(summary["peak_concurrency"], 1)
            persisted = path.read_text(encoding="utf-8")
            self.assertNotIn("SECRET_POST_ID", persisted)
            self.assertNotIn("SECRET_AUTH", persisted)


if __name__ == "__main__":
    unittest.main()
