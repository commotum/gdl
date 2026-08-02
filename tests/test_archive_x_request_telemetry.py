import io
import json
import sys
import tempfile
import types
import urllib.response
import urllib.request
import unittest
from email.message import Message
from pathlib import Path
from unittest import mock

import requests
import urllib3.connectionpool
from yt_dlp.networking._urllib import UrllibRH
from yt_dlp.networking.common import Request as YtdlpRequest


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_x_request_telemetry as telemetry


class FakeResponse:
    def __init__(self, status=200, length="12"):
        self.status = status
        self.status_code = status
        self.code = status
        self.headers = {"Content-Length": length}


class OptionTests(unittest.TestCase):
    def test_private_options_are_removed_and_validated(self):
        path, operation, remaining = telemetry.parse_runner_options(
            [
                "--archive-x-request-telemetry",
                "/private/requests.json",
                "--archive-x-operation",
                "context_metadata",
                "--no-download",
                "https://x.com/i/web/status/1",
            ]
        )

        self.assertEqual(path, Path("/private/requests.json"))
        self.assertEqual(operation, "context_metadata")
        self.assertEqual(
            remaining,
            ["--no-download", "https://x.com/i/web/status/1"],
        )

    def test_partial_duplicate_and_unknown_options_fail_closed(self):
        cases = (
            ["--archive-x-operation", "timeline"],
            ["--archive-x-request-telemetry", "requests.json"],
            [
                "--archive-x-request-telemetry",
                "one.json",
                "--archive-x-request-telemetry",
                "two.json",
                "--archive-x-operation",
                "timeline",
            ],
            [
                "--archive-x-request-telemetry",
                "requests.json",
                "--archive-x-operation",
                "post-123-secret",
            ],
        )
        for values in cases:
            with self.subTest(values=values), self.assertRaises(
                telemetry.RequestTelemetryError
            ):
                telemetry.parse_runner_options(list(values))


class ClassificationTests(unittest.TestCase):
    def test_categories_and_endpoint_labels_are_allowlisted(self):
        cases = {
            "https://x.com/i/api/graphql/hash/TweetDetail?secret=one": (
                "x_api",
                "tweet_detail",
            ),
            "https://api.twitter.com/1.1/test.json?secret=two": (
                "x_api",
                "x_api_other",
            ),
            "https://x.com/i/js_inst?secret=three": (
                "x_support",
                "client_bootstrap",
            ),
            "https://pbs.twimg.com/media/name.jpg?token=secret": (
                "media_cdn",
                "media_asset",
            ),
            "https://t.co/private-code": ("x_redirect", "x_redirect"),
            "https://example.test/private/path?secret=four": (
                "external",
                "external_http",
            ),
        }
        for url, expected in cases.items():
            with self.subTest(url=url):
                self.assertEqual(telemetry.classify_url(url), expected)

        self.assertEqual(telemetry.classify_url("file:///private/file"), (None, None))


class PhaseLedgerTests(unittest.TestCase):
    def test_every_current_network_phase_has_an_actual_call_budget(self):
        matrix = {
            "info": (
                "https://x.com/i/js_inst",
                "https://x.com/i/api/graphql/hash/UserByScreenName",
            ),
            "timeline": (
                "https://x.com/i/js_inst",
                "https://x.com/i/api/graphql/hash/UserTweetsAndReplies",
                "https://pbs.twimg.com/media/a.jpg",
            ),
            "retry_media": (
                "https://x.com/i/api/graphql/hash/TweetResultByRestId",
                "https://video.twimg.com/video/a.mp4",
            ),
            "profile_avatar": (
                "https://x.com/i/api/graphql/hash/UserByScreenName",
                "https://pbs.twimg.com/profile_images/a.jpg",
            ),
            "profile_background": (
                "https://x.com/i/api/graphql/hash/UserByScreenName",
                "https://pbs.twimg.com/profile_banners/a.jpg",
            ),
            "context_metadata": (
                "https://x.com/i/api/graphql/hash/TweetDetail",
            ),
            "context_exact": (
                "https://x.com/i/api/graphql/hash/TweetResultByRestId",
                "https://x.com/i/api/graphql/hash/TweetDetail",
            ),
            "context_media": (
                "https://x.com/i/api/graphql/hash/TweetResultByRestId",
                "https://pbs.twimg.com/media/context.jpg",
            ),
            "direct_media": (
                "https://pbs.twimg.com/media/context.jpg",
            ),
            "descriptor_refresh": (
                "https://x.com/i/api/graphql/hash/TweetResultByRestId",
                "https://x.com/i/api/graphql/hash/TweetDetail",
            ),
            "legacy_walk": (
                "https://x.com/i/api/graphql/hash/UserByScreenName",
                "https://x.com/i/api/graphql/hash/SearchTimeline",
                "https://x.com/i/api/graphql/hash/SearchTimeline",
            ),
        }
        for operation, urls in matrix.items():
            with self.subTest(operation=operation):
                recorder = telemetry.RequestRecorder(
                    Path("unused.json"), operation
                )
                for url in urls:
                    recorder.observe(
                        transport="fixture",
                        url=url + "?secret=NEVER",
                        method="GET",
                        send=lambda: FakeResponse(),
                        status_getter=lambda response: response.status,
                        headers_getter=lambda response: response.headers,
                    )
                value = recorder.value(0)
                self.assertEqual(value["summary"]["operation"], operation)
                self.assertEqual(value["summary"]["actual_requests"], len(urls))
                self.assertEqual(
                    sum(value["summary"]["by_endpoint"].values()), len(urls)
                )
                self.assertNotIn("NEVER", json.dumps(value))

    def test_redirect_and_failed_retry_attempts_remain_visible(self):
        recorder = telemetry.RequestRecorder(
            Path("unused.json"), "retry_media"
        )
        outcomes = (
            ("https://t.co/link", 302),
            ("https://video.twimg.com/video/a.mp4", 500),
            ("https://video.twimg.com/video/a.mp4", 200),
        )
        for url, status in outcomes:
            recorder.observe(
                transport="fixture",
                url=url,
                method="GET",
                send=lambda status=status: FakeResponse(status),
                status_getter=lambda response: response.status,
                headers_getter=lambda response: response.headers,
            )
        summary = recorder.value(0)["summary"]
        self.assertEqual(summary["actual_requests"], 3)
        self.assertEqual(summary["redirects"], 1)
        self.assertEqual(summary["by_status"], {"200": 1, "302": 1, "500": 1})


class RecorderTests(unittest.TestCase):
    def test_authoritative_gate_wraps_each_actual_x_attempt_but_not_cdn(self):
        class Gate:
            def __init__(self):
                self.reservations = []
                self.completions = []

            def reserve(self, category, endpoint):
                self.reservations.append((category, endpoint))
                if category == "media_cdn":
                    return None
                return types.SimpleNamespace(
                    waited_seconds=0.125, wait_source="spacing"
                )

            def complete(self, reservation, **outcome):
                self.completions.append((reservation, outcome))

        gate = Gate()
        recorder = telemetry.RequestRecorder(
            Path("unused.json"), "context_exact", request_gate=gate
        )
        urls = (
            "https://x.com/i/js_inst",
            "https://x.com/i/api/graphql/hash/TweetResultByRestId",
            "https://x.com/i/api/graphql/hash/TweetDetail",
            "https://pbs.twimg.com/media/context.jpg",
        )
        for url in urls:
            recorder.observe(
                transport="fixture",
                url=url,
                method="GET",
                send=lambda: FakeResponse(),
                status_getter=lambda response: response.status,
                headers_getter=lambda response: response.headers,
            )
        value = recorder.value(0)
        self.assertEqual(len(gate.reservations), 4)
        self.assertEqual(len(gate.completions), 4)
        self.assertIsNone(gate.completions[-1][0])
        self.assertEqual(value["summary"]["actual_requests"], 4)
        self.assertEqual(value["summary"]["pacing_wait_ms"], 375)
        self.assertEqual(
            value["summary"]["by_wait_source"],
            {"none": 1, "spacing": 3},
        )
        self.assertEqual(
            [event["pacing_wait_source"] for event in value["events"]],
            ["spacing", "spacing", "spacing", "none"],
        )

    def test_gate_completion_failure_is_fail_closed_after_counted_attempt(self):
        class Gate:
            def reserve(self, _category, _endpoint):
                return types.SimpleNamespace(
                    waited_seconds=0, wait_source="none"
                )

            def complete(self, _reservation, **_outcome):
                raise RuntimeError("private scheduler detail")

        recorder = telemetry.RequestRecorder(
            Path("unused.json"), "timeline", request_gate=Gate()
        )
        with self.assertRaisesRegex(RuntimeError, "private scheduler"):
            recorder.observe(
                transport="fixture",
                url="https://x.com/i/api/graphql/hash/UserTweetsAndReplies",
                method="GET",
                send=lambda: FakeResponse(),
                status_getter=lambda response: response.status,
                headers_getter=lambda response: response.headers,
            )
        self.assertEqual(recorder.value(1)["summary"]["actual_requests"], 1)

    def test_actual_events_are_aggregated_without_sensitive_values(self):
        secret = "TOP-SECRET-SIGNED-VALUE"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.json"
            recorder = telemetry.RequestRecorder(path, "timeline")

            responses = (
                (
                    f"https://x.com/i/api/graphql/hash/TweetDetail?token={secret}",
                    "POST",
                    FakeResponse(200, "20"),
                ),
                (
                    f"https://pbs.twimg.com/media/file.jpg?token={secret}",
                    "GET",
                    FakeResponse(200, "30"),
                ),
                (
                    f"https://t.co/{secret}",
                    "GET",
                    FakeResponse(302, "0"),
                ),
            )
            for url, method, response in responses:
                self.assertIs(
                    recorder.observe(
                        transport="fixture",
                        url=url,
                        method=method,
                        send=lambda response=response: response,
                        status_getter=lambda value: value.status,
                        headers_getter=lambda value: value.headers,
                    ),
                    response,
                )

            recorder.record_session("requests")
            recorder.record_connection("video.twimg.com", "urllib3_https")
            recorder.write(0)

            raw = path.read_text(encoding="utf-8")
            self.assertNotIn(secret, raw)
            value = json.loads(raw)
            summary = value["summary"]
            self.assertEqual(summary["actual_requests"], 3)
            self.assertEqual(
                summary["by_category"],
                {"media_cdn": 1, "x_api": 1, "x_redirect": 1},
            )
            self.assertEqual(summary["redirects"], 1)
            self.assertEqual(summary["advertised_bytes"], 50)
            self.assertEqual(summary["sessions"], {"requests": 1})
            self.assertEqual(
                summary["connections"], {"urllib3_https:media_cdn": 1}
            )
            self.assertFalse(value["sensitive_values_persisted"])
            self.assertEqual(
                telemetry.read_summary(path, expected_operation="timeline"),
                summary,
            )

    def test_errors_are_counted_and_original_exception_is_preserved(self):
        recorder = telemetry.RequestRecorder(
            Path("unused.json"), "context_exact"
        )

        with self.assertRaisesRegex(requests.ConnectionError, "private detail"):
            recorder.observe(
                transport="requests",
                url="https://x.com/i/api/graphql/hash/TweetResultByRestId",
                method="GET",
                send=lambda: (_ for _ in ()).throw(
                    requests.ConnectionError("private detail")
                ),
                status_getter=lambda response: response.status_code,
                headers_getter=lambda response: response.headers,
            )

        value = recorder.value(1)
        self.assertEqual(value["summary"]["actual_requests"], 1)
        self.assertEqual(value["summary"]["failures"], 1)
        self.assertEqual(value["events"][0]["error"], "ConnectionError")
        self.assertNotIn("private detail", json.dumps(value))

    def test_event_truncation_keeps_exact_aggregate(self):
        recorder = telemetry.RequestRecorder(
            Path("unused.json"), "timeline", max_events=2
        )
        for _ in range(5):
            recorder.observe(
                transport="fixture",
                url="https://video.twimg.com/video/file.mp4",
                method="GET",
                send=lambda: FakeResponse(),
                status_getter=lambda response: response.status,
                headers_getter=lambda response: response.headers,
            )

        value = recorder.value(0)
        self.assertEqual(value["summary"]["actual_requests"], 5)
        self.assertEqual(value["summary"]["events_retained"], 2)
        self.assertEqual(value["summary"]["events_truncated"], 3)
        self.assertEqual(len(value["events"]), 2)

    def test_v1_artifact_is_normalized_and_reused_worker_start_zero_is_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.json"
            recorder = telemetry.RequestRecorder(path, "timeline")
            recorder.observe(
                transport="fixture",
                url="https://x.com/i/api/graphql/hash/UserTweetsAndReplies",
                method="GET",
                send=lambda: FakeResponse(),
                status_getter=lambda response: response.status,
                headers_getter=lambda response: response.headers,
            )
            value = recorder.value(0)
            value["schema_version"] = 1
            del value["summary"]["pacing_wait_ms"]
            del value["summary"]["by_wait_source"]
            path.write_text(json.dumps(value), encoding="utf-8")
            normalized = telemetry.read_summary(
                path, expected_operation="timeline"
            )
            self.assertEqual(normalized["pacing_wait_ms"], 0)
            self.assertEqual(normalized["by_wait_source"], {"none": 1})

            reused = telemetry.RequestRecorder(
                path, "timeline", runner_starts=0
            )
            reused.write(0)
            summary = telemetry.read_summary(
                path, expected_operation="timeline"
            )
            self.assertEqual(summary["runner_starts"], 0)

    def test_urllib3_and_urllib_transport_boundaries_are_both_observed(self):
        secret = "DO-NOT-PERSIST"

        def fake_pool_request(
            _pool, _connection, _method, _url, *_args, **_kwargs
        ):
            return FakeResponse(200, "11")

        def fake_urllib_do_open(
            _handler, _http_class, _request, **_http_conn_args
        ):
            return FakeResponse(206, "22")

        recorder = telemetry.RequestRecorder(
            Path("unused.json"), "retry_media"
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            fake_pool_request,
        ), mock.patch.object(
            urllib.request.AbstractHTTPHandler,
            "do_open",
            fake_urllib_do_open,
        ):
            with recorder.capture():
                requests.Session()
                pool = types.SimpleNamespace(
                    scheme="https", host="video.twimg.com"
                )
                urllib3.connectionpool.HTTPConnectionPool._make_request(
                    pool,
                    None,
                    "GET",
                    f"/file.mp4?token={secret}",
                )

                request = urllib.request.Request(
                    f"https://pbs.twimg.com/media/file.jpg?token={secret}",
                    headers={"Cookie": secret},
                )
                urllib.request.AbstractHTTPHandler.do_open(
                    object(), None, request
                )

        value = recorder.value(0)
        self.assertEqual(value["summary"]["actual_requests"], 2)
        self.assertEqual(
            value["summary"]["by_transport"],
            {"urllib": 1, "urllib3": 1},
        )
        self.assertEqual(value["summary"]["by_category"], {"media_cdn": 2})
        self.assertNotIn(secret, json.dumps(value))

    def test_yt_dlp_urllib_handler_reaches_actual_request_boundary(self):
        class QuietLogger:
            def stdout(self, *_args, **_kwargs):
                pass

            def error(self, *_args, **_kwargs):
                pass

            def warning(self, *_args, **_kwargs):
                pass

            def debug(self, *_args, **_kwargs):
                pass

        def fake_do_open(_handler, _http_class, request, **_kwargs):
            headers = Message()
            headers["Content-Length"] = "7"
            response = urllib.response.addinfourl(
                io.BytesIO(b"content"),
                headers,
                request.full_url,
                code=200,
            )
            response.msg = "OK"
            return response

        recorder = telemetry.RequestRecorder(
            Path("unused.json"), "retry_media"
        )
        with mock.patch.object(
            urllib.request.AbstractHTTPHandler,
            "do_open",
            fake_do_open,
        ):
            with recorder.capture():
                handler = UrllibRH(logger=QuietLogger())
                response = handler.send(
                    YtdlpRequest(
                        "https://video.twimg.com/video/file.mp4?secret=NEVER"
                    )
                )
                response.close()

        value = recorder.value(0)
        self.assertEqual(value["summary"]["actual_requests"], 1)
        self.assertEqual(value["summary"]["by_transport"], {"urllib": 1})
        self.assertEqual(value["summary"]["by_category"], {"media_cdn": 1})
        self.assertEqual(value["summary"]["advertised_bytes"], 7)
        self.assertNotIn("NEVER", json.dumps(value))

    def test_safe_write_failure_is_outcome_neutral(self):
        recorder = telemetry.RequestRecorder(Path("unused.json"), "info")
        with mock.patch.object(
            recorder, "write", side_effect=OSError("private filesystem detail")
        ):
            self.assertEqual(recorder.safe_write(0), "OSError")


if __name__ == "__main__":
    unittest.main()
