import hashlib
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests
import urllib3.connectionpool
from urllib3.response import HTTPResponse


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_x
import archive_x_context as context_x
import archive_x_descriptors as descriptor_x
import archive_x_media as media_x


def descriptor(
    post_id: str,
    ordinal: int,
    *,
    extension: str = "jpg",
    media_type: str = "photo",
    author_id: str = "2",
    author_handle: str = "other",
) -> dict:
    if media_type in {"video", "animated_gif"}:
        url_host = "video.twimg.com"
        url = (
            f"https://video.twimg.com/ext_tw_video/{post_id}/pu/vid/"
            f"1280x720/{ordinal}.{extension}?tag=fixture"
        )
    else:
        url_host = "pbs.twimg.com"
        url = f"https://pbs.twimg.com/media/{post_id}-{ordinal}.{extension}?name=orig"
    filename = (
        f"2026-01-01T00-00-00_{post_id}_{ordinal}_{author_handle}.{extension}"
    )
    relative = f"users/alice/media/context/2026/01/{filename}"
    row = {
        "schema": descriptor_x.SCHEMA,
        "schema_version": descriptor_x.SCHEMA_VERSION,
        "operation_id": "context-op",
        "run_id": "context-op",
        "source_kind": "context",
        "source_operation": "context",
        "owner_kind": "post",
        "owner_id": post_id,
        "post_id": post_id,
        "media_ordinal": ordinal,
        "media_type": media_type,
        "extension": extension,
        "private_url": url,
        "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
        "url_host": url_host,
        "filename": filename,
        "relative_directory": str(Path(relative).parent),
        "relative_path": relative,
        "width": 1200,
        "height": 800,
        "duration_seconds": 2.5 if media_type != "photo" else None,
        "bitrate": 1000 if media_type != "photo" else None,
        "alt_text": "fixture",
        "variant": {"type": media_type, "width": 1200, "height": 800},
        "posted_at": "2026-01-01T00:00:00Z",
        "original_posted_at": None,
        "author_id": author_id,
        "author_handle": author_handle,
        "conversation_id": post_id,
        "reply_id": None,
        "retweet_id": None,
        "captured_at": "2026-01-02T00:00:00Z",
    }
    row["descriptor_sha256"] = hashlib.sha256(
        descriptor_x.canonical_json(descriptor_x.descriptor_payload(row)).encode()
    ).hexdigest()
    return descriptor_x.normalize_record(row)


def post(post_id: str, count: int) -> dict:
    return {
        "tweet_id": int(post_id),
        "conversation_id": int(post_id),
        "reply_id": 0,
        "retweet_id": 0,
        "count": count,
        "date": "2026-01-01 00:00:00",
        "archived_at": "2026-01-02T00:00:00Z",
        "author": {"id": 2, "name": "other"},
        "user": {"id": 1, "name": "alice"},
    }


def batch(*rows: dict) -> descriptor_x.DescriptorBatch:
    return descriptor_x.DescriptorBatch(
        operation_id="context-op",
        run_id="context-op",
        source_kind="context",
        source_operation="context",
        rows=tuple(rows),
        source_sha256=hashlib.sha256(
            descriptor_x.canonical_json(rows).encode()
        ).hexdigest(),
        ephemeral=True,
    )


class ActualHttpFixture:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, _pool, _connection, method, url, *_args, **kwargs):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(kwargs.get("headers") or {}),
            }
        )
        status, body, headers = self.responses.pop(0)
        return HTTPResponse(
            body=io.BytesIO(body),
            status=status,
            headers=headers,
            preload_content=False,
            reason="fixture",
            request_method=method,
            request_url=url,
        )


class FakeResponse:
    def __init__(self, status, headers, chunks):
        self.status_code = status
        self.headers = headers
        self._chunks = chunks
        self.closed = False

    def iter_content(self, chunk_size):
        del chunk_size
        for chunk in self._chunks:
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError("unexpected CDN request")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    def close(self):
        pass


class DirectMediaTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.user_dir = self.root / "users" / "alice"
        state_dir = self.user_dir / "_state"
        state_dir.mkdir(parents=True)
        archive_x.atomic_write_json(
            state_dir / "state.json",
            {
                "requested_user_id": "1",
                "requested_handle": "alice",
                "canonical_handle": "alice",
            },
        )
        self.db_path = state_dir / "context.sqlite3"
        with context_x.ContextDB(self.db_path) as database:
            database.bind_identity("1", "alice")

    def tearDown(self):
        self.temporary.cleanup()

    def add_post(self, post_id="100", rows=None):
        rows = rows or (descriptor(post_id, 1),)
        metadata = post(post_id, len(rows))
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.upsert_target(
                post_id,
                conversation_id=post_id,
                depth=0,
                observed_at="2026-01-01T00:00:00Z",
            )
            database.capture(
                post_id,
                metadata,
                source_kind="x:focal",
                target_user_id="1",
                max_depth=10,
                descriptor_batches=(batch(*rows),),
            )

    def job(self, post_id="100", ordinal=1):
        with context_x.ContextDB(self.db_path, create=False) as database:
            row = database.connection.execute(
                """SELECT a.*,d.* FROM asset_jobs a
                     JOIN descriptor_generations d
                       ON d.descriptor_id=a.descriptor_id
                    WHERE a.owner_id=? AND a.media_ordinal=?""",
                (post_id, ordinal),
            ).fetchone()
            return dict(row)

    def run_worker(self, **overrides):
        options = {
            "archive_root": self.root,
            "user_dir": self.user_dir,
            "db_path": self.db_path,
            "rate_limit": None,
            "min_free_bytes": 0,
            "retry_delay": 0,
        }
        options.update(overrides)
        return media_x.run_direct_media_worker(**options)

    def test_happy_path_is_one_cdn_call_zero_x_calls_and_atomic_sidecar(self):
        self.add_post()
        payload = b"fixture-image-bytes"
        transport = ActualHttpFixture(
            [
                (
                    200,
                    payload,
                    {
                        "Content-Length": str(len(payload)),
                        "Content-Type": "image/jpeg",
                        "ETag": '"one"',
                    },
                )
            ]
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            transport,
        ):
            result = self.run_worker()

        self.assertEqual(result["captured"], 1)
        summary = result["request_telemetry"]
        self.assertEqual(summary["actual_requests"], 1)
        self.assertEqual(summary["by_category"], {"media_cdn": 1})
        self.assertEqual(summary["peak_concurrency"], 1)
        job = self.job()
        final = self.root / job["expected_relative_path"]
        self.assertEqual(final.read_bytes(), payload)
        sidecar = archive_x.load_json(Path(str(final) + ".json"), {})
        self.assertEqual(sidecar["tweet_id"], 100)
        self.assertEqual(sidecar["num"], 1)
        self.assertEqual(sidecar["sha256"], archive_x.sha256_file(final))
        self.assertEqual(job["state"], "captured")
        with context_x.ContextDB(self.db_path, create=False) as database:
            indexed = database.connection.execute(
                "SELECT * FROM archive_media WHERE asset_id=?",
                (job["asset_id"],),
            ).fetchone()
            self.assertIsNotNone(indexed)
            self.assertEqual(indexed["media_path"], final.relative_to(self.user_dir).as_posix())
            self.assertEqual(indexed["final_sha256"], archive_x.sha256_file(final))
            self.assertEqual(indexed["final_bytes"], len(payload))
            self.assertEqual(
                database.connection.execute(
                    "SELECT status FROM export_views WHERE view_name='media'"
                ).fetchone()[0],
                "dirty",
            )
        exported = archive_x.update_media_dataset(self.user_dir, "alice")
        self.assertEqual(exported, {"media_files": 1, "media_bytes": len(payload)})
        media_record = next(
            archive_x.iter_jsonl(self.user_dir / "dataset" / "media.jsonl")
        )
        self.assertEqual(media_record["post_id"], "100")
        self.assertEqual(media_record["relationship"], "context")
        self.assertEqual(media_record["media_number"], 1)
        self.assertEqual(media_record["sha256"], archive_x.sha256_file(final))
        ledger = sqlite3.connect(
            self.user_dir / "_state" / "context-downloads.sqlite3"
        )
        try:
            self.assertEqual(
                ledger.execute("SELECT entry FROM media").fetchone()[0],
                "100_0_1",
            )
        finally:
            ledger.close()
        if os.name == "posix":
            self.assertEqual(final.stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                Path(str(final) + ".json").stat().st_mode & 0o777,
                0o600,
            )

    def test_multi_asset_rollup_and_rejected_record_never_reach_transfer(self):
        rows = (
            descriptor("100", 1),
            descriptor("100", 2, extension="mp4", media_type="video"),
            descriptor(
                "100", 3, extension="mp4", media_type="animated_gif"
            ),
        )
        self.add_post(rows=rows)
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.persist_descriptor_batches(
                (batch(descriptor("999", 1)),),
                (post("100", 2),),
            )
        transport = ActualHttpFixture(
            [
                (200, b"photo", {"Content-Length": "5", "Content-Type": "image/jpeg"}),
                (200, b"gif", {"Content-Length": "3", "Content-Type": "video/mp4"}),
                (200, b"video", {"Content-Length": "5", "Content-Type": "video/mp4"}),
            ]
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            transport,
        ):
            result = self.run_worker()

        self.assertEqual(result["captured"], 3)
        self.assertEqual(result["request_telemetry"]["actual_requests"], 3)
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertIsNone(
                database.connection.execute(
                    "SELECT 1 FROM asset_jobs WHERE owner_id='999'"
                ).fetchone()
            )
            state = database.connection.execute(
                "SELECT media_state FROM targets WHERE post_id='100'"
            ).fetchone()[0]
        self.assertEqual(state, "captured")
        gif_job = self.job(ordinal=3)
        gif_sidecar = archive_x.load_json(
            Path(str(self.root / gif_job["expected_relative_path"]) + ".json"),
            {},
        )
        self.assertEqual(gif_sidecar["type"], "animated_gif")
        self.assertEqual(gif_sidecar["num"], 3)

    def test_verified_existing_file_uses_zero_network(self):
        self.add_post()
        job = self.job()
        final = self.root / job["expected_relative_path"]
        final.parent.mkdir(parents=True)
        final.write_bytes(b"already-here")
        sidecar = media_x.build_sidecar(
            job,
            account_id="1",
            account_handle="alice",
            final_path=final,
            digest=archive_x.sha256_file(final),
            byte_count=final.stat().st_size,
        )
        sidecar["num"] = "1"
        archive_x.atomic_write_json(Path(str(final) + ".json"), sidecar)
        session = FakeSession([])

        result = self.run_worker(session=session)

        self.assertEqual(result["existing"], 1)
        self.assertEqual(result["request_telemetry"]["actual_requests"], 0)
        self.assertEqual(session.calls, [])
        self.assertEqual(self.job()["state"], "captured")

    def test_allowed_redirect_is_counted_and_external_redirect_is_not_followed(self):
        self.add_post()
        payload = b"redirected"
        allowed = ActualHttpFixture(
            [
                (302, b"", {"Location": "/media/final.jpg"}),
                (
                    200,
                    payload,
                    {
                        "Content-Length": str(len(payload)),
                        "Content-Type": "image/jpeg",
                    },
                ),
            ]
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            allowed,
        ):
            result = self.run_worker()
        self.assertEqual(result["request_telemetry"]["actual_requests"], 2)
        self.assertEqual(result["request_telemetry"]["redirects"], 1)

        self.add_post("101", (descriptor("101", 1),))
        external = ActualHttpFixture(
            [(302, b"", {"Location": "https://example.com/private"})]
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            external,
        ):
            result = self.run_worker()
        self.assertEqual(result["manual_review"], 1)
        self.assertEqual(result["request_telemetry"]["actual_requests"], 1)
        self.assertEqual(len(external.calls), 1)
        telemetry_text = (
            self.user_dir / "_state" / "direct-media.requests.json"
        ).read_text(encoding="utf-8")
        self.assertNotIn("example.com", telemetry_text)
        self.assertNotIn("name=orig", telemetry_text)
        self.assertNotIn("private", str(self.job("101")["last_error_detail"]))

    def test_redirect_loop_is_bounded_and_retryable(self):
        self.add_post()
        redirects = [
            FakeResponse(302, {"Location": f"/media/hop-{index}.jpg"}, [])
            for index in range(media_x.MAX_REDIRECTS + 1)
        ]
        session = FakeSession(redirects)

        result = self.run_worker(session=session, max_assets=1, retry_delay=10)

        self.assertEqual(result["retryable"], 1)
        self.assertEqual(len(session.calls), media_x.MAX_REDIRECTS + 1)
        self.assertTrue(all(response.closed for response in redirects))
        self.assertEqual(self.job()["last_error_class"], "cdn_redirect_loop")

    def test_request_timeout_is_bounded_retryable_and_sanitized(self):
        self.add_post()
        session = FakeSession([requests.Timeout("secret URL should not persist")])

        result = self.run_worker(session=session, max_assets=1, retry_delay=10)

        self.assertEqual(result["retryable"], 1)
        self.assertEqual(len(session.calls), 1)
        job = self.job()
        self.assertEqual(job["last_error_class"], "cdn_network_error")
        self.assertNotIn("secret", str(job["last_error_detail"]))

    def test_invalid_timeout_is_rejected_before_claim_or_network(self):
        self.add_post()
        session = FakeSession([])
        with self.assertRaisesRegex(media_x.context_x.ContextError, "timeout"):
            self.run_worker(session=session, timeout=(0, 30))
        self.assertEqual(session.calls, [])
        self.assertEqual(self.job()["state"], "pending")

    def test_403_404_and_410_require_refresh_without_hidden_fallback(self):
        for index, status in enumerate((403, 404, 410), 1):
            post_id = str(200 + index)
            self.add_post(post_id, (descriptor(post_id, 1),))
            transport = ActualHttpFixture([(status, b"", {})])
            with mock.patch.object(
                urllib3.connectionpool.HTTPConnectionPool,
                "_make_request",
                transport,
            ):
                result = self.run_worker(max_assets=1)
            self.assertEqual(result["needs_refresh"], 1)
            self.assertEqual(result["request_telemetry"]["actual_requests"], 1)
            self.assertEqual(self.job(post_id)["state"], "needs_refresh")

    def test_low_disk_stops_before_any_request_and_does_not_spend_attempt(self):
        self.add_post()
        session = FakeSession([])
        result = self.run_worker(
            session=session,
            min_free_bytes=100,
            disk_free=lambda _path: 99,
        )
        self.assertEqual(result["retryable"], 1)
        self.assertEqual(session.calls, [])
        self.assertEqual(self.job()["attempts"], 0)

    def test_interrupt_retains_verified_partial_then_resumes_with_range(self):
        self.add_post()
        first = FakeSession(
            [
                FakeResponse(
                    200,
                    {
                        "Content-Length": "6",
                        "Content-Type": "image/jpeg",
                        "ETag": '"stable"',
                    },
                    [b"abc", KeyboardInterrupt()],
                )
            ]
        )
        with self.assertRaises(KeyboardInterrupt):
            self.run_worker(session=first)
        self.assertEqual(self.job()["state"], "retryable")
        partial_state = next(
            (self.user_dir / "_state" / "media-partials").glob("*.json")
        ).read_text(encoding="utf-8")
        self.assertNotIn("twimg.com", partial_state)
        self.assertNotIn("name=orig", partial_state)
        telemetry = archive_x.load_json(
            self.user_dir / "_state" / "direct-media.requests.json", {}
        )
        self.assertEqual(telemetry["exit_code"], 130)

        second = FakeSession(
            [
                FakeResponse(
                    206,
                    {
                        "Content-Length": "3",
                        "Content-Type": "image/jpeg",
                        "Content-Range": "bytes 3-5/6",
                        "ETag": '"stable"',
                    },
                    [b"def"],
                )
            ]
        )
        result = self.run_worker(session=second)
        self.assertEqual(result["captured"], 1)
        self.assertEqual(result["resumed"], 1)
        self.assertEqual(second.calls[0]["headers"]["Range"], "bytes=3-")
        final = self.root / self.job()["expected_relative_path"]
        self.assertEqual(final.read_bytes(), b"abcdef")

    def test_range_416_discards_partial_and_restarts_from_zero(self):
        self.add_post()
        first = FakeSession(
            [
                FakeResponse(
                    200,
                    {
                        "Content-Length": "6",
                        "Content-Type": "image/jpeg",
                        "ETag": '"stale"',
                    },
                    [b"abc", KeyboardInterrupt()],
                )
            ]
        )
        with self.assertRaises(KeyboardInterrupt):
            self.run_worker(session=first)

        session = FakeSession(
            [
                FakeResponse(416, {}, []),
                FakeResponse(
                    200,
                    {"Content-Length": "6", "Content-Type": "image/jpeg"},
                    [b"fresh!"],
                ),
            ]
        )
        result = self.run_worker(session=session)

        self.assertEqual(result["captured"], 1)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(session.calls[0]["headers"]["Range"], "bytes=3-")
        self.assertNotIn("Range", session.calls[1]["headers"])
        final = self.root / self.job()["expected_relative_path"]
        self.assertEqual(final.read_bytes(), b"fresh!")

    def test_crash_after_file_publication_recovers_without_redownload(self):
        self.add_post()
        transport = ActualHttpFixture(
            [(200, b"durable", {"Content-Length": "7", "Content-Type": "image/jpeg"})]
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            transport,
        ), mock.patch.object(
            media_x.context_x.ContextDB,
            "asset_succeeded",
            side_effect=RuntimeError("injected commit crash"),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                self.run_worker(clock=lambda: 0, lease_seconds=10)

        session = FakeSession([])
        result = self.run_worker(
            session=session,
            clock=lambda: 100,
            lease_seconds=10,
        )
        self.assertEqual(result["existing"], 1)
        self.assertEqual(session.calls, [])
        self.assertEqual(self.job()["state"], "captured")

    def test_media_index_fault_rolls_back_queue_and_generation_then_recovers(self):
        self.add_post()
        payload = b"atomic-media"
        transport = ActualHttpFixture(
            [(200, payload, {"Content-Length": "12", "Content-Type": "image/jpeg"})]
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            transport,
        ), mock.patch.object(
            context_x,
            "_upsert_portable_asset",
            side_effect=RuntimeError("injected media index fault"),
        ):
            with self.assertRaisesRegex(RuntimeError, "media index fault"):
                self.run_worker(clock=lambda: 0, lease_seconds=10)

        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT state FROM asset_jobs"
                ).fetchone()[0],
                "leased",
            )
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM archive_media"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                database.connection.execute(
                    "SELECT current_generation FROM archive_generation"
                ).fetchone()[0],
                0,
            )

        session = FakeSession([])
        result = self.run_worker(
            session=session,
            clock=lambda: 100,
            lease_seconds=10,
        )
        self.assertEqual(result["existing"], 1)
        self.assertEqual(session.calls, [])
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM archive_media"
                ).fetchone()[0],
                1,
            )

    def test_download_ledger_fault_retries_locally_without_redownload(self):
        self.add_post()
        payload = b"ledger-durable"
        transport = ActualHttpFixture(
            [
                (
                    200,
                    payload,
                    {
                        "Content-Length": str(len(payload)),
                        "Content-Type": "image/jpeg",
                    },
                )
            ]
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            transport,
        ), mock.patch.object(
            media_x,
            "update_download_archive",
            side_effect=OSError("injected ledger fault"),
        ):
            result = self.run_worker(
                max_assets=1, clock=lambda: 0, retry_delay=10
            )

        self.assertEqual(result["retryable"], 1)
        self.assertEqual(result["ledger_errors"], 1)
        self.assertEqual(self.job()["attempts"], 0)
        final = self.root / self.job()["expected_relative_path"]
        self.assertEqual(final.read_bytes(), payload)

        session = FakeSession([])
        result = self.run_worker(session=session, clock=lambda: 100)
        self.assertEqual(result["existing"], 1)
        self.assertEqual(result["ledger_updated"], 1)
        self.assertEqual(session.calls, [])
        self.assertEqual(self.job()["state"], "captured")

    def test_completed_partial_survives_sidecar_fault_without_redownload(self):
        self.add_post()
        transport = ActualHttpFixture(
            [(200, b"complete", {"Content-Length": "8", "Content-Type": "image/jpeg"})]
        )
        original_write = media_x.archive_x.atomic_write_json

        def fail_final_sidecar(path, value):
            if str(path).endswith(".jpg.json"):
                raise OSError("injected sidecar fault")
            return original_write(path, value)

        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            transport,
        ), mock.patch.object(
            media_x.archive_x,
            "atomic_write_json",
            side_effect=fail_final_sidecar,
        ):
            with self.assertRaisesRegex(OSError, "injected"):
                self.run_worker(clock=lambda: 0, lease_seconds=10)

        session = FakeSession([])
        result = self.run_worker(
            session=session,
            clock=lambda: 100,
            lease_seconds=10,
        )
        self.assertEqual(result["recovered_partial"], 1)
        self.assertEqual(result["downloaded"], 0)
        self.assertEqual(session.calls, [])
        self.assertEqual(self.job()["state"], "captured")

    def test_transient_failures_have_a_durable_attempt_ceiling(self):
        self.add_post()
        first = ActualHttpFixture([(500, b"", {})])
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            first,
        ):
            result = self.run_worker(
                max_assets=1,
                max_attempts=2,
                retry_delay=10,
                clock=lambda: 0,
            )
        self.assertEqual(result["retryable"], 1)
        self.assertEqual(self.job()["attempts"], 1)

        second = ActualHttpFixture([(500, b"", {})])
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            second,
        ):
            result = self.run_worker(
                max_assets=1,
                max_attempts=2,
                retry_delay=10,
                clock=lambda: 20,
            )
        self.assertEqual(result["manual_review"], 1)
        self.assertEqual(self.job()["state"], "manual_review")

    def test_incomplete_body_is_retryable_and_retains_partial_evidence(self):
        self.add_post()
        response = FakeResponse(
            200,
            {
                "Content-Length": "10",
                "Content-Type": "image/jpeg",
                "ETag": '"stable"',
            },
            [b"short"],
        )
        result = self.run_worker(
            session=FakeSession([response]),
            max_assets=1,
            retry_delay=10,
            clock=lambda: 0,
        )
        self.assertEqual(result["retryable"], 1)
        self.assertEqual(
            self.job()["last_error_class"], "incomplete_media_response"
        )
        self.assertTrue(
            any(
                path.suffix == ".part"
                for path in (self.user_dir / "_state" / "media-partials").iterdir()
            )
        )

    def test_profile_asset_uses_info_descriptor_and_main_gallery_ledger(self):
        info = {
            "id": 1,
            "name": "alice",
            "profile_image": (
                "https://pbs.twimg.com/profile_images/"
                "1873027274501783552/pic_normal.jpg"
            ),
        }
        profile = descriptor_x.profile_batch_from_info(
            info,
            user_dir=self.user_dir,
            operation_id="run:info-profile",
            run_id="run",
            captured_at="2026-01-01T00:00:00Z",
            source_relative_path="runs/run/raw/info.posts.jsonl",
            source_sha256="a" * 64,
        )
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.persist_descriptor_batches(
                (profile,), (), allow_profile=True
            )
        transport = ActualHttpFixture(
            [(200, b"avatar", {"Content-Length": "6", "Content-Type": "image/jpeg"})]
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            transport,
        ):
            result = self.run_worker()
        self.assertEqual(result["captured"], 1)
        with context_x.ContextDB(self.db_path, create=False) as database:
            row = database.connection.execute(
                """SELECT a.state,a.final_relative_path
                     FROM asset_jobs a WHERE owner_kind='profile_avatar'"""
            ).fetchone()
        self.assertEqual(row["state"], "captured")
        sidecar = archive_x.load_json(
            Path(str(self.root / row["final_relative_path"]) + ".json"), {}
        )
        self.assertEqual(sidecar["subcategory"], "avatar")
        ledger = sqlite3.connect(self.user_dir / "_state" / "downloads.sqlite3")
        try:
            entry = ledger.execute("SELECT entry FROM media").fetchone()[0]
        finally:
            ledger.close()
        self.assertTrue(entry.startswith("AV_1_"))

    def test_small_asset_priority_precedes_video_regardless_of_ordinal(self):
        rows = (
            descriptor("100", 1, extension="mp4", media_type="video"),
            descriptor("100", 2),
        )
        self.add_post(rows=rows)
        with context_x.ContextDB(self.db_path, create=False) as database:
            claimed = database.claim_asset(now=0, lease_seconds=60)
            self.assertEqual(claimed["media_ordinal"], 2)
            database.asset_failed(
                asset_id=claimed["asset_id"],
                lease_token=claimed["lease_token"],
                descriptor_id=claimed["descriptor_id"],
                state="retryable",
                error_class="interrupted",
                detail="interrupted",
                count_attempt=False,
            )

    def test_corrupt_existing_file_is_not_trusted_and_is_repaired(self):
        self.add_post()
        job = self.job()
        final = self.root / job["expected_relative_path"]
        final.parent.mkdir(parents=True)
        final.write_bytes(b"corrupt")
        sidecar = media_x.build_sidecar(
            job,
            account_id="1",
            account_handle="alice",
            final_path=final,
            digest="0" * 64,
            byte_count=final.stat().st_size,
        )
        archive_x.atomic_write_json(Path(str(final) + ".json"), sidecar)
        transport = ActualHttpFixture(
            [(200, b"repaired", {"Content-Length": "8", "Content-Type": "image/jpeg"})]
        )
        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            transport,
        ):
            result = self.run_worker()
        self.assertEqual(result["downloaded"], 1)
        self.assertEqual(final.read_bytes(), b"repaired")
        self.assertEqual(self.job()["state"], "captured")

    def test_post_write_verification_fault_never_marks_job_captured(self):
        self.add_post()
        job = self.job()
        final = self.root / job["expected_relative_path"]
        transport = ActualHttpFixture(
            [(200, b"content", {"Content-Length": "7", "Content-Type": "image/jpeg"})]
        )
        original_hash = media_x.archive_x.sha256_file

        def mismatched_after_placement(path):
            if Path(path) == final:
                return "0" * 64
            return original_hash(path)

        with mock.patch.object(
            urllib3.connectionpool.HTTPConnectionPool,
            "_make_request",
            transport,
        ), mock.patch.object(
            media_x.archive_x,
            "sha256_file",
            side_effect=mismatched_after_placement,
        ):
            result = self.run_worker(max_assets=1, retry_delay=10)
        self.assertEqual(result["retryable"], 1)
        self.assertEqual(
            self.job()["last_error_class"], "post_write_verification_failed"
        )
        self.assertNotEqual(self.job()["state"], "captured")

    def test_destination_escape_is_rejected_before_network(self):
        self.add_post()
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.connection.execute(
                """UPDATE asset_jobs
                      SET expected_relative_path='../outside.jpg'
                    WHERE owner_id='100'"""
            )
        session = FakeSession([])
        result = self.run_worker(session=session, max_assets=1)
        self.assertEqual(result["manual_review"], 1)
        self.assertEqual(session.calls, [])
        self.assertEqual(self.job()["state"], "manual_review")

    def test_network_boundary_runs_after_sqlite_claim_transaction_commits(self):
        self.add_post()

        class LockCheckingSession(FakeSession):
            def get(inner_self, url, **kwargs):
                connection = sqlite3.connect(self.db_path, timeout=0)
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.rollback()
                finally:
                    connection.close()
                return super().get(url, **kwargs)

        payload = b"outside-transaction"
        session = LockCheckingSession(
            [
                FakeResponse(
                    200,
                    {
                        "Content-Length": str(len(payload)),
                        "Content-Type": "image/jpeg",
                    },
                    [payload],
                )
            ]
        )
        result = self.run_worker(session=session)
        self.assertEqual(result["captured"], 1)


if __name__ == "__main__":
    unittest.main()
