import copy
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location(
    "archive_x_recovery", REPO / "scripts" / "archive_x.py"
)
archive_x = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(archive_x)


RUN_ID = "20260714T085152Z-4e5856"
STARTED_AT = "2026-07-14T08:51:52Z"
COMPLETED_AT = "2026-07-15T00:21:00Z"
POST_ID = "2066979169897234540"
MEDIA_NUMBER = 1
FILENAME = (
    "2026-06-16T20-20-11_2066979169897234540_1_dwarkesh_sp.mp4"
)
DOWNLOAD_ERROR = f"[download][error] Failed to download {FILENAME}\n"


def failed_download(filename=FILENAME, post_id=POST_ID, media_number=1):
    return {
        "filename": filename,
        "post_id": post_id,
        "media_number": media_number,
    }


def write_legacy_run(
    user_dir: Path,
    *,
    run_id=RUN_ID,
    started_at=STARTED_AT,
    completed_at=COMPLETED_AT,
    exit_code=4,
    cursor=None,
    interrupted=False,
    limited=False,
    log=DOWNLOAD_ERROR,
    raw=True,
):
    run_dir = user_dir / "runs" / run_id
    raw_path = run_dir / "raw" / "timeline.posts.incomplete.jsonl"
    raw_relative = raw_path.relative_to(user_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    if raw:
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text('{"tweet_id": 1}\n', encoding="utf-8")
    (run_dir / "timeline.log").write_text(log, encoding="utf-8")
    manifest = {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "limited_run": limited,
        "status": "failed",
        "endpoints": [
            {
                "endpoint": "timeline",
                "exit_code": exit_code,
                "resume_cursor": cursor,
                "interrupted": interrupted,
                "raw_path": str(raw_relative),
            }
        ],
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    return run_dir


class DownloadLogTests(unittest.TestCase):
    def test_parses_timeline_media_failure(self):
        failure = archive_x.download_failure_from_line(
            f"prefix {DOWNLOAD_ERROR.rstrip()}"
        )
        self.assertEqual(failure, failed_download())

    def test_parser_strips_directories_but_does_not_invent_media_identity(self):
        failure = archive_x.download_failure_from_line(
            "[download][error] Failed to download "
            "/tmp/profile-avatar_tszzl.jpg"
        )
        self.assertEqual(
            failure,
            {
                "filename": "profile-avatar_tszzl.jpg",
                "post_id": None,
                "media_number": None,
            },
        )
        self.assertIsNone(
            archive_x.download_failure_from_line(
                "[twitter][warning] Failed to download something"
            )
        )

    def test_analyzer_separates_download_failures_from_other_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "timeline.log"
            log_path.write_text(
                "[twitter][warning] article unavailable\n"
                + DOWNLOAD_ERROR
                + "[twitter][error] API extraction failed\n"
                + "[download][error] Failed to download odd-name.bin\n",
                encoding="utf-8",
            )
            failures, other_errors = archive_x.analyze_gallery_log(log_path)

        self.assertEqual(failures[0], failed_download())
        self.assertEqual(failures[1]["filename"], "odd-name.bin")
        self.assertEqual(other_errors, 1)

    def test_analyzer_missing_log_is_not_evidence_of_success(self):
        failures, other_errors = archive_x.analyze_gallery_log(
            Path("/definitely/missing/timeline.log")
        )
        self.assertEqual(failures, [])
        self.assertEqual(other_errors, 0)

    def test_analyzer_attaches_http_evidence_to_the_failed_asset(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "retry.log"
            log_path.write_text(
                "command: python -c \"print('[download][error] Failed')\"\n"
                "[downloader.http][warning] '404 Not Found' for "
                "'https://pbs.twimg.com/media/test?name=orig'\n"
                "[download][info] Trying fallback URL #1\n"
                "[downloader.http][warning] '404 Not Found' for "
                "'https://pbs.twimg.com/media/test?name=large'\n"
                + DOWNLOAD_ERROR,
                encoding="utf-8",
            )

            failures, other_errors = archive_x.analyze_gallery_log(log_path)

        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["http_statuses"], [404])
        self.assertEqual(failures[0]["http_error_count"], 2)
        self.assertEqual(other_errors, 0)

    def test_metadata_classifier_accepts_only_exact_download_only_failure(self):
        valid = [failed_download()]
        self.assertTrue(
            archive_x.gallery_metadata_complete(4, None, False, valid, 0)
        )
        refused = (
            (4, "saved-cursor", False, valid, 0),
            (4, None, True, valid, 0),
            (4, None, False, [], 0),
            (4, None, False, valid, 1),
            (5, None, False, valid, 0),
            (8, None, False, valid, 0),
        )
        for args in refused:
            with self.subTest(args=args):
                self.assertFalse(archive_x.gallery_metadata_complete(*args))

    def test_metadata_classifier_rejects_unaddressable_download_failure(self):
        unknown = [
            {
                "filename": "profile-avatar_tszzl.jpg",
                "post_id": None,
                "media_number": None,
            }
        ]
        self.assertFalse(
            archive_x.gallery_metadata_complete(4, None, False, unknown, 0)
        )


class ArchiveEndpointRecoveryTests(unittest.TestCase):
    def args(self, cookie_file: Path):
        return Namespace(
            cookies=cookie_file,
            request_delay="4-8",
            download_delay="1-3",
            extractor_delay="2-5",
            no_reposts=True,
            no_checksums=False,
            post_limit=None,
            retries=1,
            http_timeout=60,
            rate_limit="8M",
        )

    def call_endpoint(self, root: Path, *, write_raw: bool):
        user_dir = root / "users" / "tszzl"
        run_dir = user_dir / "runs" / "retry-run"
        run_dir.mkdir(parents=True)
        cookie_file = root / "cookies.txt"
        cookie_file.write_text("", encoding="utf-8")

        def fake_run(command, _log_path, _prefix, **_kwargs):
            if write_raw:
                config_path = Path(command[command.index("--config-json") + 1])
                config = json.loads(config_path.read_text(encoding="utf-8"))
                processors = config["extractor"]["twitter"]["postprocessors"]
                raw = next(pp for pp in processors if pp.get("event") == "post")
                raw_path = Path(raw["base-directory"]) / raw["filename"]
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text('{"tweet_id": 1}\n', encoding="utf-8")
            return 4, None, 1.0, False, [failed_download()], 0, False, 0

        with mock.patch.object(archive_x, "run_gallery_dl", side_effect=fake_run):
            result = archive_x.archive_endpoint(
                args=self.args(cookie_file),
                repo_dir=REPO,
                archive_root=root,
                user_dir=user_dir,
                handle="tszzl",
                endpoint=f"retry-media-{POST_ID}",
                run_dir=run_dir,
                archive_run_id="retry-run",
                archived_at="2026-07-15T00:00:00Z",
                date_after=None,
                cursor=None,
                target_url=f"https://x.com/tszzl/status/{POST_ID}",
                retries=8,
                http_timeout=300,
                include_reposts=True,
            )
        config = json.loads(
            (run_dir / f"retry-media-{POST_ID}.gallery-dl.json").read_text(
                encoding="utf-8"
            )
        )
        return result, config

    def test_download_only_classification_requires_a_raw_post_record(self):
        with tempfile.TemporaryDirectory() as directory:
            result, _ = self.call_endpoint(Path(directory), write_raw=False)
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["metadata_complete"])
        self.assertFalse(result["raw_has_record"])

    def test_retry_can_force_reposts_and_uses_recovery_network_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            result, config = self.call_endpoint(Path(directory), write_raw=True)
        self.assertEqual(result["status"], "media_partial")
        self.assertTrue(result["metadata_complete"])
        self.assertTrue(config["extractor"]["twitter"]["retweets"])
        self.assertEqual(config["extractor"]["twitter"]["videos"], "ytdl")
        command = result["command"]
        self.assertTrue(command[1].endswith("scripts/gallery_dl_x_runner.py"))
        self.assertEqual(command[command.index("--retries") + 1], "8")
        self.assertEqual(command[command.index("--http-timeout") + 1], "300")

    def test_stalled_endpoint_advances_synthetic_cursor_and_never_completes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = root / "users" / "tszzl"
            run_dir = user_dir / "runs" / "stalled-run"
            run_dir.mkdir(parents=True)
            cookie_file = root / "cookies.txt"
            cookie_file.write_text("", encoding="utf-8")

            def fake_run(command, _log_path, _prefix, **_kwargs):
                config_path = Path(command[command.index("--config-json") + 1])
                config = json.loads(config_path.read_text(encoding="utf-8"))
                processors = config["extractor"]["twitter"]["postprocessors"]
                raw = next(pp for pp in processors if pp.get("event") == "post")
                raw_path = Path(raw["base-directory"]) / raw["filename"]
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(
                    '{"tweet_id": 75}\n{"tweet_id": 50}\n',
                    encoding="utf-8",
                )
                return 0, None, 1.0, False, [], 0, True, 3

            with mock.patch.object(
                archive_x, "run_gallery_dl", side_effect=fake_run
            ):
                result = archive_x.archive_endpoint(
                    args=self.args(cookie_file),
                    repo_dir=REPO,
                    archive_root=root,
                    user_dir=user_dir,
                    handle="tszzl",
                    endpoint="timeline",
                    run_dir=run_dir,
                    archive_run_id="stalled-run",
                    archived_at="2026-07-15T00:00:00Z",
                    date_after=None,
                    cursor="3_100/",
                )

        self.assertEqual(result["status"], "stalled")
        self.assertFalse(result["metadata_complete"])
        self.assertEqual(result["resume_cursor"], "3_50/")
        self.assertTrue(result["synthetic_resume_cursor"])
        self.assertTrue(result["raw_path"].endswith(".incomplete.jsonl"))

    def test_stalled_endpoint_does_not_rewind_advanced_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = root / "users" / "tszzl"
            run_dir = user_dir / "runs" / "stalled-run"
            run_dir.mkdir(parents=True)
            cookie_file = root / "cookies.txt"
            cookie_file.write_text("", encoding="utf-8")

            def fake_run(command, _log_path, _prefix, **_kwargs):
                config_path = Path(command[command.index("--config-json") + 1])
                config = json.loads(config_path.read_text(encoding="utf-8"))
                processors = config["extractor"]["twitter"]["postprocessors"]
                raw = next(pp for pp in processors if pp.get("event") == "post")
                raw_path = Path(raw["base-directory"]) / raw["filename"]
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text('{"tweet_id": 50}\n', encoding="utf-8")
                return 1, "3_40/", 1.0, False, [], 0, True, 3

            with mock.patch.object(
                archive_x, "run_gallery_dl", side_effect=fake_run
            ):
                result = archive_x.archive_endpoint(
                    args=self.args(cookie_file),
                    repo_dir=REPO,
                    archive_root=root,
                    user_dir=user_dir,
                    handle="tszzl",
                    endpoint="timeline",
                    run_dir=run_dir,
                    archive_run_id="stalled-run",
                    archived_at="2026-07-15T00:00:00Z",
                    date_after=None,
                    cursor="3_100/",
                )

        self.assertEqual(result["resume_cursor"], "3_40/")
        self.assertFalse(result["synthetic_resume_cursor"])


class PendingMediaTests(unittest.TestCase):
    def test_merge_coalesces_by_filename_and_counts_distinct_run_attempts(self):
        state = {}
        failures = [
            failed_download(),
            failed_download(),
            failed_download(
                filename="2026-01-01T01-02-03_2000000000000000000_2_a.jpg",
                post_id="2000000000000000000",
                media_number=2,
            ),
            {"filename": "", "post_id": "1", "media_number": 1},
        ]
        archive_x.merge_pending_media(
            state,
            failures,
            source_run_id="run-a",
            observed_at="2026-07-14T00:00:00Z",
        )

        self.assertEqual(len(state["pending_media"]), 2)
        first = next(
            row for row in state["pending_media"] if row["filename"] == FILENAME
        )
        self.assertEqual(first["attempts"], 1)
        self.assertEqual(first["first_failed_at"], "2026-07-14T00:00:00Z")
        self.assertEqual(first["last_failed_at"], "2026-07-14T00:00:00Z")
        self.assertEqual(first["last_source_run_id"], "run-a")
        self.assertEqual(first["source_url"], f"https://x.com/i/web/status/{POST_ID}")

        archive_x.merge_pending_media(
            state,
            [failed_download(filename=f"/tmp/{FILENAME}")],
            source_run_id="run-a",
            observed_at="2026-07-14T01:00:00Z",
        )
        first = next(
            row for row in state["pending_media"] if row["filename"] == FILENAME
        )
        self.assertEqual(first["attempts"], 1)
        self.assertEqual(first["first_failed_at"], "2026-07-14T00:00:00Z")
        self.assertEqual(first["last_failed_at"], "2026-07-14T01:00:00Z")

        archive_x.merge_pending_media(
            state,
            [failed_download()],
            source_run_id="run-b",
            observed_at="2026-07-15T00:00:00Z",
        )
        first = next(
            row for row in state["pending_media"] if row["filename"] == FILENAME
        )
        self.assertEqual(first["attempts"], 2)
        self.assertEqual(first["last_source_run_id"], "run-b")

    def test_pruning_requires_final_asset_and_matching_json_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            media_dir = user_dir / "media" / "2026" / "06"
            media_dir.mkdir(parents=True)
            state = {"pending_media": [failed_download()]}
            part = media_dir / f"{FILENAME}.part"
            final = media_dir / FILENAME
            sidecar = media_dir / f"{FILENAME}.json"

            part.write_bytes(b"partial")
            sidecar.write_text("{}\n", encoding="utf-8")
            self.assertEqual(
                archive_x.prune_completed_pending_media(state, user_dir),
                [failed_download()],
            )

            part.unlink()
            sidecar.unlink()
            final.write_bytes(b"")
            sidecar.write_text("{}\n", encoding="utf-8")
            self.assertEqual(
                archive_x.prune_completed_pending_media(state, user_dir),
                [failed_download()],
            )

            final.write_bytes(b"complete")
            sidecar.write_text("", encoding="utf-8")
            self.assertEqual(
                archive_x.prune_completed_pending_media(state, user_dir),
                [failed_download()],
            )

            sidecar.write_text("not json\n", encoding="utf-8")
            self.assertEqual(
                archive_x.prune_completed_pending_media(state, user_dir),
                [failed_download()],
            )

            sidecar.write_text("{}\n", encoding="utf-8")
            self.assertEqual(
                archive_x.prune_completed_pending_media(state, user_dir), []
            )

    def test_repeated_terminal_http_failure_becomes_unavailable(self):
        state = {}
        terminal = {
            **failed_download(),
            "http_statuses": [404],
            "http_error_count": 5,
        }
        archive_x.merge_pending_media(
            state,
            [terminal],
            source_run_id="run-a",
            observed_at="2026-07-20T00:00:00Z",
        )
        self.assertEqual(len(state["pending_media"]), 1)
        self.assertEqual(state["pending_media"][0]["next_retry_at"], (
            "2026-07-21T00:00:00Z"
        ))

        archive_x.merge_pending_media(
            state,
            [terminal],
            source_run_id="run-b",
            observed_at="2026-07-21T00:00:00Z",
        )

        self.assertEqual(state["pending_media"], [])
        self.assertEqual(len(state["unavailable_media"]), 1)
        unavailable = state["unavailable_media"][0]
        self.assertEqual(unavailable["status"], "unavailable")
        self.assertEqual(
            unavailable["unavailable_reason"], "repeated_http_404_or_410"
        )

    def test_transient_failure_is_deferred_instead_of_retried_every_run(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            state = {}
            archive_x.merge_pending_media(
                state,
                [
                    {
                        **failed_download(),
                        "http_statuses": [500],
                        "http_error_count": 9,
                    }
                ],
                source_run_id="run-a",
                observed_at="2026-07-20T00:00:00Z",
            )

            self.assertEqual(
                archive_x.pending_media_due(
                    state,
                    user_dir,
                    now=archive_x.parse_datetime("2026-07-20T05:59:59Z"),
                ),
                [],
            )
            self.assertEqual(
                len(
                    archive_x.pending_media_due(
                        state,
                        user_dir,
                        now=archive_x.parse_datetime("2026-07-20T06:00:00Z"),
                    )
                ),
                1,
            )

    def test_old_pending_record_is_reclassified_from_immutable_retry_log(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            run_dir = user_dir / "runs" / "run-b"
            run_dir.mkdir(parents=True)
            (run_dir / f"retry-media-{POST_ID}.log").write_text(
                "[downloader.http][warning] '404 Not Found' for "
                "'https://pbs.twimg.com/media/test?name=orig'\n"
                + DOWNLOAD_ERROR,
                encoding="utf-8",
            )
            state = {
                "pending_media": [
                    {
                        **failed_download(),
                        "attempts": 2,
                        "first_failed_at": "2026-07-20T00:00:00Z",
                        "last_failed_at": "2026-07-22T00:00:00Z",
                        "last_source_run_id": "run-b",
                    }
                ]
            }

            changed = archive_x.reclassify_pending_media_from_logs(
                state, user_dir
            )

            self.assertEqual(changed, 1)
            self.assertEqual(state["pending_media"], [])
            self.assertEqual(state["unavailable_media"][0]["post_id"], POST_ID)


class StallRecoveryTests(unittest.TestCase):
    WAIT = "[twitter][info] Waiting for 10 minutes until 18:05:23 (rate limit)\n"

    def test_watchdog_requires_consecutive_unchanged_quota_windows(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = Path(directory) / "timeline.jsonl.partial"
            watchdog = archive_x.RateLimitProgressWatchdog(raw, 3)

            self.assertFalse(watchdog.observe(self.WAIT))
            self.assertEqual(watchdog.consecutive_stalls, 1)
            raw.write_text('{"tweet_id": 10}\n', encoding="utf-8")
            self.assertFalse(watchdog.observe(self.WAIT))
            self.assertEqual(watchdog.consecutive_stalls, 0)
            self.assertFalse(watchdog.observe(self.WAIT))
            self.assertFalse(watchdog.observe(self.WAIT))
            self.assertTrue(watchdog.observe(self.WAIT))
            self.assertEqual(watchdog.consecutive_stalls, 3)

    def test_run_gallery_stops_child_and_captures_final_cursor(self):
        child = (
            "import signal,sys,time; "
            "signal.signal(signal.SIGINT, lambda *_: "
            "(print(\"Use '-o cursor=3_50/page-token' to continue\", "
            "flush=True), sys.exit(1))); "
            "line='[twitter][info] Waiting for 10 minutes until 18:05:23 "
            "(rate limit)'; "
            "[(print(line, flush=True), time.sleep(.05)) for _ in range(3)]; "
            "time.sleep(10)"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = archive_x.run_gallery_dl(
                [sys.executable, "-c", child],
                root / "timeline.log",
                "test:timeline",
                progress_path=root / "raw.jsonl.partial",
                stalled_rate_limit_cycles=3,
            )

        status, cursor, _duration, interrupted, _failures, _errors, stalled, cycles = result
        self.assertEqual(status, 1)
        self.assertEqual(cursor, "3_50/page-token")
        self.assertFalse(interrupted)
        self.assertTrue(stalled)
        self.assertEqual(cycles, 3)

    def test_watchdog_escalates_when_child_ignores_sigint(self):
        child = (
            "import signal,time; "
            "signal.signal(signal.SIGINT, signal.SIG_IGN); "
            "line='[twitter][info] Waiting for 10 minutes until 18:05:23 "
            "(rate limit)'; "
            "[(print(line, flush=True), time.sleep(.05)) for _ in range(3)]; "
            "time.sleep(10)"
        )
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            archive_x, "CHILD_INTERRUPT_GRACE_SECONDS", 0.1
        ), mock.patch.object(
            archive_x, "CHILD_TERMINATE_GRACE_SECONDS", 0.1
        ):
            started = archive_x.time.monotonic()
            result = archive_x.run_gallery_dl(
                [sys.executable, "-c", child],
                Path(directory) / "timeline.log",
                "test:timeline",
                progress_path=Path(directory) / "raw.jsonl.partial",
                stalled_rate_limit_cycles=3,
            )
            duration = archive_x.time.monotonic() - started

        self.assertLess(duration, 2)
        self.assertLess(result[0], 0)
        self.assertFalse(result[3])
        self.assertTrue(result[6])
        self.assertEqual(result[7], 3)

    def test_checkpoint_is_not_a_resume_cursor_after_natural_exit(self):
        child = "print('[twitter][info] Archive checkpoint cursor=3_50/stale')"
        with tempfile.TemporaryDirectory() as directory:
            result = archive_x.run_gallery_dl(
                [sys.executable, "-c", child],
                Path(directory) / "timeline.log",
                "test:timeline",
            )
        self.assertEqual(result[0], 0)
        self.assertIsNone(result[1])

    def test_api_failure_prefers_advanced_checkpoint_over_stale_cursor(self):
        child = (
            "import sys; "
            "print('[twitter][info] Archive checkpoint "
            "cursor=3_1181651824673083392/'); "
            "print('[twitter][info] Waiting for 2 minutes until 14:20:24 "
            "(rate limit)'); "
            "print('[twitter][warning] API errors (1/2):'); "
            "print(\"- 'Dependency: Unspecified'\"); "
            "print('[twitter][warning] API errors (2/2):'); "
            "print(\"- 'Dependency: Unspecified'\"); "
            "print('[twitter][error] Unable to retrieve Tweets from this "
            "timeline'); "
            "print(\"[twitter][info] Use '-o "
            "cursor=3_1989611863986851957/' to continue downloading from "
            "the current position\"); "
            "sys.exit(4)"
        )
        with tempfile.TemporaryDirectory() as directory:
            result = archive_x.run_gallery_dl(
                [sys.executable, "-c", child],
                Path(directory) / "timeline.log",
                "test:timeline",
            )

        self.assertEqual(result[0], 4)
        self.assertEqual(result[1], "3_1181651824673083392/")
        self.assertEqual(result[5], 1)
        self.assertFalse(result[3])
        self.assertFalse(result[6])

    def test_download_only_failure_does_not_promote_checkpoint(self):
        child = (
            "import sys; "
            "print('[twitter][info] Archive checkpoint cursor=3_50/'); "
            "print('[download][error] Failed to download "
            "2020-05-22T17-05-54_1263878765400125440_1_visakanv.mp4'); "
            "sys.exit(4)"
        )
        with tempfile.TemporaryDirectory() as directory:
            result = archive_x.run_gallery_dl(
                [sys.executable, "-c", child],
                Path(directory) / "timeline.log",
                "test:timeline",
            )

        self.assertEqual(result[0], 4)
        self.assertIsNone(result[1])
        self.assertEqual(len(result[4]), 1)
        self.assertEqual(result[5], 0)

    def test_watchdog_prefers_checkpoint_over_stale_sigint_cursor(self):
        child = (
            "import signal,sys,time; "
            "signal.signal(signal.SIGINT, lambda *_: "
            "(print(\"Use '-o cursor=3_100/old-token' to continue\", "
            "flush=True), sys.exit(1))); "
            "print('[twitter][info] Archive checkpoint cursor=3_50/', "
            "flush=True); "
            "line='[twitter][info] Waiting for 10 minutes until 18:05:23 "
            "(rate limit)'; "
            "[(print(line, flush=True), time.sleep(.05)) for _ in range(3)]; "
            "time.sleep(10)"
        )
        with tempfile.TemporaryDirectory() as directory:
            result = archive_x.run_gallery_dl(
                [sys.executable, "-c", child],
                Path(directory) / "timeline.log",
                "test:timeline",
                progress_path=Path(directory) / "raw.jsonl.partial",
                stalled_rate_limit_cycles=3,
            )

        self.assertTrue(result[6])
        self.assertEqual(result[1], "3_50/")

    def test_synthetic_cursor_uses_oldest_valid_tweet_id(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = Path(directory) / "timeline.jsonl"
            raw.write_text(
                '{"tweet_id": 300}\n'
                'not-json\n'
                '{"tweet_id": "100"}\n'
                '{"tweet_id": 200}\n',
                encoding="utf-8",
            )
            self.assertEqual(archive_x.oldest_tweet_id(raw), "100")
            self.assertEqual(
                archive_x.synthetic_search_cursor(raw), "3_100/"
            )

    def test_manual_interrupt_prefers_demonstrably_advanced_checkpoint(self):
        self.assertEqual(
            archive_x.prefer_advanced_search_cursor(
                "3_100/old-token", "3_50/"
            ),
            "3_50/",
        )
        self.assertEqual(
            archive_x.prefer_advanced_search_cursor(
                "2_50/final-token", "3_100/"
            ),
            "3_100/",
        )
        self.assertEqual(
            archive_x.prefer_advanced_search_cursor(
                "3_50/final-token", "3_100/stale-token"
            ),
            "3_50/final-token",
        )

    def test_recovers_cursor_from_interrupted_terminal_wait_loop(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            run_dir = write_legacy_run(
                user_dir,
                interrupted=True,
                log=DOWNLOAD_ERROR + self.WAIT * 4 + "\nKeyboardInterrupt\n",
            )
            raw = run_dir / "raw" / "timeline.posts.incomplete.jsonl"
            raw.write_text(
                '{"tweet_id": 300}\n{"tweet_id": 100}\n',
                encoding="utf-8",
            )
            old_timestamp = (
                archive_x.parse_datetime(COMPLETED_AT).timestamp() - 3600
            )
            os.utime(raw, (old_timestamp, old_timestamp))
            state = {}

            recovered = archive_x.recover_stalled_interrupted_runs(
                state, user_dir, minimum_waits=3
            )

            self.assertEqual(recovered, [RUN_ID])
            self.assertEqual(state["resume"]["cursor"], "3_100/")
            self.assertTrue(state["resume"]["synthetic"])
            self.assertEqual(state["resume"]["stalled_rate_limit_cycles"], 4)
            self.assertEqual(
                archive_x.recover_stalled_interrupted_runs(
                    state, user_dir, minimum_waits=3
                ),
                [],
            )

    def test_modern_head_recovery_never_reuses_historical_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            run_dir = write_legacy_run(
                user_dir,
                interrupted=True,
                log=self.WAIT * 4 + "\nKeyboardInterrupt\n",
            )
            raw = run_dir / "raw" / "timeline.posts.incomplete.jsonl"
            raw.write_text(
                '{"tweet_id": 300}\n{"tweet_id": 100}\n',
                encoding="utf-8",
            )
            old_timestamp = (
                archive_x.parse_datetime(COMPLETED_AT).timestamp() - 3600
            )
            os.utime(raw, (old_timestamp, old_timestamp))
            historical = {
                "cursor": "3_29116490825/",
                "started_at": "2026-07-01T00:00:00Z",
            }
            state = {
                "resume": copy.deepcopy(historical),
                "modern_head": {
                    "last_successful_started_at": "2026-07-13T00:00:00Z",
                    "active": None,
                },
            }

            recovered = archive_x.recover_stalled_interrupted_runs(
                state,
                user_dir,
                minimum_waits=3,
                modern_head_mode=True,
            )

            self.assertEqual(recovered, [])
            self.assertEqual(state["resume"], historical)
            self.assertIsNone(state["modern_head"]["active"])

            manifest_path = run_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["timeline_mode"] = "modern_head"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            recovered = archive_x.recover_stalled_interrupted_runs(
                state,
                user_dir,
                minimum_waits=3,
                modern_head_mode=True,
            )
            self.assertEqual(recovered, [RUN_ID])
            self.assertEqual(state["resume"], historical)
            self.assertEqual(state["modern_head"]["active"]["cursor"], "3_100/")

    def test_does_not_synthesize_cursor_for_an_ordinary_interrupt(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            write_legacy_run(
                user_dir,
                interrupted=True,
                log="KeyboardInterrupt\n",
            )
            state = {}
            self.assertEqual(
                archive_x.recover_stalled_interrupted_runs(
                    state, user_dir, minimum_waits=3
                ),
                [],
            )
            self.assertNotIn("resume", state)

    def test_recent_raw_progress_blocks_legacy_stall_inference(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            run_dir = write_legacy_run(
                user_dir,
                interrupted=True,
                log=self.WAIT * 4 + "\nKeyboardInterrupt\n",
            )
            raw = run_dir / "raw" / "timeline.posts.incomplete.jsonl"
            recent_timestamp = (
                archive_x.parse_datetime(COMPLETED_AT).timestamp() - 60
            )
            os.utime(raw, (recent_timestamp, recent_timestamp))
            state = {}

            self.assertEqual(
                archive_x.recover_stalled_interrupted_runs(
                    state, user_dir, minimum_waits=3
                ),
                [],
            )
            self.assertNotIn("resume", state)

    def test_pruning_can_match_changed_author_suffix_by_post_and_number(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            media_dir = user_dir / "media" / "2026" / "06"
            media_dir.mkdir(parents=True)
            changed = media_dir / (
                "2026-06-16T20-20-11_2066979169897234540_1_new_name.mp4"
            )
            changed.write_bytes(b"complete")
            Path(f"{changed}.json").write_text("{}\n", encoding="utf-8")
            state = {"pending_media": [failed_download()]}

            self.assertEqual(
                archive_x.prune_completed_pending_media(state, user_dir), []
            )


class MigrationTests(unittest.TestCase):
    def test_recovers_download_only_run_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            write_legacy_run(user_dir)
            state = {"resume": {"cursor": "older", "started_at": STARTED_AT}}

            recovered = archive_x.recover_download_only_runs(state, user_dir)
            self.assertEqual(recovered, [RUN_ID])
            self.assertEqual(state["last_successful_started_at"], STARTED_AT)
            self.assertEqual(state["last_successful_completed_at"], COMPLETED_AT)
            self.assertIsNone(state["resume"])
            self.assertEqual(state["recovered_download_only_runs"], [RUN_ID])
            self.assertEqual(len(state["pending_media"]), 1)
            pending = state["pending_media"][0]
            self.assertEqual(pending["attempts"], 1)
            self.assertEqual(pending["filename"], FILENAME)
            self.assertEqual(pending["post_id"], POST_ID)
            self.assertEqual(pending["media_number"], MEDIA_NUMBER)
            self.assertEqual(pending["status"], "pending")
            self.assertEqual(pending["failure_class"], "transient")
            self.assertEqual(
                pending["next_retry_at"], "2026-07-15T06:21:00Z"
            )
            self.assertEqual(state["unavailable_media"], [])

            once = copy.deepcopy(state)
            self.assertEqual(
                archive_x.recover_download_only_runs(state, user_dir), []
            )
            self.assertEqual(state, once)

    def test_download_only_recovery_commits_only_to_modern_head(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            run_dir = write_legacy_run(user_dir)
            manifest_path = run_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["timeline_mode"] = "modern_head"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            historical = {
                "cursor": "3_29116490825/",
                "started_at": "2026-07-01T00:00:00Z",
            }
            state = {
                "resume": copy.deepcopy(historical),
                "modern_head": {
                    "last_successful_started_at": "2026-07-14T00:00:00Z",
                    "last_successful_completed_at": "2026-07-14T01:00:00Z",
                    "active": {
                        "cursor": "head-cursor",
                        "started_at": STARTED_AT,
                    },
                },
            }

            recovered = archive_x.recover_download_only_runs(
                state, user_dir, modern_head_mode=True
            )

            self.assertEqual(recovered, [RUN_ID])
            self.assertEqual(state["resume"], historical)
            self.assertEqual(
                state["modern_head"]["last_successful_started_at"], STARTED_AT
            )
            self.assertEqual(
                state["modern_head"]["last_successful_completed_at"], COMPLETED_AT
            )
            self.assertIsNone(state["modern_head"]["active"])

    def test_refuses_provisional_running_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            run_dir = write_legacy_run(user_dir)
            manifest_path = run_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["status"] = "running"
            manifest.pop("completed_at", None)
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            state = {}

            self.assertEqual(
                archive_x.recover_download_only_runs(state, user_dir), []
            )
            self.assertNotIn("last_successful_started_at", state)
            self.assertEqual(state["pending_media"], [])

    def test_finalizes_abandoned_running_manifest_without_advancing_state(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            run_dir = write_legacy_run(user_dir)
            manifest_path = run_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["status"] = "running"
            manifest.pop("completed_at", None)
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            finalized = archive_x.finalize_abandoned_manifests(
                user_dir, recovered_at="2026-07-17T02:00:00Z"
            )

            self.assertEqual(finalized, [RUN_ID])
            repaired = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(repaired["status"], "interrupted")
            self.assertEqual(
                repaired["failure_stage"],
                "process_ended_before_manifest_finalization",
            )
            self.assertEqual(repaired["completed_at"], "2026-07-17T02:00:00Z")
            self.assertTrue(repaired["finalized_on_later_startup"])

    def test_refuses_ambiguous_or_incomplete_legacy_runs(self):
        cases = {
            "cursor": {"cursor": "more-results"},
            "other-error": {
                "log": DOWNLOAD_ERROR + "[twitter][error] extraction failed\n"
            },
            "missing-raw": {"raw": False},
            "limited": {"limited": True},
            "interrupted": {"interrupted": True},
            "composite-exit": {"exit_code": 5},
            "no-download-error": {"log": "[twitter][warning] no media\n"},
        }
        for name, overrides in cases.items():
            with self.subTest(case=name), tempfile.TemporaryDirectory() as directory:
                user_dir = Path(directory) / "tszzl"
                write_legacy_run(user_dir, **overrides)
                state = {}

                self.assertEqual(
                    archive_x.recover_download_only_runs(state, user_dir), []
                )
                self.assertEqual(state["recovered_download_only_runs"], [])
                self.assertEqual(state["pending_media"], [])
                self.assertNotIn("last_successful_started_at", state)

    def test_refuses_unaddressable_media_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            write_legacy_run(
                user_dir,
                log=(
                    "[download][error] Failed to download "
                    "profile-avatar_tszzl.jpg\n"
                ),
            )
            state = {}

            self.assertEqual(
                archive_x.recover_download_only_runs(state, user_dir), []
            )
            self.assertEqual(state["pending_media"], [])

    def test_refuses_nonempty_but_invalid_raw_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            run_dir = write_legacy_run(user_dir)
            raw_path = run_dir / "raw" / "timeline.posts.incomplete.jsonl"
            raw_path.write_text("not-json\n", encoding="utf-8")
            state = {}

            self.assertEqual(
                archive_x.recover_download_only_runs(state, user_dir), []
            )
            self.assertEqual(state["pending_media"], [])

    def test_newer_resume_and_success_watermark_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "tszzl"
            write_legacy_run(user_dir)
            resume = {
                "cursor": "newer-cursor",
                "started_at": "2026-07-15T10:00:00Z",
                "date_after": "2026-07-13T10:00:00Z",
            }
            state = {
                "resume": copy.deepcopy(resume),
                "last_successful_started_at": "2026-07-15T09:00:00Z",
                "last_successful_completed_at": "2026-07-15T09:30:00Z",
            }

            self.assertEqual(
                archive_x.recover_download_only_runs(state, user_dir), [RUN_ID]
            )
            self.assertEqual(state["resume"], resume)
            self.assertEqual(
                state["last_successful_started_at"], "2026-07-15T09:00:00Z"
            )
            self.assertEqual(
                state["last_successful_completed_at"], "2026-07-15T09:30:00Z"
            )
            self.assertEqual(len(state["pending_media"]), 1)


class RecoveryParserTests(unittest.TestCase):
    def test_retry_defaults_and_overrides(self):
        parser = archive_x.build_parser(REPO)
        defaults = parser.parse_args(["--user", "tszzl"])
        self.assertEqual(defaults.http_timeout, 60)
        self.assertEqual(defaults.media_retries, 8)
        self.assertEqual(defaults.media_timeout, 300)
        self.assertEqual(defaults.stalled_rate_limit_cycles, 3)
        self.assertFalse(defaults.retry_failed_only)

        custom = parser.parse_args(
            [
                "--user",
                "tszzl",
                "--http-timeout",
                "90",
                "--media-retries",
                "12",
                "--media-timeout",
                "600",
                "--retry-failed-only",
            ]
        )
        self.assertEqual(custom.http_timeout, 90)
        self.assertEqual(custom.media_retries, 12)
        self.assertEqual(custom.media_timeout, 600)
        self.assertTrue(custom.retry_failed_only)

    def test_retry_numeric_options_must_be_positive(self):
        parser = archive_x.build_parser(REPO)
        for option in ("--http-timeout", "--media-retries", "--media-timeout"):
            with self.subTest(option=option), self.assertRaises(SystemExit):
                parser.parse_args(["--user", "tszzl", option, "0"])


class RunnerPreflightTests(unittest.TestCase):
    def test_rejects_incompatible_runner_before_archiving(self):
        failed = mock.Mock(
            returncode=32,
            stdout="",
            stderr="gallery-dl X runner: unsupported implementation\n",
        )
        with mock.patch.object(archive_x.subprocess, "run", return_value=failed):
            with self.assertRaisesRegex(
                archive_x.ArchiveError,
                "compatibility check failed.*unsupported implementation",
            ):
                archive_x.verify_gallery_dl_x_runner(REPO, "1.32.4")

    def test_accepts_matching_runner_version(self):
        passed = mock.Mock(returncode=0, stdout="1.32.4\n", stderr="")
        with mock.patch.object(archive_x.subprocess, "run", return_value=passed):
            archive_x.verify_gallery_dl_x_runner(REPO, "1.32.4")


class TimelineStateTests(unittest.TestCase):
    def test_operator_cursor_repair_is_atomic_and_stale_guarded(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "resume": {
                            "cursor": "3_100/",
                            "started_at": "2026-07-16T00:00:00Z",
                        },
                        "unrelated": "preserved",
                    }
                ),
                encoding="utf-8",
            )
            repaired = archive_x.repair_resume_cursor(
                state_path,
                expected_cursor="3_100/",
                replacement_cursor="3_50/",
                source_run_id="interrupted-run",
                repaired_at="2026-07-20T00:00:00Z",
            )
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(repaired["cursor"], "3_50/")
            self.assertEqual(saved["unrelated"], "preserved")
            self.assertEqual(
                saved["resume"]["operator_repaired_from_cursor"], "3_100/"
            )
            before = state_path.read_bytes()
            with self.assertRaises(archive_x.ArchiveError):
                archive_x.repair_resume_cursor(
                    state_path,
                    expected_cursor="3_100/",
                    replacement_cursor="3_25/",
                    source_run_id="stale",
                    repaired_at="later",
                )
            self.assertEqual(state_path.read_bytes(), before)

    def test_failure_before_first_checkpoint_preserves_existing_resume(self):
        existing = {
            "cursor": "3_100/safe-token",
            "started_at": "2026-07-16T00:00:00Z",
            "date_after": None,
            "saved_at": "2026-07-16T01:00:00Z",
        }
        state = {"resume": copy.deepcopy(existing)}

        archive_x.update_timeline_state(
            state,
            limited_run=False,
            metadata_complete=False,
            resume_cursor=None,
            handle="tszzl",
            chain_started_at="2026-07-16T00:00:00Z",
            date_after=None,
            observed_at="2026-07-17T00:00:00Z",
        )

        self.assertEqual(state, {"resume": existing})

    def test_new_checkpoint_replaces_existing_resume(self):
        state = {"resume": {"cursor": "3_100/old"}}

        archive_x.update_timeline_state(
            state,
            limited_run=False,
            metadata_complete=False,
            resume_cursor="3_50/new",
            handle="tszzl",
            chain_started_at="2026-07-16T00:00:00Z",
            date_after=None,
            observed_at="2026-07-17T00:00:00Z",
        )

        self.assertEqual(state["resume"]["cursor"], "3_50/new")
        self.assertEqual(state["resume"]["saved_at"], "2026-07-17T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
