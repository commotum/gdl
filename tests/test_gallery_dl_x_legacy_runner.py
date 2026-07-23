import importlib
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

archive_x = importlib.import_module("archive_x")
legacy = importlib.import_module("archive_x_legacy")
runner = importlib.import_module("gallery_dl_x_legacy_runner")


def response_page(*, tweet_ids=(), cursor=None, errors=()):
    entries = [
        {"entryId": f"tweet-{tweet_id}", "content": {}}
        for tweet_id in tweet_ids
    ]
    if cursor is not None:
        entries.append(
            {
                "entryId": "cursor-bottom-test",
                "content": {"value": cursor},
            }
        )
    value = {
        "data": {
            "search_by_raw_query": {
                "search_timeline": {
                    "timeline": {
                        "instructions": [
                            {"type": "TimelineAddEntries", "entries": entries}
                        ]
                    }
                }
            }
        }
    }
    if errors:
        value["errors"] = list(errors)
    return value


def page_telemetry(number, *, tweets=0, returned=None, submitted=None):
    return {
        "request_number": number,
        "query_sha256": "q",
        "submitted_cursor_sha256": submitted,
        "returned_cursor_sha256": returned,
        "cursor_repeated": bool(returned and returned == submitted),
        "tweet_entry_count": tweets,
        "api_error_count": 0,
    }


class LegacyRunnerTests(unittest.TestCase):
    def test_pinned_sources_match(self):
        runner.require_supported_legacy_gallery_dl()

    def test_changed_search_source_fails_closed(self):
        with mock.patch.object(
            runner,
            "SUPPORTED_SEARCH_TIMELINE_SHA256",
            "0" * 64,
        ):
            with self.assertRaisesRegex(
                runner.base_runner.ShimCompatibilityError,
                "search_timeline.*does not match",
            ):
                runner.require_supported_legacy_gallery_dl()

    def test_terminal_requires_no_cursor_or_configured_distinct_empty_pages(self):
        data = page_telemetry(1, tweets=3, returned="data")
        empty = [
            page_telemetry(index, returned=f"empty-{index}")
            for index in range(2, 6)
        ]
        self.assertEqual(
            runner.terminal_reason([data, *empty[:2]], 0, False, 2),
            "distinct_empty_tail",
        )
        self.assertEqual(
            runner.terminal_reason([data, *empty[:1]], 0, False, 2),
            "ambiguous",
        )
        repeated = empty.copy()
        repeated[-1] = page_telemetry(
            5, returned="same", submitted="same"
        )
        self.assertEqual(
            runner.terminal_reason([data, *repeated], 0, False, 2),
            "repeated_cursor",
        )
        self.assertEqual(
            runner.terminal_reason(
                [page_telemetry(1, tweets=1, returned=None)], 0, False, 2
            ),
            "no_cursor",
        )

    def test_recorder_hashes_cursors_and_enforces_search_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = runner.TelemetryRecorder(
                Path(directory) / "telemetry.json", 1, 1
            )
            api = types.SimpleNamespace(
                exc=types.SimpleNamespace(AbortExtraction=RuntimeError)
            )
            variables = json.dumps(
                {"rawQuery": "from:alice", "cursor": "opaque-secret"}
            )
            original = mock.Mock(
                return_value=response_page(tweet_ids=("1",), cursor="next-secret")
            )

            recorder.call(
                original,
                api,
                "/graphql/x/SearchTimeline",
                {"variables": variables},
            )

            self.assertEqual(recorder.search_requests, 1)
            self.assertEqual(recorder.pages[0]["tweet_entry_count"], 1)
            serialized = json.dumps(recorder.value(0))
            self.assertNotIn("opaque-secret", serialized)
            self.assertNotIn("next-secret", serialized)
            with self.assertRaisesRegex(RuntimeError, "request cap"):
                recorder.call(
                    original,
                    api,
                    "/graphql/x/SearchTimeline",
                    {"variables": variables},
                )
            self.assertTrue(recorder.capped)

            recorder.write(4)
            telemetry = recorder.path.read_text(encoding="utf-8")
            self.assertNotIn("opaque-secret", telemetry)
            self.assertNotIn("next-secret", telemetry)
            self.assertEqual(recorder.path.stat().st_mode & 0o777, 0o600)

    def test_runner_options_are_removed_before_gallery(self):
        path, limit, empty_tail_pages, remaining = runner.parse_runner_options(
            [
                "--archive-x-legacy-telemetry",
                "/tmp/t.json",
                "--archive-x-legacy-request-limit",
                "6",
                "--archive-x-legacy-empty-tail-pages",
                "2",
                "--version",
            ]
        )
        self.assertEqual(path, Path("/tmp/t.json"))
        self.assertEqual(limit, 6)
        self.assertEqual(empty_tail_pages, 2)
        self.assertEqual(remaining, ["--version"])

    def test_profile_identity_is_extracted_without_profile_content(self):
        response = {
            "data": {
                "user": {
                    "result": {
                        "rest_id": "16884623",
                        "legacy": {
                            "screen_name": "visakanv",
                            "description": "not persisted by telemetry",
                        },
                    }
                }
            }
        }
        self.assertEqual(runner.profile_user_ids(response), ["16884623"])
        current_shape = {
            "data": {
                "user": {
                    "result": {
                        "rest_id": "16884623",
                        "core": {"screen_name": "visakanv"},
                    }
                }
            }
        }
        self.assertEqual(runner.profile_user_ids(current_shape), ["16884623"])


class LegacyFetcherTests(unittest.TestCase):
    def test_query_uses_exact_epoch_overlap_without_id_pagination(self):
        query, url = legacy.legacy_query(
            "alice",
            "2010-10-28T00:00:00Z",
            "2010-10-29T00:00:00Z",
            include_reposts=True,
        )
        self.assertEqual(
            query,
            "from:alice since_time:1288223999 until_time:1288310401 "
            "include:retweets include:nativeretweets",
        )
        self.assertNotIn("max_id", query)
        self.assertIn("since_time%3A1288223999", url)
        self.assertIn("until_time%3A1288310401", url)

    def test_config_and_command_are_metadata_only_and_cursor_native(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = legacy.build_legacy_gallery_config(
                handle="alice",
                endpoint="legacy-test-walk-a",
                archive_root=root,
                user_dir=root / "users" / "alice",
                raw_partial=root / "raw.jsonl.partial",
                cookie_file=root / "cookies.txt",
                archive_run_id="run",
                archived_at="2026-07-22T00:00:00Z",
                request_delay="4-8",
                include_reposts=False,
                empty_tail_pages=2,
            )
            twitter = config["extractor"]["twitter"]
            self.assertNotIn("archive", twitter)
            self.assertFalse(twitter["cookies-update"])
            self.assertEqual(twitter["search-pagination"], "cursor")
            self.assertEqual(twitter["search-stop"], 1)
            self.assertFalse(twitter["quoted"])
            self.assertNotIn("post-filter", twitter)
            command = legacy.legacy_gallery_command(
                REPO,
                root / "config.json",
                root / "telemetry.json",
                request_limit=6,
                empty_tail_pages=2,
                retries=1,
                http_timeout=60,
                url="https://x.com/search?q=test",
            )
            self.assertTrue(command[1].endswith("gallery_dl_x_legacy_runner.py"))
            self.assertIn("--no-download", command)
            self.assertNotIn("--post-range", command)

    def test_raw_validation_uses_returned_dates_and_numeric_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = Path(directory) / "raw.jsonl"
            records = [
                {
                    "tweet_id": 10,
                    "date": "2010-10-28 12:00:00",
                    "author": {"id": 12345, "name": "alice"},
                    "user": {"id": 12345, "name": "alice"},
                    "reply_id": 0,
                    "retweet_id": 0,
                },
                {
                    "tweet_id": 11,
                    "date": "2010-10-29 00:00:00",
                    "author": {"id": 12345, "name": "alice"},
                    "user": {"id": 12345, "name": "alice"},
                    "reply_id": 0,
                    "retweet_id": 0,
                },
            ]
            archive_x.atomic_write_jsonl(raw, records)

            result = legacy.validate_walk_records(
                raw,
                since="2010-10-28T00:00:00Z",
                until="2010-10-29T00:00:00Z",
                requested_user_id="12345",
                requested_handle="alice",
                include_reposts=False,
            )

            self.assertEqual(result["accepted_ids"], ["10"])
            self.assertEqual(result["overlap_excluded_ids"], ["11"])

            records[0]["author"]["id"] = 999
            archive_x.atomic_write_jsonl(raw, records)
            with self.assertRaisesRegex(
                archive_x.ArchiveError, "numeric identity"
            ):
                legacy.validate_walk_records(
                    raw,
                    since="2010-10-28T00:00:00Z",
                    until="2010-10-29T00:00:00Z",
                    requested_user_id="12345",
                    requested_handle="alice",
                    include_reposts=False,
                )

    def test_telemetry_validation_rejects_query_change_and_ambiguity(self):
        query = "from:alice since_time:1 until_time:2"
        query_hash = runner.digest_text(query)
        pages = [
            {
                **page_telemetry(1, tweets=1, returned=None),
                "query_sha256": query_hash,
            }
        ]
        telemetry = {
            "schema_version": 1,
            "request_limit": 6,
            "empty_tail_pages": 2,
            "api_requests": 2,
            "search_requests": 1,
            "request_cap_reached": False,
            "terminal_reason": "no_cursor",
            "exit_code": 0,
            "pages": pages,
            "profile_user_ids": ["12345"],
            "opaque_cursor_values_persisted": False,
        }
        self.assertIs(
            legacy.validate_walk_telemetry(
                telemetry,
                expected_query=query,
                request_limit=6,
                empty_tail_pages=2,
                exit_code=0,
                expected_user_id="12345",
            ),
            telemetry,
        )
        changed = json.loads(json.dumps(telemetry))
        changed["pages"][0]["query_sha256"] = "0" * 64
        with self.assertRaisesRegex(archive_x.ArchiveError, "query changed"):
            legacy.validate_walk_telemetry(
                changed,
                expected_query=query,
                request_limit=6,
                empty_tail_pages=2,
                exit_code=0,
                expected_user_id="12345",
            )
        ambiguous = json.loads(json.dumps(telemetry))
        ambiguous["terminal_reason"] = "ambiguous"
        with self.assertRaisesRegex(archive_x.ArchiveError, "ambiguous"):
            legacy.validate_walk_telemetry(
                ambiguous,
                expected_query=query,
                request_limit=6,
                empty_tail_pages=2,
                exit_code=0,
                expected_user_id="12345",
            )

    def test_one_walk_writes_evidence_but_has_no_state_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = root / "users" / "alice"
            run_dir = user_dir / "runs" / "run-a"
            query, _ = legacy.legacy_query(
                "alice",
                "2010-10-28T00:00:00Z",
                "2010-10-29T00:00:00Z",
                include_reposts=False,
            )

            def fake_run(command, _log_path, _prefix, **kwargs):
                raw = kwargs["progress_path"]
                archive_x.atomic_write_jsonl(
                    raw,
                    [
                        {
                            "tweet_id": 10,
                            "date": "2010-10-28 12:00:00",
                            "author": {"id": 12345, "name": "alice"},
                            "user": {"id": 12345, "name": "alice"},
                            "reply_id": 0,
                            "retweet_id": 0,
                        }
                    ],
                )
                telemetry = Path(
                    command[
                        command.index("--archive-x-legacy-telemetry") + 1
                    ]
                )
                archive_x.atomic_write_json(
                    telemetry,
                    {
                        "schema_version": 1,
                        "request_limit": 6,
                        "empty_tail_pages": 2,
                        "api_requests": 2,
                        "search_requests": 1,
                        "request_cap_reached": False,
                        "terminal_reason": "no_cursor",
                        "exit_code": 0,
                        "pages": [
                            {
                                **page_telemetry(1, tweets=1, returned=None),
                                "query_sha256": runner.digest_text(query),
                            }
                        ],
                        "profile_user_ids": ["12345"],
                        "opaque_cursor_values_persisted": False,
                    },
                )
                return 0, None, 1.25, False, [], 0, False, 0

            with mock.patch.object(
                legacy.archive_x, "run_gallery_dl", side_effect=fake_run
            ):
                result = legacy.run_legacy_walk(
                    repo_dir=REPO,
                    archive_root=root,
                    user_dir=user_dir,
                    run_dir=run_dir,
                    handle="alice",
                    requested_user_id="12345",
                    archive_run_id="run-a",
                    window_id_value="legacy-window",
                    walk_id="walk-a",
                    since="2010-10-28T00:00:00Z",
                    until="2010-10-29T00:00:00Z",
                    cookie_file=root / "cookies.txt",
                    request_delay="4-8",
                    include_reposts=False,
                    request_limit=6,
                    empty_tail_pages=2,
                    retries=1,
                    http_timeout=60,
                    stalled_rate_limit_cycles=3,
                )

            self.assertEqual(result["status"], "valid")
            self.assertEqual(result["records"]["accepted_ids"], ["10"])
            raw = user_dir / result["raw_path"]
            self.assertTrue(raw.name.endswith(".posts.jsonl"))
            self.assertEqual(raw.stat().st_mode & 0o777, 0o600)
            self.assertFalse((user_dir / "_state" / "state.json").exists())

    def test_interrupted_walk_retains_incomplete_raw_and_cannot_validate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = root / "users" / "alice"
            run_dir = user_dir / "runs" / "run-a"

            def fake_run(_command, _log_path, _prefix, **kwargs):
                archive_x.atomic_write_jsonl(
                    kwargs["progress_path"],
                    [
                        {
                            "tweet_id": 10,
                            "date": "2010-10-28 12:00:00",
                            "author": {"id": 12345, "name": "alice"},
                            "user": {"id": 12345, "name": "alice"},
                        }
                    ],
                )
                return 130, None, 1.0, True, [], 0, False, 0

            with mock.patch.object(
                legacy.archive_x, "run_gallery_dl", side_effect=fake_run
            ):
                result = legacy.run_legacy_walk(
                    repo_dir=REPO,
                    archive_root=root,
                    user_dir=user_dir,
                    run_dir=run_dir,
                    handle="alice",
                    requested_user_id="12345",
                    archive_run_id="run-a",
                    window_id_value="legacy-window",
                    walk_id="walk-a",
                    since="2010-10-28T00:00:00Z",
                    until="2010-10-29T00:00:00Z",
                    cookie_file=root / "cookies.txt",
                    request_delay="4-8",
                    include_reposts=False,
                    request_limit=6,
                    empty_tail_pages=2,
                    retries=1,
                    http_timeout=60,
                    stalled_rate_limit_cycles=3,
                )

            self.assertEqual(result["status"], "ambiguous")
            self.assertTrue(result["interrupted"])
            self.assertTrue(result["raw_path"].endswith(".incomplete.jsonl"))


if __name__ == "__main__":
    unittest.main()
