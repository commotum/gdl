import importlib
import io
import json
import sys
import tempfile
import types
import unittest
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from gallery_dl.extractor.twitter import TwitterAPI, TwitterExtractor


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
archive_x = importlib.import_module("archive_x")
archive_x_legacy = importlib.import_module("archive_x_legacy")

FIXTURE = json.loads(
    (REPO / "tests" / "fixtures" / "x_legacy_transition.json").read_text(
        encoding="utf-8"
    )
)


def search_page(*, cursor=None, keep_going_on_empty=False):
    entries = []
    if cursor is not None:
        entries.append(
            {
                "entryId": "cursor-bottom-test",
                "content": {
                    "value": cursor,
                    "stopOnEmptyResponse": not keep_going_on_empty,
                },
            }
        )
    return {
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


class FakeSearchExtractor:
    def __init__(self, pagination, initial_cursor=None):
        self.pagination = pagination
        self.initial_cursor = initial_cursor
        self.updated_cursors = []
        self.retweets = False
        self.pinned = False
        self.ads = False
        self.showreplies = False
        self._user_obj = None
        self.log = mock.Mock()
        self.exc = types.SimpleNamespace(
            AbortExtraction=RuntimeError,
            AuthorizationError=RuntimeError,
        )

    def config(self, key, default=None):
        values = {
            "search-pagination": self.pagination,
            "search-stop": 0,
        }
        return values.get(key, default)

    def _init_cursor(self):
        return self.initial_cursor

    def _update_cursor(self, cursor):
        self.updated_cursors.append(cursor)
        return cursor


def fake_search_api(extractor, pages):
    api = object.__new__(TwitterAPI)
    api.extractor = extractor
    api.log = extractor.log
    api.exc = extractor.exc
    api._json_dumps = json.dumps
    api.features_pagination = {}
    remaining_pages = list(pages)
    api.seen_variables = []

    def call(_endpoint, params):
        api.seen_variables.append(json.loads(params["variables"]))
        return remaining_pages.pop(0)

    api._call = mock.Mock(side_effect=call)
    return api


class LegacyBoundaryCharacterizationTests(unittest.TestCase):
    def test_fixture_records_exact_id_domain_discontinuity_and_stall(self):
        snowflake, first_legacy, boundary = FIXTURE["records"]

        self.assertEqual(snowflake["tweet_id"], "402691293450240")
        self.assertEqual(first_legacy["tweet_id"], "29675373972")
        self.assertEqual(boundary["tweet_id"], "29116490825")
        self.assertEqual(boundary["date"], "2010-10-29 19:30:34")
        self.assertEqual(
            FIXTURE["checkpoints"], ["3_29116490825/"] * 4
        )
        self.assertEqual(FIXTURE["final_status"], "stalled")

    def test_installed_max_id_paginator_applies_snowflake_math_to_legacy_id(self):
        legacy_id = int(FIXTURE["records"][-1]["tweet_id"])
        api = object.__new__(TwitterAPI)
        api.extractor = mock.Mock()
        api.extractor._user = {"id": FIXTURE["account_id"]}
        api.extractor._cursor_prefix = "3_1173685814485643265/"
        api.log = mock.Mock()
        api._var_maxid_prev = None
        variables = {
            "rawQuery": f"from:visakanv max_id:{legacy_id}",
            "cursor": "server-cursor",
        }

        updated = api._update_variables_search_maxid(
            variables, "server-cursor", {"id_str": str(legacy_id)}
        )

        snowflake_boundary = (legacy_id - 0x400000) | 0x3FFFFF
        self.assertEqual(
            updated["rawQuery"],
            f"from:visakanv max_id:{snowflake_boundary}",
        )
        self.assertNotEqual(snowflake_boundary, legacy_id - 1)
        self.assertIsNone(updated["cursor"])
        self.assertEqual(
            api.extractor._cursor_prefix, f"3_{legacy_id}/"
        )

    def test_installed_date_paginator_misdecodes_legacy_id(self):
        boundary = FIXTURE["records"][-1]
        decoded = TwitterExtractor._tweetid_to_datetime(
            None, int(boundary["tweet_id"])
        )

        self.assertEqual(str(decoded), "2010-11-04 01:43:01")
        self.assertNotEqual(str(decoded), boundary["date"])

    def test_advanced_checkpoint_wins_over_stale_shutdown_cursor(self):
        self.assertEqual(
            archive_x.prefer_advanced_search_cursor(
                FIXTURE["stale_shutdown_cursor"],
                FIXTURE["saved_cursor"],
            ),
            FIXTURE["saved_cursor"],
        )

    def test_fixed_query_can_use_server_cursor_without_id_or_date_mutation(self):
        extractor = FakeSearchExtractor("cursor")
        api = fake_search_api(
            extractor,
            [
                search_page(cursor="opaque-page-2", keep_going_on_empty=True),
                search_page(),
            ],
        )
        query = "from:visakanv since:2010-10-28 until:2010-10-29"

        self.assertEqual(list(api.search_timeline(query)), [])

        self.assertEqual(api._call.call_count, 2)
        first, second = api.seen_variables
        self.assertEqual(first["rawQuery"], query)
        self.assertEqual(second["rawQuery"], query)
        self.assertNotIn("cursor", first)
        self.assertEqual(second["cursor"], "opaque-page-2")
        self.assertEqual(extractor.updated_cursors, ["opaque-page-2", None])

    def test_repeated_server_cursor_currently_looks_like_normal_termination(self):
        extractor = FakeSearchExtractor("cursor", initial_cursor="repeat")
        api = fake_search_api(
            extractor,
            [search_page(cursor="repeat", keep_going_on_empty=True)],
        )

        self.assertEqual(
            list(
                api.search_timeline(
                    "from:visakanv since:2010-10-28 until:2010-10-29"
                )
            ),
            [],
        )
        self.assertEqual(api._call.call_count, 1)
        self.assertEqual(extractor.updated_cursors, [None])

    def test_api_error_is_not_a_terminal_empty_page(self):
        extractor = FakeSearchExtractor("cursor")
        api = fake_search_api(
            extractor,
            [{"errors": [{"message": "Dependency: Unspecified"}]}],
        )

        with self.assertRaisesRegex(
            RuntimeError, "Unable to retrieve Tweets from this timeline"
        ):
            list(
                api.search_timeline(
                    "from:visakanv since_time:1288224000 "
                    "until_time:1288310400"
                )
            )


def fixture_archive(root: Path):
    user_dir = root / "users" / "alice"
    state_path = user_dir / "_state" / "state.json"
    state = {
        "schema": archive_x.SCHEMA_NAME,
        "schema_version": archive_x.SCHEMA_VERSION,
        "requested_handle": "alice",
        "canonical_handle": "alice",
        "requested_user_id": "12345",
        "resume": {
            "cursor": "3_29116490825/",
            "started_at": "2026-07-20T00:00:00Z",
            "date_after": None,
            "saved_at": "2026-07-21T00:00:00Z",
        },
        "pending_media": [{"filename": "keep.jpg", "post_id": "99"}],
        "unrelated": {"keep": True},
    }
    archive_x.atomic_write_json(state_path, state)
    archive_x.atomic_write_json(
        user_dir / "dataset" / "profile.json",
        {
            "profile": {
                "id": 12345,
                "name": "alice",
                "date": "2008-10-21 12:01:00",
            }
        },
    )
    archive_x.atomic_write_jsonl(
        user_dir / "dataset" / "posts.jsonl",
        [
            {
                "post_id": "29116490825",
                "posted_at": "2010-10-29 19:30:34",
            },
            {
                "post_id": "30000000000",
                "posted_at": "2010-10-30 01:00:00",
            },
        ],
    )
    run_id = "20260720T023918Z-fixture"
    archive_x.atomic_write_json(
        user_dir / "runs" / run_id / "manifest.json",
        {
            "run_id": run_id,
            "completed_at": "2026-07-21T01:04:43Z",
            "status": "stalled",
            "reposts_included": True,
            "endpoints": [
                {
                    "endpoint": "timeline",
                    "status": "stalled",
                    "resume_cursor": "3_29116490825/",
                }
            ],
        },
    )
    return user_dir, state_path, state


class LegacyStateTests(unittest.TestCase):
    def test_initialization_plan_is_exact_and_stale_guarded(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, _ = fixture_archive(Path(directory))
            before_hash = archive_x.sha256_file(state_path)

            plan = archive_x_legacy.initialization_plan(user_dir)

            self.assertFalse(plan["already_initialized"])
            self.assertEqual(
                plan["source"]["cursor"], "3_29116490825/"
            )
            self.assertEqual(plan["source"]["oldest_post_id"], "29116490825")
            self.assertEqual(
                plan["source"]["oldest_post_at"], "2010-10-29T19:30:34Z"
            )
            self.assertEqual(plan["source"]["dataset_post_count"], 2)
            self.assertEqual(
                plan["source"]["state_sha256_before_init"], before_hash
            )
            self.assertEqual(
                plan["proposed"],
                {
                    "requested_user_id": "12345",
                    "initial_until": "2010-10-30T00:00:00Z",
                    "next_until": "2010-10-30T00:00:00Z",
                    "floor_since": "2008-10-21T12:01:00Z",
                },
            )
            self.assertRegex(plan["confirmation_token"], r"^[0-9a-f]{64}$")
            self.assertEqual(archive_x.sha256_file(state_path), before_hash)

    def test_initialize_preserves_unrelated_and_modern_state(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, original = fixture_archive(Path(directory))
            plan = archive_x_legacy.initialization_plan(user_dir)

            state = archive_x.load_json(state_path, {})
            updated, changed = archive_x_legacy.initialize_state(
                state,
                plan,
                plan["confirmation_token"],
                "2026-07-22T12:00:00Z",
            )

            self.assertTrue(changed)
            for key, value in original.items():
                self.assertEqual(updated[key], value)
            self.assertEqual(updated["legacy_backfill"]["status"], "pending")
            self.assertEqual(
                updated["legacy_backfill"]["source"]["cursor"],
                original["resume"]["cursor"],
            )
            with self.assertRaisesRegex(
                archive_x.ArchiveError, "stale or incorrect"
            ):
                archive_x_legacy.initialize_state(
                    state, plan, "0" * 64, "2026-07-22T12:00:00Z"
                )

    def test_repeated_initialization_is_idempotent_but_changed_token_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, _ = fixture_archive(Path(directory))
            plan = archive_x_legacy.initialization_plan(user_dir)
            state = archive_x.load_json(state_path, {})
            initialized, _ = archive_x_legacy.initialize_state(
                state, plan, plan["confirmation_token"], "2026-07-22T12:00:00Z"
            )

            repeated, changed = archive_x_legacy.initialize_state(
                initialized,
                {},
                plan["confirmation_token"],
                "2026-07-22T13:00:00Z",
            )

            self.assertFalse(changed)
            self.assertEqual(repeated, initialized)
            with self.assertRaisesRegex(
                archive_x.ArchiveError, "different evidence"
            ):
                archive_x_legacy.initialize_state(
                    initialized, {}, "f" * 64, "2026-07-22T13:00:00Z"
                )

    def test_validation_rejects_unknown_version_identity_and_bad_frontier(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, _ = fixture_archive(Path(directory))
            plan = archive_x_legacy.initialization_plan(user_dir)
            state = archive_x.load_json(state_path, {})
            initialized, _ = archive_x_legacy.initialize_state(
                state, plan, plan["confirmation_token"], "2026-07-22T12:00:00Z"
            )
            legacy = initialized["legacy_backfill"]

            unknown = json.loads(json.dumps(legacy))
            unknown["schema_version"] = 2
            with self.assertRaisesRegex(
                archive_x.ArchiveError, "unsupported.*schema version"
            ):
                archive_x_legacy.validate_legacy_state(unknown)
            with self.assertRaisesRegex(
                archive_x.ArchiveError, "identity changed"
            ):
                archive_x_legacy.validate_legacy_state(
                    legacy, expected_user_id="999"
                )
            invalid = json.loads(json.dumps(legacy))
            invalid["next_until"] = "2008-01-01T00:00:00Z"
            with self.assertRaisesRegex(
                archive_x.ArchiveError, "frontier order"
            ):
                archive_x_legacy.validate_legacy_state(invalid)

    def test_claim_manual_review_and_completion_are_guarded(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, _ = fixture_archive(Path(directory))
            plan = archive_x_legacy.initialization_plan(user_dir)
            state = archive_x.load_json(state_path, {})
            initialized, _ = archive_x_legacy.initialize_state(
                state, plan, plan["confirmation_token"], "2026-07-22T12:00:00Z"
            )
            legacy = initialized["legacy_backfill"]

            active = archive_x_legacy.claim_window(
                legacy,
                owner_run_id="run-a",
                claimed_at="2026-07-22T12:01:00Z",
            )

            window = active["active_window"]
            self.assertEqual(window["since"], "2010-10-29T00:00:00Z")
            self.assertEqual(window["until"], "2010-10-30T00:00:00Z")
            with self.assertRaisesRegex(archive_x.ArchiveError, "window guard"):
                archive_x_legacy.complete_window(
                    active,
                    window_id_value="wrong",
                    completed_at="2026-07-22T12:02:00Z",
                    canonical_raw_sha256="a" * 64,
                    dataset_sha256="b" * 64,
                    walk_ids=["a", "b"],
                )
            completed = archive_x_legacy.complete_window(
                active,
                window_id_value=window["window_id"],
                completed_at="2026-07-22T12:02:00Z",
                canonical_raw_sha256="a" * 64,
                dataset_sha256="b" * 64,
                walk_ids=["walk-b", "walk-a"],
            )
            self.assertEqual(completed["status"], "pending")
            self.assertEqual(
                completed["next_until"], "2010-10-29T00:00:00Z"
            )

            review = archive_x_legacy.mark_manual_review(
                active,
                window_id_value=window["window_id"],
                reason="ambiguous pagination",
                observed_at="2026-07-22T12:03:00Z",
            )
            self.assertEqual(review["status"], "manual_review")
            self.assertEqual(review["next_until"], legacy["next_until"])

    def test_status_and_plan_cli_are_write_free_and_run_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, state_path, _ = fixture_archive(root)
            before = archive_x.sha256_file(state_path)
            output = io.StringIO()
            with mock.patch.object(
                archive_x_legacy.archive_x,
                "atomic_write_json",
                side_effect=AssertionError("unexpected write"),
            ), redirect_stdout(output):
                self.assertEqual(
                    archive_x_legacy.main(
                        ["--user", "alice", "--output-root", str(root), "status"]
                    ),
                    0,
                )
                self.assertEqual(
                    archive_x_legacy.main(
                        ["--user", "alice", "--output-root", str(root), "plan"]
                    ),
                    0,
                )
            self.assertEqual(archive_x.sha256_file(state_path), before)
            errors = io.StringIO()
            with redirect_stderr(errors), self.assertRaises(SystemExit) as raised:
                archive_x_legacy.main(
                    [
                        "--user",
                        "alice",
                        "--output-root",
                        str(root),
                        "run",
                        "--windows",
                        "1",
                    ]
                )
            self.assertEqual(raised.exception.code, 2)
            self.assertIn("not initialized", errors.getvalue())
            self.assertEqual(archive_x.sha256_file(state_path), before)

    def test_cli_init_is_atomic_private_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, original = fixture_archive(root)
            plan = archive_x_legacy.initialization_plan(user_dir)
            args = [
                "--user",
                "alice",
                "--output-root",
                str(root),
                "init",
                "--token",
                plan["confirmation_token"],
            ]

            with redirect_stdout(io.StringIO()):
                self.assertEqual(archive_x_legacy.main(args), 0)
            initialized_bytes = state_path.read_bytes()
            initialized = archive_x.load_json(state_path, {})
            self.assertEqual(initialized["resume"], original["resume"])
            self.assertEqual(initialized["pending_media"], original["pending_media"])
            self.assertEqual(initialized["legacy_backfill"]["status"], "pending")
            self.assertEqual(state_path.stat().st_mode & 0o777, 0o600)
            backups = list((user_dir / "_state" / "backups").glob("*.json"))
            self.assertEqual(len(backups), 1)
            self.assertEqual(archive_x.load_json(backups[0], {}), original)
            self.assertEqual(backups[0].stat().st_mode & 0o777, 0o600)

            with redirect_stdout(io.StringIO()):
                self.assertEqual(archive_x_legacy.main(args), 0)
            self.assertEqual(state_path.read_bytes(), initialized_bytes)

    def test_cli_init_failed_atomic_write_preserves_previous_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, _ = fixture_archive(root)
            plan = archive_x_legacy.initialization_plan(user_dir)
            before = state_path.read_bytes()
            errors = io.StringIO()

            with mock.patch.object(
                archive_x_legacy.archive_x,
                "atomic_write_json",
                side_effect=OSError("injected write failure"),
            ), redirect_stderr(errors), self.assertRaises(SystemExit) as raised:
                archive_x_legacy.main(
                    [
                        "--user",
                        "alice",
                        "--output-root",
                        str(root),
                        "init",
                        "--token",
                        plan["confirmation_token"],
                    ]
                )
            self.assertEqual(raised.exception.code, 2)
            self.assertIn("injected write failure", errors.getvalue())
            self.assertEqual(state_path.read_bytes(), before)

    def test_status_summaries_cover_every_lifecycle_without_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, _ = fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            state["cookie_sentinel"] = "must-not-appear"

            absent = archive_x_legacy.legacy_status_summary(state, "alice")
            self.assertEqual(absent["status"], "not_initialized")
            self.assertEqual(absent["network_requests"], 0)

            plan = archive_x_legacy.initialization_plan(user_dir)
            initialized, _ = archive_x_legacy.initialize_state(
                state, plan, plan["confirmation_token"], "2026-07-22T12:00:00Z"
            )
            pending = archive_x_legacy.legacy_status_summary(initialized, "alice")
            self.assertEqual(pending["status"], "pending")
            self.assertEqual(
                pending["next_window"],
                {
                    "since": "2010-10-29T00:00:00Z",
                    "until": "2010-10-30T00:00:00Z",
                },
            )
            self.assertIn("source-visible", pending["coverage"]["meaning"])
            self.assertNotIn("must-not-appear", json.dumps(pending))

            active_legacy = archive_x_legacy.claim_window(
                initialized["legacy_backfill"],
                owner_run_id="run-a",
                claimed_at="2026-07-22T12:01:00Z",
            )
            active_state = {**initialized, "legacy_backfill": active_legacy}
            active = archive_x_legacy.legacy_status_summary(active_state, "alice")
            self.assertEqual(active["status"], "active")
            self.assertIsNotNone(active["active_window"])

            window = active_legacy["active_window"]
            review_legacy = archive_x_legacy.mark_manual_review(
                active_legacy,
                window_id_value=window["window_id"],
                reason="ambiguous",
                observed_at="2026-07-22T12:02:00Z",
            )
            review_state = {**initialized, "legacy_backfill": review_legacy}
            review = archive_x_legacy.legacy_status_summary(review_state, "alice")
            self.assertEqual(review["status"], "manual_review")
            self.assertIn(window["window_id"], review["next_command"])

            complete_legacy = json.loads(json.dumps(initialized["legacy_backfill"]))
            complete_legacy["next_until"] = complete_legacy["floor_since"]
            complete_legacy["status"] = "complete"
            complete_legacy[
                "coverage_conclusion"
            ] = "source_visible_to_account_creation"
            complete_state = {**initialized, "legacy_backfill": complete_legacy}
            complete = archive_x_legacy.legacy_status_summary(
                complete_state, "alice"
            )
            self.assertEqual(complete["status"], "complete")
            self.assertIsNone(complete["next_command"])

    def test_plan_prints_exact_guarded_initialization_command(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, _, _ = fixture_archive(Path(directory))
            plan = archive_x_legacy.initialization_plan(user_dir)
            self.assertEqual(
                plan["initialization_command"],
                "scripts/archive-x-legacy --user alice init --token "
                + plan["confirmation_token"],
            )


def initialized_fixture_archive(root: Path):
    user_dir, state_path, original = fixture_archive(root)
    plan = archive_x_legacy.initialization_plan(user_dir)
    state = archive_x.load_json(state_path, {})
    initialized, _ = archive_x_legacy.initialize_state(
        state, plan, plan["confirmation_token"], "2026-07-22T12:00:00Z"
    )
    archive_x.atomic_write_json(state_path, initialized)
    return user_dir, state_path, original


def legacy_run_args(root: Path, **overrides):
    values = {
        "windows": 1,
        "request_limit": 6,
        "walk_attempts": 3,
        "window_attempts": 3,
        "max_leaves": 64,
        "walk_delay": "0",
        "window_delay": "0",
        "request_delay": "0",
        "cookies": root / "cookies.txt",
        "retries": 1,
        "http_timeout": 60,
        "stalled_rate_limit_cycles": 3,
    }
    values.update(overrides)
    return Namespace(**values)


def valid_walk(kwargs, post_id="29000000000", date="2010-10-29 12:00:00", count=0):
    metadata = {
        "tweet_id": int(post_id),
        "date": date,
        "author": {"id": 12345, "name": "alice"},
        "user": {"id": 12345, "name": "alice"},
        "reply_id": 0,
        "retweet_id": 0,
        "count": count,
        "archived_at": "2026-07-22T12:00:00Z",
    }
    return {
        "walk_id": kwargs["walk_id"],
        "endpoint": kwargs["walk_id"],
        "since": kwargs["since"],
        "until": kwargs["until"],
        "query_sha256": "a" * 64,
        "status": "valid",
        "exit_code": 0,
        "duration_seconds": 1.0,
        "interrupted": False,
        "stalled": False,
        "stalled_rate_limit_cycles": 0,
        "validation_error": None,
        "terminal_reason": "no_cursor",
        "records": {
            "raw_count": 1,
            "accepted_count": 1,
            "accepted_ids": [str(post_id)],
            "accepted_records": [metadata],
            "overlap_excluded_ids": [],
        },
        "raw_path": f"runs/fake/{kwargs['walk_id']}.jsonl",
        "raw_sha256": "b" * 64,
        "telemetry_path": f"runs/fake/{kwargs['walk_id']}.telemetry.json",
        "telemetry_sha256": "c" * 64,
        "config_path": f"runs/fake/{kwargs['walk_id']}.config.json",
        "config_sha256": "d" * 64,
        "log_path": f"runs/fake/{kwargs['walk_id']}.log",
        "command": ["fake"],
    }


class LegacyOrchestrationTests(unittest.TestCase):
    def test_two_matching_walks_merge_then_advance_and_queue_media(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, original = initialized_fixture_archive(root)

            def fake_walk(**kwargs):
                return valid_walk(kwargs, count=1)

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=fake_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4"
                )

            state = archive_x.load_json(state_path, {})
            self.assertEqual(result["status"], "success")
            self.assertEqual(
                state["legacy_backfill"]["next_until"],
                "2010-10-29T00:00:00Z",
            )
            self.assertEqual(state["resume"], original["resume"])
            self.assertEqual(
                [item["key"] for item in state["pending_media"] if item.get("key")],
                ["post:29000000000"],
            )
            posts = list(archive_x.iter_jsonl(user_dir / "dataset" / "posts.jsonl"))
            self.assertEqual(len(posts), 3)
            self.assertIn("29000000000", {item["post_id"] for item in posts})
            self.assertTrue(result["windows"][0]["state_committed"])

    def test_mismatched_walks_enter_manual_review_without_advancing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, _ = initialized_fixture_archive(root)
            sequence = iter(
                [
                    ("29000000000", "2010-10-29 12:00:00"),
                    ("29000000001", "2010-10-29 13:00:00"),
                    ("29000000002", "2010-10-29 14:00:00"),
                ]
            )

            def fake_walk(**kwargs):
                post_id, date = next(sequence)
                return valid_walk(kwargs, post_id=post_id, date=date)

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=fake_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4"
                )

            state = archive_x.load_json(state_path, {})
            self.assertEqual(result["status"], "manual_review")
            self.assertEqual(state["legacy_backfill"]["status"], "manual_review")
            self.assertEqual(
                state["legacy_backfill"]["next_until"],
                "2010-10-30T00:00:00Z",
            )
            self.assertEqual(
                len(list(archive_x.iter_jsonl(user_dir / "dataset" / "posts.jsonl"))),
                2,
            )

    def test_request_cap_splits_exactly_then_confirms_newer_leaf_first(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, state_path, _ = initialized_fixture_archive(root)
            calls = []

            def fake_walk(**kwargs):
                calls.append((kwargs["since"], kwargs["until"]))
                if len(calls) == 1:
                    result = valid_walk(kwargs)
                    result.update(
                        {
                            "status": "ambiguous",
                            "exit_code": 4,
                            "terminal_reason": "request_cap",
                            "records": None,
                            "validation_error": "request cap",
                        }
                    )
                    return result
                if kwargs["since"] == "2010-10-29T12:00:00Z":
                    return valid_walk(
                        kwargs,
                        post_id="29000000002",
                        date="2010-10-29 18:00:00",
                    )
                return valid_walk(
                    kwargs,
                    post_id="29000000001",
                    date="2010-10-29 06:00:00",
                )

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=fake_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4"
                )

            self.assertEqual(result["status"], "success")
            self.assertEqual(
                calls[1],
                ("2010-10-29T12:00:00Z", "2010-10-30T00:00:00Z"),
            )
            self.assertEqual(
                calls[3],
                ("2010-10-29T00:00:00Z", "2010-10-29T12:00:00Z"),
            )
            self.assertEqual(
                archive_x.load_json(state_path, {})["legacy_backfill"]["next_until"],
                "2010-10-29T00:00:00Z",
            )


class LegacyRecoveryTests(unittest.TestCase):
    def test_crash_after_dataset_merge_replays_and_deduplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, original = initialized_fixture_archive(root)

            def fake_walk(**kwargs):
                return valid_walk(kwargs)

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=fake_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ), mock.patch.object(
                archive_x_legacy,
                "complete_window",
                side_effect=archive_x.ArchiveError("injected after dataset merge"),
            ):
                with self.assertRaisesRegex(
                    archive_x.ArchiveError, "injected after dataset merge"
                ):
                    archive_x_legacy.run_legacy_archive(
                        legacy_run_args(root), REPO, root, "alice", "1.32.4"
                    )

            after_crash = archive_x.load_json(state_path, {})
            self.assertEqual(after_crash["legacy_backfill"]["status"], "active")
            self.assertEqual(after_crash["resume"], original["resume"])
            self.assertEqual(
                len(list(archive_x.iter_jsonl(user_dir / "dataset" / "posts.jsonl"))),
                3,
            )

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=fake_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4"
                )

            recovered = archive_x.load_json(state_path, {})
            self.assertEqual(result["status"], "success")
            self.assertEqual(
                recovered["legacy_backfill"]["next_until"],
                "2010-10-29T00:00:00Z",
            )
            self.assertEqual(
                len(list(archive_x.iter_jsonl(user_dir / "dataset" / "posts.jsonl"))),
                3,
            )
            self.assertIn(
                "interrupted",
                {item["status"] for item in result["recovered_manifests"]},
            )

    def test_crash_after_state_commit_recovers_exact_manifest_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, _ = initialized_fixture_archive(root)

            def fake_walk(**kwargs):
                return valid_walk(kwargs)

            original_write = archive_x.atomic_write_json
            injected = False

            def fail_final_manifest(path, value):
                nonlocal injected
                windows = value.get("windows") if isinstance(value, dict) else None
                if (
                    not injected
                    and Path(path).name == "manifest.json"
                    and isinstance(windows, list)
                    and windows
                    and windows[-1].get("state_committed") is True
                ):
                    injected = True
                    raise OSError("injected final manifest failure")
                return original_write(path, value)

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=fake_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ), mock.patch.object(
                archive_x_legacy.archive_x,
                "atomic_write_json",
                side_effect=fail_final_manifest,
            ):
                with self.assertRaisesRegex(OSError, "final manifest failure"):
                    archive_x_legacy.run_legacy_archive(
                        legacy_run_args(root), REPO, root, "alice", "1.32.4"
                    )

            state = archive_x.load_json(state_path, {})
            self.assertEqual(
                state["legacy_backfill"]["next_until"],
                "2010-10-29T00:00:00Z",
            )
            recovered = archive_x_legacy.recover_legacy_manifests(
                user_dir, state, recovered_at="2026-07-22T13:00:00Z"
            )
            self.assertEqual(
                [item["status"] for item in recovered], ["recovered_success"]
            )
            self.assertEqual(
                archive_x_legacy.recover_legacy_manifests(
                    user_dir, state, recovered_at="2026-07-22T13:01:00Z"
                ),
                [],
            )
            manifests = [
                archive_x.load_json(path, {})
                for path in (user_dir / "runs").glob("*/manifest.json")
                if archive_x.load_json(path, {}).get("mode") == "legacy_backfill"
            ]
            self.assertEqual(manifests[0]["status"], "recovered_success")
            self.assertTrue(
                manifests[0]["windows"][-1]["recovered_after_state_commit"]
            )

    def test_failed_frontier_atomic_write_leaves_active_window_replayable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, _ = initialized_fixture_archive(root)

            def fake_walk(**kwargs):
                return valid_walk(kwargs)

            original_write = archive_x.atomic_write_json

            def fail_frontier(path, value):
                legacy_state = value.get("legacy_backfill") if isinstance(value, dict) else None
                if (
                    Path(path) == state_path
                    and isinstance(legacy_state, dict)
                    and legacy_state.get("next_until") == "2010-10-29T00:00:00Z"
                ):
                    raise OSError("injected frontier write failure")
                return original_write(path, value)

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=fake_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ), mock.patch.object(
                archive_x_legacy.archive_x,
                "atomic_write_json",
                side_effect=fail_frontier,
            ):
                with self.assertRaisesRegex(OSError, "frontier write failure"):
                    archive_x_legacy.run_legacy_archive(
                        legacy_run_args(root), REPO, root, "alice", "1.32.4"
                    )

            state = archive_x.load_json(state_path, {})
            self.assertEqual(state["legacy_backfill"]["status"], "active")
            self.assertEqual(
                state["legacy_backfill"]["next_until"],
                "2010-10-30T00:00:00Z",
            )
            self.assertEqual(
                len(list(archive_x.iter_jsonl(user_dir / "dataset" / "posts.jsonl"))),
                3,
            )

    def test_manual_review_retry_is_exact_and_preserves_frontier(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, state_path, original = initialized_fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            active = archive_x_legacy.claim_window(
                state["legacy_backfill"],
                owner_run_id="run-a",
                claimed_at="2026-07-22T12:01:00Z",
            )
            window = active["active_window"]
            review = archive_x_legacy.mark_manual_review(
                active,
                window_id_value=window["window_id"],
                reason="ambiguous source response",
                observed_at="2026-07-22T12:02:00Z",
            )

            with self.assertRaisesRegex(archive_x.ArchiveError, "window guard"):
                archive_x_legacy.retry_manual_review(
                    review,
                    window_id_value="wrong",
                    operator_reason="reviewed",
                    retried_at="2026-07-22T12:03:00Z",
                )
            retried = archive_x_legacy.retry_manual_review(
                review,
                window_id_value=window["window_id"],
                operator_reason="operator approved an exact replay",
                retried_at="2026-07-22T12:03:00Z",
            )
            self.assertEqual(retried["status"], "pending")
            self.assertEqual(retried["next_until"], review["next_until"])
            self.assertEqual(original["resume"]["cursor"], "3_29116490825/")

    def test_window_attempt_limit_enters_manual_review(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, state_path, _ = initialized_fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            active = archive_x_legacy.claim_window(
                state["legacy_backfill"],
                owner_run_id="run-a",
                claimed_at="2026-07-22T12:01:00Z",
            )
            active["active_window"]["attempt"] = 3
            stopped = archive_x_legacy.resume_active_window(
                active,
                owner_run_id="run-b",
                resumed_at="2026-07-22T12:02:00Z",
                attempt_limit=3,
            )
            self.assertEqual(stopped["status"], "manual_review")
            self.assertEqual(stopped["next_until"], active["next_until"])

    def test_retry_cli_requires_exact_window_and_preserves_modern_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, state_path, original = initialized_fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            active = archive_x_legacy.claim_window(
                state["legacy_backfill"],
                owner_run_id="run-a",
                claimed_at="2026-07-22T12:01:00Z",
            )
            window = active["active_window"]
            state["legacy_backfill"] = archive_x_legacy.mark_manual_review(
                active,
                window_id_value=window["window_id"],
                reason="ambiguous source response",
                observed_at="2026-07-22T12:02:00Z",
            )
            archive_x.atomic_write_json(state_path, state)

            with redirect_stdout(io.StringIO()):
                self.assertEqual(
                    archive_x_legacy.main(
                        [
                            "--user",
                            "alice",
                            "--output-root",
                            str(root),
                            "retry",
                            "--window-id",
                            window["window_id"],
                            "--reason",
                            "operator approved exact replay",
                        ]
                    ),
                    0,
                )
            retried = archive_x.load_json(state_path, {})
            self.assertEqual(retried["legacy_backfill"]["status"], "pending")
            self.assertEqual(retried["resume"], original["resume"])


if __name__ == "__main__":
    unittest.main()
