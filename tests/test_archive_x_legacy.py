import importlib
import hashlib
import io
import json
import sys
import tempfile
import types
import unittest
from argparse import Namespace
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from gallery_dl.extractor.twitter import TwitterAPI, TwitterExtractor


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
archive_x = importlib.import_module("archive_x")
archive_x_legacy = importlib.import_module("archive_x_legacy")
archive_x_local = importlib.import_module("archive_x_local")

FIXTURE = json.loads(
    (REPO / "tests" / "fixtures" / "x_legacy_transition.json").read_text(
        encoding="utf-8"
    )
)


@contextmanager
def isolated_cli_locks(root: Path):
    """Keep CLI mutation tests independent from a real archive lock owner."""
    original_lock = archive_x.exclusive_lock
    mapped: dict[str, Path] = {}

    @contextmanager
    def temporary_lock(path: Path):
        key = str(path)
        target = mapped.setdefault(
            key, root / "test-locks" / f"lock-{len(mapped)}.lock"
        )
        with original_lock(target):
            yield

    with mock.patch.object(
        archive_x_legacy.archive_x, "exclusive_lock", temporary_lock
    ):
        yield


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


def strict_transition_archive(root: Path):
    user_dir, state_path, state = fixture_archive(root)
    run_id = "20260720T023918Z-fixture"
    raw_path = user_dir / "runs" / run_id / "raw" / "timeline.posts.incomplete.jsonl"
    archive_x.atomic_write_jsonl(
        raw_path,
        [
            {
                "tweet_id": "29116490825",
                "date": "2010-10-29 19:30:34",
                "archived_at": "2026-07-20T12:00:00Z",
                "author": {"id": "12345", "name": "alice"},
                "user": {"id": "12345", "name": "alice"},
                "reply_id": None,
                "retweet_id": None,
                "count": 0,
            }
        ],
    )
    archive_x.atomic_write_json(
        user_dir / "runs" / run_id / "manifest.json",
        {
            "run_id": run_id,
            "started_at": "2026-07-20T02:39:18Z",
            "completed_at": "2026-07-21T01:04:43Z",
            "status": "stalled",
            "failure_stage": "timeline_no_progress_watchdog",
            "reposts_included": True,
            "limited_run": False,
            "retry_failed_only": False,
            "date_after": None,
            "endpoints": [
                {
                    "endpoint": "timeline",
                    "status": "stalled",
                    "exit_code": 1,
                    "interrupted": False,
                    "stalled": True,
                    "stalled_rate_limit_cycles": 3,
                    "resume_cursor": "3_29116490825/",
                    "synthetic_resume_cursor": False,
                    "metadata_complete": False,
                    "other_error_count": 0,
                    "raw_has_record": True,
                    "raw_path": str(raw_path.relative_to(user_dir)),
                }
            ],
        },
    )
    return user_dir, state_path, state


class AutomaticLegacyTransitionTests(unittest.TestCase):
    def test_exact_pre_snowflake_watchdog_boundary_is_proven(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, _ = strict_transition_archive(Path(directory))
            before = archive_x.sha256_file(state_path)

            result = archive_x_legacy.classify_legacy_transition(
                user_dir, expected_run_id="20260720T023918Z-fixture"
            )

            self.assertEqual(result["decision"], "proven")
            self.assertEqual(
                result["reason"], "exact_pre_snowflake_watchdog_boundary"
            )
            self.assertEqual(result["oldest_post_id"], "29116490825")
            self.assertRegex(result["source_raw_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(archive_x.sha256_file(state_path), before)

    def test_one_clean_window_proves_an_exact_pre_snowflake_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, _state_path, _ = strict_transition_archive(Path(directory))
            manifest_path = (
                user_dir
                / "runs"
                / "20260720T023918Z-fixture"
                / "manifest.json"
            )
            manifest = archive_x.load_json(manifest_path, {})
            manifest["endpoints"][0]["stalled_rate_limit_cycles"] = 1
            archive_x.atomic_write_json(manifest_path, manifest)

            result = archive_x_legacy.classify_legacy_transition(user_dir)

            self.assertEqual(result["decision"], "proven")
            self.assertEqual(
                result["reason"],
                "exact_pre_snowflake_single_window_boundary",
            )
            self.assertEqual(result["confirmation_cycles"], 1)

    def test_prior_matching_run_confirms_a_new_interrupted_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, _state_path, _ = strict_transition_archive(Path(directory))
            prior_manifest_path = (
                user_dir
                / "runs"
                / "20260720T023918Z-fixture"
                / "manifest.json"
            )
            prior = archive_x.load_json(prior_manifest_path, {})
            prior["endpoints"][0]["stalled_rate_limit_cycles"] = 1
            archive_x.atomic_write_json(prior_manifest_path, prior)

            latest_run_id = "20260721T023918Z-fixture"
            latest_raw = (
                user_dir
                / "runs"
                / latest_run_id
                / "raw"
                / "timeline.posts.incomplete.jsonl"
            )
            archive_x.atomic_write_jsonl(
                latest_raw,
                list(
                    archive_x.iter_jsonl(
                        user_dir / prior["endpoints"][0]["raw_path"]
                    )
                ),
            )
            latest = json.loads(json.dumps(prior))
            latest.update(
                run_id=latest_run_id,
                started_at="2026-07-21T02:39:18Z",
                completed_at="2026-07-22T01:04:43Z",
                status="interrupted",
                failure_stage=None,
            )
            latest["endpoints"][0].update(
                status="interrupted",
                interrupted=True,
                stalled=False,
                stalled_rate_limit_cycles=0,
                raw_path=str(latest_raw.relative_to(user_dir)),
            )
            archive_x.atomic_write_json(
                latest_raw.parents[1] / "manifest.json", latest
            )

            result = archive_x_legacy.classify_legacy_transition(user_dir)

            self.assertEqual(result["decision"], "proven")
            self.assertEqual(
                result["reason"], "exact_pre_snowflake_historical_boundary"
            )
            self.assertEqual(result["source_run_id"], latest_run_id)
            self.assertEqual(
                result["confirmation_run_ids"],
                ["20260720T023918Z-fixture"],
            )

    def test_transition_watchdog_is_short_only_for_verified_legacy_floor(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, _ = strict_transition_archive(Path(directory))
            state = archive_x.load_json(state_path, {})

            legacy = archive_x_legacy.transition_watchdog_policy(
                user_dir, state, ambiguous_cycles=3
            )
            self.assertEqual(legacy["cycles"], 1)
            self.assertEqual(legacy["reason"], "verified_pre_snowflake_floor")

            archive_x.atomic_write_jsonl(
                user_dir / "dataset" / "posts.jsonl",
                [
                    {
                        "post_id": "29116490825",
                        "posted_at": "2010-11-05 00:00:00",
                    }
                ],
            )
            ambiguous = archive_x_legacy.transition_watchdog_policy(
                user_dir, state, ambiguous_cycles=3
            )
            self.assertEqual(
                ambiguous,
                {"cycles": 3, "reason": "ambiguous_or_snowflake_boundary"},
            )

    def test_weak_or_failed_boundaries_never_prove(self):
        mutations = {
            "generic stall": lambda manifest, raw: manifest.update(
                failure_stage="other"
            ),
            "api error": lambda manifest, raw: manifest["endpoints"][0].update(
                other_error_count=1
            ),
            "interrupted": lambda manifest, raw: manifest["endpoints"][0].update(
                interrupted=True
            ),
            "metadata complete": lambda manifest, raw: manifest["endpoints"][0].update(
                metadata_complete=True
            ),
            "successful exit": lambda manifest, raw: manifest["endpoints"][0].update(
                exit_code=0
            ),
            "limited run": lambda manifest, raw: manifest.update(limited_run=True),
            "missing raw evidence": lambda manifest, raw: manifest["endpoints"][0].update(
                raw_has_record=False
            ),
            "incremental cutoff": lambda manifest, raw: manifest.update(
                date_after="2026-07-01T00:00:00Z"
            ),
            "no unchanged window": lambda manifest, raw: manifest["endpoints"][0].update(
                stalled_rate_limit_cycles=0
            ),
            "identity mismatch": lambda manifest, raw: archive_x.atomic_write_jsonl(
                raw,
                [
                    {
                        "tweet_id": "29116490825",
                        "date": "2010-10-29 19:30:34",
                        "author": {"id": "999"},
                        "user": {"id": "999"},
                    }
                ],
            ),
            "post-snowflake metadata": lambda manifest, raw: archive_x.atomic_write_jsonl(
                raw,
                [
                    {
                        "tweet_id": "29116490825",
                        "date": "2010-11-05 00:00:00",
                        "author": {"id": "12345"},
                        "user": {"id": "12345"},
                    }
                ],
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                user_dir, _state_path, _ = strict_transition_archive(Path(directory))
                manifest_path = (
                    user_dir
                    / "runs"
                    / "20260720T023918Z-fixture"
                    / "manifest.json"
                )
                manifest = archive_x.load_json(manifest_path, {})
                raw_path = user_dir / manifest["endpoints"][0]["raw_path"]
                mutate(manifest, raw_path)
                archive_x.atomic_write_json(manifest_path, manifest)

                result = archive_x_legacy.classify_legacy_transition(user_dir)

                self.assertNotEqual(result["decision"], "proven")

    def test_automatic_initialization_preserves_cursor_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, original = strict_transition_archive(Path(directory))
            original_hash = archive_x.sha256_file(state_path)

            result = archive_x_legacy.automatic_initialize_legacy(
                user_dir,
                initialized_at="2026-07-22T12:00:00Z",
                expected_run_id="20260720T023918Z-fixture",
            )

            self.assertTrue(result["legacy_initialized"])
            self.assertTrue(result["modern_head_initialized"])
            state = archive_x.load_json(state_path, {})
            self.assertEqual(state["resume"], original["resume"])
            self.assertEqual(state["legacy_backfill"]["status"], "pending")
            self.assertEqual(
                state["modern_head"]["baseline_started_at"],
                "2026-07-20T02:39:18Z",
            )
            backup = Path(result["backup_path"])
            self.assertEqual(archive_x.sha256_file(backup), original_hash)
            self.assertEqual(backup.stat().st_mode & 0o777, 0o600)

            repeated = archive_x_legacy.automatic_initialize_legacy(
                user_dir, initialized_at="2026-07-22T13:00:00Z"
            )
            self.assertFalse(repeated["legacy_initialized"])
            self.assertFalse(repeated["modern_head_initialized"])
            self.assertEqual(repeated["state"], state)

            with mock.patch.object(
                archive_x_legacy,
                "classify_legacy_transition",
                side_effect=AssertionError("legacy detection repeated"),
            ):
                repeated = archive_x_legacy.automatic_initialize_legacy(
                    user_dir, initialized_at="2026-07-22T14:00:00Z"
                )
            self.assertFalse(repeated["legacy_initialized"])

    def test_existing_modern_head_accepts_fractional_archive_timestamps(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, _ = strict_transition_archive(Path(directory))
            initialized = archive_x_legacy.automatic_initialize_legacy(
                user_dir,
                initialized_at="2026-07-22T12:00:00Z",
                expected_run_id="20260720T023918Z-fixture",
            )
            state = initialized["state"]
            state["modern_head"]["last_successful_started_at"] = (
                "2026-07-23T06:41:54.531516Z"
            )
            state["modern_head"]["last_successful_completed_at"] = (
                "2026-07-23T06:45:17.511594Z"
            )
            archive_x.atomic_write_json(state_path, state)

            repeated = archive_x_legacy.automatic_initialize_legacy(
                user_dir, initialized_at="2026-07-23T07:00:00Z"
            )

            self.assertFalse(repeated["legacy_initialized"])
            self.assertFalse(repeated["modern_head_initialized"])
            self.assertEqual(
                repeated["state"]["modern_head"],
                state["modern_head"],
            )

    def test_initialized_transition_binds_confirmation_files_by_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, _state_path, _ = strict_transition_archive(Path(directory))
            initialized = archive_x_legacy.automatic_initialize_legacy(
                user_dir,
                initialized_at="2026-07-22T12:00:00Z",
                expected_run_id="20260720T023918Z-fixture",
            )
            source = initialized["state"]["legacy_backfill"]["source"]
            confirmation = source["transition_confirmations"][0]
            manifest = archive_x.load_json(
                user_dir
                / "runs"
                / confirmation["run_id"]
                / "manifest.json",
                {},
            )
            raw = user_dir / manifest["endpoints"][0]["raw_path"]
            raw.write_text(raw.read_text(encoding="utf-8") + "{}\n", encoding="utf-8")

            with self.assertRaisesRegex(
                archive_x.ArchiveError,
                "transition confirmation raw evidence changed",
            ):
                archive_x_legacy.automatic_initialize_legacy(
                    user_dir, initialized_at="2026-07-22T13:00:00Z"
                )

    def test_failed_state_write_leaves_prior_state_and_verified_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, original = strict_transition_archive(Path(directory))
            original_hash = archive_x.sha256_file(state_path)

            def fail_state(path, value):
                if Path(path) == state_path:
                    raise OSError("injected state write failure")
                archive_x.atomic_write_json(Path(path), value)

            with self.assertRaisesRegex(OSError, "injected state write failure"):
                archive_x_legacy.automatic_initialize_legacy(
                    user_dir,
                    initialized_at="2026-07-22T12:00:00Z",
                    writer=fail_state,
                )

            self.assertEqual(archive_x.load_json(state_path, {}), original)
            plan = archive_x_legacy.initialization_plan(user_dir)
            backup = archive_x_legacy.legacy_backup_path(user_dir, plan["source"])
            self.assertEqual(archive_x.sha256_file(backup), original_hash)

    def test_failed_backup_write_leaves_prior_state_without_handoff(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, original = strict_transition_archive(Path(directory))
            plan = archive_x_legacy.initialization_plan(user_dir)
            backup = archive_x_legacy.legacy_backup_path(user_dir, plan["source"])

            def fail_backup(path, value):
                raise OSError("injected backup write failure")

            with self.assertRaisesRegex(OSError, "injected backup write failure"):
                archive_x_legacy.automatic_initialize_legacy(
                    user_dir,
                    initialized_at="2026-07-22T12:00:00Z",
                    writer=fail_backup,
                )

            self.assertEqual(archive_x.load_json(state_path, {}), original)
            self.assertFalse(backup.exists())

    def test_existing_legacy_without_exact_backup_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, state_path, _ = strict_transition_archive(Path(directory))
            plan = archive_x_legacy.initialization_plan(user_dir)
            state = archive_x.load_json(state_path, {})
            initialized, _ = archive_x_legacy.initialize_state(
                state, plan, plan["confirmation_token"], "2026-07-22T12:00:00Z"
            )
            archive_x.atomic_write_json(state_path, initialized)

            with self.assertRaisesRegex(
                archive_x.ArchiveError, "missing its exact pre-init backup"
            ):
                archive_x_legacy.automatic_initialize_legacy(
                    user_dir, initialized_at="2026-07-22T13:00:00Z"
                )


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

            with isolated_cli_locks(root), redirect_stdout(io.StringIO()):
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

            with isolated_cli_locks(root), redirect_stdout(io.StringIO()):
                self.assertEqual(archive_x_legacy.main(args), 0)
            self.assertEqual(state_path.read_bytes(), initialized_bytes)

    def test_cli_init_failed_atomic_write_preserves_previous_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, _ = fixture_archive(root)
            plan = archive_x_legacy.initialization_plan(user_dir)
            before = state_path.read_bytes()
            errors = io.StringIO()

            with isolated_cli_locks(root), mock.patch.object(
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
                    "since": "2010-10-27T00:00:00Z",
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
        "max_root_windows": 1,
        "request_limit": 6,
        "root_window_days": 1,
        "empty_tail_pages": 2,
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
    return archive_x_legacy.LegacyRunOptions(**values)


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
    user_dir = Path(kwargs["user_dir"])
    run_dir = Path(kwargs["run_dir"])
    raw_path = run_dir / "raw" / f"{kwargs['walk_id']}.fixture.posts.jsonl"
    archive_x.atomic_write_jsonl(raw_path, [metadata])
    query, _url = archive_x_legacy.legacy_query(
        kwargs["handle"],
        kwargs["since"],
        kwargs["until"],
        include_reposts=kwargs["include_reposts"],
    )
    query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
    telemetry_path = run_dir / f"{kwargs['walk_id']}.fixture.telemetry.json"
    archive_x.atomic_write_json(
        telemetry_path,
        {
            "schema_version": 1,
            "request_limit": kwargs["request_limit"],
            "empty_tail_pages": kwargs["empty_tail_pages"],
            "api_requests": 1,
            "search_requests": 1,
            "request_cap_reached": False,
            "terminal_reason": "no_cursor",
            "exit_code": 0,
            "pages": [
                {
                    "request_number": 1,
                    "query_sha256": query_hash,
                    "submitted_cursor_sha256": None,
                    "returned_cursor_sha256": None,
                    "cursor_repeated": False,
                    "tweet_entry_count": 1,
                    "api_error_count": 0,
                }
            ],
            "profile_user_ids": [kwargs["requested_user_id"]],
            "profile_requests": 0,
            "identity_source": "bound_numeric_id",
            "opaque_cursor_values_persisted": False,
        },
    )
    return {
        "archive_run_id": kwargs["archive_run_id"],
        "walk_id": kwargs["walk_id"],
        "endpoint": kwargs["walk_id"],
        "since": kwargs["since"],
        "until": kwargs["until"],
        "query_sha256": query_hash,
        "status": "valid",
        "exit_code": 0,
        "duration_seconds": 1.0,
        "interrupted": False,
        "stalled": False,
        "stalled_rate_limit_cycles": 0,
        "validation_error": None,
        "terminal_reason": "no_cursor",
        "request_limit": kwargs["request_limit"],
        "empty_tail_pages": kwargs["empty_tail_pages"],
        "search_requests": 1,
        "api_requests": 1,
        "profile_requests": 0,
        "identity_source": "bound_numeric_id",
        "records": {
            "raw_count": 1,
            "accepted_count": 1,
            "accepted_ids": [str(post_id)],
            "accepted_records": [metadata],
            "overlap_excluded_ids": [],
        },
        "raw_path": str(raw_path.relative_to(user_dir)),
        "raw_sha256": archive_x.sha256_file(raw_path),
        "telemetry_path": str(telemetry_path.relative_to(user_dir)),
        "telemetry_sha256": archive_x.sha256_file(telemetry_path),
        "config_path": f"runs/fake/{kwargs['walk_id']}.config.json",
        "config_sha256": "d" * 64,
        "log_path": f"runs/fake/{kwargs['walk_id']}.log",
        "command": ["fake"],
    }


class LegacyRecordValidationTests(unittest.TestCase):
    def test_empty_repost_and_query_overlap_are_canonicalized_exactly(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_path = Path(directory) / "walk.jsonl"
            common = {
                "archived_at": "2026-08-01T00:00:00Z",
                "reply_id": 0,
                "count": 0,
            }
            records = [
                {
                    **common,
                    "tweet_id": 101,
                    "date": "2010-10-29 12:00:00",
                    "author": {"id": 12345, "name": "alice"},
                    "user": {"id": 12345, "name": "alice"},
                    "retweet_id": 0,
                },
                {
                    **common,
                    "tweet_id": 102,
                    "date": "2010-10-28 23:59:59",
                    "author": {"id": 12345, "name": "alice"},
                    "user": {"id": 12345, "name": "alice"},
                    "retweet_id": 0,
                },
                {
                    **common,
                    "tweet_id": 103,
                    "date": "2010-10-30 00:00:00",
                    "author": {"id": 12345, "name": "alice"},
                    "user": {"id": 12345, "name": "alice"},
                    "retweet_id": 0,
                },
                {
                    **common,
                    "tweet_id": 104,
                    "date": "2010-10-29 18:00:00",
                    "author": {"id": 99999, "name": "bob"},
                    "user": {"id": 12345, "name": "alice"},
                    "retweet_id": 77,
                },
            ]
            archive_x.atomic_write_jsonl(raw_path, records)

            validated = archive_x_legacy.validate_walk_records(
                raw_path,
                since="2010-10-29T00:00:00Z",
                until="2010-10-30T00:00:00Z",
                requested_user_id="12345",
                requested_handle="alice",
                include_reposts=True,
            )

            self.assertEqual(validated["accepted_ids"], ["101", "104"])
            self.assertEqual(validated["overlap_excluded_ids"], ["102", "103"])
            with self.assertRaisesRegex(
                archive_x.ArchiveError, "frozen repost policy"
            ):
                archive_x_legacy.validate_walk_records(
                    raw_path,
                    since="2010-10-29T00:00:00Z",
                    until="2010-10-30T00:00:00Z",
                    requested_user_id="12345",
                    requested_handle="alice",
                    include_reposts=False,
                )

            archive_x.atomic_write_jsonl(raw_path, [])
            empty = archive_x_legacy.validate_walk_records(
                raw_path,
                since="2010-10-29T00:00:00Z",
                until="2010-10-30T00:00:00Z",
                requested_user_id="12345",
                requested_handle="alice",
                include_reposts=True,
            )
            self.assertEqual(empty["accepted_ids"], [])
            self.assertEqual(empty["accepted_count"], 0)


class LegacyOrchestrationTests(unittest.TestCase):
    def test_legacy_command_uses_the_same_actual_request_lane(self):
        pacing = archive_x_legacy.pacing_x
        options = pacing.SchedulerOptions(
            database=Path("/archive/state.sqlite3"),
            scope_id="12345",
            delay_low=4.0,
            delay_high=8.0,
            lease_seconds=180.0,
            backoff_429_seconds=300.0,
        )
        command = archive_x_legacy.legacy_gallery_command(
            REPO,
            Path("/archive/config.json"),
            Path("/archive/walk.json"),
            Path("/archive/requests.json"),
            request_limit=6,
            empty_tail_pages=2,
            retries=1,
            http_timeout=60,
            requested_user_id="12345",
            url="https://x.com/search?q=redacted",
            scheduler_options=options,
        )

        self.assertEqual(command[command.index("--sleep-retries") + 1], "0")
        self.assertEqual(command[command.index("--sleep-429") + 1], "0")
        for option in pacing.SCHEDULER_OPTIONS:
            self.assertIn(option, command)

    def test_no_root_budget_runs_to_exact_floor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _user_dir, state_path, _ = initialized_fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            state["legacy_backfill"]["floor_since"] = "2010-10-29T00:00:00Z"
            archive_x.atomic_write_json(state_path, state)

            def fake_walk(**kwargs):
                return valid_walk(kwargs)

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=fake_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root, max_root_windows=None),
                    REPO,
                    root,
                    "alice",
                    "1.32.4",
                )

            state = archive_x.load_json(state_path, {})
            self.assertEqual(result["status"], "complete")
            self.assertIsNone(result["window_limit"])
            self.assertEqual(state["legacy_backfill"]["status"], "complete")
            self.assertEqual(
                state["legacy_backfill"]["next_until"],
                "2010-10-29T00:00:00Z",
            )

    def test_standalone_run_window_limit_is_optional(self):
        parser = archive_x_legacy.build_parser()
        args = parser.parse_args(["--user", "alice", "run"])
        self.assertIsNone(args.windows)
        self.assertEqual(args.root_window_days, 3)
        self.assertEqual(args.empty_tail_pages, 2)
        self.assertEqual(args.window_delay, "5-15")

    def test_normal_run_claims_three_days_but_existing_active_bounds_are_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, state_path, _ = initialized_fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            claimed = archive_x_legacy.claim_window(
                state["legacy_backfill"],
                owner_run_id="run-a",
                claimed_at="2026-07-22T13:00:00Z",
                root_window_days=3,
            )
            self.assertEqual(claimed["active_window"]["since"], (
                "2010-10-27T00:00:00Z"
            ))
            self.assertEqual(claimed["active_window"]["until"], (
                "2010-10-30T00:00:00Z"
            ))

            resumed = archive_x_legacy.resume_active_window(
                claimed,
                owner_run_id="run-b",
                resumed_at="2026-07-22T14:00:00Z",
                attempt_limit=3,
            )
            self.assertEqual(
                resumed["active_window"]["since"],
                claimed["active_window"]["since"],
            )
            self.assertEqual(
                resumed["active_window"]["until"],
                claimed["active_window"]["until"],
            )

    def test_two_matching_walks_merge_then_advance_and_queue_media(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, original = initialized_fixture_archive(root)
            progress_events = []
            runner = object()
            observed_runners = []

            def fake_walk(**kwargs):
                observed_runners.append(kwargs.get("runner"))
                return valid_walk(kwargs, count=1)

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=fake_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x,
                "sleep_random",
                side_effect=AssertionError("stacked legacy pacing ran"),
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4",
                    progress_callback=progress_events.append,
                    runner=runner,
                )

            state = archive_x.load_json(state_path, {})
            self.assertEqual(result["status"], "limited")
            self.assertEqual(
                state["legacy_backfill"]["next_until"],
                "2010-10-29T00:00:00Z",
            )
            self.assertEqual(state["resume"], original["resume"])
            self.assertEqual(
                state.get("pending_media", []), original.get("pending_media", [])
            )
            posts = list(archive_x.iter_jsonl(user_dir / "dataset" / "posts.jsonl"))
            self.assertEqual(len(posts), 2)
            with archive_x_legacy.context_x.ContextDB(
                user_dir / "_state" / "context.sqlite3", create=False
            ) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM archive_posts "
                        "WHERE post_id='29000000000'"
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    database.connection.execute(
                        """SELECT state FROM asset_jobs
                             WHERE owner_kind='post'
                               AND owner_id='29000000000'
                               AND media_ordinal=1"""
                    ).fetchone()[0],
                    "needs_refresh",
                )
            self.assertTrue(result["windows"][0]["state_committed"])
            self.assertTrue(
                all(
                    "command" not in walk
                    for walk in result["windows"][0]["walks"]
                )
            )
            self.assertEqual(
                [event["event"] for event in progress_events],
                [
                    "window_started", "walk_completed", "walk_completed",
                    "window_committed",
                ],
            )
            self.assertEqual(progress_events[-1]["dataset_posts"], 0)
            self.assertEqual(result["portable_export"]["dataset_posts"], 1)
            self.assertEqual(result["portable_export"]["window_count"], 1)
            self.assertEqual(observed_runners, [runner, runner])
            self.assertEqual(
                result["portable_export"]["status"],
                "deferred_to_unified_checkpoint",
            )

    def test_confirmed_legacy_descriptor_queues_direct_asset_without_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, _state_path, _ = initialized_fixture_archive(root)

            def descriptor_walk(**kwargs):
                result = valid_walk(kwargs, count=1)
                operation_id = (
                    f"{kwargs['archive_run_id']}:{kwargs['walk_id']}"
                )
                url = "https://pbs.twimg.com/media/legacy-fixture.jpg?name=orig"
                filename = "2010-10-29T12-00-00_29000000000_1_alice.jpg"
                row = {
                    "schema": archive_x_legacy.descriptor_x.SCHEMA,
                    "schema_version": archive_x_legacy.descriptor_x.SCHEMA_VERSION,
                    "operation_id": operation_id,
                    "run_id": kwargs["archive_run_id"],
                    "source_kind": "legacy",
                    "source_operation": "legacy",
                    "owner_kind": "post",
                    "owner_id": "29000000000",
                    "post_id": "29000000000",
                    "media_ordinal": 1,
                    "media_type": "photo",
                    "extension": "jpg",
                    "private_url": url,
                    "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
                    "url_host": "pbs.twimg.com",
                    "filename": filename,
                    "relative_directory": "users/alice/media/2010/10",
                    "relative_path": f"users/alice/media/2010/10/{filename}",
                    "width": 1200,
                    "height": 800,
                    "duration_seconds": None,
                    "bitrate": None,
                    "alt_text": "legacy fixture",
                    "variant": {"type": "photo", "width": 1200, "height": 800},
                    "captured_at": "2026-08-01T00:00:00Z",
                }
                row["descriptor_sha256"] = hashlib.sha256(
                    archive_x_legacy.descriptor_x.canonical_json(
                        archive_x_legacy.descriptor_x.descriptor_payload(row)
                    ).encode()
                ).hexdigest()
                row = archive_x_legacy.descriptor_x.normalize_record(row)
                artifact = (
                    Path(kwargs["run_dir"])
                    / "raw"
                    / f"{kwargs['walk_id']}.fixture.descriptors.jsonl"
                )
                archive_x.atomic_write_jsonl(artifact, [row])
                result.update(
                    {
                        "descriptor_artifact_path": str(
                            artifact.relative_to(user_dir)
                        ),
                        "descriptor_artifact_sha256": archive_x.sha256_file(
                            artifact
                        ),
                        "descriptor_operation_id": operation_id,
                        "descriptor_source_kind": "legacy",
                        "descriptor_source_operation": "legacy",
                    }
                )
                return result

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=descriptor_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4"
                )

            self.assertEqual(
                result["windows"][0]["descriptor_commit"]["rows_accepted"], 2
            )
            with archive_x_legacy.context_x.ContextDB(
                user_dir / "_state" / "context.sqlite3", create=False
            ) as database:
                descriptors = database.connection.execute(
                    "SELECT COUNT(*) FROM descriptor_generations "
                    "WHERE owner_kind='post' AND owner_id='29000000000'"
                ).fetchone()[0]
                direct_jobs = database.connection.execute(
                    "SELECT COUNT(*) FROM asset_jobs WHERE owner_id='29000000000'"
                ).fetchone()[0]
                refreshes = database.connection.execute(
                    "SELECT COUNT(*) FROM descriptor_refresh_jobs "
                    "WHERE owner_id='29000000000'"
                ).fetchone()[0]
            self.assertGreaterEqual(descriptors, 1)
            self.assertEqual(direct_jobs, 1)
            self.assertEqual(refreshes, 0)

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

            self.assertEqual(result["status"], "limited")
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

    def test_manifest_does_not_persist_command_or_private_search_query(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            initialized_fixture_archive(root)

            with mock.patch.object(
                archive_x_legacy,
                "run_legacy_walk",
                side_effect=lambda **kwargs: valid_walk(kwargs),
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root),
                    REPO,
                    root,
                    "alice",
                    "1.32.4",
                )

            public_manifest = json.dumps(result, sort_keys=True)
            self.assertNotIn('"command"', public_manifest)
            self.assertNotIn("from:alice", public_manifest)
            self.assertNotIn("since_time:", public_manifest)


class LegacyRecoveryTests(unittest.TestCase):
    def test_keyboard_interrupt_after_first_valid_walk_retains_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _user_dir, state_path, _ = initialized_fixture_archive(root)
            calls = 0

            def interrupt_after_first(**kwargs):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return valid_walk(kwargs)
                raise KeyboardInterrupt

            with mock.patch.object(
                archive_x_legacy,
                "run_legacy_walk",
                side_effect=interrupt_after_first,
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                with self.assertRaises(KeyboardInterrupt):
                    archive_x_legacy.run_legacy_archive(
                        legacy_run_args(root), REPO, root, "alice", "1.32.4"
                    )

            retained = archive_x.load_json(state_path, {})["legacy_backfill"]
            self.assertEqual(retained["status"], "active")
            self.assertEqual(
                len(retained["active_window"]["leaves"][0]["observations"]),
                1,
            )

    def test_crash_after_indexed_commit_replays_and_deduplicates(self):
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
                2,
            )
            with archive_x_legacy.context_x.ContextDB(
                user_dir / "_state" / "context.sqlite3", create=False
            ) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM legacy_intervals"
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM archive_posts WHERE post_id='29000000000'"
                    ).fetchone()[0],
                    1,
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
            self.assertEqual(result["status"], "limited")
            self.assertEqual(
                recovered["legacy_backfill"]["next_until"],
                "2010-10-29T00:00:00Z",
            )
            self.assertEqual(
                len(list(archive_x.iter_jsonl(user_dir / "dataset" / "posts.jsonl"))),
                2,
            )
            with archive_x_legacy.context_x.ContextDB(
                user_dir / "_state" / "context.sqlite3", create=False
            ) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM legacy_intervals"
                    ).fetchone()[0],
                    1,
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
                2,
            )
            with archive_x_legacy.context_x.ContextDB(
                user_dir / "_state" / "context.sqlite3", create=False
            ) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM legacy_intervals"
                    ).fetchone()[0],
                    1,
                )

    def test_export_placement_failure_stays_pending_and_replays_without_x(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, _ = initialized_fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            state["legacy_backfill"]["floor_since"] = "2010-10-29T00:00:00Z"
            archive_x.atomic_write_json(state_path, state)

            original_write = archive_x.atomic_write_json
            injected = False

            def fail_export_clear(path, value):
                nonlocal injected
                legacy_state = (
                    value.get("legacy_backfill")
                    if isinstance(value, dict)
                    else None
                )
                if (
                    not injected
                    and Path(path) == state_path
                    and isinstance(legacy_state, dict)
                    and legacy_state.get("last_indexed_checkpoint")
                    and legacy_state.get("pending_portable_exports") == []
                ):
                    injected = True
                    raise OSError("injected export-clear failure")
                return original_write(path, value)

            with mock.patch.object(
                archive_x_legacy,
                "run_legacy_walk",
                side_effect=lambda **kwargs: valid_walk(kwargs),
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ), mock.patch.object(
                archive_x_legacy.archive_x,
                "atomic_write_json",
                side_effect=fail_export_clear,
            ):
                with self.assertRaisesRegex(OSError, "export-clear failure"):
                    archive_x_legacy.run_legacy_archive(
                        legacy_run_args(root), REPO, root, "alice", "1.32.4"
                    )

            pending = archive_x.load_json(state_path, {})["legacy_backfill"]
            self.assertEqual(pending["status"], "complete")
            self.assertEqual(len(pending["pending_portable_exports"]), 1)
            with archive_x_legacy.context_x.ContextDB(
                user_dir / "_state" / "context.sqlite3", create=False
            ) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM archive_posts "
                        "WHERE post_id='29000000000'"
                    ).fetchone()[0],
                    1,
                )

            with mock.patch.object(
                archive_x_legacy,
                "run_legacy_walk",
                side_effect=AssertionError("X must not run during export repair"),
            ):
                repaired = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4"
                )

            final = archive_x.load_json(state_path, {})["legacy_backfill"]
            self.assertEqual(repaired["status"], "complete")
            self.assertEqual(final["pending_portable_exports"], [])
            self.assertEqual(final["last_indexed_checkpoint"]["window_count"], 1)

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

            with isolated_cli_locks(root), redirect_stdout(io.StringIO()):
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


class AdaptiveLegacyPolicyTests(unittest.TestCase):
    def test_sparse_360_day_fixture_reduces_search_calls_over_ninety_percent(self):
        with tempfile.TemporaryDirectory() as directory:
            _user_dir, state_path, _ = initialized_fixture_archive(Path(directory))
            state = archive_x.load_json(state_path, {})
            legacy = state["legacy_backfill"]
            initial = datetime(2010, 10, 30, tzinfo=timezone.utc)
            legacy["initial_until"] = archive_x_legacy.second_utc(initial)
            legacy["next_until"] = archive_x_legacy.second_utc(initial)
            legacy["floor_since"] = archive_x_legacy.second_utc(
                initial - timedelta(days=360)
            )
            widths = []
            index = 0
            while legacy["status"] != "complete":
                active = archive_x_legacy.claim_window(
                    legacy,
                    owner_run_id=f"fixture-{index}",
                    claimed_at="2026-08-01T00:00:00Z",
                    root_window_days=3,
                )
                window = active["active_window"]
                width = int(
                    (
                        archive_x_legacy.parse_utc(window["until"], "until")
                        - archive_x_legacy.parse_utc(window["since"], "since")
                    ).total_seconds()
                    // 86_400
                )
                widths.append(width)
                observations = [
                    {
                        "search_requests": 3,
                        "api_requests": 3,
                        "accepted_count": 0,
                    },
                    {
                        "search_requests": 3,
                        "api_requests": 3,
                        "accepted_count": 0,
                    },
                ]
                active = archive_x_legacy.update_adaptive_window_policy(
                    active,
                    confirmed_observations=observations,
                    request_limit=6,
                    empty_tail_pages=2,
                )
                legacy = archive_x_legacy.complete_window(
                    active,
                    window_id_value=window["window_id"],
                    completed_at="2026-08-01T00:00:00Z",
                    canonical_raw_sha256=f"{index + 1:064x}",
                    dataset_sha256="f" * 64,
                    walk_ids=[f"walk-{index}-a", f"walk-{index}-b"],
                )
                index += 1

        self.assertEqual(widths, [3, 6, 12, 24, 48, 90, 90, 87])
        fixed_windows = 360 // 3
        fixed_search_calls = fixed_windows * 2 * 3
        adaptive_search_calls = len(widths) * 2 * 3
        fixed_total_api_calls = fixed_search_calls + fixed_windows * 2
        adaptive_total_api_calls = adaptive_search_calls  # bound ID: no profile call
        self.assertEqual(fixed_search_calls, 720)
        self.assertEqual(adaptive_search_calls, 48)
        self.assertEqual(fixed_total_api_calls, 960)
        self.assertEqual(adaptive_total_api_calls, 48)
        self.assertGreaterEqual(
            1 - adaptive_search_calls / fixed_search_calls,
            0.50,
        )
        self.assertEqual(legacy["next_until"], legacy["floor_since"])

    def test_dense_valid_window_shrinks_only_the_next_unclaimed_root(self):
        with tempfile.TemporaryDirectory() as directory:
            _user_dir, state_path, _ = initialized_fixture_archive(Path(directory))
            legacy = archive_x.load_json(state_path, {})["legacy_backfill"]
            active = archive_x_legacy.claim_window(
                legacy,
                owner_run_id="fixture",
                claimed_at="2026-08-01T00:00:00Z",
                root_window_days=3,
            )
            original = active["active_window"].copy()
            adapted = archive_x_legacy.update_adaptive_window_policy(
                active,
                confirmed_observations=[
                    {"search_requests": 5, "api_requests": 5, "accepted_count": 30},
                    {"search_requests": 5, "api_requests": 5, "accepted_count": 30},
                ],
                request_limit=6,
                empty_tail_pages=2,
            )

        self.assertEqual(adapted["active_window"]["since"], original["since"])
        self.assertEqual(adapted["active_window"]["until"], original["until"])
        self.assertEqual(
            adapted["window_policy"]["next_window_seconds"], 129_600
        )
        self.assertEqual(
            adapted["window_policy"]["last_decision"], "dense_shrink"
        )

    def test_adaptive_and_fixed_windows_commit_the_same_canonical_posts(self):
        initial = datetime(2010, 10, 30, tzinfo=timezone.utc)
        source_records = []
        for index, age_days in enumerate((2, 17, 32, 47, 62, 77)):
            posted = initial - timedelta(days=age_days, hours=12)
            source_records.append(
                {
                    "tweet_id": 29_000_000_100 + index,
                    "date": posted.strftime("%Y-%m-%d %H:%M:%S"),
                    "author": {"id": 12345, "name": "alice"},
                    "user": {"id": 12345, "name": "alice"},
                    "reply_id": 0,
                    "retweet_id": 0,
                    "count": 0,
                    "archived_at": "2026-08-01T00:00:00Z",
                }
            )

        def exercise(root: Path, *, adaptive: bool):
            user_dir, state_path, _ = initialized_fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            state["legacy_backfill"]["initial_until"] = (
                archive_x_legacy.second_utc(initial)
            )
            state["legacy_backfill"]["next_until"] = (
                archive_x_legacy.second_utc(initial)
            )
            state["legacy_backfill"]["floor_since"] = (
                archive_x_legacy.second_utc(initial - timedelta(days=90))
            )
            archive_x.atomic_write_json(state_path, state)
            calls = []

            def source_walk(**kwargs):
                calls.append((kwargs["since"], kwargs["until"]))
                since = archive_x_legacy.parse_utc(kwargs["since"], "since")
                until = archive_x_legacy.parse_utc(kwargs["until"], "until")
                selected = [
                    record
                    for record in source_records
                    if since
                    <= archive_x.parse_datetime(record["date"])
                    < until
                ]
                placeholder_date = (since + (until - since) / 2).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                result = valid_walk(
                    kwargs,
                    post_id=str(
                        selected[0]["tweet_id"] if selected else 29_999_999_999
                    ),
                    date=(selected[0]["date"] if selected else placeholder_date),
                )
                raw_path = user_dir / result["raw_path"]
                archive_x.atomic_write_jsonl(raw_path, selected)
                result["raw_sha256"] = archive_x.sha256_file(raw_path)
                result["records"] = {
                    "raw_count": len(selected),
                    "accepted_count": len(selected),
                    "accepted_ids": sorted(
                        (str(item["tweet_id"]) for item in selected), key=int
                    ),
                    "accepted_records": selected,
                    "overlap_excluded_ids": [],
                }
                telemetry_path = user_dir / result["telemetry_path"]
                telemetry = archive_x.load_json(telemetry_path, {})
                telemetry["pages"][0]["tweet_entry_count"] = len(selected)
                archive_x.atomic_write_json(telemetry_path, telemetry)
                result["telemetry_sha256"] = archive_x.sha256_file(
                    telemetry_path
                )
                return result

            patches = [
                mock.patch.object(
                    archive_x_legacy, "run_legacy_walk", side_effect=source_walk
                ),
                mock.patch.object(
                    archive_x_legacy.archive_x, "sleep_random", return_value=0
                ),
            ]
            if not adaptive:
                patches.append(
                    mock.patch.object(
                        archive_x_legacy,
                        "update_adaptive_window_policy",
                        side_effect=lambda current, **_kwargs: current,
                    )
                )
            with patches[0], patches[1]:
                if adaptive:
                    result = archive_x_legacy.run_legacy_archive(
                        legacy_run_args(
                            root, max_root_windows=None, root_window_days=3
                        ),
                        REPO,
                        root,
                        "alice",
                        "1.32.4",
                    )
                else:
                    with patches[2]:
                        result = archive_x_legacy.run_legacy_archive(
                            legacy_run_args(
                                root, max_root_windows=None, root_window_days=3
                            ),
                            REPO,
                            root,
                            "alice",
                            "1.32.4",
                        )
            self.assertEqual(result["status"], "complete")
            with archive_x_legacy.context_x.ContextDB(
                user_dir / "_state" / "context.sqlite3", create=False
            ) as database:
                canonical = {
                    str(row[0])
                    for row in database.connection.execute(
                        "SELECT post_id FROM archive_posts"
                    )
                }
            return canonical, len(calls)

        with tempfile.TemporaryDirectory() as fixed_directory, (
            tempfile.TemporaryDirectory()
        ) as adaptive_directory:
            fixed, fixed_calls = exercise(Path(fixed_directory), adaptive=False)
            adaptive, adaptive_calls = exercise(
                Path(adaptive_directory), adaptive=True
            )

        expected = {str(record["tweet_id"]) for record in source_records}
        self.assertEqual(fixed, expected)
        self.assertEqual(adaptive, expected)
        self.assertLessEqual(adaptive_calls, fixed_calls // 2)


class DurableLegacyEvidenceTests(unittest.TestCase):
    def test_valid_invalid_valid_confirms_without_erasing_first_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _user_dir, state_path, _ = initialized_fixture_archive(root)
            calls = 0

            def sequence(**kwargs):
                nonlocal calls
                calls += 1
                result = valid_walk(kwargs)
                if calls == 2:
                    result.update(
                        {
                            "status": "ambiguous",
                            "terminal_reason": "api_error",
                            "records": None,
                            "validation_error": "transient fixture",
                        }
                    )
                return result

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=sequence
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4"
                )

            state = archive_x.load_json(state_path, {})
            self.assertEqual(calls, 3)
            self.assertEqual(result["status"], "limited")
            self.assertEqual(state["legacy_backfill"]["status"], "pending")
            self.assertEqual(
                len(state["legacy_backfill"]["last_completed_window"]["walk_ids"]),
                2,
            )

    def test_restart_reuses_one_valid_observation_and_requests_only_one_more(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _user_dir, state_path, _ = initialized_fixture_archive(root)
            first_calls = 0

            def crash_after_first(**kwargs):
                nonlocal first_calls
                first_calls += 1
                if first_calls == 1:
                    return valid_walk(kwargs)
                raise archive_x.ArchiveError("injected after first observation")

            with mock.patch.object(
                archive_x_legacy,
                "run_legacy_walk",
                side_effect=crash_after_first,
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                with self.assertRaisesRegex(
                    archive_x.ArchiveError, "after first observation"
                ):
                    archive_x_legacy.run_legacy_archive(
                        legacy_run_args(root), REPO, root, "alice", "1.32.4"
                    )

            retained = archive_x.load_json(state_path, {})["legacy_backfill"]
            self.assertEqual(
                len(retained["active_window"]["leaves"][0]["observations"]), 1
            )
            resumed_calls = 0

            def one_more(**kwargs):
                nonlocal resumed_calls
                resumed_calls += 1
                return valid_walk(kwargs)

            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=one_more
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4"
                )

            self.assertEqual(result["status"], "limited")
            self.assertEqual(resumed_calls, 1)

    def test_restart_after_two_persisted_observations_needs_no_network(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _user_dir, state_path, _ = initialized_fixture_archive(root)

            with mock.patch.object(
                archive_x_legacy,
                "run_legacy_walk",
                side_effect=lambda **kwargs: valid_walk(kwargs),
            ), mock.patch.object(
                archive_x_legacy,
                "update_adaptive_window_policy",
                side_effect=archive_x.ArchiveError(
                    "injected after second observation"
                ),
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ):
                with self.assertRaisesRegex(
                    archive_x.ArchiveError, "after second observation"
                ):
                    archive_x_legacy.run_legacy_archive(
                        legacy_run_args(root), REPO, root, "alice", "1.32.4"
                    )

            retained = archive_x.load_json(state_path, {})["legacy_backfill"]
            self.assertEqual(
                len(retained["active_window"]["leaves"][0]["observations"]), 2
            )
            retained["active_window"]["attempt"] = 3
            retained_state = archive_x.load_json(state_path, {})
            retained_state["legacy_backfill"] = retained
            archive_x.atomic_write_json(state_path, retained_state)
            with mock.patch.object(
                archive_x_legacy,
                "run_legacy_walk",
                side_effect=AssertionError("network must not run"),
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ) as _sleep:
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root), REPO, root, "alice", "1.32.4"
                )

            self.assertEqual(result["status"], "limited")
            self.assertEqual(result["windows"][0]["retained_observations_reused"], 2)

    def test_same_artifact_cannot_count_twice_and_corruption_stops_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, _ = initialized_fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            active = archive_x_legacy.claim_window(
                state["legacy_backfill"],
                owner_run_id="fixture-run",
                claimed_at="2026-08-01T00:00:00Z",
                root_window_days=1,
            )
            run_dir = user_dir / "runs" / "fixture-evidence"
            kwargs = {
                "user_dir": user_dir,
                "run_dir": run_dir,
                "handle": "alice",
                "requested_user_id": "12345",
                "archive_run_id": "fixture-evidence",
                "walk_id": "fixture-walk",
                "since": active["active_window"]["since"],
                "until": active["active_window"]["until"],
                "include_reposts": True,
                "request_limit": 6,
                "empty_tail_pages": 2,
            }
            result = valid_walk(kwargs)
            observation = archive_x_legacy.retained_observation(user_dir, result)
            retained, inserted = archive_x_legacy.append_retained_observation(
                active,
                leaf_since=kwargs["since"],
                leaf_until=kwargs["until"],
                observation=observation,
            )
            repeated, inserted_again = archive_x_legacy.append_retained_observation(
                retained,
                leaf_since=kwargs["since"],
                leaf_until=kwargs["until"],
                observation=observation,
            )
            self.assertTrue(inserted)
            self.assertFalse(inserted_again)
            self.assertIsNone(
                archive_x_legacy.confirmation_from_retained(
                    user_dir,
                    repeated["active_window"]["leaves"][0]["observations"],
                    handle="alice",
                    requested_user_id="12345",
                    include_reposts=True,
                )
            )
            forged = dict(observation)
            forged["observation_id"] = "f" * 64
            with self.assertRaisesRegex(
                archive_x.ArchiveError, "cannot count as two"
            ):
                archive_x_legacy.append_retained_observation(
                    retained,
                    leaf_since=kwargs["since"],
                    leaf_until=kwargs["until"],
                    observation=forged,
                )
            archive_x.atomic_write_jsonl(
                user_dir / observation["raw_path"],
                [{"tweet_id": 999, "date": "2010-10-29 12:00:00"}],
            )
            with self.assertRaisesRegex(archive_x.ArchiveError, "hash changed"):
                archive_x_legacy.restore_retained_observation(
                    user_dir,
                    observation,
                    handle="alice",
                    requested_user_id="12345",
                    include_reposts=True,
                )


class LegacyIndexedCommitTests(unittest.TestCase):
    def test_two_windows_use_two_indexed_commits_and_defer_portable_export(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path, _ = initialized_fixture_archive(root)
            state = archive_x.load_json(state_path, {})
            state["legacy_backfill"]["floor_since"] = "2010-10-27T00:00:00Z"
            archive_x.atomic_write_json(state_path, state)

            def window_walk(**kwargs):
                since = archive_x_legacy.parse_utc(kwargs["since"], "since")
                until = archive_x_legacy.parse_utc(kwargs["until"], "until")
                midpoint = since + (until - since) / 2
                post_id = str(28_000_000_000 + int(since.timestamp()) % 1_000_000)
                return valid_walk(
                    kwargs,
                    post_id=post_id,
                    date=midpoint.strftime("%Y-%m-%d %H:%M:%S"),
                )

            original_export = archive_x.update_post_dataset
            with mock.patch.object(
                archive_x_legacy, "run_legacy_walk", side_effect=window_walk
            ), mock.patch.object(
                archive_x_legacy.archive_x, "sleep_random", return_value=0
            ), mock.patch.object(
                archive_x_legacy.archive_x,
                "update_post_dataset",
                wraps=original_export,
            ) as materialize:
                result = archive_x_legacy.run_legacy_archive(
                    legacy_run_args(root, max_root_windows=None),
                    REPO,
                    root,
                    "alice",
                    "1.32.4",
                )

            self.assertEqual(result["status"], "complete")
            self.assertEqual(len(result["windows"]), 2)
            self.assertEqual(result["portable_export"]["window_count"], 2)
            self.assertEqual(result["portable_export"]["payload_bytes_read"], 0)
            self.assertEqual(materialize.call_count, 0)
            export = archive_x_local.checkpoint_exports(
                user_dir,
                user_dir / "_state" / "context.sqlite3",
                force=True,
            )
            self.assertTrue(
                archive_x_legacy.record_unified_export_checkpoint(
                    user_dir, export
                )
            )
            exported_state = archive_x.load_json(state_path, {})[
                "legacy_backfill"
            ]
            self.assertFalse(
                exported_state["last_completed_window"][
                    "portable_export_pending"
                ]
            )
            self.assertEqual(
                exported_state["last_portable_export"]["window_count"], 2
            )
            with archive_x_legacy.context_x.ContextDB(
                user_dir / "_state" / "context.sqlite3", create=False
            ) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM legacy_intervals"
                    ).fetchone()[0],
                    2,
                )
                plan = " ".join(
                    str(row[3])
                    for row in database.connection.execute(
                        "EXPLAIN QUERY PLAN SELECT interval_id "
                        "FROM legacy_intervals WHERE until_epoch<=? "
                        "ORDER BY until_epoch DESC,since_epoch,interval_id LIMIT 1",
                        (2_000_000_000,),
                    )
                )
            self.assertIn("legacy_intervals_bounds", plan)
            self.assertNotIn("USE TEMP B-TREE", plan)

    def test_indexed_commit_rolls_back_fully_and_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, _state_path, _ = initialized_fixture_archive(root)
            canonical = user_dir / "runs" / "fixture-index" / "raw" / "window.jsonl"
            metadata = {
                "tweet_id": 28_000_000_001,
                "date": "2010-10-29 12:00:00",
                "author": {"id": 12345, "name": "alice"},
                "user": {"id": 12345, "name": "alice"},
                "reply_id": 0,
                "retweet_id": 0,
                "archived_at": "2026-08-01T00:00:00Z",
            }
            archive_x.atomic_write_jsonl(canonical, [metadata])
            digest = archive_x.sha256_file(canonical)
            db_path = user_dir / "_state" / "context.sqlite3"
            with archive_x_legacy.context_x.ContextDB(db_path) as database:
                database.bind_identity("12345", "alice")
                database.connection.execute(
                    "CREATE TRIGGER fixture_legacy_abort BEFORE INSERT ON "
                    "legacy_intervals BEGIN SELECT RAISE(ABORT,'fixture'); END"
                )
            kwargs = {
                "canonical_path": canonical,
                "canonical_hash": digest,
                "canonical_records": [metadata],
                "handle": "alice",
                "requested_user_id": "12345",
                "run_id": "fixture-index",
                "window_id_value": "legacy-fixture-index",
                "since": "2010-10-29T00:00:00Z",
                "until": "2010-10-30T00:00:00Z",
                "observation_ids": ["a" * 64, "b" * 64],
                "observed_at": "2026-08-01T00:00:00Z",
            }
            with self.assertRaisesRegex(
                archive_x.ArchiveError, "indexed commit failed"
            ):
                archive_x_legacy.commit_indexed_legacy_window(
                    user_dir, **kwargs
                )
            with archive_x_legacy.context_x.ContextDB(
                db_path, create=False
            ) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT current_generation FROM archive_generation"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM archive_posts"
                    ).fetchone()[0],
                    0,
                )
                database.connection.execute("DROP TRIGGER fixture_legacy_abort")
            committed = archive_x_legacy.commit_indexed_legacy_window(
                user_dir, **kwargs
            )
            repeated = archive_x_legacy.commit_indexed_legacy_window(
                user_dir, **kwargs
            )
            self.assertEqual(committed["generation"], 1)
            self.assertFalse(committed["idempotent"])
            self.assertTrue(repeated["idempotent"])

    def test_indexed_legacy_commit_seeds_reply_edge_and_local_parent_directly(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, _state_path, _ = initialized_fixture_archive(root)
            canonical = user_dir / "runs" / "fixture-edge" / "raw" / "window.jsonl"
            parent = {
                "tweet_id": 28_000_000_001,
                "date": "2010-10-29 11:00:00",
                "author": {"id": 12345, "name": "alice"},
                "user": {"id": 12345, "name": "alice"},
                "reply_id": 0,
                "conversation_id": 28_000_000_001,
                "retweet_id": 0,
                "archived_at": "2026-08-01T00:00:00Z",
            }
            child = {
                **parent,
                "tweet_id": 28_000_000_002,
                "date": "2010-10-29 12:00:00",
                "reply_id": 28_000_000_001,
            }
            archive_x.atomic_write_jsonl(canonical, [parent, child])
            db_path = user_dir / "_state" / "context.sqlite3"
            with archive_x_legacy.context_x.ContextDB(db_path) as database:
                database.bind_identity("12345", "alice")

            committed = archive_x_legacy.commit_indexed_legacy_window(
                user_dir,
                canonical_path=canonical,
                canonical_hash=archive_x.sha256_file(canonical),
                canonical_records=[parent, child],
                handle="alice",
                requested_user_id="12345",
                run_id="fixture-edge",
                window_id_value="legacy-fixture-edge",
                since="2010-10-29T00:00:00Z",
                until="2010-10-30T00:00:00Z",
                observation_ids=["a" * 64, "b" * 64],
                observed_at="2026-08-01T00:00:00Z",
            )

            self.assertEqual(committed["new_edges"], 1)
            self.assertEqual(committed["local_parents"], 1)
            with archive_x_legacy.context_x.ContextDB(
                db_path, create=False
            ) as database:
                self.assertEqual(
                    tuple(
                        database.connection.execute(
                            "SELECT child_id,parent_id FROM reply_edges"
                        ).fetchone()
                    ),
                    ("28000000002", "28000000001"),
                )
                self.assertEqual(
                    database.connection.execute(
                        "SELECT state FROM targets WHERE post_id='28000000001'"
                    ).fetchone()[0],
                    "captured",
                )
                self.assertEqual(
                    database.connection.execute(
                        "SELECT edge_count FROM archive_sources"
                    ).fetchone()[0],
                    1,
                )


if __name__ == "__main__":
    unittest.main()
